//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!

  /* create bucket page */
  page_id_t bucket_page_id;
  Page *page = buffer_pool_manager_->NewPage(&bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  bucket_page->Init();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);

  /* create directory page */
  page = buffer_pool_manager_->NewPage(&directory_page_id_);
  HashTableDirectoryPage *directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  directory_page->Init(directory_page_id_, bucket_page_id);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t global_depth_mask = dir_page->GetGlobalDepthMask();
  uint32_t hash = Hash(key);
  uint32_t bucket_idx = hash & global_depth_mask;
  return bucket_idx;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
  uint32_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  return bucket_page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  Page *page = buffer_pool_manager_->FetchPage(directory_page_id_);
  HashTableDirectoryPage *directory_page = reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
  return directory_page;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  Page *page = buffer_pool_manager_->FetchPage(bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
  return bucket_page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
  Page *page = reinterpret_cast<Page *>(bucket_page);
  page->RLatch();
  bool success = bucket_page->GetValue(key, comparator_, result);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return success;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  Page *page = reinterpret_cast<Page *>(bucket_page);
  page->WLatch();
  bool success = bucket_page->Insert(key, value, comparator_);
  page->WUnlatch();
  if (success) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    table_latch_.RUnlock();
    return true;
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  table_latch_.RUnlock();

  /* A full bucket or a duplicate key-value pair. We have to decide which. Let SplitInsert do it. */
  return SplitInsert(transaction, key, value);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  /* if the bucket already has this key-value pair, return false */
  std::vector<ValueType> values;
  bucket_page->GetValue(key, comparator_, &values);
  if (std::find(values.begin(), values.end(), value) != values.end()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    return false;
  }

  /* if the bucket is not full, try Insert again */
  if (!bucket_page->IsFull()) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    return Insert(transaction, key, value);
  }

  /* if we don't have room for a new bucket, return false */
  if (directory_page->Size() >= DIRECTORY_ARRAY_SIZE) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.WUnlock();
    return false;
  }

  /* create a page for the new bucket */
  page_id_t new_bucket_page_id;
  Page *page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *new_bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page);

  /* update directory page */
  if (directory_page->GetLocalDepth(bucket_idx) >= directory_page->GetGlobalDepth()) {
    directory_page->IncrGlobalDepth();
  }
  uint32_t local_depth = directory_page->GetLocalDepth(bucket_idx);
  uint32_t global_depth = directory_page->GetGlobalDepth();
  uint32_t local_depth_low_bits = bucket_idx & ~(~0U >> local_depth << local_depth);
  for (uint32_t i = 0; i < (1U << (global_depth - local_depth)); i++) {
    uint32_t idx_to_split = (i << local_depth) | local_depth_low_bits;
    directory_page->IncrLocalDepth(idx_to_split);
    if ((i & 1) == 0) {
      directory_page->SetBucketPageId(idx_to_split, bucket_page_id);
    } else {
      directory_page->SetBucketPageId(idx_to_split, new_bucket_page_id);
    }
  }

  /* redistribute key-value pairs between these split images */
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    /* note that we don't check IsReadable because the bucket is full */
    KeyType key_to_redistribute = bucket_page->KeyAt(i);
    ValueType value_to_redistribute = bucket_page->ValueAt(i);
    if (KeyToPageId(key_to_redistribute, directory_page) == static_cast<uint32_t>(new_bucket_page_id)) {
      bucket_page->RemoveAt(i);
      new_bucket_page->Insert(key_to_redistribute, value_to_redistribute, comparator_);
    }
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(new_bucket_page_id, true);
  table_latch_.WUnlock();

  /* Now the original bucket is splitted, perform the insertion */
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);

  Page *page = reinterpret_cast<Page *>(bucket_page);
  page->WLatch();
  bool success = bucket_page->Remove(key, value, comparator_);
  page->WUnlatch();
  if (!success) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    table_latch_.RUnlock();
    return false;
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  table_latch_.RUnlock();

  /* The bucket MIGHT need merged. Let Merge check it. */
  Merge(transaction, key, value);

  return true;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  bool dirty_directory = false;

  while (true) {
    // There are three conditions under which we skip the merge:
    // 1. The bucket is no longer empty.
    // 2. The bucket has local depth 0.
    // 3. The bucket's local depth doesn't match its split image's local depth.

    uint32_t bucket_idx = KeyToDirectoryIndex(key, directory_page);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(bucket_page_id);
    uint32_t bucket_local_depth = directory_page->GetLocalDepth(bucket_idx);

    uint32_t split_image_idx = directory_page->GetSplitImageIndex(bucket_idx);
    page_id_t split_image_page_id = directory_page->GetBucketPageId(split_image_idx);
    HASH_TABLE_BUCKET_TYPE *split_image_page = FetchBucketPage(split_image_page_id);
    uint32_t split_image_local_depth = directory_page->GetLocalDepth(split_image_idx);

    if ((!bucket_page->IsEmpty() && !split_image_page->IsEmpty()) || bucket_local_depth <= 0 ||
        split_image_local_depth <= 0 || split_image_local_depth != bucket_local_depth) {
      buffer_pool_manager_->UnpinPage(bucket_page_id, false);
      buffer_pool_manager_->UnpinPage(split_image_page_id, false);
      break;
    }

    if (bucket_page->IsEmpty()) {
      std::swap(bucket_idx, split_image_idx);
      std::swap(bucket_page_id, split_image_page_id);
      std::swap(bucket_page, split_image_page);
      std::swap(bucket_local_depth, split_image_local_depth);
    }

    if (!dirty_directory) {
      dirty_directory = true;
    }
    uint32_t global_depth = directory_page->GetGlobalDepth();
    uint32_t local_depth = bucket_local_depth - 1;
    uint32_t local_depth_low_bits = bucket_idx & ~(~0U >> local_depth << local_depth);
    for (uint32_t i = 0; i < (1U << (global_depth - local_depth)); i++) {
      uint32_t idx_to_merge = (i << local_depth) | local_depth_low_bits;
      directory_page->DecrLocalDepth(idx_to_merge);
      directory_page->SetBucketPageId(idx_to_merge, bucket_page_id);
    }
    if (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }

    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(split_image_page_id, false);
    buffer_pool_manager_->DeletePage(split_image_page_id);
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, dirty_directory);
  table_latch_.WUnlock();
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
