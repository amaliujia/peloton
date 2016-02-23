//
// Created by wendongli on 2/22/16.
//

#pragma once

#include "backend/common/types.h"
#include "backend/index/index.h"

#include "composited_key_equality_checker.h"
#include "composited_key_comparator.h"

namespace peloton {
  namespace index {
    template<typename KeyType, typename KeyComparator, typename KeyEqualityChecker>
    struct CompositedKey {
      static CompositedKeyComparator<KeyType, KeyComparator> comparator_;
      static CompositedKeyEqualityChecker<KeyType, KeyEqualityChecker> equality_checker_;
      const KeyType key_;
      const ItemPointer value_;
    };
  }
}
