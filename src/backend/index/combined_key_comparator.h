//
// Created by wendongli on 2/26/16.
//

#pragma once

#include "backend/common/types.h"
#include "backend/index/Item_pointer_comparator.h"
namespace peloton {
  namespace index {
    template<class KeyType, class KeyComparator, class KeyEqualityChecker>
    struct CombinedKeyComparator {
      KeyComparator key_comparator_;
      KeyEqualityChecker key_equality_checker_;
      ItemPointerComparator item_pointer_comparator_;

      inline bool operator()(const CombinedKey<KeyType> &lhs, const CombinedKey<KeyType> &rhs) {
        if(key_comparator_(lhs.Key(), rhs.Key()))
          return true;
        if(!key_equality_checker_(lhs.Key(), rhs.Key()))
          return false;
        return item_pointer_comparator_(lhs.ItemPointer(), rhs.ItemPointer());
      }
    };
  }
}