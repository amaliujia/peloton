//
// Created by wendongli on 2/26/16.
//

#pragma once

#include "backend/common/types.h"
namespace peloton {
  namespace index {
    struct ItemPointerComparator {
      inline bool operator()(const ItemPointer &lhs, const ItemPointer &rhs) {
        return (lhs.block<rhs.block)||
          ((lhs.block==rhs.block)&&(lhs.offset<rhs.offset));
      }
    };
  }
}