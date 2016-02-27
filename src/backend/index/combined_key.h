//
// Created by wendongli on 2/26/16.
//

#include "backend/common/types.h"

namespace peloton {
  namespace index {
    template<class KeyType>
    class CombinedKey {
    public :
      CombinedKey(const storage::Tuple *tuple_key, const ItemPointer &item_pointer): item_pointer_(item_pointer) {
        key_.SetFromKey(key);
      }
      inline const KeyType &Key() const { return key_; }
      inline const ItemPointer &ItemPointer() const { return item_pointer_; }
    private:
      KeyType key_;
      const ItemPointer item_pointer_;
    };
  }
}