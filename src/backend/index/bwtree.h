//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// BWTree.h
//
// Identification: src/backend/index/BWTree.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
#include <chrono>

namespace peloton {
namespace index {


#define EPOCH 10 // unit : ms
#define ROOT_PID 1 // root pid should be 1

typedef size_t size_type;
typedef size_t PID;
constexpr int max_chain_len = 8;
constexpr int max_node_size = 20;
constexpr int min_node_size = max_node_size << 1;

enum NodeType {
	NInsert,
	NDelete,
	NRemove,
	NMerge,
	NUpdate,
	NSplit,
	NInner,
	NLeaf,
	NSplitEntry,
	NMergeEntry
};

class BWNode {
  public:
    size_type slot_usage;
    size_type chain_length;

    virtual NodeType GetType() = 0;

};

template<typename KeyType>
class BWNormalNode : public BWNode {
  public:
    KeyType low_key, high_key;
    PID left, right;

    virtual NodeType GetType() = 0;
};

template<typename KeyType>
class BWInnerNode : public BWNormalNode<KeyType>{
  public:
    KeyType keys[max_node_size + max_chain_len];
    PID children[max_node_size + max_chain_len + 1];

    NodeType GetType() {
      return NInner;
    }
};

template<typename KeyType, typename ValueType>
class BWLeafNode : public BWNormalNode<KeyType> {
  public:
    KeyType keys[max_node_size + max_chain_len];
    ValueType values[max_node_size + max_chain_len];

    NodeType GetType() {
      return NLeaf;
    }

    void ScanKey(__attribute__((unused)) const KeyType& key, __attribute__((unused)) std::vector<ValueType>& ret) {

    }
};

// template<typename KeyType>
// Add your declarations here
class BWDeltaNode : public BWNode {
  public:
    BWNode *next;
    virtual NodeType GetType() = 0;
};

template<typename KeyType, typename ValueType>
class BWInsertNode : public BWDeltaNode {
  public:
    KeyType key;
    ValueType value;

    NodeType GetType() {
      return NInsert;
    }
};

template<typename KeyType>
class BWDeleteNode : public BWDeltaNode {
  public:
    KeyType key;

    NodeType GetType() {
      return NDelete;
    }
};

template<typename KeyType>
class BWSplitNode : public BWDeltaNode {
  public:
    KeyType key;
    PID right;

    NodeType GetType() {
      return NSplit;
    }

    KeyType& GetSplitKey() const {
        return key;
    }
};

template<typename KeyType>
class BWSplitEntryNode : public BWDeltaNode {
  public:
    KeyType low_key, high_key;
    PID to;

    NodeType GetType() {
      return NSplitEntry;
    }
};

template<typename KeyType>
class BWRemoveNode : public BWDeltaNode {

  NodeType GetType() {
    return NRemove;
  }
};

template<typename KeyType>
class BWMergeNode : public BWDeltaNode {
  public:
    BWNode *right;

    NodeType GetType() {
      return NMerge;
    }
};

template<typename KeyType>
class MergeEntryNode : public BWDeltaNode {
  NodeType GetType() {
    return NMergeEntry;
  }
};

// Look up the stx btree interface for background.
// peloton/third_party/stx/btree.h
template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
class BWTree {

  KeyComparator comparator;
  KeyEqualityChecker keyEqualityChecker;

  private:
  bool IfLeafNode(  __attribute__((unused)) NodeType type){
    return false;
  }

  void InsertSplitEntry(  __attribute__((unused)) PID parent,   __attribute__((unused)) const KeyType& low_key) {

  }

  bool IfChainFul(  __attribute__((unused)) int len) {
    return false;
  }

  bool Consolidate( __attribute__((unused)) PID cur,  __attribute__((unused)) BWNode *node_ptr) {
    //TODO: if fail to consolidate, need free memory.
    return false;
  }

  bool IfOverflow(__attribute__((unused)) int size) {
    return false;
  }

  bool Split(__attribute__((unused)) PID cur, __attribute__((unused)) BWNode *node_ptr, __attribute__((unused)) KeyType& split_key) {
    //TODO: if fail to split, need free memory.
    return false;
  }

  bool DupExist(__attribute__((unused)) BWNode *node_ptr, __attribute__((unused)) const KeyType& key) {
    return false;
  }

  bool DeltaInsert(__attribute__((unused)) PID cur, __attribute__((unused)) BWNode *node_ptr, __attribute__((unused)) const KeyType& key) {
    return false;
  }

    bool InsertEntryUtil(KeyType key, ValueType value, std::vector<PID>& path, PID cur) {
        // TODO: should read ptr from pid table.
        BWNode *node_ptr = NULL;

        if (IfLeafNode(node_ptr->GetType())) {
            while (true) {
                // TODO: always get node_ptr of PID cur from pid table here;
                node_ptr = NULL;
                // If current is split node
                if (node_ptr->GetType() == NSplit) {
                    BWSplitNode<KeyType> *split_ptr = static_cast<BWSplitNode<KeyType> *>(node_ptr);
                    InsertSplitEntry(path[path.size() - 1], split_ptr->key);
                }

                // If delta chain is too long
                if (IfChainFul(0)) {
                    BWNode *new_ptr = NULL;
                    bool ret = Consolidate(cur, node_ptr);

                    if(ret == false) {
                        // No matter if succeed, continue;
                    }
                    continue;
                }

                bool if_split = false;
                // TODO: should get size from node_ptr
                while (IfOverflow(0)) {
                    KeyType split_key;
                    bool ret = Split(cur, node_ptr, split_key);
                    if_split = (ret == true || if_split == true) ? true : false;
                    if (ret == false) {
                        continue;
                    } else {
                        // TODO: should keep spliting
                        InsertSplitEntry(path[path.size() - 1], split_key);
                        // TODO: update node_ptr to the current split level.
                        node_ptr = NULL;
                    }
                }

                if (if_split == true) {
                    continue;
                }

                if (!Duplicate && DupExist(node_ptr, key)) {
                    return false;
                }

                if (!DeltaInsert(cur, node_ptr, key)) {
                    continue;
                }

                break;
            }

            return true;
        } else {
            PID next_pid = 0;
            // PID next_pid = node_ptr->GetNextPID(key);
            path.push_back(cur);
            return InsertEntryUtil(key, value, path, next_pid);
        }
    }

    void ScanKeyUtil(PID cur, KeyType key, std::vector<ValueType>& ret) {
        //TODO: get node_ptr of cur.
        BWNode *node_ptr = NULL;

        if (IfLeafNode(node_ptr->GetType())) {
            unsigned short if_delete = 0;

            while (true) {
                // TODO: assume no renmoe node delta and merge node delta.
                // Only handle split node, insert node, delete node.
                if (node_ptr->GetType() == NSplit) {
                    BWSplitNode <KeyType> *split_ptr = static_cast<BWSplitNode <KeyType> *>(node_ptr);
                    KeyType split_key = split_ptr->GetSplitKey();
                    if (comparator(key, split_key) == true) { // == true means key < split_key
                        node_ptr = split_key->next;
                    }
                } else if(node_ptr->GetType() == NInsert) {
                    BWInsertNode<KeyType, ValueType> *insert_ptr = static_cast<BWInsertNode<KeyType, ValueType> *>(node_ptr);
                    if (insert_ptr->key == key && if_delete == 0) {
                        ret.push_back(insert_ptr->value);
                        if (!Duplicate) {
                            break;
                        }
                    } else if (insert_ptr->key == key && if_delete > 0) {
                        if_delete--;
                    }

                    node_ptr = insert_ptr->next;
                } else if(node_ptr->GetType() == NDelete) {
                    BWDeleteNode<KeyType> *delete_ptr = static_cast<BWDeleteNode<KeyType> *>(node_ptr);
                    if (delete_ptr->key != key ) {
                        if_delete++;
                        if (!Duplicate) {
                            break;
                        }
                    } else if (delete_ptr->key == key) {
                        if_delete++;
                    }

                    node_ptr = delete_ptr->next;
                } else if (node_ptr->GetType() == NLeaf) {
                    BWLeafNode<KeyType, ValueType> *leaf_ptr = static_cast<BWLeafNode<KeyType, ValueType> *>(node_ptr);
                    leaf_ptr->ScanKey(key, ret);
                    break;
                } else { // TODO: handle other cases here.
                    break;
                }
            }
        } else {
            PID next_pid = 0;
            // PID next_pid = node_ptr->GetNextPID(key);
            ScanKeyUtil(next_pid, key, ret);
        }

    }

public :
  BWTree(IndexMetadata * indexMetadata): comparator(indexMetadata), keyEqualityChecker(indexMetadata)	{

  }

  bool InsertEntry(KeyType key, ValueType value) {
    std::vector<PID> path;
    bool ret = InsertEntryUtil(key, value, path, ROOT_PID);
    return ret;
  }

  void ScanKey(KeyType key, std::vector<ValueType>& ret) {
      ScanKeyUtil(ROOT_PID, key, ret);
  }
};

}  // End index namespace
}  // End peloton namespace
