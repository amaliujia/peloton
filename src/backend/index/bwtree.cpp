//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// bwtree.cpp
//
// Identification: src/backend/index/bwtree.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/index/bwtree.h"
#include "backend/common/types.h"
#include "backend/index/index_key.h"
#include "backend/storage/tuple.h"

namespace peloton {
  namespace index {

//    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
//    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::KeyComparator comparator_;
//    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
//    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::KeyEqualityChecker key_equality_checker_;

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    SplitInnerNodeUlti(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                       std::vector<KeyType> &keys,
                       std::vector<PID> &pids) {
      std::vector<KeyType> key_stack;
      std::vector<PID> pid_stack;

      const BWNode *cur_ptr = node_ptr;
      while(cur_ptr->GetType()!=NInner) {
        if(cur_ptr->GetType()==NSplitEntry) {
          const BWSplitEntryNode<KeyType> *split_entry_ptr = static_cast<const BWSplitEntryNode<KeyType> *>(cur_ptr);

          key_stack.push_back(split_entry_ptr->GetLowKey());
          pid_stack.push_back(split_entry_ptr->GetNextPID());
        }
        else {
          // TODO: handle MergeEntryNode and Split Node, if any
        }

        cur_ptr = cur_ptr->GetNext();
      }

      // come to inner node
      BWInnerNode<KeyType> *inner_ptr = static_cast<BWInnerNode<KeyType> *>(cur_ptr);
      keys = inner_ptr->GetKeys();
      pids = inner_ptr->GetPIDs();
      right_pid = inner_ptr->GetRight();

      while(!key_stack.empty()) {
        KeyType k = key_stack.back();
        PID p = pid_stack.back();
        key_stack.pop_back();
        pid_stack.pop_back();

        int pos = BinaryGreaterThan(keys, k, comparator_);
        keys.insert(keys.begin()+pos, k);
        pids.insert(pids.begin()+pos+1, k);
      }

      assert(keys.size()>0);

      split_key = keys[keys.size()/2];
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    SplitLeafNodeUtil(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                      std::vector<KeyType> &keys,
                      std::vector<ValueType> &values) {
      std::vector<OpType> op_stack;
      std::vector<KeyType> key_stack;
      std::vector<ValueType> value_stack;

      const BWNode *cur_ptr = node_ptr;
      // What kind of Delta?
      while(cur_ptr->GetType()!=NLeaf) {
        if(cur_ptr->GetType()==NInsert) {
          BWInsertNode<KeyType, ValueType> *insert_ptr = static_cast<BWInsertNode<KeyType, ValueType> *>(cur_ptr);

          op_stack.push_back(OInsert);
          key_stack.push_back(insert_ptr->GetKey());
          value_stack.push_back(insert_ptr->GetValue());
        }
        else if(cur_ptr->GetType()==NDelete) {
          BWDeleteNode<KeyType> *delete_ptr = static_cast<BWDeleteNode<KeyType> *>(cur_ptr);

          op_stack.push_back(ODelete);
          key_stack.push_back(delete_ptr->GetKey());
        }
        else {
          //TODO: how about next?
          continue;
        }
134gg
        cur_ptr = cur_ptr->GetNext();
      }

      std::map<KeyType, ValueType> key_value_map;
      BWLeafNode<KeyType, ValueType, KeyComparator> *leaf_ptr = static_cast<BWLeafNode<KeyType, ValueType, KeyComparator> *>(cur_ptr);

      right_pid = leaf_ptr->GetRight();

      std::vector<KeyType> leaf_keys = leaf_ptr->GetKeys();
      std::vector<ValueType> leaf_values = leaf_ptr->GetValues();

      for(size_t i = 0; i<leaf_keys.size(); i++) {
        key_value_map[leaf_keys[i]] = leaf_values[i];
      }

      while(op_stack.size()>0) {
        OpType op = op_stack.back();
        op_stack.pop_back();

        if(op==OInsert) {
          key_value_map[key_stack.back()] = value_stack.back();
          key_stack.pop_back();
          value_stack.pop_back();
        }
        else if(op==ODelete) {
          key_value_map.erase(key_value_map.find(key_stack.back()));
          key_stack.pop_back();
        }
        else {
          // TODO:: how about other type
        }
      }

      auto iter = key_value_map.begin();
      for(; iter!=key_value_map.end(); iter++) {
        keys.push_back(iter->first);
        values.push_back(iter->second);
      }

      split_key = keys[keys.size()/2];
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    SplitInnerNode(PID cur, const BWNode *node_ptr, KeyType &split_key, PID &right_pid) {
      std::vector<KeyType> keys;
      std::vector<PID> pids;
      PID old_right = PIDTable::PID_NULL;

      SplitLeafNodeUtil(node_ptr, split_key, old_right, keys, pids);

      // Step 1: Create new right page;
      PIDTable pidTable = PIDTable::get_table();

      BWNode *right_ptr = NULL;
      //TODO: check
      right_ptr = new BWInnerNode<KeyType>(keys, pids, cur, old_right);
      right_pid = pidTable.allocate_PID(right_ptr);

      // Step 2: Create split node
      BWNode *split_ptr = NULL;
      split_ptr = new BWSplitNode<KeyType>(node_ptr->GetSlotUsage()-keys.size(), node_ptr->GetChainLength()+1,
                                  node_ptr, split_key, right_pid, old_right);

      // Step 3: install split node
      bool ret = pidTable.bool_compare_and_swap(cur, node_ptr, split_ptr);
      if(ret==false) {
        delete right_ptr;
        delete split_ptr;
        pidTable.free_PID(right_pid);

        return false;
      }

      return true;
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    SplitLeafNode(PID cur, const BWNode *node_ptr, KeyType &split_key, PID &right_pid) {
      std::vector<KeyType> keys;
      std::vector<ValueType> values;
      PID old_right = PIDTable::PID_NULL;

      SplitLeafNodeUtil(node_ptr, split_key, old_right, keys, values);
      assert(keys.size()>=1);

      // Step 1: Create new right page;
      PIDTable pidTable = PIDTable::get_table();

      BWNode *right_ptr = NULL;
      right_ptr = new BWLeafNode<KeyType, ValueType, KeyComparator>(keys, values, cur, old_right);
      right_pid = pidTable.allocate_PID(right_ptr);

      // Step 2: Create split node
      BWNode *split_ptr = NULL;
      split_ptr = new BWSplitNode<KeyType>(node_ptr->GetSlotUsage()-keys.size(), node_ptr->GetChainLength()+1,
                                  node_ptr, split_key, right_pid, old_right);

      // Step 3: install split node
      bool ret = pidTable.bool_compare_and_swap(cur, node_ptr, split_ptr);
      if(ret==false) {
        delete right_ptr;
        delete split_ptr;
        pidTable.free_PID(right_pid);

        return false;
      }

      return true;
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    Split(PID cur, const BWNode *node_ptr, KeyType &split_key, PID &right_pid, NodeType right_type) {
      if(right_type==NLeaf) {
        return SplitLeafNode(cur, node_ptr, split_key, right_pid);
      }
      else {
        return SplitInnerNode(cur, node_ptr, split_key, right_pid);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    DupExist(const BWNode *node_ptr, const KeyType &key) {
      // assume node_ptr is the header of leaf node
      std::unordered_map<KeyType, int> map;
      int delta = 0;
      KeyEqualityChecker equality_checker;
      do {
        NodeType node_type = node_ptr->GetType();
        if(node_type==NLeaf) {
          BWLeafNode<KeyType, ValueType, KeyComparator> *leaf_node_ptr = static_cast<BWLeafNode<KeyType, ValueType, KeyComparator> *>(node_ptr);
          std::vector<KeyType> keys = leaf_node_ptr->GetKeys();
          for(auto iter = keys.begin(); iter!=keys.end(); ++iter) {
            if(equality_checker(*iter, key)&&(++delta)>0) {
              return true;
            }
          }
          return false;
        }
        else if(node_type==NInsert) {
          BWInsertNode *insert_node_ptr = static_cast<BWInsertNode<KeyType, ValueType> *>(node_ptr);
          if(equality_checker(insert_node_ptr->GetKey(), key)&&(++delta>0)) {
            return true;
          }
        }
        else if(node_type==NDelete) {
          BWDeleteNode *delete_node_ptr = static_cast<BWDeleteNode<KeyType> *>(node_ptr);
          if(equality_checker(delete_node_ptr->GetKey(), key)) {
            --delta;
          }
          //TODO: complete this
        }
        else if(node_type==NSplitEntry) {


//          } else if (node_type == NMergeEntry){

        }
        node_ptr = (static_cast<BWDeltaNode *>(node_ptr))->GetNext();
      } while(true);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    DeltaInsert(PID cur, BWNode *node_ptr, const KeyType &key, const ValueType &value) {
      PIDTable pidTable = PIDTable::get_table();
      BWInsertNode<KeyType, ValueType> *insert_node_ptr = new BWInsertNode<KeyType, ValueType>(
              node_ptr->GetSlotUsage()+1, node_ptr->GetChainLength()+1, node_ptr, key, value);
      if(pidTable.bool_compare_and_swap(cur, node_ptr, insert_node_ptr))
        return true;
      else
        delete insert_node_ptr;
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    InsertEntryUtil(KeyType key, ValueType value, std::vector<PID> &path, PID cur) {

      BWNode *node_ptr = NULL;
      while(true) {
        // TODO: Get from last element of path.
        node_ptr = NULL;
        // If current is split node
        if(node_ptr->GetType()==NSplit) {
          BWSplitNode<KeyType> *split_ptr = static_cast<BWSplitNode<KeyType> *>(node_ptr);
          InsertSplitEntry(path, split_ptr->GetSplitKey(), split_ptr->GetRightPID());
          //continue;
        }

        // If delta chain is too long
        if(node_ptr->IfChainTooLong()) {
          Consolidate(cur, node_ptr);
          path.pop_back();
          continue;
        }

        // TODO: should get size from node_ptr
        if(node_ptr->IfOverflow()) {
          KeyType split_key;
          PID right_pid;
          NodeType right_type = Unknown;
          if(node_ptr->IfLeafNode()) {
            right_type = NLeaf;
          }
          else {
            right_type = NInner;
          }
          bool ret = Split(cur, node_ptr, split_key, right_pid, right_type);
          if(ret==true) {
            // note: only do two step.
            InsertSplitEntry(path, split_key, right_pid);
          }
          // retry
          path.pop_back();
          continue;
        }

        if(node_ptr->IfLeafNode()) {
          // TODO: check
          if(DupExist(node_ptr, key)) {
            return false;
          }

          if(!DeltaInsert(cur, node_ptr, key, value)) {
            continue;
          }

          return true;
        }
        else {
          PID next_pid = GetNextPID(path.back(), key);
          path.push_back(cur);
        }
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    ScanKeyUtil(PID cur, KeyType key, std::vector<ValueType> &ret) {
      PIDTable pidTable = PIDTable::get_table();
      const BWNode *node_ptr = pidTable.get(cur);

      if(node_ptr->GetType()==NLeaf) {
        unsigned short delete_count = 0;
        while(true) {
          // TODO: assume no renmoe node delta and merge node delta.
          // Only handle split node, insert node, delete node.
          if(node_ptr->GetType()==NSplit) {
            BWSplitNode<KeyType> *split_ptr = static_cast<BWSplitNode<KeyType> *>(node_ptr);
            KeyType split_key = split_ptr->GetSplitKey();
            if(comparator_(key, split_key)==true) { // == true means key < split_key
              node_ptr = split_ptr->GetNext();
            }
            else {
              node_ptr = pidTable.get(split_key->GetRightPID());
            }
          }
          else if(node_ptr->GetType()==NInsert) {
            BWInsertNode<KeyType, ValueType> *insert_ptr = static_cast<BWInsertNode<KeyType, ValueType> *>(node_ptr);
            if(key_equality_checker_(insert_ptr->GetKey(), key)==true&&delete_count==0) {
              ret.push_back(insert_ptr->GetKey());
//                if(!Duplicate) {
//                  break;
//                }
            }
            else if(key_equality_checker_(insert_ptr->GetKey(), key)==true&&delete_count>0) {
              delete_count--;
            }

            node_ptr = insert_ptr->GetNext();
          }
          else if(node_ptr->GetType()==NDelete) {
            BWDeleteNode<KeyType> *delete_ptr = static_cast<BWDeleteNode<KeyType> *>(node_ptr);
            if(key_equality_checker_(delete_ptr->GetKey(), key)==false) {
              delete_count++;
//                if(!Duplicate) {
//                  break;
//                }
            }
            else if(key_equality_checker_(delete_ptr->GetKey(), key)==true) {
              delete_count++;
            }

            node_ptr = delete_ptr->GetNext();
          }
          else if(node_ptr->GetType()==NLeaf) {
            BWLeafNode<KeyType, ValueType> *leaf_ptr = static_cast<BWLeafNode<KeyType, ValueType> *>(node_ptr);
            // TOOD: assume no scan key here.
            leaf_ptr->ScanKey(key, comparator_, ret);
            break;
          }
          else { // TODO: handle other cases here.
            break;
          }
        }
      }
      else {
        PID next_pid = GetNextPID(cur, key);
        ScanKeyUtil(next_pid, key, ret);
      }
    }
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Consolidate(PID cur, const BWNode *node_ptr) {
      //TODO: if fail to consolidate, need free memory.
      return false;
    }

    /*
     * Create a consolidated inner node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    const BWInnerNode *BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConstructConsolidatedInnerNode(
      const BWNode *node_chain) {
      assert(node_chain!=NULL);
      std::vector<KeyType> keys;
      std::vector<PID> children;
      PID left, right;
      ConstructConsolidatedInnerNodeInternal(node_chain, keys, children, left, right);
      return new BWInnerNode(keys, children, left, right);
    }

    /*
     * Create a consolidated leaf node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    const BWLeafNode *BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConstructConsolidatedLeafNode(const BWNode *node_chain) {
      std::vector<KeyType> keys;
      std::vector<ValueType> values;
      PID left, right;
      ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left, right);
      return new BWLeafNode(keys, values, left, right);
    }

    /*
     * Construct vector keys and values from a leaf node linked list, along with its left and right siblings.
     * Collect everthing from node_chain and put it to (or remove it from) keys and values.
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConstructConsolidatedLeafNodeInternal(
      const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values, PID &left, PID &right) {
      if(node_chain->GetType()==NLeaf) {
        // arrive at bottom
        const BWLeafNode *node = static_cast<const BWLeafNode<KeyType, ValueType, KeyComparator> *>(node_chain);
        keys = node->GetKeys();
        values = node->GetValues();
        left = node->GetLeft();
        right = node->GetRight();
        return ;
      }

      // still at delta chain
      assert(node_chain->GetNext()!=NULL);
      ConstructConsolidatedLeafNodeInternal(node_chain->GetNext(), keys, values, left, right);
      switch(node_chain->GetType()) {
        case NInsert:
          ConsolidateInsertNode(static_cast<const BWInsertNode<KeyType, ValueType> *>(node_chain), keys, values);
          break;
        case NDelete:
          // not implemented
          assert(0);
          break;
        case NSplit:
          ConsolidateSplitNode(static_cast<const BWSplitNode<KeyType> *>(node_chain), keys, values);
          break;
        case NMerge:
          assert(0);
          break;
        case NRemove:
          assert(0);
          break;
        default:
          // fault
          assert(0);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConstructConsolidatedInnerNodeInternal(
      const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<PID> &children, PID &left, PID &right) {
      if(node_chain->GetType()==NInner) {
        // arrive at bottom
        const BWInnerNode *node = static_cast<const BWInnerNode<KeyType> *>(node_chain);
        keys = node->GetKeys();
        children = node->GetChildren();
        left = node->GetLeft();
        right = node->GetRight();
        return ;
      }

      // still at delta chain
      assert(node_chain->GetNext()!=NULL);
      ConstructConsolidatedLeafNodeInternal(node_chain->GetNext(), keys, children, left, right);
      switch(node_chain->GetType()) {
        case NSplit:
          ConsolidateSplitNode(static_cast<const BWSplitNode<KeyType> *>(node_chain), keys, children);
          break;
        case NSplitEntry:
          ConsolidateSplitEntryNode(static_cast<const BWSplitEntryNode<KeyType> *>(node_chain), keys, children);
          break;
        case NMerge:
          // not implemented
          assert(0);
          break;
        case NRemove:
          assert(0);
          break;
        default:
          // fault
          assert(0);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateInsertNode(const BWInsertNode *node, std::vector<KeyType> &keys, std::vector<ValueType> &values) {
      assert(keys.size()==values.size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetType();
      auto first = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
      keys.insert(first, key);
      values.insert(first, value);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateSplitNode(const BWSplitNode *node, std::vector<KeyType> &keys, std::vector<ValueType> &values) {
      assert(keys.size()==values.size());
      const KeyType &key = node->GetSplitKey();
      // TODO key belongs to left or right?
      auto position = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
      assert(position!=keys.end()&&key_equality_checker_(*position, key));
      auto dist = std::distance(keys.begin(), position);
      keys.erase(keys.begin()+dist, keys.end());
      values.erase(values.begin()+dist, values.end());
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateSplitNode(const BWSplitNode *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      assert(keys.size()+1==children.size());
      const KeyType &key = node->GetSplitKey();
      // TODO key belongs to left or right?
      auto position = std::lower_bound(keys.begin(), keys.end(), key);
      assert(position!=keys.end()&&key_equality_checker_(*position, key));
      auto dist = std::distance(keys.begin(), position);
      keys.erase(keys.begin()+dist-1, keys.end());
      children.erase(children.begin()+dist, children.end());
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateSplitEntryNode(const BWSplitEntryNode *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      const KeyType &low_key = node->GetLowKey();
      const KeyType &high_key = node->GetHightKey();
      PID new_page = node->GetNextPID();
      // TODO key belongs to left or right?
      auto position = std::lower_bound(keys.begin(), keys.end(), high_key, comparator_);
      assert(position!=keys.begin()&&key_equality_checker_(*position, high_key));
      auto dist = std::distance(keys.begin(), position);
      keys.insert(position, low_key);
      children.insert(children.begin()+dist+1, new_page);
    }

// Explicit template instantiations

    template
    class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
            IntsEqualityChecker<1>>;

    template
    class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
            IntsEqualityChecker<2>>;

    template
    class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
            IntsEqualityChecker<3>>;

    template
    class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
            IntsEqualityChecker<4>>;

    template
    class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
            GenericEqualityChecker<4>>;

    template
    class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
            GenericEqualityChecker<8>>;

    template
    class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
            GenericEqualityChecker<12>>;

    template
    class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
            GenericEqualityChecker<16>>;

    template
    class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
            GenericEqualityChecker<24>>;

    template
    class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
            GenericEqualityChecker<32>>;

    template
    class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
            GenericEqualityChecker<48>>;

    template
    class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
            GenericEqualityChecker<64>>;

    template
    class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
            GenericEqualityChecker<96>>;

    template
    class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
            GenericEqualityChecker<128>>;

    template
    class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
            GenericEqualityChecker<256>>;

    template
    class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
            GenericEqualityChecker<512>>;

    template
    class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
            TupleKeyEqualityChecker>;


    PIDTable PIDTable::global_table_;

  }  // End index namespace
}  // End peloton namespace