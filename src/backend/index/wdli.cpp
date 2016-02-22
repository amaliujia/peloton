//
// Created by wendongli on 2/21/16.
//

#include "bwtree.h"
#include "pid_table.h"

#include <vector>
#include <utility>

namespace peloton {
  namespace index {

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::Consolidate(PID cur,
                                                                                               BWNode *node_ptr) {
      //TODO: if fail to consolidate, need free memory.
      return false;
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    BWInnerNode *BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConstructConsolidatedInnerNode(
            BWNode *node_chain) {
      assert(node_chain!=NULL);
      std::vector<KeyType> keys;
      std::vector<PID> children;
      PID left, right;
      ConstructConsolidatedInnerNodeInternal(node_chain, keys, children, left, right);
      return new BWInnerNode(keys, children, left, right);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    BWLeafNode *BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConstructConsolidatedLeafNode(BWNode *node_chain) {
      std::vector<KeyType> keys;
      std::vector<ValueType> values;
      PID left, right;
      ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left, right);
      return new BWLeafNode(keys, values, left, right);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConstructConsolidatedLeafNodeInternal(
            BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values, PID &left, PID &right) {
      if(node_chain->GetType()==NLeaf) {
        // arrive at bottom
        BWLeafNode *node = static_cast<BWLeafNode *>(node_chain);
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
          ConsolidateInsertNode(static_cast<BWInsertNode *>(node_chain), keys, values);
          break;
        case NDelete:
          // not implemented
          assert(0);
          break;
        case NSplit:
          ConsolidateSplitNode(static_cast<BWSplitNode *>(node_chain), keys, values);
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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConstructConsolidatedInnerNodeInternal(
            BWNode *node_chain, std::vector<KeyType> &keys, std::vector<PID> &children, PID &left, PID &right) {
      if(node_chain->GetType()==NInner) {
        // arrive at bottom
        BWInnerNode *node = static_cast<BWInnerNode *>(node_chain);
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
          ConsolidateSplitNode(static_cast<BWSplitNode *>(node_chain), keys, children);
          break;
        case NSplitEntry:
          ConsolidateSplitEntryNode(static_cast<BWSplitEntryNode *>(node_chain), keys, children);
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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateInsertNode(BWInsertNode *node, std::vector<KeyType> &keys, std::vector<ValueType> &values) {
      assert(keys.size()==values.size());
      KeyType key = node->GetKey();
      ValueType value = node->GetType();
      auto first = std::lower_bound(keys.begin(), keys.end(), key, comparator);
      keys.insert(first, key);
      values.insert(first, value);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateSplitNode(BWSplitNode *node, std::vector<KeyType> &keys, std::vector<ValueType> &values) {
      assert(keys.size()==values.size());
      KeyType key = node->GetSplitKey();
      ValueType value = node->GetSplitValue();
      // TODO key belongs to left or right?
      auto first = std::lower_bound(keys.begin(), keys.end(), key, comparator);
      auto dist = std::distance(keys.begin(), first);
      for(auto i = dist-1; i<keys.size(); ++i) {
        if(!keyEqualityChecker(keys[i], key)) {
          // error
          assert(0);
        }
        if(valueEqualityChecker(values[i], value)) {
          keys.erase(keys.begin()+i, keys.end());
          values.erase(values.begin()+i, values.end());
          return ;
        }
      }
      // error
      assert(0);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateSplitNode(BWSplitNode *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      assert(keys.size()+1==values.size());
      KeyType key = node->GetSplitKey();
      PID split_child = node->GetSplitChild();
      // TODO key belongs to left or right?
      auto split_place = std::lower_bound(children.begin(), children.end(), split_child);
      assert(*split_place==split_child&&
                     (split_place+1==children.end()||*(split_place+1)!=split_child));
      auto dist = std::distance(keys.begin(), split_place);
      // TODO to finish
      // error
      assert(0);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateSplitNode(BWSplitNode *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      KeyType key = node->GetSplitKey();
      ValueType value = node->GetSplitValue();
      // TODO key belongs to left or right?
      auto first = std::lower_bound(keys.begin(), keys.end(), key, comparator);
      auto dist = std::distance(keys.begin(), first);
      for(auto i = dist-1; i<keys.size(); ++i) {
        if(!keyEqualityChecker(keys[i], key)) {
          // error
          assert(0);
        }
        if(valueEqualityChecker(values[i], value)) {
          keys.erase(keys.begin()+i, keys.end());
          values.erase(values.begin()+i, values.end());
          return ;
        }
      }
      // error
      assert(0);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateSplitEntryNode(BWSplitEntryNode *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      KeyType key = node->GetSplitKey();
      ValueType value = node->GetSplitValue();
      KeyType range_low;
      KeyType range_high;
      PID new_page = node->GetNextPID();
      // TODO key belongs to left or right?
      auto first = std::lower_bound(keys.begin(), keys.end(), range_low, comparator);
      auto dist = std::distance(keys.begin(), first);
      for(auto i = dist-1; i<keys.size(); ++i) {
        if(!keyEqualityChecker(keys[i], key)) {
          // error
          assert(0);
        }
        if(valueEqualityChecker(values[i], value)) {
          keys.erase(keys.begin()+i, keys.end());
          values.erase(values.begin()+i, values.end());
          return ;
        }
      }
      // error
      assert(0);
    }
  };
}