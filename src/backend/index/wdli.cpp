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
                                                                                               const BWNode *node_ptr) {
      //TODO: if fail to consolidate, need free memory.
      return false;
    }

    /*
     * Create a consolidated inner node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    const BWInnerNode *BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConstructConsolidatedInnerNode(
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
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    const BWLeafNode *BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConstructConsolidatedLeafNode(const BWNode *node_chain) {
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
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConstructConsolidatedLeafNodeInternal(
            const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values, PID &left, PID &right) {
      if(node_chain->GetType()==NLeaf) {
        // arrive at bottom
        const BWLeafNode *node = static_cast<const BWLeafNode *>(node_chain);
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
          ConsolidateInsertNode(static_cast<const BWInsertNode *>(node_chain), keys, values);
          break;
        case NDelete:
          // not implemented
          assert(0);
          break;
        case NSplit:
          ConsolidateSplitNode(static_cast<const BWSplitNode *>(node_chain), keys, values);
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
            const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<PID> &children, PID &left, PID &right) {
      if(node_chain->GetType()==NInner) {
        // arrive at bottom
        const BWInnerNode *node = static_cast<const BWInnerNode *>(node_chain);
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
          ConsolidateSplitNode(static_cast<const BWSplitNode *>(node_chain), keys, children);
          break;
        case NSplitEntry:
          ConsolidateSplitEntryNode(static_cast<const BWSplitEntryNode *>(node_chain), keys, children);
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
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateInsertNode(const BWInsertNode *node, std::vector<KeyType> &keys, std::vector<ValueType> &values) {
      assert(keys.size()==values.size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetType();
      auto first = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
      keys.insert(first, key);
      values.insert(first, value);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateSplitNode(const BWSplitNode *node, std::vector<KeyType> &keys, std::vector<ValueType> &values) {
      assert(keys.size()==values.size());
      const KeyType &key = node->GetSplitKey();
      // TODO key belongs to left or right?
      auto position = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
      assert(position!=keys.end()&&key_equality_checker_(*position, key));
      auto dist = std::distance(keys.begin(), position);
      keys.erase(keys.begin()+dist, keys.end());
      values.erase(values.begin()+dist, values.end());
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, bool Duplicate>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateSplitNode(const BWSplitNode *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      assert(keys.size()+1==children.size());
      const KeyType &key = node->GetSplitKey();
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
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateSplitNode(const BWSplitNode *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      KeyType key = node->GetSplitKey();
      // TODO key belongs to left or right?
      auto first = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
      auto dist = std::distance(keys.begin(), first);
      for(auto i = dist-1; i<keys.size(); ++i) {
        if(!key_equality_checker_(keys[i], key)) {
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
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, Duplicate>::ConsolidateSplitEntryNode(const BWSplitEntryNode *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      KeyType key = node->GetSplitKey();
      ValueType value = node->GetSplitValue();
      KeyType range_low;
      KeyType range_high;
      PID new_page = node->GetNextPID();
      // TODO key belongs to left or right?
      auto first = std::lower_bound(keys.begin(), keys.end(), range_low, comparator_);
      auto dist = std::distance(keys.begin(), first);
      for(auto i = dist-1; i<keys.size(); ++i) {
        if(!key_equality_checker_(keys[i], key)) {
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