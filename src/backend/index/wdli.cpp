//
// Created by wendongli on 2/21/16.
//

#include "bwtree.h"
#include "pid_table.h"

#include <vector>
#include <utility>

namespace peloton {
  namespace index {

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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConstructConsolidatedInnerNodeInternal(
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
  };
}