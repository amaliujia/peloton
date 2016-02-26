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
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::Consolidate(PID cur __attribute__ ((unused)),
                                                                                    const BWNode *node_ptr __attribute__ ((unused))) {
      //TODO: if fail to consolidate, need free memory.
      return false;
    }

    /*
     * Create a consolidated inner node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    const BWInnerNode<KeyType> *BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConstructConsolidatedInnerNode(
            const BWNode *node_chain) {
      assert(node_chain!=NULL);
      std::vector<KeyType> keys;
      std::vector<PID> children;
      PID left, right;
      ConstructConsolidatedInnerNodeInternal(node_chain, keys, children, left, right);
      return new BWInnerNode<KeyType>(keys, children, left, right);
    }

    /*
     * Create a consolidated leaf node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    const BWLeafNode<KeyType, ValueType, KeyComparator> *BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConstructConsolidatedLeafNode(
            const BWNode *node_chain) {
      std::vector<KeyType> keys;
      std::vector<ValueType> values;
      PID left, right;
      ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left, right);
      return new BWLeafNode<KeyType, ValueType, KeyComparator>(keys, values, left, right);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::CreateLeafNodeView(const BWNode *node_chain,
                                                                                           std::vector<KeyType> &keys,
                                                                                           std::vector<ValueType> &values,
                                                                                           PID &left, PID &right) {
      ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left, right);
    };

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::CreateInnerNodeView(const BWNode *node_chain,
                                                                                            std::vector<KeyType> &keys,
                                                                                            std::vector<PID> &children,
                                                                                            PID &left, PID &right) {
      ConstructConsolidatedInnerNodeInternal(node_chain, keys, children, left, right);
    };

    /*
     * Construct vector keys and values from a leaf node linked list, along with its left and right siblings.
     * Collect everthing from node_chain and put it to (or remove it from) keys and values.
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConstructConsolidatedLeafNodeInternal(
            const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values, PID &left,
            PID &right) {
      assert(node_chain->IfLeafNode());
      if(node_chain->GetType()==NLeaf) {
        // arrive at bottom
        const BWLeafNode<KeyType, ValueType, KeyComparator> *node = static_cast<const BWLeafNode<KeyType, ValueType, KeyComparator> *>(node_chain);
        keys = node->GetKeys();
        values = node->GetValues();
        left = node->GetLeft();
        right = node->GetRight();
        return;
      }

      // still at delta chain
      assert(node_chain->GetNext()!=NULL);
      ConstructConsolidatedLeafNodeInternal(node_chain->GetNext(), keys, values, left, right);
      switch(node_chain->GetType()) {
        case NInsert:
          ConsolidateInsertNode(static_cast<const BWInsertNode<KeyType, ValueType> *>(node_chain), keys, values);
          break;
        case NDelete:
          ConsolidateDeleteNode(static_cast<const BWDeleteNode<KeyType> *>(node_chain), keys, values);
          break;
        case NSplit:
          ConsolidateSplitNode(static_cast<const BWSplitNode<KeyType> *>(node_chain), keys, values, right);
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
      assert(node_chain->IfInnerNode());
      if(node_chain->GetType()==NInner) {
        // arrive at bottom
        const BWInnerNode<KeyType> *node = static_cast<const BWInnerNode<KeyType> *>(node_chain);
        keys = node->GetKeys();
        children = node->GetChildren();
        left = node->GetLeft();
        right = node->GetRight();
        return;
      }

      // still at delta chain
      assert(node_chain->GetNext()!=NULL);
      ConstructConsolidatedInnerNodeInternal(node_chain->GetNext(), keys, children, left, right);
      switch(node_chain->GetType()) {
        case NSplit:
          ConsolidateSplitNode(static_cast<const BWSplitNode<KeyType> *>(node_chain), keys, children, right);
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
        case NMergeEntry:
          assert(0);
          break;
        default:
          // fault
          assert(0);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateInsertNode(
            const BWInsertNode<KeyType, ValueType> *node,
            std::vector<KeyType> &keys,
            std::vector<ValueType> &values) {
      assert(keys.size()==values.size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetValue();
      auto position = std::lower_bound(keys.cbegin(), keys.cend(), key, comparator_);
      // check for both existence and uniqueness
      assert(position!=keys.cend()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys.cend()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys.cbegin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys.cbegin(), position);

      keys.insert(position, key);
      values.insert(values.begin()+dist, value);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateDeleteNode(
            const BWDeleteNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values) {
      assert(keys.size()==values.size());
      const KeyType &key = node->GetKey();
      auto position = std::lower_bound(keys.cbegin(), keys.cend(), key, comparator_);
      // check for both existence and uniqueness
      assert(position!=keys.cend()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys.cend()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys.cbegin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys.cbegin(), position);

      keys.erase(position);
      values.erase(values.cbegin()+dist);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateSplitNode(
            const BWSplitNode<KeyType> *node,
            std::vector<KeyType> &keys,
            std::vector<ValueType> &values,
            PID &right) {
      assert(keys.size()==values.size());
      const KeyType &key = node->GetSplitKey();
      auto position = std::lower_bound(keys.cbegin(), keys.cend(), key, comparator_);
      // check for both existence and uniqueness
      assert(position!=keys.cend()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys.cend()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys.cbegin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys.cbegin(), position);
      keys.erase(position, keys.cend());
      values.erase(values.cbegin()+dist, values.cend());
      right = node->GetRightPID();
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateSplitNode(
            const BWSplitNode<KeyType> *node,
            std::vector<KeyType> &keys,
            std::vector<PID> &children,
            PID &right) {
      assert(keys.size()+1==children.size());
      const KeyType &key = node->GetSplitKey();
      auto position = std::lower_bound(keys.cbegin(), keys.cend(), key, comparator_);
      // check for both existence and uniqueness
      assert(position!=keys.cbegin()&&
             position!=keys.cend()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys.cend()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys.cbegin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys.cbegin(), position);
      keys.erase(position-1, keys.cend());
      children.erase(children.cbegin()+dist, children.cend());
      right = node->GetRightPID();
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::ConsolidateSplitEntryNode(
            const BWSplitEntryNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      assert(keys.size()+1==children.size());
      const KeyType &low_key = node->GetLowKey();
      // TODO handle cases where no high_key exists
      const KeyType &high_key = node->GetHightKey();
      PID new_page = node->GetNextPID();
      auto position = std::lower_bound(keys.cbegin(), keys.cend(), high_key, comparator_);
      // check for both existence and uniqueness
      // TODO assertion only valid when there is at least one key in keys, the same as we have high key
      assert(position==keys.cend()&&
             key_equality_checker_(high_key, *position)&&
             (position+1==keys.cend()||!key_equality_checker_(high_key, *(position+1)))&&
             (position==keys.cbegin()||!key_equality_checker_(high_key, *(position-1))));
      auto dist = std::distance(keys.cbegin(), position);
      keys.insert(position, low_key);
      children.insert(children.cbegin()+dist+1, new_page);
    }
  };
}