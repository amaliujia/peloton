//
// Created by wendongli on 2/21/16.
//

#include "bwtree.h"
#include "pid_table.h"

#include <vector>
#include <utility>
#include <limits>

namespace peloton {
  namespace index {
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    Consolidate(PID cur, const BWNode *node_ptr) {
      const BWNode *new_node = nullptr;
      if(node_ptr->IfLeafNode())
        new_node = ConstructConsolidatedLeafNode(node_ptr);
      else
        new_node = ConstructConsolidatedInnerNode(node_ptr);
      if(!pid_table_.bool_compare_and_swap(cur, node_ptr, new_node)) {
        delete new_node;
        return false;
      }
      return true;
    }

    /*
     * Create a consolidated inner node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedInnerNode(
            const BWNode *node_chain) {
      assert(node_chain!=NULL);
      std::vector<KeyType> keys;
      std::vector<PID> children;
      PID left, right;
      ConstructConsolidatedInnerNodeInternal(node_chain, keys, children, left, right);
      return new BWInnerNode<KeyType>(keys, children, left, right, node_chain->GetVersionNumber());
    }

    /*
     * Create a consolidated leaf node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedLeafNode(
            const BWNode *node_chain) {
      if(!Duplicate) {
        std::vector<KeyType> keys;
        std::vector<ValueType> values;
        PID left, right;
        ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left, right);
        return new BWLeafNode<KeyType, ValueType>(keys, values, left, right, node_chain->GetVersionNumber());
      }
      else {
        std::vector<KeyType> keys;
        std::vector<std::vector<ValueType>> values;
        PID left, right;
        ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left, right);
        return new BWLeafNode<KeyType, std::vector<ValueType>>(keys, values, left, right, node_chain->GetVersionNumber());
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    CreateLeafNodeView(const BWNode *node_chain,
                       std::vector<KeyType> &keys,
                       std::vector<ValueType> &values,
                       PID &left, PID &right) {
      assert(!Duplicate);
      ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left, right);
    };

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    CreateLeafNodeView(const BWNode *node_chain,
                       std::vector<KeyType> &keys,
                       std::vector<std::vector<ValueType>> &values,
                       PID &left, PID &right) {
      assert(Duplicate);
      ConstructConsolidatedLeafNodeInternal(node_chain, keys, values, left, right);
    };

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    CreateInnerNodeView(const BWNode *node_chain,
                        std::vector<KeyType> &keys,
                        std::vector<PID> &children,
                        PID &left, PID &right) {
      ConstructConsolidatedInnerNodeInternal(node_chain, keys, children, left, right);
    };

    /*
     * Construct vector keys and values from a leaf node linked list, along with its left and right siblings.
     * Collect everthing from node_chain and put it to (or remove it from) keys and values.
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedLeafNodeInternal(
            const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values, PID &left,
            PID &right) {
      assert(!Duplicate);
      assert(node_chain->IfLeafNode());
      if(node_chain->GetType()==NLeaf) {
        // arrive at bottom
        const BWLeafNode<KeyType, ValueType> *node = static_cast<const BWLeafNode<KeyType, ValueType> *>(node_chain);
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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedLeafNodeInternal(
            const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<std::vector<ValueType>> &values, PID &left,
            PID &right) {
      assert(Duplicate);
      assert(node_chain->IfLeafNode());
      if(node_chain->GetType()==NLeaf) {
        // arrive at bottom
        const BWLeafNode<KeyType, std::vector<ValueType>> *node = static_cast<const BWLeafNode<KeyType, std::vector<ValueType>> *>(node_chain);
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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedInnerNodeInternal(
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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateInsertNode(
            const BWInsertNode<KeyType, ValueType> *node,
            std::vector<KeyType> &keys,
            std::vector<ValueType> &values) {
      assert(!Duplicate);
      assert(keys.size()==values.size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetValue();
      auto position = std::upper_bound(keys.cbegin(), keys.cend(), key, comparator_);
      // check for non-existence
      assert(position==keys.cbegin()||!key_equality_checker_(key, *(position-1)));
      auto dist = std::distance(keys.cbegin(), position);
      keys.insert(position, key);
      values.insert(values.begin()+dist, value);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateInsertNode(
            const BWInsertNode<KeyType, ValueType> *node,
            std::vector<KeyType> &keys,
            std::vector<std::vector<ValueType>> &values) {
      assert(Duplicate);
      assert(keys.size()==values.size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetValue();
      auto position = std::upper_bound(keys.cbegin(), keys.cend(), key, comparator_);
      auto dist = std::distance(keys.cbegin(), position);
      if(position==keys.cbegin()||!key_equality_checker_(key, *(position-1))) {
        values[dist].push_back(value);
      }
      else {
        keys.insert(position, key);
        values.insert(values.begin()+dist, {value});
      }
    }


    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateDeleteNode(
            const BWDeleteNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values) {
      assert(!Duplicate);
      assert(keys.size()==values.size());
      const KeyType &key = node->GetKey();
      auto position = std::lower_bound(keys.cbegin(), keys.cend(), key, comparator_);
      // check for both existence and uniqueness
      assert(position!=keys.cend()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys.cend()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys.cbegin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys.cbegin(), position);
      assert(value_equality_checker_(values[dist], node->GetValue()));
      keys.erase(position);
      values.erase(values.cbegin()+dist);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateDeleteNode(
            const BWDeleteNode<KeyType> *node, std::vector<std::vector<KeyType>> &keys, std::vector<ValueType> &values) {
      assert(!Duplicate);
      assert(keys.size()==values.size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetValue();
      auto position = std::lower_bound(keys.cbegin(), keys.cend(), key, comparator_);
      // check for existence
      assert(position!=keys.cend()&&
             key_equality_checker_(key, *position));
      auto dist = std::distance(keys.cbegin(), position);
      std::vector<ValueType> &value_vector = values[dist];
      //TODO
      auto item = std::find(value_vector.crbegin(), value_vector.crend(), );
      assert(item!=value_vector.crend());
      value_vector.erase(item);
      if(value_vector.size()==0) {
        keys.erase(position);
        values.erase(values.cbegin()+dist);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateSplitNode(
            const BWSplitNode<KeyType> *node,
            std::vector<KeyType> &keys,
            std::vector<ValueType> &values,
            PID &right) {
      assert(!Duplicate);
      assert(node->IfLeafNode());
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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateSplitNode(
            const BWSplitNode<KeyType> *node,
            std::vector<KeyType> &keys,
            std::vector<std::vector<ValueType>> &values,
            PID &right) {
      assert(Duplicate);
      assert(node->IfLeafNode());
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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateSplitNode(
            const BWSplitNode<KeyType> *node,
            std::vector<KeyType> &keys,
            std::vector<PID> &children,
            PID &right) {
      assert(node->IfInnerNode());
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

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateSplitEntryNode(
            const BWSplitEntryNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<PID> &children) {
      assert(node->IfInnerNode());
      assert(keys.size()+1==children.size());
      const KeyType &low_key = node->GetLowKey();
      if(node->HasHighKey()) {
        const KeyType &high_key = node->GetHightKey();
        PID new_page = node->GetNextPID();
        auto position = std::lower_bound(keys.cbegin(), keys.cend(), high_key, comparator_);
        // check for both existence and uniqueness
        assert(position!=keys.cend()&&
               key_equality_checker_(high_key, *position)&&
               (position+1==keys.cend()||!key_equality_checker_(high_key, *(position+1)))&&
               (position==keys.cbegin()||!key_equality_checker_(high_key, *(position-1))));
        auto dist = std::distance(keys.cbegin(), position);
        keys.insert(position, low_key);
        children.insert(children.cbegin()+dist+1, new_page);
      }
      else {
        assert(keys.empty()||comparator_(keys.back(), low_key));
        PID new_page = node->GetNextPID();
        keys.push_back(low_key);
        children.push_back(new_page);
      }
    }
  };
}