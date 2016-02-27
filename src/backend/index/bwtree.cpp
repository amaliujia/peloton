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
#include "backend/index/pid_table.h"
#include "bwtree.h"

namespace peloton {
  namespace index {

//    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
//    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::KeyComparator comparator_;
//    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
//    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::KeyEqualityChecker key_equality_checker_;
/*
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    SplitInnerNodeUtil(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
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
      const BWInnerNode<KeyType> *inner_ptr = static_cast<const BWInnerNode<KeyType> *>(cur_ptr);
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
        pids.insert(pids.begin()+pos+1, p);
      }

      assert(keys.size()>0);

      split_key = keys[keys.size()/2];
    }
*/
    /*
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker>
    void BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker>::
    SplitLeafNodeUtil(const BWNode *, KeyType &, PID &,
                      std::vector<KeyType> &,
                      std::vector<ValueType> &) {

      // TODO this function cannot pass compiler, fix it later.
      SplitLeafNodeUtil(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
        std::vector<KeyType> &keys,
        std::vector<ValueType> &values) {
      return;

      std::vector<OpType> op_stack;
      std::vector<KeyType> key_stack;
      std::vector<ValueType> value_stack;

      const BWNode *cur_ptr = node_ptr;
      // What kind of Delta?
      while(cur_ptr->GetType()!=NLeaf) {
        if(cur_ptr->GetType()==NInsert) {
          const BWInsertNode<KeyType, ValueType> *insert_ptr = static_cast<const BWInsertNode<KeyType, ValueType> *>(cur_ptr);

          op_stack.push_back(OInsert);
          key_stack.push_back(insert_ptr->GetKey());
          value_stack.push_back(insert_ptr->GetValue());
        }
        else if(cur_ptr->GetType()==NDelete) {
          const BWDeleteNode<KeyType> *delete_ptr = static_cast<const BWDeleteNode<KeyType> *>(cur_ptr);

          op_stack.push_back(ODelete);
          key_stack.push_back(delete_ptr->GetKey());
        }
        else {
          //TODO: how about next?
          continue;
        }
        cur_ptr = cur_ptr->GetNext();
      }

      std::map<KeyType, ValueType> key_value_map;
      const BWLeafNode<KeyType, ValueType, KeyComparator> *leaf_ptr = static_cast<const BWLeafNode<KeyType, ValueType, KeyComparator> *>(cur_ptr);

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
    */

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    InsertSplitEntry(const BWNode *top, const KeyType &key, std::vector<PID> &path, std::vector<VersionNumber> &version_number) {
      assert(path.size()==version_number.size());
      assert(top->GetType()==NSplit);
      const BWSplitNode<KeyType> *split_ptr = static_cast<const BWSplitNode<KeyType> *>(top);
      const KeyType &low_key = split_ptr->GetSplitKey();
      const PID &right_pid = split_ptr->GetRightPID();

      if(path.size()>1) { // In this case, this is a normal second step split
        // get the parent
        PID pid = path[path.size()-2];
        while(true) {
          // irst check if it is safe to do so
          PID save_pid = path.back();
          path.pop_back();
          VersionNumber save_vn = version_number.back();
          version_number.pop_back();
          const BWNode *node = pid_table_.get(pid);
          if(!CheckStatus(node, key, path, version_number)) {
            path.push_back(save_pid);
            version_number.push_back(save_vn);
            return false;
          }

          // then check version number
          assert(node->IfInnerNode());
          if(node->GetVersionNumber()!=version_number[version_number.size()-2])
            return false;

          std::vector<KeyType> keys_view;
          std::vector<PID> children_view;
          PID left_view, right_view;
          CreateInnerNodeView(node, keys_view, children_view, left_view, right_view);
          auto position = std::upper_bound(keys_view.cbegin(), keys_view.cend(), low_key, comparator_);

          // then check if this split entry has been inserted by others
          if(position!=keys_view.cbegin()&&key_equality_checker_(low_key, *(position-1)))
            return true;

          // try to insert the split entry
          const BWNode *split_entry;
          if(position!=keys_view.cend())
            split_entry = new BWSplitEntryNode<KeyType>(node, low_key, *position, right_pid);
          else
            split_entry = new BWSplitEntryNode<KeyType>(node, low_key, right_pid);
          if(pid_table_.bool_compare_and_swap(pid, node, split_entry))
            return true;

          // fail, clean up, retry
          delete split_entry;
        }
      }
      else {
        assert(path.back()==root_);
        const BWNode *old_root = pid_table_.get(root_);
        VersionNumber old_root_version = old_root->GetVersionNumber();
        PID new_pid = pid_table_.allocate_PID(old_root);
        BWNode *new_root = new BWInnerNode<KeyType>({low_key}, {new_pid, right_pid}, nullptr, nullptr, old_root_version+1);
        if(!pid_table_.bool_compare_and_swap(root_, old_root, new_root)) {
          delete new_root;
          pid_table_.free_PID(new_pid);
          return false;
        }
        path.push_back(new_pid);
        version_number.insert(version_number.cbegin(), (VersionNumber)(old_root_version+1));
        return true;
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    SplitInnerNode(const BWNode *inner_node, const PID &node_pid) {
      assert(inner_node->IfInnerNode());
      std::vector<KeyType> keys_view;
      std::vector<PID> children_view;
      PID left_view, right_view;
      CreateInnerNodeView(inner_node, keys_view, children_view, left_view, right_view);
      assert(keys_view.size()+1==children_view.size());
      assert(keys_view.size()>=max_node_size);

      auto index = keys_view.size()/2;
      const KeyType &low_key = keys_view[index];
      const BWNode *new_node = new BWInnerNode<KeyType>(
              std::vector<KeyType>(keys_view.cbegin()+index, keys_view.cend()),
              std::vector<PID>(children_view.cbegin()+index, children_view.cend()),
              node_pid,
              inner_node->GetRight(),
              std::numeric_limits<VersionNumber>::min()
              );
      PID new_pid = pid_table_.allocate_PID(new_node);
      //TODO slot usage, keys or children
      const BWNode *split_node = new BWSplitNode<KeyType>(index-1, inner_node, low_key, new_pid);

      if(!pid_table_.bool_compare_and_swap(node_pid, inner_node, split_node)) {
        delete new_node;
        delete split_node;
        pid_table_.free_PID(new_pid);
        return nullptr;
      }

      return split_node;
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    SplitLeafNode(const BWNode *leaf_node, const PID &node_pid) {
      assert(leaf_node->IfLeafNode());
      if(!Duplicate) {
        std::vector<KeyType> keys_view;
        std::vector<ValueType> values_view;
        PID left_view, right_view;
        CreateLeafNodeView(leaf_node, keys_view, values_view, left_view, right_view);
        assert(keys_view.size()==values_view.size());
        assert(keys_view.size()>=max_node_size);

        auto index = keys_view.size()/2;
        const KeyType &low_key = keys_view[index];
        const BWNode *new_node = new BWLeafNode<KeyType, ValueType>(
                std::vector<KeyType>(keys_view.cbegin()+index, keys_view.cend()),
                std::vector<ValueType>(values_view.cbegin()+index, values_view.cend()),
                node_pid,
                leaf_node->GetRight()
        );
        PID new_pid = pid_table_.allocate_PID(new_node);
        const BWNode *split_node = new BWSplitNode<KeyType>(index, leaf_node, low_key, new_pid);

        if(!pid_table_.bool_compare_and_swap(node_pid, leaf_node, split_node)) {
          delete new_node;
          delete split_node;
          pid_table_.free_PID(new_pid);
          return nullptr;
        }
        return split_node;
      }
      else {
        std::vector<KeyType> keys_view;
        std::vector<std::vector<ValueType>> values_view;
        PID left_view, right_view;
        CreateLeafNodeView(leaf_node, keys_view, values_view, left_view, right_view);
        assert(keys_view.size()==values_view.size());
        assert(keys_view.size()>=max_node_size);

        auto index = keys_view.size()/2;
        const KeyType &low_key = keys_view[index];
        const BWNode *new_node = new BWLeafNode<KeyType, std::vector<ValueType>>(
                std::vector<KeyType>(keys_view.cbegin()+index, keys_view.cend()),
                std::vector<std::vector<ValueType>>(values_view.cbegin()+index, values_view.cend()),
                node_pid,
                leaf_node->GetRight()
        );
        PID new_pid = pid_table_.allocate_PID(new_node);
        const BWNode *split_node = new BWSplitNode<KeyType>(index, leaf_node, low_key, new_pid);

        if(!pid_table_.bool_compare_and_swap(node_pid, leaf_node, split_node)) {
          delete new_node;
          delete split_node;
          pid_table_.free_PID(new_pid);
          return nullptr;
        }
        return split_node;
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    Split(const BWNode *node_ptr, const PID &node_pid) {
      switch(node_ptr->GetType()) {
        case NLeaf:
          return SplitLeafNode(node_ptr, node_pid);
        case NInner:
          return SplitInnerNode(node_ptr, node_pid);
        default:
          assert(0);
          return nullptr;
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    CheckStatus(const BWNode *node_ptr, const KeyType &key, std::vector<PID> &path, std::vector<VersionNumber> &version_number) {
      const PID &current = path.back();
      if(node_ptr->GetType()==NSplit) {
        // first try to help finishing the split
        bool result = InsertSplitEntry(node_ptr, key, path, version_number);
        assert(path.size()==version_number.size());
        // if split results in invalid path, go back
        if(!result) {
          if(path.size()>1) {
            path.pop_back();
            version_number.pop_back();
          }
          return false;
        }
        const BWSplitNode<KeyType> *split_ptr = static_cast<const BWSplitNode<KeyType> *>(node_ptr);
        // if split succeeded, but we are on the wrong path
        if(!comparator_(key, split_ptr->GetSplitKey())) {
          if(path.size()>1) {
            path.pop_back();
            version_number.pop_back();
          }
          return false;
        }
      }

      // If delta chain is too long
      if(node_ptr->IfChainTooLong()) {
        Consolidate(current, node_ptr);
        //TODO no need to pop back?
        //path.pop_back();
        return false;
      }

      // if the node is too big
      if(node_ptr->IfOverflow()) {
        // first try the first step of split
        node_ptr = Split(node_ptr, current);
        if(node_ptr==nullptr)
          return false;
        // then try the second step of split
        bool result = InsertSplitEntry(node_ptr, key, path, version_number);
        assert(path.size()==version_number.size());
        if(!result) {
          if(path.size()>1) {
            path.pop_back();
            version_number.pop_back();
          }
          return false;
        }
        const BWSplitNode<KeyType> *split_ptr = static_cast<const BWSplitNode<KeyType> *>(node_ptr);
        if(!comparator_(key, split_ptr->GetSplitKey())) {
          if(path.size()>1) {
            path.pop_back();
            version_number.pop_back();
          }
          return false;
        }
      }

      return true;
    };

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ExistKey(const BWNode *node_ptr, const KeyType &key) {
      // assume node_ptr is the header of leaf node
      //std::unordered_map<KeyType, int> map;
      int delta = 0;
      //KeyEqualityChecker equality_checker;
      do {
        NodeType node_type = node_ptr->GetType();
        if(node_type==NLeaf) {
          const BWLeafNode<KeyType, ValueType, KeyComparator> *leaf_node_ptr = static_cast<const BWLeafNode<KeyType, ValueType, KeyComparator> *>(node_ptr);
          std::vector<KeyType> keys = leaf_node_ptr->GetKeys();
          for(auto iter = keys.begin(); iter!=keys.end(); ++iter) {
            if(key_equality_checker_(*iter, key)&&(++delta)>0) {
              return true;
            }
          }
          return false;
        }
        else if(node_type==NInsert) {
          const BWInsertNode<KeyType, ValueType> *insert_node_ptr = static_cast<const BWInsertNode<KeyType, ValueType> *>(node_ptr);
          if(key_equality_checker_(insert_node_ptr->GetKey(), key)&&(++delta>0)) {
            return true;
          }
        }
        else if(node_type==NDelete) {
          const BWDeleteNode<KeyType> *delete_node_ptr = static_cast<const BWDeleteNode<KeyType> *>(node_ptr);
          if(key_equality_checker_(delete_node_ptr->GetKey(), key)) {
            --delta;
          }
          //TODO: complete this
        }
        else if(node_type==NSplitEntry) {


//          } else if (node_type == NMergeEntry){

        }
        node_ptr = (static_cast<const BWDeltaNode *>(node_ptr))->GetNext();
      } while(true);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    DeltaInsert(const PID &cur, const BWNode *node_ptr, const KeyType &key, const ValueType &value, bool need_expand) {
      const BWNode *insert_node_ptr =
              new BWInsertNode<KeyType, ValueType>(node_ptr, node_ptr->GetSlotUsage()+(need_expand?1:0), key, value);
      if(!pid_table_.bool_compare_and_swap(cur, node_ptr, insert_node_ptr)) {
        delete insert_node_ptr;
        return false;
      }
      return true;
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    DeltaDelete(const PID &cur, const BWNode *node_ptr, const KeyType &key, const ValueType &value, bool &exist_value) {
      if(!Duplicate) {
        const BWNode *delete_node_ptr =
                new BWDeleteNode<KeyType, ValueType>(node_ptr, node_ptr->GetSlotUsage()-1, key, value);
        if(!pid_table_.bool_compare_and_swap(cur, node_ptr, delete_node_ptr)) {
          delete delete_node_ptr;
          exist_value = true;
          return false;
        }
        return true;
      }
      else {
        std::vector<KeyType> keys_view;
        std::vector<std::vector<ValueType>> values_view;
        PID left_view, right_view;
        CreateLeafNodeView(node_ptr, keys_view, values_view, left_view, right_view);
        assert(keys_view.size()+1==values_view.size());
        auto position = std::lower_bound(keys_view.cbegin(), keys_view.cend(), comparator_);
        assert(position!=keys_view.cend()&&key_equality_checker_(key, *position));
        auto dist = std::distance(keys_view.cbegin(), position);
        std::vector<ValueType> &value_vector = values_view[dist];
        //TODO
        if(!std::find_if(value_vector.cbegin(), value_vector.cend(), )) {
          exist_value = false;
          return false;
        }
        exist_value = true;
        const BWNode *delete_node_ptr =
                new BWDeleteNode<KeyType, ValueType>(node_ptr, node_ptr->GetSlotUsage()-(value_vector.size()==1?1:0), key, value);
        if(!pid_table_.bool_compare_and_swap(cur, node_ptr, delete_node_ptr)) {
          delete delete_node_ptr;
          return false;
        }
        return true;

      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    InsertEntryUtil(const KeyType &key, const ValueType &value, std::vector<PID> &path, std::vector<VersionNumber> &version_number) {
      while(true) {
        assert(path.size()==version_number.size());
        const PID &current = path.back();
        const BWNode *node_ptr = pid_table_.get(current);
        /*
        VersionNumber version = node_ptr->GetVersionNumber();
        if(version!=version_number.back()) {
          path.pop_back();
          version_number.pop_back();
          if(path.empty()) {
            path.push_back(root_);
            version_number.push_back(pid_table_.get(root_)->GetVersionNumber());
          }
          continue;
        }
        */
        // If current is split node
        if(!CheckStatus(node_ptr, key, path, version_number))
          continue;
        /*
        if(node_ptr->GetType()==NSplit) {
          bool result = InsertSplitEntry(node_ptr, path, version_number);
          assert(path.size()==version_number.size());
          if(!result) {
            if(path.size()>1) {
              path.pop_back();
              version_number.pop_back();
            }
            continue;
          }
          const BWSplitNode<KeyType> *split_ptr = static_cast<const BWSplitNode<KeyType> *>(node_ptr);
          if(!comparator_(key, split_ptr->GetSplitKey()))
            continue;
        }

        // If delta chain is too long
        if(node_ptr->IfChainTooLong()) {
          Consolidate(current, node_ptr);
          //TODO no need to pop back?
          //path.pop_back();
          continue;
        }

        if(node_ptr->IfOverflow()) {
          node_ptr = Split(node_ptr, current);
          if(node_ptr==nullptr)
            continue;
          InsertSplitEntry(node_ptr, path, version_number);
          assert(path.size()==version_number.size());
          // retry
          if(path.size()>1) {
            path.pop_back();
            version_number.pop_back();
          }
          continue;
        }
        */
        if(node_ptr->IfInnerNode()) {
          std::vector<KeyType> keys_view;
          std::vector<PID> children_view;
          PID left_view, right_view;
          CreateInnerNodeView(node_ptr, keys_view, children_view, left_view, right_view);
          assert(keys_view.size()+1==children_view.size());
          auto position = std::upper_bound(keys_view.cbegin(), keys_view.cend(), comparator_);
          auto dist = std::distance(keys_view.cbegin(), position);
          PID next_pid = children_view[dist];
          VersionNumber final_number = pid_table_.get(path.back())->GetVersionNumber();
          if(final_number!=version_number.back()) {
            version_number.pop_back();
            version_number.push_back(final_number);
            continue;
          }
          path.push_back(next_pid);
          version_number.push_back(pid_table_.get(next_pid)->GetVersionNumber());
        }
        else {
          assert(node_ptr->IfLeafNode());
          if(ExistKey(node_ptr, key)) {
            if(!Duplicate)
              return false;
            if(!DeltaInsert(current, node_ptr, key, value, true))
              continue;
            return true;
          }
          else {
            if(!DeltaInsert(current, node_ptr, key, value, false))
              continue;
            return true;
          }
        }
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    DeleteEntryUtil(const KeyType &key, const ValueType &value, std::vector<PID> &path, std::vector<VersionNumber> &version_number) {
      while(true) {
        assert(path.size()==version_number.size());
        const PID &current = path.back();
        const BWNode *node_ptr = pid_table_.get(current);
        /*
        VersionNumber version = node_ptr->GetVersionNumber();
        if(version!=version_number.back()) {
          path.pop_back();
          version_number.pop_back();
          if(path.empty()) {
            path.push_back(root_);
            version_number.push_back(pid_table_.get(root_)->GetVersionNumber());
          }
          continue;
        }
        */
        // If current is split node
        if(!CheckStatus(node_ptr, key, path, version_number))
          continue;
        /*
        if(node_ptr->GetType()==NSplit) {
          bool result = InsertSplitEntry(node_ptr, path, version_number);
          assert(path.size()==version_number.size());
          if(!result) {
            if(path.size()>1) {
              path.pop_back();
              version_number.pop_back();
            }
            continue;
          }
          const BWSplitNode<KeyType> *split_ptr = static_cast<const BWSplitNode<KeyType> *>(node_ptr);
          if(!comparator_(key, split_ptr->GetSplitKey()))
            continue;
        }

        // If delta chain is too long
        if(node_ptr->IfChainTooLong()) {
          Consolidate(current, node_ptr);
          //TODO no need to pop back?
          //path.pop_back();
          continue;
        }

        if(node_ptr->IfOverflow()) {
          node_ptr = Split(node_ptr, current);
          if(node_ptr==nullptr)
            continue;
          InsertSplitEntry(node_ptr, path, version_number);
          assert(path.size()==version_number.size());
          // retry
          if(path.size()>1) {
            path.pop_back();
            version_number.pop_back();
          }
          continue;
        }
        */
        if(node_ptr->IfInnerNode()) {
          std::vector<KeyType> keys_view;
          std::vector<PID> children_view;
          PID left_view, right_view;
          CreateInnerNodeView(node_ptr, keys_view, children_view, left_view, right_view);
          assert(keys_view.size()+1==children_view.size());
          auto position = std::upper_bound(keys_view.cbegin(), keys_view.cend(), comparator_);
          auto dist = std::distance(keys_view.cbegin(), position);
          PID next_pid = children_view[dist];
          VersionNumber final_number = pid_table_.get(path.back())->GetVersionNumber();
          if(final_number!=version_number.back()) {
            version_number.pop_back();
            version_number.push_back(final_number);
            continue;
          }
          path.push_back(next_pid);
          version_number.push_back(pid_table_.get(next_pid)->GetVersionNumber());
        }
        else {
          assert(node_ptr->IfLeafNode());
          if(!ExistKey(node_ptr, key))
            return false;
          bool exist_value;
          bool result = DeltaDelete(current, node_ptr, key, value, exist_value);
          if(result)
            return true;
          if(!exist_value) {
            assert(Duplicate);
            return false;
          }
          continue;
        }
      }
    }
/*
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ScanKeyUtil(PID cur, KeyType key, std::vector<ValueType> &ret) {
      PIDTable& pidTable = PIDTable::get_table();
      const BWNode *node_ptr = pidTable.get(cur);

      if(node_ptr->GetType()==NLeaf) {
        unsigned short delete_count = 0;
        while(true) {
          // TODO: assume no renmoe node delta and merge node delta.
          // Only handle split node, insert node, delete node.
          if(node_ptr->GetType()==NSplit) {
            const BWSplitNode<KeyType> *split_ptr = static_cast<const BWSplitNode<KeyType> *>(node_ptr);
            KeyType split_key = split_ptr->GetSplitKey();
            if(comparator_(key, split_key)==true) { // == true means key < split_key
              node_ptr = split_ptr->GetNext();
            }
            else {
              node_ptr = pidTable.get(split_ptr->GetRightPID());
            }
          }
          else if(node_ptr->GetType()==NInsert) {
            const BWInsertNode<KeyType, ValueType> *insert_ptr = static_cast<const BWInsertNode<KeyType, ValueType> *>(node_ptr);
            if(key_equality_checker_(insert_ptr->GetKey(), key)==true&&delete_count==0) {
              ret.push_back(insert_ptr->GetValue());
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
            const BWDeleteNode<KeyType> *delete_ptr = static_cast<const BWDeleteNode<KeyType> *>(node_ptr);
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
            const BWLeafNode<KeyType, ValueType, KeyComparator> *leaf_ptr = static_cast<const BWLeafNode<KeyType, ValueType, KeyComparator> *>(node_ptr);
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
*/

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

  }  // End index namespace
}  // End peloton namespace