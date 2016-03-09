
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

#include <ctime>
#include <cstdlib>
#include <limits>
#include <chrono>
#include <thread>

namespace peloton {
  namespace index {





    GarbageCollector GarbageCollector::global_gc_;

    void *Begin(void *arg) {
      LOG_DEBUG("GarbageCollector::Begin()");
      GarbageCollector *gc = static_cast<GarbageCollector *>(arg);
      while (!(gc->stopped_)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(GarbageCollector::epoch_interval_));
        gc->head_ = new Epoch((gc->timer_)++, gc->head_);
        gc->ReclaimGarbage();
      }
      LOG_DEBUG("GarbageCollector::Begin() Stopped");
      return NULL;
    }

    const int GarbageCollector::epoch_interval_ = 10; //ms

    void GarbageCollector::ReclaimGarbage() {
      //LOG_DEBUG("GarbageCollector::ReclaimGarbage()");
      myassert(head_!=nullptr&&head_->next_!=nullptr);
      if(last_stopped_prev_==nullptr)
        last_stopped_prev_ = head_->next_;

      // check if all the epochs beyond last_stopped_prev are safe to delete
      for(Epoch *p = last_stopped_prev_->next_;
          p!=nullptr; p = p->next_)
        if(!(p->SafeToReclaim()))
          return ;
      //LOG_DEBUG("GarbageCollector::ReclaimGarbage() safe to reclaim");
      // if so, we delete all the epochs beyond prev
      ReclaimGarbageList(last_stopped_prev_->next_);
      last_stopped_prev_->next_ = nullptr;
      last_stopped_prev_ = nullptr;
    }

    void GarbageCollector::ReclaimGarbageList(Epoch *head) {
      Epoch *next;
      while(head!=nullptr) {
        myassert(head->SafeToReclaim());
        next = head->next_;
        delete head;
        head = next;
      }
    }





    const BWBaseNode *BWBaseNode::GenerateRandomNodeChain(int length) {
      return BWNode<index::IntsKey<1>, index::IntsComparator<1>>::GenerateRandomNodeChain(length);
    }

    template<typename KeyType, typename KeyComparator>
    const BWNode<KeyType, KeyComparator> *
    BWNode<KeyType, KeyComparator>::
    GenerateRandomNodeChain(int length) {
      //LOG_DEBUG("BWNode::GenerateRandomNodeChain(%d)", length);
      srand(time(NULL));
      myassert(length>=0);
      if(length==0)
        return nullptr;

      const BWNode<KeyType, KeyComparator> *head =
              GenerateRandomNode((NodeType)(rand()%2), nullptr);
      myassert(head->GetType()==NLeaf||head->GetType()==NInner);

      while(--length>0) {
        head = GenerateRandomNode((NodeType)(rand()%4+2), head);
        myassert(head->GetType()==NInsert||head->GetType()==NDelete||
               head->GetType()==NSplit||head->GetType()==NSplitEntry);
      }

      return head;
    }

    template<typename KeyType, typename KeyComparator>
    const BWNode<KeyType, KeyComparator> *
    BWNode<KeyType, KeyComparator>::
    GenerateRandomNode(NodeType type, const BWNode<KeyType, KeyComparator> *next) {
      switch(type) {
        case NInner:
          myassert(next==nullptr);
          return new BWInnerNode<IntsKey<1>, IntsComparator<1>>(
                  std::vector<IntsKey<1>>(),
                  std::vector<PID>({PIDTable<KeyType, KeyComparator>::PID_NULL}),
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  false, false);
        case NLeaf:
          myassert(next==nullptr);
          return new BWLeafNode<IntsKey<1>, IntsComparator<1>, ItemPointer>(
                  std::vector<IntsKey<1>>(), std::vector<ItemPointer>(),
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  false, false);
        case NInsert:
          myassert(next!=nullptr);
          return new BWInsertNode<IntsKey<1>, IntsComparator<1>, ItemPointer>(
                  next, 1,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  IntsKey<1>(), ItemPointer());
        case NDelete:
          myassert(next!=nullptr);
          return new BWDeleteNode<IntsKey<1>, IntsComparator<1>, ItemPointer>(
                  next, 1,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  IntsKey<1>(), ItemPointer());
        case NSplit:
          myassert(next!=nullptr);
          return new BWSplitNode<IntsKey<1>, IntsComparator<1>>(
                  next, 1,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  IntsKey<1>(), PIDTable<KeyType, KeyComparator>::PID_NULL);
        case NSplitEntry:
          myassert(next!=nullptr);
          return new BWSplitEntryNode<IntsKey<1>, IntsComparator<1>>(
                  next,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  PIDTable<KeyType, KeyComparator>::PID_NULL,
                  IntsKey<1>(), PIDTable<KeyType, KeyComparator>::PID_NULL);
        default:
          myassert(0);
          return nullptr;
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    SubmitGarbageNode(const BWNode<KeyType, KeyComparator> *node) {
      if(node==nullptr)
        return ;
      if(node->IfInnerNode()) {
        std::vector<KeyType> keys_view;
        std::vector<PID> children_view;
        PID left_view, right_view;
        CreateInnerNodeView(node, &keys_view, &children_view, &left_view, &right_view);
        for(auto child = children_view.begin(); child!=children_view.end(); ++child) {
          const BWNode<KeyType, KeyComparator> *child_node = pid_table_.get(*(child));
          pid_table_.free_PID(*(child));
          SubmitGarbageNode(child_node);
        }
      }
      GarbageCollector::global_gc_.SubmitGarbage(node);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    size_t
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    GetMemoryFootprint(const PID &node_pid) const {
      const BWNode<KeyType, KeyComparator> *node = pid_table_.get(node_pid);
      size_t size = 0;
      if(node->IfInnerNode()) {
        std::vector<KeyType> keys_view;
        std::vector<PID> children_view;
        PID left_view, right_view;
        CreateInnerNodeView(node, &keys_view, &children_view, &left_view, &right_view);
        for(PID child: children_view)
          size += GetMemoryFootprint(child);
      }
      return size+node->GetMemoryFootprint();
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    CompactNode(const PID &node_pid) {
      const BWNode<KeyType, KeyComparator> *node = pid_table_.get(node_pid);
      // if safe and worth to consolidate
      if(node->GetType()!=NSplit&&
         node->GetChainLength()>compact_chain_len_threshold) {
        Consolidate(node_pid, node);
        node = pid_table_.get(node_pid);
      }
      // recursively consolidate all children
      if(node->IfInnerNode()) {
        std::vector<KeyType> keys_view;
        std::vector<PID> children_view;
        PID left_view, right_view;
        CreateInnerNodeView(node, &keys_view, &children_view, &left_view, &right_view);
        for(PID child: children_view)
          CompactNode(child);
      }
      return true;
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode<KeyType, KeyComparator> *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    SplitInnerNode(const BWNode<KeyType, KeyComparator> *inner_node, const PID &node_pid) {
      myassert(inner_node->IfInnerNode());
      std::vector<KeyType> keys_view;
      std::vector<PID> children_view;
      PID left_view, right_view;
      CreateInnerNodeView(inner_node, &keys_view, &children_view, &left_view, &right_view);
      myassert(keys_view.size() + 1 == children_view.size());
      myassert(keys_view.size() >= max_node_size);

      // create new inner node
      auto index = keys_view.size()/2;
      // long enough
      myassert(index>0);
      const KeyType &split_key = keys_view[index-1];
      const BWNode<KeyType, KeyComparator> *new_node =
              new BWInnerNode<KeyType, KeyComparator>(
                std::vector<KeyType>(keys_view.cbegin()+index, keys_view.cend()),
                std::vector<PID>(children_view.cbegin()+index, children_view.cend()),
                node_pid, right_view,
                split_key, inner_node->GetHighKey(),
                true, inner_node->HasHighKey()
                );
      PID new_pid = pid_table_.allocate_PID(new_node);

      const BWNode<KeyType, KeyComparator> *split_node =
              new BWSplitNode<KeyType, KeyComparator>(
                      inner_node, index-1,
                      left_view, new_pid,
                      split_key, new_pid);

      if(!pid_table_.bool_compare_and_swap(node_pid, inner_node, split_node)) {
        delete new_node;
        delete split_node;
        pid_table_.free_PID(new_pid);
        return nullptr;
      }

      return split_node;
    }

    /*
     * split a leaf node
     * on success, it returns the split node (as in the first step of split)
     * on failure, it returns nullptr
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode<KeyType, KeyComparator> *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    SplitLeafNode(const BWNode<KeyType, KeyComparator> *leaf_node, const PID &node_pid) {
      myassert(leaf_node->IfLeafNode());
      if(!Duplicate) {
        std::vector<KeyType> keys_view;
        std::vector<ValueType> values_view;
        PID left_view, right_view;
        CreateLeafNodeView(leaf_node, &keys_view, &values_view, &left_view, &right_view);
        myassert(keys_view.size()==values_view.size());
        myassert(keys_view.size()>=max_node_size);

        // find the split position, and get the low key
        auto index = keys_view.size()/2;
        const KeyType &split_key = keys_view[index];

        // create a new leaf node and a new split node
        const BWNode<KeyType, KeyComparator> *new_node =
                new BWLeafNode<KeyType, KeyComparator, ValueType>(
                  std::vector<KeyType>(keys_view.cbegin()+index, keys_view.cend()),
                  std::vector<ValueType>(values_view.cbegin()+index, values_view.cend()),
                  node_pid, right_view,
                  split_key, leaf_node->GetHighKey(),
                  true, leaf_node->HasHighKey());
        PID new_pid = pid_table_.allocate_PID(new_node);
        const BWNode<KeyType, KeyComparator> *split_node =
                new BWSplitNode<KeyType, KeyComparator>(
                        leaf_node, index,
                        left_view, new_pid,
                        split_key, new_pid);

        // try to atomically install the split node
        if(!pid_table_.bool_compare_and_swap(node_pid, leaf_node, split_node)) {
          delete new_node;
          delete split_node;
          pid_table_.free_PID(new_pid);
          return nullptr;
        }
        return split_node;
      }
      else { // the same except we have different type for values_view
        std::vector<KeyType> keys_view;
        std::vector<std::vector<ValueType>> values_view;
        PID left_view, right_view;
        CreateLeafNodeView(leaf_node, &keys_view, &values_view, &left_view, &right_view);
        myassert(keys_view.size()==values_view.size());
        myassert(keys_view.size()>=max_node_size);

        auto index = keys_view.size()/2;
        const KeyType &split_key = keys_view[index];
        const BWNode<KeyType, KeyComparator> *new_node =
                new BWLeafNode<KeyType, KeyComparator, std::vector<ValueType>>(
                  std::vector<KeyType>(keys_view.cbegin()+index, keys_view.cend()),
                  std::vector<std::vector<ValueType>>(values_view.cbegin()+index, values_view.cend()),
                  node_pid, right_view,
                  split_key, leaf_node->GetHighKey(),
                  true, leaf_node->HasHighKey());
        PID new_pid = pid_table_.allocate_PID(new_node);
        const BWNode<KeyType, KeyComparator> *split_node =
                new BWSplitNode<KeyType, KeyComparator>(
                        leaf_node, index,
                        left_view, new_pid,
                        split_key, new_pid);

        if(!pid_table_.bool_compare_and_swap(node_pid, leaf_node, split_node)) {
          delete new_node;
          delete split_node;
          pid_table_.free_PID(new_pid);
          return nullptr;
        }
        return split_node;
      }
    }

    /*
     * try to split a node
     * it calls other functions depending on the node type to finish the function
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode<KeyType, KeyComparator> *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    Split(const BWNode<KeyType, KeyComparator> *node_ptr, const PID &node_pid) {
      if (node_ptr->IfLeafNode()) {
        return SplitLeafNode(node_ptr, node_pid);
      }else {
        return SplitInnerNode(node_ptr, node_pid);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    InsertEntryUtil(const KeyType &key, const ValueType &value,
                    std::vector<PID> &path, VersionNumber root_version_number) {
      //LOG_TRACE("InsertEntryUtil()");
      while(true) {
        // first check if we are in the right node
        const PID &current = path.back();
        //LOG_DEBUG("InsertEntryUtil while loop(%lu).", (unsigned long)current);
        const BWNode<KeyType, KeyComparator> *node_ptr = pid_table_.get(current);
        if(!node_ptr->IfInRange(key, key_comparator_)) {
          myassert(path.size()>1);
          //LOG_DEBUG("pid %lu is a wrong node.", (unsigned long)path.back());
          path.pop_back();
          continue;
        }
        if(path.size()==1&&root_version_number!=root_version_number_)
          root_version_number = root_version_number_;

        if(!CheckStatus(node_ptr, key, path, root_version_number))
          continue;

        if(node_ptr->IfInnerNode()) {
          PID next_pid = FindNextNodePID(node_ptr, key);
          path.push_back(next_pid);
        }
        else {
          myassert(node_ptr->IfLeafNode());
          myassert(path.size()>1);
          size_t value_vector_size = 0;
          if(ExistKeyValue(node_ptr, key, value, value_vector_size)) {
            if(!Duplicate) {
              return false;
            }
            if(!DeltaInsert(current, node_ptr, key, value, false)) {
              continue;
            }
            return true;
          }
          else {
            if(!Duplicate&&value_vector_size>0)
              return false;
            if(!DeltaInsert(current, node_ptr, key, value, value_vector_size==0)) {
              continue;
            }
            return true;
          }
        }
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    DeleteEntryUtil(const KeyType &key, const ValueType &value,
                    std::vector<PID> &path, VersionNumber root_version_number) {
      //LOG_TRACE("DeleteEntryUtil()");
      while(true) {
        // first check if we are in the right node
        const PID &current = path.back();
        const BWNode<KeyType, KeyComparator> *node_ptr = pid_table_.get(current);
        if(!node_ptr->IfInRange(key, key_comparator_)) {
          myassert(path.size()>1);
          path.pop_back();
          continue;
        }
        if(path.size()==1&&root_version_number!=root_version_number_)
          root_version_number = root_version_number_;

        // If current is split node
        if(!CheckStatus(node_ptr, key, path, root_version_number))
          continue;

        if(node_ptr->IfInnerNode()) {
          PID next_pid = FindNextNodePID(node_ptr, key);
          path.push_back(next_pid);
        }
        else {
          myassert(node_ptr->IfLeafNode());
          myassert(path.size()>1);
          size_t value_vector_size;
          if(!ExistKeyValue(node_ptr, key, value, value_vector_size)) {
            return false;
          }
          if(DeltaDelete(current, node_ptr, key, value, value_vector_size==1))
            return true;
          continue;
        }
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ScanKeyUtil(const KeyType &key, std::vector<ValueType> &result,
                std::vector<PID> &path, VersionNumber root_version_number) {
      //LOG_TRACE("ScanKeyUtil()");
      while(true) {
        // first check if we are in the right node
        const PID &current = path.back();
        const BWNode<KeyType, KeyComparator> *node_ptr = pid_table_.get(current);
        if(!node_ptr->IfInRange(key, key_comparator_)) {
          myassert(path.size()>1);
          path.pop_back();
          continue;
        }
        if(path.size()==1&&root_version_number!=root_version_number_)
          root_version_number = root_version_number_;

        if(!CheckStatus(node_ptr, key, path, root_version_number))
          continue;

        if(node_ptr->IfInnerNode()) {
          PID next_pid = FindNextNodePID(node_ptr, key);
          path.push_back(next_pid);
        }
        else {
          myassert(node_ptr->IfLeafNode());
          myassert(path.size()>1);
          if(!Duplicate) {
            std::vector<KeyType> keys_view;
            std::vector<ValueType> values_view;
            PID left_view, right_view;
            CreateLeafNodeView(node_ptr, &keys_view, &values_view, &left_view, &right_view);
            auto position = std::lower_bound(keys_view.begin(), keys_view.end(), key, key_comparator_);
            if(position==keys_view.end()||!key_equality_checker_(key, *position))
              return ;
            myassert((position==keys_view.begin()||!key_equality_checker_(key, *(position-1)))&&
                     (position+1==keys_view.end()||!key_equality_checker_(key, *(position+1))));
            auto dist = std::distance(keys_view.begin(), position);
            //LOG_DEBUG("ScanKeyUtil dist=%lu", (unsigned long)dist);
            result.push_back(values_view[dist]);
          }
          else {
            std::vector<KeyType> keys_view;
            std::vector<std::vector<ValueType>> values_view;
            PID left_view, right_view;
            CreateLeafNodeView(node_ptr, &keys_view, &values_view, &left_view, &right_view);
            auto position = std::lower_bound(keys_view.begin(), keys_view.end(), key, key_comparator_);
            if(position==keys_view.end()||!key_equality_checker_(key, *position))
              return ;
            myassert((position==keys_view.begin()||!key_equality_checker_(key, *(position-1)))&&
                     (position+1==keys_view.end()||!key_equality_checker_(key, *(position+1))));
            auto dist = std::distance(keys_view.begin(), position);
            //LOG_DEBUG("ScanKeyUtil dist=%lu, values_view.size=%lu", (unsigned long)dist, values_view.size());
            result = values_view[dist];
          }
          return ;
        }
      }
    }

    /*
     * check the status of a node
     * it will check whether some SMOs (split, consolidate) are needed
     * it returns false when some SMO has been done that we need to retry this function
     * returns true if it passes all checks
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    CheckStatus(const BWNode<KeyType, KeyComparator> *node_ptr, const KeyType &key,
                std::vector<PID> &path, const VersionNumber &root_version_number) {
      //LOG_TRACE("CheckStatus()");
      myassert(node_ptr->IfInRange(key, key_comparator_));
      const PID &current = path.back();

      // if we need to help others finish the second step of split
      if(node_ptr->GetType()==NSplit) {
        bool result = InsertSplitEntry(node_ptr, key, path, root_version_number);
        // if failed, rollback to the parent node
        if(!result) {
          if(path.size()>1) {
            path.pop_back();
          }
          //LOG_TRACE("CheckStatus() SplitNode");
          return false;
        }
      }

      // if delta chain is too long
      if(node_ptr->IfChainTooLong()) {
        Consolidate(current, node_ptr);
        //LOG_TRACE("CheckStatus() Too Long");
        return false;
      }

      // if node size is too big
      if(node_ptr->IfOverflow()) {
        // try the first step of split
        node_ptr = Split(node_ptr, current);
        if(node_ptr==nullptr)
          return false;
        // the same as helping others to finish the second step of split
        bool result = InsertSplitEntry(node_ptr, key, path, root_version_number);
        if(!result) {
          if(path.size()>1) {
            path.pop_back();
          }
          //LOG_TRACE("CheckStatus() Overflow");
          return false;
        }
        // always retry
        //LOG_TRACE("CheckStatus() Overflow");
        return false;
      }
      //LOG_TRACE("CheckStatus() Return True");
      return true;
    };

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ExistKeyValue(const BWNode<KeyType, KeyComparator> *node_ptr,
                  const KeyType &key, const ValueType &value,
                  size_t &value_vector_size) const {
      //LOG_TRACE("ExistKeyValue()");
      // assume node_ptr is the header of leaf node
      myassert(node_ptr->IfLeafNode());
      value_vector_size = 0;
      if(Duplicate) {
        std::vector<KeyType> keys_view;
        std::vector<std::vector<ValueType>> values_view;
        PID left, right;
        CreateLeafNodeView(node_ptr, &keys_view, &values_view, &left, &right);
        auto position = std::lower_bound(keys_view.begin(), keys_view.end(), key, key_comparator_);
        if(position==keys_view.end()||!key_equality_checker_(key, *position))
          return false;
        auto dist = std::distance(keys_view.begin(), position);
        const std::vector<ValueType> &value_vector = values_view[dist];
        value_vector_size = value_vector.size();
        myassert(value_vector_size>0);
        auto value_position = std::lower_bound(value_vector.begin(), value_vector.end(), value, value_comparator_);
        return value_position!=value_vector.end()&&value_equality_checker_(*value_position, value);
      }
      else {
        std::vector<KeyType> keys_view;
        std::vector<ValueType> values_view;
        PID left, right;
        CreateLeafNodeView(node_ptr, &keys_view, &values_view, &left, &right);
        auto position = std::lower_bound(keys_view.begin(), keys_view.end(), key, key_comparator_);
        if(position==keys_view.end()||!key_equality_checker_(key, *position))
          return false;
        value_vector_size = 1;
        auto dist = std::distance(keys_view.begin(), position);
        return value_equality_checker_(values_view[dist], value);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    PID
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    FindNextNodePID(const BWNode<KeyType, KeyComparator> *node_ptr, const KeyType &key) const {
      //LOG_TRACE("FindNextNodePID()");
      myassert(node_ptr->IfInRange(key, key_comparator_));
      std::vector<KeyType> keys_view;
      std::vector<PID> children_view;
      PID left_view, right_view;
      CreateInnerNodeView(node_ptr, &keys_view, &children_view, &left_view, &right_view);
      myassert(keys_view.size() + 1 == children_view.size());
      auto position = std::upper_bound(keys_view.begin(), keys_view.end(), key, key_comparator_);
      auto dist = std::distance(keys_view.begin(), position);
      return children_view[dist];
    }


    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode<KeyType, KeyComparator> *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    FirstLeafNode() const {
      // assume we already registered
      //LOG_TRACE("enter");
      PID next_pid = root_;
      const BWNode<KeyType, KeyComparator> *node_ptr = pid_table_.get(next_pid);
      while(!node_ptr->IfLeafNode()) {
        std::vector<KeyType> keys;
        std::vector<PID> children;
        PID left, right;
        CreateInnerNodeView(node_ptr, &keys, &children, &left, &right);
        myassert(children.size() != 0);
        next_pid = children[0];
        node_ptr = pid_table_.get(next_pid);
      }
      //LOG_TRACE("leave");
      return node_ptr;
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode<KeyType, KeyComparator> *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    FindLeafNode(const KeyType &key) {
      //LOG_TRACE("enter");
      // assume we already registered
      std::vector<PID> path = {root_};
      VersionNumber root_version_number = root_version_number_;

      while(true) {
        // first check if we are in the right node
        const PID &current = path.back();
        const BWNode<KeyType, KeyComparator> *node_ptr = pid_table_.get(current);
        if(!node_ptr->IfInRange(key, key_comparator_)) {
          myassert(path.size()>1);
          path.pop_back();
          continue;
        }
        if(path.size()==1&&root_version_number!=root_version_number_)
          root_version_number = root_version_number_;

        if(!CheckStatus(node_ptr, key, path, root_version_number))
          continue;

        if(node_ptr->IfInnerNode()) {
          PID next_pid = FindNextNodePID(node_ptr, key);
          path.push_back(next_pid);
        }
        else {
          //LOG_TRACE("leave");
          return node_ptr;
        }
      }
    }

    /*
     * insert a split entry node at the parent of 'top' to finish the second step of split
     * path contains the pids all the way from the root down to 'top'
     * if the second step is finished (either by us or by others), this function returns true
     * if the function detects we have mismatch version numbers in our path, it returns false
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    InsertSplitEntry(const BWNode<KeyType, KeyComparator> *top, const KeyType &key,
                     std::vector<PID> &path, const VersionNumber &root_version_number){
      myassert(top->GetType()==NSplit);
      // get split information
      const BWSplitNode<KeyType, KeyComparator> *split_ptr =
              static_cast<const BWSplitNode<KeyType, KeyComparator> *>(top);
      const KeyType &split_key = split_ptr->GetSplitKey();
      const PID &to = split_ptr->GetSplitTo();

      if(path.size()>1) { // In this case, this is a normal second step split
        // get the parent
        PID pid = path[path.size()-2];
        while(true) {
          const BWNode<KeyType, KeyComparator> *parent_node = pid_table_.get(pid);
          if((path.size()==2&&root_version_number!=root_version_number_)||
             !parent_node->IfInRange(key, key_comparator_))
            return false;

          // try to find the high key
          std::vector<KeyType> keys_view;
          std::vector<PID> children_view;
          PID left_view, right_view;
          CreateInnerNodeView(parent_node, &keys_view, &children_view, &left_view, &right_view);

          auto position = std::upper_bound(keys_view.begin(), keys_view.end(), split_key, key_comparator_);

          // then check if this split entry has been inserted by others
          if(position!=keys_view.begin()&&key_equality_checker_(split_key, *(position-1)))
            return true;

          // try to insert the split entry
          const BWNode<KeyType, KeyComparator> *split_entry;
          if(position!=keys_view.end())
            split_entry = new BWSplitEntryNode<KeyType, KeyComparator>(
                    parent_node, split_key, *position, to);
          else
            split_entry = new BWSplitEntryNode<KeyType, KeyComparator>(
                    parent_node, split_key, to);
          if(pid_table_.bool_compare_and_swap(pid, parent_node, split_entry))
            return true;

          // fail, clean up, retry
          delete split_entry;
        }
      }
      else {
        myassert(path.back()==root_);
        const BWNode<KeyType, KeyComparator> *old_root = pid_table_.get(root_);
        // allocate a new pid for the original root node
        PID new_pid = pid_table_.allocate_PID(old_root);

        // allocate a new node for the original root pid
        const BWNode<KeyType, KeyComparator> *new_root =
                new BWInnerNode<KeyType, KeyComparator>(
                        {split_key}, {new_pid, to},
                        PIDTable<KeyType, KeyComparator>::PID_NULL,
                        PIDTable<KeyType, KeyComparator>::PID_NULL,
                        false, false);
        // try to atomically install the new root
        if(!pid_table_.bool_compare_and_swap(root_, old_root, new_root)) {
          delete new_root;
          pid_table_.free_PID(new_pid);
          return false;
        }
        ++root_version_number_;
        return false;
      }
    }

    /*
     * try to insert an insertion delta node on top
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    DeltaInsert(const PID &cur, const BWNode<KeyType, KeyComparator> *node_ptr,
                const KeyType &key, const ValueType &value, bool need_expand) {
      //LOG_TRACE("DeltaInsert(cur=%lu)", (unsigned long) cur);
      const BWNode<KeyType, KeyComparator> *insert_node_ptr =
              new BWInsertNode<KeyType, KeyComparator, ValueType>(
                      node_ptr, node_ptr->GetSlotUsage() + (need_expand ? 1 : 0),
                      key, value);
      if(!pid_table_.bool_compare_and_swap(cur, node_ptr, insert_node_ptr)) {
        delete insert_node_ptr;
        return false;
      }
      return true;
    }

    /*
     * try to insert a delete delta node on top of node_ptr
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    DeltaDelete(const PID &cur, const BWNode<KeyType, KeyComparator> *node_ptr,
                const KeyType &key, const ValueType &value, bool need_shrink) {
      //LOG_TRACE("DeltaDelete(cur=%lu)", (unsigned long) cur);
      const BWNode<KeyType, KeyComparator> *delete_node_ptr =
              new BWDeleteNode<KeyType, KeyComparator, ValueType>(
                      node_ptr, node_ptr->GetSlotUsage() - (need_shrink ? 1 : 0),
                      key, value);
      if(!pid_table_.bool_compare_and_swap(cur, node_ptr, delete_node_ptr)) {
        delete delete_node_ptr;
        return false;
      }
      return true;
    }

    /*
     * try to replace a long node chain with a consolidated node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    bool
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    Consolidate(const PID &cur, const BWNode<KeyType, KeyComparator> *node_ptr) {
      const BWNode<KeyType, KeyComparator> *new_node = nullptr;
      if(node_ptr->IfLeafNode())
        new_node = ConstructConsolidatedLeafNode(node_ptr);
      else
        new_node = ConstructConsolidatedInnerNode(node_ptr);
      if(!pid_table_.bool_compare_and_swap(cur, node_ptr, new_node)) {
        delete new_node;
        return false;
      }
      GarbageCollector::global_gc_.SubmitGarbage(node_ptr);
      return true;
    }

    /*
     * Create a consolidated inner node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode<KeyType, KeyComparator> *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedInnerNode(
      const BWNode<KeyType, KeyComparator> *node_chain) const {
      myassert(node_chain!=NULL);
      std::vector<KeyType> keys;
      std::vector<PID> children;
      PID left, right;
      ConstructConsolidatedInnerNodeInternal(node_chain, &keys, &children, &left, &right);
      return new BWInnerNode<KeyType, KeyComparator>(
              keys, children,
              left, right,
              node_chain->GetLowKey(), node_chain->GetHighKey(),
              node_chain->HasLowKey(), node_chain->HasHighKey());
    }

    /*
     * Create a consolidated leaf node logically equivalent to the argument node_chain.
     * Return the new node
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    const BWNode<KeyType, KeyComparator> *
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedLeafNode(
      const BWNode<KeyType, KeyComparator> *node_chain) const {
      if(!Duplicate) {
        std::vector<KeyType> keys;
        std::vector<ValueType> values;
        PID left, right;
        ConstructConsolidatedLeafNodeInternal(node_chain, &keys, &values, &left, &right);
        return new BWLeafNode<KeyType, KeyComparator, ValueType>(
                keys, values,
                left, right,
                node_chain->GetLowKey(), node_chain->GetHighKey(),
                node_chain->HasLowKey(), node_chain->HasHighKey());
      }
      else {
        std::vector<KeyType> keys;
        std::vector<std::vector<ValueType>> values;
        PID left, right;
        ConstructConsolidatedLeafNodeInternal(node_chain, &keys, &values, &left, &right);
        return new BWLeafNode<KeyType, KeyComparator, std::vector<ValueType>>(
                keys, values,
                left, right,
                node_chain->GetLowKey(), node_chain->GetHighKey(),
                node_chain->HasLowKey(), node_chain->HasHighKey());
      }
    }

    /*
     * Construct vector keys and values from a leaf node linked list, along with its left and right siblings.
     * Collect everthing from node_chain and put it to (or remove it from) keys and values.
     */
    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedLeafNodeInternal(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys,
      std::vector<ValueType> *values,
      PID *left, PID *right) const {
      myassert(!Duplicate);
      myassert(node_chain->IfLeafNode());
      if(node_chain->GetType()==NLeaf) {
        // arrive at bottom
        const BWLeafNode<KeyType, KeyComparator, ValueType> *node =
                static_cast<const BWLeafNode<KeyType, KeyComparator, ValueType> *>(node_chain);
        if(keys!=nullptr)
          *keys = node->GetKeys();
        if(values!=nullptr)
          *values = node->GetValues();
        if(left!=nullptr)
          *left = node->GetLeft();
        if(right!=nullptr)
          *right = node->GetRight();
        return;
      }

      // still at delta chain
      myassert(node_chain->GetNext()!=NULL);
      ConstructConsolidatedLeafNodeInternal(node_chain->GetNext(), keys, values, left, right);
      switch(node_chain->GetType()) {
        case NInsert:
          ConsolidateInsertNode(static_cast<const BWInsertNode<KeyType, KeyComparator, ValueType> *>(node_chain), keys, values);
          break;
        case NDelete:
          ConsolidateDeleteNode(static_cast<const BWDeleteNode<KeyType, KeyComparator, ValueType> *>(node_chain), keys, values);
          break;
        case NSplit:
          ConsolidateSplitNode(static_cast<const BWSplitNode<KeyType, KeyComparator> *>(node_chain), keys, values, right);
          break;
        case NMerge:
          myassert(0);
          break;
        case NRemove:
          myassert(0);
          break;
        default:
          // fault
          myassert(0);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedLeafNodeInternal(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys,
      std::vector<std::vector<ValueType>> *values,
      PID *left, PID *right) const {
      myassert(Duplicate);
      myassert(node_chain->IfLeafNode());
      if(node_chain->GetType() == NLeaf) {
        // arrive at bottom
        const BWLeafNode<KeyType, KeyComparator, std::vector<ValueType>> *node =
                static_cast<const BWLeafNode<KeyType, KeyComparator, std::vector<ValueType>> *>(node_chain);
        if(keys!=nullptr)
          *keys = node->GetKeys();
        if(values!=nullptr)
          *values = node->GetValues();
        if(left!=nullptr)
          *left = node->GetLeft();
        if(right!=nullptr)
          *right = node->GetRight();
        return;
      }

      // still at delta chain
      myassert(node_chain->GetNext() != NULL);
      ConstructConsolidatedLeafNodeInternal(node_chain->GetNext(), keys, values, left, right);
      switch(node_chain->GetType()) {
        case NInsert: {
          ConsolidateInsertNode(static_cast<const BWInsertNode<KeyType, KeyComparator, ValueType> *>(node_chain), keys, values);
          break;
        }
        case NDelete: {
          ConsolidateDeleteNode(static_cast<const BWDeleteNode<KeyType, KeyComparator, ValueType> *>(node_chain), keys, values);
          break;
        }
        case NSplit: {
          ConsolidateSplitNode(static_cast<const BWSplitNode<KeyType, KeyComparator> *>(node_chain), keys, values, right);
          break;

        }
        case NMerge: {
          myassert(0);
          break;
        }
        case NRemove: {
          myassert(0);
          break;
        }
        default:
          // fault
          myassert(0);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConstructConsolidatedInnerNodeInternal(
      const BWNode<KeyType, KeyComparator> *node_chain,
      std::vector<KeyType> *keys,
      std::vector<PID> *children,
      PID *left, PID *right) const {
      myassert(node_chain->IfInnerNode());
      if(node_chain->GetType()==NInner) {
        // arrive at bottom
        const BWInnerNode<KeyType, KeyComparator> *node =
                static_cast<const BWInnerNode<KeyType, KeyComparator> *>(node_chain);
        if(keys!=nullptr)
          *keys = node->GetKeys();
        if(children!=nullptr)
          *children = node->GetChildren();
        if(left!=nullptr)
          *left = node->GetLeft();
        if(right!=nullptr)
          *right = node->GetRight();
        return;
      }

      // still at delta chain
      myassert(node_chain->GetNext()!=NULL);
      ConstructConsolidatedInnerNodeInternal(node_chain->GetNext(), keys, children, left, right);
      switch(node_chain->GetType()) {
        case NSplit:
          ConsolidateSplitNode(static_cast<const BWSplitNode<KeyType, KeyComparator> *>(node_chain), keys, children, right);
          break;
        case NSplitEntry:
          ConsolidateSplitEntryNode(static_cast<const BWSplitEntryNode<KeyType, KeyComparator> *>(node_chain), keys, children);
          break;
        case NMerge:
          // not implemented
          myassert(0);
          break;
        case NRemove:
          myassert(0);
          break;
        case NMergeEntry:
          myassert(0);
          break;
        default:
          // fault
          myassert(0);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateInsertNode(
      const BWInsertNode<KeyType, KeyComparator, ValueType> *node,
      std::vector<KeyType> *keys,
      std::vector<ValueType> *values) const {
      myassert(!Duplicate);
      myassert(keys->size()==values->size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetValue();
      auto position = std::upper_bound(keys->begin(), keys->end(), key, key_comparator_);
      // check for non-existence
      myassert(position==keys->begin()||!key_equality_checker_(key, *(position-1)));
      auto dist = std::distance(keys->begin(), position);
      keys->insert(position, key);
      values->insert(values->begin()+dist, value);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateInsertNode(
      const BWInsertNode<KeyType, KeyComparator, ValueType> *node,
      std::vector<KeyType> *keys,
      std::vector<std::vector<ValueType>> *values) const {
      myassert(Duplicate);
      myassert(keys->size()==values->size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetValue();
      auto position = std::upper_bound(keys->begin(), keys->end(), key, key_comparator_);
      auto dist = std::distance(keys->begin(), position);
      if(position==keys->begin()||!key_equality_checker_(key, *(position-1))) {
        keys->insert(position, key);
        values->insert(values->begin()+dist, {value});
      }
      else {
        std::vector<ValueType> &value_vector = (*values)[dist-1];
        auto value_position = std::upper_bound(value_vector.begin(), value_vector.end(), value, value_comparator_);
        value_vector.insert(value_position, value);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateDeleteNode(
      const BWDeleteNode<KeyType, KeyComparator, ValueType> *node,
      std::vector<KeyType> *keys,
      std::vector<ValueType> *values) const {
      myassert(!Duplicate);
      myassert(keys->size()==values->size());
      const KeyType &key = node->GetKey();
      auto position = std::lower_bound(keys->begin(), keys->end(), key, key_comparator_);
      // check for both existence and uniqueness
      myassert(position!=keys->end()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys->end()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys->begin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys->begin(), position);
      myassert(value_equality_checker_((*values)[dist], node->GetValue()));
      keys->erase(position);
      values->erase(values->begin()+dist);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateDeleteNode(
      const BWDeleteNode<KeyType, KeyComparator, ValueType> *node,
      std::vector<KeyType> *keys,
      std::vector<std::vector<ValueType>> *values) const {
      myassert(Duplicate);
      myassert(keys->size()==values->size());
      const KeyType &key = node->GetKey();
      const ValueType &value = node->GetValue();
      auto position = std::lower_bound(keys->begin(), keys->end(), key, key_comparator_);
      // check for existence
      myassert(position!=keys->end()&&
             key_equality_checker_(key, *position));
      auto dist = std::distance(keys->begin(), position);
      std::vector<ValueType> &value_vector = (*values)[dist];

      auto begin = std::lower_bound(value_vector.begin(), value_vector.end(), value, value_comparator_);
      auto end = std::upper_bound(value_vector.begin(), value_vector.end(), value, value_comparator_);
      if(begin==value_vector.begin()&&end==value_vector.end()) {
        keys->erase(position);
        values->erase(values->begin()+dist);
      }
      else {
        value_vector.erase(begin, end);
      }
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateSplitNode(
      const BWSplitNode<KeyType, KeyComparator> *node,
      std::vector<KeyType> *keys,
      std::vector<ValueType> *values,
      PID *right) const {
      myassert(!Duplicate);
      myassert(node->IfLeafNode());
      myassert(keys->size()==values->size());
      const KeyType &key = node->GetSplitKey();
      auto position = std::lower_bound(keys->begin(), keys->end(), key, key_comparator_);
      // check for both existence and uniqueness
      myassert(position!=keys->end()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys->end()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys->begin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys->begin(), position);
      keys->erase(position, keys->end());
      values->erase(values->begin()+dist, values->end());
      if(right!=nullptr)
        *right = node->GetSplitTo();
      myassert(!Duplicate);
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateSplitNode(
      const BWSplitNode<KeyType, KeyComparator> *node,
      std::vector<KeyType> *keys,
      std::vector<std::vector<ValueType>> *values,
      PID *right) const {
      myassert(Duplicate);
      myassert(node->IfLeafNode());
      myassert(keys->size()==values->size());
      const KeyType &key = node->GetSplitKey();
      auto position = std::lower_bound(keys->begin(), keys->end(), key, key_comparator_);
      // check for both existence and uniqueness
      myassert(position!=keys->end()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys->end()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys->begin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys->begin(), position);
      keys->erase(position, keys->end());
      values->erase(values->begin()+dist, values->end());
      if(right!=nullptr)
        *right = node->GetSplitTo();
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateSplitNode(
      const BWSplitNode<KeyType, KeyComparator> *node,
      std::vector<KeyType> *keys,
      std::vector<PID> *children,
      PID *right) const {
      myassert(node->IfInnerNode());
      myassert(keys->size()+1==children->size());
      const KeyType &key = node->GetSplitKey();
      auto position = std::lower_bound(keys->begin(), keys->end(), key, key_comparator_);
      // check for both existence and uniqueness
      myassert(position!=keys->begin()&&
             position!=keys->end()&&
             key_equality_checker_(key, *position)&&
             (position+1==keys->end()||!key_equality_checker_(key, *(position+1)))&&
             (position==keys->begin()||!key_equality_checker_(key, *(position-1))));
      auto dist = std::distance(keys->begin(), position);
      keys->erase(position, keys->end());
      children->erase(children->begin()+dist+1, children->end());
      if(right!=nullptr)
        *right = node->GetSplitTo();
    }

    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    void
    BWTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker, ValueComparator, ValueEqualityChecker, Duplicate>::
    ConsolidateSplitEntryNode(
            const BWSplitEntryNode<KeyType, KeyComparator> *node,
            std::vector<KeyType> *keys,
            std::vector<PID> *children) const {
      myassert(node->IfInnerNode());
      myassert(keys->size()+1==children->size());
      const KeyType &to_low_key = node->GetToLowKey();
      if(node->HasToHighKey()) {
        const KeyType &to_high_key = node->GetToHighKey();
        const PID &to = node->GetTo();
        auto position = std::lower_bound(keys->begin(), keys->end(), to_high_key, key_comparator_);
        // check for both existence and uniqueness
        myassert(position!=keys->end()&&
               key_equality_checker_(to_high_key, *position)&&
               (position+1==keys->end()||!key_equality_checker_(to_high_key, *(position+1)))&&
               (position==keys->begin()||!key_equality_checker_(to_high_key, *(position-1))));
        auto dist = std::distance(keys->begin(), position);
        keys->insert(position, to_low_key);
        children->insert(children->begin()+dist+1, to);
      }
      else {
        myassert(keys->empty()||key_comparator_(keys->back(), to_low_key));
        const PID &to = node->GetTo();
        keys->push_back(to_low_key);
        children->push_back(to);
      }
    }


    template<typename KeyType, typename KeyComparator>
    void
    BWInnerNode<KeyType, KeyComparator>::
    Print(const PIDTable<KeyType, KeyComparator> &, int indent) const {
      std::string s;
      for (size_t i = 0; i < indent; i++) {
        s += "\t";
      }
      LOG_DEBUG("%sInnerNode: size;: %lu", s.c_str(), keys_.size());
    }














// Explicit template instantiations

    template
    class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
            IntsEqualityChecker<1>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
      IntsEqualityChecker<2>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
      IntsEqualityChecker<3>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
      IntsEqualityChecker<4>, ItemPointerComparator, ItemPointerEqualityChecker, true>;


    template
    class BWTree<IntsKey<1>, ItemPointer, IntsComparator<1>,
      IntsEqualityChecker<1>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<IntsKey<2>, ItemPointer, IntsComparator<2>,
      IntsEqualityChecker<2>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<IntsKey<3>, ItemPointer, IntsComparator<3>,
      IntsEqualityChecker<3>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<IntsKey<4>, ItemPointer, IntsComparator<4>,
      IntsEqualityChecker<4>, ItemPointerComparator, ItemPointerEqualityChecker, false>;


    //
    template
    class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
            GenericEqualityChecker<4>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
            GenericEqualityChecker<8>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
            GenericEqualityChecker<12>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
            GenericEqualityChecker<16>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
            GenericEqualityChecker<24>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
            GenericEqualityChecker<32>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
            GenericEqualityChecker<48>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
            GenericEqualityChecker<64>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
            GenericEqualityChecker<96>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
            GenericEqualityChecker<128>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
            GenericEqualityChecker<256>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
            GenericEqualityChecker<512>, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    template
    class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
            TupleKeyEqualityChecker, ItemPointerComparator, ItemPointerEqualityChecker, true>;

    //


    template
    class BWTree<GenericKey<4>, ItemPointer, GenericComparator<4>,
      GenericEqualityChecker<4>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<8>, ItemPointer, GenericComparator<8>,
      GenericEqualityChecker<8>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<12>, ItemPointer, GenericComparator<12>,
      GenericEqualityChecker<12>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<16>, ItemPointer, GenericComparator<16>,
      GenericEqualityChecker<16>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<24>, ItemPointer, GenericComparator<24>,
      GenericEqualityChecker<24>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<32>, ItemPointer, GenericComparator<32>,
      GenericEqualityChecker<32>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<48>, ItemPointer, GenericComparator<48>,
      GenericEqualityChecker<48>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<64>, ItemPointer, GenericComparator<64>,
      GenericEqualityChecker<64>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<96>, ItemPointer, GenericComparator<96>,
      GenericEqualityChecker<96>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<128>, ItemPointer, GenericComparator<128>,
      GenericEqualityChecker<128>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<256>, ItemPointer, GenericComparator<256>,
      GenericEqualityChecker<256>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<GenericKey<512>, ItemPointer, GenericComparator<512>,
      GenericEqualityChecker<512>, ItemPointerComparator, ItemPointerEqualityChecker, false>;

    template
    class BWTree<TupleKey, ItemPointer, TupleKeyComparator,
      TupleKeyEqualityChecker, ItemPointerComparator, ItemPointerEqualityChecker, false>;



  }  // End index namespace
}  // End peloton namespace
