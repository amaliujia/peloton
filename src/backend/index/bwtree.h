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
//
#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"
//#include "backend/index/pid_table.h"
#include "backend/common/value.h"
#include "backend/index/item_pointer_comparator.h"
#include "backend/index/item_pointer_equality_checker.h"


#include <boost/lockfree/stack.hpp>



#include <cstdint>
#include <vector>
#include <map>
#include <set>
#include <chrono>

namespace peloton {
  namespace index {


    typedef std::uint_fast32_t PID;




    typedef size_t size_type;
    typedef uint_fast8_t VersionNumber;
    constexpr int max_chain_len = 8;
    constexpr int max_node_size = 20;
    constexpr int min_node_size = max_node_size<<1;
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
      NMergeEntry,
      Unknown
    };

    class BWNode {
    public:
      virtual NodeType GetType() const = 0;

      virtual const BWNode *GetNext() const = 0;

      inline const VersionNumber &GetVersionNumber() const { return version_number_; }

      virtual const PID &GetLeft() const = 0;

      virtual const PID &GetRight() const = 0;

      inline bool IfLeafNode() const {
        return if_leaf_;
      }

      inline bool IfInnerNode() const {
        return !if_leaf_;
      }

      inline bool IfChainTooLong() const {
        return chain_length_>max_chain_len;
      }

      inline bool IfOverflow() const {
        return slot_usage_>max_node_size;
      }

      inline bool IfUnderflow() const {
        return slot_usage_<min_node_size;
      }

      inline size_type GetSlotUsage() const { return slot_usage_; }

      inline size_type GetChainLength() const { return chain_length_; }

      virtual ~BWNode() { };

    protected:
      const size_type slot_usage_;
      const size_type chain_length_;
      const bool if_leaf_;
      const VersionNumber version_number_;
      BWNode(const size_type &slot_usage, const size_type &chain_length, bool if_leaf, const VersionNumber &version_number):
              slot_usage_(slot_usage), chain_length_(chain_length), if_leaf_(if_leaf), version_number_(version_number) { }

    private:
      BWNode(const BWNode &) = delete;

      BWNode &operator=(const BWNode &) = delete;
    };

    class BWNormalNode: public BWNode {
    public:
      virtual NodeType GetType() const = 0;

      // normal nodes don't have a next
      const BWNode *GetNext() const {
        return nullptr;
      };

      // left and right are not used or updated now
      inline const PID& GetLeft() const {
        return left_;
      }

      inline const PID& GetRight() const {
        return right_;
      }

    protected:
      BWNormalNode(const size_type &slot_usage, const size_type &chain_length, bool if_leaf, const PID &left,
                   const PID &right, const VersionNumber &version_number):
              BWNode(slot_usage, chain_length, if_leaf, version_number),
              left_(left),
              right_(right) { }
      const PID left_, right_;
    };

    template<typename KeyType>
    class BWInnerNode: public BWNormalNode {
    public:
      BWInnerNode(const std::vector<KeyType> &keys, const std::vector<PID> &children, const PID &left, const PID &right, const VersionNumber &version_number):
              BWNormalNode(keys.size(), 0, false, left, right, version_number),
              keys_(keys),
              children_(children) { }

      inline NodeType GetType() const {
        return NInner;
      }

      inline const std::vector<KeyType> &GetKeys() const {
        return keys_;
      }

      inline const std::vector<PID> &GetChildren() const {
        return children_;
      }
      inline const std::vector<PID> &GetPIDs() const {
        return GetChildren();
      }

    protected:
      const std::vector<KeyType> keys_;
      const std::vector<PID> children_;
    };

    template<typename KeyType, typename ValueType>
    class BWLeafNode: public BWNormalNode {
    public:
      // keys and values should not be used anymore
      BWLeafNode(const std::vector<KeyType> &keys, const std::vector<ValueType> &values, const PID &left, const PID &right, const VersionNumber &version_number):
              BWNormalNode(keys.size(), 0, true, left, right, version_number),
              keys_(keys),
              values_(values) { }
      BWLeafNode(const std::vector<KeyType> &keys, const std::vector<ValueType> &values, const PID &left, const PID &right):
              BWLeafNode(keys, values, left, right, std::numeric_limits<VersionNumber>::min()) { }

      inline const std::vector<KeyType> &GetKeys() const { return keys_; }

      inline const std::vector<ValueType> &GetValues() const { return values_; }
      inline NodeType GetType() const {
        return NLeaf;
      }

    protected:
      const std::vector<KeyType> keys_;
      const std::vector<ValueType> values_;
    };

    class BWDeltaNode: public BWNode {
    public:
      inline virtual NodeType GetType() const = 0;

      inline const BWNode *GetNext() const {
        return next_;
      }

      const PID &GetLeft() const {
        return next_->GetLeft();
      }

      const PID &GetRight() const {
        return next_->GetRight();
      }

      protected:
      const BWNode * next_;

      //TODO slot_usage, chain_length, next, if_leaf
      BWDeltaNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const VersionNumber &version_number):
              BWNode(slot_usage, chain_length, next->IfLeafNode(), version_number), next_(next) { }
    };

    template<typename KeyType, typename ValueType>
    class BWInsertNode: public BWDeltaNode {
    public:
      BWInsertNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key, const ValueType &value):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), key_(key), value_(value) { }
      BWInsertNode(const BWNode *next, const size_type &slot_usage, const KeyType &key, const ValueType &value):
              BWInsertNode(slot_usage, next->GetChainLength()+1, next, key, value) { }

      inline NodeType GetType() const {
        return NInsert;
      }

      inline const KeyType &GetKey() const {
        return key_;
      }

      inline const ValueType &GetValue() const {
        return value_;
      }

    protected:
      const KeyType key_;
      const ValueType value_;
    };

    template<typename KeyType, typename ValueType>
    class BWDeleteNode: public BWDeltaNode {
    public:
      inline NodeType GetType() const {
        return NDelete;
      }

      BWDeleteNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key, const ValueType &value):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), key_(key), value_(value) { }
      BWDeleteNode(const BWNode *next, const size_type &slot_usage, const KeyType &key, const ValueType &value):
              BWDeleteNode(slot_usage, next->GetChainLength()+1, next, key, value) { }

      inline const KeyType &GetKey() const {
        return key_;
      }

      inline const ValueType &GetValue() const {
        return value_;
      }

    protected:
      const KeyType key_;
      const ValueType value_;
    };

    template<typename KeyType>
    class BWSplitNode: public BWDeltaNode {
    public:
      inline NodeType GetType() const {
        return NSplit;
      }

      inline const KeyType &GetSplitKey() const {
        return key_;
      }

      inline PID GetRightPID() const {
        return right_;
      }

      BWSplitNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key, const PID &right):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()+1), key_(key), right_(right) { }
      BWSplitNode(const size_type &slot_usage, const BWNode *next, const KeyType &key, const PID &right):
              BWSplitNode(slot_usage, next->GetChainLength()+1, next, key, right) { }

    protected:
      const KeyType key_;
      const PID right_;
    };

    template<typename KeyType>
    class BWSplitEntryNode: public BWDeltaNode {
    public:

      BWSplitEntryNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &low, const KeyType &high,
                       const PID &to):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), low_key_(low), high_key_(high), to_(to), has_high_key_(true) { }

      BWSplitEntryNode(const BWNode *next, const KeyType &low, const KeyType &high, PID to):
              BWSplitEntryNode(next->GetSlotUsage()+1, next->GetChainLength()+1, next, low, high, to) { }

      BWSplitEntryNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &low, const PID &to):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), low_key_(low), high_key_(low), to_(to), has_high_key_(false) { }

      BWSplitEntryNode(const BWNode *next, const KeyType &low, PID to):
              BWSplitEntryNode(next->GetSlotUsage()+1, next->GetChainLength()+1, next, low, to) { }

      inline NodeType GetType() const {
        return NSplitEntry;
      }

      inline const KeyType &GetLowKey() const {
        return low_key_;
      }

      inline const KeyType &GetHightKey() const {
        return high_key_;
      }

      inline bool HasHighKey() const {
        return has_high_key_;
      }

      inline const PID &GetNextPID() const {
        return to_;
      }

    protected:
      const KeyType low_key_, high_key_;
      const PID to_;
      const bool has_high_key_;
    };

    template<typename KeyType>
    class BWRemoveNode: public BWDeltaNode {
    public:
      BWRemoveNode(size_type slot_usage, size_type chain_length, const BWNode *next):
              BWDeltaNode(slot_usage, chain_length, next, version_number_) { }

      inline NodeType GetType() const {
        return NRemove;
      }
    };

    template<typename KeyType>
    class BWMergeNode: public BWDeltaNode {
    public:
      BWMergeNode(size_type slot_usage, size_type chain_length, const BWNode *next, const BWNode *right):
              BWDeltaNode(slot_usage, chain_length, next, version_number_), right_(right) { }

      inline NodeType GetType() const {
        return NMerge;
      }

    protected:
      const BWNode *right_;
    };

    // TODO
    template<typename KeyType>
    class MergeEntryNode: public BWDeltaNode {
    public:
      MergeEntryNode(size_type slot_usage, size_type chain_length, BWNode *next):
              BWDeltaNode(slot_usage, chain_length, next, version_number_) { }

      inline NodeType GetType() const {
        return NMergeEntry;
      }
    };

    enum OpType {
      OInsert,
      ODelete,
      OUnknown
    };

    class Operation {
    public:
      virtual OpType GetOpType() const = 0;

    };

    class InsertOperation: public Operation {
    public:
      OpType GetOpType() {
        return OInsert;
      }
    };

    class DeleteOperation: public Operation {
    public:
      OpType GetOpType() {
        return ODelete;
      }
    };

    // todo:





    typedef const BWNode * Address;
    class PIDTable {
      /*
       * class storing the mapping table between a PID and its corresponding address
       * the mapping table is organized as a two-level array, like a virtual memory table.
       * the first level table is statically allocated, second level tables are allocated as needed
       * reclaimed PIDs are stored in a stack which will be given out first upon new allocations.
       * to achieve both latch-free and simple of implementation, this table can only reclaim PIDs but not space.
       */
      public:
      typedef std::atomic<PID> CounterType;

      private:
      static constexpr unsigned int first_level_bits = 14;
      static constexpr unsigned int second_level_bits = 10;
      static constexpr PID first_level_mask = 0xFFFC00;
      static constexpr PID second_level_mask = 0x3FF;
      static constexpr unsigned int first_level_slots = 1<<first_level_bits;
      static constexpr unsigned int second_level_slots = 1<<second_level_bits;

      public:
      // NULL for PID
      static const PID PID_NULL;
      //static constexpr PID PID_ROOT = 0;

      PIDTable(): counter_(0) {
        first_level_table_[0] = (Address *)malloc(sizeof(Address)*second_level_slots);
      }

      ~PIDTable() {
        PID counter = counter_;
        for(PID i=0; i<counter; ++i) {
          free(first_level_table_[i]);
        }
        assert(counter==counter_);
      }

      // get the address corresponding to the pid
      inline Address get(PID pid) const {
        return first_level_table_
        [(pid&first_level_mask)>>second_level_bits]
        [pid&second_level_mask];
      }

      // free up a new PID
      inline void free_PID(PID pid) {
        free_PIDs.push(pid);
        // bool push_result = freePIDs_.push(pid);
        // assert(push_result);
      }

      // allocate a new PID, use argument "address" as its initial address
      inline PID allocate_PID(Address address) {
        PID result;
        if(!free_PIDs.pop(result))
          result = allocate_new_PID(address);
        set(result, address);
        return result;
      }

      // atomically CAS the address associated with "pid"
      // it compares the content associated with "pid" with "original"
      // if they are same, change the content associated with pid to "to" and return true
      // otherwise return false directly
      bool bool_compare_and_swap(PID pid, const Address original, const Address to) {
        int row = (int)((pid&first_level_mask)>>second_level_bits);
        int col = (int)(pid&second_level_mask);
        return __sync_bool_compare_and_swap(
          &first_level_table_[row][col], original, to);
      }

      private:
      Address *first_level_table_[first_level_slots] = {nullptr};
      CounterType counter_;
      boost::lockfree::stack <PID> free_PIDs;

      PIDTable(const PIDTable &) = delete;

      PIDTable(PIDTable &&) = delete;

      PIDTable &operator=(const PIDTable &) = delete;

      PIDTable &operator=(PIDTable &&) = delete;

      // allow GC to free PIDs
      friend class PIDNode;

      // no reclaimed PIDs available, need to allocate a new PID
      inline PID allocate_new_PID(Address ) {
        PID pid = counter_++;
        if(is_first_PID(pid)) {
          Address *table = (Address* )malloc(sizeof(Address)*second_level_slots);
          first_level_table_[((pid&first_level_mask)>>second_level_bits)+1] = table;
        }
        return pid;
      }

      // set the content associated with "pid" to "address"
      // only used upon "pid"'s allocation
      inline void set(PID pid, Address address) {
        int row = (int)((pid&first_level_mask)>>second_level_bits);
        int col = (int)(pid&second_level_mask);
        first_level_table_[row][col] = address;
      }

      inline static bool is_first_PID(PID pid) {
        return (pid&second_level_mask)==0;
      }
    };














































    template<typename KeyType, typename ValueType, class KeyComparator, class KeyEqualityChecker, class ValueComparator, class ValueEqualityChecker, bool Duplicate>
    class BWTree {
      KeyComparator comparator_;
      KeyEqualityChecker key_equality_checker_;
      ValueComparator value_comparator_;
      ValueEqualityChecker value_equality_checker_;
      PID root_, first_leaf_;
      PIDTable pid_table_;
    public :
      BWTree(IndexMetadata *indexMetadata): comparator_(indexMetadata), key_equality_checker_(indexMetadata) {
        const BWNode *first_leaf_node;
        if(!Duplicate)
          first_leaf_node = new BWLeafNode<KeyType, ValueType>(std::vector<KeyType>(), std::vector<ValueType>(),
                                                               PIDTable::PID_NULL, PIDTable::PID_NULL);
        else
          first_leaf_node = new BWLeafNode<KeyType, std::vector<ValueType>>(std::vector<KeyType>(), std::vector<std::vector<ValueType>>(),
                                                                            PIDTable::PID_NULL, PIDTable::PID_NULL);
        first_leaf_ = pid_table_.allocate_PID(first_leaf_node);

        const BWNode *root_node = new BWInnerNode<KeyType>(std::vector<KeyType>(), {first_leaf_}, PIDTable::PID_NULL,
                                                           PIDTable::PID_NULL, std::numeric_limits<VersionNumber>::min());
        root_ = pid_table_.allocate_PID(root_node);
      }

      bool InsertEntry(const KeyType &key, const ValueType &value) {
        std::vector<PID> path = {root_};
        std::vector<VersionNumber> version_number = {pid_table_.get(root_)->GetVersionNumber()};
        return InsertEntryUtil(key, value, path, version_number);
      }

      bool DeleteEntry(const KeyType &key, const ValueType &value) {
        std::vector<PID> path = {root_};
        std::vector<VersionNumber> version_number = {pid_table_.get(root_)->GetVersionNumber()};
        return DeleteEntryUtil(key, value, path, version_number);
      }
/*
      void ScanKey(KeyType key, std::vector<ValueType> &ret) {
        ScanKeyUtil(PIDTable::PID_ROOT, key, ret);
      }

      void ScanAllKeys(std::vector<ValueType> & ) {
        PIDTable & pidTable = PIDTable::get_table();
        const BWNode *node_ptr = pidTable.get(PIDTable::PID_ROOT);

        while (node_ptr->GetType() != NLeaf) {
          const BWInnerNode<KeyType> *cur_ptr = static_cast<const BWInnerNode<KeyType> *>(node_ptr);

          PID next_pid = cur_ptr->GetPIDs()[0];
          node_ptr = pidTable.get(next_pid);
        }

        // reach first leaf node

      }
*/
    private:
      bool CheckStatus(const BWNode *node, const KeyType &key, std::vector<PID> &path, std::vector<VersionNumber> &version_number);

      bool InsertEntryUtil(const KeyType &key, const ValueType &value, std::vector<PID> &path, std::vector<VersionNumber> &version_number);

      bool DeleteEntryUtil(const KeyType &key, const ValueType &value, std::vector<PID> &path, std::vector<VersionNumber> &version_number);
      
      const BWNode *Split(const BWNode *node_ptr, const PID &node_pid);

      const BWNode *SplitInnerNode(const BWNode *leaf_node, const PID &node_pid);

      const BWNode *SplitLeafNode(const BWNode *leaf_node, const PID &node_pid);

      bool InsertSplitEntry(const BWNode *top, std::vector<PID> &path, std::vector<VersionNumber> &version_number){
        assert(path.size()==version_number.size());
        assert(top->GetType()==NSplit);
        const BWSplitNode<KeyType> *split_ptr = static_cast<const BWSplitNode<KeyType> *>(top);
        const KeyType &low_key = split_ptr->GetSplitKey();
        const PID &right_pid = split_ptr->GetRightPID();

        if(path.size()>1) { // In this case, this is a normal second step split
          // get the parent
          PID pid = path[path.size()-2];
          while(true) {
            const BWNode *parent_node = pid_table_.get(pid);
            if(version_number[version_number.size()-2]!=parent_node->GetVersionNumber())
              return false;

            std::vector<KeyType> keys_view;
            std::vector<PID> children_view;
            PID left_view, right_view;
            CreateInnerNodeView(parent_node, keys_view, children_view, left_view, right_view);
            auto position = std::upper_bound(keys_view.cbegin(), keys_view.cend(), low_key, comparator_);

            // then check if this split entry has been inserted by others
            if(position!=keys_view.cbegin()&&key_equality_checker_(low_key, *(position-1)))
              return true;

            // try to insert the split entry
            const BWNode *split_entry;
            if(position!=keys_view.cend())
              split_entry = new BWSplitEntryNode<KeyType>(parent_node, low_key, *position, right_pid);
            else
              split_entry = new BWSplitEntryNode<KeyType>(parent_node, low_key, right_pid);
            if(pid_table_.bool_compare_and_swap(pid, parent_node, split_entry))
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
          BWNode *new_root = new BWInnerNode<KeyType>({}, {new_pid}, PIDTable::PID_NULL, PIDTable::PID_NULL, old_root_version+1);
          if(!pid_table_.bool_compare_and_swap(root_, old_root, new_root)) {
            delete new_root;
            pid_table_.free_PID(new_pid);
            return false;
          }
          return false;
          /*
          path.push_back(new_pid);
          version_number.insert(version_number.begin(), (VersionNumber)(old_root_version+1));
          return InsertSplitEntry(top, key, path, version_number);
           */
        }
      }

       bool ExistKey(const BWNode *node_ptr, const KeyType &key);

       void CreateLeafNodeView(const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values, PID &left, PID &right);

       void CreateLeafNodeView(const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<std::vector<ValueType>> &values, PID &left, PID &right);

       void CreateInnerNodeView(const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<PID> &children, PID &left, PID &right);

       bool Consolidate(PID cur, const BWNode *node_ptr);

       const BWNode *ConstructConsolidatedInnerNode(const BWNode *node_chain);

       const BWNode *ConstructConsolidatedLeafNode(const BWNode *node_chain);

       void ConstructConsolidatedLeafNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                 std::vector<ValueType> &values, PID &left, PID &right);
       void ConstructConsolidatedLeafNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                 std::vector<std::vector<ValueType>> &values, PID &left, PID &right);

      void ConstructConsolidatedInnerNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                  std::vector<PID> &children, PID &left, PID &right);

      void ConsolidateInsertNode(const BWInsertNode<KeyType, ValueType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values);
      void ConsolidateInsertNode(const BWInsertNode<KeyType, ValueType> *node, std::vector<KeyType> &keys, std::vector<std::vector<ValueType>> &values);

      void ConsolidateDeleteNode(const BWDeleteNode<KeyType, ValueType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values);
      void ConsolidateDeleteNode(const BWDeleteNode<KeyType, ValueType> *node, std::vector<KeyType> &keys, std::vector<std::vector<ValueType>> &values);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<ValueType> &values,
                                PID &right);
      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<std::vector<ValueType>> &values,
                                PID &right);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys, std::vector<PID> &children,
                                PID &right);

      void ConsolidateSplitEntryNode(const BWSplitEntryNode<KeyType> *node, std::vector<KeyType> &keys,
                                     std::vector<PID> &children);

      PID GetRightPID(const BWNode *node_ptr) {
        const BWNode *cur_ptr = node_ptr;
        while(cur_ptr->GetType()!=NLeaf||cur_ptr->GetType()!=NInner) {
          cur_ptr = cur_ptr->GetNext();
        }

        return cur_ptr->GetLeft();
      }

      /*
       *  In inner levels, the delta nodes should be SplitEntryNode, MergeEntryNode, SplitNode.
       *  For now, only handle SplitEntryNode and SplitNode.
       *
       *  This function has handle duplicate keys
       *
       *  TODO: Handle MergeEntryNode.
       */
/*
      void SplitInnerNodeUtil(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                              std::vector<KeyType> &keys,
                              std::vector<PID> &pids);
*/

      // TODO: handle duplicate keys
/*
      void SplitLeafNodeUtil(const BWNode *node_ptr, KeyType &split_key, PID &right_pid,
                             std::vector<KeyType> &keys,
                             std::vector<ValueType> &values);
*/


      bool DeltaInsert(const PID &cur, const BWNode *node_ptr, const KeyType &key, const ValueType &value, bool need_expand);
      bool DeltaDelete(const PID &cur, const BWNode *node_ptr, const KeyType &key, const ValueType &value, bool &exist_value);

      // TODO: handle MergeEntryNode
/*
      PID GetNextPID(PID cur, KeyType key) {
        const BWNode *cur_ptr = PIDTable::get_table().get(cur);

        while(cur_ptr->GetType()!=NInner) {
          if(cur_ptr->GetType()==NSplitEntry) {
            const BWSplitEntryNode<KeyType> *split_entry_ptr = static_cast<const BWSplitEntryNode<KeyType> *>(cur_ptr);

            if(!comparator_(key, split_entry_ptr->GetLowKey())&&comparator_(key, split_entry_ptr->GetHightKey())) {
              return split_entry_ptr->GetNextPID();
            }
          }
          else if(cur_ptr->GetType()==NMergeEntry) {

          }
          else {

          }

          cur_ptr = cur_ptr->GetNext();
        }

        //const BWInnerNode<KeyType> *inner_ptr = static_cast<const BWInnerNode<KeyType> *>(cur_ptr);
        // TODO: implement scan_key;

        return PIDTable::PID_ROOT;
      }
*/


//      void ScanKeyUtil(PID cur, KeyType key, std::vector<ValueType> &ret);


    };


    ////////////////////////////////////////////////////////////////



  }  // End index namespace
}  // End peloton namespace
