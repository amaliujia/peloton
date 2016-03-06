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
#include "backend/common/value.h"
#include "backend/index/item_pointer_comparator.h"
#include "backend/index/item_pointer_equality_checker.h"

#include <mutex>
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
    constexpr int max_chain_len = 4;
    constexpr int max_node_size = 3;
    constexpr int min_node_size = max_node_size/2; // 3 / 2 = 1
    enum NodeType {
      NInner = 0,
      NLeaf = 1,
      NInsert = 2,
      NDelete = 3,
      NSplit = 4,
      NSplitEntry = 5,
      NMerge = 6,
      NRemove = 7,
      NMergeEntry = 8
    };

    class PIDTable;

    class BWNode {
    public:
      static const BWNode *GenerateRandomNodeChain(int length);
      static const BWNode *GenerateRandomNode(NodeType type, const BWNode *next);

      virtual NodeType GetType() const = 0;

      virtual const BWNode *GetNext() const = 0;

      inline const VersionNumber &GetVersionNumber() const { return version_number_; }

      virtual const PID &GetLeft() const = 0;

      virtual const PID &GetRight() const = 0;

      virtual void Print(PIDTable & , int indent) const = 0;

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

      BWNode(const size_type &slot_usage, const size_type &chain_length, bool if_leaf,
             const VersionNumber &version_number):
              slot_usage_(slot_usage), chain_length_(chain_length), if_leaf_(if_leaf),
              version_number_(version_number) { }

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
      inline const PID &GetLeft() const {
        return left_;
      }

      inline const PID &GetRight() const {
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
      BWInnerNode(const std::vector<KeyType> &keys, const std::vector<PID> &children, const PID &left, const PID &right,
                  const VersionNumber &version_number):
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

      //TODO what is this for?
      inline const std::vector<PID> &GetPIDs() const {
        return GetChildren();
      }

      void Print(PIDTable&, int indent) const;

    protected:
      const std::vector<KeyType> keys_;
      const std::vector<PID> children_;
    };

    template<typename KeyType, typename ValueType>
    class BWLeafNode: public BWNormalNode {
    public:
      // keys and values should not be used anymore
      BWLeafNode(const std::vector<KeyType> &keys, const std::vector<ValueType> &values, const PID &left,
                 const PID &right, const VersionNumber &version_number):
              BWNormalNode(keys.size(), 0, true, left, right, version_number),
              keys_(keys),
              values_(values) { }

      BWLeafNode(const std::vector<KeyType> &keys, const std::vector<ValueType> &values, const PID &left,
                 const PID &right):
              BWLeafNode(keys, values, left, right, std::numeric_limits<VersionNumber>::min()) { }

      inline const std::vector<KeyType> &GetKeys() const { return keys_; }

      inline const std::vector<ValueType> &GetValues() const { return values_; }

      inline NodeType GetType() const {
        return NLeaf;
      }

      void Print(PIDTable &, int indent) const {
        std::string s;
        for (size_t i = 0; i < indent; i++) {
          s += "\t";
        }
        LOG_INFO("%sLeafNode, left %lu, right %lu, size %lu", s.c_str(), left_, right_, keys_.size());
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
        assert("BWDelta::GetLeft()"&&0);
        return next_->GetLeft();
      }

      const PID &GetRight() const {
        assert("BWDelta::GetRight()"&&0);
        return next_->GetRight();
      }

    protected:
      const BWNode *next_;

      BWDeltaNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next,
                  const VersionNumber &version_number):
              BWNode(slot_usage, chain_length, next->IfLeafNode(), version_number), next_(next) { }
    };

    template<typename KeyType, typename ValueType>
    class BWInsertNode: public BWDeltaNode {
    public:
      BWInsertNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key,
                   const ValueType &value):
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
      void Print(PIDTable& pid_table, int indent) const {
        std::string s;
        for (size_t i = 0; i < indent; i++) {
          s += "\t";
        }
        LOG_INFO("%sInsertNode", s.c_str());
        next_->Print(pid_table, indent);
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

      BWDeleteNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key,
                   const ValueType &value):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), key_(key), value_(value) { }

      BWDeleteNode(const BWNode *next, const size_type &slot_usage, const KeyType &key, const ValueType &value):
              BWDeleteNode(slot_usage, next->GetChainLength()+1, next, key, value) { }

      inline const KeyType &GetKey() const {
        return key_;
      }

      inline const ValueType &GetValue() const {
        return value_;
      }

      void Print(PIDTable& pid_table, int indent) const {
        std::string s;
        for (size_t i = 0; i < indent; i++) {
          s += "\t";
        }
        LOG_INFO("%sDeleteNode", s.c_str());
        next_->Print(pid_table, indent);
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

      inline const PID &GetRightPID() const {
        return right_;
      }

      BWSplitNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next, const KeyType &key,
                  const PID &right):
              BWDeltaNode(slot_usage, chain_length, next, (VersionNumber) (next->GetVersionNumber()+1)), key_(key),
              right_(right) { }

      BWSplitNode(const size_type &slot_usage, const BWNode *next, const KeyType &key, const PID &right):
              BWSplitNode(slot_usage, next->GetChainLength()+1, next, key, right) { }

      void Print(PIDTable& pid_table, int indent) const {
        std::string s;
        for (size_t i = 0; i < indent; i++) {
          s += "\t";
        }
        LOG_INFO("%sSplitNode, right pid %lu", s.c_str(), (unsigned long)right_);
        next_->Print(pid_table, indent);
      }
      protected:
    protected:
      const KeyType key_;
      const PID right_;
    };

    template<typename KeyType>
    class BWSplitEntryNode: public BWDeltaNode {
    public:

      BWSplitEntryNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next,
                       const KeyType &low, const KeyType &high,
                       const PID &to):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), low_key_(low), high_key_(high),
              to_(to), has_high_key_(true) { }

      BWSplitEntryNode(const BWNode *next, const KeyType &low, const KeyType &high, PID to):
              BWSplitEntryNode(next->GetSlotUsage()+1, next->GetChainLength()+1, next, low, high, to) { }

      BWSplitEntryNode(const size_type &slot_usage, const size_type &chain_length, const BWNode *next,
                       const KeyType &low, const PID &to):
              BWDeltaNode(slot_usage, chain_length, next, next->GetVersionNumber()), low_key_(low), high_key_(low),
              to_(to), has_high_key_(false) { }

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

      void Print(PIDTable& pid_table, int indent) const {
        std::string s;
        for (size_t i = 0; i < indent; i++) {
          s += "\t";
        }
        LOG_INFO("%s SplitEntry, to pid %lu", s.c_str(), to_);

        next_->Print(pid_table, indent);
      }
      protected:

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

    typedef const BWNode *Address;

//    class PIDTable {
        /*
         * class storing the mapping table between a PID and its corresponding address
         * the mapping table is organized as a two-level array, like a virtual memory table.
         * the first level table is statically allocated, second level tables are allocated as needed
         * reclaimed PIDs are stored in a stack which will be given out first upon new allocations.
         * to achieve both latch-free and simple of implementation, this table can only reclaim PIDs but not space.
         */
/*    private:
        std::unordered_map<PID, Address> pid_table_;
        std::mutex lock_;
        long long id;
    public:
        // NULL for PID
        static const PID PID_NULL;
        //static constexpr PID PID_ROOT = 0;

        PIDTable() {
          id = 0;
        }

        ~PIDTable() {
          for (auto kv : pid_table_) {
            free_PID(kv.first);
          }
        }

        // get the address corresponding to the pid
        inline Address get(PID pid) {
          Address ret;
          lock_.lock();
          ret = pid_table_[pid];
          lock_.unlock();
          return ret;
        }

        // free up a new PID
        inline void free_PID(PID pid) {
          lock_.lock();
          delete(pid_table_[pid]);
          pid_table_.erase(pid_table_.find(pid));
          lock_.unlock();
        }

        // allocate a new PID, use argument "address" as its initial address
        inline PID allocate_PID(Address address) {
          PID result;
          lock_.lock();
          id++;
          result = id;
          pid_table_[id] = address;
          lock_.unlock();
          return result;
        }

        // atomically CAS the address associated with "pid"
        // it compares the content associated with "pid" with "original"
        // if they are same, change the content associated with pid to "to" and return true
        // otherwise return false directly
        bool bool_compare_and_swap(PID pid, const Address, const Address to) {
          lock_.lock();
          pid_table_[pid] = to;
          LOG_INFO("install %p to slot %lu", to, pid);
          lock_.unlock();
          return true;
        }
    };
*/

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

      PIDTable(): counter_(0), free_PIDs(256) {
        first_level_table_[0] = (Address *) malloc(sizeof(Address)*second_level_slots);
      }

      ~PIDTable() {
        PID counter = counter_;
        PID num_slots = ((counter&first_level_mask)>>second_level_bits)+1+1;
        LOG_DEBUG("counter=%lu, table[%lu][%lu], num_slots=%lu", counter, (counter&first_level_mask)>>second_level_bits, counter&second_level_mask, num_slots);
        for(PID i = 0; i<num_slots; ++i) {
          free(first_level_table_[i]);
        }
        assert(counter==counter_);
      }

      // get the address corresponding to the pid
      inline Address get(PID pid) const {

//        LOG_DEBUG("PIDTable::get(%lu)=table[%lu][%lu]=%p", pid,
//                  (pid&first_level_mask)>>second_level_bits,
//                  pid&second_level_mask,
//                  first_level_table_
//                  [(pid&first_level_mask)>>second_level_bits]
//                  [pid&second_level_mask]);
        return first_level_table_
        [(pid&first_level_mask)>>second_level_bits]
        [pid&second_level_mask];
      }

      // free up a new PID
      inline void free_PID(PID pid) {
//        LOG_DEBUG("PIDTable::free(%lu)", pid);
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
//        LOG_DEBUG("PIDTable::allocatePID(%p)=%lu", address, result);
        return result;
      }

      // atomically CAS the address associated with "pid"
      // it compares the content associated with "pid" with "original"
      // if they are same, change the content associated with pid to "to" and return true
      // otherwise return false directly
      bool bool_compare_and_swap(PID pid, const Address original, const Address to) {
        int row = (int) ((pid&first_level_mask)>>second_level_bits);
        int col = (int) (pid&second_level_mask);
        return __sync_bool_compare_and_swap(
                &first_level_table_[row][col], original, to);
      }

    private:
      Address *first_level_table_[first_level_slots] = {nullptr};
      CounterType counter_;
      boost::lockfree::stack<PID> free_PIDs;

      PIDTable(const PIDTable &) = delete;

      PIDTable(PIDTable &&) = delete;

      PIDTable &operator=(const PIDTable &) = delete;

      PIDTable &operator=(PIDTable &&) = delete;

      // allow GC to free PIDs
      friend class PIDNode;

      // no reclaimed PIDs available, need to allocate a new PID
      inline PID allocate_new_PID(Address) {
        PID pid = counter_++;
        if(is_first_PID(pid)) {
          LOG_DEBUG("%lu table[%lu][%lu] is first pid", pid, (pid&first_level_mask)>>second_level_bits, pid&second_level_mask);
          Address *table = (Address *) malloc(sizeof(Address)*second_level_slots);
          first_level_table_[((pid&first_level_mask)>>second_level_bits)+1] = table;
        }
        return pid;
      }

      // set the content associated with "pid" to "address"
      // only used upon "pid"'s allocation
      inline void set(PID pid, Address address) {
        int row = (int) ((pid&first_level_mask)>>second_level_bits);
        int col = (int) (pid&second_level_mask);
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
      PID root_;
      PIDTable pid_table_;
    public :
      BWTree(IndexMetadata *indexMetadata): comparator_(indexMetadata), key_equality_checker_(indexMetadata) {
        // a BWtree has at least two nodes residing at two levels.
        // at initialization, one root node pointing to one leaf node
        const BWNode *first_leaf_node;
        if(!Duplicate)
          first_leaf_node = new BWLeafNode<KeyType, ValueType>(std::vector<KeyType>(), std::vector<ValueType>(),
                                                               PIDTable::PID_NULL, PIDTable::PID_NULL);
        else
          first_leaf_node = new BWLeafNode<KeyType, std::vector<ValueType>>(std::vector<KeyType>(),
                                                                            std::vector<std::vector<ValueType>>(),
                                                                            PIDTable::PID_NULL, PIDTable::PID_NULL);
        PID first_leaf = pid_table_.allocate_PID(first_leaf_node);
        LOG_INFO("PID of first_leaf is %lu. address:%p", (unsigned long)first_leaf, first_leaf_node);
        const BWNode *root_node = new BWInnerNode<KeyType>(std::vector<KeyType>(), {first_leaf}, PIDTable::PID_NULL,
                                                           PIDTable::PID_NULL,
                                                           std::numeric_limits<VersionNumber>::min());
        root_ = pid_table_.allocate_PID(root_node);
        LOG_INFO("PID of root is %lu. address:%p", (unsigned long)root_, root_node);
      }

      void PrintSelf(PID pid, const BWNode *node, int indent) {
        node->Print(pid_table_, indent);
        if(node->IfInnerNode()) {
          std::vector<KeyType> keys;
          std::vector<PID> children;
          PID left, right;
          CreateInnerNodeView(node, keys, children, left, right);
          std::string s;
          for (size_t i = 0; i < indent; i++) {
            s += "\t";
          }
          LOG_DEBUG("%sInner summary: size=%lu, self=%lu, left=%lu, right=%lu", s.c_str(), keys.size(), pid, left, right);
          for(int i=0; i<children.size(); ++i)
            PrintSelf(children[i], pid_table_.get(children[i]), indent+2);
        }
        else {
          assert(!Duplicate);
          std::vector<KeyType> keys;
          std::vector<ValueType> values;
          PID left, right;
          CreateLeafNodeView(node, keys, values, left, right);
          std::string s;
          for (size_t i = 0; i < indent; i++) {
            s += "\t";
          }
          LOG_DEBUG("%sLeaf summary: size=%lu, self=%lu, left=%lu, right=%lu", s.c_str(), keys.size(), pid, left, right);
        }
      }

      inline bool InsertEntry(const KeyType &key, const ValueType &value) {
        std::vector<PID> path = {root_};
        std::vector<VersionNumber> version_number = {pid_table_.get(root_)->GetVersionNumber()};
        bool result = InsertEntryUtil(key, value, path, version_number);
        LOG_DEBUG("++++++++++++++++++++++++++++++++++++++++++++++++ print begin  ++++++++++++++++++++++++++++++++++++++");
        LOG_DEBUG(" ");
        LOG_DEBUG(" ");
        LOG_DEBUG(" ");
        PrintSelf(root_, pid_table_.get(root_), 0);
        LOG_DEBUG(" ");
        LOG_DEBUG(" ");
        LOG_DEBUG(" ");
        LOG_DEBUG("------------------------------------------------ print end  --------------------------------------");

        return result;
      }

      inline bool DeleteEntry(const KeyType &key, const ValueType &value) {
        std::vector<PID> path = {root_};
        std::vector<VersionNumber> version_number = {pid_table_.get(root_)->GetVersionNumber()};
        return DeleteEntryUtil(key, value, path, version_number);
      }

      void ScanKey(KeyType key, std::vector<ValueType> &ret) {
        LOG_DEBUG("Entering ScanKey");
        ScanKeyUtil(root_, key, ret);
        LOG_DEBUG("ret.size():%lu", ret.size());
        LOG_DEBUG("Leaving ScanKey");
      }

      void ScanAllKeys(std::vector<ValueType> &ret) {
        PID next_pid = root_;
        const BWNode *node_ptr = pid_table_.get(next_pid);

        // Print tree structure
        PrintSelf(root_, pid_table_.get(root_), 0);

        while(!node_ptr->IfLeafNode()) {
          LOG_DEBUG("ScanAllKeys, pass node PID=%lu", (unsigned long)next_pid);
          std::vector<KeyType> keys;
          std::vector<PID> children;
          PID left, right;
          CreateInnerNodeView(node_ptr, keys, children, left, right);
          assert(children.size() != 0);
          next_pid = children[0];
          node_ptr = pid_table_.get(next_pid);
        }

        // reach first leaf node
        // Ready to scan
        LOG_DEBUG("ScanAllKeys, first leaf node PID=%lu", (unsigned long)next_pid);
        while(node_ptr != NULL) {
          PID left, right;
          if(!Duplicate) {
            std::vector<KeyType> keys;
            std::vector<ValueType> values;
            CreateLeafNodeView(node_ptr, keys, values, left, right);
            const unsigned long temp = ret.size();
            ret.insert(ret.end(), values.begin(), values.end());
            LOG_DEBUG("ScanAllKeys no dup, PID=%lu, node size=%lu, values size=%lu, ret size before=%lu, ret size after=%lu",
                      (unsigned long)next_pid, node_ptr->GetSlotUsage(), values.size(), temp, ret.size());
          }
          else {
            std::vector<KeyType> keys;
            std::vector<std::vector<ValueType>> values;
            CreateLeafNodeView(node_ptr, keys, values, left, right);
            LOG_DEBUG("ScanAllKeys dup, PID=%lu, node size=%lu, values size=%lu, ret size before=%lu",
                      (unsigned long)next_pid, node_ptr->GetSlotUsage(), values.size(), ret.size());
            for(const auto &v : values) {
              ret.insert(ret.end(), v.begin(), v.end());
              LOG_DEBUG("ScanAllKeys dup, PID=%lu, values size of this key=%lu", (unsigned long)next_pid, v.size());
            }
            LOG_DEBUG("ScanAllKeys dup, PID=%lu, ret size after=%lu", (unsigned long)next_pid, ret.size());
          }

          // Assume node_ptr->GetRight() returns the most recent split node delta's right (aka new page
          // in that split), if any.
          next_pid = right;
          if (right != PIDTable::PID_NULL){
//          if(node_ptr->GetRight()!=null_) {
//            node_ptr = pid_table_.get(node_ptr->GetRight());
            node_ptr = pid_table_.get(right);
          }
          else {
            LOG_DEBUG("ScanAllKeys, reach end");
            node_ptr = NULL;
          }
        }
      }

    private:
      bool CheckStatus(const BWNode *node, const KeyType &key, std::vector<PID> &path,
                       std::vector<VersionNumber> &version_number);

      bool InsertEntryUtil(const KeyType &key, const ValueType &value, std::vector<PID> &path,
                           std::vector<VersionNumber> &version_number);

      bool DeleteEntryUtil(const KeyType &key, const ValueType &value, std::vector<PID> &path,
                           std::vector<VersionNumber> &version_number);

      const BWNode *Split(const BWNode *node_ptr, const PID &node_pid);

      const BWNode *SplitInnerNode(const BWNode *leaf_node, const PID &node_pid);

      const BWNode *SplitLeafNode(const BWNode *leaf_node, const PID &node_pid);

      bool InsertSplitEntry(const BWNode *top, std::vector<PID> &path, std::vector<VersionNumber> &version_number);

      bool ExistKey(const BWNode *node_ptr, const KeyType &key);

      void CreateLeafNodeView(const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<ValueType> &values,
                              PID &left, PID &right);

      void CreateLeafNodeView(const BWNode *node_chain, std::vector<KeyType> &keys,
                              std::vector<std::vector<ValueType>> &values, PID &left, PID &right);

      void CreateInnerNodeView(const BWNode *node_chain, std::vector<KeyType> &keys, std::vector<PID> &children,
                               PID &left, PID &right);

      bool Consolidate(PID cur, const BWNode *node_ptr);

      const BWNode *ConstructConsolidatedInnerNode(const BWNode *node_chain);

      const BWNode *ConstructConsolidatedLeafNode(const BWNode *node_chain);

      void ConstructConsolidatedLeafNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                 std::vector<ValueType> &values, PID &left, PID &right);

      void ConstructConsolidatedLeafNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                 std::vector<std::vector<ValueType>> &values, PID &left, PID &right);

      void ConstructConsolidatedInnerNodeInternal(const BWNode *node_chain, std::vector<KeyType> &keys,
                                                  std::vector<PID> &children, PID &left, PID &right);

      void ConsolidateInsertNode(const BWInsertNode<KeyType, ValueType> *node, std::vector<KeyType> &keys,
                                 std::vector<ValueType> &values);

      void ConsolidateInsertNode(const BWInsertNode<KeyType, ValueType> *node, std::vector<KeyType> &keys,
                                 std::vector<std::vector<ValueType>> &values);

      void ConsolidateDeleteNode(const BWDeleteNode<KeyType, ValueType> *node, std::vector<KeyType> &keys,
                                 std::vector<ValueType> &values);

      void ConsolidateDeleteNode(const BWDeleteNode<KeyType, ValueType> *node, std::vector<KeyType> &keys,
                                 std::vector<std::vector<ValueType>> &values);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys,
                                std::vector<ValueType> &values,
                                PID &right);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys,
                                std::vector<std::vector<ValueType>> &values,
                                PID &right);

      void ConsolidateSplitNode(const BWSplitNode<KeyType> *node, std::vector<KeyType> &keys,
                                std::vector<PID> &children,
                                PID &right);

      void ConsolidateSplitEntryNode(const BWSplitEntryNode<KeyType> *node, std::vector<KeyType> &keys,
                                     std::vector<PID> &children);

      void ScanKeyUtil(PID cur, const KeyType &key, std::vector<ValueType> &ret) {
        LOG_DEBUG("Entering ScanKeyUtil");
        const BWNode *node_ptr = pid_table_.get(cur);

        if(node_ptr->IfLeafNode()) {
          LOG_DEBUG("ScanKeyUtil reach leaf node PID=%lu", (unsigned long)cur);
          if(!Duplicate) {
            std::vector<KeyType> keys;
            std::vector<ValueType> values;
            PID left, right;
            CreateLeafNodeView(node_ptr, keys, values, left, right);

            // unique
            auto position = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
            if(position!=keys.end()&&key_equality_checker_(key, *position)) {
              auto dist = std::distance(keys.begin(), position);
              ret.push_back(values[dist]);
            }
          }
          else {
            std::vector<KeyType> keys;
            std::vector<std::vector<ValueType>> values;
            PID left, right;
            CreateLeafNodeView(node_ptr, keys, values, left, right);
            auto position = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
            if(position!=keys.end()&&key_equality_checker_(key, *position)) {
              auto dist = std::distance(keys.begin(), position);
              ret.insert(ret.end(), values[dist].begin(), values[dist].end());
            }
          }
        }
        else {
          LOG_DEBUG("ScanKeyUtil reach inner node PID=%lu", (unsigned long)cur);
          std::vector<KeyType> keys;
          std::vector<PID> children;
          PID left, right;
          CreateInnerNodeView(node_ptr, keys, children, left, right);
          /*
           *  An iterator to the lower bound of val in the range.
           *  If all the element in the range compare less than val, the function returns last.
           *  I assume if the element is the smallest in the range, return begin
           *  Otherwise returnl lowkey of keyspace [lowkey, highkey) in which current key is.
           */
          //TODO already done
          //TODO evil
          /*auto position = std::lower_bound(keys.begin(), keys.end(), key, comparator_);
          if(position==keys.begin()) {
            ScanKeyUtil(children[0], key, ret);
          }
          else {
            auto dist = std::distance(keys.begin(), position);
            ScanKeyUtil(children[dist], key, ret);
          }*/
          auto position = std::upper_bound(keys.begin(), keys.end(), key, comparator_);
          auto dist = std::distance(keys.begin(), position);
          ScanKeyUtil(children[dist], key, ret);
        }
      }

      bool DeltaInsert(const PID &cur, const BWNode *node_ptr, const KeyType &key, const ValueType &value,
                       bool need_expand);

      bool DeltaDelete(const PID &cur, const BWNode *node_ptr, const KeyType &key, const ValueType &value,
                       bool &exist_value);
    };
  }  // End index namespace
}  // End peloton namespace
