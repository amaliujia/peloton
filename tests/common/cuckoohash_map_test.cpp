#include <unordered_set>
#include <stdlib.h>
#include <time.h>
#include <random>
#include <chrono>
#include <cstdint>
#include <limits>

#include "harness.h"
#include "backend/common/logger.h"
#include "backend/common/cuckoohash_map.h"
#include "backend/common/types.h"

#include <boost/functional/hash.hpp>

namespace peloton {
namespace test {

class CUCKOOTest : public PelotonTest {};

typedef std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>> value_type;

typedef cuckoohash_map<std::string, value_type *> HashMapType;



TEST_F(CUCKOOTest, BasicTest) {
  HashMapType cuckoo_map;
  
  if (cuckoo_map.contains("a")) {
    cuckoo_map.find("a")->insert(std::make_pair(12,12));
  } else {
    cuckoo_map.insert("a", new value_type());
    cuckoo_map.find("a")->insert(std::make_pair(12,12));
  }
  EXPECT_EQ(cuckoo_map.size(), 1);
}

void Insert(HashMapType *cuckoo_map) {
  std::mt19937_64 gen(
      std::chrono::system_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<size_t> dist(
      std::numeric_limits<size_t>::min(),
                          std::numeric_limits<size_t>::max());

  for (int i = 0; i < 30; i++) {
    size_t lv = dist(gen) + i;
    oid_t tv = dist(gen) + i + 1;
    bool ok = cuckoo_map->update_fn(std::to_string(i), [&] (value_type *inner) {
      inner->insert(std::make_pair(lv, tv));
    });
    
    if (!ok) {
      cuckoo_map->upsert(std::to_string(i), [&](value_type *inner) {
        // It is possbile this insert would succeed.
        // I won't check since I am using unordered_set, even insert succeed,
        // another won't hurt.
        inner->insert(std::make_pair(lv, tv));
      }, new value_type());

      bool ok = cuckoo_map->update_fn(std::to_string(i), [&] (value_type *inner) {
        inner->insert(std::make_pair(lv, tv));
      });

      // There is no way second update fail.
      EXPECT_EQ(ok, true);
    }
  }
}

TEST_F(CUCKOOTest, MultiThreadedTest) {
  HashMapType cuckoo_map;
  size_t numThreads = 5;
  int iter_num = 30;

  LaunchParallelTest(numThreads, Insert, &cuckoo_map);
  EXPECT_EQ(cuckoo_map.size(), iter_num);
 
  for (int i = 0; i < iter_num; i++) {
    EXPECT_EQ(cuckoo_map.contains(std::to_string(i)), true); 
    EXPECT_EQ(cuckoo_map.find(std::to_string(i))->size(), numThreads); 
  }

  // delete pointers
  auto lt = cuckoo_map.lock_table();
  for (const auto& it : lt) {
      delete it.second;
  }
}

}
}
