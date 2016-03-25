#include <unordered_set>
#include <stdlib.h>
#include <time.h>

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
  std::vector<std::string> vec;
  for (int i = 0; i < 30; i++) {
    vec.push_back(std::to_string(i));
  }
  srand (time(NULL));
  size_t iSecret = rand() % 1000000 + 1;
  for (int i = 0; i < 30; i++) {
    if (!cuckoo_map->contains(vec[i])) {
      cuckoo_map->insert(vec[i], new value_type());
    }
    size_t lv = iSecret + i;
    oid_t tv = iSecret + i + 1;
    
    // value_type v;
    cuckoo_map->find(vec[i])->insert(std::make_pair(lv, tv));
  }
}

TEST_F(CUCKOOTest, MultiThreadedTest) {
  HashMapType cuckoo_map;
  size_t numThreads = 5;
  LaunchParallelTest(numThreads, Insert, &cuckoo_map);
  EXPECT_EQ(cuckoo_map.size(), 30);
 
  for (int i = 0; i < 30; i++) {
    EXPECT_EQ(cuckoo_map.contains(std::to_string(i)), true); 
    EXPECT_EQ(cuckoo_map.find(std::to_string(i))->size(), numThreads); 
  }

  // const auto& pair_set = cuckoo_map.find(std::to_string(10));
  // EXPECT_EQ(pair_set.find(std::make_pair(0, 0)) != pair_set.end(), true);
}

}
}
