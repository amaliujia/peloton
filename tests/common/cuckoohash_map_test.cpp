#include <unordered_set>
#include <stdlib.h>
#include <time.h>

#include "harness.h"
#include "backend/common/cuckoohash_map.h"
#include "backend/common/types.h"

#include <boost/functional/hash.hpp>

namespace peloton {
namespace test {

class CUCKOOTest : public PelotonTest {};

Typedef  cuckoohash_map<std::string,
  std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>>> HashMapType;

TEST_F(CUCKOOTest, BasicTest) {
  cuckoohash_map<std::string,
                 std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>>
                   > cuckoo_map;
  if (cuckoo_map.contains("a")) {
    cuckoo_map.find("a").insert(std::make_pair(12,12));
  } else {
    cuckoo_map.insert("a", std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>>());
    cuckoo_map.find("a").insert(std::make_pair(12,12));
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
      cuckoo_map->insert(vec[i], std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>>());
    }
    size_t lv = iSecret + i;
    oid_t lv = iSecret + i + 1;
    cuckoo_map->find(vec[i]).insert(std::make_pair<lv, tv>);
  }
}

TEST_F(CUCKOOTest, MultiThreadedTest) {
  HashMapType cuckoo_map;

  LaunchParallelTest(5, Insert, &cuckoo_map);
  EXPECT_EQ(cockoo_map.size(), 150);
}

}
}
