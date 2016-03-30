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

typedef cuckoohash_map<std::string, std::unique_ptr<value_type>> HashMapType;



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
    size_t lv = iSecret + i;
    oid_t tv = iSecret + i + 1;

    bool ok = cuckoo_map->update_fn(vec[i], [] (std::unique_ptr<value_type>& inner) {
      inner->insert(std::make_pair(lv, tv));
    });

    if (!ok) {
      cuckoo_map->upsert(vec[i], [](std::unique_ptr<value_type>& inner){
        // It is possbile this insert would succeed.
        // I won't check since I am using unordered_set, even insert succeed,
        // another won't hurt.
        inner->insert(std::make_pair(lv, tv));
      }, std::unique<value_type>(new value_type()));

      bool ok = cuckoo_map->update_fn(vec[i], [] (std::unique_ptr<value_type>& inner) {
        inner->insert(std::make_pair(lv, tv));
      });

      // There is no way second update fail.
      assert(ok == true);
    }
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
