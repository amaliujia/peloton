#include <unordered_set>

#include "harness.h"
#include "backend/common/cuckoohash_map.h"
#include "backend/common/types.h"

#include <boost/functional/hash.hpp>

namespace peloton {
namespace test {

class CUCKOOTest : public PelotonTest {};

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
  // EXPECT_EQ(1, 1);
}

}
}
