#include <unordered_set>

#include <backend/common/cuckoohash_map.h>
#include <backend/common/types.h>
#include <boost/functional/hash.hpp>

namespace peloton {
namespace test {
TEST_F(CacheTest, Basic) {
  cuckoohash_map<std::string,
                 std::unordered_set<std::pair<size_t, oid_t>, boost::hash<std::pair<size_t, oid_t>>>> cuckoo_map;

  cuckoo_map["a"].insert(std::make_pair(12,12));
}


}
}
