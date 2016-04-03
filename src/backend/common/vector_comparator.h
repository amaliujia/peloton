//
// Created by Rui Wang on 16-3-29.
//

#pragma once

#include <vector>
#include <set>

template <typename ValueType>
class VectorComparator {
public:
  bool Compare(const std::vector<ValueType>& v1, const std::vector<ValueType>& v2) {
    std::multiset<ValueType> set_v1(v1.begin(), v1.end());
    std::multiset<ValueType> set_v2(v2.begin(), v2.end());
    return set_v1 == set_v2;
  }
};