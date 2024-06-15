#ifndef __LRU_CACHE_H_
#define __LRU_CACHE_H_

#include <cassert>
#include <cstddef>
#include <list>
#include <memory>
#include <unordered_map>

namespace btrieve {
template <typename K, typename V> class LRUCache {

public:
  LRUCache(size_t maxSize_) : maxSize(maxSize_) {}

  void cache(const K &key, const V &value) {
    auto pos = keyValuesMap.find(key);
    if (pos == keyValuesMap.end()) {
      orderedKeys.push_front(key);
      keyValuesMap[key] = {std::shared_ptr<V>(new V(value)),
                           orderedKeys.begin()};
      if (keyValuesMap.size() > maxSize) {
        keyValuesMap.erase(orderedKeys.back());
        orderedKeys.pop_back();
      }
    } else {
      orderedKeys.erase(pos->second.second);
      orderedKeys.push_front(key);

      // update the values in keyValuesMap
      pos->second.first = std::shared_ptr<V>(new V(value));
      pos->second.second = orderedKeys.begin();
    }
  }

  std::shared_ptr<V> get(const K &key) {
    auto pos = keyValuesMap.find(key);
    if (pos == keyValuesMap.end()) {
      return nullptr;
    }

    orderedKeys.erase(pos->second.second);
    orderedKeys.push_front(key);

    // update the value in keyValuesMap
    pos->second.second = orderedKeys.begin();

    return pos->second.first;
  }

  size_t size() {
    assert(orderedKeys.size() == keyValuesMap.size());
    return orderedKeys.size();
  }

  void clear() {
    orderedKeys.clear();
    keyValuesMap.clear();
  }

  void remove(const K &key) {
    orderedKeys.remove(key);
    keyValuesMap.erase(key);
  }

private:
  size_t maxSize;
  // most recently used is at the front, least recently used at the back
  std::list<K> orderedKeys;
  std::unordered_map<
      K, std::pair<std::shared_ptr<V>, typename std::list<K>::iterator>>
      keyValuesMap;
};
} // namespace btrieve
#endif