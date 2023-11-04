#include "primer/trie.h"
#include <algorithm>
#include <cstddef>
#include <memory>
#include <stack>
#include <string_view>
#include <utility>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (!root_) {
    return nullptr;
  }
  const TrieNode *iter = root_.get();

  for (char ch : key) {
    if (iter->children_.find(ch) == iter->children_.end()) {
      return nullptr;
    }
    iter = iter->children_.at(ch).get();
  }
  auto node_with_value = dynamic_cast<const TrieNodeWithValue<T> *>(iter);

  return node_with_value ? node_with_value->value_.get() : nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  std::unique_ptr<TrieNode> cur = (root_ ? root_ : std::make_shared<const TrieNode>())->Clone();
  std::stack<std::unique_ptr<TrieNode>> st;

  bool is_leaf_node = false;
  for (char ch : key) {
    st.push(cur->Clone());

    if (cur->children_.find(ch) != cur->children_.end()) {
      cur = cur->children_.at(ch)->Clone();
    } else {
      is_leaf_node = true;
      cur = std::make_unique<TrieNode>();
    }
  }

  std::shared_ptr<const TrieNode> node_item =
      is_leaf_node ? std::make_shared<const TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)))
                   : std::make_shared<const TrieNodeWithValue<T>>(std::move(cur->children_),
                                                                  std::make_shared<T>(std::move(value)));
  for (int i = key.size() - 1; i >= 0; --i) {
    cur = std::move(st.top());
    st.pop();

    cur->children_[key[i]] = std::move(node_item);
    node_item = std::move(cur);
  }

  return Trie(std::move(node_item));
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (!root_) {
    return *this;
  }

  std::unique_ptr<TrieNode> cur = root_->Clone();
  std::stack<std::unique_ptr<TrieNode>> st;

  for (char ch : key) {
    if (cur->children_.find(ch) == cur->children_.end()) {
      return *this;
    }

    st.push(cur->Clone());
    cur = cur->children_.at(ch)->Clone();
  }

  bool meet_value_node = false;
  auto node_item = std::make_shared<const TrieNode>(cur->children_);
  for (int i = key.size() - 1; i >= 0; --i) {
    cur = std::move(st.top());
    st.pop();

    if (!node_item->children_.empty()) {
      cur->children_[key[i]] = std::move(node_item);
    } else {
      if (node_item->is_value_node_) {
        meet_value_node = true;
        cur->children_[key[i]] = std::move(node_item);
      } else if (!meet_value_node) {
        cur->children_.erase(key[i]);
      } else {
        cur->children_[key[i]] = std::move(node_item);
      }
    }

    node_item = std::move(cur);
  }

  if (node_item->children_.empty()) {
    return {};
  }

  return Trie(std::move(node_item));
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
