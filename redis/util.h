#pragma once
#include <list>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include <folly/Random.h>
#include <folly/String.h>
namespace redis::util{

    inline bool StartsWith(std::string_view text,std::string_view prefix)noexcept
    {
        return prefix.empty() ||
               (text.size() >= prefix.size() && memcmp(text.data(), prefix.data(), prefix.size()) == 0);
    }
    inline std::vector<std::string_view> Split(std::string_view src, const char delims =','){
        std::vector<std::string_view> output;
        folly::split(delims,folly::StringPiece(src.data(),src.size()),output);
        return output;
    }
    template<typename InputIt>
    InputIt RandOne(InputIt first,InputIt last)
    {
        const auto size = std::distance(first, last);
        if (size == 0)return last;
        using diff_t = typename std::iterator_traits<InputIt>::difference_type;
        //随机访问迭代器O(1),其他O(N)
        return std::next(first, folly::Random::rand32(int(0), (int)(size)));
    }

    template<typename T>
    T RandOne(std::initializer_list<T> init_list)
    {
        return *RandOne<std::initializer_list<T>::iterator>(init_list.begin(), init_list.end());
    }

    template<typename Container>
    auto RandOne(Container& container)
    {
        return RandOne(std::begin(container), std::end(container));
    }
    template<typename K,typename V>
    auto RandOne(std::unordered_map<K, V>& c)->decltype(std::begin(c))
    {
        if (c.empty())return c.end();
        if (c.size() == 1)return c.begin();

        int bucket, bucket_size;
        do
        {
            bucket = folly::Random::rand32(0,c.bucket_count());
        } while ((bucket_size = c.bucket_size(bucket)) == 0);
        return RandOne(c.begin(bucket), c.end(bucket));
    }

    template<typename T>
    auto RandOne(std::unordered_set<T>& c)->decltype(std::begin(c))
    {
        if (c.empty())return c.end();
        if (c.size() == 1)return c.begin();
        int bucket, bucket_size;
        do
        {
            bucket = Rand(0,c.bucket_count() - 1);
        } while ((bucket_size = c.bucket_size(bucket)) == 0);
        return RandOne(c.begin(bucket), c.end(bucket));
    }
}