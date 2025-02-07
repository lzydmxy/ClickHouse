#pragma once
#include <Core/Field.h>
#include <set>

namespace DB
{
class SymbolUtils
{
public:
    static bool contains(const std::vector<String> & symbols, const String & symbol);
    static bool containsAll(const std::set<String> & left_symbols, const std::set<String> & right_symbols);
    static bool containsAll(const std::vector<String> & left_symbols, const std::set<String> & right_symbols);

    template <typename K, typename... TArgs, template<typename...> class T>
    static bool containsAll(const std::unordered_set<K> & left_symbols, const T<K, TArgs...> & right_symbols)
    {
        for (const auto & symbol : right_symbols)
        {
            if (!left_symbols.contains(symbol))
            {
                return false;
            }
        }
        return true;
    }
};

}
