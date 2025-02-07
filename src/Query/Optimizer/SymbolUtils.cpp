
#include <algorithm>
#include <Query/Optimizer/SymbolUtils.h>

namespace DB
{
bool SymbolUtils::contains(const std::vector<String> & symbols, const String & symbol)
{
    return std::find(symbols.begin(), symbols.end(), symbol) != symbols.end();
}

bool SymbolUtils::containsAll(const std::set<String> & left_symbols, const std::set<String> & right_symbols)
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

bool SymbolUtils::containsAll(const std::vector<String> & left_symbols, const std::set<String> & right_symbols)
{
    for (const auto & symbol : right_symbols)
    {
        if (!SymbolUtils::contains(left_symbols, symbol))
        {
            return false;
        }
    }
    return true;
}

}
