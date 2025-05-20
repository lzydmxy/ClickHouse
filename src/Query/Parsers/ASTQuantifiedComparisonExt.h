#pragma once

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTWithAlias.h>

namespace DB
{
//AST For Quantified Comparison, for example, '> all', '= all'
class ASTQuantifiedComparisonExt : public ASTWithAlias
{

public:
    enum class QuantifierType
    {
        ANY,
        ALL,
        SOME
    };
    String comparator;
    QuantifierType quantifier_type;
    String getID(char delim) const override;
    ASTPtr clone() const override;

protected:
    void formatImplWithoutAlias(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
    void appendColumnNameImpl(WriteBuffer & ostr) const override;
};

using QuantifierType = ASTQuantifiedComparisonExt::QuantifierType;

template <typename... Args>
std::shared_ptr<ASTQuantifiedComparisonExt> makeASTQuantifiedComparison(const String & comparator, QuantifierType & quantifier_type, Args &&... args)
{
    const auto quantified_comparison = std::make_shared<ASTQuantifiedComparisonExt>();

    quantified_comparison->comparator = comparator;
    quantified_comparison->quantifier_type = quantifier_type;
    quantified_comparison->children = {std::forward<Args>(args)...};
    return quantified_comparison;
}

}
