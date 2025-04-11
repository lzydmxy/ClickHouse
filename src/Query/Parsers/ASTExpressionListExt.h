#pragma once

#include <Parsers/ASTExpressionList.h>

namespace DB
{

class ASTExpressionListExt : public ASTExpressionList
{
    public:
    explicit ASTExpressionListExt(char separator_ = ',')
        : ASTExpressionList(separator_)
    {
    }

    explicit ASTExpressionListExt(const ASTExpressionList& original)
    {
        this->children = original.children;
    }

    void appendColumnName(WriteBuffer &) const override;
    String getID(char) const override { return "ExpressionListExt"; }
    ASTPtr clone() const override;
};

}
