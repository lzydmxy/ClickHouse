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
    void appendColumnName(WriteBuffer &) const override;
};

}
