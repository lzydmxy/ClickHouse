#pragma once

#include <Parsers/ASTExpressionList.h>

namespace DB
{

class ASTExpressionListExt : public ASTExpressionList
{
public:
    void appendColumnName(WriteBuffer &) const override;
};

}
