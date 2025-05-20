#include <Query/Parsers/ASTExpressionListExt.h>
#include <IO/Operators.h>

namespace DB
{

void ASTExpressionListExt::appendColumnName(WriteBuffer & ostr) const
{
    writeChar('(', ostr);
    for (auto it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
            writeCString(", ", ostr);

        (*it)->appendColumnName(ostr);
    }
    writeChar(')', ostr);
}

ASTPtr ASTExpressionListExt::clone() const
{
    auto clone = std::make_shared<ASTExpressionListExt>(*this);
    clone->cloneChildren();
    return clone;
}

}
