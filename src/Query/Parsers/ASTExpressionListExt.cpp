#include <Query/Parsers/ASTExpressionListExt.h>
#include <IO/Operators.h>

namespace DB
{

void ASTExpressionListExt::appendColumnName(WriteBuffer & ostr) const
{
    writeChar('(', ostr);
    if (!children.empty()) {
        for (const auto &child : children) {
            if (&child != &children.front())
                writeCString(", ", ostr);
            
            if (child) {
                child->appendColumnName(ostr);
            }
        }
    }
    writeChar(')', ostr);
}
}
