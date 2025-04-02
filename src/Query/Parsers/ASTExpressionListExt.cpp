#include <Query/Parsers/ASTExpressionListExt.h>
#include <IO/Operators.h>

namespace DB
{

void ASTExpressionListExt::appendColumnName(WriteBuffer & ostr) const
{
    writeChar('(', ostr);
    if (!children.empty()) {
        for (const auto *it = children.begin(); it != children.end(); ++it)
        {
            if (it != children.begin())
                writeCString(", ", ostr);
    
            if (*it) {
                (*it)->appendColumnName(ostr);
            }
        }
    }
    writeChar(')', ostr);
}

}
