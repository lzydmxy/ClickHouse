#include <Query/Parsers/ASTPartitionExt.h>

namespace DB
{

ASTPtr ASTPartitionExt::clone() const
{
    auto res = std::make_shared<ASTPartitionExt>(*this);
    auto baseClonePtr = ASTPartition::clone();
    *static_cast<ASTPartition *>(res.get()) = *dynamic_cast<ASTPartition *>(baseClonePtr.get());

    return res;
}

}
