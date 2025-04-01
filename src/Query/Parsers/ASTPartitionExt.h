#pragma once

#include <Parsers/ASTPartition.h>

namespace DB
{

/// Either a (possibly compound) expression representing a partition value or a partition ID.
class ASTPartitionExt : public ASTPartition
{
public:
    String fields_str; /// The extent of comma-separated partition expression fields without parentheses.

    ASTPtr clone() const override;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        //todo: now just a fake impl for build
        return;
    }
};

}
