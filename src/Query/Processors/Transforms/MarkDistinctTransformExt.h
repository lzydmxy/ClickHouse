#pragma once

#include <Processors/ISimpleTransform.h>
#include <Interpreters/Set.h>

namespace DB
{
class MarkDistinctTransformExt : public ISimpleTransform
{
public:
    MarkDistinctTransformExt(const Block & header_, String marker_symbol_, std::vector<String> distinct_symbols_);

    String getName() const override { return "MarkDistinctTransformExt"; }
    static Block transformHeader(Block header, String symbol);

protected:
    void transform(Chunk & chunk) override;

private:
    std::vector<String> distinct_symbols;
    Set distinct_set;
};
}
