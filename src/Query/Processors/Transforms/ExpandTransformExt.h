#pragma once
#include <vector>
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class ActionsDAG;

class ExpandTransformExt : public ISimpleTransform
{
public:
    ExpandTransformExt(const Block & header_, const Block & output_header_, std::vector<ExpressionActionsPtr> expressions_);
    String getName() const override { return "ExpandTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    std::vector<ExpressionActionsPtr> expressions;
};

}
