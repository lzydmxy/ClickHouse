#pragma once

#include <Query/Analyzer/ASTEquals.h>
#include <Query/Parsers/ASTVisitor.h>
#include <Query/Processors/QueryPlan/PlanVisitor.h>
#include <Query/Optimizer/EqualityASTMap.h>

namespace DB
{
using ConstASTSet = EqualityASTSet;

class ExpressionExtractor
{
public:
    static std::vector<ConstASTPtr> extract(PlanNodePtr & node);
};

class ExpressionVisitor : public PlanNodeVisitor<Void, std::vector<ConstASTPtr>>
{
public:
    Void visitPlanNode(PlanNodeBase &, std::vector<ConstASTPtr> & expressions) override;
    Void visitProjectionStepExtNode(ProjectionStepExtNode &, std::vector<ConstASTPtr> & expressions) override;
    Void visitFilterStepExtNode(FilterStepExtNode &, std::vector<ConstASTPtr> & expressions) override;
    Void visitAggregatingStepExtNode(AggregatingStepExtNode &, std::vector<ConstASTPtr> & expressions) override;
    Void visitApplyStepExtNode(ApplyStepExtNode &, std::vector<ConstASTPtr> & expressions) override;
    Void visitJoinStepExtNode(JoinStepExtNode &, std::vector<ConstASTPtr> & expressions) override;
};

class SubExpressionExtractor
{
public:
    static ConstASTSet extract(ConstASTPtr node);
};

class SubExpressionVisitor : public ConstASTVisitor<Void, ConstASTSet>
{
public:
    Void visitNode(const ConstASTPtr &, ConstASTSet & context) override;
    Void visitASTFunction(const ConstASTPtr &, ConstASTSet & context) override;
    Void visitASTIdentifier(const ConstASTPtr &, ConstASTSet & context) override;
};

}

