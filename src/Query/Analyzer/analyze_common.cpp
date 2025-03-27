#include <Query/Analyzer/analyze_common.h>

#include <Query/Analyzer/ExpressionVisitor.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Query/Common/PredicateConst.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_EXPRESSION;
}

ASTPtr joinCondition(const ASTTableJoin & join)
{
    if (join.using_expression_list)
        return join.using_expression_list;
    if (join.on_expression)
        return join.on_expression;
    return nullptr;
}

bool isNormalInnerJoin(const ASTTableJoin & join)
{
    return join.kind == JoinKind::Inner && (
               join.strictness == JoinStrictness::All ||
               join.strictness == JoinStrictness::Unspecified
           );
}

bool isCrossJoin(const ASTTableJoin & join)
{
    return isCrossOrComma(join.kind);
}

bool isSemiOrAntiJoin(const ASTTableJoin & join)
{
    return join.strictness == JoinStrictness::Semi || join.strictness == JoinStrictness::Anti;
}

bool isAsofJoin(const ASTTableJoin & join)
{
    return join.strictness == JoinStrictness::Asof;
}

String getFunctionForInequality(ASOF::Inequality inequality)
{
    if (inequality == ASOF::Inequality::Less)
        return "less";
    else if (inequality == ASOF::Inequality::Greater)
        return "greater";
    else if (inequality == ASOF::Inequality::LessOrEquals)
        return "lessOrEquals";
    else if (inequality == ASOF::Inequality::GreaterOrEquals)
        return "greaterOrEquals";
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown inequality");
}

static void expressionToCnf(const ASTPtr & node, std::vector<ASTPtr> & members)
{
    if (const auto * func = node->as<ASTFunction>(); func && func->name == "and")
    {
        for (const auto & child : func->arguments->children)
            expressionToCnf(child, members);
        return;
    }
    members.push_back(node);
}

std::vector<ASTPtr> expressionToCnf(const ASTPtr & node)
{
    std::vector<ASTPtr> members;
    expressionToCnf(node, members);
    return members;
}

ASTPtr cnfToExpression(const ASTs & cnf)
{
    if (cnf.empty())
        return PredicateConst::TRUE_VALUE;

    if (cnf.size() == 1)
        return cnf[0];

    return makeASTFunction("and", cnf);
}

namespace
{
    struct ExtractExpressionContext
    {
        const std::function<bool(const ASTPtr &)> & expr_filter;
        std::vector<ASTPtr> result;

        explicit ExtractExpressionContext(const std::function<bool(const ASTPtr &)> & expr_filter_): expr_filter(expr_filter_) {}
    };

    class ExtractAnalyzerExpressionVisitor: public AnalyzerExpressionVisitor<ExtractExpressionContext>
    {
    protected:
        void visitExpression(ASTPtr & node, IAST &, ExtractExpressionContext & ctx) override
        {
            if (ctx.expr_filter(node))
                ctx.result.push_back(node);
        }
    public:
        using AnalyzerExpressionVisitor<ExtractExpressionContext>::AnalyzerExpressionVisitor;
    };
}

std::vector<ASTPtr> extractExpressions(ContextPtr context, Analysis & analysis, ASTPtr root, bool include_subquery,
                                       const std::function<bool(const ASTPtr &)> & filter)
{
    ExtractExpressionContext extractor_context {filter};
    ExtractAnalyzerExpressionVisitor extractor_visitor {context};

    if (include_subquery)
        traverseExpressionTree<ExtractExpressionContext, ExpressionTraversalIncludeSubqueryVisitor>(root, extractor_visitor, extractor_context, analysis, context);
    else
        traverseExpressionTree(root, extractor_visitor, extractor_context, analysis, context);

    return extractor_context.result;
}

std::vector<ASTPtr> extractReferencesToScope(ContextPtr context, Analysis & analysis, ASTPtr root, ScopePtr scope, bool include_subquery)
{
    auto filter = [&](const ASTPtr & node) -> bool {
        if (auto col_ref = analysis.tryGetColumnReference(node); col_ref && col_ref->scope == scope)
            return true;

        return false;
    };

    return extractExpressions(context, analysis, root, include_subquery, filter);
}

void expandOrderByAll(ASTSelectQuery * select_query, const ASTs & select_expressions)
{
    auto * all_elem = select_query->orderBy()->children[0]->as<ASTOrderByElement>();
    if (!all_elem)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Select analyze for not order by asts.");

    auto order_expression_list = std::make_shared<ASTExpressionList>();

    for (const auto & expr : select_expressions)
    {
        if (auto * identifier = expr->as<ASTIdentifier>(); identifier != nullptr)
            if (Poco::toUpper(identifier->name()) == "ALL" || Poco::toUpper(identifier->alias) == "ALL")
                throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION,
                                "Cannot use ORDER BY ALL to sort a column with name 'all', please disable setting `enable_order_by_all` and try again");

        if (auto * function = expr->as<ASTFunction>(); function != nullptr)
            if (Poco::toUpper(function->alias) == "ALL")
                throw Exception(ErrorCodes::UNEXPECTED_EXPRESSION,
                                "Cannot use ORDER BY ALL to sort a column with name 'all', please disable setting `enable_order_by_all` and try again");

        auto elem = std::make_shared<ASTOrderByElement>();
        elem->direction = all_elem->direction;
        elem->nulls_direction = all_elem->nulls_direction;
        elem->nulls_direction_was_explicitly_specified = all_elem->nulls_direction_was_explicitly_specified;
        elem->children.push_back(expr);
        order_expression_list->children.push_back(elem);
    }

    select_query->setExpression(ASTSelectQuery::Expression::ORDER_BY, order_expression_list);
    select_query->order_by_all = false;
}

std::pair<bool, UInt64> recursivelyCollectMaxOrdinaryExpressions(const ASTPtr & expr, ASTExpressionList & into)
{
    if (expr->as<ASTIdentifier>() || expr->as<ASTFieldReferenceExt>())
    {
        into.children.push_back(expr);
        return {false, 1};
    }
    auto * function = expr->as<ASTFunction>();
    if (!function)
        return {false, 0};
    if (AggregateUtils::isAggregateFunction(*function))
        return {true, 0};
    UInt64 pushed_children = 0;
    bool has_aggregate = false;
    for (const auto & child : function->arguments->children)
    {
        auto [child_has_aggregate, child_pushed_children] = recursivelyCollectMaxOrdinaryExpressions(child, into);
        has_aggregate |= child_has_aggregate;
        pushed_children += child_pushed_children;
    }
    /// The current function is not aggregate function and there is no aggregate function in its arguments,
    /// so use the current function to replace its arguments
    if (!has_aggregate)
    {
        for (UInt64 i = 0; i < pushed_children; i++)
            into.children.pop_back();
        into.children.push_back(expr);
        pushed_children = 1;
    }
    return {has_aggregate, pushed_children};
}

/** Expand GROUP BY ALL by extracting all the SELECT-ed expressions that are not aggregate functions.
  *
  * For a special case that if there is a function having both aggregate functions and other fields as its arguments,
  * the `GROUP BY` keys will contain the maximum non-aggregate fields we can extract from it.
  *
  * Example:
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY ALL
  * will expand as
  * SELECT substring(a, 4, 2), substring(substring(a, 1, 2), 1, count(b)) FROM t GROUP BY substring(a, 4, 2), substring(a, 1, 2)
  */
void expandGroupByAll(ASTSelectQuery * select_query, const ASTs & select_expressions)
{
    auto group_expression_list = std::make_shared<ASTExpressionList>();
    for (const auto & expr : select_expressions)
        recursivelyCollectMaxOrdinaryExpressions(expr, *group_expression_list);
    select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_expression_list);
}

}
