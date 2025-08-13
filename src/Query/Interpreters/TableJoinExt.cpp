#include <Query/Interpreters/TableJoinExt.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{

void TableJoinExt::setInequalCondition(ExpressionActionsPtr inequal_condition_actions_, String inequal_column_name_)
{
    inequal_condition_actions = inequal_condition_actions_;
    inequal_column_name = inequal_column_name_;
}

bool TableJoinExt::allowMergeJoin() const
{
    bool is_any = (strictness() == JoinStrictness::Any);
    bool is_all = (strictness() == JoinStrictness::All);
    bool is_semi = (strictness() == JoinStrictness::Semi);

    bool all_join = is_all && (isInner(kind()) || isLeft(kind()) || isRight(kind()) || isFull(kind()));
    bool special_left = isLeft(kind()) && (is_any || is_semi);
    return all_join || special_left;
}

void TableJoinExt::addInequalConditions(const ASTs & inequal_conditions, const NamesAndTypesList & columns_for_join, ContextPtr context)
{
    if (inequal_conditions.empty())
        return;

    ASTPtr mixed_inequal_condition = inequal_conditions[0];
    for (size_t i = 1; i < inequal_conditions.size(); ++i)
    {
        mixed_inequal_condition = makeASTFunction("and", mixed_inequal_condition, inequal_conditions[i]);
    }

    /**
    * generate actions for inequal conditions that used for filtering rows in join.
    */
    inequal_column_name = mixed_inequal_condition->getColumnName();
    auto syntax_result = TreeRewriter(context).analyze(mixed_inequal_condition, columns_for_join);
    inequal_condition_actions = ExpressionAnalyzer(mixed_inequal_condition, syntax_result, context).getActions(false);
    LOG_DEBUG(getLogger("TableJoin"), "addInequalConditions: mixed_inequal_condition: {}", queryToString(mixed_inequal_condition));
}

String TableJoinExt::kindToString(JoinKind kind)
{
    switch(kind)
    {
        case JoinKind::Comma:
            return "COMMA";
        case JoinKind::Cross:
            return "CROSS";
        case JoinKind::Full:
            return "FULL";
        case JoinKind::Inner:
            return "INNER";
        case JoinKind::Left:
            return "LEFT";
        case JoinKind::Right:
            return "RIGHT";
        default:
            return "UNKNOWN";
    }
}

String TableJoinExt::strictnessToString(JoinStrictness strictness)
{
    switch(strictness)
    {
        case JoinStrictness::All:
            return "ALL";
        case JoinStrictness::Anti:
            return "ANTI";
        case JoinStrictness::Any:
            return "ANY";
        case JoinStrictness::Asof:
            return "ASOF";
        case JoinStrictness::RightAny:
            return "RIGHTANY";
        case JoinStrictness::Semi:
            return "SIMI";
        case JoinStrictness::Unspecified:
            return "UNSPECIFIED";
    }
}

bool TableJoinExt::enableParallelHashJoin() const
{
    if (table_join.kind == JoinKind::Cross)
        return false;
    if (isSpecialStorage() || !oneDisjunct())
        return false;
    return true;
}

}
