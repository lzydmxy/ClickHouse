#include <Query/Interpreters/TableJoinExt.h>

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

}
