#pragma once

#include <Parsers/IAST.h>
#include <Query/Parsers/ASTHelper.h>


namespace DB
{
/** Element of expression: INTO <TOTAL_BUCKET_NUMBER> SPLIT_NUMBER <SPLIT_NUMBER> WITH_RANGE
  */
class ASTClusterByElementExt : public IAST
{
public:

    Int64 split_number;
    bool is_with_range;
    bool is_user_defined_expression;

    ASTClusterByElementExt() = default;

    ASTClusterByElementExt(ASTPtr columns_elem, ASTPtr total_bucket_number_elem, Int64 split_number_, bool is_with_range_, bool is_user_defined_expression_)
        : split_number(split_number_), is_with_range(is_with_range_), is_user_defined_expression(is_user_defined_expression_)
    {
        children.push_back(columns_elem);
        children.push_back(total_bucket_number_elem);
    }

    const ASTPtr & getColumns() const { return children.front(); }
    const ASTPtr & getTotalBucketNumber() const { return children.back(); }

    String getID(char) const { return "ClusterByElement"; }
    ASTPtr clone() const;

protected:
    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const;
};
}
