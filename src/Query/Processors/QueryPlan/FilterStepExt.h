#pragma once

#include <Processors/QueryPlan/FilterStep.h>
#include <Query/Parsers/ASTHelper.h>

namespace DB
{

class FilterStepExt : public FilterStep
{
public:
  const ConstASTPtr & getFilter() const { return filter; }
  void setFilter(ConstASTPtr new_filter) { filter = std::move(new_filter);}

  static ConstASTPtr rewriteRuntimeFilter(const ConstASTPtr & filter, QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & build_context);

  static std::pair<ConstASTPtr, ConstASTPtr> splitLargeInValueList(const ConstASTPtr & filter, UInt64 limit);
  static std::vector<ConstASTPtr> removeLargeInValueList(const std::vector<ConstASTPtr> & filters, UInt64 limit);

private:
  ConstASTPtr filter;
};

}