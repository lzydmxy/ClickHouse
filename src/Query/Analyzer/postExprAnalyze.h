#pragma once

#include <Query/Analyzer/Analysis.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Interpreters/Context_fwd.h>
#include <Query/Common/OptimizerContext.h>


namespace DB
{
    // postponed analyze used for subcolumn optimization
    void postExprAnalyze(
        ASTFunctionPtr & function, const ColumnsWithTypeAndName & processed_arguments, Analysis & analysis, ContextPtr context);
}
