#include <Query/Analyzer/ExecutePrewhereSubqueryVisitor.h>

#include <Query/Analyzer/function_utils.h>
#include <QueryPipeline/BlockIO.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/InterpreterFactory.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Query/Common/OptimizerContext.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
    extern const int TOO_MANY_ROWS;
}

static ASTPtr addTypeConversion(std::unique_ptr<ASTLiteral> && ast, const String & type_name)
{
    auto func = std::make_shared<ASTFunction>();
    ASTPtr res = func;
    func->alias = ast->alias;
    func->prefer_alias_to_column_name = ast->prefer_alias_to_column_name;
    ast->alias.clear();
    func->name = "CAST";
    auto exp_list = std::make_shared<ASTExpressionList>();
    func->arguments = exp_list;
    func->children.push_back(func->arguments);
    exp_list->children.emplace_back(ast.release());
    exp_list->children.emplace_back(std::make_shared<ASTLiteral>(type_name));
    return res;
}


void ExecutePrewhereSubquery::visit(ASTSubquery & subquery, ASTPtr & ast) const
{
    rewriteSubqueryToScalarLiteral(subquery, ast);
}

void ExecutePrewhereSubquery::visit(ASTFunction & function, ASTPtr & ast) const
{
    auto type = getFunctionType(function, context);
    if (type == FunctionType::IN_SUBQUERY)
    {
        auto & subquery = function.arguments->children[1];
        rewriteSubqueryToSet(subquery->as<ASTSubquery &>(), subquery);
    }
    else if (type == FunctionType::EXISTS_SUBQUERY)
    {
        bool exists;
        if (function.name == "not")
        {
            auto & subquery = function.arguments->children[0]->as<ASTFunction &>().arguments->children[0];
            exists = !rewriteSubqueryToSet(subquery->as<ASTSubquery &>(), subquery);
        }
        else
        {
            auto & subquery = function.arguments->children[0];
            exists = rewriteSubqueryToSet(subquery->as<ASTSubquery &>(), subquery);
        }

        ast = exists ? std::make_shared<ASTLiteral>(true) : std::make_shared<ASTLiteral>(false);
    }
}

void ExecutePrewhereSubquery::rewriteSubqueryToScalarLiteral(ASTSubquery & subquery, ASTPtr & ast) const
{
    ContextMutablePtr subquery_context = Context::createCopy(context);
    Settings subquery_settings = context->getSettings();
    subquery_settings.max_result_rows = 1;
    subquery_settings.extremes = false;
    // internal SQL doesn't work well in optimizer mode, mainly due to PlanSegmentExecutor
    subquery_context->getOptimizerContext()->getSettingsRef().enable_optimizer = false;
    subquery_context->setSettings(subquery_settings);

    ASTPtr subquery_select = subquery.children.at(0);
    auto interpreter = InterpreterFactory::instance().get(subquery_select, subquery_context,
                                               SelectQueryOptions(QueryProcessingStage::Complete).setInternal(true));
    auto io = interpreter->execute();
    PullingAsyncPipelineExecutor executor(io.pipeline);
    io.pipeline.setProgressCallback(context->getProgressCallback());
    io.pipeline.setProcessListElement(context->getProcessListElement());


    Block block;
    try
    {
        while (block.rows() == 0 && executor.pull(block))
        {
        }

        if (!block)
        {
            auto types = executor.getHeader().getDataTypes();
            if (types.size() != 1)
                types = {std::make_shared<DataTypeTuple>(types)};

            auto & type = types[0];
            if (!type->isNullable())
            {
                if (!type->canBeInsideNullable())
                    throw Exception(
                        ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                        "Scalar subquery returned empty result of type {} which cannot be Nullable",
                        type->getName());

                type = makeNullable(type);
            }

            /// Interpret subquery with empty result as Null literal
            auto ast_null = std::make_unique<ASTLiteral>(Null());
            auto ast_new = addTypeConversion(std::move(ast_null), type->getName());
            ast_new->setAlias(ast->tryGetAlias());
            ast = std::move(ast_new);
            return;
        }

        if (block.rows() > 1)
            throw Exception(
            ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
            "Scalar subquery expected 1 row, got {} rows",
            std::to_string(block.rows()));

        Block rest;
        while (rest.rows() == 0 && executor.pull(rest))
        {
        }

        if (rest.rows() > 0)
            throw Exception(
                ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned more than one non-empty block");
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::TOO_MANY_ROWS)
            throw Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar subquery returned too many rows");
        else
            throw;
    }

    size_t columns = block.columns();
    if (columns == 1)
    {
        auto lit = std::make_unique<ASTLiteral>((*block.safeGetByPosition(0).column)[0]);
        lit->alias = subquery.alias;
        lit->prefer_alias_to_column_name = subquery.prefer_alias_to_column_name;
        ast = addTypeConversion(std::move(lit), block.safeGetByPosition(0).type->getName());
    }
    else
    {
        auto tuple = std::make_shared<ASTFunction>();
        tuple->alias = subquery.alias;
        ast = tuple;
        tuple->name = "tuple";
        auto exp_list = std::make_shared<ASTExpressionList>();
        tuple->arguments = exp_list;
        tuple->children.push_back(tuple->arguments);

        exp_list->children.resize(columns);
        for (size_t i = 0; i < columns; ++i)
        {
            exp_list->children[i] = addTypeConversion(
                std::make_unique<ASTLiteral>((*block.safeGetByPosition(i).column)[0]), block.safeGetByPosition(i).type->getName());
        }
    }
}

bool ExecutePrewhereSubquery::rewriteSubqueryToSet(ASTSubquery & subquery, ASTPtr & ast) const
{
    ContextMutablePtr subquery_context = Context::createCopy(context);
    Settings subquery_settings = context->getSettings();
    subquery_settings.extremes = false;
    // internal SQL doesn't work well in optimizer mode, mainly due to PlanSegmentExecutor
    subquery_context->getOptimizerContext()->getSettingsRef().enable_optimizer = false;
    subquery_context->setSettings(subquery_settings);

    ASTPtr subquery_select = subquery.children.at(0);
    auto interpreter = InterpreterFactory::instance().get(subquery_select, subquery_context,
                                               SelectQueryOptions(QueryProcessingStage::Complete).setInternal(true));
    auto io = interpreter->execute();
    PullingAsyncPipelineExecutor executor(io.pipeline);
    io.pipeline.setProgressCallback(context->getProgressCallback());
    io.pipeline.setProcessListElement(context->getProcessListElement());
    size_t columns = executor.getHeader().columns();

    auto array = std::make_shared<ASTFunction>();
    array->name = "array";
    array->alias = subquery.alias;
    array->arguments = std::make_shared<ASTExpressionList>();
    array->children.push_back(array->arguments);

    Block block;
    while (true)
    {
         while (block.rows() == 0 && executor.pull(block))
         {
         }

        if (!block || !block.rows())
            break;
        if (columns == 1)
        {
            for (size_t position = 0; position < block.rows(); position++)
            {
                auto literal = std::make_unique<ASTLiteral>((*block.safeGetByPosition(0).column)[position]);
                literal->alias = subquery.alias;
                literal->prefer_alias_to_column_name = subquery.prefer_alias_to_column_name;
                auto cast = addTypeConversion(std::move(literal), block.safeGetByPosition(0).type->getName());
                array->arguments->children.emplace_back(cast);
            }
        }
        else
        {
            for (size_t position = 0; position < block.rows(); position++)
            {
                auto tuple = std::make_shared<ASTFunction>();
                tuple->alias = subquery.alias;
                ast = tuple;
                tuple->name = "tuple";
                auto exp_list = std::make_shared<ASTExpressionList>();
                tuple->arguments = exp_list;
                tuple->children.push_back(tuple->arguments);

                exp_list->children.resize(columns);
                for (size_t i = 0; i < columns; ++i)
                {
                    exp_list->children[i] = addTypeConversion(
                        std::make_unique<ASTLiteral>((*block.safeGetByPosition(i).column)[position]), block.safeGetByPosition(i).type->getName());
                }
                array->arguments->children.emplace_back(tuple);
            }
        }
    }

    if (array->arguments->children.empty())
    {
        /// Interpret subquery with empty result as Null literal
        if (columns == 1)
        {
            auto ast_new = std::make_unique<ASTLiteral>(Null());
            ast_new->setAlias(ast->tryGetAlias());
            ast = std::move(ast_new);
            return false;
        }
        else
        {
            auto tuple = std::make_shared<ASTFunction>();
            tuple->alias = subquery.alias;
            ast = tuple;
            tuple->name = "tuple";
            auto exp_list = std::make_shared<ASTExpressionList>();
            tuple->arguments = exp_list;
            tuple->children.push_back(tuple->arguments);

            exp_list->children.resize(columns);
            for (size_t i = 0; i < columns; ++i)
                exp_list->children[i] = std::make_unique<ASTLiteral>(Null());
            array->arguments->children.emplace_back(tuple);

            ast = std::move(array);
            return false;
        }
    }
    else
    {
        ast = std::move(array);
        return true;
    }
}

}
