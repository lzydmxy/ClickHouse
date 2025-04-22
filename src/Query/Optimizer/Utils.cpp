#include <Query/Optimizer/Utils.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Query/Analyzer/TypeAnalyzer.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Context.h>
#include <Query/Optimizer/ExpressionDeterminism.h>
#include <Query/Optimizer/ExpressionExtractor.h>
#include <Query/Optimizer/SymbolsExtractor.h>
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Storages/StorageDistributed.h>
#include <boost/math/special_functions/math_fwd.hpp>

#include <optional>

extern const char * build_version;

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace Utils
{

void assertIff(bool expression1, bool expression2)
{
    bool expression = (!(expression1) || (expression2)) && (!(expression2) || (expression1));
    if (!expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal State");
}

void checkState(bool expression)
{
    if (!expression)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal State");
    }
}

void checkState(bool expression, const String & msg)
{
    if (!expression)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal State: {}", msg);
    }
}

void checkArgument(bool expression)
{
    if (!expression)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal Argument");
    }
}

void checkArgument(bool expression, const String & msg)
{
    if (!expression)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal Argument: {}", msg);
    }
}

bool isIdentity(const String & symbol, const ConstASTPtr & expression) {
    return isIdentity(std::make_pair(symbol, expression));
}

bool isIdentity(const Assignment & assignment)
{
    String symbol = assignment.first;
    if (const auto * identifier = assignment.second->as<const ASTIdentifier>())
        return identifier->name() == symbol;
    return false;
}

bool isIdentity(const Assignments & assignments)
{
    return std::all_of(assignments.begin(), assignments.end(), [](const Assignment & assignment) {
        return isIdentity(assignment);
    });
}

bool isIdentity(const ProjectionStepExt & step)
{
    return !step.isFinalProject() && Utils::isIdentity(step.getAssignments());
}

bool isAlias(const Assignment & assignment)
{
    return getAstType(assignment.second) == ASTType::ASTIdentifier;
}

bool isAlias(const Assignments & assignments)
{
    return std::all_of(assignments.begin(), assignments.end(), [](const Assignment & assignment) { return isAlias(assignment); });
}

bool isIdentifierOrIdentifierCast(const ConstASTPtr & expression)
{
    if (const auto * function = expression->as<ASTFunction>())
    {
        return Poco::toLower(function->name) == "cast" && getAstType(function->arguments->children[0]) == ASTType::ASTIdentifier;
    }
    return getAstType(expression) == ASTType::ASTIdentifier;
}

ConstASTPtr tryUnwrapCast(const ConstASTPtr & expression, ContextMutablePtr context, const NamesAndTypes & names_and_types)
{
    if (const auto * function = expression->as<ASTFunction>();
        function && Poco::toLower(function->name) == "cast" && function->arguments->children.size() == 2)
    {
        auto & source_expression = function->arguments->children[0];
        auto source_type = TypeAnalyzer::getType(source_expression, context, names_and_types);

        const auto * target_type_name = function->arguments->children[1]->as<ASTLiteral>();
        auto target_type = DataTypeFactory::instance().get(target_type_name->value.safeGet<String>());

        auto super_type = tryGetLeastSupertype(DataTypes{source_type, target_type});
        if (super_type != nullptr && super_type->equals(*target_type))
        {
            return source_expression;
        }
    }
    return expression;
}

NameToNameMap extractIdentities(const ProjectionStepExt & project)
{
    NameToNameMap result;
    for (const auto & assignment: project.getAssignments())
        if (auto identifier = assignment.second->as<const ASTIdentifier>())
            result.emplace(assignment.first, identifier->name());
    return result;
}

std::unordered_map<String, String> computeIdentityTranslations(const Assignments & assignments)
{
    std::unordered_map<String, String> output_to_input;
    for (const auto & assignment : assignments)
    {
        if (const auto * identifier = assignment.second->as<ASTIdentifier>())
        {
            output_to_input[assignment.first] = identifier->name();
        }
    }
    return output_to_input;
}

ASTPtr extractAggregateToFunction(const AggregateDescription & aggregate_description)
{
    const auto function = std::make_shared<ASTFunction>();
    function->name = aggregate_description.function->getName();
    function->arguments = std::make_shared<ASTExpressionList>();
    function->children.push_back(function->arguments);
    for (auto & argument : aggregate_description.argument_names)
        function->arguments->children.emplace_back(std::make_shared<ASTIdentifier>(argument));
    if (!aggregate_description.parameters.empty())
    {
        function->parameters = std::make_shared<ASTExpressionList>();
        for (auto & parameter : aggregate_description.parameters)
            function->parameters->children.emplace_back(std::make_shared<ASTLiteral>(parameter));
    }
    return function;
}

bool containsAggregateFunction(const ASTPtr & ast)
{
    if (auto function = ast->as<ASTFunction>())
        if (AggregateFunctionFactory::instance().isAggregateFunctionName(function->name))
            return true;
    for (const auto & child : ast->children)
        if (containsAggregateFunction(child))
            return true;
    return false;
}


bool canIgnoreNullsDirection(const DataTypePtr & type)
{
    return !type->isNullable() && type->getTypeId() != TypeIndex::Float32 && type->getTypeId() != TypeIndex::Float64;
}

bool checkFunctionName(const ASTFunction & function, const String & expect_name)
{
    if (function.name == expect_name)
        return true;

    // todo: zhangwanyun1, need getCanonicalName
    // auto res = FunctionFactory::instance().getCanonicalName(function.name);
    //
    // if (res)
    // {
    //     auto & canonical_name = *res;
    //     return canonical_name == expect_name ||
    //         (FunctionFactory::instance().isCaseInsensitive(canonical_name) && canonical_name == Poco::toLower(expect_name));
    // }

    return false;
}

bool ConstASTPtrOrdering::operator()(const ConstASTPtr & predicate_1, const ConstASTPtr & predicate_2) const
{
    size_t symbol_size_1 = SymbolsExtractor::extract(predicate_1).size();
    size_t symbol_size_2 = SymbolsExtractor::extract(predicate_2).size();
    if (symbol_size_1 != symbol_size_2)
        return symbol_size_1 < symbol_size_2;

    size_t sub_expression_size_1 = SubExpressionExtractor::extract(predicate_1).size();
    size_t sub_expression_size_2 = SubExpressionExtractor::extract(predicate_2).size();
    if (sub_expression_size_1 != sub_expression_size_2)
        return sub_expression_size_1 < sub_expression_size_2;

    return predicate_1->getColumnName() < predicate_2->getColumnName();
}

//Determine whether it is NAN
bool isFloatingPointNaN(const DataTypePtr & type, const Field & value)
{
    TypeIndex type_id = type->getTypeId();

    if (type_id == TypeIndex::Float32)
        return std::isnan(value.get<Float64>());

    if (type_id == TypeIndex::Float64)
        return std::isnan(value.get<Float64>());

    return false;
}

String flipOperator(const String & name)
{
    if (name == "equals")
        return name;
    if (name == "notEquals")
        return name;
    if (name == "less")
        return "greater";
    if (name == "lessOrEquals")
        return "greaterOrEquals";
    if (name == "greater")
        return "less";
    if (name == "greaterOrEquals")
        return "lessOrEquals";

    throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Unsupported comparison");
}

bool canChangeOutputRows(const Assignments & assignments, ContextPtr context)
{
    for (const auto & assignment: assignments)
        if (ExpressionDeterminism::canChangeOutputRows(assignment.second, context))
            return true;

    return false;
}

bool canChangeOutputRows(const ProjectionStepExt & project, ContextPtr context)
{
    return canChangeOutputRows(project.getAssignments(), context);
}

static void extractNameToTypeImpl(PlanNodeBase * node, std::optional<NameToType> & res)
{
    if (node && res)
    {
        for (const auto & item : node->getCurrentDataStream().header)
        {
            const auto & name = item.name;
            const auto & type = item.type;
            if (auto it = res->find(name); it != res->end() && !it->second->equals(*type))
            {
                res = std::nullopt;
                break;
            }

            res->emplace(name, type);
        }
    }

    if (res)
    {
        for (auto & child : node->getChildren())
            extractNameToTypeImpl(child.get(), res);
    }
}


std::optional<NameToType> extractNameToType(const PlanNodeBase & node)
{
    std::optional<NameToType> res = NameToType{};
    extractNameToTypeImpl(const_cast<PlanNodeBase *>(&node), res);
    return res;
}

std::string getVersionFromSystem()
{
    if(build_version != nullptr && build_version[0] != '\0')
        return std::string(build_version);
    return "";
}
}
}
