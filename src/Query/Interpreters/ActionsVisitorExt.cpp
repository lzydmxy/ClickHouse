#include <Query/Interpreters/ActionsVisitorExt.h>

#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int UNKNOWN_IDENTIFIER;
extern const int NOT_AN_AGGREGATE;
extern const int UNEXPECTED_EXPRESSION;
extern const int TYPE_MISMATCH;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int INCORRECT_ELEMENT_OF_SET;
extern const int BAD_ARGUMENTS;
extern const int DUPLICATE_COLUMN;
extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

static size_t getTypeDepth(const DataTypePtr & type)
{
    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
        return 1 + getTypeDepth(array_type->getNestedType());
    else if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
        return 1 + (tuple_type->getElements().empty() ? 0 : getTypeDepth(tuple_type->getElements().at(0)));

    return 0;
}

static Field extractValueFromNode(const ASTPtr & node, const IDataType & type, ContextPtr context)
{
    if (const auto * lit = node->as<ASTLiteral>())
    {
        return convertFieldToType(lit->value, type);
    }
    else if (node->as<ASTFunction>())
    {
        std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(node, context);
        return convertFieldToType(value_raw.first, type, value_raw.second.get());
    }
    else
        throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Incorrect element of set. Must be literal or constant expression.");
}


static Block createBlockFromAST(const ASTPtr & node, const DataTypes & types, ContextPtr context)
{
    /// Will form a block with values from the set.

    Block header;
    size_t num_columns = types.size();
    for (size_t i = 0; i < num_columns; ++i)
        header.insert(ColumnWithTypeAndName(types[i]->createColumn(), types[i], "_" + toString(i)));

    MutableColumns columns = header.cloneEmptyColumns();

    DataTypePtr tuple_type;
    Row tuple_values;
    const auto & list = node->as<ASTExpressionList &>();
    bool transform_null_in = context->getSettingsRef().transform_null_in;
    for (const auto & elem : list.children)
    {
        if (num_columns == 1)
        {
            /// One column at the left of IN.

            Field value = extractValueFromNode(elem, *types[0], context);
            bool need_insert_null = transform_null_in && types[0]->isNullable();

            if (!value.isNull() || need_insert_null)
                columns[0]->insert(value);
        }
        else if (elem->as<ASTFunction>() || elem->as<ASTLiteral>())
        {
            /// Multiple columns at the left of IN.
            /// The right hand side of in should be a set of tuples.

            Field function_result;
            const Tuple * tuple = nullptr;

            /// Tuple can be represented as a function in AST.
            auto * func = elem->as<ASTFunction>();
            if (func && func->name != "tuple")
            {
                if (!tuple_type)
                    tuple_type = std::make_shared<DataTypeTuple>(types);

                /// If the function is not a tuple, treat it as a constant expression that returns tuple and extract it.
                function_result = extractValueFromNode(elem, *tuple_type, context);
                if (function_result.getType() != Field::Types::Tuple)
                    throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Invalid type of set. Expected tuple, got {}"
                        , String(function_result.getTypeName()));

                tuple = &function_result.get<Tuple>();
            }

            /// Tuple can be represented as a literal in AST.
            auto * literal = elem->as<ASTLiteral>();
            if (literal)
            {
                /// The literal must be tuple.
                if (literal->value.getType() != Field::Types::Tuple)
                    throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Invalid type in set. Expected tuple, got {}",
                        literal->value.getTypeName());

                tuple = &literal->value.get<Tuple>();
            }

            assert(tuple || func);

            size_t tuple_size = tuple ? tuple->size() : func->arguments->children.size(); //-V1004
            if (tuple_size != num_columns)
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Incorrect size of tuple in set: {} instead of {}", toString(tuple_size), toString(num_columns));

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            /// Fill tuple values by evaluation of constant expressions.
            size_t i = 0;
            for (; i < tuple_size; ++i)
            {
                Field value = tuple ? convertFieldToType((*tuple)[i], *types[i])
                                    : extractValueFromNode(func->arguments->children[i], *types[i], context);

                bool need_insert_null = transform_null_in && types[i]->isNullable();

                /// If at least one of the elements of the tuple has an impossible (outside the range of the type) value,
                ///  then the entire tuple too.
                if (value.isNull() && !need_insert_null)
                    break;

                tuple_values[i] = value;
            }

            if (i == tuple_size)
                for (i = 0; i < tuple_size; ++i)
                    columns[i]->insert(tuple_values[i]);
        }
        else
            throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Incorrect element of set");
    }

    return header.cloneWithColumns(std::move(columns));
}

template<typename Collection>
static Block createBlockFromCollection(const Collection & collection, const DataTypes & types, bool transform_null_in)
{
    size_t columns_num = types.size();
    MutableColumns columns(columns_num);
    for (size_t i = 0; i < columns_num; ++i)
        columns[i] = types[i]->createColumn();

    Row tuple_values;
    for (const auto & value : collection)
    {
        if (columns_num == 1)
        {
            auto field = convertFieldToType(value, *types[0]);
            bool need_insert_null = transform_null_in && types[0]->isNullable();
            if (!field.isNull() || need_insert_null)
                columns[0]->insert(std::move(field));
        }
        else
        {
            if (value.getType() != Field::Types::Tuple)
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Invalid type in set. Expected tuple, got {}"
                    , value.getTypeName());

            const auto & tuple = value.template get<const Tuple &>();
            size_t tuple_size = tuple.size();

            if (tuple_size != columns_num)
                throw Exception(ErrorCodes::INCORRECT_ELEMENT_OF_SET, "Incorrect size of tuple in set: {} instead of {}."
                    , tuple_size, columns_num);

            if (tuple_values.empty())
                tuple_values.resize(tuple_size);

            size_t i = 0;
            for (; i < tuple_size; ++i)
            {
                tuple_values[i] = convertFieldToType(tuple[i], *types[i]);
                bool need_insert_null = transform_null_in && types[i]->isNullable();
                if (tuple_values[i].isNull() && !need_insert_null)
                    break;
            }

            if (i == tuple_size)
                for (i = 0; i < tuple_size; ++i)
                    columns[i]->insert(std::move(tuple_values[i]));
        }
    }

    Block res;
    for (size_t i = 0; i < columns_num; ++i)
        res.insert(ColumnWithTypeAndName{std::move(columns[i]), types[i], "_" + toString(i)});
    return res;
}

Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const std::shared_ptr<ASTFunction> & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context)
{
    auto get_tuple_type_from_ast = [context](const auto & func) -> DataTypePtr
    {
        if (func && (func->name == "tuple" || func->name == "array") && !func->arguments->children.empty())
        {
            /// Won't parse all values of outer tuple.
            auto element = func->arguments->children.at(0);
            std::pair<Field, DataTypePtr> value_raw = evaluateConstantExpression(element, context);
            return std::make_shared<DataTypeTuple>(DataTypes({value_raw.second}));
        }

        return evaluateConstantExpression(func, context).second;
    };

    const DataTypePtr & right_arg_type = get_tuple_type_from_ast(right_arg);

    size_t left_tuple_depth = getTypeDepth(left_arg_type);
    size_t right_tuple_depth = getTypeDepth(right_arg_type);
    ASTPtr elements_ast;

    /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
    if (left_tuple_depth == right_tuple_depth)
    {
        ASTPtr exp_list = std::make_shared<ASTExpressionList>();
        exp_list->children.push_back(right_arg);
        elements_ast = exp_list;
    }
    /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4)); etc.
    else if (left_tuple_depth + 1 == right_tuple_depth)
    {
        const auto * set_func = right_arg->as<ASTFunction>();
        if (!set_func || (set_func->name != "tuple" && set_func->name != "array"))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Incorrect type of 2nd argument for function 'in'"
                            ". Must be subquery or set of elements with type {}.", left_arg_type->getName());

        elements_ast = set_func->arguments;
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid types for IN function: {} and {}.", left_arg_type->getName(), right_arg_type->getName());

    return createBlockFromAST(elements_ast, set_element_types, context);
}

Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const ASTPtr & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context)
{
    auto [right_arg_value, right_arg_type] = evaluateConstantExpression(right_arg, context);

    const size_t left_type_depth = getTypeDepth(left_arg_type);
    const size_t right_type_depth = getTypeDepth(right_arg_type);

    Block block;
    bool tranform_null_in = context->getSettingsRef().transform_null_in;

    /// 1 in 1; (1, 2) in (1, 2); identity(tuple(tuple(tuple(1)))) in tuple(tuple(tuple(1))); etc.
    if (left_type_depth == right_type_depth)
    {
        Array array{right_arg_value};
        block = createBlockFromCollection(array, set_element_types, tranform_null_in);
    }
    /// 1 in (1, 2); (1, 2) in ((1, 2), (3, 4)); etc.
    else if (left_type_depth + 1 == right_type_depth)
    {
        auto type_index = right_arg_type->getTypeId();
        if (type_index == TypeIndex::Tuple)
            block = createBlockFromCollection(right_arg_value.get<const Tuple &>(), set_element_types, tranform_null_in);
        else if (type_index == TypeIndex::Array)
            block = createBlockFromCollection(right_arg_value.get<const Tuple &>(), set_element_types, tranform_null_in);
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported value type at the right-side of IN: {}."
            , right_arg_type->getName());
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported value type at the right-side of IN: {}."
        , right_arg_type->getName());;

    return block;
}

}
