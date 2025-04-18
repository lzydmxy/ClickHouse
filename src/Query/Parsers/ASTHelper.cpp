#include <Query/Parsers/ASTHelper.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Query/ProtosHelper/PlanSerDerHelper.h>
#include <Query/ProtosHelper/ASTSerDerHelper.h>
#include "Interpreters/IdentifierSemantic.h"
#include "Query/Parsers/ASTType.h"
#include <Query/ProtosHelper/FieldHelper.h>

namespace DB
{


namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
}

namespace
{
    void serializeASTWithAlias(const IAST & ast, WriteBuffer & buf)
    {
        if (const auto * casted = ast.as<ASTWithAlias>())
        {
            writeBinary(casted->alias, buf);
            writeBinary(casted->prefer_alias_to_column_name, buf);

            return;
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not convert {} to ASTWithAlias", toString(getAstType(ast)));
    }

    void deserializeASTWithAlias(ASTWithAlias & ast, ReadBuffer & buf)
    {
        readBinary(ast.alias, buf);
        readBinary(ast.prefer_alias_to_column_name, buf);
    }
}

void astToLowerCase(const ASTPtr & ast)
{
    if (auto * casted_ast = ast->as<ASTConstraintDeclaration>())
    {
        boost::to_lower(casted_ast->name);
    }
    else if (auto * casted_ast = ast->as<ASTProjectionDeclaration>())
    {
        boost::to_lower(casted_ast->name);
    }
    else if (auto * casted_ast = ast->as<ASTTableColumnReference>())
    {
        boost::to_lower(casted_ast->column_name);
    }
    else if (auto * casted_ast = ast->as<ASTQuantifiedComparisonExt>())
    {
        boost::to_lower(casted_ast->alias);
        boost::to_lower(casted_ast->comparator);
    }
    else if (auto * casted_ast = ast->as<ASTIdentifier>())
    {
        boost::to_lower(casted_ast->alias);
        boost::to_lower(casted_ast->full_name);
        for (auto &name_part : casted_ast->name_parts)
            boost::to_lower(name_part);
    
        if (casted_ast->semantic)
            boost::to_lower(casted_ast->semantic->table);
    }
    else if (auto * casted_ast = ast->as<ASTTableIdentifier>())
    {
        boost::to_lower(casted_ast->alias);
        boost::to_lower(casted_ast->full_name);
        for (auto &name_part : casted_ast->name_parts)
            boost::to_lower(name_part);
    
        if (casted_ast->semantic)
            boost::to_lower(casted_ast->semantic->table);
    }
    else if (auto * casted_ast = ast->as<ASTWindowDefinition>())
    {
        boost::to_lower(casted_ast->parent_window_name);
    }
    else if (auto * casted_ast = ast->as<ASTWindowListElement>())
    {
        boost::to_lower(casted_ast->name);
    }
    else if (auto * casted_ast = ast->as<ASTTableExpression>())
    {
        if (casted_ast->database_and_table_name)
        {
            astToLowerCase(casted_ast->database_and_table_name);
        }
        else if (casted_ast->table_function)
        {
            astToLowerCase(casted_ast->table_function);
        }
        else
        {
            astToLowerCase(casted_ast->subquery);
        }
    }

    // TODO wujianchao add more types
}

void astToUpperCase(const ASTPtr & ast)
{
    if (auto * casted_ast = ast->as<ASTConstraintDeclaration>())
    {
        boost::to_upper(casted_ast->name);
    }
    else if ( auto * casted_ast = ast->as<ASTProjectionDeclaration>())
    {
        boost::to_upper(casted_ast->name);
    }
    else if (auto * casted_ast = ast->as<ASTTableColumnReference>())
    {
        boost::to_upper(casted_ast->column_name);
    }
    else if (auto * casted_ast = ast->as<ASTQuantifiedComparisonExt>())
    {
        boost::to_upper(casted_ast->alias);
        boost::to_upper(casted_ast->comparator);
    }
    else if (auto * casted_ast = ast->as<ASTIdentifier>())
    {
        boost::to_upper(casted_ast->alias);
        boost::to_upper(casted_ast->full_name);
        for (auto &name_part : casted_ast->name_parts)
            boost::to_upper(name_part);
    
        if (casted_ast->semantic)
            boost::to_upper(casted_ast->semantic->table);
    }
    else if (auto * casted_ast = ast->as<ASTTableIdentifier>())
    {
        boost::to_upper(casted_ast->alias);
        boost::to_upper(casted_ast->full_name);
        for (auto &name_part : casted_ast->name_parts)
            boost::to_upper(name_part);
    
        if (casted_ast->semantic)
            boost::to_upper(casted_ast->semantic->table);
    }
    else if (auto * casted_ast = ast->as<ASTWindowDefinition>())
    {
        boost::to_upper(casted_ast->parent_window_name);
    }
    else if (auto * casted_ast = ast->as<ASTWindowListElement>())
    {
        boost::to_upper(casted_ast->name);
    }
    else if (auto * casted_ast = ast->as<ASTTableExpression>())
    {
        if (casted_ast->database_and_table_name)
        {
            astToUpperCase(casted_ast->database_and_table_name);
        }
        else if (casted_ast->table_function)
        {
            astToUpperCase(casted_ast->table_function);
        }
        else
        {
            astToUpperCase(casted_ast->subquery);
        }
    }


    // TODO wujianchao add more types
}

void setOrReplaceAST(ASTPtr & cur_ast, ASTPtr & old_child, const ASTPtr & new_child)
{
    if (!new_child)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to set or replace AST subtree with nullptr");

    if (old_child == new_child)
        return;

    /// set ast
    if (!old_child)
    {
        old_child = new_child;
        cur_ast->children.push_back(old_child);
        return;
    }

    /// replace ast
    for (auto & current_child: cur_ast->children)
    {
        if (current_child == old_child)
        {
            current_child = new_child;
            old_child = new_child;
            return;
        }
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "AST subtree not found in children");
}

void serializeASTImpl(const ConstASTPtr & ast, WriteBuffer & buf)
{
    serializeASTImpl(*ast, buf);
}

// todo: zhangwanyun1, other feat: if support SqlHints, then add related serialize, hints.serialize(buf);
void serializeASTImpl(const IAST & ast, WriteBuffer & buf)
{
    if (const auto * casted = ast.as<ASTArrayJoin>())
    {
        serializeEnum(casted->kind, buf);
        serializeAST(casted->expression_list, buf);
    }
    else if (const auto * casted = ast.as<ASTTablesInSelectQueryElement>())
    {
        serializeAST(casted->table_join, buf);
        serializeAST(casted->table_expression, buf);
        serializeAST(casted->array_join, buf);
    }
    else if (const auto * casted = ast.as<ASTTablesInSelectQuery>())
    {
        serializeASTs(casted->children, buf);
    }
    else if (const auto * casted = ast.as<ASTTableExpression>())
    {
        serializeAST(casted->database_and_table_name, buf);
        serializeAST(casted->table_function, buf);
        serializeAST(casted->subquery, buf);

        writeBinary(casted->final, buf);

        serializeAST(casted->sample_size, buf);
        serializeAST(casted->sample_offset, buf);
    }
    else if (const auto * casted = ast.as<ASTTableJoin>())
    {
        serializeEnum(casted->locality, buf);
        serializeEnum(casted->strictness, buf);
        serializeEnum(casted->kind, buf);

        serializeAST(casted->using_expression_list, buf);
        serializeAST(casted->on_expression, buf);
    }
    else if (const auto * casted = ast.as<ASTIdentifier>())
    {
        //parent serialize
        serializeASTWithAlias(ast, buf);
 
        writeBinary(casted->full_name, buf);
        writeBinary(casted->name_parts, buf);
        if (casted->semantic)
        {
            writeBinary(true, buf);
            writeBinary(casted->semantic->special, buf);
            writeBinary(casted->semantic->can_be_alias, buf);
            writeBinary(casted->semantic->covered, buf);
            if (casted->semantic->membership)
            {
                writeBinary(true, buf);
                writeBinary(casted->semantic->membership.value(), buf);
            }
            else
                writeBinary(false, buf);
        
            writeBinary(casted->semantic->table, buf);
            writeBinary(casted->semantic->legacy_compound, buf);
        }
    }
    else if (const auto * casted = ast.as<ASTTableIdentifier>())
    {
        //parent serialize
        serializeASTWithAlias(ast, buf);

        writeBinary(casted->full_name, buf);
        writeBinary(casted->name_parts, buf);
        if (casted->semantic)
        {
            writeBinary(true, buf);
            writeBinary(casted->semantic->special, buf);
            writeBinary(casted->semantic->can_be_alias, buf);
            writeBinary(casted->semantic->covered, buf);
            if (casted->semantic->membership)
            {
                writeBinary(true, buf);
                writeBinary(casted->semantic->membership.value(), buf);
            }
            else
                writeBinary(false, buf);

            writeBinary(casted->semantic->table, buf);
            writeBinary(casted->semantic->legacy_compound, buf);
        }
    }
    else if (const auto * casted = ast.as<ASTWindowDefinition>())
    {
        writeBinary(casted->parent_window_name, buf);
        serializeAST(casted->partition_by, buf);
        serializeAST(casted->order_by, buf);
        writeBinary(casted->frame_is_default, buf);
        writeBinary(static_cast<UInt8>(casted->frame_type), buf);
        writeBinary(static_cast<UInt8>(casted->frame_begin_type), buf);
        serializeAST(casted->frame_begin_offset, buf);
        writeBinary(casted->frame_begin_preceding, buf);
        writeBinary(static_cast<UInt8>(casted->frame_end_type), buf);
        serializeAST(casted->frame_end_offset, buf);
        writeBinary(casted->frame_end_preceding, buf);
    }
    else if (const auto * casted = ast.as<ASTWindowListElement>())
    {
        writeBinary(casted->name, buf);
        serializeAST(casted->definition, buf);
    }
    else if (const auto * casted = ast.as<ASTSampleRatio>())
    {
        writeBinary(casted->ratio.numerator, buf);
        writeBinary(casted->ratio.denominator, buf);
    }
    else if (const auto * casted = ast.as<ASTSetQuery>())
    {
        writeBinary(casted->is_standalone, buf);
        writeBinary(casted->size(), buf);
        for (const auto & change : casted->changes)
        {
            writeBinary(change.name, buf);
            writeFieldBinary(change.value, buf);
        }
    }
    else if (const auto * casted = ast.as<ASTExpressionListExt>())
    {
        writeBinary(casted->separator, buf);
        serializeASTs(casted->children, buf);
    }
    else if (const auto * casted = ast.as<ASTFunction>())
    {
        /// serialize alias
        serializeASTWithAlias(ast, buf);

        // serialize function
        writeBinary(casted->name, buf);
        serializeAST(casted->arguments, buf);
        serializeAST(casted->parameters, buf);
        writeBinary(casted->is_window_function, buf);
        writeBinary(casted->window_name, buf);
        serializeAST(casted->window_definition, buf);
        writeBinary(casted->no_empty_args, buf);
    }
    else if (const auto * casted = ast.as<ASTFunctionWithKeyValueArguments>())
    {
        writeBinary(casted->name, buf);
        serializeAST(casted->elements, buf);
        writeBinary(casted->has_brackets, buf);
    }
    else if (const auto * casted = ast.as<ASTNameTypePair>())
    {
        writeBinary(casted->name, buf);
        serializeAST(casted->type, buf);
    }
    else if (const auto * casted = ast.as<ASTOrderByElement>())
    {
        writeBinary(casted->direction, buf);
        writeBinary(casted->nulls_direction, buf);
        writeBinary(casted->nulls_direction_was_explicitly_specified, buf);

        serializeAST(casted->getCollation(), buf);

        writeBinary(casted->with_fill, buf);
        serializeAST(casted->getFillFrom(), buf);
        serializeAST(casted->getFillTo(), buf);
        serializeAST(casted->getFillStep(), buf);

        serializeASTs(casted->children, buf);
    }
    else if (const auto * casted = ast.as<ASTPartition>())
    {
        serializeAST(*casted->value, buf);
        writeBinary(casted->fields_count.value(), buf);
        serializeAST(*casted->id, buf);
    }
    else if (const auto * casted = ast.as<ASTQualifiedAsterisk>())
    {
        serializeASTs(casted->children, buf);
    }

    // todo wujianchao add more types
    else
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement serialize of {}", toString(getAstType(ast)));
}

// todo: zhangwanyun1, other feat: if support SqlHints, then add related deserialize, hints.deserialize(buf);
ASTPtr deserializeASTImpl(ASTType type, ReadBuffer & buf)
{
    switch (type)
    {
        case ASTType::ASTArrayJoin:
        {
            auto ast = std::make_shared<ASTArrayJoin>();
            deserializeEnum(ast->kind, buf);
            ast->expression_list = deserializeASTWithChildren(ast->children, buf);
            return ast;
        }
        case ASTType::ASTTablesInSelectQueryElement:
        {
            auto ast = std::make_shared<ASTTablesInSelectQueryElement>();
            ast->table_join = deserializeASTWithChildren(ast->children, buf);
            ast->table_expression = deserializeASTWithChildren(ast->children, buf);
            ast->array_join = deserializeASTWithChildren(ast->children, buf);
            return ast;
        }
        case ASTType::ASTTablesInSelectQuery:
        {
            auto ast = std::make_shared<ASTTablesInSelectQuery>();
            ast->children = deserializeASTs(buf);
            return ast;
        }
        case ASTType::ASTTableExpression:
        {
            auto ast = std::make_shared<ASTTableExpression>();
            ast->database_and_table_name = deserializeASTWithChildren(ast->children, buf);
            ast->table_function = deserializeASTWithChildren(ast->children, buf);
            ast->subquery = deserializeASTWithChildren(ast->children, buf);

            readBinary(ast->final, buf);

            ast->sample_size = deserializeASTWithChildren(ast->children, buf);
            ast->sample_offset = deserializeASTWithChildren(ast->children, buf);
            return ast;
        }
        case ASTType::ASTTableJoin:
        {
            auto ast = std::make_shared<ASTTableJoin>();
            deserializeEnum(ast->locality, buf);
            deserializeEnum(ast->strictness, buf);
            deserializeEnum(ast->kind, buf);

            ast->using_expression_list = deserializeASTWithChildren(ast->children, buf);
            ast->on_expression = deserializeASTWithChildren(ast->children, buf);
            return ast;
        }
        case ASTType::ASTIdentifier:
        {
            auto ast = std::make_shared<ASTIdentifier>("");
            deserializeASTWithAlias(*ast, buf);

            readBinary(ast->full_name, buf);
            readBinary(ast->name_parts, buf);
        
            bool has_semantic;
            readBinary(has_semantic, buf);
            if (has_semantic)
            {
                ast->semantic = std::make_shared<IdentifierSemanticImpl>();
                readBinary(ast->semantic->special, buf);
                readBinary(ast->semantic->can_be_alias, buf);
                readBinary(ast->semantic->covered, buf);

                bool has_member;
                readBinary(has_member, buf);
                if (has_member){
                    size_t member_tmp;
                    readBinary(member_tmp, buf);
                    ast->semantic->membership = member_tmp;
                }

                readBinary(ast->semantic->table, buf);
                readBinary(ast->semantic->legacy_compound, buf);
            }
            return ast;
        }
        case ASTType::ASTTableIdentifier:
        {
            auto ast = std::make_shared<ASTTableIdentifier>("");
            deserializeASTWithAlias(*ast, buf);

            readBinary(ast->full_name, buf);
            readBinary(ast->name_parts, buf);
        
            bool has_semantic;
            readBinary(has_semantic, buf);
            if (has_semantic)
            {
                ast->semantic = std::make_shared<IdentifierSemanticImpl>();
                readBinary(ast->semantic->special, buf);
                readBinary(ast->semantic->can_be_alias, buf);
                readBinary(ast->semantic->covered, buf);

                bool has_member;
                readBinary(has_member, buf);
                if (has_member){
                    size_t member_tmp;
                    readBinary(member_tmp, buf);
                    ast->semantic->membership = member_tmp;
                }

                readBinary(ast->semantic->table, buf);
                readBinary(ast->semantic->legacy_compound, buf);
            }
            return ast;
        }
        case ASTType::ASTWindowDefinition:
        {
            auto ast = std::make_shared<ASTWindowDefinition>();
            readBinary(ast->parent_window_name, buf);
            ast->partition_by = deserializeASTWithChildren(ast->children, buf);
            ast->order_by = deserializeASTWithChildren(ast->children, buf);
        
            readBinary(ast->frame_is_default, buf);
            UInt8 frame_type_num = 0;
            readBinary(frame_type_num, buf);
            ast->frame_type = static_cast<WindowFrame::FrameType>(frame_type_num);
        
            UInt8 frame_begin_type_num = 0;
            readBinary(frame_begin_type_num, buf);
            ast->frame_begin_type = static_cast<WindowFrame::BoundaryType>(frame_begin_type_num);
        
            ast->frame_begin_offset = deserializeAST(buf);
            readBinary(ast->frame_begin_preceding, buf);
        
            UInt8 frame_end_type_num = 0;
            readBinary(frame_end_type_num, buf);
            ast->frame_end_type = static_cast<WindowFrame::BoundaryType>(frame_end_type_num);
        
            ast->frame_end_offset = deserializeAST(buf);
            readBinary(ast->frame_end_preceding, buf);
        
            return ast;
        }
        case ASTType::ASTWindowListElement:
        {
            auto ast = std::make_shared<ASTWindowListElement>();
            readBinary(ast->name, buf);
            ast->definition = deserializeAST(buf);
            return ast;
        }
        case ASTType::ASTSampleRatio:
        {
            ASTSampleRatio::Rational ratio;
            readBinary(ratio.numerator, buf);
            readBinary(ratio.denominator, buf);
    
            auto ast = std::make_shared<ASTSampleRatio>(ratio);
            return ast;
        }
        case ASTType::ASTSetQuery:
        {
            auto ast = std::make_shared<ASTSetQuery>();
            readBinary(ast->is_standalone, buf);
            size_t size;
            readBinary(size, buf);
            for (size_t i = 0; i < size; ++i) {
                SettingChange change;
                readBinary(change.name, buf);
                readFieldBinary(change.value, buf);
                ast->changes.push_back(change);
            }

            return ast;
        }
        case ASTType::ASTExpressionListExt:
        {
            auto ast = std::make_shared<ASTExpressionListExt>();
            readBinary(ast->separator, buf);
            ast->children = deserializeASTs(buf);
            return ast;
        }
        case ASTType::ASTFunction:
        {
            auto ast = std::make_shared<ASTFunction>();
            // deserialize alias
            deserializeASTWithAlias(*ast, buf);
            // deserialize function
            readBinary(ast->name, buf);
            ast->arguments = deserializeASTWithChildren(ast->children, buf);
            ast->parameters = deserializeASTWithChildren(ast->children, buf);
            readBinary(ast->is_window_function, buf);
            readBinary(ast->window_name, buf);
            ast->window_definition = deserializeASTWithChildren(ast->children, buf);
            readBinary(ast->no_empty_args, buf);
            return ast;
        }
        case ASTType::ASTFunctionWithKeyValueArguments:
        {
            auto ast = std::make_shared<ASTFunctionWithKeyValueArguments>();
            readBinary(ast->name, buf);
            ast->elements = deserializeASTWithChildren(ast->children, buf);
            readBinary(ast->has_brackets, buf);
            return ast;
        }
        case ASTType::ASTNameTypePair:
        {
            auto ast = std::make_shared<ASTNameTypePair>();
            readBinary(ast->name, buf);
            ast->type = deserializeASTWithChildren(ast->children, buf);
            return ast;
        }
        case ASTType::ASTOrderByElement:
        {
            auto ast = std::make_shared<ASTOrderByElement>();
            readBinary(ast->direction, buf);
            readBinary(ast->nulls_direction, buf);
            readBinary(ast->nulls_direction_was_explicitly_specified, buf);

            ast->setCollation(deserializeAST(buf));

            readBinary(ast->with_fill, buf);
            ast->setFillFrom(deserializeAST(buf));
            ast->setFillStep(deserializeAST(buf));

            ast->children = deserializeASTs(buf);
            return ast;
        }
        case ASTType::ASTPartition:
        {
            auto ast = std::make_shared<ASTPartition>();
            ast->setPartitionID(deserializeAST(buf));
            readBinary(ast->fields_count.value(), buf);
            ast->setPartitionID(deserializeAST(buf));
            return ast;
        }
        case ASTType::ASTQualifiedAsterisk:
        {
            auto ast = std::make_shared<ASTQualifiedAsterisk>();
            ast->children = deserializeASTs(buf);
            return ast;
        }

        // todo wujianchao add more types
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implement deserializeASTImpl AST for {}", toString(type));
    }
}

ASTFunctionPtr makeASTFunctionWithVectorArgs(ASTFunctionPtr & ast, const String &name, ASTs &&args)
{
    auto function = std::make_shared<ASTFunction>();
    ast->name = name;
    ast->arguments = std::make_shared<ASTExpressionList>();
    ast->children.push_back(function->arguments);
    ast->arguments->children = std::move(args);

    return function;
}

}
