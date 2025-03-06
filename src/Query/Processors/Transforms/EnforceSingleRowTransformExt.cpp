#include <Query/Interpreters/JoinUtilsExt.h>
#include <Query/Processors/Transforms/EnforceSingleRowTransformExt.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
}

EnforceSingleRowTransformExt::EnforceSingleRowTransformExt(const Block & header_)
    : IProcessor({header_}, {header_}), input(inputs.front()), output(outputs.front())
{
}

IProcessor::Status EnforceSingleRowTransformExt::prepare()
{
    // continuous pull data from input until we get an exception or input is finished
    if (!input.isFinished())
    {
        input.setNeeded();
        if (!input.hasData())
            return Status::NeedData;

        auto input_data = input.pullData();
        size_t rows = input_data.chunk.getNumRows();
        if (input_data.exception)
        {
            single_row = std::move(input_data);
            has_input = true;
        }
        else if (rows == 0)
        {
            return Status::NeedData;
        }
        else if (rows > 1 || (rows == 1 && has_input))
        {
            single_row.exception = std::make_exception_ptr(
                Exception(ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY, "Scalar sub-query has returned multiple rows"));
            has_input = true;
        }
        else
        {
            single_row = makeOutputNullable(std::move(input_data.chunk));
            has_input = true;
        }
    }
    else
    {
        if (!has_input)
        {
            single_row = createNullSingleRow();
            has_input = true;
        }

        if (output_finished)
        {
            output.finish();
            return Status::Finished;
        }
    }

    // if we have an exception, push into output and close input.
    if (single_row.exception)
    {
        // An exception occurred during processing.
        output.pushData(std::move(single_row));
        output.finish();
        input.close();
        output_finished = true;
        return Status::Finished;
    }

    // push into output if we have data
    if (!output_finished && single_row.chunk.hasRows())
    {
        if (output.canPush())
        {
            output.pushData(std::move(single_row));
            output_finished = true;
        }
        return Status::PortFull;
    }

    return Status::NeedData;
}

Port::Data EnforceSingleRowTransformExt::createNullSingleRow() const
{
    Port::Data data;

    Block null_block;
    for (const auto & col : input.getHeader().getColumnsWithTypeAndName())
    {
        if (!col.type->isNullable() && !JoinCommon::canBecomeNullable(col.type))
        {
            data.exception = std::make_exception_ptr(Exception(
                ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY,
                " Scalar subquery returned empty result of type {} which cannot be Nullable",
                col.type->getName()));
            return data;
        }
        auto column = JoinCommon::convertTypeToNullable(col.type)->createColumn();
        column->insertDefault();
        null_block.insert({std::move(column), JoinCommon::convertTypeToNullable(col.type), col.name});
    }
    data.chunk.setColumns(null_block.getColumns(), 1);
    return data;
}

Port::Data EnforceSingleRowTransformExt::makeOutputNullable(Chunk && input_data) const
{
    Block null_block;
    Columns input_columns = input_data.detachColumns();
    for (size_t i = 0; i < input_columns.size(); i++)
    {
        auto & column = input_columns[i];
        const auto & type = input.getHeader().getColumnsWithTypeAndName()[i].type;
        if (!column->isNullable() && JoinCommon::canBecomeNullable(type))
        {
            auto nullable = JoinCommon::tryConvertColumnToNullable(column);
            if (!nullable)
            {
                Port::Data data;
                data.exception
                    = std::make_exception_ptr(Exception(ErrorCodes::LOGICAL_ERROR, "type {} cannot be Nullable", type->getName()));
                return data;
            }
            input_columns[i] = nullable;
        }
    }
    Port::Data data;
    data.chunk.setColumns(std::move(input_columns), 1);
    return data;
}

}
