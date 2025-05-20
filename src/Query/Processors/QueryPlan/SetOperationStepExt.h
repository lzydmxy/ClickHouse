#pragma once
#include <Columns/ColumnConst.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

using OutputToInputs = std::unordered_map<String, std::vector<String>>;

class SetOperationStepExt : public IQueryPlanStep
{
public:
    SetOperationStepExt(DataStreams input_streams_, DataStream output_stream_, OutputToInputs output_to_inputs_);
    const OutputToInputs & getOutToInputs() const;
    NameToNameMap getOutToInput(size_t source_idx) const;

    void serializeToProtoBase(Protos::SetOperationStepExt & proto) const;
    static std::tuple<DataStreams, DataStream, std::unordered_map<String, std::vector<String>>>
    deserializeFromProtoBase(const Protos::SetOperationStepExt & proto);

protected:
    OutputToInputs output_to_inputs;
};

inline ColumnPtr getCommonColumnForUnion(const std::vector<const ColumnWithTypeAndName *> & columns)
{
    ColumnWithTypeAndName result = *columns[0];
    size_t num_const = 0;
    DataTypes types(columns.size());
    for (size_t i = 0; i < columns.size(); ++i)
    {
        types[i] = columns[i]->type;
        if (isColumnConst(*columns[i]->column))
            ++num_const;
    }

    static auto same_constants = [](const IColumn & a, const IColumn & b)
    { return assert_cast<const ColumnConst &>(a).getField() == assert_cast<const ColumnConst &>(b).getField(); };

    /// Create supertype column saving constness if possible.
    bool save_constness = false;
    if (columns.size() == num_const)
    {
        save_constness = true;
        for (size_t i = 1; i < columns.size(); ++i)
        {
            const ColumnWithTypeAndName & first = *columns[0];
            const ColumnWithTypeAndName & other = *columns[i];

            if (!same_constants(*first.column, *other.column))
            {
                save_constness = false;
                break;
            }
        }
    }

    ColumnPtr column = result.type->createColumn();
    if (save_constness)
        column = result.type->createColumnConst(0, assert_cast<const ColumnConst &>(*columns[0]->column).getField());

    return column;
}

}
