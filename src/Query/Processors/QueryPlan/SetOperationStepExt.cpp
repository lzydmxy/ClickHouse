#include <DataTypes/Serializations/ISerialization.h>
#include <Query/Optimizer/Utils.h>
#include <Query/Processors/QueryPlan/SetOperationStepExt.h>
#include <Query/ProtosHelper/ProtosSerDerHelper.h>

namespace DB
{

SetOperationStepExt::SetOperationStepExt(DataStreams input_streams_, DataStream output_stream_, OutputToInputs output_to_inputs_)
    : output_to_inputs(std::move(output_to_inputs_))
{
    input_streams = std::move(input_streams_);

    if (output_stream_.header.getNamesAndTypes().empty())
        output_stream = input_streams.front();
    else
        output_stream = output_stream_;

    size_t num_selects = input_streams.size();
    std::vector<const ColumnWithTypeAndName *> columns(num_selects);
    for (size_t column_num = 0; column_num < output_stream->header.columns(); ++column_num)
    {
        ColumnWithTypeAndName & result_elem = output_stream->header.getByPosition(column_num);
        for (size_t i = 0; i < num_selects; ++i)
        {
            if (output_to_inputs.contains(result_elem.name))
            {
                for (auto & input_name : output_to_inputs[result_elem.name])
                    if (input_streams[i].header.findByName(input_name))
                        columns[i] = input_streams[i].header.findByName(input_name);
            }
            else
                columns[i] = &input_streams[i].header.getByPosition(column_num);
        }
        result_elem.column = getCommonColumnForUnion(columns);
    }

    if (output_to_inputs.empty())
    {
        for (size_t i = 0; i < output_stream->header.columns(); ++i)
        {
            String output_symbol = output_stream->header.getByPosition(i).name;
            std::vector<String> inputs;
            for (auto & input_stream : input_streams)
            {
                String input_symbol = input_stream.header.getByPosition(i).name;
                inputs.emplace_back(input_symbol);
            }
            output_to_inputs[output_symbol] = inputs;
        }
    }

    for (const auto & value : output_to_inputs)
    {
        Utils::checkArgument(
            value.second.size() == input_streams.size(), "Every source needs to map its symbols to an output operation symbol");
    }

    // Make sure each source positionally corresponds to their Symbol values in the Multimap
    for (size_t i = 0; i < input_streams.size(); i++)
    {
        for (auto value : output_to_inputs)
        {
            const Names & input_symbols = input_streams[i].header.getNames();
            String symbol = value.second[i];
            Utils::checkArgument(
                std::find(input_symbols.begin(), input_symbols.end(), symbol) != input_symbols.end(),
                "Every source needs to map its symbols to an output operation symbol");
        }
    }
}

const OutputToInputs & SetOperationStepExt::getOutToInputs() const
{
    return output_to_inputs;
}

void SetOperationStepExt::serializeToProtoBase(Protos::SetOperationStepExt & proto) const
{
    for (const auto & element : input_streams)
        ProtosSerDerHelper::toProto(element, *proto.add_input_streams());
    if (!output_stream.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "empty output stream");
    ProtosSerDerHelper::toProto(output_stream.value(), *proto.mutable_output_stream());
    serializeMapToProto(output_to_inputs, *proto.mutable_output_to_inputs());
}

std::tuple<DataStreams, DataStream, std::unordered_map<String, std::vector<String>>>
SetOperationStepExt::deserializeFromProtoBase(const Protos::SetOperationStepExt & proto)
{
    DataStreams input_streams;
    for (const auto & proto_element : proto.input_streams())
    {
        DataStream element;
        ProtosSerDerHelper::fillFromProto(element, proto_element);
        input_streams.emplace_back(std::move(element));
    }
    DataStream output_stream;
    ProtosSerDerHelper::fillFromProto(output_stream, proto.output_stream());
    auto output_to_inputs = deserializeMapFromProto<String, std::vector<String>>(proto.output_to_inputs());

    return std::make_tuple(input_streams, output_stream, output_to_inputs);
}

NameToNameMap SetOperationStepExt::getOutToInput(size_t source_idx) const
{
    NameToNameMap res;
    for (const auto & [out, inputs] : output_to_inputs)
        res.emplace(out, inputs.at(source_idx));
    return res;
}

}
