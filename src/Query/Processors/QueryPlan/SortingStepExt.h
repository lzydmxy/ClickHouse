#pragma once

#include <Core/SortDescription.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Query/Protos/EnumMacros.h>

namespace DB
{
using SizeOrVariable = std::variant<size_t, String>;

/// Sorts stream of data. See MergeSortingTransform.
class SortingStepExt : public ITransformingStep
{
public:
    ENUM_WITH_PROTO_CONVERTER(
        Stage, // enum name
        Protos::SortingStepExt::Stage, // proto enum message
        (FULL),
        (MERGE),
        (PARTIAL),
        (PARTIAL_NO_MERGE)
    );

    explicit SortingStepExt(const DataStream & input_stream, SortDescription description_, SizeOrVariable limit_, Stage stage_, SortDescription prefix_description_ = {}, bool enable_adaptive_spill_ = false);

    String getName() const override { return "Sorting"; }

    const SortDescription & getSortDescription() const { return result_description; }
    const SortDescription & getPrefixDescription() const { return prefix_description; }
    void setPrefixDescription(const SortDescription & prefix_description_) { prefix_description = prefix_description_; }
    Stage getStage() const { return stage; }
    void setStage(Stage stage_) { stage = stage_; }

    const SizeOrVariable & getLimit() const
    {
        return limit;
    }
    UInt64 getLimitValue() const
    {
        return std::get<UInt64>(limit);
    }
    void setLimit(UInt64 limit_) { limit = limit_; }
    bool hasPreparedParam() const
    {
        return std::holds_alternative<String>(limit);
    }
    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void describeActions(JSONBuilder::JSONMap & map) const override;
    void describeActions(FormatSettings & settings) const override;

    /// Add limit or change it to lower value.
    void updateLimit(size_t limit_);

    void toProto(Protos::SortingStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<SortingStepExt> fromProto(const Protos::SortingStepExt & proto, ContextPtr);
    std::shared_ptr<IQueryPlanStep> copy(ContextPtr ptr) const;
    void setInputStreams(const DataStreams & input_streams_);

    // todo: zhangwanyun1, other feat: support PreparedStatement
    // void prepare(const PreparedStatementContext & prepared_context) override;

private:
    const SortDescription result_description;
    SizeOrVariable limit;
    Stage stage;
    SortDescription prefix_description;
    bool enable_adaptive_spill = false;

    void setSizeOrVariableToProto(const SizeOrVariable & size_or_var, Protos::SizeOrVariable & proto) const;
    static std::optional<SizeOrVariable> getSizeOrVariableFromProto(const Protos::SizeOrVariable & proto);
};

}
