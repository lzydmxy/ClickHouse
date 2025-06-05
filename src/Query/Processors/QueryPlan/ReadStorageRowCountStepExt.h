#pragma once

#include <Processors/QueryPlan/ISourceStep.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/AggregateDescription.h>
#include <Interpreters/Cluster.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

namespace DB
{

class ReadStorageRowCountStepExt : public ISourceStep
{
public:
    explicit ReadStorageRowCountStepExt(Block output_header, ASTPtr query_, AggregateDescription agg_desc_, bool is_final_agg_, StorageID storage_id_, ContextPtr context_);
    
    String getName() const override { return "ReadStorageRowCountExt"; }
    
    AggregateDescription getAggregateDescription() const { return agg_desc; }

    ASTPtr getQuery() const { return query; }

    StorageID getStorageID() const { return storage_id; }

    void setNumRows(UInt64 num_rows_) { num_rows = num_rows_; }

    UInt64 getNumRows() const { return num_rows; }
    ContextPtr getContext() const { return context; }
    bool isFinal() const { return is_final_agg; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    std::shared_ptr<IQueryPlanStep> copy(ContextPtr) const;

    void toProto(Protos::ReadStorageRowCountStepExt & proto, bool for_hash_equals = false) const;
    static std::shared_ptr<ReadStorageRowCountStepExt> fromProto(const Protos::ReadStorageRowCountStepExt & proto, ContextPtr context);

private:
    ASTPtr query;
    AggregateDescription agg_desc;
    std::shared_ptr<Cluster> optimized_cluster;
    UInt64 num_rows;
    bool is_final_agg;
    StorageID storage_id;
    ContextPtr context;
};

}
