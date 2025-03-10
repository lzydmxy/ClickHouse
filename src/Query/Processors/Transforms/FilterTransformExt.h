#pragma once

#include <Processors/Transforms/FilterTransform.h>


namespace DB
{

class FilterTransformExt : public FilterTransform
{
public:
    FilterTransformExt(
        const Block & header_, ExpressionActionsPtr expression_, String filter_column_name_,
        bool remove_filter_column_, bool on_totals_ = false, std::shared_ptr<std::atomic<size_t>> rows_filtered_ = nullptr, bool dynamic_ = false);

    void transform(Chunk & chunk) override;
    void doTransform(Chunk & chunk);
private:
    bool dynamic;

};

}
