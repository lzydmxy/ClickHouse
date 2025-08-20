#pragma once

#include <Interpreters/JoinUtils.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/IJoin.h>


namespace DB
{

namespace JoinCommon
{

DataTypePtr tryConvertTypeToNullable(const DataTypePtr & type);
DataTypePtr removeTypeNullability(const DataTypePtr & type);

ColumnPtr tryConvertColumnToNullable(ColumnPtr col);
bool isJoinCompatibleTypes(const DataTypePtr & left, const DataTypePtr & right);
}

class NotJoinedStreamFromMultipleJoins final : public IBlocksStream
{
public:
    explicit NotJoinedStreamFromMultipleJoins(std::vector<IBlocksStreamPtr> && streams_)
        : streams(std::move(streams_))
    {
    }

protected:
    Block nextImpl() override
    {
        while (current_stream_idx < streams.size())
        {
            Block res = streams[current_stream_idx]->next();
            if (res)
                return res;

            ++current_stream_idx;
        }
        return {};
    }

private:
    std::vector<IBlocksStreamPtr> streams;
    size_t current_stream_idx = 0;
};

}

