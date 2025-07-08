#include "NativeChunkOutputStream.h"
#include <Compression/CompressedWriteBuffer.h>
#include <Core/Block.h>
#include <IO/VarInt.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Query/Exchange/ChunkInfo.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

NativeChunkOutputStream::NativeChunkOutputStream(
    WriteBuffer & ostr_, const Block & header_)
    : ostr(ostr_), header(header_)
{
}

static void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, UInt64 offset, UInt64 limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column = column->convertToFullColumnIfConst()->decompress();

    ISerialization::SerializeBinaryBulkSettings settings;
    settings.getter = [&ostr](ISerialization::SubstreamPath) -> WriteBuffer * { return &ostr; };
    settings.position_independent_encoding = false;
    settings.low_cardinality_max_dictionary_size = 0; //-V1048
    //TODO: isFullState() is not supported in ColumnLowCardinality
    // if (column->lowCardinality())
    // {
    //     auto const *lc = typeid_cast<const ColumnLowCardinality *>(column.get());
    //     if (lc->isFullState())
    //     {
    //         auto const *lc_type = typeid_cast<const DataTypeLowCardinality *>(&type);
    //         if (lc_type)
    //         {
    //             auto full_type = lc_type->getFullLowCardinalityTypePtr();
    //             auto serialization = full_type->getDefaultSerialization();
    //             ISerialization::SerializeBinaryBulkStatePtr state;
    //             serialization->serializeBinaryBulkStatePrefix(*full_column, settings, state);
    //             serialization->serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
    //             serialization->serializeBinaryBulkStateSuffix(settings, state);
    //             return ;
    //         }
    //     }
    // }
    auto serialization = type.getDefaultSerialization();

    ISerialization::SerializeBinaryBulkStatePtr state;
    serialization->serializeBinaryBulkStatePrefix(*full_column, settings, state);
    serialization->serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
    serialization->serializeBinaryBulkStateSuffix(settings, state);
}


void NativeChunkOutputStream::write(const Chunk & chunk)
{
    /// chunk info
    auto chunk_info = chunk.getChunkInfo();
    if (chunk_info)
    {
        const auto agg_chunk_info = typeid_cast<const AggregatedChunkInfo *>(chunk_info.get());
        // Only supported AggregatedChunkInfo
        if (agg_chunk_info)
        {
            writeVarUInt(1, ostr);
            writeVarUInt(static_cast<UInt8>(ChunkType::AggregatedChunkInfo), ostr);
            writeAggregatedChunkInfo(ostr, agg_chunk_info);
        }
        else
        {
            writeVarUInt(0, ostr);
        }
    }
    else
    {
        writeVarUInt(0, ostr);
    }
    /// Dimensions
    size_t columns = chunk.getNumColumns();
    size_t rows = chunk.getNumRows();

    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    for (size_t i = 0; i < columns; ++i)
    {
        // DataTypePtr data_type = header.getDataTypes().at(i);
        // ColumnPtr column_ptr = chunk.getColumns()[i];
        //
        // /// Name/Type, we don't need write name/type here.
        // /// Data
        // if (rows) /// Zero items of data is always represented as zero number of bytes.
        //     writeData(*data_type, column_ptr, ostr, 0, 0);

        DataTypePtr data_type = header.getDataTypes().at(i);
        auto column_ptr = chunk.getColumns()[i];

        column_ptr = recursiveRemoveSparse(column_ptr);

        /// Data
        if (rows)    /// Zero items of data is always represented as zero number of bytes.
            writeData(*data_type, column_ptr, ostr, 0, 0);
    }
}
}
