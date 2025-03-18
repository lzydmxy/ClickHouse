#pragma once

#include <Parsers/IAST.h>
#include <Storages/IStorage_fwd.h>

namespace DB
{
/// this internal AST is only used by optimizer.
/// Represent the origin table and column of a identifier.
class ASTTableColumnReference : public IAST
{
public:
    const IStorage * storage;
    // the node id of the TableScanStep, used to identify different occurrence of a same table in self-join cases.
    size_t unique_id;
    String column_name;

    ASTTableColumnReference(const IStorage * storage_, size_t unique_id_, String column_name_)
        : storage(storage_), unique_id(unique_id_), column_name(std::move(column_name_))
    {
    }

    String getID(char delim) const override;

    void appendColumnName(WriteBuffer &) const override;

    //ASTType getType() const override { return ASTType::ASTTableColumnReference; }

    ASTPtr clone() const override { return std::make_shared<ASTTableColumnReference>(storage, unique_id, column_name); }

    //void toLowerCase() override { boost::to_lower(column_name); }

   //void toUpperCase() override { boost::to_upper(column_name); }

    void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

struct TableInputRef
{
    StoragePtr storage;
    size_t unique_id;

    String getDatabaseTableName() const
    {
        //todo: need to get table name
        //return storage->getStorageID().getFullTableName();
        return {};
    }
    String toString() const { return getDatabaseTableName() + "#" + std::to_string(unique_id); }
};

struct TableInputRefHash
{
    size_t operator()(const TableInputRef & ref) const { return std::hash<UInt32>()(ref.unique_id); }
};

struct TableInputRefEqual
{
    bool operator()(const TableInputRef & lhs, const TableInputRef & rhs) const
    {
        return lhs.storage == rhs.storage && lhs.unique_id == rhs.unique_id;
    }
};

}
