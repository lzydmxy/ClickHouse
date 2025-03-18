#include <Query/Parsers/ASTTableColumnReference.h>

#include <Storages/IStorage.h>
#include <IO/WriteHelpers.h>

namespace DB
{

static inline String formatStorageName(const IStorage * storage, size_t unique_id, const String & column_name, char delim = '.')
{
    return storage->getStorageID().getFullTableName() + "#" + std::to_string(unique_id)
        + delim + column_name;
}

String ASTTableColumnReference::getID(char delim) const
{
    return std::string("TableColumnRef") + delim + formatStorageName(storage, unique_id, column_name, delim);
}

void ASTTableColumnReference::appendColumnName(WriteBuffer & buffer) const
{
    writeString(getID('.'), buffer);
}

void ASTTableColumnReference::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    writeString(getID('.'), settings.ostr);
}
}
