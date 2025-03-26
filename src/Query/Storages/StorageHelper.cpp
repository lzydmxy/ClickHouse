#include <Query/Storages/StorageHelper.h>


namespace DB
{
NamesAndTypesList getSubcolumnsOfObjectColumns(StorageSnapshotPtr storage_snapshot)
{
    auto all_columns = storage_snapshot->getMetadataForQuery()->getColumns().get(GetColumnsOptions::All);
    auto size = all_columns.size();
    extendObjectColumns(all_columns, storage_snapshot->object_columns, true);
    auto start_it = all_columns.begin();
    std::advance(start_it, size);
    NamesAndTypesList result;
    result.splice(result.end(), all_columns, start_it, all_columns.end());
    return result;
}

NamesAndTypesList getSubcolumnsOfAllPhysical(const ColumnsDescription & columns_description)
{
    NamesAndTypesList result;
    auto columns_list = columns_description.getAllPhysical();
    auto size = columns_list.size();
    columns_description.addSubcolumnsToList(columns_list);
    auto start_it = columns_list.begin();
    std::advance(start_it, size);
    result.splice(result.end(), columns_list, start_it, columns_list.end());
    return result;
}

}
