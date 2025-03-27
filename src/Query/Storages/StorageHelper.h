#pragma once

#include <Storages/StorageSnapshot.h>
#include <DataTypes/ObjectUtils.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{
NamesAndTypesList getSubcolumnsOfObjectColumns(StorageSnapshotPtr storage_snapshot);
NamesAndTypesList getSubcolumnsOfAllPhysical(const ColumnsDescription & columns_description);
}
