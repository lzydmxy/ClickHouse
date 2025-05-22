#pragma once

#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Storages/StorageFactory.h>

namespace DB
{

template <typename T>
struct shared_ptr_helper
{
    template <typename... TArgs>
    static std::shared_ptr<T> create(TArgs &&... args)
    {
        return std::shared_ptr<T>(new T(std::forward<TArgs>(args)...));
    }
};


class StorageMockDistributed final : public shared_ptr_helper<StorageMockDistributed>, public IStorage
{
    friend struct shared_ptr_helper<StorageMockDistributed>;

public:
    static constexpr auto ENGINE_NAME = "MockDistributed";

    String getName() const override { return ENGINE_NAME; }

    bool supportsOptimizer() const  { return true; }
    bool supportsDistributedRead() const { return true; }

protected:
    StorageMockDistributed(
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_,
        ASTStorage * storage_def,
        const String & comment,
        ContextPtr context_)
        : IStorage(table_id_)
    {
        StorageInMemoryMetadata storage_metadata;
        storage_metadata.setColumns(std::move(columns_description_));
        storage_metadata.setConstraints(std::move(constraints_));
        storage_metadata.setComment(comment);
        setInMemoryMetadata(storage_metadata);
    }
};

static void registerStorageMockDistributedDistirubted(StorageFactory & factory)
{
    factory.registerStorage(
        StorageMockDistributed::ENGINE_NAME,
        [](const StorageFactory::Arguments & args) {
            return StorageMockDistributed::create(
                args.table_id,
                args.columns,
                args.constraints,
                args.storage_def,
                args.comment,
                args.getContext());
        },
        {.supports_settings = true, .supports_sort_order = true});
}

inline void tryRegisterStorageMockDistributed()
{
    static struct Register
    {
        Register()
        {
            auto & factory = StorageFactory::instance();
            registerStorageMockDistributedDistirubted(factory);
        }
    } registered;
}

}
