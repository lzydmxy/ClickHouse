#include "NFSObjectStorage.h"
#include <Common/getRandomASCIIString.h>
#include <Common/filesystemHelpers.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/copyData.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/NFS/ReadBufferFromNFS.h>
#include <Storages/NFS/WriteBufferFromNFS.h>
#include <Interpreters/Context.h>
#include <aws/core/utils/DateTime.h>


namespace CurrentMetrics
{
    extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int CANNOT_UNLINK;
    extern const int UNSUPPORTED_METHOD;
}

void NFSObjectStorage::startup()
{
    if (!fs::is_directory(root_path))
    {
        try
        {
            fs::create_directories(root_path);
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot create the directory of disk {} ({}).", name, root_path);
            throw;
        }
    }
}

void NFSObjectStorage::shutdown()
{
}

ObjectStorageKey NFSObjectStorage::generateObjectKeyForPath(const std::string & /* path */) const
{
    /// Path to store the new NFS object.
    /// Total length is 32 a-z characters for enough randomness.
    /// First 3 characters are used as a prefix for
    constexpr size_t key_name_total_size = 32;
    constexpr size_t key_name_prefix_size = 3;
    const String & date = Aws::Utils::DateTime::CalculateLocalTimestampAsString("%Y%m%d");

    /// Path to store new NFS object.
    String obj_path = fmt::format("{}/{}/{}",
                       date,
                       getRandomASCIIString(key_name_prefix_size),
                       getRandomASCIIString(key_name_total_size - key_name_prefix_size));

    return ObjectStorageKey::createAsRelative(root_path, obj_path);
}

bool NFSObjectStorage::exists(const StoredObject & object) const
{
    return fs::exists(fs::path(object.remote_path));
}

std::unique_ptr<ReadBufferFromFileBase> NFSObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> /*read_hint*/,
    std::optional<size_t> /*file_size*/) const
{
    return std::make_unique<ReadBufferFromNFS>(object.remote_path, patchSettings(read_settings));
}

std::unique_ptr<ReadBufferFromFileBase> NFSObjectStorage::readObjects( /// NOLINT
    const StoredObjects & objects,
    const ReadSettings & read_settings,
    std::optional<size_t> /* read_hint */,
    std::optional<size_t> /* file_size */) const
{
    auto disk_read_settings = patchSettings(read_settings);
    auto global_context = Context::getGlobalContextInstance();

    auto read_buffer_creator =
        [this, disk_read_settings]
        (bool /* restricted_seek */, const std::string & path) -> std::unique_ptr<ReadBufferFromFileBase>
    {
        return std::make_unique<ReadBufferFromNFS>(
            fs::path(path),
            disk_read_settings,
            settings->nfs_max_single_read_retries,
            /* offset */ 0,
            /* read_until_position */ 0,
            /* use_external_buffer */ true);
    };

    switch (read_settings.remote_fs_method)
    {
        case RemoteFSReadMethod::read:
        {
            return std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator), objects, "file:", disk_read_settings,
                global_context->getFilesystemCacheLog(), /* use_external_buffer */false);
        }
        case RemoteFSReadMethod::threadpool:
        {
            auto impl = std::make_unique<ReadBufferFromRemoteFSGather>(
                std::move(read_buffer_creator), objects, "file:", disk_read_settings,
                global_context->getFilesystemCacheLog(), /* use_external_buffer */true);

            auto & reader = global_context->getThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER);
            return std::make_unique<AsynchronousBoundedReadBuffer>(
                std::move(impl), reader, read_settings,
                global_context->getAsyncReadCounters(),
                global_context->getFilesystemReadPrefetchesLog());
        }
    }
}

std::unique_ptr<WriteBufferFromFileBase> NFSObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    if (attributes.has_value())
        throw Exception(
            ErrorCodes::UNSUPPORTED_METHOD,
            "NFS API doesn't support custom attributes/metadata for stored objects");

    const String & nfs_path = object.remote_path.substr(0, object.remote_path.find_last_of('/') + 1);
    if (!fs::is_directory(nfs_path))
    {
        try
        {
            fs::create_directories(nfs_path);
        }
        catch (...)
        {
            LOG_ERROR(log, "Cannot create the directory of disk {} ({}).", name, nfs_path);
            throw;
        }
    }
    int flags = (mode == WriteMode::Append) ? (O_APPEND | O_CREAT | O_WRONLY) : -1;
    return std::make_unique<WriteBufferFromNFS>(object.remote_path, config, write_settings,
        buf_size, flags);
}

/// Return true if the directory has any child
bool NFSObjectStorage::hasAnyChild(const std::string & path) const
{
    fs::directory_iterator end;
    return fs::directory_iterator(path) != end;
}

/// Remove file. Throws exception if file doesn't exists or it's a directory.
void NFSObjectStorage::removeObject(const StoredObject & object)
{
    auto nfs_path = fs::path(object.remote_path);
    if (0 != unlink(nfs_path.c_str()) && errno != ENOENT)
        throw Exception(ErrorCodes::CANNOT_UNLINK, "Cannot unlink file {}", nfs_path.string());

    // Remove null parent directory
    const String & parent_path = object.remote_path.substr(0, object.remote_path.find_last_of('/') + 1);
    if (!hasAnyChild(parent_path))
    {
        LOG_TEST(log, "Remove null parent directory {}", parent_path);
        fs::remove(parent_path);
    }
}

void NFSObjectStorage::removeObjects(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObject(object);
}

void NFSObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    if (exists(object))
        removeObject(object);
}

void NFSObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

ObjectMetadata NFSObjectStorage::getObjectMetadata(const std::string &) const
{
    throw Exception(
        ErrorCodes::UNSUPPORTED_METHOD,
        "NFS API doesn't support custom attributes/metadata for stored objects");
}

void NFSObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> /* object_to_attributes */)
{
    auto in = readObject(object_from, read_settings);
    auto out = writeObject(object_to, WriteMode::Rewrite, /* attributes= */ {}, /* buf_size= */ DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
}

void NFSObjectStorage::applyNewSettings(
    const Poco::Util::AbstractConfiguration & /* config */, const std::string & /* config_prefix */, ContextPtr /* context */)
{
}

std::unique_ptr<IObjectStorage> NFSObjectStorage::cloneObjectStorage(const std::string &, const Poco::Util::AbstractConfiguration &, const std::string &, ContextPtr)
{
    throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "NFS object storage doesn't support cloning");
}

}

