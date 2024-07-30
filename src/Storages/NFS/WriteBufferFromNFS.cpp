#include "config.h"
#include <IO/WriteHelpers.h>
#include <Storages/NFS/WriteBufferFromNFS.h>
#include <Common/Throttler.h>
#include <sys/uio.h>

namespace ProfileEvents
{
    extern const Event RemoteWriteThrottlerBytes;
    extern const Event RemoteWriteThrottlerSleepMicroseconds;
}


namespace DB
{

namespace ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_FSYNC;
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int FILE_DOESNT_EXIST;
}


struct WriteBufferFromNFS::WriteBufferFromNFSImpl
{
    std::string nfs_file_path;
    const Poco::Util::AbstractConfiguration & config;
    WriteSettings write_settings;
    int fd;

    WriteBufferFromNFSImpl(
            const std::string & nfs_file_path_,
            const Poco::Util::AbstractConfiguration & config_,
            const WriteSettings write_settings_,
            int flags)
        : nfs_file_path(nfs_file_path_), config(config_), write_settings(write_settings_)
    {
#ifdef __APPLE__
        bool o_direct = (flags != -1) && (flags & O_DIRECT);
        if (o_direct)
            flags = flags & ~O_DIRECT;
#endif

        mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
        fd = ::open(nfs_file_path.c_str(), flags == -1 ? O_WRONLY | O_TRUNC | O_CREAT | O_CLOEXEC : flags | O_CLOEXEC, mode);
        if (-1 == fd)
            throw Exception(errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE,
                "Cannot open file {}", nfs_file_path);

#ifdef __APPLE__
        if (o_direct)
        {
            if (fcntl(fd, F_NOCACHE, 1) == -1)
                throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Cannot set F_NOCACHE on file {}", file_name);
        }
#endif
    }

    ~WriteBufferFromNFSImpl()
    {
        if (fd < 0)
            return;
        ::close(fd);
    }

    int write(const char * start, size_t size) const
    {
        int bytes_written = 0;
        ssize_t res = 0;
        {
            //res = ::write(fd, start, size);
            struct iovec vec[1];
            vec[0].iov_base = const_cast<char*>(start);
            vec[0].iov_len = size;
            res = ::writev(fd, vec, 1);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            String error_file_name = nfs_file_path;
            if (error_file_name.empty())
                error_file_name = "(fd = " + toString(fd) + ")";
            throw Exception(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR,
                "Cannot write to NFS file {}", error_file_name);
        }

        if (res > 0)
        {
            bytes_written += res;
            if (write_settings.remote_throttler)
                write_settings.remote_throttler->add(res, ProfileEvents::RemoteWriteThrottlerBytes, ProfileEvents::RemoteWriteThrottlerSleepMicroseconds);
        }

       return bytes_written;
    }

    void sync() const
    {
        /// Request OS to sync data with storage medium.
#if defined(OS_DARWIN)
        int res = ::fsync(fd);
#else
        int res = ::fdatasync(fd);
#endif
        if (-1 == res)
            throw Exception(ErrorCodes::CANNOT_FSYNC, "Cannot NFS fsync {}", nfs_file_path);
    }
};

WriteBufferFromNFS::WriteBufferFromNFS(
        const std::string & nfs_file_path_,
        const Poco::Util::AbstractConfiguration & config_,
        const WriteSettings write_settings_,
        size_t buf_size_,
        int flags)
    : WriteBufferFromFileBase(buf_size_, nullptr, 0)
    , impl(std::make_unique<WriteBufferFromNFSImpl>(nfs_file_path_, config_, write_settings_, flags))
    , filename(nfs_file_path_)
{
}

void WriteBufferFromNFS::nextImpl()
{
    if (!offset())
        return;

    size_t bytes_written = 0;

    while (bytes_written != offset())
        bytes_written += impl->write(working_buffer.begin() + bytes_written, offset() - bytes_written);
}


void WriteBufferFromNFS::sync()
{
    impl->sync();
}


void WriteBufferFromNFS::finalizeImpl()
{
    try
    {
        next();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


WriteBufferFromNFS::~WriteBufferFromNFS()
{
    finalize();
}

}

