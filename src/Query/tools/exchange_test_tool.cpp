#include "exchange_remote_helper.h"

using namespace DB;
using namespace Tools;

int main(int argc, char ** argv)
{
    std::cout << "Begin run exchange test" << std::endl;
    if (argc < 8)
    {
        std::cout << "Parameters : mode[all/client/server] log_level[trace|debug|information] ip port query_num thread_num chunk_num" << std::endl;
        return 0;
    }
    ClientParam param;
    const char * md = argv[1];
    const char* log_level = argv[2];
    param.server = argv[3];
    param.port = argv[4];
    param.query_num = atoi(argv[5]);
    param.thread_num = atoi(argv[6]);
    param.chunk_num = atoi(argv[7]);
    ExchangeRemoteClient client{param, log_level};
    RunMode mode{RunMode::ALL};
    if (strcmp(md, "all") == 0)
    {
        mode = RunMode::ALL;
    }
    if (strcmp(md, "server") == 0)
    {
        mode = RunMode::SERVER;
    }
    if (strcmp(md, "client") == 0)
    {
        mode = RunMode::CLIENT;
    }
    client.run(mode);
}
