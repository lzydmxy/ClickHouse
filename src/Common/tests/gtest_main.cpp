#include <gtest/gtest.h>
#include <Common/tests/gtest_global_context.h>
#include <Loggers/Loggers.h>
#include <Poco/ConsoleChannel.h>

const char * loglevel = "information";

class ContextEnvironment : public testing::Environment
{
public:
    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        std::cout << "Setup log level:" << loglevel << std::endl;
        Poco::Logger::root().setLevel(loglevel);
        getContext();
    }
};

int main(int argc, char ** argv)
{
    if (argc >= 2)
    {
        const char * tag = argv[1];
        if (strcmp(tag, "debug") == 0)
            loglevel = "debug";
        else if(strcmp(tag, "trace") == 0)
            loglevel = "trace";
    }

    testing::InitGoogleTest(&argc, argv);

    testing::AddGlobalTestEnvironment(new ContextEnvironment);

    return RUN_ALL_TESTS();
}
