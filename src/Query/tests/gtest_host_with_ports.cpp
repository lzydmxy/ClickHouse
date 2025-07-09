#include <vector>
#include <string>
#include <gtest/gtest.h>
#include <Query/ProtosHelper/HostWithPorts.h>

using namespace DB;

namespace
{

TEST(HostWithPortsTest, addBracketsIfIpv6)
{
    EXPECT_EQ(addBracketsIfIpv6(""), std::string{});
    EXPECT_EQ(addBracketsIfIpv6("127.0.0.1"), std::string{"127.0.0.1"});
    EXPECT_EQ(addBracketsIfIpv6("::1"), std::string{"[::1]"});
    EXPECT_EQ(addBracketsIfIpv6("[::1]"), std::string{"[::1]"});
    EXPECT_EQ(addBracketsIfIpv6("www.google.com"), std::string{"www.google.com"});
    EXPECT_EQ(addBracketsIfIpv6("::"), std::string{"[::]"});
    EXPECT_EQ(addBracketsIfIpv6("[::]"), std::string{"[::]"});
}
 
TEST(HostWithPortsTest, removeBracketsIfIpv6)
{
    EXPECT_EQ(removeBracketsIfIpv6(""), std::string{});
    EXPECT_EQ(removeBracketsIfIpv6("127.0.0.1"), std::string{"127.0.0.1"});
    EXPECT_EQ(removeBracketsIfIpv6("::1"), std::string{"::1"});
    EXPECT_EQ(removeBracketsIfIpv6("[::1]"), std::string{"::1"});
    EXPECT_EQ(removeBracketsIfIpv6("www.google.com"), std::string{"www.google.com"});
    EXPECT_EQ(removeBracketsIfIpv6("::"), std::string{"::"});
    EXPECT_EQ(removeBracketsIfIpv6("[::]"), std::string{"::"});
}
 
TEST(HostWithPortsTest, isSameHost)
{
    bool res;
    res = isSameHost("", "");
    EXPECT_TRUE(res);
    res = isSameHost("::1", "[::1]");
    EXPECT_TRUE(res);
    res = isSameHost("::1", "127.0.0.1");
    EXPECT_FALSE(res);
    res = isSameHost("127.0.0.1", "127.0.0.1");
    EXPECT_TRUE(res);
    res = isSameHost("[::1]", "[::1]");
    EXPECT_TRUE(res);
    res = isSameHost("www.google.com", "www.google.com");
    EXPECT_TRUE(res);
    res = isSameHost("www.google.com", "www.bytedance.com");
    EXPECT_FALSE(res);
}
 
TEST(HostWithPortsTest, HostWithPortsGetAddress)
{
    constexpr uint16_t rpc_port = 9000;
    constexpr uint16_t tcp_port = 9001;
    constexpr uint16_t http_port = 9002;

    HostWithPorts hp0 {"::1", rpc_port, tcp_port, http_port, ""};
    EXPECT_EQ(hp0.getRPCAddress(), "[::1]:9000");
    EXPECT_EQ(hp0.getTCPAddress(), "[::1]:9001");
    EXPECT_EQ(hp0.getHTTPAddress(), "[::1]:9002");

    HostWithPorts hp1 {"[::1]", rpc_port, tcp_port, http_port, ""};
    EXPECT_EQ(hp1.getRPCAddress(), "[::1]:9000");
    EXPECT_EQ(hp1.getTCPAddress(), "[::1]:9001");
    EXPECT_EQ(hp1.getHTTPAddress(), "[::1]:9002");

    HostWithPorts hp2 {"127.0.0.1", rpc_port, tcp_port, http_port, ""};
    EXPECT_EQ(hp2.getRPCAddress(), "127.0.0.1:9000");
    EXPECT_EQ(hp2.getTCPAddress(), "127.0.0.1:9001");
    EXPECT_EQ(hp2.getHTTPAddress(), "127.0.0.1:9002");

    HostWithPorts hp3 {"www.google.com", rpc_port, tcp_port, http_port, ""};
    EXPECT_EQ(hp3.getRPCAddress(), "www.google.com:9000");
    EXPECT_EQ(hp3.getTCPAddress(), "www.google.com:9001");
    EXPECT_EQ(hp3.getHTTPAddress(), "www.google.com:9002");
}
 
TEST(HostWithPortsTest, createHostPortString)
{
    std::string res;
    res = createHostPortString("::", 8000);
    EXPECT_EQ(res, "[::]:8000");
    res = createHostPortString("[::1]", 8000);
    EXPECT_EQ(res, "[::1]:8000");
    res = createHostPortString("127.0.0.1", 8000);
    EXPECT_EQ(res, "127.0.0.1:8000");
    res = createHostPortString("www.google.com", 8000);
    EXPECT_EQ(res, "www.google.com:8000");
}

TEST(HostWithPortsTest, HostWithPortHash)
{
    constexpr uint16_t rpc_port = 9000;
    constexpr uint16_t tcp_port = 9001;
    constexpr uint16_t http_port = 9002;
    // constexpr uint16_t exchange_port = 9003;
    // constexpr uint16_t exchange_status_port = 9004;
    std::hash<DB::HostWithPorts> hasher;

    HostWithPorts hp0 {"::1", rpc_port, tcp_port, http_port, ""};
    EXPECT_EQ(hasher(hp0), hasher(hp0));
    HostWithPorts hp1 {"[::1]", rpc_port, tcp_port, http_port, ""};
    EXPECT_EQ(hasher(hp1), hasher(hp1));

    HostWithPorts hp2 {"[1:1:3:1::166]", rpc_port, tcp_port, http_port, ""};
    HostWithPorts hp3 {"1:1:3:1::166", rpc_port, tcp_port, http_port, ""};
    EXPECT_EQ(hasher(hp2), hasher(hp3));
    EXPECT_EQ(hasher(hp2), hasher(hp2));
    EXPECT_EQ(hasher(hp3), hasher(hp3));

    HostWithPorts hp4 {"10.1.1.1", rpc_port, tcp_port, http_port, ""};
    EXPECT_EQ(hasher(hp4), hasher(hp4));
}
 
TEST(HostWithPortsTest, truncateNetworkInterfaceIfHas)
{
    {
        const std::string host{"1:aaaa:1:1::1111%eno1"};
        const std::string truncated_host = truncateNetworkInterfaceIfHas(host);
        const std::string expected{"1:aaaa:1:1::1111"};
        EXPECT_EQ(expected, truncated_host);
    }

    {
        const std::string host{"1:aaaa:1:1::1111"};
        const std::string truncated_host = truncateNetworkInterfaceIfHas(host);
        const std::string expected{"1:aaaa:1:1::1111"};
        EXPECT_EQ(expected, truncated_host);
    }

    {
        const std::string host{"10.1.1.1%eno1"};
        const std::string truncated_host = truncateNetworkInterfaceIfHas(host);
        const std::string expected{"10.1.1.1"};
        EXPECT_EQ(expected, truncated_host);
    }

    {
        const std::string host{"10.1.1.1"};
        const std::string truncated_host = truncateNetworkInterfaceIfHas(host);
        const std::string expected{"10.1.1.1"};
        EXPECT_EQ(expected, truncated_host);
    }

    {
        const std::string host{"google"};
        const std::string truncated_host = truncateNetworkInterfaceIfHas(host);
        const std::string expected{"google"};
        EXPECT_EQ(expected, truncated_host);
    }

    {
        const std::string host{"www.google.com"};
        const std::string truncated_host = truncateNetworkInterfaceIfHas(host);
        const std::string expected{"www.google.com"};
        EXPECT_EQ(expected, truncated_host);
    }
    {
        const std::string host;
        const std::string truncated_host = truncateNetworkInterfaceIfHas(host);
        const std::string expected;
        EXPECT_EQ(expected, truncated_host);
    }
}

}
