#include <Query/tests/gtest_protobuf_common.h>
#include <Query/ProtosHelper/ASTSerDerHelper.h>
#include <Query/Executor/PlanSegment.h>

using namespace DB;


TEST(ASTSerDerTest, serDerLiteral)
{
    auto lit1 = std::make_shared<ASTLiteral>(true);
    auto lit2 = std::make_shared<ASTLiteral>("lalala");

    auto func = makeASTFunction("and", lit1, lit2);

    std::cout << func->formatForLogging(0) << std::endl;
    WriteBufferFromOwnString buf;
    serializeAST(func, buf);

    ReadBufferFromMemory read_buf(buf.str());
    auto deserialized_ast = deserializeAST(read_buf);
    std::cout << deserialized_ast->formatForLogging(0) << std::endl;
}
