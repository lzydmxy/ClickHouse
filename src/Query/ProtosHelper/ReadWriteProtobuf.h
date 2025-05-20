
#include <base/types.h>
#include <google/protobuf/message.h>

namespace DB
{

UInt64 sipHash64Protobuf(const google::protobuf::Message & proto);

}
