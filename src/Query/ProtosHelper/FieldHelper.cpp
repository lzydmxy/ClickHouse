#include "FieldHelper.h"
#include <Core/Field.h>
#include <Common/FieldVisitorWriteBinary.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Query/Common/FieldVisitorReadBinary.h>
#include <Query/Protos/plan_node.pb.h>
#include <IO/ReadBufferFromString.h>

namespace DB
{

void writeFieldBinary(const Field & field, WriteBuffer & buf)
{
    auto type = field.getType();
    writeBinary(static_cast<UInt8>(type), buf);
    Field::dispatch([&buf](const auto & value) { FieldVisitorWriteBinary()(value, buf); }, field);
}

void readFieldBinary(Field & field, ReadBuffer & buf)
{
    UInt8 read_type = 0;
    readBinary(read_type, buf);
    field = getBinaryValue(read_type, buf);
}

void FieldToProto(const Field & field, Protos::Field & proto)
{
    WriteBufferFromOwnString buf;
    writeFieldBinary(field, buf);
    proto.set_blob(std::move(buf.str()));
}

void FieldFillFromProto(Field & field, const Protos::Field & proto)
{
    auto s = proto.blob();
    ReadBufferFromString buf(proto.blob());
    readFieldBinary(field, buf);
}

}
