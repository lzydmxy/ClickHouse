#include "FieldHelper.h"
#include <Core/Field.h>
#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorWriteBinary.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Query/Common/ReadHelpers.h>
#include <Query/Common/FieldVisitorReadBinary.h>

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
    auto type = static_cast<Field::Types::Which>(read_type);
    field = dispatchField(FieldVisitorReadBinary(buf), type);
}

}
