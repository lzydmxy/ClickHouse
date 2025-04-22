#pragma once

#include <Core/Field.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>


namespace DB
{


class FieldHelper
{
public:
    // used for both protobuf and original serde
    static void writeFieldBinaryBlobImpl(const Field & field, Field::Types::Which type, WriteBuffer & buf);
    // used for both protobuf and original serde
    static void readFieldBinaryBlobImpl(Field & field, Field::Types::Which type, ReadBuffer & buf);
};

}
