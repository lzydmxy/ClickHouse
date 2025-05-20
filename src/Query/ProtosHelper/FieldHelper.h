#pragma once
#include <Core/Field.h>

namespace DB
{


namespace Protos
{
class Field;
}

void readFieldBinary(Field & field, ReadBuffer & buf);
void writeFieldBinary(const Field & field, WriteBuffer & buf);

void FieldToProto(const Field & field, Protos::Field & proto);
void FieldFillFromProto(Field & field, const Protos::Field & proto);

}
