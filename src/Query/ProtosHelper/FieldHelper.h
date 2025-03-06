#pragma once
#include <Core/Field.h>

namespace DB
{

void readFieldBinary(Field & field, ReadBuffer & buf);
void writeFieldBinary(const Field & field, WriteBuffer & buf);

}
