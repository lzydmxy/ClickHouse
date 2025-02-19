#pragma once
#include <IO/ReadBuffer.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{

/// Skip non-numeric characters.
inline void skipNonNumericIfAny(ReadBuffer & buf)
{
    while (!buf.eof() &&  ! (isNumericASCII(*buf.position())))
        ++buf.position();
}

void readWord(String & s, ReadBuffer & buf);

}
