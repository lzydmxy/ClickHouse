#include "ReadHelpers.h"
#include <Common/PODArray.h>
#include <base/find_symbols.h>

namespace DB
{

template <typename T>
static void appendToStringOrVector(T & s, ReadBuffer & rb, const char * end)
{
    s.append(rb.position(), end - rb.position());
}

template <typename Vector>
void readWordInto(Vector & s, ReadBuffer & buf)
{
    while (!buf.eof())
    {
        char * next_pos = find_first_symbols<'\t', '\n', ' '>(buf.position(), buf.buffer().end());

        appendToStringOrVector(s, buf, next_pos);
        buf.position() = next_pos;

        if (buf.hasPendingData())
            return;
        s += buf.position()++;
    }
}

void readWord(String & s, ReadBuffer & buf)
{
    s.clear();
    readWordInto(s, buf);
}

}
