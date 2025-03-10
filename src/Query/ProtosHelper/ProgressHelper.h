#pragma once
#include <IO/Progress.h>
#include <Query/ProtosHelper/QueryProto.h>

namespace DB
{

class ProgressHelper
{
public:
    static RProgress toProto(const ProgressValues & vals);
    static ProgressValues fromProto(const RProgress & progress);
    static RProgress progressToProto(const Progress & progress);
    static Progress progressFromProto(const RProgress & progress);

    static bool empty(const ProgressValues & vals);
    static ProgressValues reduce(const ProgressValues & v1, const ProgressValues & v2);
    static ProgressValues add(const ProgressValues & v1, const ProgressValues & v2);
    static bool equals(const ProgressValues & v1, const ProgressValues & v2);
    static String toString(const ProgressValues & vals);
};

}
