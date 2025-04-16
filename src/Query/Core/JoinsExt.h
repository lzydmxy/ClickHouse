#pragma once
#include <string>
#include <Query/Protos/EnumMacros.h>
#include <Query/Protos/common.pb.h>

#include <Core/Joins.h>

namespace DB
{

ENUM_TO_PROTO_CONVERTER(
    JoinKind,
    Protos::JoinKind,
    (Inner), /// Leave only rows that was JOINed.
    (Left), /// If in "right" table there is no corresponding rows, use default values instead.
    (Right),
    (Full),
    (Cross), /// Direct product. Strictness and condition doesn't matter.
    (Comma), /// Same as direct product. Intended to be converted to INNER JOIN with conditions from WHERE.
    (Paste)
);

/// Allows more optimal JOIN for typical cases.
ENUM_TO_PROTO_CONVERTER(
    JoinStrictness, // enum name
    Protos::JoinStrictness, // proto enum message
    (Unspecified),
    (RightAny), /// Old ANY JOIN. If there are many suitable rows in right table, use any from them to join.
    (Any), /// Semi Join with any value from filtering table. For LEFT JOIN with Any and RightAny are the same.
    (All), /// If there are many suitable rows to join, use all of them and replicate rows of "left" table (usual semantic of JOIN).
    (Asof), /// For the last JOIN column, pick the latest value
    (Semi), /// LEFT or RIGHT. SEMI LEFT JOIN filters left table by values exists in right table. SEMI RIGHT - otherwise.
    (Anti) /// LEFT or RIGHT. Same as SEMI JOIN but filter values that are NOT exists in other table.
);

ENUM_TO_PROTO_CONVERTER(
    ASOFJoinInequality, // enum name
    Protos::ASOFJoinInequality, // proto enum message
    (None, 0),
    (Less),
    (Greater),
    (LessOrEquals),
    (GreaterOrEquals)
);

ENUM_TO_PROTO_CONVERTER(
    JoinAlgorithm, // enum name
    Protos::JoinAlgorithm, // proto enum message
    (DEFAULT, 0),
    (AUTO),
    (HASH),
    (PARTIAL_MERGE),
    (PREFER_PARTIAL_MERGE),
    (PARALLEL_HASH),
    (GRACE_HASH),
    (DIRECT),
    (FULL_SORTING_MERGE)
);

}
