#pragma clang diagnostic push
#pragma clang system_header
#pragma GCC diagnostic ignored "-Wall"
#pragma clang diagnostic ignored "-Wunused-variable"
#pragma clang diagnostic ignored "-Wtype-limits"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wdocumentation"
#pragma clang diagnostic ignored "-Wreserved-id-macro"
#pragma clang diagnostic ignored "-Wshadow-field-in-constructor"
#pragma clang diagnostic ignored "-Wextra"
#pragma clang diagnostic ignored "-Wextra-semi-stmt"
#pragma clang diagnostic ignored "-Wcast-align"
#pragma clang diagnostic ignored "-Wshadow-uncaptured-local"
#pragma clang diagnostic ignored "-Wcovered-switch-default"

#pragma push_macro("IS_BIG_ENDIAN")
#undef IS_BIG_ENDIAN

#include <hll.hpp>
#include <cpc_sketch.hpp>
#include <cpc_union.hpp>
#include <kll_sketch.hpp>
#include <theta_a_not_b.hpp>
#include <theta_intersection.hpp>
#include <theta_sketch.hpp>
#include <theta_union.hpp>

#pragma pop_macro("IS_BIG_ENDIAN")

#pragma clang diagnostic pop
