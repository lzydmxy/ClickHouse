#pragma once

#include <Interpreters/ActionsVisitor.h>

namespace DB
{
/** Create a block for set from expression.
  * 'set_element_types' - types of what are on the left hand side of IN.
  * 'right_arg' - list of values: 1, 2, 3 or list of tuples: (1, 2), (3, 4), (5, 6).
  *
  *  We need special implementation for ASTFunction, because in case, when we interpret
  *  large tuple or array as function, `evaluateConstantExpression` works extremely slow.
  *
  *  Note: this and following functions are used in third-party applications in Arcadia, so
  *  they should be declared in header file.
  *
  */
Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const std::shared_ptr<ASTFunction> & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context);

/** Create a block for set from literal.
  * 'set_element_types' - types of what are on the left hand side of IN.
  * 'right_arg' - Literal - Tuple or Array.
  */
Block createBlockForSet(
    const DataTypePtr & left_arg_type,
    const ASTPtr & right_arg,
    const DataTypes & set_element_types,
    ContextPtr context);
}
