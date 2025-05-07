#pragma once

#include <Common/FieldVisitorToString.h>

namespace DB
{
class FieldVisitorToStringWithoutQuoted : public FieldVisitorToString
{
public:
    using FieldVisitorToString::operator();
    static String removeQuoted(const String & s)
    {
        if (s.size() > 1)
            return s.substr(1, s.size() - 2);
        return s;
    }

    String operator()(const UInt64 & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const UInt128 & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const UInt256 & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const Int64 & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const Int128 & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const Int256 & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const UUID & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const IPv4 & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const IPv6 & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const String & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const Array & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const Tuple & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const Map & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const Object & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const DecimalField<Decimal32> & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const DecimalField<Decimal64> & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const DecimalField<Decimal128> & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const DecimalField<Decimal256> & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    String operator()(const AggregateFunctionStateData & x) const
    {
        Field f(x);
        return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    }
    // String operator()(const BitMap64 & x) const
    // {
    //     Field f(x);
    //     return removeQuoted(applyVisitor(FieldVisitorToString(), f));
    // }
};
}
