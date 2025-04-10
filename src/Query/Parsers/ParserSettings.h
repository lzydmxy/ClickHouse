#pragma once

#include <Query/Common/OptimizerSettings.h>

namespace DB
{

struct ParserSettingsImpl
{
    mutable bool parse_literal_as_decimal;

    /// determine if apply the rewritings for adaptive type cast
    mutable bool apply_adaptive_type_cast;

    /// update mutable items with the settings of current context
    void changeMutableSettings(const OptimizerSettings & s) const
    {
        apply_adaptive_type_cast = s.adaptive_type_cast;
        parse_literal_as_decimal = s.parse_literal_as_decimal;
    }

    /// update mutable items with other ParserSettingsImpl
    void changeMutableSettings(const ParserSettingsImpl & s) const
    {
        apply_adaptive_type_cast = s.apply_adaptive_type_cast;
        parse_literal_as_decimal = s.parse_literal_as_decimal;
    }

    /// demonstrate nullable info with explicit null modifiers (including nested types)
    bool explicit_null_modifiers;
    bool parse_mysql_ddl;
    bool parse_bitwise_operators;
    /// treat " as identifier quote character (like the ` quote character) and not as a string quote character
    bool ansi_quotes;
};

struct ParserSettings
{
    const static inline ParserSettingsImpl CLICKHOUSE{
        .parse_literal_as_decimal = false,
        .apply_adaptive_type_cast = false,
        .explicit_null_modifiers = false,
        .parse_mysql_ddl = false,
        .parse_bitwise_operators = false,
        .ansi_quotes = true,
    };

    const static inline ParserSettingsImpl MYSQL{
        .parse_literal_as_decimal = true,
        .apply_adaptive_type_cast = false,
        .explicit_null_modifiers = true,
        .parse_mysql_ddl = true,
        .parse_bitwise_operators = true,
        .ansi_quotes = false,
    };

    const static inline ParserSettingsImpl ANSI{
        .parse_literal_as_decimal = true,
        .apply_adaptive_type_cast = false,
        .explicit_null_modifiers = true,
        .parse_mysql_ddl = false,
        .parse_bitwise_operators = false,
        .ansi_quotes = true,
    };

    // deprecated. use `valueOf(const Settings & s)` instead
    static ParserSettingsImpl valueOf(enum DialectType dt)
    {
        switch (dt)
        {
            case DialectType::CLICKHOUSE:
                return CLICKHOUSE;
            case DialectType::ANSI:
                return ANSI;
            case DialectType::MYSQL:
                return MYSQL;
        }
    }

    static ParserSettingsImpl valueOf(const OptimizerSettings & s)
    {
        const auto setting_impl = [&]() -> ParserSettingsImpl {
            switch (s.dialect_type) {
                case DialectType::CLICKHOUSE: return CLICKHOUSE;
                case DialectType::ANSI: return ANSI;
                case DialectType::MYSQL: return MYSQL;
                default:
                    throw std::invalid_argument("Unsupported DialectType");
            }
        }();
        setting_impl.changeMutableSettings(s);
        return setting_impl;
    }
};

}
