#include <Query/Parsers/ASTDictionary.h>
#include <Poco/String.h>
#include <IO/Operators.h>
#include <Common/FieldVisitorToString.h>


namespace JDDB
{

ASTPtr ASTDictionaryRange::clone() const
{
    auto res = std::make_shared<ASTDictionaryRange>();
    res->min_attr_name = min_attr_name;
    res->max_attr_name = max_attr_name;
    return res;
}

void ASTDictionaryRange::formatImpl(const FormatSettings & settings,
                                    FormatState &,
                                    FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "RANGE"
                  << (settings.hilite ? hilite_none : "")
                  << "("
                  << (settings.hilite ? hilite_keyword : "")
                  << "MIN "
                  << (settings.hilite ? hilite_none : "")
                  << min_attr_name << " "
                  << (settings.hilite ? hilite_keyword : "")
                  << "MAX "
                  << (settings.hilite ? hilite_none : "")
                  << max_attr_name << ")";
}

void ASTDictionaryRange::serialize(WriteBuffer & buf) const
{
    writeBinary(min_attr_name, buf);
    writeBinary(max_attr_name, buf);
}

void ASTDictionaryRange::deserializeImpl(ReadBuffer & buf)
{
    readBinary(min_attr_name, buf);
    readBinary(max_attr_name, buf);
}

ASTPtr ASTDictionaryRange::deserialize(ReadBuffer & buf)
{
    auto res = std::make_shared<ASTDictionaryRange>();
    res->deserializeImpl(buf);
    return res;
}

ASTPtr ASTDictionaryLifetime::clone() const
{
    const auto res = std::make_shared<ASTDictionaryLifetime>();
    res->min_sec = min_sec;
    res->max_sec = max_sec;
    return res;
}

void ASTDictionaryLifetime::formatImpl(const FormatSettings & settings,
                                       FormatState &,
                                       FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "LIFETIME"
                  << (settings.hilite ? hilite_none : "")
                  << "("
                  << (settings.hilite ? hilite_keyword : "")
                  << "MIN "
                  << (settings.hilite ? hilite_none : "")
                  << min_sec << " "
                  << (settings.hilite ? hilite_keyword : "")
                  << "MAX "
                  << (settings.hilite ? hilite_none : "")
                  << max_sec << ")";
}

void ASTDictionaryLifetime::serialize(WriteBuffer & buf) const
{
    writeBinary(min_sec, buf);
    writeBinary(max_sec, buf);
}

void ASTDictionaryLifetime::deserializeImpl(ReadBuffer & buf)
{
    readBinary(min_sec, buf);
    readBinary(max_sec, buf);
}

ASTPtr ASTDictionaryLifetime::deserialize(ReadBuffer & buf)
{
    auto res = std::make_shared<ASTDictionaryLifetime>();
    res->deserializeImpl(buf);
    return res;
}


ASTPtr ASTDictionaryLayout::clone() const
{
    auto res = std::make_shared<ASTDictionaryLayout>();
    res->layout_type = layout_type;
    if (parameters) res->set(res->parameters, parameters->clone());
    res->has_brackets = has_brackets;
    return res;
}


void ASTDictionaryLayout::formatImpl(const FormatSettings & settings,
                                     FormatState & state,
                                     FormatStateStacked frame) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "LAYOUT"
                  << (settings.hilite ? hilite_none : "")
                  << "("
                  << (settings.hilite ? hilite_keyword : "")
                  << Poco::toUpper(layout_type)
                  << (settings.hilite ? hilite_none : "");

    if (has_brackets)
        settings.ostr << "(";

    if (parameters) parameters->formatImpl(settings, state, frame);

    if (has_brackets)
        settings.ostr << ")";

    settings.ostr << ")";
}

void ASTDictionaryLayout::serialize(WriteBuffer & buf) const
{
    writeBinary(layout_type, buf);
    /// TODO
    // writeBinary(parameters, buf);
    writeBinary(has_brackets, buf);
}

void ASTDictionaryLayout::deserializeImpl(ReadBuffer & buf)
{
    readBinary(layout_type, buf);
    /// TODO
    // readBinary(parameters, buf);
    readBinary(has_brackets, buf);
}

ASTPtr ASTDictionaryLayout::deserialize(ReadBuffer & buf)
{
    auto res = std::make_shared<ASTDictionaryLayout>();
    res->deserializeImpl(buf);
    return res;
}

ASTPtr ASTDictionarySettings::clone() const
{
    auto res = std::make_shared<ASTDictionarySettings>();
    res->changes = changes;

    return res;
}

void ASTDictionarySettings::formatImpl(const FormatSettings & settings,
                                        FormatState &,
                                        FormatStateStacked) const
{

    settings.ostr << (settings.hilite ? hilite_keyword : "")
                  << "SETTINGS"
                  << (settings.hilite ? hilite_none : "")
                  << "(";
    for (auto it = changes.begin(); it != changes.end(); ++it)
    {
        if (it != changes.begin())
            settings.ostr << ", ";

        settings.ostr << it->name << " = " << applyVisitor(DB::FieldVisitorToString(), it->value);
    }
    settings.ostr << (settings.hilite ? hilite_none : "") << ")";
}


void ASTDictionarySettings::serialize(WriteBuffer & buf) const
{
    IAST_EXT::serialize(changes, buf);
}

void ASTDictionarySettings::deserializeImpl(ReadBuffer & buf)
{
    IAST_EXT::deserialize(changes, buf);
}

ASTPtr ASTDictionarySettings::deserialize(ReadBuffer & buf)
{
    auto res = std::make_shared<ASTDictionarySettings>();
    res->deserializeImpl(buf);
    return res;
}


ASTPtr ASTDictionary::clone() const
{
    auto res = std::make_shared<ASTDictionary>();

    if (primary_key)
        res->set(res->primary_key, primary_key->clone());

    if (source)
        res->set(res->source, source->clone());

    if (lifetime)
        res->set(res->lifetime, lifetime->clone());

    if (layout)
        res->set(res->layout, layout->clone());

    if (range)
        res->set(res->range, range->clone());

    if (dict_settings)
        res->set(res->dict_settings, dict_settings->clone());

    res->clickhouse_db = clickhouse_db;
    res->clickhouse_tb = clickhouse_tb;
    res->clickhouse_invalidate_query = clickhouse_invalidate_query;
    res->clickhouse_query = clickhouse_query;

    return res;
}


void ASTDictionary::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (primary_key)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "PRIMARY KEY "
            << (settings.hilite ? hilite_none : "");
        primary_key->formatImpl(settings, state, frame);
    }

    if (source)
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << settings.nl_or_ws << "SOURCE"
            << (settings.hilite ? hilite_none : "");
        settings.ostr << "(";
        source->formatImpl(settings, state, frame);
        settings.ostr << ")";
    }

    if (lifetime)
    {
        settings.ostr << settings.nl_or_ws;
        lifetime->formatImpl(settings, state, frame);
    }

    if (layout)
    {
        settings.ostr << settings.nl_or_ws;
        layout->formatImpl(settings, state, frame);
    }

    if (range)
    {
        settings.ostr << settings.nl_or_ws;
        range->formatImpl(settings, state, frame);
    }

    if (dict_settings)
    {
        settings.ostr << settings.nl_or_ws;
        dict_settings->formatImpl(settings, state, frame);
    }
}

void ASTDictionary::serialize(WriteBuffer & buf) const
{
    /// TODO
    // writeBinary(primary_key, buf);
    // writeBinary(source, buf);
    // writeBinary(lifetime, buf);
    // writeBinary(layout, buf);
    // writeBinary(range, buf);
    // writeBinary(dict_settings, buf);
    writeBinary(clickhouse_db, buf);
    writeBinary(clickhouse_db, buf);
    writeBinary(clickhouse_invalidate_query, buf);
    writeBinary(clickhouse_query, buf);
}

void ASTDictionary::deserializeImpl(ReadBuffer & buf)
{
    /// TODO
    // readBinary(primary_key, buf);
    // readBinary(source, buf);
    // readBinary(lifetime, buf);
    // readBinary(layout, buf);
    // readBinary(range, buf);
    // readBinary(dict_settings, buf);
    readBinary(clickhouse_db, buf);
    readBinary(clickhouse_db, buf);
    readBinary(clickhouse_invalidate_query, buf);
    readBinary(clickhouse_query, buf);
}

ASTPtr ASTDictionary::deserialize(ReadBuffer & buf)
{
    auto res = std::make_shared<ASTDictionary>();
    res->deserializeImpl(buf);
    return res;
}

}
