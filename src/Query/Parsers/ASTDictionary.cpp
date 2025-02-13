#include <Query/Parsers/ASTDictionary.h>


namespace JDDB
{

ASTPtr ASTDictionaryRange::clone() const
{
    auto res = std::make_shared<ASTDictionaryRange>();
    res->min_attr_name = min_attr_name;
    res->max_attr_name = max_attr_name;
    return res;
}

ASTPtr ASTDictionaryLifetime::clone() const
{
    const auto res = std::make_shared<ASTDictionaryLifetime>();
    res->min_sec = min_sec;
    res->max_sec = max_sec;
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


ASTPtr ASTDictionarySettings::clone() const
{
    auto res = std::make_shared<ASTDictionarySettings>();
    res->changes = changes;

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

}
