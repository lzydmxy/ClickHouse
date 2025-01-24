#include <Query/Analyzer/Scope.h>

namespace DB
{

bool FieldDescription::matchName(const QualifiedName & target_name) const
{
    /// match name
    bool matched = !name.empty() && !target_name.empty() && name == target_name.getLast();

    /// match prefix
    if (matched)
        matched = prefix.hasSuffix(target_name.getPrefix());

    return matched;
}

bool FieldDescription::matchName(const String & target_name) const
{
    return !name.empty() && name == target_name;
}

FieldDescription FieldDescription::withNewName(const String & new_name) const
{
    return {new_name, type, prefix, origin_columns, substituted_by_asterisk, can_be_array_joined};
}

FieldDescription FieldDescription::withNewPrefix(const QualifiedName & new_prefix) const
{
    return {name, type, new_prefix, origin_columns, substituted_by_asterisk, can_be_array_joined};
}

void FieldDescription::copyOriginInfo(const FieldDescription & source_field)
{
    origin_columns = source_field.origin_columns;
}

ResolvedField::ResolvedField(ScopePtr scope_, size_t local_index_): scope(scope_), local_index(local_index_)
{
    hierarchy_index = scope->getHierarchyOffset() + local_index;
}

const FieldDescription & ResolvedField::getFieldDescription() const
{
    return scope->at(local_index);
}

ScopePtr Scope::getLocalParent() const
{
    return query_boundary ? nullptr : parent;
}

ScopePtr Scope::getQueryBoundaryScope() const
{
    return query_boundary ? this : parent->getQueryBoundaryScope();
}

ScopePtr Scope::getOuterQueryScope() const
{
    return getQueryBoundaryScope()->parent;
}

bool Scope::hasOuterQueryScope(ScopePtr other) const
{
    if (auto outer_query_scope = getOuterQueryScope())
        return outer_query_scope->isLocalScope(other) || outer_query_scope->hasOuterQueryScope(other);

    return false;
}

bool Scope::isLocalScope(ScopePtr other) const
{
    return this == other || (getLocalParent() != nullptr && getLocalParent()->isLocalScope(other));
}

size_t Scope::getHierarchyOffset() const
{
    const auto *local_parent = getLocalParent();
    return local_parent ? local_parent->getHierarchySize() : 0;
}

size_t Scope::getHierarchySize() const
{
    const auto *local_parent = getLocalParent();
    return (local_parent ? local_parent->getHierarchySize() : 0) + size();
}

Names Scope::getOriginColumns() const
{
    Names columns;
    columns.reserve(field_descriptions.size());

    for (const auto & field: field_descriptions)
        columns.push_back(field.getOriginColumnName());

    return columns;
}

NameSet Scope::getNamesSet() const
{
    NameSet columns;
    columns.reserve(field_descriptions.size());

    for (const auto & field : field_descriptions)
        columns.emplace(field.name);

    return columns;
}

String Scope::toString() const
{
    String str;
    for (const auto & field : field_descriptions)
        str += "(prefix: " + field.prefix.toString() + ", name: " + field.name + ", type: " + field.type->getName() + "), ";

    return str;
}

ScopePtr ScopeFactory::createScope(Scope::ScopeType type, ScopePtr parent, bool query_boundary, FieldDescriptions field_descriptions)
{
    scopes.emplace_back(type, parent, query_boundary, std::move(field_descriptions));
    return &scopes.back();
}

ScopePtr ScopeFactory::createLambdaScope(ScopePtr parent, FieldDescriptions field_descriptions)
{
    return createScope(Scope::ScopeType::LAMBDA, parent, false, std::move(field_descriptions));
}

}
