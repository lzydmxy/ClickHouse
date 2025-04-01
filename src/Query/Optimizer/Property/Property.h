#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <DataTypes/IDataType.h>
#include <Core/Field.h>
#include <Query/Analyzer/ASTEquals.h>
#include <Query/Protos/EnumMacros.h>
#include <Query/ProtosHelper/QueryProto.h>
#include <Query/Optimizer/Property/Equivalences.h>
#include <Parsers/ASTIdentifier.h>
#include <Query/Parsers/ASTClusterByElementExt.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/queryToString.h>
#include <Query/Optimizer/ConstHashAST.h>

//#include <Functions/FunctionsHashing.h>

/**
 * A partition operation divides a relation into disjoint subsets, called partitions.
 * A partition function defines which rows belong to which partitions. Partitioning
 * applies to the whole relation.
 */

namespace DB
{

using SymbolEquivalences = Equivalences<String>;
using SymbolEquivalencesPtr = std::shared_ptr<SymbolEquivalences>;
using PartitioningHandle = RPartitioningHandle;
using Component = RPartitioningComponent;
using ConstASTPtr = std::shared_ptr<const IAST>;
//using ConstASTMap = EqualityASTMap<ConstASTPtr>;

struct FieldWithType
{
    DataTypePtr type;
    Field value;
    bool operator==(const FieldWithType & other) const
    {
        return type->equals(*other.type) && value == other.value;
    }
    bool operator!=(const FieldWithType & other) const
    {
        return !operator==(other);
    }
};

class Constants
{
public:
    Constants() = default;
    explicit Constants(std::map<String, FieldWithType> values_) : values(std::move(values_))
    {
    }
    const std::map<String, FieldWithType> & getValues() const
    {
        return values;
    }
    bool contains(const String & name) const
    {
        return values.contains(name);
    }

    Constants translate(const std::unordered_map<String, String> & identities) const;
    //Constants normalize(const SymbolEquivalences & symbol_equivalences) const;
    String toString() const;

private:
    std::map<String, FieldWithType> values{};
};

using ConstantsSet = std::vector<Constants>;

class Partitioning
{
public:
/*
    ENUM_WITH_PROTO_CONVERTER(
        PartitioningHandle,
        Protos::Partitioning::PartitioningHandle,
        (SINGLE, 0),
        (COORDINATOR, 1),
        (FIXED_HASH, 2),
        (FIXED_ARBITRARY, 3),
        (FIXED_BROADCAST, 4),
        (SCALED_WRITER, 5),
        (BUCKET_TABLE, 6),
        (ARBITRARY, 7),
        (FIXED_PASSTHROUGH, 8),
        (UNKNOWN, 9));

    ENUM_WITH_PROTO_CONVERTER(
        Component, // enum name
        Protos::Partitioning::Component, // proto enum message
        (ANY, 0),
        (COORDINATOR, 1),
        (WORKER, 2));
    */

    Partitioning(const Names & columns_) : Partitioning(PartitioningHandle::Partitioning_Handle_FIXED_HASH, columns_) { }

    Partitioning(
        PartitioningHandle handle_ = PartitioningHandle::Partitioning_Handle_UNKNOWN,
        Names columns_ = {},
        bool require_handle_ = false,
        UInt64 buckets_ = 0,
        ASTPtr bucket_expr_ = nullptr,
        bool enforce_round_robin_ = true,
        Component component_ = Component::Partitioning_Component_ANY,
        bool exactly_match_ = false,
        bool satisfy_worker_ = false)
        : handle(handle_)
        , columns(std::move(columns_))
        , require_handle(require_handle_)
        , buckets(buckets_)
        , bucket_expr(bucket_expr_)
        , enforce_round_robin(enforce_round_robin_)
        , component(component_)
        , exactly_match(exactly_match_)
        , satisfy_worker(satisfy_worker_)
    {
    }
    void setHandle(PartitioningHandle handle_) { handle = handle_; }
    PartitioningHandle getHandle() const { return handle; }
    const Names & getColumns() const { return columns; }
    void setColumns(Names columns_)
    {
        columns = std::move(columns_);
    }
    UInt64 getBuckets() const { return buckets; }
    void setBuckets(UInt64 buckets_) { buckets = buckets_; }
    bool isEnforceRoundRobin() const { return enforce_round_robin; }
    void setEnforceRoundRobin(bool enforce_round_robin_) { enforce_round_robin = enforce_round_robin_; }
    bool isRequireHandle() const { return require_handle; }
    void setRequireHandle(bool require_handle_) { require_handle = require_handle_; }
    Component getComponent() const { return component; }
    void setComponent(Component component_) { component = component_; }
    bool isExactlyMatch() const { return exactly_match; }

    bool isPartitionHandle() const { return handle == PartitioningHandle::Partitioning_Handle_BUCKET_TABLE || handle == PartitioningHandle::Partitioning_Handle_FIXED_HASH; }

    bool isExchangeSchema(bool support_bucket_shuffle) const;
    bool isSimpleExchangeSchema(bool support_bucket_shuffle) const;

    ASTPtr getShuffleExpr() const;

    String getHashFunc(String default_func) const
    {
        //todo: now just a fake impl for build
        return "";
    }
    Array getParams() const
    {
        //todo: now just a fake impl for build
        return {};
    }

    void resetIfPartitionHandle()
    {
        if (!isPartitionHandle())
        {
            return;
        }
        this->columns = {};
        this->handle = PartitioningHandle::Partitioning_Handle_UNKNOWN;
        this->bucket_expr = nullptr;
        this->buckets = 0;
    }

    bool isSatisfyWorker() const
    {
        return satisfy_worker;
    }

    void setSatisfyWorker(bool satisfy_worker_)
    {
        this->satisfy_worker = satisfy_worker_;
    }

    Partitioning translate(const std::unordered_map<String, String> & identities, bool discard_not_in = false) const;
    Partitioning normalize(const SymbolEquivalences & symbol_equivalences) const;
    bool satisfy(const Partitioning &, const Constants & constants) const;
    bool isPartitionOn(const Partitioning &, const Constants & constants) const;

    bool isPreferred() const { return preferred; }
    void setPreferred(bool preferred_) { preferred = preferred_; }

    size_t hash() const;
    bool operator==(const Partitioning & other) const
    {
        return preferred == other.preferred && handle == other.handle && columns == other.columns && require_handle == other.require_handle && buckets == other.buckets
            && enforce_round_robin == other.enforce_round_robin && ASTEquality::compareTree(bucket_expr, other.bucket_expr);
    }

    ASTPtr getBucketExpr() const { return bucket_expr; }
    void setBucketExpr(const ASTPtr & bucket_expr_) { bucket_expr = bucket_expr_; }

    String toString() const;

private:
    PartitioningHandle handle;
    Names columns;
    bool require_handle;
    UInt64 buckets;
    ASTPtr bucket_expr;
    bool enforce_round_robin;
    Component component;
    bool exactly_match;
    bool satisfy_worker;
    bool preferred = false;
};

}

