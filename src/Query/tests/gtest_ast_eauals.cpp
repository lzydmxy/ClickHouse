#include <gtest/gtest.h>
#include <Query/Analyzer/ASTEquals.h>


using namespace DB;

namespace {
    // create different Literal
    ASTPtr makeIntLiteral(int value, const String & unique_name = "")
    {
        auto literal = std::make_shared<ASTLiteral>(value);
        literal->unique_column_name = unique_name;
        return literal;
    }

    ASTPtr makeStringLiteral(const String & str, bool use_legacy_name = false)
    {
        auto literal = std::make_shared<ASTLiteral>(str);
        literal->use_legacy_column_name_of_tuple = use_legacy_name;
        return literal;
    }

    ASTPtr makeNullLiteral()
    {
        return std::make_shared<ASTLiteral>(Field());
    }

    // create different Func
    ASTPtr createFunc(const String & name, const ASTPtr & arg)
    {
        return makeASTFunction(name, arg);
    }

    ASTPtr createFuncWIthTwoArgs(const String & name, const ASTPtr & arg1, const ASTPtr & arg2)
    {
        return makeASTFunction(name, arg1, arg2);
    }

    ASTPtr createSimpleFunc(const String & name)
    {
        return makeASTFunction(name);
    }

    // create different Identifier
    ASTPtr makeIdentifier(const String & name)
    {
        auto ident = std::make_shared<ASTIdentifier>(name);
        return ident;
    }

    ASTPtr createCompoundID(const std::vector<String> & parts, bool special = false)
    {
        return std::make_shared<ASTIdentifier>(std::vector<String>(parts), special);
    }

    // create different TableIdentifier
    ASTPtr createTableID(const String & db, const String & table, UUID uuid = UUIDHelpers::Nil)
    {
        auto id = std::make_shared<ASTTableIdentifier>(db, table);
        id->uuid = uuid;
        return id;
    }

    ASTPtr createSimpleTableID(const String & table)
    {
        return std::make_shared<ASTTableIdentifier>(table);
    }

    // create different WindowDefinition
    ASTPtr createBaseWindowDef()
    {
        auto def = std::make_shared<ASTWindowDefinition>();
        def->partition_by = makeASTFunction("tuple", makeIdentifier("col1"));
        def->order_by = makeASTFunction("tuple", makeIdentifier("col2"));
        def->children.push_back(def->partition_by);
        def->children.push_back(def->order_by);
        return def;
    }

    void addFrameToWindow(ASTWindowDefinition & def,
                          WindowFrame::FrameType type,
                          WindowFrame::BoundaryType begin_type,
                          WindowFrame::BoundaryType end_type)
    {
        def.frame_is_default = false;
        def.frame_type = type;
        def.frame_begin_type = begin_type;
        def.frame_end_type = end_type;
    }

    // create different Subquery
    ASTPtr createSubquery(ASTPtr query, const String & cte_name = "")
    {
        auto subquery = std::make_shared<ASTSubquery>(query);
        subquery->cte_name = cte_name;
        return subquery;
    }

    ASTPtr createSimpleSelect()
    {
        return makeASTFunction("select", makeIntLiteral(1));
    }

    // create different ArrayJoin
    ASTPtr createArrayJoin(DB::ASTArrayJoin::Kind kind, const ASTs & exprs)
    {
        auto array_join = std::make_shared<DB::ASTArrayJoin>();
        array_join->kind = kind;
        array_join->expression_list = std::make_shared<ASTExpressionList>();
        array_join->expression_list->children = exprs;
        array_join->children.push_back(array_join->expression_list);
        return array_join;
    }

    ASTPtr createAliasedExpr(const String & name, const String & alias)
    {
        auto expr = makeIdentifier(name);
        expr->setAlias(alias);
        return expr;
    }

    // create different OrderByElement
    ASTPtr createOrderElement(int direction, const String& column)
    {
        auto elem = std::make_shared<ASTOrderByElement>();
        elem->direction = direction;
        elem->children.push_back(makeIdentifier(column));
        return elem;
    }

    void addCollation(ASTOrderByElement & elem, const String & collation)
    {
        elem.setCollation(makeStringLiteral(collation));
    }

    void addFillClauses(ASTOrderByElement & elem, const ASTPtr & from, const ASTPtr & to, const ASTPtr & step)
    {
        elem.with_fill = true;
        elem.setFillFrom(from);
        elem.setFillTo(to);
        elem.setFillStep(step);
    }

    // create different SetQuery
    ASTPtr createSetQuery(const SettingsChanges& changes = {},
                      const std::vector<String>& defaults = {},
                      const NameToNameVector& params = {})
    {
        auto set_query = std::make_shared<ASTSetQuery>();
        set_query->changes = changes;
        set_query->default_settings = defaults;
        set_query->query_parameters = params;
        return set_query;
    }

    SettingsChanges::value_type makeSetting(const String& key, const Field& value)
    {
        return {key, value};
    }

    // create different TableColumnReference
    ASTPtr createTableColumnReference(const IStorage * storage, size_t unique_id, const String & column)
    {
        auto ref = std::make_shared<ASTTableColumnReference>(storage, unique_id, column);
        return ref;
    }

    class MockStorage : public IStorage
    {
        public:
            MockStorage(const StorageID &table_id_) : IStorage(table_id_) {}

            std::string getName() const override { return "MockStorage"; }
    };

    // create different SelectQuery
    ASTPtr createBaseSelect()
    {
        auto query = std::make_shared<ASTSelectQuery>();
        query->setExpression(ASTSelectQuery::Expression::SELECT, makeASTFunction("tuple", makeIdentifier("col1")));
        query->setExpression(ASTSelectQuery::Expression::TABLES, makeASTFunction("table", makeIdentifier("tbl")));
        return query;
    }

    void addWhereClause(ASTSelectQuery & query, ASTPtr condition)
    {
        query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(condition));
    }
}

TEST(ASTEqualsTest, CompareASTLiteral)
{
    {
        // same value test
        auto lit1 = makeIntLiteral(42);
        auto lit2 = makeIntLiteral(42);
        EXPECT_TRUE(DB::ASTEquality::compareTree(lit1, lit2));

        auto lit_int = makeIntLiteral(42);
        auto lit_str = makeStringLiteral("42");
        // Int vs String
        EXPECT_FALSE(DB::ASTEquality::compareTree(lit_int, lit_str));
    }
    {
        // unique column name test: only compare value
        auto lit1 = makeIntLiteral(42, "col_a");
        auto lit2 = makeIntLiteral(42, "col_b");
        EXPECT_TRUE(DB::ASTEquality::compareTree(lit1, lit2));
    }
    {
        // tuple legacy name test
        // use_legacy_column_name_of_tuple = true
        auto lit1 = makeStringLiteral("(1,2)", true);
        // use_legacy_column_name_of_tuple = false
        auto lit2 = makeStringLiteral("(1,2)", false);
        EXPECT_TRUE(DB::ASTEquality::compareTree(lit1, lit2));
    }
    {
        // null value test
        auto null1 = makeNullLiteral();
        auto null2 = makeNullLiteral();
        auto non_null = makeIntLiteral(0);

        EXPECT_TRUE(DB::ASTEquality::compareTree(null1, null2));
        EXPECT_FALSE(DB::ASTEquality::compareTree(null1, non_null));
    }
    {
        // hash consistency test
        auto lit1 = makeStringLiteral("hello");
        auto lit2 = makeStringLiteral("hello");

        size_t hash1 = DB::ASTEquality::hashTree(lit1);
        size_t hash2 = DB::ASTEquality::hashTree(lit2);
        EXPECT_EQ(hash1, hash2);
    }
    {
        // clone test
        auto original = makeIntLiteral(100, "special_col");

        auto cloned = original->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(original, cloned));
    }
    {
        // empty String VS Null
        auto empty_str = makeStringLiteral("");
        auto null_lit = makeNullLiteral();
        EXPECT_FALSE(DB::ASTEquality::compareTree(empty_str, null_lit));
    }
}

TEST(ASTEqualsTest, CompareASTFunction)
{
    {
        // same func name and args
        auto func1 = createFuncWIthTwoArgs("add", makeIntLiteral(1), makeIntLiteral(2));
        auto func2 = createFuncWIthTwoArgs("add", makeIntLiteral(1), makeIntLiteral(2));
        EXPECT_TRUE(DB::ASTEquality::compareTree(func1, func2));
    }
    {
        // different func name
        auto func1 = createFuncWIthTwoArgs("add", makeIntLiteral(1), makeIntLiteral(2));
        auto func2 = createFuncWIthTwoArgs("sub", makeIntLiteral(1), makeIntLiteral(2));
        EXPECT_FALSE(DB::ASTEquality::compareTree(func1, func2));
    }
    {
        // different number of args
        auto func1 = createFuncWIthTwoArgs("add", makeIntLiteral(1), makeIntLiteral(2));
        auto func2 = createFunc("add", makeIntLiteral(1));
        auto func3 = createSimpleFunc("add");
        EXPECT_FALSE(DB::ASTEquality::compareTree(func1, func2));
        EXPECT_FALSE(DB::ASTEquality::compareTree(func1, func3));

        // different content of args
        auto func4 = createFuncWIthTwoArgs("add", makeIntLiteral(1), makeStringLiteral("x"));
        auto func5 = createFuncWIthTwoArgs("add", makeIntLiteral(1), makeStringLiteral("y"));
        EXPECT_FALSE(DB::ASTEquality::compareTree(func3, func4));
    }
    {
        // window definition test
        auto func1 = createSimpleFunc("rank");
        func1->as<ASTFunction>()->is_window_function = true;
        func1->as<ASTFunction>()->window_definition = makeASTFunction("window_spec");
        func1->children.push_back(func1->as<ASTFunction>()->window_definition);

        auto func2 = func1->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(func1, func2));

        // change window definition
        func2->as<ASTFunction>()->window_definition = makeASTFunction("another_spec");
        EXPECT_FALSE(DB::ASTEquality::compareTree(func1, func2));
    }
    {
        // lambda func test
        // lambda(x, x + 1)
        auto lambda_args = createFuncWIthTwoArgs("tuple", makeIdentifier("x"), makeIdentifier("y"));
        auto lambda_expr = createFuncWIthTwoArgs("plus", makeIdentifier("x"), makeIntLiteral(1));
        auto lambda_func = createFuncWIthTwoArgs("lambda", lambda_args, lambda_expr);
        lambda_func->as<ASTFunction>()->is_lambda_function = true;

        auto cloned = lambda_func->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(lambda_func, cloned));

        // change lambda func
        auto modified_expr = createFuncWIthTwoArgs("minus", makeIdentifier("x"), makeIntLiteral(1));
        cloned->as<ASTFunction>()->arguments->children[1] = modified_expr;
        EXPECT_FALSE(DB::ASTEquality::compareTree(lambda_func, cloned));
    }
    {
        // hash consistency test
        auto func1 = createFunc("sqrt", makeIntLiteral(2));
        auto func2 = createFunc("sqrt", makeIntLiteral(2));

        size_t hash1 = DB::ASTEquality::hashTree(func1);
        size_t hash2 = DB::ASTEquality::hashTree(func2);
        EXPECT_EQ(hash1, hash2);

        // change argument
        func2->as<ASTFunction>()->arguments->children[0] = makeIntLiteral(3);
        size_t hash3 = DB::ASTEquality::hashTree(func2);
        EXPECT_NE(hash1, hash3);
    }
    {
        // custom comparator test
        auto func1 = createFunc("secret_func", makeIntLiteral(42));
        auto func2 = createFunc("secret_func", makeIntLiteral(42));

        // ignore difference of arguments
        auto ignoreArgsComparator = [](const ASTPtr& left, const ASTPtr& right) -> std::optional<bool> {
            if (left->as<ASTFunction>() && right->as<ASTFunction>()) {
                return std::make_optional(left->as<ASTFunction>()->name == right->as<ASTFunction>()->name);
            }
            return std::nullopt;
        };

        EXPECT_TRUE(DB::ASTEquality::compareTree(func1, func2, ignoreArgsComparator));

        // change func name
        func2->as<ASTFunction>()->name = "another_func";
        EXPECT_FALSE(DB::ASTEquality::compareTree(func1, func2, ignoreArgsComparator));
    }
    {
        // empty arguments test
        auto func1 = createSimpleFunc("now");
        auto func2 = createSimpleFunc("now");
        func1->as<ASTFunction>()->no_empty_args = true;
        func2->as<ASTFunction>()->no_empty_args = false;
        EXPECT_TRUE(DB::ASTEquality::compareTree(func1, func2));
    }
    {
        // null handling test
        ASTPtr null_arg;
        auto func1 = createFuncWIthTwoArgs("func", null_arg, makeIntLiteral(0));
        auto func2 = createFuncWIthTwoArgs("func", null_arg, makeIntLiteral(0));
        EXPECT_TRUE(DB::ASTEquality::compareTree(func1, func2));
    }
}

TEST(ASTEqualsTest, CompareASTIdentifier)
{
    {
        // simple equality test
        auto id1 = makeIdentifier("col1");
        auto id2 = makeIdentifier("col1");
        EXPECT_TRUE(DB::ASTEquality::compareTree(id1, id2));

        auto id3 = makeIdentifier("col1");
        auto id4 = makeIdentifier("col2");
        EXPECT_FALSE(DB::ASTEquality::compareTree(id3, id4));
    }
    {
        // compound equality test
        auto id1 = createCompoundID({"db", "table", "col"});
        auto id2 = createCompoundID({"db", "table", "col"});
        EXPECT_TRUE(DB::ASTEquality::compareTree(id1, id2));

        auto id3 = createCompoundID({"db", "table"});
        auto id4 = createCompoundID({"db", "view"});
        EXPECT_FALSE(DB::ASTEquality::compareTree(id3, id4));

        auto id5 = createCompoundID({"a", "b"});
        auto id6 = createCompoundID({"b", "a"});
        EXPECT_FALSE(DB::ASTEquality::compareTree(id5, id6));
    }
    {
        // hash consistency test
        auto id1 = createCompoundID({"db", "table"});
        auto id2 = createCompoundID({"db", "table"});

        size_t hash1 = DB::ASTEquality::hashTree(id1);
        size_t hash2 = DB::ASTEquality::hashTree(id2);
        EXPECT_EQ(hash1, hash2);

        id2->as<ASTIdentifier>()->setShortName("view");
        size_t hash3 = DB::ASTEquality::hashTree(id2);
        EXPECT_NE(hash1, hash3);
    }
    {
        // clone test
        auto original = createCompoundID({"a", "b", "c"}, true);

        auto cloned = original->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(original, cloned));
    }
    {
        // empty name test
        auto empty1 = makeIdentifier("");
        auto empty2 = makeIdentifier("");
        EXPECT_TRUE(DB::ASTEquality::compareTree(empty1, empty2));
    }
    {
        // single vs compound
        auto simple = makeIdentifier("db.table");
        auto compound = createCompoundID({"db", "table"});
        EXPECT_TRUE(DB::ASTEquality::compareTree(simple, compound));
    }
}

TEST(ASTEqualsTest, CompareASTTableIdentifier)
{
    {
        // simple equality test
        auto tbl1 = createTableID("db", "table");
        auto tbl2 = createTableID("db", "table");
        EXPECT_TRUE(DB::ASTEquality::compareTree(tbl1, tbl2));

        auto tbl3 = createTableID("db1", "table");
        auto tbl4 = createTableID("db2", "table");
        EXPECT_FALSE(DB::ASTEquality::compareTree(tbl3, tbl4));
    }
    {
        // UUID test
        auto uuid1 = UUIDHelpers::generateV4();
        auto uuid2 = UUIDHelpers::generateV4();

        auto tbl1 = createTableID("db1", "table", uuid1);
        auto tbl2 = createTableID("db1", "table", uuid1);
        auto tbl3 = createTableID("db2", "table", uuid2);

        EXPECT_TRUE(DB::ASTEquality::compareTree(tbl1, tbl2));
        EXPECT_FALSE(DB::ASTEquality::compareTree(tbl1, tbl3));
    }
    {
        // get test
        auto tbl = createTableID("my_db", "my_table");
        auto tbl2 = makeIdentifier("my_table");

        EXPECT_EQ(tbl->as<ASTTableIdentifier>()->getDatabaseName(), "my_db");
        EXPECT_TRUE(DB::ASTEquality::compareTree(tbl->as<ASTTableIdentifier>()->getTable(), tbl2));
    }
    {
        // ASTTableIdentifier VS ASTIdentifier
        auto base_id = createCompoundID({"db", "table"});
        auto tbl_id = createTableID("db", "table");

        EXPECT_FALSE(DB::ASTEquality::compareTree(base_id, tbl_id));
    }
    {
        // hash consistency test
        auto uuid1 = UUIDHelpers::generateV4();
        auto uuid2 = UUIDHelpers::generateV4();
        auto tbl1 = createTableID("db1", "table", uuid1);
        auto tbl2 = createTableID("db1", "table", uuid1);
        auto tbl3 = createTableID("db2", "table", uuid2);
        auto tbl4 = createTableID("db2", "table");

        size_t hash1 = DB::ASTEquality::hashTree(tbl1);
        size_t hash2 = DB::ASTEquality::hashTree(tbl2);
        size_t hash3 = DB::ASTEquality::hashTree(tbl3);
        size_t hash4 = DB::ASTEquality::hashTree(tbl4);

        EXPECT_EQ(hash1, hash2);
        EXPECT_NE(hash1, hash3);
        EXPECT_EQ(hash3, hash4);
    }
    {
        // clone test
        auto uuid = UUIDHelpers::generateV4();
        auto tbl1 = createTableID("db", "table", uuid);

        auto tbl2 = tbl1->as<ASTTableIdentifier>()->clone();

        EXPECT_TRUE(DB::ASTEquality::compareTree(tbl1, tbl2));
    }
    {
        // empty database test
        auto tbl1 = createTableID("", "table");
        auto tbl2 = createTableID("", "table");
        auto tbl3 = createSimpleTableID("table");

        EXPECT_TRUE(DB::ASTEquality::compareTree(tbl1, tbl2));
        EXPECT_TRUE(DB::ASTEquality::compareTree(tbl1, tbl3));
    }
    {
        // NULL UUID handle test
        auto tbl1 = createTableID("db", "table", UUIDHelpers::Nil);
        auto tbl2 = createTableID("db", "table");
        EXPECT_TRUE(DB::ASTEquality::compareTree(tbl1, tbl2));
    }
}

TEST(ASTEqualsTest, CompareASTWindowDefinition)
{
    {
        // simple equality test
        auto win1 = createBaseWindowDef();
        auto win2 = win1->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(win1, win2));

        win2->as<ASTWindowDefinition>()->parent_window_name = "parent";
        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, win2));
    }
    {
        // different partition by test
        auto win1 = createBaseWindowDef();
        auto win2 = win1->clone();
        win2->as<ASTWindowDefinition>()->partition_by->children[0] = makeIdentifier("col3");
        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, win2));
    }
    {
        // null order by test
        auto win1 = createBaseWindowDef();
        auto win2 = win1->clone();
        win2->as<ASTWindowDefinition>()->order_by = nullptr;
        win2->as<ASTWindowDefinition>()->children.erase(win2->as<ASTWindowDefinition>()->children.begin() + 1);
        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, win2));
    }
    {
        // different frame type test
        auto win1 = createBaseWindowDef();
        auto win2 = win1->clone();
        addFrameToWindow(*win1->as<ASTWindowDefinition>(), WindowFrame::FrameType::RANGE, WindowFrame::BoundaryType::Unbounded, WindowFrame::BoundaryType::Current);
        addFrameToWindow(*win2->as<ASTWindowDefinition>(), WindowFrame::FrameType::ROWS, WindowFrame::BoundaryType::Unbounded, WindowFrame::BoundaryType::Current);
        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, win2));
    }
    {
        // different frame boundary offset test
        auto win1 = createBaseWindowDef();
        auto win2 = win1->clone();

        win1->as<ASTWindowDefinition>()->frame_begin_offset = makeIntLiteral(1);
        win2->as<ASTWindowDefinition>()->frame_begin_offset = makeIntLiteral(2);
        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, win2));
    }
    {
        // different frame type test
        auto win1 = createBaseWindowDef();
        auto win2 = win1->clone();

        addFrameToWindow(*win2->as<ASTWindowDefinition>(), WindowFrame::FrameType::ROWS, WindowFrame::BoundaryType::Unbounded, WindowFrame::BoundaryType::Current);

        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, win2));
    }
    {
        // hash consistency test
        auto win1 = createBaseWindowDef();
        addFrameToWindow(*win1->as<ASTWindowDefinition>(), WindowFrame::FrameType::ROWS,
                         WindowFrame::BoundaryType::Offset, WindowFrame::BoundaryType::Offset);
        win1->as<ASTWindowDefinition>()->frame_begin_offset = makeIntLiteral(3);
        win1->as<ASTWindowDefinition>()->children.push_back(win1->as<ASTWindowDefinition>()->frame_begin_offset);
        win1->as<ASTWindowDefinition>()->frame_end_offset = makeIntLiteral(5);
        win1->as<ASTWindowDefinition>()->children.push_back(win1->as<ASTWindowDefinition>()->frame_end_offset);

        auto win2 = win1->clone();
        size_t hash1 = DB::ASTEquality::hashTree(win1);
        size_t hash2 = DB::ASTEquality::hashTree(win2);
        EXPECT_EQ(hash1, hash2);

        win2->as<ASTWindowDefinition>()->partition_by = nullptr;
        win2->as<ASTWindowDefinition>()->children.erase(win2->as<ASTWindowDefinition>()->children.begin());
        size_t hash3 = DB::ASTEquality::hashTree(win2);
        EXPECT_NE(hash1, hash3);
    }
    {
        // null component handling test
        auto win1 = createBaseWindowDef();
        auto win2 = createBaseWindowDef();

        win1->as<ASTWindowDefinition>()->partition_by = nullptr;
        win1->children.erase(win1->children.begin());
        win2->as<ASTWindowDefinition>()->order_by = nullptr;
        win2->children.erase(win2->children.begin() + 1);

        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, win2));
        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, createBaseWindowDef()));
    }
    {
        // frame direction mismatch test
        auto win1 = createBaseWindowDef();
        auto win2 = win1->clone();

        addFrameToWindow(*win1->as<ASTWindowDefinition>(), WindowFrame::FrameType::ROWS,
                         WindowFrame::BoundaryType::Offset, WindowFrame::BoundaryType::Offset);
        addFrameToWindow(*win2->as<ASTWindowDefinition>(), WindowFrame::FrameType::ROWS,
                         WindowFrame::BoundaryType::Offset, WindowFrame::BoundaryType::Offset);

        win1->as<ASTWindowDefinition>()->frame_begin_preceding = true;
        win2->as<ASTWindowDefinition>()->frame_begin_preceding = false;
        EXPECT_FALSE(DB::ASTEquality::compareTree(win1, win2));
    }
}

TEST(ASTEqualsTest, CompareASTSubquery)
{
    {
        // simple equality test
        auto query = createSimpleSelect();
        auto sub1 = createSubquery(query);
        auto sub2 = createSubquery(query->clone());
        EXPECT_TRUE(DB::ASTEquality::compareTree(sub1, sub2));
    }
    {
        // different cte test
        auto sub1 = createSubquery(createSimpleSelect(), "cte_a");
        auto sub2 = createSubquery(createSimpleSelect(), "cte_b");
        EXPECT_FALSE(DB::ASTEquality::compareTree(sub1, sub2));
    }
    {
        // different subquery content
        auto sub1 = createSubquery(makeASTFunction("select", makeIntLiteral(1)));
        auto sub2 = createSubquery(makeASTFunction("select", makeIntLiteral(2)));
        EXPECT_FALSE(DB::ASTEquality::compareTree(sub1, sub2));
    }
    {
        // clone test
        auto original = createSubquery(createSimpleSelect(), "my_cte");
        original->setAlias("alias");
        auto cloned = original->clone();

        EXPECT_TRUE(DB::ASTEquality::compareTree(original, cloned));
    }
    {
        // hash consistency test
        auto sub1 = createSubquery(createSimpleSelect(), "cte");
        auto sub2 = createSubquery(createSimpleSelect()->clone(), "cte");

        size_t hash1 = DB::ASTEquality::hashTree(sub1);
        size_t hash2 = DB::ASTEquality::hashTree(sub2);
        EXPECT_EQ(hash1, hash2);

        // change cte name
        sub2->as<ASTSubquery>()->cte_name = "another_cte";
        size_t hash3 = DB::ASTEquality::hashTree(sub2);
        EXPECT_EQ(hash1, hash3);
    }
    {
        // null subquery handle test
        ASTPtr null_query;
        auto sub1 = createSubquery(null_query);
        auto sub2 = createSubquery(null_query);
        auto sub3 = createSubquery(createSimpleSelect());

        EXPECT_TRUE(DB::ASTEquality::compareTree(sub1, sub2));
        EXPECT_FALSE(DB::ASTEquality::compareTree(sub1, sub3));
    }
    {
        // different alias test
        auto sub1 = createSubquery(createSimpleSelect());
        auto sub2 = createSubquery(createSimpleSelect());
        sub1->setAlias("a");
        sub2->setAlias("b");

        EXPECT_TRUE(DB::ASTEquality::compareTree(sub1, sub2));
    }
}

TEST(ASTEqualsTest, CompareASTArrayJoin)
{
    {
        // simple equality test
        auto expr1 = makeIdentifier("arr");
        auto expr2 = makeIdentifier("nested");

        auto join1 = createArrayJoin(ASTArrayJoin::Kind::Inner, {expr1, expr2});
        auto join2 = createArrayJoin(ASTArrayJoin::Kind::Inner, {expr1->clone(), expr2->clone()});
        EXPECT_TRUE(DB::ASTEquality::compareTree(join1, join2));
    }
    {
        // different kind test
        auto expr = makeIdentifier("arr");
        auto inner_join = createArrayJoin(ASTArrayJoin::Kind::Inner, {expr});
        auto left_join = createArrayJoin(ASTArrayJoin::Kind::Left, {expr});
        EXPECT_FALSE(DB::ASTEquality::compareTree(inner_join, left_join));
    }
    {
        // different expression list test
        auto base_join = createArrayJoin(ASTArrayJoin::Kind::Inner, {makeIdentifier("arr")});

        auto two_exprs = createArrayJoin(ASTArrayJoin::Kind::Inner, {makeIdentifier("arr"), makeIdentifier("nested")});
        EXPECT_FALSE(DB::ASTEquality::compareTree(base_join, two_exprs));

        auto diff_expr = createArrayJoin(ASTArrayJoin::Kind::Inner, {makeIdentifier("different")});
        EXPECT_FALSE(DB::ASTEquality::compareTree(base_join, diff_expr));
    }
    {
        // hash consistency test
        auto join1 = createArrayJoin(ASTArrayJoin::Kind::Left, {createAliasedExpr("arr", "a")});
        auto join2 = join1->clone();

        size_t hash1 = DB::ASTEquality::hashTree(join1);
        size_t hash2 = DB::ASTEquality::hashTree(join2);
        EXPECT_EQ(hash1, hash2);

        auto expr = join2->as<ASTArrayJoin>()->expression_list;
        expr->children.pop_back();
        join2->as<ASTArrayJoin>()->children.pop_back();
        if (expr)
            join2->as<ASTArrayJoin>()->children.push_back(expr);

        size_t hash3 = DB::ASTEquality::hashTree(join2);
        EXPECT_NE(hash1, hash3);
    }
    {
        // empty expression list test
        auto empty1 = createArrayJoin(DB::ASTArrayJoin::Kind::Inner, {});
        auto empty2 = createArrayJoin(DB::ASTArrayJoin::Kind::Inner, {});
        auto non_empty = createArrayJoin(DB::ASTArrayJoin::Kind::Inner, {makeIdentifier("x")});

        EXPECT_TRUE(DB::ASTEquality::compareTree(empty1, empty2));
        EXPECT_FALSE(DB::ASTEquality::compareTree(empty1, non_empty));
    }
    {
        // different order of expression list test
        auto expr_a = makeIdentifier("a");
        auto expr_b = makeIdentifier("b");

        auto join_ab = createArrayJoin(DB::ASTArrayJoin::Kind::Inner, {expr_a, expr_b});
        auto join_ba = createArrayJoin(DB::ASTArrayJoin::Kind::Inner, {expr_b, expr_a});
        EXPECT_FALSE(DB::ASTEquality::compareTree(join_ab, join_ba));
    }
}

TEST(ASTEqualsTest, CompareASTOrderByElement)
{
    {
        // simple equality test
        auto elem1 = createOrderElement(1, "score");
        auto elem2 = createOrderElement(1, "score");
        EXPECT_TRUE(DB::ASTEquality::compareTree(elem1, elem2));
    }
    {
        // different null handle test
        auto elem1 = createOrderElement(1, "id");
        auto elem2 = elem1->clone();
        elem2->as<ASTOrderByElement>()->nulls_direction = -1;
        elem2->as<ASTOrderByElement>()->nulls_direction_was_explicitly_specified = true;
        EXPECT_FALSE(DB::ASTEquality::compareTree(elem1, elem2));
    }
    {
        // fill clause test
        auto elem1 = createOrderElement(1, "time");
        addFillClauses(*elem1->as<ASTOrderByElement>(), makeIntLiteral(0), makeIntLiteral(100), makeIntLiteral(5));

        auto elem2 = elem1->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(elem1, elem2));

        // change fill step
        elem2->as<ASTOrderByElement>()->setFillStep(makeIntLiteral(10));
        EXPECT_FALSE(DB::ASTEquality::compareTree(elem1, elem2));
    }
    {
        // different collation test
        auto elem1 = createOrderElement(1, "name");
        addCollation(*elem1->as<ASTOrderByElement>(), "utf8");

        auto elem2 = elem1->clone();
        elem2->as<ASTOrderByElement>()->setCollation(makeStringLiteral("binary"));
        EXPECT_FALSE(DB::ASTEquality::compareTree(elem1, elem2));
    }
    {
        // hash consistency test
        auto elem = createOrderElement(-1, "timestamp");
        elem->as<ASTOrderByElement>()->nulls_direction_was_explicitly_specified = true;
        addFillClauses(*elem->as<ASTOrderByElement>(), makeStringLiteral("2023-01-01"), nullptr, nullptr);

        size_t hash1 = DB::ASTEquality::hashTree(elem);

        auto elem2 = elem->clone();
        elem2->as<ASTOrderByElement>()->nulls_direction_was_explicitly_specified = false;
        size_t hash2 = DB::ASTEquality::hashTree(elem2);
        EXPECT_EQ(hash1, hash2);
    }
    {
        // partial fill clause test
        auto elem1 = createOrderElement(1, "id");
        elem1->as<ASTOrderByElement>()->with_fill = true;
        // only FILL_FROM
        elem1->as<ASTOrderByElement>()->setFillFrom(makeIntLiteral(0));

        auto elem2 = elem1->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(elem1, elem2));

        // add FILL_TO
        elem2->as<ASTOrderByElement>()->setFillTo(makeIntLiteral(100));
        EXPECT_FALSE(DB::ASTEquality::compareTree(elem1, elem2));
    }
    {
        // explicit nulls direction flag test
        auto elem1 = createOrderElement(1, "value");
        auto elem2 = elem1->clone();
        elem2->as<ASTOrderByElement>()->nulls_direction_was_explicitly_specified = true;
        EXPECT_FALSE(DB::ASTEquality::compareTree(elem1, elem2));
    }
}

TEST(ASTEqualsTest, CompareASTSetQuery)
{
    {
        // simple equality test
        auto set1 = createSetQuery({makeSetting("max_memory", 1000)});
        auto set2 = createSetQuery({makeSetting("max_memory", 1000)});
        EXPECT_TRUE(DB::ASTEquality::compareTree(set1, set2));

        auto set3 = createSetQuery({makeSetting("timeout", 30)});
        auto set4 = createSetQuery({makeSetting("timeout", 60)});
        EXPECT_FALSE(DB::ASTEquality::compareTree(set3, set4));
    }
    {
        // default setting test
        auto set1 = createSetQuery({}, {"merge_tree"});
        auto set2 = createSetQuery({}, {"merge_tree"});
        auto set3 = createSetQuery({}, {"distributed"});

        EXPECT_TRUE(DB::ASTEquality::compareTree(set1, set2));
        EXPECT_TRUE(DB::ASTEquality::compareTree(set1, set3));
    }
    {
        // query parameters test
        NameToNameVector params1 = {{"param1", "value1"}};
        NameToNameVector params2 = {{"param2", "value2"}};

        auto set1 = createSetQuery({}, {}, params1);
        auto set2 = createSetQuery({}, {}, params2);
        EXPECT_TRUE(DB::ASTEquality::compareTree(set1, set2));
    }
    {
        // standalone flag test
        auto set1 = createSetQuery();
        auto set2 = createSetQuery();
        set2->as<ASTSetQuery>()->is_standalone = false;
        EXPECT_TRUE(DB::ASTEquality::compareTree(set1, set2));
    }
    {
        // hash consistency test
        auto set1 = createSetQuery({makeSetting("concurrent", 1)}, {"log_queries"});
        auto set2 = set1->clone();

        size_t hash1 = DB::ASTEquality::hashTree(set1);
        size_t hash2 = DB::ASTEquality::hashTree(set2);
        EXPECT_EQ(hash1, hash2);

        set2->as<ASTSetQuery>()->print_in_format = false;
        size_t hash3 = DB::ASTEquality::hashTree(set2);
        EXPECT_EQ(hash1, hash3);
    }
    {
        // empty test
        auto empty1 = createSetQuery();
        auto empty2 = createSetQuery();
        auto non_empty = createSetQuery({makeSetting("tmp", 0)});

        EXPECT_TRUE(DB::ASTEquality::compareTree(empty1, empty2));
        EXPECT_FALSE(DB::ASTEquality::compareTree(empty1, non_empty));
    }
    {
        // different setting order test
        SettingsChanges changes1 = {makeSetting("a", 1), makeSetting("b", 2)};
        SettingsChanges changes2 = {makeSetting("b", 2), makeSetting("a", 1)};

        auto set1 = createSetQuery(changes1);
        auto set2 = createSetQuery(changes2);
        EXPECT_FALSE(DB::ASTEquality::compareTree(set1, set2));
    }
}

TEST(ASTEqualsTest, CompareASTTableColumnReference)
{
    auto storage_id1 = StorageID("db1", "table1", UUIDHelpers::generateV4());
    auto storage_id2 = StorageID("db2", "table2", UUIDHelpers::generateV4());
    MockStorage storage1(storage_id1);
    MockStorage storage2(storage_id2);

    {
        // simple equality test
        auto ref1 = createTableColumnReference(&storage1, 1001, "price");
        auto ref2 = createTableColumnReference(&storage1, 1001, "price");
        EXPECT_TRUE(DB::ASTEquality::compareTree(ref1, ref2));

        auto ref3 = createTableColumnReference(&storage1, 1001, "price");
        auto ref4 = createTableColumnReference(&storage2, 1001, "price");
        EXPECT_FALSE(DB::ASTEquality::compareTree(ref3, ref4));

        auto ref5 = createTableColumnReference(&storage1, 1001, "price");
        auto ref6 = createTableColumnReference(&storage1, 1002, "price");
        EXPECT_FALSE(DB::ASTEquality::compareTree(ref5, ref6));

        auto ref7 = createTableColumnReference(&storage1, 1001, "price");
        auto ref8 = createTableColumnReference(&storage1, 1001, "cost");
        EXPECT_FALSE(DB::ASTEquality::compareTree(ref7, ref8));
    }
    {
        // clone test
        auto ref1 = createTableColumnReference(&storage1, 1001, "price");
        auto ref2 = ref1->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(ref1, ref2));
    }
    {
        // hash consistency test
        auto ref1 = createTableColumnReference(&storage1, 1001, "price");
        auto ref2 = ref1->clone();
        size_t hash1 = DB::ASTEquality::hashTree(ref1);
        size_t hash2 = DB::ASTEquality::hashTree(ref2);
        EXPECT_EQ(hash1, hash2);

        auto ref3 = createTableColumnReference(&storage1, 1001, "price_2");
        size_t hash3 = DB::ASTEquality::hashTree(ref3);
        EXPECT_NE(hash1, hash3);
    }
    {
        // null storage test
        auto ref1 = createTableColumnReference(nullptr, 0, "dummy");
        auto ref2 = createTableColumnReference(nullptr, 0, "dummy");
        EXPECT_TRUE(DB::ASTEquality::compareTree(ref1, ref2));
    }
    {
        // max id test
        auto ref1 = createTableColumnReference(&storage1, SIZE_MAX, "id");
        auto ref2 = createTableColumnReference(&storage1, SIZE_MAX, "id");
        EXPECT_TRUE(DB::ASTEquality::compareTree(ref1, ref2));
    }
}

TEST(ASTEqualsTest, CompareASTSelectQuery)
{
    {
        // simple equality test
        auto select1 = createBaseSelect();
        auto select2 = select1->clone();
        EXPECT_TRUE(DB::ASTEquality::compareTree(select1, select2));
    }
    {
        // different clause test
        auto base = createBaseSelect();
        auto with_where = createBaseSelect();
        addWhereClause(*with_where->as<ASTSelectQuery>(), makeASTFunction("equals", makeIdentifier("id"), makeIntLiteral(1)));

        EXPECT_FALSE(DB::ASTEquality::compareTree(base, with_where));
    }
    {
        // different flag test
        auto q1 = createBaseSelect();
        auto q2 = createBaseSelect();
        q2->as<ASTSelectQuery>()->distinct = true;
        EXPECT_TRUE(DB::ASTEquality::compareTree(q1, q2));
    }
    {
        // group by clause test
        auto q1 = createBaseSelect();
        auto q2 = createBaseSelect();

        auto group_by1 = makeASTFunction("grouping_sets", makeIdentifier("dept"));
        auto group_by2 = makeASTFunction("grouping_sets", makeIdentifier("team"));

        q1->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by1);
        q2->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by2);
        EXPECT_FALSE(DB::ASTEquality::compareTree(q1, q2));
    }
    {
        // hash consistency test
        auto q1 = createBaseSelect();
        q1->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::LIMIT_LENGTH, makeIntLiteral(10));

        size_t hash1 = DB::ASTEquality::hashTree(q1);
        size_t hash2 = DB::ASTEquality::hashTree(q1->clone());
        EXPECT_EQ(hash1, hash2);

        q1->as<ASTSelectQuery>()->limit_with_ties = true;
        size_t hash3 = DB::ASTEquality::hashTree(q1);
        EXPECT_EQ(hash1, hash3);

        q1->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::LIMIT_OFFSET, makeIntLiteral(5));
        size_t hash4 = DB::ASTEquality::hashTree(q1);
        EXPECT_NE(hash1, hash4);
    }
    {
        // empty clause test
        auto empty = std::make_shared<ASTSelectQuery>();
        auto with_select = createBaseSelect();
        EXPECT_FALSE(DB::ASTEquality::compareTree(empty, with_select));
    }
    {
        // different clause order test
        auto q1 = createBaseSelect();
        auto q2 = createBaseSelect();

        q1->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::WHERE, makeIntLiteral(true));
        q1->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::HAVING, makeIntLiteral(false));

        q2->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::HAVING, makeIntLiteral(false));
        q2->as<ASTSelectQuery>()->setExpression(ASTSelectQuery::Expression::WHERE, makeIntLiteral(true));

        EXPECT_FALSE(DB::ASTEquality::compareTree(q1, q2));
    }

}
