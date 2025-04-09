#include <gtest/gtest.h>
#include <Query/Core/SortDescriptionHelper.h>

using namespace DB;


TEST(SortDescriptionHelperTest, formatSortDescriptionTest)
{
    SortColumnDescription sort_desc("col");
    EXPECT_EQ(format(sort_desc), "col ASC NULLS LAST");
}
