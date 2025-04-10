#include <DataTypes/DataTypesNumber.h>
#include <Query/Common/MapHelpers.h>
#include <gtest/gtest.h>
#include "DataTypes/DataTypeFactory.h"

using namespace DB;


TEST(MapHelpersTest, tryConvertToMapKeyFieldTest)
{
    auto data_type_uint8 = DataTypeFactory::instance().get("UInt8");
    Field a_number = tryConvertToMapKeyField(data_type_uint8, "2");
    EXPECT_TRUE(a_number.safeGet<UInt64>() == 2);

    Field not_a_number = tryConvertToMapKeyField(data_type_uint8, "not a number");
    EXPECT_TRUE(not_a_number.safeGet<UInt64>() == 0);
}

TEST(MapHelpersTest, checkAndSetMapSeparatorTest)
{
    EXPECT_EQ(getMapSeparator(), "__");

    checkAndSetMapSeparator("___");
    EXPECT_EQ(getMapSeparator(), "___");

    EXPECT_THROW(checkAndSetMapSeparator("应该抛异常"), Exception);
    EXPECT_EQ(getMapSeparator(), "___");

    checkAndSetMapSeparator("__");  // roll back
    EXPECT_EQ(getMapSeparator(), "__");
}

TEST(MapHelpersTest, genMapKeyFilePrefixTest)
{
    EXPECT_EQ(genMapKeyFilePrefix("col."), "__col%2E__");
    EXPECT_EQ(genMapBaseFilePrefix("col."), "__col%2E_base.");
}

TEST(MapHelpersTest, getImplicitColNameForMapKeyTest)
{
    EXPECT_EQ(getImplicitColNameForMapKey("col", "key"), "__col__key");
    EXPECT_EQ(getImplicitFileNamePrefixForMapKey("col", "key"), "__col__key");
}
