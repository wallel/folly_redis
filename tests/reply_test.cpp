#include <gtest/gtest.h>
#include "redis/reply.h"
#include "redis/builders.h"

TEST(ReplyTest,BasicAssertions){
    GTEST_EXPECT_TRUE(redis::Reply().IsNull());

    auto rpl =redis::Reply();
    rpl.set("test",redis::Reply::StringType::BulkString);
    GTEST_EXPECT_TRUE(rpl.IsBulkString());
    EXPECT_EQ(rpl.AsString(),"test");

    rpl.set("test111",redis::Reply::StringType::SimpleString);
    GTEST_EXPECT_TRUE(rpl.IsSimpleString());
    EXPECT_EQ(rpl.AsString(),"test111");

    rpl << redis::Reply(12) << redis::Reply("invalid command",redis::Reply::StringType::Error);
    GTEST_EXPECT_TRUE(rpl.IsArray());
    auto& arr = rpl.AsArray();
    EXPECT_EQ(arr.size(),2);

    GTEST_EXPECT_TRUE(arr[0].IsInteger());
    EXPECT_EQ(arr[0].AsInteger(),12);

    GTEST_EXPECT_TRUE(arr[1].IsError());
    EXPECT_EQ(arr[1].AsString(),"invalid command");
}

TEST(BuildersTest,BasicAssertions){

}

