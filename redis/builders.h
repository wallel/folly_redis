#pragma once
#include <deque>
#include <memory>

#include <folly/io/IOBufQueue.h>

#include "redis/reply.h"
namespace redis {
        class Builder
        {
        public:
            explicit Builder(folly::IOBufQueue& buf):buf_(buf){};
            virtual ~Builder() = default;
            virtual bool Check() = 0;
            virtual bool IsReady() {
                return ready_;
            }
            virtual Reply Build() {
                return std::move( rpl_ );
            }
        protected:
            bool ready_{false};
            folly::IOBufQueue& buf_;
            Reply rpl_;
        };

        class SimpleStringBuilder: public Builder
        {
        public:
            explicit SimpleStringBuilder(folly::IOBufQueue& buf):Builder(buf){};
            bool Check() override;
        };
        class ErrorBuilder : public Builder
        {
        public:
            explicit ErrorBuilder(folly::IOBufQueue& buf):Builder(buf), ssbuilder_(buf){};
            bool Check() override;
        private:
            SimpleStringBuilder ssbuilder_;
        };

        class IntBuilder : public Builder {
        public:
            explicit IntBuilder(folly::IOBufQueue& buf):Builder(buf){};
            bool Check() override;
            int64_t Integer()const {
                return val_;
            }
        private:
            int64_t val_;
        };

        class BulkStringBuilder : public Builder {
        public:
            explicit BulkStringBuilder(folly::IOBufQueue& buf):Builder(buf), intBuilder_(buf){};
            bool Check() override;
        private:
            bool fetchSize();
            bool fetchStr();
        private:
            IntBuilder intBuilder_;
            int64_t size_{0};
        };
        class ArrayBuilder : public Builder {
        public:
            explicit ArrayBuilder(folly::IOBufQueue& buf):Builder(buf), intBuilder_(buf){};
            bool Check() override;
        private:
            bool fetchSize();
            bool fetchField();
        private:
            IntBuilder intBuilder_;
            int64_t size_{ 0 };
            std::unique_ptr<Builder> curbuilder_;
        };

        class ReplyBuilder {
        public:
            explicit ReplyBuilder(folly::IOBufQueue& buf):buffer_(buf){};
            //返回第一个
            void operator>>( Reply& rpl ) const;
            const Reply& GetFront() const;
            //删除第一个
            void PopFront();
            //是否可用
            bool IsReplyAvailable()const;
            void Reset();
        public:
            bool Build();
        private:
            folly::IOBufQueue& buffer_;
            std::unique_ptr<Builder> builder_;
            std::deque<Reply> valid_replies_;
        };
}