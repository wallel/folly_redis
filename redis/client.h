#pragma once
#include <functional>
#include <map>
#include <queue>
#include <optional>

#include <folly/logging/xlog.h>

#include "redis/client_interface.h"
#include "redis/conn.h"
namespace redis
{
    class RedisSubscriber;
    class REDIS_EXPORT RedisClient:public ClientInterface{
    public:
        explicit RedisClient(folly::Executor* ex):ClientInterface(ex){}
        ~RedisClient()override{
            XLOG(DBG,"redis client release");
        }
        folly::Future<folly::Unit> Connect( const std::string& host, int port, const std::string& pass = "", int dbindex = 0, int32_t timeout_ms = 2000 )override;
        void Close() override;
        bool IsConnected()const;
    public:
        std::shared_ptr<RedisClient> shared()
        {
            return std::dynamic_pointer_cast<RedisClient>(shared_from_this());
        }
        std::weak_ptr<RedisClient> weaked()
        {
            return shared();
        }
        std::shared_ptr<Conn> Connection()const { return conn_; }
    protected:
        folly::Future<Reply> Query(Command cmd)override;
        void Run(Command cmd)override;
    private:
        friend class Command;
        friend class RedisSubscriber;
        std::shared_ptr<Conn>  conn_;                  // redis连接
        Conn::ReplyCallback rpl_callback_{nullptr};
    };

    class REDIS_EXPORT RedisSubscriber:public std::enable_shared_from_this<RedisSubscriber>
    {
    public:
        enum class MsgType
        {
            SUBSCRIBE,
            UNSUBSCRIBE,
            PSUBSCRIBE,
            PUNSUBSCRIBE,
            MESSAGE,
            PMESSAGE
        };
        // 回调接口
        class SubscriberCallback
        {
        public:
            virtual ~SubscriberCallback() = default;
            //message消息
            virtual void OnMessage(std::string channel, std::string msg) = 0;
            //pmessage 消息
            virtual void OnPMessage(std::string pattern, std::string channel, std::string msg) = 0;
            //SUBSCRIBE,UNSUBSCRIBE,PSUBSCRIBE,PUNSUBSCRIBE消息
            virtual void OnMeta(MsgType, std::optional<std::string> channel, int64_t num) = 0;
        };
    public:
        /**
         * caller make sure lifetime of callback is ok
         */
        explicit RedisSubscriber(folly::Executor* ex, SubscriberCallback* callback)
        :client_(std::make_shared<RedisClient>(ex)),
        callback_(callback)
        {
        }
        folly::Future<folly::Unit> Connect(const std::string& host, int port, const std::string& pass = "", int dbindex = 0, int32_t timeout_ms = 2000)
        {
            return client_->Connect(host, port, pass, dbindex, timeout_ms).thenValue([shared=shared_from_this()](folly::Unit&& unit)
            {
                shared->client_->conn_->AddFlag(Conn::SUBSCRIBER);
                shared->client_->conn_->SetReplyCallback([weak{ std::weak_ptr(shared) }](Reply&& rpl)
                {
                    if(!weak.expired())weak.lock()->onReply(std::move(rpl));
                });
                return unit;
            });
        }
        folly::Future<folly::Unit> Connect(const RedisConf& conf, int32_t timeout_ms = 2000)
        {
            return Connect(conf.addr,conf.port,conf.auth,conf.db,timeout_ms);
        }
        void Close()
        {
            client_->Close();
        }
        bool IsConnected()
        {
            return client_->IsConnected();
        }
    public:
        void Subscribe(const std::string& channel)const;
        void Subscribe(const std::vector<std::string>& channels)const;

        void Unsubscribe()const;
        void Unsubscribe(const std::string& channel)const;
        void Unsubscribe(const std::vector<std::string>& channels)const;

        void PSubscribe(const std::string& pattern)const;
        void PSubscribe(const std::vector<std::string>& patterns)const;

        void PUnsubscribe()const;
        void PUnsubscribe(const std::string& channel)const;
        void PUnsubscribe(const std::vector<std::string>& channels)const;
    private:
        void onReply(Reply&& rpl)const;
    private:
        std::shared_ptr<RedisClient> client_{nullptr};
        SubscriberCallback* callback_{nullptr};
    };
}