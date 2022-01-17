#pragma once
#include <memory>
#include <string>

#include "redis/redis_export.h"
#include "redis/command.h"
namespace redis
{
    struct REDIS_EXPORT RedisConf
    {
        std::string addr{};
        int port{ 0 };
        std::string auth{};
        int db{ 0 };
    };
    /**
     * 公共接口
     */
    class REDIS_EXPORT ClientInterface:public std::enable_shared_from_this<ClientInterface>
    {
    public:
        explicit ClientInterface(folly::Executor* ex):exec_(ex){}
        virtual ~ClientInterface()=default;

        virtual folly::Future<folly::Unit> Connect(const std::string& host, int port, const std::string& pass = "", int dbindex = 0, int32_t timeout_ms = 2000)=0;

        folly::Future<folly::Unit> Connect(const RedisConf& conf, int32_t timeout_ms = 2000) {
            return Connect(conf.addr, conf.port, conf.auth, conf.db, timeout_ms);
        }
        virtual void Close()=0;
        auto GetExecutor()const { return exec_; }
    public:
        Command Cmd(std::string cmd = "")
        {
            if (cmd.empty())return Command(shared_from_this());
            return Command(shared_from_this(), std::move(cmd), false);
        }
        virtual Command Pipeline()
        {
            return Command(shared_from_this(), true);
        }
    protected:
        virtual folly::Future<Reply> Query(Command cmd)=0;
        virtual void Run(Command cmd)=0;
    protected:
        friend class Command;
        folly::Executor::KeepAlive<folly::Executor> exec_;  // 默认回调执行环境
    };
}
