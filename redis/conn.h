#pragma once
#include <functional>
#include <mutex>

#include <folly/futures/Future.h>
#include <folly/io/async/AsyncSocket.h>

#include "redis/command.h"
#include "redis/builders.h"

namespace redis
{
    class ClusterConns;
    class Conn:
            folly::AsyncSocket::ConnectCallback,
            folly::AsyncReader::ReadCallback,
            folly::AsyncWriter::WriteCallback,
            public std::enable_shared_from_this<Conn> {
    public:
        enum Flag
        {
            SINGLE      =1, //单例的redis
            CLUSTER     =2, //集群redis=>
            SUBSCRIBER  =4, //订阅链接
        };
    public:
        using ConnectCallback = std::function<folly::SemiFuture<folly::Unit>(Conn& )>;
        using ReplyCallback = std::function < void(Reply&& ) > ;
    public:
        Conn() = default;
        explicit Conn(const Flag flag) : flags_(flag) {}
        explicit Conn(const std::shared_ptr<ClusterConns>& cluster): flags_(CLUSTER), cluster_(cluster){}
        ~Conn()override;
        //连接
        folly::SemiFuture<folly::Unit> Connect( const std::string& host, int port,std::string pass="",int32_t db=0, int32_t timeout_ms = 0);
        //断开
        void Close();
        //判断是否连接
        bool IsConnected()const;
        void Send(std::unique_ptr<folly::IOBuf> buf);
    public:
        void AddFlag(Flag flag) { flags_ |= flag; }
        bool IsClusterConn()const { return (flags_ & CLUSTER) > 0; }
        bool IsSubscriberConn()const { return (flags_ & SUBSCRIBER) > 0; }
    public:
        void SetReplyCallback( const ReplyCallback& cb )
        {
            reply_cb_ = cb;
        }
        void SetReplyCallback( ReplyCallback&& cb )
        {
            reply_cb_ = std::move( cb );
        }
        const folly::SocketAddress& Addr()const { return addr_; }
    public:
        folly::SemiFuture<Reply> Query(Command cmd);
        void Run(Command cmd);
    private:
        void connectSuccess() noexcept override;
        void connectErr(const folly::AsyncSocketException &ex) noexcept override;

        void readEOF() noexcept override;
        void readErr(const folly::AsyncSocketException &ex) noexcept override;
        void getReadBuffer(void **bufReturn, size_t *lenReturn) override;
        void readDataAvailable(size_t len) noexcept override;

        void writeSuccess() noexcept override;
        void writeErr(size_t bytesWritten, const folly::AsyncSocketException &ex) noexcept override;

        void reconnect();
        folly::SemiFuture<Reply> queryInternal(Command cmd, bool append = true);
        void OnReply(Reply&& rpl);
    private:
        ReplyCallback reply_cb_;

        /***********************reply****************************************/
        folly::IOBufQueue buf_{ folly::IOBufQueue::cacheChainLength() };
        ReplyBuilder builder_{buf_};
        struct WaittingCommand
        {
            std::vector<CommandVal> cmds;
            folly::Promise<Reply> reply;
            bool ignore{ false };
            bool pipeline{ false };
        };
        std::mutex                                  cmds_mtx_;
        std::deque<WaittingCommand>                 cmds_;  // 等待中的命令列表
        /***********************connect info********************************/
        folly::SocketAddress addr_;
        std::string pass_;
        int32_t     db_index_{0};
        int32_t timeout_ms_{2000}; //连接超时
        int32_t flags_{SINGLE};
        std::atomic_bool closing{false};
        std::atomic_bool reconnecting{false};
        folly::Promise<folly::Unit> connectPromise_;

        std::shared_ptr<folly::AsyncSocket> cli_;
        folly::Executor::KeepAlive<folly::EventBase> eventBase_;
        /***************************************************************/
        //重连次数
        std::atomic<int> reconnect_count_{0};
        /***************************************************************/
        //集群支持
        std::weak_ptr<ClusterConns> cluster_;
    };
}
