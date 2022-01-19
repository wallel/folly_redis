#pragma once
#include <cstdint>
#include <map>
#include <string>

#include "redis/client_interface.h"
#include "redis/conn.h"
//1. 节点信息 = > ip, 端口, 槽位
//2. 槽位 = > 节点的映射
//3. move, ask错误处理
//4. 到每个节点的连接 = > 区分主节点和从节点
//5. TODO 区分命令读命令写命令 ? ? ? ?
//6. TODO 读写分离
//7. TODO 单个节点断线重连=>积压的命令怎么处理????
//8. TODO 异常处理
//9. TODO 异步集群接口PIPELINE支持
//          ==>1. pipeline只支持操作相同的slot,先发送到节点上,拿到全部结果
//          ==>2. 如果有move/ask错误,把整个pipeline转发到目标节点,把未完成的命令重新发送一次
//          ==>3. 反复循环直到所有命令都返回

namespace redis
{
    struct Slot
    {
        int32_t min;
        int32_t max;
        friend bool operator<(const Slot& lhs, const Slot& rhs)
        {
            return lhs.max < rhs.max;
        }
    };
    struct Node
    {
        std::string host;
        int32_t port;
        bool slave;
        friend bool operator==(const Node& lhs, const Node& rhs)
        {
            return lhs.host == rhs.host && lhs.port == rhs.port;
        }
        friend bool operator<(const Node&lhs,const Node& rhs)
        {
            if(lhs.host == rhs.host)return lhs.port < rhs.port;
            return lhs.host < rhs.host;
        }
    };
}

namespace std {
    template <>
    struct hash<redis::Node> {
        size_t operator()(const redis::Node& x) const;
    };
}

namespace redis
{
    class ClusterClient;
    //管理集群每个节点的连接
    class ClusterConns:public std::enable_shared_from_this<ClusterConns>
    {
    public:
        using Shards = std::multimap<Slot, Node>; //contain master,slaves
        using Conns = std::unordered_map<Node, std::shared_ptr<Conn>>;
    public:
        // 需要连接到所有的节点(包含主节点)
        folly::SemiFuture<folly::Unit> Connect(const std::string& host, int port,std::string pass="", int32_t timeout_ms = 0);
        // 关闭所有连接
        void Close();
        //更新整个集群信息
        folly::SemiFuture<folly::Unit> Update();
    public:
        folly::SemiFuture<Reply> Query(int32_t slot,Command cmd);
        void Run(int32_t slot,Command cmd);
    public:
        void SetConnectCallback(const Conn::ConnectCallback& cb) {
            connect_cb_ = cb;
        }
        void SetConnectCallback(Conn::ConnectCallback&& cb) {
            connect_cb_ = std::move(cb);
        }
        void SetReplyCallback(const Conn::ReplyCallback& cb)
        {
            reply_cb_ = cb;
        }
        void SetReplyCallback(Conn::ReplyCallback&& cb)
        {
            reply_cb_ = std::move(cb);
        }
        static Shards ParseSlots(Reply&& rpl);
        folly::SemiFuture<folly::Unit> UpdateShards(Shards&& shards);
        std::shared_ptr<Conn> GetConn(const Node& node) const{
            auto it = conns_.find(node);
            if(it == conns_.end())return nullptr;
            return it->second;
        }
    private:
        std::shared_ptr<Conn> GetConn(int32_t slot);
    private:
        friend class ClusterClient;
        //连接回调
        Conn::ConnectCallback connect_cb_;
        //redis回包回调
        Conn::ReplyCallback reply_cb_;
        //集群信息l
        Shards shareds_;
        //所有连接
        Conns conns_;
        //
        std::string pass_;
        int32_t timeout_ms_;
    };

    class ClusterClient:public ClientInterface
    {
    public:
        explicit ClusterClient(folly::Executor* ex) :ClientInterface(ex) {}
        ~ClusterClient()override {
            XLOG(DBG,"ClusterClient release");
        }
        folly::Future<folly::Unit> Connect(const std::string& host, int port, const std::string& pass = "", int dbindex = 0, int32_t timeout_ms = 2000)override;
        void Close() override;
    public:
        std::shared_ptr<ClusterClient> shared()
        {
            return std::dynamic_pointer_cast<ClusterClient>(shared_from_this());
        }
        std::weak_ptr<ClusterClient> weaked()
        {
            return shared();
        }
    protected:
        folly::Future<Reply> Query(Command cmd)override;
        void Run(Command cmd)override;

    private:
        std::shared_ptr<ClusterConns>  conn_;
    };
}

