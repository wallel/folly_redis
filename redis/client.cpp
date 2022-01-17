#include "redis/client.h"
#include <future>
#include <queue>

#include <folly/logging/xlog.h>
namespace redis
{
    folly::Future<folly::Unit>
    RedisClient::Connect(const std::string &host, int port, const std::string &pass, int dbindex, int32_t timeout_ms) {
        if(!conn_){
            conn_  =std::make_shared<Conn>(Conn::SINGLE);
        }
        return conn_->Connect(host, port,pass,dbindex, timeout_ms).via(exec_);
    }
    void RedisClient::Close() {
        if(conn_)conn_->Close();
    }
    bool RedisClient::IsConnected()const {
        return conn_&& conn_->IsConnected();
    }
    folly::Future<Reply> RedisClient::Query(Command cmd)
    {
        return conn_->Query(std::move(cmd)).via(exec_);
    }
    void RedisClient::Run(Command cmd)
    {
        return conn_->Run(std::move(cmd));
    }
    void RedisSubscriber::Subscribe(const std::string& channel)const
    {
        auto buf = client_->Cmd().Cmd("SUBSCRIBE").Arg(channel).Build().Serialize();
        client_->conn_->Send(buf.move());
    }
    void RedisSubscriber::Subscribe(const std::vector<std::string>& channels)const
    {
        auto buf = client_->Cmd().Cmd("SUBSCRIBE").Arg(channels).Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    void RedisSubscriber::Unsubscribe()const
    {
        auto buf = client_->Cmd().Cmd("UNSUBSCRIBE").Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    void RedisSubscriber::Unsubscribe(const std::string& channel)const
    {
        auto buf = client_->Cmd().Cmd("UNSUBSCRIBE").Arg(channel).Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    void RedisSubscriber::Unsubscribe(const std::vector<std::string>& channels)const
    {
        auto buf = client_->Cmd().Cmd("UNSUBSCRIBE").Arg(channels).Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    void RedisSubscriber::PSubscribe(const std::string& pattern)const
    {
        auto buf = client_->Cmd().Cmd("PSUBSCRIBE").Arg(pattern).Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    void RedisSubscriber::PSubscribe(const std::vector<std::string>& patterns)const
    {
        auto buf = client_->Cmd().Cmd("PSUBSCRIBE").Arg(patterns).Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    void RedisSubscriber::PUnsubscribe()const
    {
        auto buf = client_->Cmd().Cmd("PUNSUBSCRIBE").Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    void RedisSubscriber::PUnsubscribe(const std::string& channel)const
    {
        auto buf = client_->Cmd().Cmd("PUNSUBSCRIBE").Arg(channel).Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    void RedisSubscriber::PUnsubscribe(const std::vector<std::string>& channels)const
    {
        auto buf = client_->Cmd().Cmd("PUNSUBSCRIBE").Arg(channels).Build().Serialize();
        client_->conn_->Send(buf.move());
    }

    RedisSubscriber::MsgType msgType(std::string const& type)
    {
        if ("message" == type) {
            return RedisSubscriber::MsgType::MESSAGE;
        }
        else if ("pmessage" == type) {
            return RedisSubscriber::MsgType::PMESSAGE;
        }
        else if ("subscribe" == type) {
            return RedisSubscriber::MsgType::SUBSCRIBE;
        }
        else if ("unsubscribe" == type) {
            return RedisSubscriber::MsgType::UNSUBSCRIBE;
        }
        else if ("psubscribe" == type) {
            return RedisSubscriber::MsgType::PSUBSCRIBE;
        }
        else if ("punsubscribe" == type) {
            return RedisSubscriber::MsgType::PUNSUBSCRIBE;
        }
        return RedisSubscriber::MsgType::MESSAGE; // Silence "no return" warnings.
    }

    void RedisSubscriber::onReply(Reply&& rpl)const
    {
        auto conn = client_->Connection();
        if(!rpl.IsArray())
        {
            if (rpl.IsError() || rpl.IsString()) XLOG(ERR) << "RedisSubscriber[" << conn->Addr().getAddressStr() << "] received:" << rpl.AsString();
            return;
        }
        auto arr = rpl.AsArray();
        if(arr.empty())
        {
            XLOG(ERR) << "RedisSubscriber[" << conn->Addr().getAddressStr() << "] received message error: array is empty";
        }
        if(!callback_)
        {
            std::stringstream ss;
            ss << rpl;
            XLOG(ERR)<<"RedisSubscriber[" << conn->Addr().getAddressStr() << "] received message:"<<ss.str();
            return;
        }
        switch (auto type = msgType(arr[0].AsString()))
        {
        case MsgType::MESSAGE:
        {
            if(arr.size()!=3 || !arr[1].IsString() || !arr[2].IsString())
            {
                std::stringstream ss;
                ss << rpl;
                XLOG(ERR) << "RedisSubscriber[" << conn->Addr().getAddressStr() << "] received error message:" << ss.str();
                return;
            }
            folly::via(client_->GetExecutor(), [this,rpl{std::move(rpl)}]()
                {
                    auto arr = rpl.AsArray();
                    callback_->OnMessage(arr[1].AsString(),arr[2].AsString());
                });
        }
        break;
        case MsgType::PMESSAGE:
        {
            if (arr.size() != 4 || !arr[1].IsString() || !arr[2].IsString() || !arr[3].IsString())
            {
                std::stringstream ss;
                ss << rpl;
                XLOG(ERR) << "RedisSubscriber[" << conn->Addr().getAddressStr() << "] received error pmessage:" << ss.str();
                return;
            }
            folly::via(client_->GetExecutor(), [this, rpl{ std::move(rpl) }]()
            {
                auto arr = rpl.AsArray();
                callback_->OnPMessage(arr[1].AsString(), arr[2].AsString(),arr[3].AsString());
            });
        }
        break;
        case MsgType::PSUBSCRIBE:
        case MsgType::SUBSCRIBE:
        case MsgType::UNSUBSCRIBE:
        case MsgType::PUNSUBSCRIBE:
        {
            if (arr.size() != 3 || !(arr[1].IsString()||arr[1].IsNull()) || !arr[2].IsInteger())
            {
                std::stringstream ss;
                ss << rpl;
                XLOG(ERR) << "RedisSubscriber[" << conn->Addr().getAddressStr() << "] received error meta:" << ss.str();
                return;
            }
            folly::via(client_->GetExecutor(), [this, rpl{ std::move(rpl) },type]()
            {
                auto& arr = rpl.AsArray();
                std::optional<std::string> channel;
                if (arr[1].IsString())channel = arr[1].AsString();
                callback_->OnMeta(type,channel,arr[2].AsInteger());
            });
        }
        break;
        }
    }
}
