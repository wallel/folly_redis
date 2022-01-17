#include "redis/command.h"
#include "redis/client_interface.h"

namespace redis{

    std::string Command::OverflowTypeToString(OverflowType op)
    {
        switch (op) {
        case OverflowType::Fail:
            return "FAIL";
        case OverflowType::Wrap:
            return "WARP";
        case OverflowType::Sat:
            return "SAT";
        case OverflowType::ServerDefault:
            return "";
        }
        return "";
    }

    std::string Command::BitfieldOpToString(BitfieldOpType op)
    {
        switch ( op ) {
        case BitfieldOpType::Get:
            return "GET";
        case BitfieldOpType::Set:
            return "SET";
        case BitfieldOpType::Incrby:
            return "INCRBY";
        }
        return "";
    }

    std::string Command::AggregateMethodToString(AggregateMethod method)
    {
        switch (method) {
        case AggregateMethod::Sum:
            return "SUM";
        case AggregateMethod::Min:
            return "MIN";
        case AggregateMethod::Max:
            return "MAX";
        case AggregateMethod::ServerDefault:
        default:
            return "";
        }
    }

    folly::IOBufQueue Command::Serialize() const
    {
        folly::IOBufQueue buf(folly::IOBufQueue::cacheChainLength());
        SerializeTo(buf);
        return buf;
    }
    void Command::SerializeTo(folly::IOBufQueue& buf)const
    {
        for (auto& cmd : cmds_) {
            buf.append(cmd.cmd.data(), cmd.cmd.size());
        }
    }
    folly::Future<Reply> Command::Query(){
        buildCommand();
        if(client_)return client_->Query(std::move(*this));
        return folly::makeFuture<Reply>(std::runtime_error("need a valid redis client"));
    }
    void Command::Run()
    {
        buildCommand();
        if(client_)client_->Run(std::move(*this));
    }
}

