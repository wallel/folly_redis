#include "redis/conn.h"

#include <folly/executors/GlobalExecutor.h>
#include <folly/logging/xlog.h>
namespace redis
{
    //redis重连延迟
    const static int32_t MAX_REDIS_RECONNECT_DELAY=5000;

    Conn::~Conn()
    {
        Close();
    }

    folly::SemiFuture<folly::Unit> Conn::Connect(const std::string& host, int port, std::string pass/* = ""*/, int32_t db /*= 0*/, int32_t timeout_ms /*= 0 */ )
    {
        addr_.setFromHostPort(host,static_cast<uint16_t>(port));
        pass_ = std::move(pass);
        db_index_ = db;
        if(timeout_ms!=0)timeout_ms_=timeout_ms;

        eventBase_ = folly::getGlobalIOExecutor()->getEventBase();
        eventBase_->runInEventBaseThread([shared=shared_from_this()]{
            XLOGF(DBG,"eventbase thread[{}]", folly::getOSThreadID());
            shared->cli_ = folly::AsyncSocket::newSocket(shared->eventBase_.get());
            shared->cli_->connect(shared.get(),shared->addr_,shared->timeout_ms_);
        });
        return connectPromise_.getSemiFuture();
    }

    void Conn::Close()
    {
        if(closing)return;
        closing=true;
        XLOGF(DBG,"close redis connect [{}]",addr_.getAddressStr());
        if ( cli_ )cli_->getEventBase()->runImmediatelyOrRunInEventBaseThreadAndWait([this]{
            cli_.reset();
        });
    }
    bool Conn::IsConnected() const
    {
        return cli_ && cli_->good();
    }
    void Conn::Send(std::unique_ptr<folly::IOBuf> buf)
    {
        cli_->getEventBase()->runInEventBaseThread([shared = shared_from_this(), buf{ std::move(buf) }]()mutable 
        {
            shared->cli_->writeChain(shared.get(),std::move(buf));
        });
    }

    folly::SemiFuture<Reply> Conn::Query(Command cmd)
    {
        return queryInternal(std::move(cmd));
    }

    void Conn::Run(Command cmd)
    {
        if (cmd.Build().Empty())return;
        folly::IOBufQueue buf(folly::IOBufQueue::cacheChainLength());
        cmd.SerializeTo(buf);
        WaittingCommand wait;
        wait.ignore = true;
        wait.cmds = std::move(cmd).Commands();
        {
            std::lock_guard<std::mutex> lock(cmds_mtx_);
            cmds_.emplace_back(std::move(wait));
        }
        Send(buf.move());
    }

    folly::SemiFuture<Reply> Conn::queryInternal(Command cmd, bool append)
    {
        if (cmd.Build().Empty()) {
            return folly::makeFuture<Reply>(std::invalid_argument("please give at least one command"));
        }
        if (IsConnected())
        {
            folly::IOBufQueue buf(folly::IOBufQueue::cacheChainLength());
            cmd.SerializeTo(buf);
            Send(buf.move());
        }
        WaittingCommand wait;
        wait.ignore = false;
        wait.cmds = std::move(cmd).Commands();
        auto future = wait.reply.getSemiFuture();
        {
            std::lock_guard<std::mutex> lock(cmds_mtx_);
            if (append) {
                cmds_.emplace_back(std::move(wait));
            }
            else
            {
                cmds_.emplace_front(std::move(wait));
            }
        }
        
        return future;
    }

    void Conn::OnReply(Reply&& rpl)
    {
        //TODO move,ask错误处理
        std::lock_guard<std::mutex> lock(cmds_mtx_);
        if(cmds_.empty())
        {
            //pubsub
            if(IsSubscriberConn() && reply_cb_)
            {
                reply_cb_(std::move(rpl));
            }
            return;
        }
        auto& cmd = cmds_.front();
        if (cmd.ignore)
        {
            size_t i = 0;
            for (; i < cmd.cmds.size(); i++) {
                if (cmd.cmds[i].rpl)continue;
                if (rpl.IsError())XLOGF(ERR,"redis command {} result error:{}", cmd.cmds[i].cmd, rpl.AsString());
                cmd.cmds[i].rpl = std::move(rpl);
                break;
            }
            //所有都回来了
            if (i == cmd.cmds.size() - 1) {
                cmds_.pop_front();
            }
        }
        else
        {
            size_t i = 0;
            for (; i < cmd.cmds.size(); i++) {
                auto& cur = cmd.cmds[i];
                if (cur.rpl)continue;
                if (cur.ignore && rpl.IsError()) {
                    XLOGF(ERR,"redis command {} result error:{}", cur.cmd, rpl.AsString());
                }
                cur.rpl = std::move(rpl);
                break;
            }
            // 所有的reply都回来了
            if (i == cmd.cmds.size() - 1)
            {
                Reply result;
                for (auto& cur : cmd.cmds) {
                    if (!cur.ignore) result << cur.rpl.value();
                }
                //单一结果直接直接回复
                if (result.AsArray().size() == 1 && !cmd.pipeline)
                {
                    cmd.reply.setValue(std::move(std::move(result).AsArray()[0]));
                }
                else
                {
                    cmd.reply.setValue(std::move(result));
                }
                cmds_.pop_front();
            }
        }
    }
    void Conn::connectSuccess() noexcept {
        XLOGF(INFO,"connect success thread[{}]", folly::getOSThreadID());
        cli_->setReadCB(this);
        cli_->setCloseOnExec();
        reconnect_count_ = 0;
        reconnecting = false;

        auto r = folly::makeSemiFuture();
        if(!pass_.empty())
        {
            r = std::move(r).deferValue([shared = shared_from_this()](folly::Unit&&)
            {
                return shared->queryInternal(std::move(Command::Create(false).Auth(shared->pass_).Build()), false).unit();
            });
        }
        if(db_index_!=0 && !IsClusterConn())
        {
            r = std::move(r).deferValue([shared = shared_from_this()](folly::Unit&&)
            {
                    auto cmd = std::move(Command::Create(false).Select(shared->db_index_).Build());
                    return shared->queryInternal(std::move(cmd), false).unit();
            });
        }
        //TODO 直接重发吗??,有部分已经发送成功的话怎么处理????
        {
            std::lock_guard<std::mutex> lock(cmds_mtx_);
            if (!cmds_.empty())
            {
                r = std::move(r).deferValue([shared = shared_from_this()](folly::Unit&&)
                {
                    folly::IOBufQueue buf(folly::IOBufQueue::cacheChainLength());
                    for (auto& cmd : shared->cmds_) {
                        for (auto& sub : cmd.cmds)
                        {
                            buf.append(sub.cmd.data(), sub.cmd.size());
                        }
                    }
                    shared->Send(buf.move());
                    return folly::makeSemiFuture();
                });
            }
        }

        std::move(r).via(eventBase_).then([shared = shared_from_this()](folly::Try<folly::Unit>&& r)
        {
            if(!shared->connectPromise_.isFulfilled())
            {
                shared->connectPromise_.setTry(std::move(r));
            }else
            {
                if(r.hasException())
                {
                    XLOG(ERR,"reconnect to redis[{}] error:{}", shared->addr_.getAddressStr(), r.exception().what());
                    shared->reconnect();
                }
            }
        });
    }
    void Conn::connectErr(const folly::AsyncSocketException &ex) noexcept {
        if(!connectPromise_.isFulfilled()){
            connectPromise_.setException(ex);
        }else{
            XLOGF(ERR,"connect to redis [{}] err:{},reconnect_count:{}",addr_.getAddressStr(),ex.what(),reconnect_count_);
            reconnect();
        }
    }
    void Conn::readEOF() noexcept {
        if(cli_ && !cli_->isClosedBySelf()){
            XLOGF(ERR,"redis conn[{}] lost!!,closed by server[{}]",addr_.getAddressStr(),cli_->isClosedByPeer());
            reconnect();
        }
    }
    void Conn::readErr(const folly::AsyncSocketException &ex) noexcept {
        XLOGF(ERR,"redis conn read error:{}",ex.what());
        reconnect();
    }

    void Conn::getReadBuffer(void **bufReturn, size_t *lenReturn) {
        //TODO malloc
        auto [buf,len] =buf_.preallocate(1024,1024);
        *bufReturn=buf;
        *lenReturn = len;
    }
    void Conn::readDataAvailable(size_t len) noexcept {
        XLOGF(ERR,"redis conn readDataAvailablethread[{}]", folly::getOSThreadID());
        buf_.postallocate(len);
        while (builder_.Build());
        while(builder_.IsReplyAvailable())
        {
            auto rpy = builder_.GetFront();
            OnReply(std::move(rpy));
            builder_.PopFront();
        }
    }
    void Conn::writeSuccess() noexcept {
    }
    void Conn::writeErr(size_t bytesWritten, const folly::AsyncSocketException &ex) noexcept {
        XLOGF(ERR,"redis conn write error, written bytes:{}, ex:{}",bytesWritten,ex.what());
        reconnect();
        //TODO 已经写入了部分数据怎么处理????
    }
    void Conn::reconnect() {
        if(!cli_ && reconnecting)return;
        XLOGF(ERR,"try reconnect to redis [{}],reconnect_count:{}",reconnect_count_);
        if(reconnect_count_ > 0)
        {
           auto delay = reconnect_count_ * 1000;
           if(delay > MAX_REDIS_RECONNECT_DELAY) delay=MAX_REDIS_RECONNECT_DELAY;
            reconnecting=true;
           folly::makeFuture()
            .delayed(std::chrono::milliseconds(delay))
            .thenValue([shared=shared_from_this()](folly::Unit&&){
                shared->reconnect_count_+=1;
                shared->cli_->getEventBase()->runInEventBaseThread([shared]{
                    auto evt = shared->cli_->getEventBase();
                    shared->cli_.reset();
                    shared->cli_=folly::AsyncSocket::newSocket(evt);
                    shared->cli_->connect(shared.get(),shared->addr_,shared->timeout_ms_);
                });
           });
        }else
        {
            reconnect_count_ +=1;
            reconnecting=true;
            cli_->getEventBase()->runInEventBaseThread([shared=shared_from_this()]{
                auto evt = shared->cli_->getEventBase();
                shared->cli_.reset();
                shared->cli_=folly::AsyncSocket::newSocket(evt);
                shared->cli_->connect(shared.get(),shared->addr_,shared->timeout_ms_);
            });
        }
    }

}
