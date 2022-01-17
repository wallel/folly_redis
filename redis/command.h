#pragma once
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <folly/lang/SafeAssert.h>
#include <folly/futures/Future.h>
#include <folly/io/IOBufQueue.h>
#include <folly/logging/xlog.h>

#include "redis/reply.h"
namespace redis{
    class ClientInterface;

    struct CommandVal{
        std::string cmd{};
        std::string key{}; //对应的key值(clsuter中需要用来计算hash)
        bool ignore{ false };
        std::optional<Reply> rpl{};
        CommandVal(std::string _cmd,std::string _key,bool _ignore)
        :cmd(std::move(_cmd)), key(std::move(_key)), ignore(_ignore)
        {}
    };
    class Command{
    public:
        using Self = Command;
    public:
        static Command Create(bool pipe)
        {
            return Command(nullptr, pipe);
        };

        ~Command(){
            XLOG(DBG,"Command release");
        }
        Command(const Command&)=delete;
        Command& operator=(const Command&)=delete;
        Command(Command&&)noexcept=default;

        Command& Arg(std::string arg){
            current_cmd_.emplace_back(std::move(arg));
            return *this;
        }
        Command& Arg(const char* arg){
            current_cmd_.emplace_back(std::string(arg));
            return *this;
        }
        Command& Key(std::string arg) {
            current_key_ = arg;
            current_cmd_.emplace_back(std::move(arg));
            return *this;
        }
        Command& Key(const char* arg) {
            current_key_ = arg;
            current_cmd_.emplace_back(std::string(arg));
            return *this;
        }
        Command& SetKey(std::string arg) {
            current_key_ = std::move(arg);
            return *this;
        }
        template<class T>
        Command& Arg(T&& t){
            current_cmd_.emplace_back(folly::to<std::string>(std::forward<T>(t)));
            return *this;
        }
        template<class T>
        Command& Arg(const std::vector<T>& ts)
        {
            for (auto& t : ts)
            {
                Arg(t);
            }
            return *this;
        }
        Command& Ignore(){
            current_ignore_=true;
            return *this;
        }
        Command& Cmd(std::string cmd){
            if(!pipe_ && !current_cmd_.empty()){
                folly::throw_exception(std::invalid_argument("multi Cmd can only call with pipe"));
            }
            buildCommand();
            current_cmd_.emplace_back(std::move(cmd));
            return *this;
        }
    public:
        Self& Auth( const std::string& password)
        {
            return Cmd("AUTH").Arg(password);
        }
        Self& Select( int index)
        {
            return Cmd("SELECT").Arg(index);
        }
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //string
    public:
        //std::string
        Self& Append( const std::string& key, const std::string& value){
            return Cmd("APPEND").Key(key).Arg(value);
        }
        Self& Decr( const std::string& key){
            return Cmd("DECR").Key(key);
        }
        Self& Decrby( const std::string& key, int val){
            return Cmd("DECRBY").Key(key).Arg(val);
        }
        Self& Incr( const std::string& key){
            return Cmd("INCR").Key(key);
        }
        Self& Incrby( const std::string& key, int incr){
            return Cmd("INCRBY").Key(key).Arg(incr);
        }
        Self& IncrbyFloat( const std::string& key, float incr){
            return Cmd("INCRBYFLOAT").Key(key).Arg(incr);
        }
        Self& Del( const std::vector<std::string>& keys){
            FOLLY_SAFE_CHECK(!keys.empty(), "redis DEL need at least one key");
            return Cmd("DEL").SetKey(keys.front()).Arg(keys);
        }
        Self& Exists( const std::vector<std::string>& keys){
            FOLLY_SAFE_CHECK(!keys.empty(), "redis EXISTS need at least one key");
            auto& cmd = Cmd("EXISTS").SetKey(keys.front()).Arg(keys);
        }
        Self& Expire( const std::string& key, int seconds){
            return Cmd("EXPIRE").Key(key).Arg(seconds);
        }
        Self& ExpireAt( const std::string& key, int timestamp){
            return Cmd("EXPIREAT").Key(key).Arg(timestamp);
        }
        Self& Echo( const std::string& msg){
            return Cmd("Echo").Arg(msg);
        }
        Self& StrLen( const std::string& key){
            return Cmd("STRLEN").Key(key);
        }

        Self& GetRange( const std::string& key, int start, int end){
            return Cmd("GETRANGE").Key(key).Arg(start).Arg(end);
        }
        Self& GetSet( const std::string& key, const std::string& val){
            return Cmd("GETSET").Key(key).Arg(val);
        }
        Self& Get( const std::string& key){
            return Cmd("GET").Key(key);
        }
        Self& MGet( const std::vector<std::string>& keys){
            FOLLY_SAFE_CHECK(!keys.empty(), "redis MGET need at least one key");
            return Cmd("MGET").SetKey(keys.front()).Arg(keys);
        }
        Self& MSet( const std::vector<std::pair<std::string, std::string>>& key_vals){
            FOLLY_SAFE_CHECK(!key_vals.empty(), "redis MSet need at least one key");
            auto& cmd = Cmd("MSET");
            bool first = true;
            for(auto [k,v]:key_vals){
                if (first) {
                    cmd.Key(std::move(k)).Arg(std::move(v));
                    first = false;
                    continue;
                }
                cmd.Arg(std::move(k)).Arg(std::move(v));
            }
            return cmd;
        }

        Self& MSetNX( const std::vector<std::pair<std::string, std::string>>& key_vals){
            FOLLY_SAFE_CHECK(!key_vals.empty(), "redis MSetNX need at least one key");
            auto& cmd = Cmd("MSETNX");
            bool first = true;
            for(auto [k,v]:key_vals){
                if (first) {
                    cmd.Key(std::move(k)).Arg(std::move(v));
                    first = false;
                    continue;
                }
                cmd.Arg(std::move(k)).Arg(std::move(v));
            }
            return cmd;
        }
        Self& PSetEX( const std::string& key, int64_t ms, const std::string& val){
            return Cmd("PSETEX").Key(key).Arg(ms).Arg(val);
        }
        Self& Set( const std::string& key, const std::string& value){
            return Cmd("SET").Key(key).Arg(value);
        }
        Self& SetAdvanced( const std::string& key, const std::string& value, bool ex, int ex_sec, bool px, int px_milli,
                           bool nx, bool xx){
            auto& cmd = Cmd("SET").Key(key).Arg(value);
            if(ex) cmd.Arg("EX").Arg(ex_sec);
            if(px)cmd.Arg("PX").Arg(px_milli);
            if(nx)cmd.Arg("NX");
            if(xx)cmd.Arg("XX");
            return *this;
        }
        Self& SetEX( const std::string& key, int64_t seconds, const std::string& value){
            return Cmd("SETEX").Key(key).Arg(seconds).Arg(value);
        }
        Self& SetNX( const std::string& key, const std::string& value){
            return Cmd("SETNX").Key(key).Arg(value);
        }
        Self& SetRange( const std::string& key, int offset, const std::string& value){
            return Cmd("SETRANGE").Key(key).Arg(offset).Arg(value);
        }
        Self& PExpire( const std::string& key, int ms){
            return Cmd("PEXPIRE").Key(key).Arg(ms);
        }
        Self& PExpireAt( const std::string& key, int ms_timestamp){
            return Cmd("PEXPIREAT").Key(key).Arg(ms_timestamp);
        }

        Self& Ping(){
            return Cmd("PING");
        }
        Self& Ping( const std::string& message){
            return Cmd("PING").Arg(message);
        }

        Self& PTtl( const std::string& key){
            return Cmd("PTTL").Key(key);
        }
        Self& Ttl( const std::string& key){
            return Cmd("TTL").Key(key);
        }
        Self& Quit( ){
            return Cmd("QUIT");
        }
        Self& Type( const std::string& key){
            return Cmd("TYPE").Key(key);
        }

        Self& Rename( const std::string& key, const std::string& newkey){
            return Cmd("RENAME").Key(key).Arg(newkey);
        }
        Self& RenameNX( const std::string& key, const std::string& newkey){
            return Cmd("RENAMENX").Key(key).Arg(newkey);
        }

        Self& Scan( std::size_t cursor){
            return Scan(cursor,"",0);
        }
        Self& Scan( std::size_t cursor, const std::string& pattern){
            return Scan(cursor,pattern,0);
        }
        Self& Scan( std::size_t cursor, std::size_t count){
            return Scan(cursor,"",count);
        }
        Self& Scan( std::size_t cursor, const std::string& pattern, std::size_t count){
            auto& cmd = Cmd("SCAN").Arg(cursor);
            if(!pattern.empty())cmd.Arg("MATCH").Arg(pattern);
            if(count > 0) cmd.Arg("COUNT").Arg(count);
            return cmd;
        }

    public:
        ////////////////////////////////////////////////////////////////////////////
        //bit
        enum class OverflowType {
            Wrap,
            Sat,
            Fail,
            ServerDefault,
        };
        static std::string OverflowTypeToString( OverflowType op );
        enum class BitfieldOpType {
            Get,
            Set,
            Incrby,
        };
        static std::string BitfieldOpToString( BitfieldOpType op );
        struct BitFieldOperation {
            BitfieldOpType op;
            std::string type;
            int offset;
            int value;
            OverflowType overflow;

            static BitFieldOperation Get( const std::string& type, int offset, OverflowType overflow = OverflowType::ServerDefault )
            {
                return { BitfieldOpType::Get,type,offset,0,overflow };
            }
            static BitFieldOperation Set( const std::string& type, int offset, int value, OverflowType overflow = OverflowType::ServerDefault )
            {
                return { BitfieldOpType::Set,type,offset,value,overflow };
            }
            static BitFieldOperation Incrby( const std::string& type, int offset, int increment, OverflowType overflow = OverflowType::ServerDefault )
            {
                return { BitfieldOpType::Incrby,type,offset,increment,overflow };
            }
        };
        Self& BitCount( const std::string& key)
        {
            return Cmd("BITCOUNT").Key(key);
        }
        Self& BitCount( const std::string& key, int start, int end)
        {
            return Cmd("BITCOUNT").Key(key).Arg(start).Arg(end);
        }
        Self& BitField( const std::string& key, const std::vector<BitFieldOperation>& operations)
        {
            auto& cmd = Cmd("BITFIELD").Key(key);
            for (const auto& operation : operations) {
                cmd.Arg(BitfieldOpToString(operation.op))
                    .Arg(operation.type)
                    .Arg(operation.offset);
                if ( operation.op == BitfieldOpType::Set || operation.op == BitfieldOpType::Incrby ) {
                    cmd.Arg(operation.value);
                }
                if ( operation.overflow != OverflowType::ServerDefault ) {
                    cmd.Arg( "OVERFLOW" );
                    cmd.Arg( OverflowTypeToString( operation.overflow ) );
                }
            }
            return cmd;
        }

        Self& BitOP( const std::string& operation, const std::string& destkey, const std::vector<std::string>& keys)
        {
            return Cmd("BITOP").Arg(operation).Key(destkey).Arg(keys);
        }
        Self& BitPos( const std::string& key, int bit)
        {
            return Cmd("BITPOS").Key(key).Arg(bit);
        }
        Self& BitPos( const std::string& key, int bit, int start)
        {
            return Cmd("BITPOS").Key(key).Arg(bit).Arg(start);
        }
        Self& BitPos( const std::string& key, int bit, int start, int end)
        {
            return Cmd("BITPOS").Key(key).Arg(bit).Arg(start).Arg(end);
        }
        Self& GetBit( const std::string& key, int offset)
        {
            return Cmd("GETBIT").Key(key).Arg(offset);
        }
        Self& SetBit( const std::string& key, int offset, const std::string& value)
        {
            return Cmd("SETBIT").Key(key).Arg(offset).Arg(value);
        }
    public:
        ////////////////////////////////////////////////////////////////////////////
        //pubsub
        Self& Publish( const std::string& channel, const std::string& message)
        {
            return Cmd("PUBLISH").Arg(channel).Arg(message);
        }
        Self& PubSub( const std::string& subcommand, const std::vector<std::string>& args)
        {
            return Cmd("PUBLISH").Arg(subcommand).Arg(args);
        }
    public:
        ////////////////////////////////////////////////////////////////////////////
        //transaction
        Self& Multi() {
            return Cmd("MULTI");
        }
        Self& Exec()
        {
            return Cmd("EXEC");
        }
        Self& DisCard()
        {
            return Cmd("DISCARD");
        }
        Self& UnWatch()
        {
            return Cmd("UNWATCH");
        }
        Self& Watch( const std::vector<std::string>& keys)
        {
            return  Cmd("WATCH").Arg(keys);
        }
    public:
        ////////////////////////////////////////////////////////////////////////////
        //Client info
        Self& ClientId() {
            return Cmd("CLIENT").Arg("ID");
        }

        Self& ClientInfo() {
            return Cmd("CLIENT").Arg("INFO");
        }

        Self& ClientGetName() {
            return Cmd("CLIENT").Arg("GETNAME");
        }

        Self& ClientSetName(const std::string& name) {
            return Cmd("CLIENT").Arg("SETNAME").Arg(name);
        }
     public:
        ////////////////////////////////////////////////////////////////////////////
        //script
        Self& Eval( const std::string& script, const std::vector<std::string>& keys,
                    const std::vector<std::string>& args)
        {
            return Cmd("EVAL").Arg(script).Arg(keys.size()).Arg(keys).Arg(args);
        }
        Self& EvalSha( const std::string& sha1, const std::vector<std::string>& keys,
                       const std::vector<std::string>& args)
        {
            return Cmd("EVALSHA").Arg(sha1).Arg(keys.size()).Arg(keys).Arg(args);
        }
        
        Self& ScriptDebug( const std::string& mode)
        {
            return Cmd("SCRIPT").Arg("DEBUG").Arg(mode);
        }
        Self& ScriptExists( const std::vector<std::string>& scripts)
        {
            return Cmd("SCRIPT").Arg("EXISTS").Arg(scripts);
        }
        Self& ScriptFlush()
        {
            return Cmd("SCRIPT").Arg("FLUSH");
        }
        Self& ScriptKill()
        {
            return Cmd("SCRIPT").Arg("KILL");
        }
        Self& ScriptLoad( const std::string& script)
        {
            return Cmd("SCRIPT").Arg("LOAD").Arg(script);
        }
    public:
        ////////////////////////////////////////////////////////////////////////////
        //hash
        Self& HDel( const std::string& key, const std::vector<std::string>& fields)
        {
            return Cmd("HDEL").Key(key).Arg(fields);
        }
        Self& HExists( const std::string& key, const std::string& field )
        {
            return Cmd("HEXISTS").Key(key).Arg(field);
        }
        Self& HGet( const std::string& key, const std::string& field )
        {
            return Cmd("HGET").Key(key).Arg(field);
        }
        Self& HGetAll( const std::string& key )
        {
            return Cmd("HGETALL").Key(key);
        }
        Self& HIncrby( const std::string& key, const std::string& field, int incr )
        {
            return Cmd("HINCRBY").Key(key).Arg(field).Arg(incr);
        }
        Self& HIncrbyFloat( const std::string& key, const std::string& field, float incr)
        {
            return Cmd("HINCRBYFLOAT").Key(key).Arg(field).Arg(incr);
        }
        Self& HKeys( const std::string& key )
        {
            return Cmd("HKEYS").Key(key);
        }
        Self& HLen( const std::string& key )
        {
            return Cmd("HLEN").Key(key);
        }
        Self& HMGet( const std::string& key, const std::vector<std::string>& fields )
        {
            return Cmd("HMGET").Key(key).Arg(fields);
        }
        Self& HMSet( const std::string& key, const std::vector<std::pair<std::string, std::string>>& field_val)
        {
            auto& cmd =  Cmd("HMSET").Key(key);
            for(auto& kv:field_val)
            {
                cmd.Arg(kv.first).Arg(kv.second);
            }
            return cmd;
        }
        Self& HScan( const std::string& key, std::size_t cursor )
        {
            return HScan(key, cursor, "", 0);
        }
        Self& HScan( const std::string& key, std::size_t cursor, const std::string& pattern)
        {
            return HScan(key, cursor, pattern, 0);
        }
        Self& HScan( const std::string& key, std::size_t cursor, std::size_t count )
        {
            return HScan(key, cursor, "", count);
        }
        Self& HScan( const std::string& key, std::size_t cursor, const std::string& pattern, std::size_t count)
        {
            auto& cmd=Cmd("HSCAN").Key(key).Arg(cursor);
            if (!pattern.empty())cmd.Arg("MATCH").Arg(pattern);
            if (count > 0)cmd.Arg("COUNT").Arg(count);
            return cmd;
        }
        Self& HSet( const std::string& key, const std::string& field, const std::string& value)
        {
            return Cmd("HSET").Key(key).Arg(field).Arg(value);
        }
        Self& HSetNX( const std::string& key, const std::string& field, const std::string& value)
        {
            return Cmd("HSETNX").Key(key).Arg(field).Arg(value);
        }
        Self& HStrLen( const std::string& key, const std::string& field )
        {
            return Cmd("HSTRLEN").Key(key).Arg(field);
        }
        Self& HVals( const std::string& key )
        {
            return Cmd("HVALS").Key(key);
        }
    public:
        ////////////////////////////////////////////////////////////////////////////
        //set
        Self& SAdd( const std::string& key, const std::vector<std::string>& members )
        {
            return Cmd("SADD").Key(key).Arg(members);
        }
        Self& SCard( const std::string& key )
        {
            return Cmd("SCARD").Key(key);
        }
        Self& SDiff( const std::vector<std::string>& keys )
        {
            FOLLY_SAFE_CHECK(!keys.empty(), "redis SDIFF need at least one key");
            return Cmd("SDIFF").SetKey(keys.front()).Arg(keys);
        }
        Self& SDiffstore( const std::string& destination, const std::vector<std::string>& keys)
        {
            return Cmd("SDIFFSTORE").SetKey(destination).Arg(destination).Arg(keys);
        }
        Self& SInter( const std::vector<std::string>& keys )
        {
            FOLLY_SAFE_CHECK(!keys.empty(), "redis SINTER need at least one key");
            return Cmd("SINTER").SetKey(keys.front()).Arg(keys);
        }
        Self& SInterstore( const std::string& destination, const std::vector<std::string>& keys)
        {
            return Cmd("SINTERSTORE").Key(destination).Arg(keys);
        }
        Self& SIsmember( const std::string& key, const std::string& member )
        {
            return Cmd("SISMEMBER").Key(key).Arg(member);
        }
        Self& SMembers( const std::string& key )
        {
            return Cmd("SMEMBERS").Key(key);
        }
        Self& SMove( const std::string& source, const std::string& destination, const std::string& member)
        {
            return Cmd("SMOVE").Key(source).Arg(destination).Arg(member);
        }
        Self& SPop( const std::string& key )
        {
            return Cmd("SPOP").Key(key);
        }
        Self& SPop( const std::string& key, int count )
        {
            return  Cmd("SPOP").Key(key).Arg(count);
        }
        Self& SRandmember( const std::string& key )
        {
            return Cmd("SRANDMEMBER").Key(key);
        }
        Self& SRandmember( const std::string& key, int count )
        {
            return Cmd("SRANDMEMBER").Key(key).Arg(count);
        }
        Self& SRem( const std::string& key, const std::vector<std::string>& members )
        {
            return Cmd("SREM").Key(key).Arg(members);
        }
        Self& SScan( const std::string& key, std::size_t cursor )
        {
            return SScan(key, cursor, "", 0);
        }
        Self& SScan( const std::string& key, std::size_t cursor, const std::string& pattern)
        {
            return SScan(key, cursor, pattern, 0);
        }
        Self& SScan( const std::string& key, std::size_t cursor, std::size_t count )
        {
            return SScan(key, cursor, "", count);
        }
        Self& SScan( const std::string& key, std::size_t cursor, const std::string& pattern, std::size_t count)
        {
            auto& cmd = Cmd("SSCAN").Key(key).Arg(cursor);
            if (!pattern.empty()) cmd.Arg("MATCH").Arg(pattern);
            if (count > 0) cmd.Arg("COUNT").Arg(count);
        }
        Self& SUnion( const std::vector<std::string>& keys )
        {
            FOLLY_SAFE_CHECK(!keys.empty(), "redis SUNION need at least one key");
            return Cmd("SUNION").SetKey(keys.front()).Arg(keys);
        }
        Self& SUnionStore( const std::string& destination, const std::vector<std::string>& keys)
        {
            return Cmd("SUNIONSTORE").Key(destination).Arg(keys);
        }
    public:
        ////////////////////////////////////////////////////////////////////////////
        //zset
        enum class AggregateMethod {
            Sum,
            Min,
            Max,
            ServerDefault,
        };
        static std::string AggregateMethodToString( AggregateMethod method );
        Self& BZPopMin( const std::vector<std::string>& keys, int timeout )
        {
            FOLLY_SAFE_CHECK(!keys.empty(), "redis BZPOPMIN need at least one key");
            return Cmd("BZPOPMIN").SetKey(keys.front()).Arg(keys).Arg(timeout);
        }
        Self& BZPopMax( const std::vector<std::string>& keys, int timeout )
        {
            FOLLY_SAFE_CHECK(!keys.empty(), "redis BZPOPMAX need at least one key");
            return Cmd("BZPOPMAX").SetKey(keys.front()).Arg(keys).Arg(timeout);
        }
        Self& ZPopMin( const std::string& key, int count )
        {
            return Cmd("ZPOPMIN").Key(key).Arg(count);
        }
        Self& ZPopMax( const std::string& key, int count )
        {
            return Cmd("ZPOPMAX").Key(key).Arg(count);
        }

        Self& ZAdd( const std::string& key, const std::vector<std::string>& options,
                    const std::multimap<std::string, std::string>& score_members)
        {
            auto& cmd = Cmd("ZADD").Key(key).Arg(options);
            for(auto& kv:score_members)
            {
                cmd.Arg(kv.first).Arg(kv.second);
            }
            return cmd;
        }

        Self& ZCard( const std::string& key )
        {
            return Cmd("ZCARD").Key(key);
        }
        Self& ZCount( const std::string& key, int min, int max )
        {
            return Cmd("ZCOUNT").Key(key).Arg(min).Arg(max);
        }
        Self& ZCount( const std::string& key, double min, double max )
        {
            return Cmd("ZCOUNT").Key(key).Arg(min).Arg(max);
        }
        Self& ZCount( const std::string& key, const std::string& min, const std::string& max)
        {
            return Cmd("ZCOUNT").Key(key).Arg(min).Arg(max);
        }
        Self& ZIncrby( const std::string& key, int incr, const std::string& member )
        {
            return Cmd("ZINCRBY").Key(key).Arg(incr).Arg(member);
        }
        Self& ZIncrby( const std::string& key, double incr, const std::string& member )
        {
            return Cmd("ZINCRBY").Key(key).Arg(incr).Arg(member);
        }
        Self& ZIncrby( const std::string& key, const std::string& incr, const std::string& member)
        {
            return Cmd("ZINCRBY").Key(key).Arg(incr).Arg(member);
        }
        Self& ZInterStore( const std::string& destination, std::size_t numkeys, const std::vector<std::string>& keys,
                           std::vector<std::size_t> weights, AggregateMethod method)
        {
            auto& cmd =Cmd("ZINTERSTORE").Key(destination).Arg(numkeys).Arg(keys);
            if(!weights.empty())
            {
                cmd.Arg("WEIGHTS");
                for(auto& w:weights)
                {
                    cmd.Arg(w);
                }
            }
            if(method!=AggregateMethod::ServerDefault)
            {
                cmd.Arg("AGGREGATE").Arg(AggregateMethodToString(method));
            }
            return cmd;
        }
        Self& ZLexCount( const std::string& key, int min, int max )
        {
            return Cmd("ZLEXCOUNT").Key(key).Arg(min).Arg(max);
        }
        Self& ZLexCount( const std::string& key, double min, double max )
        {
            return Cmd("ZLEXCOUNT").Key(key).Arg(min).Arg(max);
        }
        Self& ZLexCount( const std::string& key, const std::string& min, const std::string& max)
        {
            return Cmd("ZLEXCOUNT").Key(key).Arg(min).Arg(max);
        }
        template<class T>
        Self& ZRange( const std::string& key, T start, T stop,bool withscores)
        {
            auto& cmd = Cmd("ZRANGE").Key(key).Arg(start).Arg(stop);
            if (withscores)cmd.Arg("WITHSCORES");
            return cmd;
        }
        Self& ZRange( const std::string& key, const std::string& start, const std::string& stop )
        {
            return ZRange( key, start, stop, false );
        }
        template<class T>
        Self& ZRangeByLex( const std::string& key, T min, T max )
        {
            return ZRangeByLex( key, min , max, false );
        }
        template<class T>
        Self& ZRangeByLex( const std::string& key, T min, T max, bool withscores )
        {
            return ZRangeByLex( key, min, max,0,0, withscores );
        }
        template<class T>
        Self& ZRangeByLex( const std::string& key, T min, T max, std::size_t offset, std::size_t count )
        {
            return ZRangeByLex( key, min, max, offset, count, false );
        }
        template<class T>
        Self& ZRangeByLex( const std::string& key, T min, T max, std::size_t offset, std::size_t count, bool withscores )
        {
            auto& cmd = Cmd("ZRANGEBYLEX").Key(key).Arg(min).Arg(max);
            if(withscores)cmd.Arg("WITHSCORES");
            if(offset !=0 || count !=0)
            {
                cmd.Arg(offset).Arg(count);
            }
            return cmd;
        }

        template<class T>
        Self& ZRangeByScore( const std::string& key, T min, T max )
        {
            return ZRangeByScore( key,  min, max, false );
        }
        template<class T>
        Self& ZRangeByScore( const std::string& key, T min, T max, bool withscores )
        {
            return ZRangeByScore( key,  min, max,0,0, withscores );
        }
        template<class T>
        Self& ZRangeByScore( const std::string& key, T min, T max, std::size_t offset, std::size_t count )
        {
            return ZRangeByScore( key,  min , max , offset, count, false );
        }
        template<class T>
        Self& ZRangeByScore( const std::string& key, T min, T max, std::size_t offset, std::size_t count, bool withscores )
        {
            auto& cmd =Cmd("ZRANGEBYSCORE").Key(key).Arg(min).Arg(max);
            if ( withscores ) {
                cmd.Arg( "WITHSCORES" );
            }
    
            if ( offset != 0 || count != 0 ) {
                cmd.Arg( "LIMIT" ).Arg(offset).Arg(count);
            }
            return cmd;
        }

        Self& ZRank( const std::string& key, const std::string& member )
        {
            return Cmd("ZRANK").Key(key).Arg(member);
        }

        Self& ZRem( const std::string& key, const std::vector<std::string>& members )
        {
            return Cmd("ZREM").Key(key).Arg(members);
        }

        template<class T>
        Self& ZRemRangeByLex( const std::string& key, T min, T max )
        {
            return Cmd("ZREMRANGEBYLEX").Key(key).Arg(min).Arg(max);
        }

        template<class T>
        Self& ZRemRangeByRank( const std::string& key, T start, T stop ) {
            return Cmd("ZREMRANGEBYRANK").Key(key).Arg(start).Arg(stop);
        }
        template<class T>
        Self& ZRemRangeByScore( const std::string& key, T min, T max )
        {
            return Cmd("ZREMRANGEBYSCORE").Key(key).Arg(min).Arg(max);
        }
        template<class T>
        Self& ZRevRange(const std::string& key, T start, T stop)
        {
            return ZRevRange(key, start, stop, false);
        }
        template<class T>
        Self& ZRevRange( const std::string& key, T start, T stop, bool withscores )
        {
            auto& cmd = Cmd("ZREVRANGE").Key(key).Arg(start).Arg(stop);
            if (withscores)cmd.Arg("WITHSCORES");
            return cmd;
        }

        template<class T>
        Self& ZRevRangeByLex( const std::string& key, T max, T min )
        {
            return ZRevRangeByLex( key, max, min, false );
        }
        template<class T>
        Self& ZRevRangeByLex( const std::string& key, T max, T min, bool withscores )
        {
            return ZRevRangeByLex(key, max, min, 0, 0, withscores);
        }

        template<class T>
        Self& ZRevRangeByLex( const std::string& key, T max, T min, std::size_t offset, std::size_t count )
        {
            return ZRevRangeByLex( key, max, min, offset, count, false );
        }
        template<class T>
        Self& ZRevRangeByLex( const std::string& key, T max, T min, std::size_t offset, std::size_t count, bool withscores )
        {
            auto& cmd = Cmd("ZREVRANGEBYLEX").Key(key).Arg(max).Arg(min);
            if (withscores)cmd.Arg("WITHSCORES");
            if (offset != 0 || count != 0)cmd.Arg("LIMIT").Arg(offset).Arg(count);
            return cmd;
        }

        template<class T>
        Self& ZRevRangeByScore( const std::string& key, T&& max, T&& min, bool withscores )
        {
            return ZRevRangeByScore( key, std::forward<T>( max ), std::forward<T>(min ),0,0, withscores );
        }
        template<class T>
        Self& ZRevRangeByScore( const std::string& key, T&& max, T&& min )
        {
            return ZRevRangeByScore( key, std::forward<T>(max), std::forward<T>(min), false );
        }
        template<class T>
        Self& ZRevRangeByScore( const std::string& key, T&& max, T&& min, std::size_t offset, std::size_t count )
        {
            return ZRevRangeByScore( key, std::forward<T>( max ), std::forward<T>( min ), offset, count, false );
        }
        template<class T>
        Self& ZRevRangeByScore( const std::string& key, T&& max, T&& min, std::size_t offset, std::size_t count, bool withscores )
        {
            auto& cmd = Cmd("ZREVRANGEBYSCORE").Key(key).Arg(std::forward<T>(max)).Arg(std::forward<T>(min));
            if (withscores)cmd.Arg("WITHSCORES");
            if (offset != 0 || count != 0)cmd.Arg("LIMIT").Arg(offset).Arg(count);
            return cmd;
        }

        Self& ZRevRank( const std::string& key, const std::string& member )
        {
            return Cmd("ZREVRANK").Key(key).Arg(member);
        }

        Self& ZScan( const std::string& key, std::size_t cursor )
        {
            return ZScan(key, cursor, "", 0);
        }
        Self& ZScan( const std::string& key, std::size_t cursor, const std::string& pattern )
        {
            return ZScan(key, cursor, pattern, 0);
        }
        Self& ZScan( const std::string& key, std::size_t cursor, std::size_t count )
        {
            return ZScan(key, cursor, "", count);
        }
        Self& ZScan( const std::string& key, std::size_t cursor, const std::string& pattern, std::size_t count )
        {
            auto& cmd = Cmd("ZSCAN").Key(key).Arg(cursor);
            if (!pattern.empty())cmd.Arg("MATCH").Arg(pattern);
            if (count > 0)cmd.Arg("COUNT").Arg(count);
            return cmd;
        }

        Self& ZScore( const std::string& key, const std::string& member )
        {
            return Cmd("ZSCORE").Key(key).Arg(member);
        }

        Self& ZUnionStore( const std::string& destination, std::size_t numkeys, const std::vector<std::string>& keys,
                           std::vector<std::size_t> weights, AggregateMethod method)
        {
            auto& cmd = Cmd("ZUNIONSTORE").Key(destination).Arg(numkeys).Arg(keys);
            if(!weights.empty())
            {
                cmd.Arg("WEIGHTS");
                for (auto& w : weights)cmd.Arg(w);
            }
            if(method !=AggregateMethod::ServerDefault)
            {
                cmd.Arg("AGGREGATE").Arg(AggregateMethodToString(method));
            }
            return cmd;
        }
    public:
        ////////////////////////////////////////////////////////////////////////////
        //list
        Self& BLpop( const std::vector<std::string>& keys, int timeout )
        {
            FOLLY_SAFE_CHECK(!keys.empty(), "redis BLPOP need at least one key");
            return Cmd("BLPOP").SetKey(keys.front()).Arg(keys).Arg(timeout);
        }
        Self& BRpop( const std::vector<std::string>& keys, int timeout )
        {
            FOLLY_SAFE_CHECK(!keys.empty(), "redis BRPOP need at least one key");
            return Cmd("BRPOP").SetKey(keys.front()).Arg(keys).Arg(timeout);
        }
        Self& BRpoplpush( const std::string& src, const std::string& dst, int timeout )
        {
            return Cmd("BRPOPLPUSH").Key(src).Arg(dst).Arg(timeout);
        }

        Self& LIndex( const std::string& key, int index )
        {
            return Cmd("LINDEX").Key(key).Arg(index);
        }
        Self& LInsert( const std::string& key, const std::string& before_after, const std::string& pivot,
                       const std::string& value )
        {
            return Cmd("LINSERT").Key(key).Arg(before_after).Arg(pivot).Arg(value);
        }
        Self& LLen( const std::string& key )
        {
            return Cmd("LLEN").Key(key);
        }
        Self& LPop( const std::string& key )
        {
            return Cmd("LPOP").Key(key);
        }
        Self& LPush( const std::string& key, const std::vector<std::string>& values )
        {
            return Cmd("LPUSH").Key(key).Arg(values);
        }
        Self& LPushX( const std::string& key, const std::string& value )
        {
            return Cmd("LPUSHX").Key(key).Arg(value);
        }
        Self& LRange( const std::string& key, int start, int stop )
        {
            return Cmd("LRANGE").Key(key).Arg(start).Arg(stop);
        }
        Self& LRem( const std::string& key, int count, const std::string& value )
        {
            return Cmd("LREM").Key(key).Arg(count).Arg(value);
        }
        Self& LSet( const std::string& key, int index, const std::string& value )
        {
            return Cmd("LSET").Key(key).Arg(index).Arg(value);
        }
        Self& LTrim( const std::string& key, int start, int stop )
        {
            return Cmd("LTRIM").Key(key).Arg(start).Arg(stop);
        }

        Self& RPop( const std::string& key )
        {
            return Cmd("LPOP").Key(key);
        }
        Self& RPopLPush( const std::string& source, const std::string& destination )
        {
            return Cmd("RPopLPush").Key(source).Arg(destination);
        }
        Self& RPush( const std::string& key, const std::vector<std::string>& values )
        {
            return Cmd("RPUSH").Key(key).Arg(values);
        }
        Self& RPushX( const std::string& key, const std::string& value )
        {
            return Cmd("RPUSHX").Key(key).Arg(value);
        }
    public:
        ////////////////////////////////////////////////////////////////////////////
        struct XClaimOption
        {
            int64_t      Idle;
            std::time_t* Time;
            int64_t      RetryCount;
            bool         Force;
            bool         JustId;
        };
        struct XRangeOption
        {
            std::string Start;
            std::string Stop;
            int64_t Count;
        };
        struct XPendingOption {
            XRangeOption Range;
            std::string Consumer;
        };
        using Streams_t = std::pair<std::vector<std::string>, std::vector<std::string>>;
        struct XReadOption {
            Streams_t Streams;
            int64_t Count;
            int64_t Block;
        };
        struct XReadGroupOption {
            std::string Group;
            std::string Consumer;
            Streams_t Streams;
            int64_t Count;
            int64_t Block;
            bool NoAck;
        };
        //stream
        Self& XAck( const std::string& stream, const std::string& group, const std::vector<std::string>& message_ids )
        {
            return Cmd("XACK").Key(stream).Arg(group).Arg(message_ids);
        }
        Self& XAdd( const std::string& key, const std::string& id, const std::multimap<std::string, std::string>& field_members )
        {
            auto& cmd = Cmd("XADD").Key(key).Arg(id);
            for(auto& kv:field_members)
            {
                cmd.Arg(kv.first).Arg(kv.second);
            }
            return cmd;
        }
        Self& XClaim( const std::string& stream, const std::string& group, const std::string& consumer, int min_idle_time,
                      const std::vector<std::string>& message_ids, const XClaimOption& options)
        {
            auto& cmd = Cmd("").Key(stream).Arg(group).Arg(consumer).Arg(min_idle_time).Arg(message_ids);
            if (options.Idle > 0)cmd.Arg("IDLE").Arg(options.Idle);
            if (options.Time != nullptr)cmd.Arg("TIME").Arg(*options.Time);
            if (options.RetryCount > 0)cmd.Arg("RETRYCOUNT").Arg(options.RetryCount);
            if (options.Force)cmd.Arg("FORCE");
            if (options.JustId)cmd.Arg("JUSTID");
            return cmd;
        }
        Self& XDel( const std::string& key, const std::vector<std::string>& id_members )
        {
            return Cmd("XDEL").Key(key).Arg(id_members);
        }
        Self& XGroupCreate( const std::string& key, const std::string& group_name )
        {
            return Cmd("XGROUP").Arg("CREATE").Key(key).Arg(group_name).Arg("$");
        }
        Self& XGroupCreate( const std::string& key, const std::string& group_name, const std::string& id )
        {
            return Cmd("XGROUP").Arg("CREATE").Key(key).Arg(group_name).Arg(id);
        }
        Self& XGroupCreate( const std::string& key, const std::string& group_name, const std::string& id, bool mkstream )
        {
            auto& cmd =Cmd("XGROUP").Arg("CREATE").Key(key).Arg(group_name).Arg(id);
            if (mkstream)cmd.Arg("MKSTREAM");
            return cmd;
        }

        Self& XGroupSetID( const std::string& key, const std::string& group_name )
        {
            return Cmd("XGROUP").Arg("SETID").Key(key).Arg(group_name).Arg("$");
        }
        Self& XGroupSetID( const std::string& key, const std::string& group_name, const std::string& id )
        {
            return Cmd("XGROUP").Arg("SETID").Key(key).Arg(group_name).Arg(id);
        }
        Self& XGroupDestroy( const std::string& key, const std::string& group_name )
        {
            return Cmd("XGROUP").Arg("DESTROY").Key(key).Arg(group_name);
        }
        Self& XGroupDelConsumer( const std::string& key, const std::string& group_name, const std::string& consumer_name )
        {
            return Cmd("XGROUP").Arg("DELCONSUMER").Key(key).Arg(group_name).Arg(consumer_name);
        }
        Self& XInfoConsumers( const std::string& key, const std::string& group_name )
        {
            return Cmd("XINFO").Arg("CONSUMERS").Key(key).Arg(group_name);
        }

        Self& XInfoGroups( const std::string& key )
        {
            return Cmd("XINFO").Arg("GROUPS").Key(key);
        }
        Self& XInfoStream( const std::string& stream )
        {
            return Cmd("XINFO").Arg("STREAM").Key(stream);
        }
        Self& XLen( const std::string& stream )
        {
            return Cmd("XLEN").Key(stream);
        }
        Self& XPending( const std::string& stream, const std::string& group, const XPendingOption& options )
        {
            auto& cmd = Cmd("XPENDING").Key(stream).Arg(group);
            if (!options.Range.Start.empty())cmd.Arg(options.Range.Start).Arg(options.Range.Stop).Arg(options.Range.Count);
            if (!options.Consumer.empty())cmd.Arg(options.Consumer);
            return cmd;
        }
        Self& XRange( const std::string& stream, const XRangeOption& options )
        {
            auto& cmd = Cmd("XRANGE").Key(stream);
            if (options.Count > 0)cmd.Arg("COUNT").Arg(options.Count);
            return cmd;
        }
        Self& XRead( const XReadOption& a )
        {
            FOLLY_SAFE_CHECK(!a.Streams.first.empty(), "redis XREAD need at least one stream");
            auto& cmd = Cmd("XREAD");
            if (a.Count > 0)cmd.Arg("COUNT").Arg(a.Count);
            if (a.Block > 0)cmd.Arg("BLOCK").Arg(a.Block);
            return cmd.Arg("STREAMS").SetKey(a.Streams.first.front()).Arg(a.Streams.first).Arg(a.Streams.second);
        }
        Self& XReadGroup( const XReadGroupOption& a )
        {
            FOLLY_SAFE_CHECK(!a.Streams.first.empty(), "redis XREADGROUP need at least one stream");
            auto& cmd= Cmd("XREADGROUP").Arg("GROUP").Arg(a.Group).Arg(a.Consumer);
            if (a.Count > 0)cmd.Arg("COUNT").Arg(a.Count);
            if (a.Block > 0)cmd.Arg("BLOCK").Arg(a.Block);
            if (a.NoAck) cmd.Arg("NOACK");
            return cmd.Arg("STREAMS").SetKey(a.Streams.first.front()).Arg(a.Streams.first).Arg(a.Streams.second);
        }

        Self& XRevRange( const std::string& key, const XRangeOption& range_args )
        {
            auto& cmd = Cmd("XREVRANGE").Key("key").Arg(range_args.Stop).Arg(range_args.Start);
            if (range_args.Count > 0)cmd.Arg(range_args.Count);
            return cmd;
        }

        Self& XTrim( const std::string& stream, int max_len )
        {
            return Cmd("XTRIM").Key(stream).Arg("MAXLEN").Arg(max_len);
        }
        Self& XTrimApprox( const std::string& key, int max_len )
        {
            return Cmd("XTRIM").Key(key).Arg("MAXLEN").Arg("~").Arg(max_len);
        }
    public:
        //执行结果
        folly::Future<Reply> Query();
        //不关心结果
        void Run();
        Self& Build()
        {
            buildCommand();
            return *this;
        }
    public:
        folly::IOBufQueue Serialize()const;
        void SerializeTo(folly::IOBufQueue& buf)const;
        std::vector<CommandVal> Commands()&&
        {
            return std::move(cmds_);
        }
        const std::vector<CommandVal>& Commands()const&
        {
            return cmds_;
        }
        bool Empty()const
        {
            return cmds_.empty();
        }
    private:
        friend class ClientInterface;
        explicit Command(std::shared_ptr<ClientInterface> client):pipe_(false),client_(std::move(client)){
            XLOG(DBG,"COMMAND new");
        };
        explicit Command(std::shared_ptr<ClientInterface> client,bool pipe):pipe_(pipe),client_(std::move(client)){
            XLOG(DBG,"COMMAND new");
        }
        Command(std::shared_ptr<ClientInterface> client,std::string key,bool pipe):pipe_(pipe),client_(std::move(client)){
            XLOG(DBG,"COMMAND new");
            if(!key.empty()){
                current_cmd_.emplace_back(std::move(key));
            }
        }
        void buildCommand(){
            if(current_cmd_.empty())return;
            std::string result = folly::to<std::string>("*",current_cmd_.size(),"\r\n");
            for ( const auto& part : current_cmd_ ) {
                folly::toAppend("$",part.length(),"\r\n",part,"\r\n",&result);
            }
            cmds_.emplace_back(std::move(result),std::move(current_key_), current_ignore_);
            current_cmd_.clear();
            current_key_.clear();
            current_ignore_=false;
        }
    private:
        std::vector<std::string> current_cmd_;
        std::string current_key_;
        bool current_ignore_{false};

        std::vector<CommandVal> cmds_;
        bool pipe_{false};
        std::shared_ptr<ClientInterface> client_;
    };
}
