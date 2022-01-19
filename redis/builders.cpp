#include "redis/builders.h"

#include <stdexcept>

#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/io/Cursor.h>

#include "redis/util.h"
namespace redis{
    bool SimpleStringBuilder::Check()
    {
        if ( ready_ )return true;
        if(buf_.empty())return false;
        folly::io::Cursor cur(buf_.front());
        auto idx = -1;
        for(auto i=0; i < buf_.chainLength();i++){
            auto c = cur.read<char>();
            if(c=='\r' && *cur.peekBytes().data()=='\n'){
                idx=i;
                break;
            }
        }
        if(idx > 0){
            auto r = buf_.split(idx)->moveToFbString();
            buf_.trimStart(2);
            rpl_.set( r.toStdString(), Reply::StringType::SimpleString);
            ready_ = true;
            return true;
        }
        return false;
    }

    bool ErrorBuilder::Check()
    {
        ssbuilder_.Check();
        if ( ssbuilder_.IsReady() )
        {
            ready_ = ssbuilder_.IsReady();
            auto str =ssbuilder_.Build().AsString();
            auto type = Reply::StringType::Error;
             if(util::StartsWith(str,"ASK")){
                type =Reply::StringType::AskError;
             }else if(util::StartsWith(str,"MOVED")){
                 type =Reply::StringType::MovedError;
             }
            rpl_.set(std::move(str), type );
            return true;
        }
        return false;
    }
    bool BulkStringBuilder::fetchSize()
    {
        if ( intBuilder_.IsReady() )return true;
        intBuilder_.Check();
        if ( !intBuilder_.IsReady() )return false;
        size_ = intBuilder_.Integer();
        if ( size_ == 0 || size_ == -1 ) {
            rpl_.set();
            ready_ = true;
        }
        return true;
    }
    bool BulkStringBuilder::Check() {
        do
        {
            if ( ready_ ) break;
            if ( !fetchSize() || ready_ )break;
            fetchStr();
        } while ( false );
        return ready_;
    }

    bool BulkStringBuilder::fetchStr()
    {
        if(buf_.chainLength() < ( static_cast<std::size_t>( size_ ) + 2 ) ){
            return false;
        }
        folly::io::Cursor cur(buf_.front());
        cur.skip(size_);
        auto c = cur.read<char>();
        auto c1 = cur.read<char>();
        if ( c != '\r' || c1 !='\n') {
            throw std::runtime_error( "wrong ending sequence" );
        }
        auto r = buf_.split(size_)->moveToFbString();
        rpl_.set( r.toStdString(), Reply::StringType::BulkString);
        buf_.trimStart(2);
        ready_ = true;
        return true;
    }

    bool IntBuilder::Check() {
        if ( ready_ )return true;
        if(buf_.empty())return false;
        folly::io::Cursor cur(buf_.front());
        auto idx = -1;
        for(auto i=0; i < buf_.chainLength();i++){
            auto c = cur.read<char>();
            if(c=='\r' && *cur.peekBytes().data()=='\n'){
                idx=i;
                break;
            }
        }
        if(idx > 0){
            auto r = buf_.split(idx)->moveToFbString();
            buf_.trimStart(2);
            val_ = folly::to<int64_t>( r );
            rpl_.set( val_ );
            ready_ = true;
            return true;
        }
        return false;
    }

    bool ArrayBuilder::Check()
    {
        do
        {
            if(buf_.empty())break;
            if ( ready_ )break;
            if ( !fetchSize() )break;
            while (!ready_ )
            {
                if ( !fetchField() )break;
            }
        } while ( false );
        return ready_;
    }

    bool ArrayBuilder::fetchSize()
    {
        if ( intBuilder_.IsReady() )return true;
        intBuilder_.Check();
        if ( !intBuilder_.IsReady() )return false;
        size_ = intBuilder_.Integer();
        if ( size_ == 0ul ) {
            rpl_.set();
            ready_ = true;
        }
        return true;
    }
    std::unique_ptr<Builder> CreateBuilder( unsigned char id,folly::IOBufQueue& buf );

    bool ArrayBuilder::fetchField()
    {
        if(buf_.empty())return false;
        if ( !curbuilder_ ) {
            auto id =*buf_.front()->data();
            curbuilder_ = CreateBuilder( id,buf_ );
            buf_.trimStart(1);
        }
        curbuilder_->Check();
        if ( !curbuilder_->IsReady() )
            return false;
        rpl_ << curbuilder_->Build();
        curbuilder_.reset();
        if ( rpl_.AsArray().size() == size_ )ready_ = true;
        return true;
    }
    std::unique_ptr<Builder> CreateBuilder( unsigned char id,folly::IOBufQueue& buf )
    {
        switch ( id )
        {
        case '+':
            return std::make_unique<SimpleStringBuilder>(buf);
        case '-':
            return std::make_unique<ErrorBuilder>(buf);
        case ':':
            return std::make_unique<IntBuilder>(buf);
        case '$':
            return std::make_unique<BulkStringBuilder>(buf);
        case '*':
            return std::make_unique<ArrayBuilder>(buf);
        default:
            return nullptr;
        }
    }

    void ReplyBuilder::operator>>( Reply& rpl ) const
    {
        rpl = GetFront();
    }

    const Reply& ReplyBuilder::GetFront() const
    {
        if ( !IsReplyAvailable() )
        {
            throw std::runtime_error( "no reply" );
        }
        return valid_replies_.front();
    }

    void ReplyBuilder::PopFront()
    {
        if ( !IsReplyAvailable() )
        {
            throw std::runtime_error( "no reply" );
        }
        valid_replies_.pop_front();
    }

    bool ReplyBuilder::IsReplyAvailable() const
    {
        return !valid_replies_.empty();
    }

    void ReplyBuilder::Reset()
    {
        builder_.reset();
        buffer_.clear();
    }

    bool ReplyBuilder::Build()
    {
        if ( buffer_.empty() )return false;

        if ( !builder_ ) {
            auto id =*buffer_.front()->data();
            builder_ = CreateBuilder( id,buffer_ );
            buffer_.trimStart( 1);
        }
        builder_->Check();
        if ( builder_->IsReady() ) {
            valid_replies_.push_back( builder_->Build() );
            builder_.reset();
            return true;
        }
        return false;
    }
}