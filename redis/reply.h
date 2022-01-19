#pragma once
#include <string>
#include <utility>
#include <vector>

#include "redis/redis_export.h"
namespace redis
{
    /**
     * redis reply
     */
    class REDIS_EXPORT Reply
    {
    public:
        enum class Type
        {
            Error = 0,
            BulkString = 1,
            SimpleString = 2,
            Null = 3,
            Integer = 4,
            Array = 5,
            AskError=6,
            MovedError=7,
        };
        enum class StringType
        {
            Error = 0,
            BulkString = 1,
            SimpleString = 2,
            AskError=6,
            MovedError=7,
        };
    public:
        Reply() : type_{ Type::Null } {};
        Reply( std::string  value, StringType type ) : type_{ static_cast<Type>( type ) }, str_val_(std::move( value )) {};
        explicit Reply( int64_t val ) : type_{ Type::Integer }, int_val_{val} {};
        explicit Reply( std::vector<Reply>  rows ) : type_{ Type::Array }, arr_val_{std::move( rows )} {};
    public:
        ~Reply() = default;
        Reply( const Reply& ) = default;
        Reply& operator=( const Reply& ) = default;
        Reply( Reply&& ) noexcept;
        Reply& operator=( Reply&& ) noexcept;
    public:
        bool IsArray()const {
            return type_ == Type::Array;
        };
        bool IsSimpleString() const {
            return type_ == Type::SimpleString;
        }
        bool IsError()const {
            return type_ == Type::Error;
        }
        bool IsMovedError() const {
            return type_ ==Type::MovedError;
        }
        bool IsAskError() const {
            return type_ == Type::AskError;
        }
        bool IsNull()const {
            return type_ == Type::Null;
        }
        bool IsBulkString()const {
            return type_ == Type::BulkString;
        }
        bool IsString()const {
            return IsBulkString() || IsSimpleString() || IsError() || IsMovedError() || IsAskError();
        }
        bool IsInteger()const {
            return type_ == Type::Integer;
        }
        bool Ok()const {
            return !IsError();
        }
        explicit operator bool() const {
            return !IsError() && !IsNull();
        };
        std::string ToString();
    public:
        const std::string& Error()const;
        const std::vector<Reply>& AsArray() const&;
        std::vector<Reply> AsArray() &&;
        const std::string& AsString() const&;
        std::string AsString()&&;
        int64_t AsInteger() const;
    public:
        void set() {
            type_ = Type::Null;
        };
        void set( std::string value, StringType type ) {
            type_ = static_cast<Type>( type );
            str_val_ = std::move(value);
        }
        void set( int64_t value )
        {
            type_ = Type::Integer;
            int_val_ = value;
        }
        void set( const std::vector<Reply>& rows )
        {
            type_ = Type::Array;
            arr_val_ = rows;
        }
        Reply& operator<<( const Reply& reply ) {
            type_ = Type::Array;
            arr_val_.push_back( reply );
            return *this;
        }
        Reply& operator<<( Reply&& reply ) {
            type_ = Type::Array;
            arr_val_.push_back( std::move(reply) );
            return *this;
        }
    public:
        Type GetType() const {
            return type_;
        }
    private:
        Type type_;
        std::vector<Reply> arr_val_;
        std::string str_val_{};
        int64_t int_val_{};
    };
}

REDIS_EXPORT std::ostream&  operator<<( std::ostream& os, const redis::Reply& reply );

