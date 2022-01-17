#include "redis/reply.h"
#include <sstream>
namespace redis
{
    Reply::Reply( Reply&& other ) noexcept
    {
        if ( this != &other ) {
            type_ = other.type_;
            arr_val_ = std::move( other.arr_val_ );
            str_val_ = std::move( other.str_val_ );
            int_val_ = other.int_val_;
            other.type_ = Type::Null;
        }
    }

    std::string Reply::ToString()
    {
        std::stringstream ss;
        ss << *this;
        return ss.str();
    }

    const std::string& Reply::Error() const
    {
        if ( !IsError() )throw std::runtime_error( "Reply is not an error" );
        return str_val_;
    }

    const std::vector<Reply>& Reply::AsArray() const&
    {
        if ( !IsArray() )throw std::runtime_error( "Reply is not an array" );
        return arr_val_;
    }

    std::vector<Reply> Reply::AsArray() &&
    {
        if (!IsArray())throw std::runtime_error("Reply is not an array");
        return std::move(arr_val_);
    }

    const std::string& Reply::AsString() const&
    {
        if ( !IsString() )throw std::runtime_error( "Reply is not a string" );
        return str_val_;
    }

    std::string Reply::AsString() &&
    {
        if (!IsString())throw std::runtime_error("Reply is not a string");
        return std::move(str_val_);
    }
    int64_t Reply::AsInteger() const
    {
        if ( !IsInteger() )throw std::runtime_error( "Reply is not an integer" );
        return int_val_;
    }

    Reply& Reply::operator=( Reply&& other ) noexcept
    {
        if ( this != &other ) {
            type_ = other.type_;
            arr_val_ = std::move( other.arr_val_ );
            str_val_ = std::move( other.str_val_ );
            int_val_ = other.int_val_;
            other.type_ = Type::Null;
        }
        return *this;
    }
}

std::ostream& operator<<( std::ostream& os, const redis::Reply& reply )
{
    switch ( reply.GetType() )
    {
    case redis::Reply::Type::Null:
    {
        os << std::string( "(nil)" );
        break;
    }
    case redis::Reply::Type::SimpleString:
    case redis::Reply::Type::BulkString:
    case redis::Reply::Type::Error:
    {
        os << reply.AsString();
        break;
    }
    case redis::Reply::Type::Integer:
    {
        os << reply.AsInteger();
        break;
    }
    case redis::Reply::Type::Array:
    {
        os << "[";
        for ( const auto& item : reply.AsArray() )
            os << item << ",";
        os << "]";
        break;
    }
    default:
        break;
    }
    return os;
}
