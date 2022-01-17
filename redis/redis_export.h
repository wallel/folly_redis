#pragma once
#ifdef REDIS_STATIC_DEFINE
#  define REDIS_EXPORT
#  define REDIS_NO_EXPORT
#else
#  ifndef REDIS_EXPORT
#    ifdef REDIS_EXPORTS
        /* We are building this library */
#      define REDIS_EXPORT 
#    else
        /* We are using this library */
#      define REDIS_EXPORT 
#    endif
#  endif

#  ifndef REDIS_NO_EXPORT
#    define REDIS_NO_EXPORT 
#  endif
#endif

#ifndef REDIS_DEPRECATED
#  define REDIS_DEPRECATED __declspec(deprecated)
#endif

#ifndef REDIS_DEPRECATED_EXPORT
#  define REDIS_DEPRECATED_EXPORT REDIS_EXPORT REDIS_DEPRECATED
#endif

#ifndef REDIS_DEPRECATED_NO_EXPORT
#  define REDIS_DEPRECATED_NO_EXPORT REDIS_NO_EXPORT REDIS_DEPRECATED
#endif

#if 0 /* DEFINE_NO_DEPRECATED */
#  ifndef REDIS_NO_DEPRECATED
#    define REDIS_NO_DEPRECATED
#  endif
#endif
