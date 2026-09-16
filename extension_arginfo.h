/* This file must be kept in sync with extension.stub.php. */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_franken_cache_tiered_get, 0, 1, MAY_BE_STRING|MAY_BE_FALSE)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_cache_tiered_add, 0, 3, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, ttl, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_cache_tiered_set, 0, 3, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, ttl, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_cache_tiered_forever, 0, 2, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_cache_tiered_forget, 0, 1, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_cache_tiered_touch, 0, 2, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, ttl, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_cache_tiered_flush, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_cache_tiered_increment, 0, 2, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_cache_tiered_decrement, 0, 2, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(franken_cache_tiered_get);
ZEND_FUNCTION(franken_cache_tiered_add);
ZEND_FUNCTION(franken_cache_tiered_set);
ZEND_FUNCTION(franken_cache_tiered_forever);
ZEND_FUNCTION(franken_cache_tiered_forget);
ZEND_FUNCTION(franken_cache_tiered_touch);
ZEND_FUNCTION(franken_cache_tiered_flush);
ZEND_FUNCTION(franken_cache_tiered_increment);
ZEND_FUNCTION(franken_cache_tiered_decrement);

static const zend_function_entry franken_cache_functions[] = {
    ZEND_FE(franken_cache_tiered_get, arginfo_franken_cache_tiered_get)
    ZEND_FE(franken_cache_tiered_add, arginfo_franken_cache_tiered_add)
    ZEND_FE(franken_cache_tiered_set, arginfo_franken_cache_tiered_set)
    ZEND_FE(franken_cache_tiered_forever, arginfo_franken_cache_tiered_forever)
    ZEND_FE(franken_cache_tiered_forget, arginfo_franken_cache_tiered_forget)
    ZEND_FE(franken_cache_tiered_touch, arginfo_franken_cache_tiered_touch)
    ZEND_FE(franken_cache_tiered_flush, arginfo_franken_cache_tiered_flush)
    ZEND_FE(franken_cache_tiered_increment, arginfo_franken_cache_tiered_increment)
    ZEND_FE(franken_cache_tiered_decrement, arginfo_franken_cache_tiered_decrement)
    ZEND_FE_END
};
