/* This file must be kept in sync with extension.stub.php. */

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_MASK_EX(arginfo_franken_tiered_memory_get, 0, 1, MAY_BE_STRING|MAY_BE_FALSE)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_tiered_memory_set, 0, 3, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, ttl, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_tiered_memory_forever, 0, 2, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_tiered_memory_forget, 0, 1, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_tiered_memory_touch, 0, 2, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, key, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, ttl, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_franken_tiered_memory_flush, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_FUNCTION(franken_tiered_memory_get);
ZEND_FUNCTION(franken_tiered_memory_set);
ZEND_FUNCTION(franken_tiered_memory_forever);
ZEND_FUNCTION(franken_tiered_memory_forget);
ZEND_FUNCTION(franken_tiered_memory_touch);
ZEND_FUNCTION(franken_tiered_memory_flush);

static const zend_function_entry franken_tiered_functions[] = {
    ZEND_FE(franken_tiered_memory_get, arginfo_franken_tiered_memory_get)
    ZEND_FE(franken_tiered_memory_set, arginfo_franken_tiered_memory_set)
    ZEND_FE(franken_tiered_memory_forever, arginfo_franken_tiered_memory_forever)
    ZEND_FE(franken_tiered_memory_forget, arginfo_franken_tiered_memory_forget)
    ZEND_FE(franken_tiered_memory_touch, arginfo_franken_tiered_memory_touch)
    ZEND_FE(franken_tiered_memory_flush, arginfo_franken_tiered_memory_flush)
    ZEND_FE_END
};
