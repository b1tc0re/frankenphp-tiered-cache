#include "extension.h"
#include "extension_arginfo.h"

#include "_cgo_export.h"

static void franken_tiered_throw_memory_error(void)
{
    zend_throw_error(NULL, "franken_tiered MemoryCache operation failed");
}

PHP_FUNCTION(franken_tiered_memory_get)
{
    zend_string *key;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(key)
    ZEND_PARSE_PARAMETERS_END();

    int status = 0;
    zend_string *value = franken_tiered_memory_get_go(key, &status);
    if (status < 0) {
        franken_tiered_throw_memory_error();
        RETURN_THROWS();
    }
    if (status == 0) {
        RETURN_FALSE;
    }
    if (value == NULL) {
        RETURN_EMPTY_STRING();
    }

    RETURN_STR(value);
}

PHP_FUNCTION(franken_tiered_memory_set)
{
    zend_string *key;
    zend_string *value;
    zend_long ttl;

    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STR(key)
        Z_PARAM_STR(value)
        Z_PARAM_LONG(ttl)
    ZEND_PARSE_PARAMETERS_END();

    if (ttl <= 0) {
        zend_value_error("ttl must be greater than zero");
        RETURN_THROWS();
    }

    int result = franken_tiered_memory_set_go(key, value, ttl);
    if (result < 0) {
        franken_tiered_throw_memory_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_tiered_memory_forever)
{
    zend_string *key;
    zend_string *value;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(key)
        Z_PARAM_STR(value)
    ZEND_PARSE_PARAMETERS_END();

    int result = franken_tiered_memory_forever_go(key, value);
    if (result < 0) {
        franken_tiered_throw_memory_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_tiered_memory_forget)
{
    zend_string *key;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(key)
    ZEND_PARSE_PARAMETERS_END();

    int result = franken_tiered_memory_forget_go(key);
    if (result < 0) {
        franken_tiered_throw_memory_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_tiered_memory_touch)
{
    zend_string *key;
    zend_long ttl;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(key)
        Z_PARAM_LONG(ttl)
    ZEND_PARSE_PARAMETERS_END();

    if (ttl <= 0) {
        zend_value_error("ttl must be greater than zero");
        RETURN_THROWS();
    }

    int result = franken_tiered_memory_touch_go(key, ttl);
    if (result < 0) {
        franken_tiered_throw_memory_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_tiered_memory_flush)
{
    ZEND_PARSE_PARAMETERS_NONE();

    int result = franken_tiered_memory_flush_go();
    if (result < 0) {
        franken_tiered_throw_memory_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

zend_module_entry franken_tiered_module_entry = {
    STANDARD_MODULE_HEADER,
    "franken_tiered",
    franken_tiered_functions,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    "0.0.0-dev",
    STANDARD_MODULE_PROPERTIES
};
