#include "extension.h"
#include "extension_arginfo.h"

#include "_cgo_export.h"

#include <string.h>

static void franken_cache_throw_tiered_error(void)
{
    zend_throw_error(NULL, "franken_cache TieredCache operation failed");
}

PHP_FUNCTION(franken_cache_tiered_get)
{
    zend_string *key;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(key)
    ZEND_PARSE_PARAMETERS_END();

    int status = 0;
    zend_string *value = franken_cache_tiered_get_go(key, &status);
    if (status < 0) {
        franken_cache_throw_tiered_error();
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

PHP_FUNCTION(franken_cache_tiered_many)
{
    zval *keys;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ARRAY(keys)
    ZEND_PARSE_PARAMETERS_END();

    array_init(return_value);
    uint32_t count = zend_hash_num_elements(Z_ARRVAL_P(keys));
    if (count == 0) {
        return;
    }

    franken_cache_many_item *items = safe_emalloc(count, sizeof(*items), 0);
    uint32_t index = 0;
    int invalid_key = 0;
    zval *entry;

    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(keys), entry) {
        if (Z_TYPE_P(entry) != IS_STRING) {
            zend_type_error("franken_cache_tiered_many() keys must be strings");
            invalid_key = 1;
            break;
        }
        items[index].key = Z_STR_P(entry);
        items[index].value = NULL;
        items[index].found = 0;
        index++;
    } ZEND_HASH_FOREACH_END();

    int result = -1;
    if (!invalid_key) {
        result = franken_cache_tiered_many_go(items, index);
    }

    if (invalid_key || result < 0) {
        for (uint32_t i = 0; i < index; i++) {
            if (items[i].value != NULL) {
                zend_string_release(items[i].value);
            }
        }
        efree(items);
        if (!invalid_key) {
            franken_cache_throw_tiered_error();
        }
        RETURN_THROWS();
    }

    for (uint32_t i = 0; i < index; i++) {
        if (items[i].found) {
            if (items[i].value == NULL) {
                add_assoc_stringl_ex(
                    return_value,
                    ZSTR_VAL(items[i].key),
                    ZSTR_LEN(items[i].key),
                    "",
                    0
                );
                continue;
            }
            add_assoc_str_ex(
                return_value,
                ZSTR_VAL(items[i].key),
                ZSTR_LEN(items[i].key),
                items[i].value
            );
            items[i].value = NULL;
        } else {
            add_assoc_bool_ex(
                return_value,
                ZSTR_VAL(items[i].key),
                ZSTR_LEN(items[i].key),
                0
            );
        }
    }
    efree(items);
}

PHP_FUNCTION(franken_cache_tiered_add)
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

    int result = franken_cache_tiered_add_go(key, value, ttl);
    if (result < 0) {
        franken_cache_throw_tiered_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_cache_tiered_put_many)
{
    zval *values;
    zend_long ttl;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_ARRAY(values)
        Z_PARAM_LONG(ttl)
    ZEND_PARSE_PARAMETERS_END();

    if (ttl <= 0) {
        zend_value_error("ttl must be greater than zero");
        RETURN_THROWS();
    }

    uint32_t count = zend_hash_num_elements(Z_ARRVAL_P(values));
    if (count == 0) {
        RETURN_FALSE;
    }

    franken_cache_put_many_item *items = safe_emalloc(count, sizeof(*items), 0);
    zend_string **owned_keys = ecalloc(count, sizeof(*owned_keys));
    uint32_t index = 0;
    int invalid_value = 0;
    zval *entry;
    zend_ulong numeric_key;
    zend_string *key;

    ZEND_HASH_FOREACH_KEY_VAL(Z_ARRVAL_P(values), numeric_key, key, entry) {
        if (Z_TYPE_P(entry) != IS_STRING) {
            zend_type_error("franken_cache_tiered_put_many() values must be strings");
            invalid_value = 1;
            break;
        }

        if (key != NULL) {
            items[index].key = key;
        } else {
            owned_keys[index] = zend_long_to_str(numeric_key);
            items[index].key = owned_keys[index];
        }
        items[index].value = Z_STR_P(entry);
        index++;
    } ZEND_HASH_FOREACH_END();

    int result = -1;
    if (!invalid_value) {
        result = franken_cache_tiered_put_many_go(items, index, ttl);
    }

    for (uint32_t i = 0; i < index; i++) {
        if (owned_keys[i] != NULL) {
            zend_string_release(owned_keys[i]);
        }
    }
    efree(owned_keys);
    efree(items);

    if (invalid_value || result < 0) {
        if (!invalid_value) {
            franken_cache_throw_tiered_error();
        }
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_cache_tiered_set)
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

    int result = franken_cache_tiered_set_go(key, value, ttl);
    if (result < 0) {
        franken_cache_throw_tiered_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_cache_tiered_forever)
{
    zend_string *key;
    zend_string *value;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(key)
        Z_PARAM_STR(value)
    ZEND_PARSE_PARAMETERS_END();

    int result = franken_cache_tiered_forever_go(key, value);
    if (result < 0) {
        franken_cache_throw_tiered_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_cache_tiered_forget)
{
    zend_string *key;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STR(key)
    ZEND_PARSE_PARAMETERS_END();

    int result = franken_cache_tiered_forget_go(key);
    if (result < 0) {
        franken_cache_throw_tiered_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_cache_tiered_touch)
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

    int result = franken_cache_tiered_touch_go(key, ttl);
    if (result < 0) {
        franken_cache_throw_tiered_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_cache_tiered_flush)
{
    ZEND_PARSE_PARAMETERS_NONE();

    int result = franken_cache_tiered_flush_go();
    if (result < 0) {
        franken_cache_throw_tiered_error();
        RETURN_THROWS();
    }

    RETURN_BOOL(result);
}

PHP_FUNCTION(franken_cache_tiered_increment)
{
    zend_string *key;
    zend_long value;
    zend_long result = 0;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(key)
        Z_PARAM_LONG(value)
    ZEND_PARSE_PARAMETERS_END();

    int status = franken_cache_tiered_increment_go(key, value, &result);
    if (status < 0) {
        franken_cache_throw_tiered_error();
        RETURN_THROWS();
    }

    RETURN_LONG(result);
}

PHP_FUNCTION(franken_cache_tiered_decrement)
{
    zend_string *key;
    zend_long value;
    zend_long result = 0;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(key)
        Z_PARAM_LONG(value)
    ZEND_PARSE_PARAMETERS_END();

    int status = franken_cache_tiered_decrement_go(key, value, &result);
    if (status < 0) {
        franken_cache_throw_tiered_error();
        RETURN_THROWS();
    }

    RETURN_LONG(result);
}

static char franken_cache_version_storage[64];

void franken_cache_set_version(const char *version)
{
    size_t length = strlen(version);
    if (length >= sizeof(franken_cache_version_storage)) {
        length = sizeof(franken_cache_version_storage) - 1;
    }

    memcpy(franken_cache_version_storage, version, length);
    franken_cache_version_storage[length] = '\0';
    franken_cache_module_entry.version = franken_cache_version_storage;
}

zend_module_entry franken_cache_module_entry = {
    STANDARD_MODULE_HEADER,
    "franken_cache",
    franken_cache_functions,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    STANDARD_MODULE_PROPERTIES
};
