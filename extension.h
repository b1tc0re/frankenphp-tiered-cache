#ifndef FRANKEN_CACHE_EXTENSION_H
#define FRANKEN_CACHE_EXTENSION_H

#include <php.h>

extern zend_module_entry franken_cache_module_entry;

typedef struct {
    zend_string *key;
    zend_string *value;
} franken_cache_put_many_item;

typedef struct {
    zend_string *key;
    zend_string *value;
    int found;
} franken_cache_many_item;

#endif
