package frankentiered

/*
#include "extension.h"
*/
import "C"

import (
	"unsafe"

	"github.com/dunglas/frankenphp"
)

func init() {
	frankenphp.RegisterExtension(unsafe.Pointer(&C.franken_tiered_module_entry))
}
