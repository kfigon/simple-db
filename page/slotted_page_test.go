package page

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestSlotEntryGuard(t *testing.T) {
	var p PageOffset
	assert.Equal(t, slotEntrySize, int(unsafe.Sizeof(p)), "slot entry size expected to be given size. Adjust the constant")
}
