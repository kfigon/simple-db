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

func TestAccess(t *testing.T) {
	t.SkipNow()
	page := NewEmptySlottedPage(0)
	id := page.AppendCell([]byte{1,2,3,4,5})
	data, err := page.ReadCell(id)
	
	assert.NoError(t, err)
	assert.Equal(t, []byte{1,2,3,4,5}, data)
}