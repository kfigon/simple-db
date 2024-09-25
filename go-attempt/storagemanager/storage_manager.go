package storagemanager

import (
	"bytes"
	"simple-db/page"
)

type StorageManager struct {
	Directory page.DirectoryPage
	SchemaPages []page.SchemaPage
	DataPages []page.DataPage

	Data bytes.Buffer
}

func NewStorageManager(root *page.RootPage) *StorageManager {
	// todo: init. build internal state of directory and schemas
	// from basic pages
	return nil
}

func (s *StorageManager) ReadPage(p page.PageId) []byte{
	// todo
	return nil
}

func (s *StorageManager) WritePage(p page.PageId, data []byte) error {
	// todo
	return nil
}