package naive

type StorageEngine struct{}

func NewStorageEngine() *StorageEngine {
	return &StorageEngine{}
}

func (s *StorageEngine) AllocatePage(pageTyp PageType, name string) (PageID, *GenericPage) {
	// find last page of the type
	// connect to previous page
	// or allocate complete new page, mark as start
	// count number of pages
	return 0, nil
}

func (s *StorageEngine) AllocateOverflowPage(data []byte) PageID {
	// allocate pages to fit all data
	// count pages
	// return first page ID
	return 0
}

func (s *StorageEngine) ReadPage(p PageID) (*GenericPageHeader, []byte, bool) {
	return nil, nil, false
}

func (s *StorageEngine) persistPage(id PageID, pageData []byte) {
	// store in persistance medium (in mem now)
}

func (s *StorageEngine) AddTuple(pid PageID, t Tuple) error {
	// add tuple to the page id
	// check if the page type is fine
	return nil
}

func (s *StorageEngine) ReadPages(startingPageID PageID) PageIteratorCombined {
	// generic pager for all types, byte content. Add convenience methods to deserialize
	return nil
}
