package naive

import (
	"iter"
)

type pageIterators struct {
	*Storage
}

func (p pageIterators) NewPageIterator(startingPage PageID) PageIterator {
	currentPageId := startingPage
	return func(yield func(PageID, *GenericPage) bool) {
		for currentPageId != 0 && int(currentPageId) < len(p.Storage.allPages){
			currentPage := p.Storage.allPages[currentPageId]
			if !yield(currentPageId, currentPage) {
				break
			}
			currentPageId = currentPage.Header.NextPage
		}
	}
}

type PageIterator iter.Seq2[PageID, *GenericPage]
func (p PageIterator) tuples() TupleIterator {
	return func(yield func([]byte) bool) {
		for _, thisPage := range p {
			for tuple := range thisPage.SlotArray.Iterator() {
				if !yield(tuple) {
					return
				}
			}
		}
	}
}

func (p pageIterators) directoryPages() PageIterator {
	return p.NewPageIterator(p.root.DirectoryPageStart)
}

type TupleIterator iter.Seq[[]byte]

func (p pageIterators) FindStartingPageForEntity(pageType PageType, name string) (PageID, bool) {
	for dir := range p.DirectoryEntriesIterator() {
		if dir.Name == name && dir.PageTyp == pageType {
			return dir.StartingPage, true
		}
	}
	return 0, false
}

func (p pageIterators) NewEntityIterator(pageType PageType, name string) TupleIterator {
	startId, _ := p.FindStartingPageForEntity(pageType, name)
	return p.NewPageIterator(startId).tuples()
}

func (p pageIterators) RowIterator(name string, schema []FieldName, schemaLookup map[FieldName]FieldType) RowIter {
	startId, _ := p.FindStartingPageForEntity(DataPageType, name)
	return func(yield func(Row) bool) {
		for tup := range p.NewPageIterator(startId).tuples() {
			row := parseToRow(tup, schema, schemaLookup)
			if !yield(row) {
				return
			}
		}
	}
}

func (p pageIterators) DirectoryEntriesIterator() iter.Seq[DirectoryTuple] {
	return func(yield func(DirectoryTuple) bool) {
		for d := range p.directoryPages().tuples() {
			dir := must(DeserializeDirectoryTuple(d))
			if !yield(*dir) {
				break
			}
		}
	}
}

func (p pageIterators) SchemaEntriesIterator(name string) iter.Seq[SchemaTuple] {
	return func(yield func(SchemaTuple) bool) {
		for d := range p.NewEntityIterator(SchemaPageType, name){
			sch := must(DeserializeSchemaTuple(d))
			if !yield(*sch) {
				break
			}
		}
	}
}
