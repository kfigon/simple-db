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
		for currentPageId != 0 && int(currentPageId) < int(p.root.NumberOfPages) {
			currentPage := p.Storage.getPage(currentPageId)
			if !yield(currentPageId, currentPage) {
				break
			}
			currentPageId = currentPage.Header.NextPage
		}
	}
}

func (p pageIterators) AllPages(startingPage PageID) PageIterator {
	return func(yield func(PageID, *GenericPage) bool) {
		for currentPageId := startingPage; int32(currentPageId) < p.root.NumberOfPages; currentPageId++ {
			currentPage := p.Storage.getPage(currentPageId)
			if !yield(currentPageId, currentPage) {
				break
			}
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

func (p pageIterators) schemaPages() PageIterator {
	return p.NewPageIterator(p.root.SchemaPageStart)
}

type TupleIterator iter.Seq[[]byte]

func (p pageIterators) FindStartingPageForEntity(pageType PageType, name string) (PageID, bool) {
	for sch := range p.SchemaEntriesIterator() {
		if sch.Name == name && sch.PageTyp == pageType {
			return sch.StartingPageID, true
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

func (p pageIterators) SchemaEntriesIterator() iter.Seq[SchemaTuple2] {
	return func(yield func(SchemaTuple2) bool) {
		for s := range p.schemaPages().tuples() {
			sch := must(DeserializeSchemaTuple(s))
			if !yield(*sch) {
				break
			}
		}
	}
}
