package naive

// keep root as special, useful for better metadata handling
type RootzPagezzz struct{}

// generic page with slotted
type NewPageStruct struct {
	GenericPageHeader
	*Slotted
}

type OverflowPagezz struct{}

func NewPagezz() *NewPageStruct {
	return nil
}

func (n *NewPageStruct) Add(t Tuple) (SlotIdx, error) {
	// find place, add
	// return typed error when cant fit. Upper layer should then allocate new
	return 0, nil
}

func (n *NewPageStruct) Read(s SlotIdx) (Tuple, error) {
	// delegate read to slotted page
	return Tuple{}, nil
}

func (n *NewPageStruct) Put(s SlotIdx, t Tuple) error {
	// delegate to slotted
	// return typed error when cant fit. Upper layer should then allocate new
	return nil
}

func (n *NewPageStruct) Tuples() NewTupleIterator {
	return nil
}
