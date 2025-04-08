package naive

import "iter"

type RowIter iter.Seq[Row] 
func Select(rows RowIter, predicate func(Row)bool) RowIter {
	return func(yield func(Row) bool) {
		for r := range rows {
			if !predicate(r){
				continue
			} else if !yield(r) {
				return
			}
		}
	}
}

func Project(rows RowIter, fields []FieldName) RowIter {
	return func(yield func(Row) bool) {
		for r := range rows {
			mapped := Row{}

			for _, f := range fields {
				if v, ok := r[f]; ok {
					mapped[f] = v
				}
			}
			if !yield(mapped) {
				return
			}
		}
	}
}

func Product(rows RowIter, rows2 RowIter) RowIter {
	return func(yield func(Row) bool) {
		for r1 := range rows {
			for r2 := range rows2 {
				newData := Row{}
				for k, v := range r1 {
					newData[k]=v
				}
				for k, v := range r2 {
					newData[k]=v
				}

				if !yield(newData) {
					return
				}
			}
		}
	}
}



