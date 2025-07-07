package naive

import "fmt"


func must[T any](v T, err error) T {
	debugAsserErr(err, "expected no error")
	return v
}

func debugAssert(expectTrue bool, format string, args ...any) {
	if assertionsEnabled && !expectTrue {
		panic(fmt.Sprintf(format, args...))
	}
}

func debugAsserErr(err error, format string, args ...any) {
	debugAssert(err == nil, format, args...)
}