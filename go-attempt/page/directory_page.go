package page

type DirectoryPageHeader struct {
	PageType PageType
}

type DirectoryPage struct {
	Header DirectoryPageHeader
	Data []byte
}