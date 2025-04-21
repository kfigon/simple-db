* [x] sql
* [x] inmemory
    * [x] storage manager
    * [x] schema
    * [x] basic sql interpretation
* [x] binary serialization, pages, linked lists
    * [x] just serialize the content, then recover. No live updates. No overflow pages
        * [x] serialize schema
        * [x] serialize data
        * [x] header
        * [x] dump/load in cli
    * [x] slot array
        * [x] serialization in pages, but just ONE page per type. Serialize everything in single page
    * [x] integrate slotted pages in rest of the metadata and catalog
        * [x] Storage - serialize db header page
        * [x] Storage - de/serialize schema in new format. How to format it on disk? Keep schema cached inmem for efficiency
        * [x] Storage - pageID allocations
        * [x] Storage - remove AllData, replace with page linked list read from catalog
        * [x] Storage - page iterator
        * [x] Storage - slot array cell iterator. Connect with page iterator
    * [x] directory page on disk, rebuild on startup
    * [x] schema page on disk, rebuild on startup
    * [x] add tuple to last page, do not create pages excessively
    * [x] generic serialization/deserialization
    * [ ] overflow pages
* [ ] tool for debugging data on disk
* [ ] indexes with btree on disk
* [ ] transactions, acid
* [ ] concurrency
* [ ] recovery
* [x] operators - Row abstraction might be replaced by just array of columns, to reduce memory
* [ ] join

fix a bug with > 2 logical operators in where
updates
LOG
transaction, recovery
indexes
mvcc