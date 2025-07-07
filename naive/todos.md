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
* [ ] log
    * [ ] log structure investigation
    * [ ] implement log
    * [ ] use log for changes
    * [ ] integrate log in all writes
        * forward iteration - for crash recovery
        * backward iteration - for rollback
* [ ] updates
* [ ] indexes with btree on disk
* [ ] try to understand different storage layouts - index organized storage instead of heap file
* [ ] transactions, acid
* [ ] recovery
* [ ] concurrency, mvcc
* [ ] join
* [ ] order
* [ ] better update support - overflow pages, page garbage collection, dead tuples and dead cell cleanups 
* [x] operators - Row abstraction might be replaced by just array of columns, to reduce memory
* [ ] work through a book from E. Sciore. Edit: I have an issue with that book, I don't get all the explanations and code
* [ ] revisit again lectures from CMU. Think about order of implementation and metadata structure. Maybe focus on reading sqlite code or tony's sary mkdb?

* best video on pratt parser [link](https://www.youtube.com/watch?v=0c8b7YfsBKs), explains binding power instead of predecence
