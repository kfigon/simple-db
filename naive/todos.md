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
    * [x] serialization simplification, cleanup ser de lib
    * [x] fix all byte reads to use reader interface, not read all
    * [x] rework storage to hold raw bytes, not generic page structs
        * [x] abstract all reads
        * [x] funnel all writes and updates to single method
        * [x] serialize and persist changes to each pages
        * [x] serialization and deserialization of the db
    * [ ] introduce ExecutionEngine class to provide high level api
    * [ ] overflow pages
    * [ ] nil column support
    * [ ] ditch directory pages. Mimic sqlite tuple format and schema:
        * no directory page type
        * just the schema page:
            * page type | name | starting page | sql statement 
            * parse that sql statement on boot and use as schema
            * tuple layout:
            | len() | content|
                    | num of fields, col 0, col 1... col N | data 0, data 1 ... data N|
            * types: 0 - null, 1 bool, 2 int, 3 string, 4 blob (these 2 has len+content), 5 overflow string, 6 overflow blob (data: len+starting PageID of rest)

* [ ] indexes with btree on disk
* [x] read sqlite code and docs, how it works and get inspired
    * [arch](https://www.sqlite.org/arch.html)
    * [format](https://www.sqlite.org/fileformat2.html)
* [ ] log
    * [ ] should a log be separate file? Should we follow page layout?
    * [ ] log structure investigation (sql vs physical changes - old val, new val, rowid + offset)
    * [ ] implement log
    * [ ] use log for changes
    * [ ] integrate log in all writes
        * forward iteration - for crash recovery
        * backward iteration - for rollback
* [ ] transactions, acid
* [ ] recovery

* [ ] cleanup code 
    * [x] separate iterators and access methods.
    * [ ] move some stuff to exection engine
    * [x] schema outside of directory
    * [x] schema pages that can store different tables
* [ ] tool for debugging data on disk
* [ ] better update support - overflow pages, page garbage collection, dead tuples and dead cell cleanups 
* [x] try to understand different storage layouts - page storage (heap file, tree), page layout (log structured, tuple oriented - slotted pages, index organized storage). Storage models - row, column, mix
* [ ] tuple header with types, like sqlite does
    * [ ] overflow pages for data bigger than > 2kb
    * [ ] null columns
    * [ ] semi-self contained
* [ ] join
* [ ] order
* [ ] group by
* [ ] updates
* [ ] concurrency, mvcc
* [ ] pesistence or persistence abstraction

* [x] operators - Row abstraction might be replaced by just array of columns, to reduce memory
* [ ] ~~work through a book from E. Sciore. Edit: I have an issue with that book, I don't get all the explanations and code~~
* [x] revisit again lectures from CMU. Think about order of implementation and metadata structure. Maybe focus on reading sqlite code or tony's sary mkdb?

* best video on pratt parser [link](https://www.youtube.com/watch?v=0c8b7YfsBKs), explains binding power instead of predecence