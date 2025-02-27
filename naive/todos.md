* [x] sql
* [x] inmemory
    * [x] storage manager
    * [x] schema
    * [x] basic sql interpretation
* [ ] binary serialization, pages, linked lists
    * [x] just serialize the content, then recover. No live updates. No overflow pages
        * [x] serialize schema
        * [x] serialize data
        * [x] header
        * [x] dump/load in cli
    * [x] slot array
        * [x] serialization in pages, but just ONE page per type. Serialize everything in single page
    * [ ] integrate slotted pages in rest of the metadata and catalog
        * [ ] Storage - pageID allocations
        * [x] Storage - serialize db header page
        * [ ] Storage - de/serialize schema in new format. Keep schema cached inmem for efficiency
        * [ ] Storage - remove AllData, replace with page linked list read from catalog
        * [ ] Storage - rework operators. Impl seq scans for select with iterator
    * [ ] consider using reflection for serialization for more abstracted usage
    * [ ] overflow pages
* [ ] indexes with btree on disk
* [ ] transactions, acid
* [ ] concurrency
* [ ] recovery
* [ ] operators

