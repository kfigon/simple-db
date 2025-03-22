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
        * [x] Storage - serialize db header page
        * [ ] Storage - de/serialize schema in new format. How to format it on disk? Keep schema cached inmem for efficiency
        * [x] Storage - pageID allocations
        * [ ] Storage - remove AllData, replace with page linked list read from catalog
        * [x] Storage - page iterator
        * [x] Storage - slot array cell iterator. Connect with page iterator
    * [ ] directory page on disk, rebuild on startup
    * [ ] schema page on disk, rebuild on startup
    * [ ] use reflection for serialization for more abstracted usage
    * [ ] overflow pages
* [ ] indexes with btree on disk
* [ ] transactions, acid
* [ ] concurrency
* [ ] recovery
* [ ] operators

priorities - finish reflection based ser/deser
directory page
schema page