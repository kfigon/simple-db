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
    * [ ] slot array
        * [x] serialization in pages, but just ONE page per type. Serialize everything in single page
        * [ ] integrate slotted pages in rest of the metadata and catalog
        * [ ] consider using reflection for serialization for more abstracted usage
        * [ ] overflow pages
* [ ] indexes
* [ ] transactions, acid
* [ ] concurrency
* [ ] recovery
* [ ] operators

