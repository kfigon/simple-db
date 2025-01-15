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
    * [ ] serialization in pages, but just ONE page per type. Serialize everything in single page
    * [ ] slot array for live updates
* [ ] indexes
* [ ] transactions, acid
* [ ] concurrency
* [ ] recovery
* [ ] operators

