# clone of sqlite

based on [chidb](http://chi.cs.uchicago.edu/chidb/index.html) from university of chicago

# db
single file - sequence of pages. Page is our storage unit, being a set of disk blocks. Different kind of pages - tables, indexes etc.

btree page == btree node. Each pointer is just a page ID

page internally is a slotted page - this way we can store variable size data in a page, inserts and sorting is easy. Slotted index grows to the end of the page, Cell data grows to the beginning. Page has a header to point what's inside

Overflow page - linked list of pages to store bigger data

table is set of pages
Row format - each Cell is just a row, sequence of bytes.


## btree

b - balanced
* b+ trees for table data
    *  like Btree in which each node contains only keys (not key–value pairs), and to which an additional level is added at the bottom with linked leaves
* btrees for indexes


real project start 17.09.24

# backlog
* [x] page design
* [ ] storage manager
    * [x] pages - separete data format from objects
    * [ ] linked list pages
    * [ ] page iterator
    * [x] slotted pages
    * [x] primitive de/serialization
    * [x] page serialization
    * [ ] page deserialization
    * [ ] catalog
    * [ ] schema
    * [ ] overflow pages
    * [x] storage manager to handle OS interaction
* [ ] btree
* [ ] sql
    * [x] lexer
    * [ ] parser
* [ ] query planner (ast evaluator)
    * [ ] vm or tree walking
* [ ] concurrency
* [ ] client-server
    * [ ] protocol
* [ ] go stdlib db interface



# materials
* [guy writing a db from scratch](https://www.youtube.com/watch?v=5Pc18ge9ohI)
* [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/)
* [CMU lectures](https://www.youtube.com/playlist?list=PLA5Lqm4uh9Bbq-E0ZnqTIa8LRaL77ica6)

btree:
* [wiki](https://en.wikipedia.org/wiki/B-tree)
* [baeldung](https://www.baeldung.com/cs/b-tree-data-structure)
* [nice btree explanation video](https://www.youtube.com/watch?v=SI6E4Ma2ddg)
* [btree vs b+tree discussion in databases](https://www.youtube.com/watch?v=UzHl2VzyZS4)
* [btree](https://ayende.com/blog/162945/b-trees-and-why-i-love-them-part-i)
* [btree in go](https://www.cloudcentric.dev/implementing-a-b-tree-in-go/) 
* [indexing talk](https://www.youtube.com/watch?v=HubezKbFL7E)
    * order of columns in index matters (The moment it encounters an inequality, the indexing stops right there)
    * functions == full table scan
    * inequality operators matters
    * add relevant columns even from select to index
