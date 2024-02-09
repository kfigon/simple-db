# clone of sqlite

based on [chidb](http://chi.cs.uchicago.edu/chidb/index.html) from university of chicago

* b+ trees for table data
    *  like Btree in which each node contains only keys (not key–value pairs), and to which an additional level is added at the bottom with linked leaves
* btrees for indexes

## btree

b - balanced

* [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/)
* [YT playlist](https://www.youtube.com/playlist?list=PLA5Lqm4uh9Bbq-E0ZnqTIa8LRaL77ica6)
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