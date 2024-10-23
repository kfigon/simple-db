# table of contents

* REPL for testing
* simple append only database
* storage manager
    * data format on disk - pages, tuples
    * metadata
    * OS interface
* Btree - fundamental data structure for databases
* data operators - inserts, updates, queries. Just method calls
* sql
    * lexer
    * parser
    * interpreter
* abstracting data operators for sql
* indexes - how to make things fast
* ACID
    * availability - skip
    * consistency - constraints
    * isolation
        * transactions
        * concurrency
    * durability - write ahead log, how to survive crash 


## additional notes
* initial prototype in high level language like Scala or Python - to speed the implementation and catch errors
* final simplified version in Go for teaching
* show closed door before we show the key
* show how to write automated tests
* provide acceptance tests for the solution for each chapter
* excercises/questions at the end
* build the project incrementally - each chapter should have new addition to the project
