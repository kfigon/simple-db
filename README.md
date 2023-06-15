# Simple relational database

based on [CMU course](https://15445.courses.cs.cmu.edu/fall2021/)

# Components
* query planning
* operator execution
* access methods
* buffer pool manager - we don't want to use memory mapped files and we don't want to use virtual memory - OS can interfere with us, we know better what's the context of these IOs. Buffer pool allows to maintain pages and flush them to the disk
* disk manager - IO to the disk