use std::{mem, collections::{HashMap, HashSet}};


#[derive(Debug, PartialEq)]
struct PageHeader {
    size: usize
}

#[derive(Debug, PartialEq)]
struct Page {
    head: PageHeader,
    data: Vec<u8>
}

impl Page {
    fn new(data: Vec<u8>) -> Self {
        Page { 
            head: PageHeader { size: data.len() },
            data: data 
        }
    }
}

#[derive(Debug, PartialEq)]
struct StorageError(String);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
struct PageId(usize);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
struct TableName(String);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, serde::Serialize, serde::Deserialize)]
struct FieldName(String);

// currently inmemory. Storage manager == buffer pool manager here
struct StorageManager {
    // todo: mutexes

    // todo: move to separate Pager class
    // ids: HashMap<usize, PageId> // todo - primary key to PageId
    data: Vec<Page>, // all pages, full disk
    
    // metadata, stored in special files
    schemas: HashMap<TableName, Vec<FieldName>>,
    page_directory: HashMap<TableName, HashSet<PageId>>,
}

impl StorageManager {
    fn new() -> Self {
        Self { 
            data: Vec::new(), 
            schemas: HashMap::new(), 
            page_directory: HashMap::new(), 
        }
    }

    fn store_new(&mut self, data: Vec<u8>) -> Result<PageId, StorageError> {
        self.data.push(Page::new(data));
        Ok(PageId(self.data.len()-1))
    }

    fn update(&mut self, page_id: PageId, data: Vec<u8>) -> Result<(), StorageError> {
        self.data.get(page_id.0).ok_or(StorageError(format!("cant find page of id {}", page_id.0)))?;
        _ = mem::replace(&mut self.data[page_id.0], Page::new(data));
        Ok(())
    }

    fn read(&mut self, id: PageId) -> Option<&Page> {
        self.data.get(id.0)
    }

    fn insert_data(&mut self, table_name: TableName, data: HashMap<FieldName, String>) -> Result<PageId, StorageError> {
        // todo: validate schema
        let page_id = self.store_new(marshall(data)?)?;
        let page_ids = self.page_directory.entry(table_name).or_insert(HashSet::default());
        page_ids.insert(page_id);
        
        Ok(page_id)
    }

    fn update_data(&mut self, id: PageId, data: HashMap<FieldName, String>) -> Result<PageId, StorageError> {
        // todo: validate schema

        self.update(id, marshall(data)?)?;
        Ok(id)
    }
}

fn marshall<T: serde::ser::Serialize>(data: T) -> Result<Vec<u8>, StorageError> {
    bincode::serialize(&data).map_err(|e: Box<bincode::ErrorKind>| StorageError(format!("error during serialization {e}")))
}

fn unmarshall<'a, T: serde::de::Deserialize<'a>,>(data: &'a Vec<u8>) -> Result<T, StorageError> {
    bincode::deserialize(&data[..]).map_err(|e: Box<bincode::ErrorKind>| StorageError(format!("error during deserialization {e}")))
}

#[cfg(test)]
mod marshalling_tests {
    use super::*;

    #[test]
    fn marshall_int() {
        let input = marshall(1234).unwrap();
        let out: i32 = unmarshall(&input).unwrap();
        assert_eq!(1234, out);
    }

    #[test]
    fn marshall_string() {
        let input = marshall("foobar").unwrap();
        let out: String = unmarshall(&input).unwrap();
        assert_eq!("foobar", &out);
    }

    #[derive(Eq, PartialEq, Debug, serde::Serialize, serde::Deserialize, Clone)]
    struct MyData {
        name: String,
        age: u32,
        d: Vec<u8>
    }
    #[test]
    fn marshall_struct() {
        let my_data = MyData{name: "foo".to_string(), age: 123, d: vec![1,2,3,4]};

        let input = marshall(my_data.clone()).unwrap();
        let out: MyData = unmarshall(&input).unwrap();
        assert_eq!(my_data, out);
    }
}


#[cfg(test)]
mod persistance_tests {
    use super::*;

    #[test]
    fn read_unknown() {
        let mut s = StorageManager::new();
        assert_eq!(s.read(PageId(12)), None)
    }

    #[test]
    fn insert_read() {
        let mut s = StorageManager::new();
        assert_eq!(s.read(PageId(0)), None);

        s.store_new(vec![1,2,3]).unwrap();
        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![1,2,3])));
    }

    #[test]
    fn modify() {
        let mut s = StorageManager::new();
        s.store_new(vec![1,2,3]).unwrap();
        s.store_new(vec![4,5,6]).unwrap();

        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![1,2,3])));
        assert_eq!(s.read(PageId(1)), Some(&Page::new(vec![4,5,6])));

        s.update(PageId(0), vec![87]).unwrap();
        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![87])));
        assert_eq!(s.read(PageId(1)), Some(&Page::new(vec![4,5,6])));
    }


    #[test]
    fn modify_unknown() {
        let mut s = StorageManager::new();
        s.store_new(vec![1,2,3]).unwrap();
        s.store_new(vec![4,5,6]).unwrap();

        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![1,2,3])));
        assert_eq!(s.read(PageId(1)), Some(&Page::new(vec![4,5,6])));

        assert!(s.update(PageId(2), vec![87]).is_err());
    }

    #[test]
    fn insert() {
        let data = HashMap::from([
            (FieldName("foo".to_string()), "1234".to_string()),
            (FieldName("bar".to_string()), "the value".to_string()),
        ]);
        let table_name = TableName("the_table".to_string());

        let mut s = StorageManager::new();
        let id = s.insert_data(table_name, data).unwrap();
        
        let p = s.read(id).unwrap();
        let read_data: HashMap<FieldName, String> = unmarshall(&p.data).unwrap();
        assert_eq!(read_data, HashMap::from([
            (FieldName("foo".to_string()), "1234".to_string()),
            (FieldName("bar".to_string()), "the value".to_string()),
        ]));
    }

    #[test]
    fn update() {
        let data = HashMap::from([
            (FieldName("foo".to_string()), "1234".to_string()),
            (FieldName("bar".to_string()), "the value".to_string()),
        ]);
        let table_name = TableName("the_table".to_string());

        let mut s = StorageManager::new();
        let id = s.insert_data(table_name, data).unwrap();
        
        s.update_data(id, HashMap::from([
            (FieldName("fooooo".to_string()), "1234".to_string()),
            (FieldName("barooo".to_string()), "the value".to_string()),
        ])).unwrap();

        let p = s.read(id).unwrap();
        let read_data: HashMap<FieldName, String> = unmarshall(&p.data).unwrap();
        assert_eq!(read_data, HashMap::from([
            (FieldName("fooooo".to_string()), "1234".to_string()),
            (FieldName("barooo".to_string()), "the value".to_string()),
        ]));
    }
}