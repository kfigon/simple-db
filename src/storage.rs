use std::mem;


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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
struct PageId(usize);

// currently inmemory. Storage manager == buffer pool manager here
// todo: mutex
struct StorageManager {
    data: Vec<Page>
}

impl StorageManager {
    fn new() -> Self {
        StorageManager { data: Vec::new() }
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
}