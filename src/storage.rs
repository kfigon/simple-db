use std::{mem, collections::{HashMap, HashSet}};

use crate::utils::{marshall, unmarshall, StorageError, PageId, TableName, FieldName};

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


// currently inmemory. Storage manager == buffer pool manager here
// we also don't care about slotted array
struct Pager {
    data: Vec<Page>, // all pages, full disk
    page_size: usize
}

impl Pager {
    fn new(page_size: usize) -> Self {
        Self { 
            data: Vec::new(), 
            page_size: page_size
        }
    }

    fn store_new(&mut self, mut data: Vec<u8>) -> Result<PageId, StorageError> {
        let last_page_id = self.data.last_mut();
        match last_page_id {
            None => {
                let new_page = Page::new(data);
                self.data.push(new_page);
            },
            Some(p) => {
                if p.head.size >= self.page_size {
                    let new_page = Page::new(data);
                    self.data.push(new_page);
                } else {
                    p.data.append(&mut data);
                    p.head.size = p.data.len();
                }
            }
        }
        Ok(PageId(self.data.len()-1))
    }

    fn update(&mut self, page_id: PageId, data: Vec<u8>) -> Result<(), StorageError> {
        let page = self.data.get_mut(page_id.0).ok_or(StorageError(format!("cant find page of id {}", page_id.0)))?;
        page.data = data;
        page.head.size = page.data.len();
        
        Ok(())
    }

    fn read(&self, id: PageId) -> Option<&Page> {
        self.data.get(id.0)
    }
}


#[cfg(test)]
mod persistance_tests {
    use super::*;

    #[test]
    fn read_unknown() {
        let mut s = Pager::new(5);
        assert_eq!(s.read(PageId(12)), None)
    }

    #[test]
    fn insert_read() {
        let mut s = Pager::new(512);
        assert_eq!(s.read(PageId(0)), None);

        s.store_new(vec![1,2,3]).unwrap();
        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![1,2,3])));

        s.store_new(vec![4,5,6]).unwrap();
        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![1,2,3,4,5,6])));
    }

    #[test]
    fn modify() {
        let mut s = Pager::new(3);
        s.store_new(vec![1,2,3]).unwrap();
        s.store_new(vec![4,5,6]).unwrap();

        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![1,2,3])));
        assert_eq!(s.read(PageId(1)), Some(&Page::new(vec![4,5,6])));

        s.update(PageId(0), vec![87]).unwrap();
        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![87])));
        assert_eq!(s.read(PageId(1)), Some(&Page::new(vec![4,5,6])));
    }

    #[test]
    fn modify_same_page() {
        let mut s = Pager::new(512);
        s.store_new(vec![1,2,3]).unwrap();
        s.store_new(vec![4,5,6]).unwrap();

        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![1,2,3,4,5,6])));

        s.update(PageId(0), vec![87]).unwrap();
        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![87])));
    }

    #[test]
    fn modify_unknown() {
        let mut s = Pager::new(3);
        s.store_new(vec![1,2,3]).unwrap();
        s.store_new(vec![4,5,6]).unwrap();

        assert_eq!(s.read(PageId(0)), Some(&Page::new(vec![1,2,3])));
        assert_eq!(s.read(PageId(1)), Some(&Page::new(vec![4,5,6])));

        assert!(s.update(PageId(2), vec![87]).is_err());
    }
}

struct StorageManager {
    pager: Pager,

    // ids: HashMap<usize, PageId> // todo, dont use pageIds in this layer - primary key to PageId
    page_directory: HashMap<TableName, HashSet<PageId>>,
    schemas: HashMap<TableName, FieldName>
}

impl StorageManager {
    fn new() -> Self{
        Self { pager: Pager::new(512), page_directory: HashMap::new(), schemas: HashMap::new() }
    }

    fn insert_data(&mut self, table_name: TableName, data: HashMap<FieldName, String>) -> Result<PageId, StorageError> {
        // todo: validate schema
        let page_id = self.pager.store_new(marshall(data)?)?;
        let page_ids = self.page_directory.entry(table_name).or_insert(HashSet::default());
        page_ids.insert(page_id);
        
        Ok(page_id)
    }

    // todo: remove pages from this layer
    fn update_data(&mut self, id: PageId, data: HashMap<FieldName, String>) -> Result<PageId, StorageError> {
        // todo: validate schema

        self.pager.update(id, marshall(data)?)?;
        Ok(id)
    }

    // todo: remove, don't use pages here
    fn read(&self, id: PageId) -> Option<&Page> {
        self.pager.read(id)
    }
}

#[cfg(test)]
mod storage_test {
    use super::*;

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