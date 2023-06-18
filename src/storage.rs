
#[derive(Debug)]
struct PageHeader {
    size: u32
}

#[derive(Debug)]
struct Page {
    head: PageHeader,
    data: Vec<u8>
}

struct StorageError(String);
struct PageId(usize);

// currently inmemory
// todo: mutex
struct StorageManager {
    data: Vec<Page>
}

impl StorageManager {
    fn new() -> Self {
        StorageManager { data: Vec::new() }
    }

    fn persist(&mut self, data: Vec<u8>) -> Result<PageId, StorageError> {
        todo!()
    }

    fn read(&mut self, id: PageId) -> Option<Page> {
        todo!()
    }
}

fn marshall<T: serde::ser::Serialize>(data: T) -> Result<Vec<u8>, String> {
    bincode::serialize(&data).map_err(|e| format!("error during serialization {e}"))
}

fn unmarshall<'a, T: serde::de::Deserialize<'a>,>(data: &'a Vec<u8>) -> Result<T,String> {
    bincode::deserialize(&data[..]).map_err(|e| format!("error during deserialization {e}"))
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
    #[ignore]
    fn foo() {
        todo!()
    }
}