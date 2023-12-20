#[derive(Debug, PartialEq)]
pub struct StorageError(pub String);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct PageId(pub usize);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd)]
pub struct TableName(pub String);

#[derive(Debug, PartialEq, Eq, Hash, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct FieldName(pub String);

pub fn marshall<T: serde::ser::Serialize>(data: T) -> Result<Vec<u8>, StorageError> {
    bincode::serialize(&data).map_err(|e: Box<bincode::ErrorKind>| StorageError(format!("error during serialization {e}")))
}

pub fn unmarshall<'a, T: serde::de::Deserialize<'a>,>(data: &'a Vec<u8>) -> Result<T, StorageError> {
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
