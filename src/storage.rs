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

fn marshall<T>(data: T) -> Vec<u8> {
    todo!()
}

fn unmarshall<T>(data: Vec<u8>) -> T {
    todo!()
}

#[cfg(test)]
mod marshalling_tests {
    use super::*;

    #[test]
    fn marshall_int() {
        assert_eq!(marshall::<i32>(1234), vec![0x12,0x34,0,0])
    }

    #[test]
    fn unmarshall_int() {
        assert_eq!(unmarshall::<i32>(vec![0x12,0x34,0,0]), 1234)
    }

    #[test]
    #[ignore]
    fn marshall_string() {
        todo!()
    }

    #[test]
    #[ignore]
    fn unmarshall_string() {
        todo!()
    }

    #[test]
    #[ignore]
    fn marshall_struct() {
        todo!()
    }

    #[test]
    #[ignore]
    fn unmarshall_struct() {
        todo!()
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