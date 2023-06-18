use core::slice;
use std::{mem, io::Read};

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

fn marshall<T: Sized>(data: T) -> Vec<u8> {
    let d = unsafe {
        ::core::slice::from_raw_parts(
            (&data as *const T) as *const u8,
            ::core::mem::size_of::<T>(),
        )
    };
    Vec::<u8>::from(d)
}

fn unmarshall<T>(data: Vec<u8>) -> Result<T,String> {
    let mut out: T = unsafe { mem::zeroed() };

    let struct_size = mem::size_of::<T>();
    unsafe {
        let slice = slice::from_raw_parts_mut(&mut out as *mut _ as *mut u8, struct_size);
        data.as_slice().read_exact(slice).map_err(|e| format!("Error during unmarshalling: {e}"))?;
    }

    Ok(out)
}

#[cfg(test)]
mod marshalling_tests {
    use super::*;

    #[test]
    fn marshall_int() {
        assert_eq!(marshall::<u8>(12), vec![12]);
        assert_eq!(marshall::<i32>(1234), vec![0xd2,0x4,0,0]);
        assert_eq!(marshall::<i32>(-1234), vec![0x2e,0xfb, 0xff,0xff]);
    }

    #[test]
    fn unmarshall_int() {
        assert_eq!(unmarshall::<u8>(vec![12]), Ok(12));
        assert_eq!(unmarshall::<i32>(vec![0xd2,0x4,0,0]), Ok(1234));
        assert_eq!(unmarshall::<i32>(vec![0x2e,0xfb, 0xff,0xff]), Ok(-1234));
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