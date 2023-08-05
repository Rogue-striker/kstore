use std::{fs::{File, OpenOptions}, collections::HashMap, io::{self, BufReader, SeekFrom, Seek, Read, BufWriter, Write}, path::Path };
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use serde_derive::{Serialize, Deserialize};
use crc::Crc;

type ByteString = Vec<u8>;
type ByteStr = [u8];

#[derive(Serialize, Deserialize, Debug)]
pub struct KeyValuePair {
    key: ByteString,
    value: ByteString
}

#[derive(Debug)]
pub struct ActionKv {
    f: File,
    pub index: HashMap<ByteString, u64>
}

impl ActionKv {

    pub fn open(path: &Path) -> io::Result<Self>  {
        let f = OpenOptions::new().read(true).write(true).append(true).open(path)?;

        Ok(ActionKv { f: f, index: HashMap::new() })
    }

    pub fn load(&mut self) ->io::Result<()> {
        let mut f = BufReader::new(&mut self.f);
         
        loop {
            let current_position = f.seek(SeekFrom::Current(0))?;

            let maybe_kv = ActionKv::process_record(&mut f);

            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            break;
                        }
                        _ => return Err(err),
                    }
                },
            };
            self.index.insert(kv.key, current_position);
        }
        Ok(())
    }   

    fn process_record<R: Read>(f: &mut R) -> io::Result<KeyValuePair>{
        let saved_checksum = f.read_u32::<LittleEndian>()?;
        let key_len = f.read_u32::<LittleEndian>()?;
        let value_len = f.read_u32::<LittleEndian>()?;
        let data_len = key_len + value_len;
        let mut data = ByteString::with_capacity(value_len as usize);
        {
            f.by_ref().take(data_len as u64).read_to_end(&mut data);
        }
        debug_assert_eq!(data.len(), data_len as usize);

        let crc32 = Crc::<u32>::new(&crc::CRC_32_CKSUM);
        let checksum = crc32.checksum(&data);
        
        if checksum != saved_checksum {
            panic!("data corruption current checksum {:08x} != {:08x} saved_checksum", checksum, saved_checksum)
        }

        let value = data.split_off(key_len as usize);
        let key = data;

        Ok(KeyValuePair { key, value })
    }

    pub fn seek_to_end(&mut self) -> io::Result<u64> {
        self.f.seek(SeekFrom::End(0))
    }

    pub fn get(&mut self, key : &ByteStr) -> io::Result<Option<ByteString>> {
        let position = match self.index.get(key) {
            None => return Ok(None),
            Some(position) => *position,
        };

        let kv = self.get_at(position)?;

        Ok(Some(ByteString::from(kv.value)))
    }

    pub fn get_at(&mut self, position: u64) -> io::Result<KeyValuePair> {
        let mut f = BufReader::new(&mut self.f);
        f.seek(SeekFrom::Start(position))?;

        let kv = ActionKv::process_record(&mut f)?;

        Ok(kv)
    }

    pub fn find(&mut self, target: &ByteStr) -> io::Result<Option<(u64, ByteString)>> {
        let mut f = BufReader::new(&mut self.f);

        let mut found: Option<(u64, ByteString)> = None;
        
        loop {
            let position = f.seek(SeekFrom::Current(0))?;

            let maybe_kv = ActionKv::process_record(&mut f);
            let kv = match maybe_kv {
                Ok(kv) => kv,
                Err(err) => {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            break;
                        },
                        _ => return Err(err),
                    }
                },
            };
            if kv.key == target {
                found = Some((position, kv.value));
            }
        }

        Ok(found)
    }

    pub fn insert(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        let position = self.insert_but_ignore_index(key, value)?;
        self.index.insert(key.to_vec(), position);

        Ok(())
    }

    pub fn insert_but_ignore_index(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<u64>{
        let mut f = BufWriter::new(&mut self.f);

        let key_len = key.len();
        let value_len = value.len();

        let mut temp = ByteString::with_capacity(key_len + value_len);

        for byte in key {
            temp.push(*byte);
        }

        for byte in value {
            temp.push(*byte);
        }

        let crc32 = Crc::<u32>::new(&crc::CRC_32_CKSUM);
        let checksum = crc32.checksum(&temp);

        let next_byte = SeekFrom::End(0);

        let current_position = f.seek(SeekFrom::Current(0))?;

        f.seek(next_byte);
        f.write_u32::<LittleEndian>(checksum)?;
        f.write_u32::<LittleEndian>(key_len as u32);
        f.write_u32::<LittleEndian>(value_len as u32);
        f.write_all(&mut temp)?;

        Ok(current_position)
    }

    pub fn update(&mut self, key: &ByteStr, value: &ByteStr) -> io::Result<()> {
        self.insert(key,value)
    }

    pub fn delete(&mut self, key: &ByteStr) -> io::Result<()> {
        self.insert(key, b"")
    }

}
