//! Little-endian binary codec helpers shared by commands, descriptors and the wire
//! protocol.

use crate::{Error, Result};

#[derive(Debug, Default)]
pub struct W(pub Vec<u8>);

impl W {
    pub fn new() -> Self {
        Self(Vec::with_capacity(64))
    }
    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.0.push(v);
        self
    }
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn bytes(&mut self, b: &[u8]) -> &mut Self {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
        self
    }
    pub fn opt_bytes(&mut self, b: Option<&[u8]>) -> &mut Self {
        match b {
            None => self.u8(0),
            Some(b) => self.u8(1).bytes(b),
        }
    }
    pub fn finish(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Debug)]
pub struct R<'a>(pub &'a [u8]);

impl<'a> R<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.0.len() < n {
            return Err(Error::Codec("truncated".into()));
        }
        let (h, t) = self.0.split_at(n);
        self.0 = t;
        Ok(h)
    }
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn u32(&mut self) -> Result<u32> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }
    pub fn u64(&mut self) -> Result<u64> {
        let mut a = [0u8; 8];
        a.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(a))
    }
    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let n = usize::try_from(self.u32()?).map_err(|_| Error::Codec("len".into()))?;
        Ok(self.take(n)?.to_vec())
    }
    pub fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(match self.u8()? {
            0 => None,
            _ => Some(self.bytes()?),
        })
    }
    pub fn done(&self) -> Result<()> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(Error::Codec(format!("{} trailing bytes", self.0.len())))
        }
    }
}
