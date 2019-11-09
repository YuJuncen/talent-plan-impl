use std::io::{Error, Seek, SeekFrom};

pub(crate) trait SeekExt {
    fn current_position(&mut self) -> std::io::Result<usize>;
    fn seek_to(&mut self, n: usize) -> std::io::Result<usize>;
    fn seek_to_end(&mut self) -> std::io::Result<usize>;
    fn seek_to_start(&mut self) -> std::io::Result<usize>;
}

impl<R: Seek> SeekExt for R {
    fn current_position(&mut self) -> std::io::Result<usize> {
        self.seek(SeekFrom::Current(0))
            .map(|n| n as usize)
    }

    fn seek_to(&mut self, n: usize) -> std::io::Result<usize> {
        self.seek(SeekFrom::Start(n as u64))
            .map(|n| n as usize)
    }

    fn seek_to_end(&mut self) -> std::io::Result<usize> {
        self.seek(SeekFrom::End(0))
            .map(|n| n as usize)
    }

    fn seek_to_start(&mut self) -> std::io::Result<usize> {
        self.seek(SeekFrom::Start(0))
            .map(|n| n as usize)
    }
}