use std::io::{BufRead, Result as IoResult};
use regex::Regex;

#[derive(Debug, Clone)]
pub struct ChunkConfig {
    pub strategy: ChunkStrategy,
    pub max_chunk_lines: usize,
    pub max_chunk_size: usize,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            strategy: ChunkStrategy::Line,
            max_chunk_lines: 1000,
            max_chunk_size: 1024 * 1024, // 1MB
        }
    }
}

#[derive(Debug, Clone)]
pub enum ChunkStrategy {
    Line,
    FixedLines(usize),
    StartPattern(Regex),
    Delimiter(String),
}

#[derive(Debug)]
pub struct Chunk {
    pub content: String,
    pub line_count: usize,
    pub start_line: usize,
}

pub struct LineChunker {
    config: ChunkConfig,
    current_chunk: String,
    current_lines: usize,
    chunk_start_line: usize,
    global_line_number: usize,
}

impl LineChunker {
    pub fn new(config: ChunkConfig) -> Self {
        Self {
            config,
            current_chunk: String::new(),
            current_lines: 0,
            chunk_start_line: 1,
            global_line_number: 0,
        }
    }

    pub fn add_line(&mut self, line: String) -> Option<Chunk> {
        self.global_line_number += 1;

        match &self.config.strategy {
            ChunkStrategy::Line => {
                Some(Chunk {
                    content: line,
                    line_count: 1,
                    start_line: self.global_line_number,
                })
            }
            
            ChunkStrategy::FixedLines(size) => {
                let size = *size; // Copy the value to avoid borrow conflicts
                self.add_line_to_chunk(&line);
                
                if self.current_lines >= size || self.exceeds_safety_limits() {
                    Some(self.emit_current_chunk())
                } else {
                    None
                }
            }
            
            ChunkStrategy::StartPattern(regex) => {
                if regex.is_match(&line) && !self.current_chunk.is_empty() {
                    let chunk = self.emit_current_chunk();
                    self.add_line_to_chunk(&line);
                    Some(chunk)
                } else {
                    self.add_line_to_chunk(&line);
                    if self.exceeds_safety_limits() {
                        Some(self.emit_current_chunk())
                    } else {
                        None
                    }
                }
            }
            
            ChunkStrategy::Delimiter(delimiter) => {
                if line.trim() == delimiter.trim() {
                    if !self.current_chunk.is_empty() {
                        Some(self.emit_current_chunk())
                    } else {
                        None
                    }
                } else {
                    self.add_line_to_chunk(&line);
                    if self.exceeds_safety_limits() {
                        Some(self.emit_current_chunk())
                    } else {
                        None
                    }
                }
            }
        }
    }

    pub fn flush(&mut self) -> Option<Chunk> {
        if !self.current_chunk.is_empty() {
            Some(self.emit_current_chunk())
        } else {
            None
        }
    }

    fn add_line_to_chunk(&mut self, line: &str) {
        if !self.current_chunk.is_empty() {
            self.current_chunk.push('\n');
        }
        self.current_chunk.push_str(line);
        self.current_lines += 1;
    }

    fn emit_current_chunk(&mut self) -> Chunk {
        let chunk = Chunk {
            content: std::mem::take(&mut self.current_chunk),
            line_count: self.current_lines,
            start_line: self.chunk_start_line,
        };

        self.current_lines = 0;
        self.chunk_start_line = self.global_line_number + 1;
        
        chunk
    }

    fn exceeds_safety_limits(&self) -> bool {
        self.current_lines >= self.config.max_chunk_lines 
            || self.current_chunk.len() >= self.config.max_chunk_size
    }
}

pub fn chunk_lines<R: BufRead>(
    reader: R,
    config: ChunkConfig,
) -> IoResult<Vec<String>> {
    let mut chunker = LineChunker::new(config);
    let mut chunks = Vec::new();

    for line_result in reader.lines() {
        let line = line_result?;
        if let Some(chunk) = chunker.add_line(line) {
            chunks.push(chunk.content);
        }
    }

    if let Some(final_chunk) = chunker.flush() {
        chunks.push(final_chunk.content);
    }

    Ok(chunks)
}

pub fn parse_chunk_strategy(strategy_spec: &str) -> Result<ChunkStrategy, String> {
    match strategy_spec {
        "line" => Ok(ChunkStrategy::Line),
        
        s if s.starts_with("lines:") => {
            let count = s[6..].parse::<usize>()
                .map_err(|_| format!("Invalid line count: {}", &s[6..]))?;
            Ok(ChunkStrategy::FixedLines(count))
        }
        
        s if s.starts_with("start-pattern:") => {
            let regex = Regex::new(&s[14..])
                .map_err(|e| format!("Invalid start pattern regex: {}", e))?;
            Ok(ChunkStrategy::StartPattern(regex))
        }
        
        s if s.starts_with("delimiter:") => {
            Ok(ChunkStrategy::Delimiter(s[10..].to_string()))
        }
        
        unknown => Err(format!("Unknown chunk strategy: {}", unknown)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_line_strategy() {
        let input = "line1\nline2\nline3";
        let config = ChunkConfig {
            strategy: ChunkStrategy::Line,
            ..Default::default()
        };

        let chunks = chunk_lines(Cursor::new(input), config).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], "line1");
        assert_eq!(chunks[1], "line2");
        assert_eq!(chunks[2], "line3");
    }

    #[test]
    fn test_fixed_lines_strategy() {
        let input = "line1\nline2\nline3\nline4\nline5";
        let config = ChunkConfig {
            strategy: ChunkStrategy::FixedLines(2),
            ..Default::default()
        };

        let chunks = chunk_lines(Cursor::new(input), config).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], "line1\nline2");
        assert_eq!(chunks[1], "line3\nline4");
        assert_eq!(chunks[2], "line5");
    }

    #[test]
    fn test_start_pattern_strategy() {
        let input = "2024-01-01 Start\nContinuation\n2024-01-02 Another\nMore data";
        let config = ChunkConfig {
            strategy: ChunkStrategy::StartPattern(
                Regex::new(r"^\d{4}-\d{2}-\d{2}").unwrap()
            ),
            ..Default::default()
        };

        let chunks = chunk_lines(Cursor::new(input), config).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], "2024-01-01 Start\nContinuation");
        assert_eq!(chunks[1], "2024-01-02 Another\nMore data");
    }

    #[test]
    fn test_delimiter_strategy() {
        let input = "section1\ndata1\n---\nsection2\ndata2\n---\nsection3";
        let config = ChunkConfig {
            strategy: ChunkStrategy::Delimiter("---".to_string()),
            ..Default::default()
        };

        let chunks = chunk_lines(Cursor::new(input), config).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], "section1\ndata1");
        assert_eq!(chunks[1], "section2\ndata2");
        assert_eq!(chunks[2], "section3");
    }

    #[test]
    fn test_safety_limits() {
        let input = "line1\nline2\nline3\nline4\nline5";
        let config = ChunkConfig {
            strategy: ChunkStrategy::FixedLines(10), // Won't trigger
            max_chunk_lines: 2, // This will trigger
            ..Default::default()
        };

        let chunks = chunk_lines(Cursor::new(input), config).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0], "line1\nline2");
        assert_eq!(chunks[1], "line3\nline4");
        assert_eq!(chunks[2], "line5");
    }
}