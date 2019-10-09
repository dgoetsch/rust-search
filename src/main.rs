extern crate clap;
extern crate threadpool;
extern crate memmap;

use clap::{Arg, App, SubCommand};
use std::fs::{self, File};
use std::sync::mpsc::{Sender, Receiver, SendError};
use std::sync::mpsc;
use std::thread;
use std::io;
use std::io::prelude::*;
use std::path::PathBuf;
use self::memmap::Mmap;
use self::threadpool::ThreadPool;
use std::time::{Duration, SystemTime};
use std::io::{Write, Stdout};

#[macro_use]
extern crate log;
extern crate simplelog;

use simplelog::*;


fn main() {
    let matches = App::new("search")
        .version("0.0.1")
        .author("Devyn Goetsch")
        .about("reads stuff")
        .arg(Arg::with_name("path")
            .help("path to search")
            .required(true)
            .index(1))
        .arg(Arg::with_name("query")
            .help("string to query for")
            .required(true)
            .index(2))
        .arg(Arg::with_name("debug_file")
            .long("debug_file")
            .help("debug log file, none if absent")
            .takes_value(true))
        .arg(Arg::with_name("log_level")
            .long("log_level")
            .help("log level to print to standaord out, off if absent")
            .takes_value(true))
        .get_matches();
    let (tx, rx): (Sender<(PathBuf, String)>, Receiver<(PathBuf, String)>) = mpsc::channel();
    let (result_sender, result_receiver): (Sender<SearchResult>, Receiver<SearchResult>) = mpsc::channel();
    let num_workers = 16;

    let search = Search::new(tx, result_sender, num_workers);
   
    init_logger(matches.value_of("log_level"), matches.value_of("debug_file"));

    matches.value_of("path")
        .and_then(|path| matches.value_of("query").map(|query| Ok((PathBuf::from(path), query.to_string()))))
        .unwrap_or( Err(AppError::Startup("Missing Required Params".to_string())))
        .and_then(|(path, query)| search.search(path, query))
        .map(|()| Search::process_queries(search, rx, result_receiver))
        .err().iter()
        .for_each(|err| {
            error!("Unrecoverable error: {:?}", err);
            std::process::exit(1);
        })
}

fn init_logger(log_level: Option<&str>, debug_file: Option<&str>) -> Result<(), AppError> {
    let log_level = log_level.map(|log_level| match log_level.to_lowercase().as_str() {
            "trace" => LevelFilter::Trace,
            "debug" => LevelFilter::Debug,
            "info" => LevelFilter::Info,
            "warn" => LevelFilter::Warn,
            "error" => LevelFilter::Error,
            _ => LevelFilter::Off
        })
        .unwrap_or(LevelFilter::Off);

    let mut loggers:  Vec<Box<SharedLogger>> = vec![ WriteLogger::new(log_level, Config::default(), std::io::stderr())];

    debug_file.into_iter()
        .for_each(|file_name| {
            File::create(file_name)
                .or_else(|err| File::open(file_name))
                .map(|file|
                    loggers.push(WriteLogger::new(LevelFilter::Debug, Config::default(), file)));
        });
            
    CombinedLogger::init(loggers)
        .map_err(|err| { AppError::Startup(format!("Could not initialize logger because {:?}", err)) })
}

#[derive(Debug, Clone)]
enum AppError {
    Startup(String),
    FileIO(String),
    Send(String),
    Aggregate(Vec<AppError>)
}

struct Search {
    tx: Sender<(PathBuf, String)>,
    result_sender: Sender<SearchResult>,
    thread_pool: ThreadPool,
}

#[derive(Debug, Clone)]
enum SearchResult {
    Contents(String, usize),
    File(String),
    Dir(String),
    Error(AppError, (PathBuf, String))
}


fn lift<T>(results: Vec<Result<T, AppError>>) -> Result<Vec<T>, AppError> {
    let (ok, err): (Vec<Result<T, AppError>>, Vec<Result<T, AppError>>) = results
        .into_iter()
        .partition(|r| r.is_ok());
    if err.is_empty() {
        Ok(ok.into_iter().flat_map(|r| r.ok()).collect::<Vec<T>>())
    } else {
        Err(AppError::Aggregate(err.into_iter().flat_map(|r| r.err()).collect::<Vec<AppError>>()))
    }
}


impl Search {
    fn new(tx: Sender<(PathBuf, String)>, result_sender: Sender<SearchResult>, num_workers: usize) -> Search {
        Search{
            tx: tx, 
            result_sender: result_sender, 
            thread_pool: ThreadPool::new(num_workers),
        }
    }

    fn process_queries(search: Search, rx: Receiver<(PathBuf, String)>, result_receiver: Receiver<SearchResult>) {
        let query_thread = thread::spawn(move || {
            let mut next_query = rx.try_recv();
            while next_query.is_ok() {
                next_query
                    .map(|(path, query)| search.search(path, query));
                next_query = rx.try_recv();
            }

            search.thread_pool.join();
        });

        let result_thread = thread::spawn(move || {
            let mut writer = std::io::stdout();
            let mut next_result = result_receiver.recv();
            while next_result.is_ok(){
                next_result.map(|search_result| {
                    match search_result {
                        SearchResult::Contents(path, pos) => writer.write_fmt(format_args!("{}::{}\n", path, pos)),
                        SearchResult::File(path) => writer.write_fmt(format_args!("{}\n", path)),
                        SearchResult::Dir(path) => writer.write_fmt(format_args!("{}\n", path)),
                        SearchResult::Error(error, (path, query)) => {
                            error!("Error while searching {:?} for {}: {:?}", path, query, error);
                            Ok(())
                        }  
                    }
                })
                .err().into_iter()
                .for_each(|err| error!("Error while reporting result: {:?}", err));
                next_result = result_receiver.recv();
            }
            writer.flush();
        });

        
        debug!("waiting for queries");
        query_thread.join();
        debug!("waiting for results");
        result_thread.join();
    }

    fn search(&self, path: PathBuf, query: String) -> Result<(), AppError> {
        self.matadata(path.clone())
            .map(|meta| meta.file_type().is_dir())
            .and_then(|is_dir| match is_dir {
                true => self.search_dir(path.clone(), query.clone()),
                false => self.search_file(path.clone(), query.clone())
            })
    }

    fn matadata(&self, path: PathBuf) -> Result<fs::Metadata, AppError> {
        fs::metadata(path)
            .map_err(|e| AppError::FileIO(e.to_string()))
    }

    fn search_dir(&self, path: PathBuf, query: String) -> Result<(), AppError> {
        path.clone().to_str()
            .filter(|p| p.to_string().ends_with(query.as_str()))
            .map(|p| self.result_sender.send(SearchResult::Dir(p.to_string())));

        fs::read_dir(path.clone())
            .map_err(|e| AppError::FileIO(e.to_string()))
            .map(|entries|
                entries
                    .into_iter()
                    .map(|r| { r
                        .map_err(|e| AppError::FileIO(e.to_string()))
                        .and_then(|entry| { self.tx.send((entry.path(), query.clone())).map_err(|e| AppError::Send(e.to_string()) )})
                    })
                    .collect::<Vec<Result<(), AppError>>>())
            .and_then(lift)
            .map(|_| ())
    }

    

    fn search_file(&self, path: PathBuf, query: String) -> Result<(), AppError> {
        let query_clone = query.clone();
        let query_bytes = query.clone().into_bytes();
        let sender = self.result_sender.clone();
        let path_clone = path.clone();

        self.thread_pool.execute(move || {
             let result = fs::File::open(path_clone.clone())
                .and_then(|f|  unsafe { Mmap::map(&f) })
                .map(|mem_map| {mem_map
                    .iter()
                    .fold((0, 0), |(num_matched, pos), file_byte| {
                        let mut match_count = num_matched;
                        if match_count >= query_bytes.len() && match_count > 0 {
                            let result = SearchResult::Contents(path_clone.to_str().unwrap_or("").to_string(), pos);
                            sender.send(result.clone())
                                .or_else(|send_err| sender.send(SearchResult::Error(AppError::Send(send_err.to_string()), (path_clone.clone(), query_clone.clone()))))
                                .err().iter()
                                .for_each(|err| error!("Could not handle {:?} because {:?}", result, err));
                            match_count = 0;
                        }
                        
                        (
                            query_bytes
                                .get(match_count)
                                .filter(|query_byte| *file_byte == **query_byte)
                                .map(|_| match_count + 1)
                                .unwrap_or(0), 
                            pos + 1
                        )
                    })
                })
                .map_err(|e| AppError::FileIO(e.to_string()));

            match result {
                Err(e) => sender.send(SearchResult::Error(e.clone(), (path_clone, query_clone)))
                    .err().iter()
                    .for_each(|err| error!("Could not handle error {:?} because {:?}", e, err)),
                _ => {}
            };     
        });

        path.to_str()
            .filter(|p| path.file_name().and_then(|os_str| os_str.to_str()).unwrap_or("").contains(query.as_str()))
            .map(|p| self.result_sender.send(SearchResult::Dir(p.to_string())).map_err(|err| AppError::Send(err.to_string())))
            .unwrap_or(Ok(()))
    }
}