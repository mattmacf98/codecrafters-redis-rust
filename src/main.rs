#![allow(unused_imports)]
use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex}, thread};
use clap::Parser;

use crate::{instance::{master::MasterInstance, replica::ReplicaInstance, Instance}, redis::{client::{self, CacheVal, Client, StringCacheVal}, create_array_resp, create_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub mod resp;
pub mod redis;
pub mod commands;
pub mod instance;

#[derive(Parser)]
#[command(name = "codecrafters-redis")]
#[command(about = "A Redis server implementation")]
struct Args {
    /// Port to bind the server to
    #[arg(long, default_value = "6379")]
    port: u16,
    /// Replicate from another Redis server
    #[arg(long)]
    replicaof: Option<String>,
    // RDB dir
    #[arg(long, default_value = "./")]
    dir: String,
    // RDB file
    #[arg(long, default_value = "dump.rdb")]
    dbfilename: String,
}

fn main() {
    // Parse command line arguments
    let args = Args::parse();
    if args.replicaof.is_some() {
        let instance = ReplicaInstance::new(args.port.to_string(), args.dir.clone(), args.dbfilename.clone(), args.replicaof.clone());
        instance.start();
    } else {
        let instance = MasterInstance::new(args.port.to_string(), args.dir.clone(), args.dbfilename.clone());
        instance.start();
    }
}