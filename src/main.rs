#[macro_use]
extern crate bson;
extern crate mongo_driver;
extern crate chrono;

use crate::db_store::DataStore;
use std::env;
use std::process;

mod db_store;

fn main() {
    // collect and parse args. extract db URI and credentials
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        println!("Missing arguments: rust-mongodb-adapter [URI] [USERNAME] [PASSWORD]");
        process::exit(-1);
    }

    let uri = &args[1]; // local setup: "mongodb://172.43.0.1:27017"
    let username = &args[2];
    let password = &args[3];

    println!("Connecting to MongoDb!");
    let mut data_store: db_store::MongoDataStore = db_store::DataStore::new();
    data_store.initialize(Some(db_store::ConnectionOptions {
        uri: uri.clone(),
        username: username.clone(),
        password: password.clone()
    }));

    println!("Connected to MongoDb.");

    println!("Getting orders...");
    let orders = data_store.get_work_orders();

    println!("Orders Len: {}", orders.len());

    orders.into_iter()
        .for_each(|order| println!("Order data: {}", order.unwrap()))
}
