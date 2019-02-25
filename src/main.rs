#[macro_use]
extern crate bson;
extern crate mongo_driver;
extern crate chrono;

use crate::db_store::DataStore;
use std::env;
use std::process;
use crate::db_store::WorkOrder;
use chrono::prelude::*;

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

    println!("Creating test order...");
    let insertion = data_store.create_new(&WorkOrder {
        order_id: String::from("665599"),
        size: String::from("1"),
        filled: String::from("0"),
        status: String::from("Accepted"),
        ticker: String::from("BTCUSD"),
        mic: String::from("LIQD"),
        action: "BUY".to_string(),
        timestamp: Utc::now(),
        lastModified: Utc::now()
    });

    if insertion.is_none() {
        println!("Could not insert  new order!")
    }

    println!("Get order with id: 665599");
    let single_order = data_store.get_data_by_id("665599".to_string()).unwrap();

    println!("Found: {}", single_order);

    println!("Updating last order size...");
    let updated_order = data_store.update(&WorkOrder {
        order_id: String::from("665599"),
        size: String::from("5"),
        filled: String::from("0"),
        status: String::from("Accepted"),
        ticker: String::from("BTCUSD"),
        mic: String::from("LIQD"),
        action: "BUY".to_string(),
        timestamp: Utc::now(),
        lastModified: Utc::now()
    });

    println!("Getting all orders...");
    let orders = data_store.get_data();

    println!("Orders Len: {}", orders.len());

    orders.into_iter()
        .for_each(|order| println!("Order data: {}", order.unwrap()))
}
