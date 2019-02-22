#[macro_use]
extern crate bson;
extern crate mongo_driver;

use crate::db_store::DataStore;

mod db_store;

fn main() {
    println!("Connecting to MongoDb!");
    let mut data_store: db_store::MongoDataStore = db_store::DataStore::new();
    data_store.initialize();
    println!("Connected to MongoDb.");

    println!("Getting orders...");
    let orders = data_store.get_work_orders();

    println!("Orders Len: {}", orders.len());

    orders.into_iter()
        .for_each(|order| println!("Order data: {}", order.unwrap()))
}
