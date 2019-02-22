use std::fmt;
use std::sync::Arc;

use chrono::prelude::*;
use mongo_driver::client::{ClientPool, Uri};
use mongo_driver::CommandAndFindOptions;
use mongo_driver::flags;
use mongo_driver::Result;

use crate::bson;

trait StoredData {}

pub trait DataStore<T> {
    fn new() -> Self;
    fn initialize(&mut self, options: Option<ConnectionOptions>) -> bool;
    fn get_work_orders(&mut self) -> Vec<Option<T>>;
}

pub struct ConnectionOptions {
    pub uri: String,
    pub username: String,
    pub password: String,
}

pub struct MongoDataStore {
    initialized: bool,
    client_pool: Option<Arc<ClientPool>>,
}

pub struct WorkOrder {
    order_id: String,
    size: String,
    filled: String,
    status: String,
    ticker: String,
    mic: String,
    action: String,
    lastModified: DateTime<Utc>,
}

impl fmt::Display for WorkOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[order_id: {}, size: {}, filled: {}, status: {}, ticker: {}, mic: {}, action: {}, lastModified: {}]",
               self.order_id, self.size, self.filled, self.status, self.ticker, self.mic, self.action,
               self.lastModified)
    }
}

impl DataStore<WorkOrder> for MongoDataStore {
    fn new() -> Self {
        MongoDataStore { initialized: false, client_pool: None }
    }

    fn initialize(&mut self, options: Option<ConnectionOptions>) -> bool {
        if options.is_none() {
            panic!("Missing database connection options!")
        }

        let connection_options = options.unwrap();
        let uri = Uri::new(connection_options.uri).unwrap();
        let pool = Arc::new(ClientPool::new(uri.clone(), None));

        self.client_pool = Some(pool);

        self.initialized = true;
        true
    }

    fn get_work_orders(&mut self) -> Vec<Option<WorkOrder>> {
        if !self.initialized {
            panic!("DB connection hasn't been initialized!")
        }

        let client_pool = self.client_pool.clone().unwrap();
        let client = client_pool.pop();

        let work_orders_collection = client.get_collection("finfabrik", "workOrder");

        let query = doc! {};
        let search_options = CommandAndFindOptions {
            query_flags: flags::Flags::new(),
            skip: 0,
            limit: 10,
            batch_size: 0,
            fields: Some(doc! {
                                "orderId"   => true,
                                "size"      => true,
                                "filled"    => true,
                                "status"    => true,
                                "ticker"    => true,
                                "mic"       => true,
                                "action"    => true,
                                "lastModified" => true
                                }), // projection fields
            read_prefs: None,
        };

        let cursor = work_orders_collection.find(&query, Some(&search_options)).unwrap();

        cursor.into_iter()
            .map(|document| {
                match document {
                    Ok(doc) => {
                        // guard for missing fields
                        let id = match doc.get_str("orderId") {
                            Ok(val) => val,
                            Err(_) => &""
                        };

                        Some(WorkOrder {
                            order_id: get_field_as_str("orderId", &doc),
                            size: get_field_as_str("size", &doc),
                            filled: get_field_as_str("filled", &doc),
                            status: get_field_as_str("status", &doc),
                            ticker: get_field_as_str("ticker", &doc),
                            mic: get_field_as_str("mic", &doc),
                            action: get_field_as_str("action", &doc),
                            lastModified: get_field_as_datetime("lastModified", &doc),
                        })
                    }
                    Err(err) => {
                        println!("Error retrieving Mongo document: {:?}", err);
                        None
                    }
                }
            })
            .collect::<Vec<Option<WorkOrder>>>()
    }
}

/*
* Helper functions: Converters
*/

fn get_field_as_str(field_name: &str, document: &bson::Document) -> String {
    let id = match document.get_str(field_name) {
        Ok(val) => val,
        Err(_) => &""
    };

    String::from(id)
}

fn get_field_as_datetime(field_name: &str, document: &bson::Document) -> DateTime<Utc> {
    let datetime = match document.get_utc_datetime(field_name) {
        Ok(val) => val.clone(),
        Err(_) => Utc::now() // fixme. not that great
    };

    datetime
}