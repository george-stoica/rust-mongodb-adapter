use std::fmt;
use std::sync::Arc;

use chrono::prelude::*;
use mongo_driver::client::{ClientPool, Uri};
use mongo_driver::CommandAndFindOptions;
use mongo_driver::flags;
use mongo_driver::Result;

use crate::bson;
use bson::Document;

trait StoredData {}

pub trait DataStore<T> {
    fn new() -> Self;
    fn initialize(&mut self, options: Option<ConnectionOptions>) -> bool;
    fn get_data(&mut self) -> Vec<Option<T>>;
    fn create_new(&self, data: &T) -> Option<T>;
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
    pub order_id: String,
    pub size: String,
    pub filled: String,
    pub status: String,
    pub ticker: String,
    pub mic: String,
    pub action: String,
    pub timestamp: DateTime<Utc>,
    pub lastModified: DateTime<Utc>,
}

impl fmt::Display for WorkOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[order_id: {}, size: {}, filled: {}, status: {}, ticker: {}, mic: {}, action: {}, timestamp: {}, lastModified: {}]",
               self.order_id, self.size, self.filled, self.status, self.ticker, self.mic, self.action,
               self.timestamp, self.lastModified)
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

    fn get_data(&mut self) -> Vec<Option<WorkOrder>> {
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
                        Some(WorkOrder {
                            order_id: get_field_as_str("orderId", &doc),
                            size: get_field_as_str("size", &doc),
                            filled: get_field_as_str("filled", &doc),
                            status: get_field_as_str("status", &doc),
                            ticker: get_field_as_str("ticker", &doc),
                            mic: get_field_as_str("mic", &doc),
                            action: get_field_as_str("action", &doc),
                            timestamp: get_field_as_datetime("timestamp", &doc),
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

    fn create_new(&self, data: &WorkOrder) -> Option<WorkOrder> {
        if !self.initialized {
            panic!("DB connection hasn't been initialized!")
        }

        let client_pool = self.client_pool.clone().unwrap();
        let client = client_pool.pop();

        let work_orders_collection = client.get_collection("finfabrik", "workOrder");

        // map from data param
        let doc_to_insert = map_to_mongo_doc(data);

        println!("Inserting new document...");
        let insert_result = work_orders_collection.insert(&doc_to_insert,  None);

        return match insert_result {
            Ok(result) => {
                println!("Inserted document successully");
                Some(map_to_external_model(&doc_to_insert))
            },
            Err(err) => {
                println!("Error inserting WorkOrder: {}. Error: {}", data, err);
                None
            }
        }
    }
}

/*
* Helper functions: Converters
*/

fn map_to_mongo_doc(data: &WorkOrder) -> bson::Document {
    doc! {
        "orderId" => data.order_id.clone(),
        "size" => data.size.clone(),
        "filled" => data.filled.clone(),
        "status" => data.status.clone(),
        "ticker"  => data.ticker.clone(),
        "mic" => data.mic.clone(),
        "action" => data.action.clone(),
        "timestamp" => data.timestamp.clone(),
        "lastModified" => data.lastModified.clone()
    }
}

fn map_to_external_model(mongo_doc: &Document) -> WorkOrder {
    WorkOrder {
        order_id: "".to_string(),
        size: "".to_string(),
        filled: "".to_string(),
        status: "".to_string(),
        ticker: "".to_string(),
        mic: "".to_string(),
        action: "".to_string(),
        timestamp: get_field_as_datetime("timestamp", mongo_doc),
        lastModified: get_field_as_datetime("lastModified", mongo_doc)
    }
}

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