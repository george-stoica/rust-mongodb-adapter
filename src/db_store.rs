use std::fmt;
use std::sync::Arc;

use bson::Document;
use chrono::prelude::*;
use mongo_driver::client::{ClientPool, Uri};
use mongo_driver::CommandAndFindOptions;
use mongo_driver::flags;

use crate::bson;

pub trait DataStore<T> {
    fn new(options: Option<ConnectionOptions>) -> Self;
    fn get_data(&self) -> Vec<Option<T>>;
    fn get_data_by_id(&self, id: String) -> Option<T>;
    fn create_new(&self, data: &T) -> Option<T>;
    fn update(&self, data: &T) -> Option<T>;
    fn delete(&self, id: String) -> bool;
}

pub struct ConnectionOptions {
    pub uri: String,
    pub username: String,
    pub password: String,
}

pub struct MongoDataStore {
    initialized: bool,
    client_pool: Arc<ClientPool>,
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
    pub last_modified: DateTime<Utc>,
}

impl fmt::Display for WorkOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[order_id: {}, size: {}, filled: {}, status: {}, ticker: {}, mic: {}, action: {}, \
        timestamp: {}, last_modified: {}]",
               self.order_id, self.size, self.filled, self.status, self.ticker, self.mic, self.action,
               self.timestamp, self.last_modified)
    }
}

impl DataStore<WorkOrder> for MongoDataStore {
    fn new(options: Option<ConnectionOptions>) -> Self {
        if options.is_none() {
            panic!("Missing database connection options!")
        }

        let connection_options = options.unwrap();
        let uri = Uri::new(connection_options.uri).unwrap();
        let pool = Arc::new(ClientPool::new(uri.clone(), None));

        MongoDataStore { initialized: true, client_pool: pool }
    }

    fn get_data(&self) -> Vec<Option<WorkOrder>> {
        let client_pool = &self.client_pool;
        let client = client_pool.pop();

        let work_orders_collection = client.get_collection("finfabrik", "workOrder");

        let query = doc! {
            "$query" => {},
            "$sort"  => {
                "timestamp" => -1
            }
        };

        let search_options = CommandAndFindOptions {
            query_flags: flags::Flags::new(),
            skip: 0,
            limit: 10,
            batch_size: 0,
            fields: Some(build_out_doc_projection()),
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
                            last_modified: get_field_as_datetime("last_modified", &doc),
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

    fn get_data_by_id(&self, id: String) -> Option<WorkOrder> {
        let client_pool = &self.client_pool;
        let client = client_pool.pop();

        let work_orders_collection = client.get_collection("finfabrik", "workOrder");

        let filter = doc! {
            "orderId": id.clone()
        };

        let search_options = CommandAndFindOptions {
            query_flags: flags::Flags::new(),
            skip: 0,
            limit: 10,
            batch_size: 0,
            fields: Some(build_out_doc_projection()),
            read_prefs: None,
        };

        let result = work_orders_collection.find(&filter, Some(&search_options))
            .unwrap()
            .next();

        match result {
            Some(mongo_result) => match mongo_result {
                Ok(found) => Some(map_to_external_model(&found)),
                Err(err) => {
                    println!("Error calling find by ID for ID: {}, Err: {}", id.clone(), err);
                    None
                }
            }
            None => {
                println!("No orders found with ID: {}", id);
                None
            }
        }
    }

    fn create_new(&self, data: &WorkOrder) -> Option<WorkOrder> {
        let client_pool = &self.client_pool;
        let client = client_pool.pop();

        let work_orders_collection = client.get_collection("finfabrik", "workOrder");

        // map from data param
        let doc_to_insert = map_to_mongo_doc(data);

        println!("Inserting new document...");
        let insert_result = work_orders_collection.insert(&doc_to_insert, None);

        return match insert_result {
            Ok(result) => {
                println!("Inserted document successully");
                Some(map_to_external_model(&doc_to_insert))
            }
            Err(err) => {
                println!("Error inserting WorkOrder: {}. Error: {}", data, err);
                None
            }
        };
    }

    /*
    * TODO: implement partial updates. Currently this function updates the entire document.
    */
    fn update(&self, data: &WorkOrder) -> Option<WorkOrder> {
        let client_pool = &self.client_pool;
        let client = client_pool.pop();

        let work_orders_collection = client.get_collection("finfabrik", "workOrder");

        let filter = doc! {
            "orderId": data.order_id.clone()
        };

        let update_result = work_orders_collection.update(&filter,
                                                          &map_to_mongo_doc(data),
                                                          None);

        match update_result {
            Ok(_) => Some(WorkOrder {
                order_id: String::from("665599"),
                size: String::from("5"),
                filled: String::from("0"),
                status: String::from("Accepted"),
                ticker: String::from("BTCUSD"),
                mic: String::from("LIQD"),
                action: "BUY".to_string(),
                timestamp: Utc::now(),
                last_modified: Utc::now(),
            }),
            Err(_) => {
                println!("Error updating document with id: {}", data.order_id.clone());
                None
            }
        }
    }

    fn delete(&self, id: String) -> bool {
        let client_pool = &self.client_pool;
        let client = client_pool.pop();

        let work_orders_collection = client.get_collection("finfabrik", "workOrder");

        let filter = doc! {
            "orderId" => id.clone()
        };

        let delete_result = work_orders_collection.remove(&filter, None);

        match delete_result {
            Ok(_) => true,
            Err(err) => {
                println!("Error deleting document with ID: {}. Err: {}", id.clone(), err);
                false
            }
        }
    }
}

/*
* Helper functions: Converters
*/

fn build_out_doc_projection() -> Document {
    doc! {
                                "_id"       => true,
                                "orderId"   => true,
                                "size"      => true,
                                "filled"    => true,
                                "status"    => true,
                                "ticker"    => true,
                                "mic"       => true,
                                "action"    => true,
                                "last_modified" => true
                                }
}

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
        "last_modified" => data.last_modified.clone()
    }
}

fn map_to_external_model(mongo_doc: &Document) -> WorkOrder {
    WorkOrder {
        order_id: get_field_as_str("orderId", mongo_doc),
        size: get_field_as_str("size", mongo_doc),
        filled: get_field_as_str("filled", mongo_doc),
        status: get_field_as_str("status", mongo_doc),
        ticker: get_field_as_str("ticker", mongo_doc),
        mic: get_field_as_str("mic", mongo_doc),
        action: get_field_as_str("action", mongo_doc),
        timestamp: get_field_as_datetime("timestamp", mongo_doc),
        last_modified: get_field_as_datetime("last_modified", mongo_doc),
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