use std::fmt;
use std::sync::Arc;

use mongo_driver::client::{ClientPool, Uri};
use mongo_driver::CommandAndFindOptions;
use mongo_driver::flags;
use mongo_driver::Result;

use crate::bson;

trait StoredData {}

pub trait DataStore<T> {
    fn new() -> Self;
    fn initialize(&mut self) -> bool;
    fn get_work_orders(&mut self) -> Vec<Option<T>>;
}

pub struct MongoDataStore {
    initialized: bool,
    client_pool: Option<Arc<ClientPool>>,
}

pub struct WorkOrder {
    order_id: String
}

impl fmt::Display for WorkOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[order_id: {}]", self.order_id)
    }
}

impl DataStore<WorkOrder> for MongoDataStore {
    fn new() -> Self {
        MongoDataStore { initialized: false, client_pool: None }
    }

    fn initialize(&mut self) -> bool {
        let uri = Uri::new("mongodb://172.43.0.1:27017").unwrap();
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
            fields: None,
            read_prefs: None,
        };

        let cursor = work_orders_collection.find(&query, Some(&search_options)).unwrap();

        let orders = cursor.into_iter()
            .map(|document| {
                match document {
                    Ok(doc) => {
                        // guard for missing fields
                        let id = match doc.get_str("orderId") {
                            Ok(val) => val,
                            Err(_) => &""
                        };

//                        println!("Got doc {} with ID: {}", doc, id);
                        Some(WorkOrder { order_id: String::from(id) })
                    }
                    Err(err) => {
                        println!("Error retrieving Mongo document: {:?}", err);
                        None
                    }
                }
            })
            .collect::<Vec<Option<WorkOrder>>>();

        // return line. todo can return with previous expression
        orders
    }
}