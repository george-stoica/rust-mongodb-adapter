### Mongodb adapter sample
Sample project that connects to a Mongodb instance and executes queries.
Uses https://github.com/thijsc/mongo-rust-driver which is a wrapper around https://github.com/mongodb/mongo-c-driver.

```
cargo build
cargo run
```

Execute sample
```
rust-mongodb-adapter [URI] [USERNAME] [PASSWORD]
```
with Cargo inside project root
```
cargo run [URI] [USERNAME] [PASSWORD]
```

- URI - MongoDb connection URI
- USERNAME - Mongodb connection username
- PASSWORD - Mongodb connection password