use bytes::Bytes;
use futures::StreamExt;
use restate_common::types::ServiceId;
use restate_storage_api::state_table::StateTable;
use restate_storage_api::{Storage, Transaction};
use restate_storage_grpc::storage::v1::{
    key, scan_request::Filter, storage_client::StorageClient, Key, Pair, Range, ScanRequest, State,
};
use restate_storage_grpc::Options;
use restate_test_utils::assert_eq;
use restate_test_utils::test;
use tempfile::tempdir;
use tonic::transport::Channel;

#[test(tokio::test)]
async fn test_state() {
    //
    // create a rocksdb storage from options
    //
    let temp_dir = tempdir().expect("could not get a tempdir");
    let path = temp_dir
        .path()
        .to_str()
        .expect("can not convert a path to string")
        .to_string();

    let opts = restate_storage_rocksdb::Options {
        path,
        ..Default::default()
    };
    let rocksdb = opts.build();
    let opts: Options = Default::default();
    let svc = opts.build(rocksdb.clone());

    let (drain, watch) = drain::channel();
    let handle = tokio::spawn(svc.run(watch));

    let mut txn = rocksdb.transaction();
    for i in 0..101 {
        let svc_key = format!("key-{}", i / 10);
        txn.put_user_state(
            1337,
            &ServiceId::new("svc-1", svc_key),
            &Bytes::from(format!("{i}")),
            &Bytes::from(format!("{i}")),
        )
        .await;
    }
    txn.commit().await.expect("write transaction failed");

    let client = StorageClient::connect("http://0.0.0.0:9091")
        .await
        .expect("connecting to the storage service failed");

    let partition_key = Some(1337);

    let tests = vec![
        (
            "Prefix filter with no key parts should return all rows",
            Filter::Prefix(Key {
                key: Some(key::Key::State(State {
                    partition_key: None,
                    service_name: None,
                    service_key: None,
                    // all keys
                    state_key: None,
                })),
            }),
            101,
            0,
        ),
        (
            "Prefix filter with some key parts should return some rows",
            Filter::Prefix(Key {
                key: Some(key::Key::State(State {
                    partition_key,
                    service_name: Some("svc-1".into()),
                    // 0 key (ie 0-9)
                    service_key: Some("key-0".into()),
                    // all keys
                    state_key: None,
                })),
            }),
            10,
            0,
        ),
        (
            "Prefix filter with bad key parts should return no rows",
            Filter::Prefix(Key {
                key: Some(key::Key::State(State {
                    partition_key: Some(404),
                    service_name: None,
                    service_key: None,
                    // all keys
                    state_key: None,
                })),
            }),
            0,
            0,
        ),
        (
            "Range filter with start and end should return values between, inclusive",
            Filter::Range(Range {
                start: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 0 key (ie 0-9)
                        service_key: Some("key-0".into()),
                        state_key: Some("1".into()),
                    })),
                }),
                end: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 0 key (ie 0-9)
                        service_key: Some("key-0".into()),
                        state_key: Some("5".into()),
                    })),
                }),
            }),
            5,
            1,
        ),
        (
            "Range filter with start and end reversed should return no rows",
            Filter::Range(Range {
                start: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 0 key (ie 0-9)
                        service_key: Some("key-0".into()),
                        state_key: Some("5".into()),
                    })),
                }),
                end: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 0 key (ie 0-9)
                        service_key: Some("key-0".into()),
                        state_key: Some("1".into()),
                    })),
                }),
            }),
            0,
            0,
        ),
        (
            "Range filter with same start and end should return one row",
            Filter::Range(Range {
                start: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 0 key (ie 0-9)
                        service_key: Some("key-1".into()),
                        state_key: Some("10".into()),
                    })),
                }),
                end: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 0 key (ie 0-9)
                        service_key: Some("key-1".into()),
                        state_key: Some("10".into()),
                    })),
                }),
            }),
            1,
            10,
        ),
        (
            "Range filter with no start should return rows up to the end, inclusive",
            Filter::Range(Range {
                start: Some(Key {
                    key: Some(key::Key::State(Default::default())),
                }),
                end: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 0 key (ie 0-9)
                        service_key: Some("key-0".into()),
                        state_key: Some("5".into()),
                    })),
                }),
            }),
            6,
            0,
        ),
        (
            "Range filter with no end should return rows from the start, inclusive",
            Filter::Range(Range {
                start: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 9 key (ie 90-99)
                        service_key: Some("key-9".into()),
                        state_key: Some("95".into()),
                    })),
                }),
                // will also range over key-10, so this will cover 95-100
                end: Some(Key {
                    key: Some(key::Key::State(Default::default())),
                }),
            }),
            6,
            95,
        ),
        (
            "Range filter with no start or end should return all rows",
            Filter::Range(Range {
                start: Some(Key {
                    key: Some(key::Key::State(Default::default())),
                }),
                end: Some(Key {
                    key: Some(key::Key::State(Default::default())),
                }),
            }),
            101,
            0,
        ),
        (
            "Range filter with end beyond the last row should return rows from the start, inclusive",
            Filter::Range(Range {
                start: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        // 9 key (ie 90-99)
                        service_key: Some("key-9".into()),
                        state_key: Some("95".into()),
                    })),
                }),
                // beyond the last key
                end: Some(Key {
                    key: Some(key::Key::State(State {
                        partition_key,
                        service_name: Some("svc-1".into()),
                        service_key: Some("key-10".into()),
                        state_key: Some("105".into()),
                    })),
                }),
            }),
            6,
            95,
        ),
        (
            "Key filter should return one row",
            Filter::Key(Key {
                key: Some(key::Key::State(State {
                    partition_key,
                    service_name: Some("svc-1".into()),
                    service_key: Some("key-0".into()),
                    // all keys
                    state_key: Some("0".into()),
                })),
            }),
            1,
            0,
        ),
        (
            "Key filter with bad key should return no rows",
            Filter::Key(Key {
                key: Some(key::Key::State(State {
                    partition_key,
                    service_name: Some("svc-1".into()),
                    service_key: Some("key-0".into()),
                    // all keys
                    state_key: Some("i dont exist".into()),
                })),
            }),
            0,
            0,
        ),
    ];

    for (name, filter, count, offset) in tests {
        let pairs = request(
            client.clone(),
            ScanRequest {
                filter: Some(filter),
            },
        )
        .await;
        assert_eq!(pairs.len(), count, "{}", name);
        pairs.iter().enumerate().for_each(|(i, pair)| {
            if let Some(Key {
                key: Some(key::Key::State(key)),
            }) = pair.key.clone()
            {
                assert_eq!(key.partition_key, Some(1337), "{}", name);
                assert_eq!(key.service_name, Some("svc-1".into()), "{}", name);
                assert_eq!(
                    pair.value,
                    Bytes::from(format!("{}", i + offset)),
                    "{}",
                    name
                );
            } else {
                panic!("a state key was not found in returned pair {pair:?} during test '{name}'",)
            }
        });
    }

    drain.drain().await;
    handle
        .await
        .expect("join failed")
        .expect("gRPC server failed")
}

async fn request(mut client: StorageClient<Channel>, request: ScanRequest) -> Vec<Pair> {
    client
        .scan(request)
        .await
        .expect("scan request failed")
        .into_inner()
        .map(|b| b.expect("pair result was not Ok"))
        .collect()
        .await
}
