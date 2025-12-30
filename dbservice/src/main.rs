use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;

use jsonrpc_v2::{Data, Error, Params, Server};
use serde::Deserialize;
use serde::Serialize;
use zkwasm_host_circuits::host::datahash::DataHashRecord;
use zkwasm_host_circuits::host::datahash::MongoDataHash;
use zkwasm_host_circuits::host::db::RocksDB;
use zkwasm_host_circuits::host::db::TreeDB;
use zkwasm_host_circuits::host::merkle::MerkleTree;
use zkwasm_host_circuits::host::mongomerkle::MerkleRecord;
use zkwasm_host_circuits::host::mongomerkle::MongoMerkle;
use zkwasm_host_circuits::constants::MERKLE_DEPTH;

use std::time::Instant;

//use tokio::runtime::Runtime;

static DB: OnceLock<RocksDB> = OnceLock::new();
static LOG_CSM_LATENCY: OnceLock<bool> = OnceLock::new();
static SESSIONS: OnceLock<Mutex<HashMap<String, Arc<Mutex<OverlayState>>>>> = OnceLock::new();
static NEXT_SESSION_ID: AtomicU64 = AtomicU64::new(1);

fn log_csm_latency() -> bool {
    *LOG_CSM_LATENCY.get_or_init(|| {
        std::env::var("LOG_CSM_LATENCY")
            .ok()
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    })
}

#[derive(Default)]
struct OverlayState {
    merkle: HashMap<[u8; 32], Option<MerkleRecord>>,
    data: HashMap<[u8; 32], Option<DataHashRecord>>,
}

#[derive(Clone)]
struct SessionDB {
    base: RocksDB,
    overlay: Arc<Mutex<OverlayState>>,
}

impl TreeDB for SessionDB {
    fn get_merkle_record(&self, hash: &[u8; 32]) -> Result<Option<MerkleRecord>, anyhow::Error> {
        let guard = self
            .overlay
            .lock()
            .map_err(|_| anyhow::anyhow!("overlay lock poisoned"))?;
        if let Some(record) = guard.merkle.get(hash) {
            return Ok(record.clone());
        }
        drop(guard);
        self.base.get_merkle_record(hash)
    }

    fn set_merkle_record(&mut self, record: MerkleRecord) -> Result<(), anyhow::Error> {
        let mut guard = self
            .overlay
            .lock()
            .map_err(|_| anyhow::anyhow!("overlay lock poisoned"))?;
        guard.merkle.insert(record.hash, Some(record));
        Ok(())
    }

    fn set_merkle_records(&mut self, records: &Vec<MerkleRecord>) -> Result<(), anyhow::Error> {
        let mut guard = self
            .overlay
            .lock()
            .map_err(|_| anyhow::anyhow!("overlay lock poisoned"))?;
        for record in records {
            guard.merkle.insert(record.hash, Some(record.clone()));
        }
        Ok(())
    }

    fn get_data_record(&self, hash: &[u8; 32]) -> Result<Option<DataHashRecord>, anyhow::Error> {
        let guard = self
            .overlay
            .lock()
            .map_err(|_| anyhow::anyhow!("overlay lock poisoned"))?;
        if let Some(record) = guard.data.get(hash) {
            return Ok(record.clone());
        }
        drop(guard);
        self.base.get_data_record(hash)
    }

    fn set_data_record(&mut self, record: DataHashRecord) -> Result<(), anyhow::Error> {
        let mut guard = self
            .overlay
            .lock()
            .map_err(|_| anyhow::anyhow!("overlay lock poisoned"))?;
        guard.data.insert(record.hash, Some(record));
        Ok(())
    }

    fn set_data_records(&mut self, records: &Vec<DataHashRecord>) -> Result<(), anyhow::Error> {
        let mut guard = self
            .overlay
            .lock()
            .map_err(|_| anyhow::anyhow!("overlay lock poisoned"))?;
        for record in records {
            guard.data.insert(record.hash, Some(record.clone()));
        }
        Ok(())
    }

    fn start_record(&mut self, _record_db: RocksDB) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("SessionDB does not support record"))
    }

    fn stop_record(&mut self) -> anyhow::Result<RocksDB> {
        Err(anyhow::anyhow!("SessionDB does not support record"))
    }

    fn is_recording(&self) -> bool {
        false
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct UpdateLeafRequest {
    session: Option<String>,
    root: [u8; 32],
    data: [u8; 32],
    index: String, // u64 encoding
}
#[derive(Clone, Deserialize, Serialize)]
pub struct GetLeafRequest {
    session: Option<String>,
    root: [u8; 32],
    index: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct UpdateRecordRequest {
    session: Option<String>,
    hash: [u8; 32],
    data: Vec<String>, // vec u64 string
}
#[derive(Clone, Deserialize, Serialize)]
pub struct GetRecordRequest {
    session: Option<String>,
    hash: [u8; 32],
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyTxsRequest {
    session: Option<String>,
    root: [u8; 32],
    txs: Vec<ApplyTxTrace>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyTxTrace {
    writes: Vec<ApplyLeafWrite>,
    update_records: Vec<ApplyRecordUpdate>,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ApplyLeafWrite {
    index: String,
    data: [u8; 32],
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ApplyRecordUpdate {
    hash: [u8; 32],
    data: Vec<String>, // vec u64 string
}

#[derive(Clone, Deserialize, Serialize)]
pub struct PingRequest {}

#[derive(Clone, Deserialize, Serialize)]
pub struct BeginSessionRequest {}

#[derive(Clone, Deserialize, Serialize)]
pub struct DropSessionRequest {
    session: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct ResetSessionRequest {
    session: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct CommitSessionRequest {
    session: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct CommitSessionResponse {
    merkle_records: u64,
    data_records: u64,
}

fn sessions() -> &'static Mutex<HashMap<String, Arc<Mutex<OverlayState>>>> {
    SESSIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_db(session: Option<String>) -> Result<Rc<RefCell<dyn TreeDB>>, Error> {
    let db = DB.get().expect("DB not initialized").clone();
    if let Some(session) = session {
        let overlay = sessions()
            .lock()
            .map_err(|_| Error::INTERNAL_ERROR)?
            .get(&session)
            .cloned();
        let Some(overlay) = overlay else {
            return Err(Error::INVALID_PARAMS);
        };
        let db = SessionDB { base: db, overlay };
        Ok(Rc::new(RefCell::new(db)))
    } else {
        Ok(Rc::new(RefCell::new(db)))
    }
}

fn get_mt(root: [u8; 32], session: Option<String>) -> Result<MongoMerkle<32>, Error> {
    let db = get_db(session)?;
    Ok(MongoMerkle::<MERKLE_DEPTH>::construct([0; 32], root, Some(db)))
}

async fn ping(Params(_request): Params<PingRequest>) -> Result<bool, Error> {
    Ok(true)
}

async fn begin_session(Params(_request): Params<BeginSessionRequest>) -> Result<String, Error> {
    let id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
    let session = format!("s{id}");
    let overlay = Arc::new(Mutex::new(OverlayState::default()));
    sessions()
        .lock()
        .map_err(|_| Error::INTERNAL_ERROR)?
        .insert(session.clone(), overlay);
    Ok(session)
}

async fn drop_session(Params(request): Params<DropSessionRequest>) -> Result<bool, Error> {
    let removed = sessions()
        .lock()
        .map_err(|_| Error::INTERNAL_ERROR)?
        .remove(&request.session)
        .is_some();
    Ok(removed)
}

async fn reset_session(Params(request): Params<ResetSessionRequest>) -> Result<bool, Error> {
    let overlay = sessions()
        .lock()
        .map_err(|_| Error::INTERNAL_ERROR)?
        .get(&request.session)
        .cloned();
    let Some(overlay) = overlay else {
        return Err(Error::INVALID_PARAMS);
    };
    let mut guard = overlay
        .lock()
        .map_err(|_| Error::INTERNAL_ERROR)?;
    guard.merkle.clear();
    guard.data.clear();
    Ok(true)
}

async fn commit_session(
    Params(request): Params<CommitSessionRequest>,
) -> Result<CommitSessionResponse, Error> {
    let overlay = sessions()
        .lock()
        .map_err(|_| Error::INTERNAL_ERROR)?
        .get(&request.session)
        .cloned();
    let Some(overlay) = overlay else {
        return Err(Error::INVALID_PARAMS);
    };

    actix_web::web::block(move || -> Result<CommitSessionResponse, Error> {
        let (merkle, data) = {
            let mut guard = overlay
                .lock()
                .map_err(|_| Error::INTERNAL_ERROR)?;
            let merkle = std::mem::take(&mut guard.merkle);
            let data = std::mem::take(&mut guard.data);
            (merkle, data)
        };

        let merkle_records: Vec<MerkleRecord> = merkle.into_values().flatten().collect();
        let data_records: Vec<DataHashRecord> = data.into_values().flatten().collect();

        let mut base = DB.get().expect("DB not initialized").clone();
        base.set_merkle_records(&merkle_records)
            .map_err(|e| {
                println!("commit_session: set_merkle_records error {:?}", e);
                Error::INTERNAL_ERROR
            })?;
        for record in data_records.iter().cloned() {
            base.set_data_record(record).map_err(|e| {
                println!("commit_session: set_data_record error {:?}", e);
                Error::INTERNAL_ERROR
            })?;
        }

        Ok(CommitSessionResponse {
            merkle_records: merkle_records.len() as u64,
            data_records: data_records.len() as u64,
        })
    })
    .await
    .map_err(|_| Error::INTERNAL_ERROR)?
}

async fn update_leaf(Params(request): Params<UpdateLeafRequest>) -> Result<[u8; 32], Error> {
    let start = if log_csm_latency() {
        Some(Instant::now())
    } else {
        None
    };
    let index = u64::from_str_radix(request.index.as_str(), 10).unwrap();
    let mut mt = get_mt(request.root, request.session)?;
    mt.update_leaf_data_with_proof(index, &request.data.to_vec())
        .map_err(|e| {
            println!("update leaf data with proof error {:?}", e);
            Error::INTERNAL_ERROR
        })?;
    let hash = mt.get_root_hash();
    if let Some(start) = start {
        println!("time taken for update_leaf is {:?}", start.elapsed());
    }
    Ok(hash)
}

async fn get_leaf(Params(request): Params<GetLeafRequest>) -> Result<[u8; 32], Error> {
    let start = if log_csm_latency() {
        Some(Instant::now())
    } else {
        None
    };
    let index = u64::from_str_radix(request.index.as_str(), 10).unwrap();
    let mt = get_mt(request.root, request.session)?;
    let (leaf, _) = mt.get_leaf_with_proof(index).map_err(|e| {
        println!("get leaf error {:?}", e);
        Error::INTERNAL_ERROR
    })?;
    if let Some(start) = start {
        println!("time taken for get_leaf is {:?}", start.elapsed());
    }
    Ok(leaf.data.unwrap_or([0; 32]))
}
async fn update_record(Params(request): Params<UpdateRecordRequest>) -> Result<(), Error> {
    let db = get_db(request.session)?;
    let mut mongo_datahash = MongoDataHash::construct([0; 32], Some(db));
    mongo_datahash
        .update_record(DataHashRecord {
            hash: request.hash,
            data: request
                .data
                .iter()
                .map(|x| {
                    let x = u64::from_str_radix(x, 10).unwrap();
                    x.to_le_bytes()
                })
                .flatten()
                .collect::<Vec<u8>>(),
        })
        .map_err(|e| {
            println!("update record error {:?}", e);
            Error::INTERNAL_ERROR
        })?;
    Ok(())
}

async fn get_record(Params(request): Params<GetRecordRequest>) -> Result<Vec<String>, Error> {
    let db = get_db(request.session)?;
    let mongo_datahash = MongoDataHash::construct([0; 32], Some(db));
    let datahashrecord = mongo_datahash.get_record(&request.hash).map_err(|e| {
        println!("get record error {:?}", e);
        Error::INTERNAL_ERROR
    })?;
    let data = datahashrecord.map_or(vec![], |r| {
        r.data
            .chunks_exact(8)
            .into_iter()
            .into_iter()
            .map(|x| u64::from_le_bytes(x.try_into().unwrap()).to_string())
            .collect::<Vec<String>>()
    });
    Ok(data)
}

async fn apply_txs(Params(request): Params<ApplyTxsRequest>) -> Result<Vec<[u8; 32]>, Error> {
    let start = if log_csm_latency() {
        Some(Instant::now())
    } else {
        None
    };

    let db = get_db(request.session)?;
    let mut mt = MongoMerkle::<MERKLE_DEPTH>::construct([0; 32], request.root, Some(db.clone()));

    let mut roots: Vec<[u8; 32]> = Vec::with_capacity(request.txs.len());
    let mut data_records: Vec<DataHashRecord> = Vec::new();
    for tx in request.txs.iter() {
        for rec in tx.update_records.iter() {
            let data = rec
                .data
                .iter()
                .map(|x| {
                    let x = u64::from_str_radix(x, 10).map_err(|_| Error::INVALID_PARAMS)?;
                    Ok(x.to_le_bytes())
                })
                .collect::<Result<Vec<[u8; 8]>, Error>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<u8>>();
            data_records.push(DataHashRecord { hash: rec.hash, data });
        }

        for w in tx.writes.iter() {
            let index = u64::from_str_radix(w.index.as_str(), 10).map_err(|_| Error::INVALID_PARAMS)?;
            mt.update_leaf_data_with_proof(index, &w.data.to_vec())
                .map_err(|e| {
                    println!("apply_txs update_leaf error {:?}", e);
                    Error::INTERNAL_ERROR
                })?;
        }

        roots.push(mt.get_root_hash());
    }

    if !data_records.is_empty() {
        db.borrow_mut().set_data_records(&data_records).map_err(|e| {
            println!("apply_txs set_data_records error {:?}", e);
            Error::INTERNAL_ERROR
        })?;
    }

    if let Some(start) = start {
        println!("time taken for apply_txs is {:?}", start.elapsed());
    }

    Ok(roots)
}

async fn apply_txs_final(Params(request): Params<ApplyTxsRequest>) -> Result<[u8; 32], Error> {
    let start = if log_csm_latency() {
        Some(Instant::now())
    } else {
        None
    };

    let db = get_db(request.session)?;
    let mut mt = MongoMerkle::<MERKLE_DEPTH>::construct([0; 32], request.root, Some(db.clone()));

    let mut leaf_updates: Vec<(u64, [u8; 32])> = Vec::new();
    let mut data_records: Vec<DataHashRecord> = Vec::new();
    for tx in request.txs.iter() {
        for rec in tx.update_records.iter() {
            let data = rec
                .data
                .iter()
                .map(|x| {
                    let x = u64::from_str_radix(x, 10).map_err(|_| Error::INVALID_PARAMS)?;
                    Ok(x.to_le_bytes())
                })
                .collect::<Result<Vec<[u8; 8]>, Error>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<u8>>();
            data_records.push(DataHashRecord { hash: rec.hash, data });
        }

        for w in tx.writes.iter() {
            let index = u64::from_str_radix(w.index.as_str(), 10).map_err(|_| Error::INVALID_PARAMS)?;
            leaf_updates.push((index, w.data));
        }
    }

    mt.update_leaves_batch(&leaf_updates)
        .map_err(|e| {
            println!("apply_txs_final update_leaves_batch error {:?}", e);
            Error::INTERNAL_ERROR
        })?;

    if !data_records.is_empty() {
        db.borrow_mut()
            .set_data_records(&data_records)
            .map_err(|e| {
                println!("apply_txs_final set_data_records error {:?}", e);
                Error::INTERNAL_ERROR
            })?;
    }

    if let Some(start) = start {
        println!("time taken for apply_txs_final is {:?}", start.elapsed());
    }

    Ok(mt.get_root_hash())
}

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(version = "1.0", author = "Sinka")]
struct Args {
    /// The URI to be processed
    #[clap(short, long)]
    uri: String,
}

fn main() -> std::io::Result<()> {
    let args = Args::parse();
    if DB.set(RocksDB::new(args.uri).unwrap()).is_err() {
        panic!("DB already initialized");
    }
    let rpc = Server::new()
        .with_data(Data::new(String::from("Hello!")))
        .with_method("ping", ping)
        .with_method("begin_session", begin_session)
        .with_method("drop_session", drop_session)
        .with_method("reset_session", reset_session)
        .with_method("commit_session", commit_session)
        .with_method("update_leaf", update_leaf)
        .with_method("get_leaf", get_leaf)
        .with_method("update_record", update_record)
        .with_method("get_record", get_record)
        .with_method("apply_txs", apply_txs)
        .with_method("apply_txs_final", apply_txs_final)
        .finish();

    actix_web::rt::System::new().block_on(
        actix_web::HttpServer::new(move || {
            let rpc = rpc.clone();
            actix_web::App::new().service(
                actix_web::web::service("/")
                    .guard(actix_web::guard::Post())
                    .finish(rpc.into_web_service()),
            )
        })
        .bind("0.0.0.0:3030")?
        .run(),
    )
}

#[test]
fn update_leaf_test() {
    let tmp = tempfile::tempdir().unwrap();
    let db = Rc::new(RefCell::new(RocksDB::new(tmp.path()).unwrap()));

    let root = zkwasm_host_circuits::host::mongomerkle::DEFAULT_HASH_VEC[MERKLE_DEPTH];
    let mut mt = MongoMerkle::<MERKLE_DEPTH>::construct([0; 32], root, Some(db));

    let index = 2_u64.pow(MERKLE_DEPTH as u32) - 1;
    let data = [0x2eu8; 32];

    let proof = mt
        .update_leaf_data_with_proof(index, &data.to_vec())
        .unwrap();
    assert_eq!(mt.get_root_hash(), proof.root);

    let (leaf, leaf_proof) = mt.get_leaf_with_proof(index).unwrap();
    assert_eq!(leaf.data.unwrap_or([0; 32]), data);
    assert!(mt.verify_proof(&leaf_proof).unwrap());
}

#[test]
fn apply_txs_final_matches_apply_txs_last_root_and_persists_records() {
    let root = zkwasm_host_circuits::host::mongomerkle::DEFAULT_HASH_VEC[MERKLE_DEPTH];
    // The Merkle API uses "node index" (heap indexing), where leaf indices are in:
    // [2^DEPTH - 1, 2^(DEPTH+1) - 2].
    let first_leaf = 2_u64.pow(MERKLE_DEPTH as u32) - 1;
    let index_a = first_leaf;
    let index_b = first_leaf + 1;
    let data1 = [1u8; 32];
    let data2 = [2u8; 32];
    let data3 = [3u8; 32];
    let rec_hash = [7u8; 32];

    let txs = vec![
        ApplyTxTrace {
            writes: vec![ApplyLeafWrite {
                index: index_a.to_string(),
                data: data1,
            }],
            update_records: vec![ApplyRecordUpdate {
                hash: rec_hash,
                data: vec!["11".to_string(), "22".to_string()],
            }],
        },
        ApplyTxTrace {
            writes: vec![
                ApplyLeafWrite {
                    index: index_a.to_string(),
                    data: data2,
                },
                ApplyLeafWrite {
                    index: index_b.to_string(),
                    data: data3,
                },
            ],
            update_records: vec![ApplyRecordUpdate {
                hash: rec_hash,
                data: vec!["33".to_string(), "44".to_string()],
            }],
        },
    ];

    let expected_last_root = {
        let tmp = tempfile::tempdir().unwrap();
        let db = Rc::new(RefCell::new(RocksDB::new(tmp.path()).unwrap()));
        let mut mt = MongoMerkle::<MERKLE_DEPTH>::construct([0; 32], root, Some(db.clone()));

        let mut data_records: Vec<DataHashRecord> = Vec::new();
        let mut roots: Vec<[u8; 32]> = Vec::with_capacity(txs.len());
        for tx in txs.iter() {
            for rec in tx.update_records.iter() {
                let data = rec
                    .data
                    .iter()
                    .map(|x| u64::from_str_radix(x, 10).unwrap().to_le_bytes())
                    .flatten()
                    .collect::<Vec<u8>>();
                data_records.push(DataHashRecord {
                    hash: rec.hash,
                    data,
                });
            }
            for w in tx.writes.iter() {
                let index = u64::from_str_radix(&w.index, 10).unwrap();
                mt.update_leaf_data_with_proof(index, &w.data.to_vec())
                    .unwrap();
            }
            roots.push(mt.get_root_hash());
        }

        db.borrow_mut().set_data_records(&data_records).unwrap();
        *roots.last().unwrap()
    };

    let (final_root, db) = {
        let tmp = tempfile::tempdir().unwrap();
        let db = Rc::new(RefCell::new(RocksDB::new(tmp.path()).unwrap()));
        let mut mt = MongoMerkle::<MERKLE_DEPTH>::construct([0; 32], root, Some(db.clone()));

        let mut leaf_updates: Vec<(u64, [u8; 32])> = Vec::new();
        let mut data_records: Vec<DataHashRecord> = Vec::new();
        for tx in txs.iter() {
            for rec in tx.update_records.iter() {
                let data = rec
                    .data
                    .iter()
                    .map(|x| u64::from_str_radix(x, 10).unwrap().to_le_bytes())
                    .flatten()
                    .collect::<Vec<u8>>();
                data_records.push(DataHashRecord {
                    hash: rec.hash,
                    data,
                });
            }
            for w in tx.writes.iter() {
                let index = u64::from_str_radix(&w.index, 10).unwrap();
                leaf_updates.push((index, w.data));
            }
        }

        mt.update_leaves_batch(&leaf_updates).unwrap();
        db.borrow_mut().set_data_records(&data_records).unwrap();

        (mt.get_root_hash(), db)
    };

    assert_eq!(final_root, expected_last_root);

    let mongo_datahash = MongoDataHash::construct([0; 32], Some(db));
    let record = mongo_datahash.get_record(&rec_hash).unwrap().unwrap();
    let want = [
        33u64.to_le_bytes().to_vec(),
        44u64.to_le_bytes().to_vec(),
    ]
    .concat();
    assert_eq!(record.data, want);
}
