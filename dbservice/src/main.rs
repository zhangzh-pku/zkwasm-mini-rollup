use std::cell::RefCell;
use std::rc::Rc;

use jsonrpc_v2::{Data, Error, Params, Server};
use serde::Deserialize;
use serde::Serialize;
use zkwasm_host_circuits::host::datahash::DataHashRecord;
use zkwasm_host_circuits::host::datahash::MongoDataHash;
use zkwasm_host_circuits::host::db::RocksDB;
use zkwasm_host_circuits::host::db::TreeDB;
use zkwasm_host_circuits::host::merkle::MerkleTree;
use zkwasm_host_circuits::host::mongomerkle::{MongoMerkle, DEFAULT_HASH_VEC};
use zkwasm_host_circuits::constants::MERKLE_DEPTH;

use std::sync::OnceLock;
use std::time::Instant;

//use tokio::runtime::Runtime;

static mut DB: Option<Rc<RefCell<dyn TreeDB>>> = None;
static LOG_CSM_LATENCY: OnceLock<bool> = OnceLock::new();

fn log_csm_latency() -> bool {
    *LOG_CSM_LATENCY.get_or_init(|| {
        std::env::var("LOG_CSM_LATENCY")
            .ok()
            .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    })
}

#[derive(Clone, Deserialize, Serialize)]
pub struct UpdateLeafRequest {
    root: [u8; 32],
    data: [u8; 32],
    index: String, // u64 encoding
}
#[derive(Clone, Deserialize, Serialize)]
pub struct GetLeafRequest {
    root: [u8; 32],
    index: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct UpdateRecordRequest {
    hash: [u8; 32],
    data: Vec<String>, // vec u64 string
}
#[derive(Clone, Deserialize, Serialize)]
pub struct GetRecordRequest {
    hash: [u8; 32],
}

#[derive(Clone, Deserialize, Serialize)]
pub struct PingRequest {}

fn get_mt(root: [u8; 32]) -> MongoMerkle<32> {
    MongoMerkle::<MERKLE_DEPTH>::construct([0; 32], root, unsafe { DB.clone() })
}

async fn ping(Params(_request): Params<PingRequest>) -> Result<bool, Error> {
    Ok(true)
}

async fn update_leaf(Params(request): Params<UpdateLeafRequest>) -> Result<[u8; 32], Error> {
    let start = if log_csm_latency() {
        Some(Instant::now())
    } else {
        None
    };
    let index = u64::from_str_radix(request.index.as_str(), 10).unwrap();
    let hash = actix_web::web::block(move || {
        let mut mt = get_mt(request.root);
        mt.update_leaf_data_with_proof(index, &request.data.to_vec())
            .map_err(|e| {
                println!("update leaf data with proof error {:?}", e);
                Error::INTERNAL_ERROR
            })?;
        Ok(mt.get_root_hash())
    })
    .await
    .map_err(|_| Error::INTERNAL_ERROR)?;
    if let Some(start) = start {
        println!("time taken for update_leaf is {:?}", start.elapsed());
    }
    hash
}

async fn get_leaf(Params(request): Params<GetLeafRequest>) -> Result<[u8; 32], Error> {
    let start = if log_csm_latency() {
        Some(Instant::now())
    } else {
        None
    };
    let index = u64::from_str_radix(request.index.as_str(), 10).unwrap();
    let leaf = actix_web::web::block(move || {
        let mt = get_mt(request.root);
        let (leaf, _) = mt.get_leaf_with_proof(index).map_err(|e| {
            println!("get leaf error {:?}", e);
            Error::INTERNAL_ERROR
        })?;
        Ok(leaf)
    })
    .await
    .map_err(|_| Error::INTERNAL_ERROR)?;
    if let Some(start) = start {
        println!("time taken for get_leaf is {:?}", start.elapsed());
    }
    leaf.map(|l| {
        l.data.unwrap_or([0; 32])
    })
}
async fn update_record(Params(request): Params<UpdateRecordRequest>) -> Result<(), Error> {
    let _ = actix_web::web::block(move || {
        let mut mongo_datahash = MongoDataHash::construct([0; 32], unsafe { DB.clone() });
        mongo_datahash.update_record({
            DataHashRecord {
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
            }
        })
    })
    .await
    .map_err(|_| Error::INTERNAL_ERROR)?;
    Ok(())
}

async fn get_record(Params(request): Params<GetRecordRequest>) -> Result<Vec<String>, Error> {
    let datahashrecord = actix_web::web::block(move || {
        let mongo_datahash = MongoDataHash::construct([0; 32], unsafe { DB.clone() });
        mongo_datahash.get_record(&request.hash).unwrap()
    })
    .await
    .map_err(|_| Error::INTERNAL_ERROR)?;
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
    unsafe { DB = Some(Rc::new(RefCell::new(RocksDB::new(args.uri).unwrap()))) };
    let rpc = Server::new()
        .with_data(Data::new(String::from("Hello!")))
        .with_method("ping", ping)
        .with_method("update_leaf", update_leaf)
        .with_method("get_leaf", get_leaf)
        .with_method("update_record", update_record)
        .with_method("get_record", get_record)
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

    let root = DEFAULT_HASH_VEC[MERKLE_DEPTH];
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
