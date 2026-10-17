use crate::kvraft::v1::{
    DeleteRequest, GetRequest, KvResponse, SetRequest, StatusRequest, Value as ProtoValue,
    kv_store_server::KvStore, value::Data,
};
use crate::lsm::wal::log_record::Value;
use crate::runtime::service::{ServiceOperation, ServiceRequest, ServiceResponse};

use tokio::sync::{mpsc::UnboundedSender, oneshot};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

pub struct GrpcKVStore {
    service_tx: UnboundedSender<ServiceRequest>,
}

impl GrpcKVStore {
    pub fn new(service_tx: UnboundedSender<ServiceRequest>) -> Self {
        Self { service_tx }
    }

    async fn dispatch(&self, operation: ServiceOperation) -> Result<ServiceResponse, Status> {
        let (response_tx, response_rx) = oneshot::channel();

        self.service_tx
            .send(ServiceRequest {
                operation,
                response_tx,
            })
            .map_err(|_| Status::unavailable("Raft runtime unavailable"))?;

        response_rx
            .await
            .map_err(|_| Status::internal("Raft runtime dropped the response"))
    }
}

fn from_proto(value: ProtoValue) -> Result<Value, Status> {
    match value.data {
        Some(Data::IntValue(v)) => Ok(Value::Int(v)),
        Some(Data::FloatValue(v)) => {
            if !v.is_finite() {
                return Err(Status::invalid_argument("float value must be finite"));
            }
            Ok(Value::Float(v))
        }
        Some(Data::StringValue(v)) => Ok(Value::String(v)),
        None => Err(Status::invalid_argument("value is required")),
    }
}

fn to_proto(value: Value) -> ProtoValue {
    let data = match value {
        Value::Int(v) => Data::IntValue(v),
        Value::Float(v) => Data::FloatValue(v),
        Value::String(v) => Data::StringValue(v),
    };

    ProtoValue { data: Some(data) }
}

fn to_response(response: ServiceResponse) -> KvResponse {
    KvResponse {
        success: response.success,
        value: response.value.map(to_proto),
        message: response.message,
        leader_id: response.leader_id,
        term: response.term,
    }
}

#[tonic::async_trait]
impl KvStore for GrpcKVStore {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<KvResponse>, Status> {
        let key = request.into_inner().key;

        let result = self.dispatch(ServiceOperation::Get { key }).await?;

        Ok(Response::new(to_response(result)))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<KvResponse>, Status> {
        let request = request.into_inner();

        let value = from_proto(
            request
                .value
                .ok_or_else(|| Status::invalid_argument("value is required"))?,
        )?;

        let result = self
            .dispatch(ServiceOperation::Set {
                key: request.key,
                value,
            })
            .await?;

        Ok(Response::new(to_response(result)))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<KvResponse>, Status> {
        let key = request.into_inner().key;

        let result = self.dispatch(ServiceOperation::Delete { key }).await?;

        Ok(Response::new(to_response(result)))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> Result<Response<KvResponse>, Status> {
        let result = self.dispatch(ServiceOperation::Status).await?;

        Ok(Response::new(to_response(result)))
    }
}

pub async fn run_grpc_listener(
    addr: String,
    service_tx: UnboundedSender<ServiceRequest>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = addr.parse()?;
    let service = GrpcKVStore::new(service_tx);

    println!("[gRPC] Listening on {addr}");

    Server::builder()
        .add_service(crate::kvraft::v1::kv_store_server::KvStoreServer::new(
            service,
        ))
        .serve(addr)
        .await?;

    Ok(())
}
