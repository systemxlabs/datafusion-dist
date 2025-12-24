use std::{collections::HashMap, fmt::Debug, sync::Arc};

use datafusion::{
    arrow::datatypes::SchemaRef,
    error::DataFusionError,
    execution::FunctionRegistry,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        ExecutionPlan, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
    },
    prelude::SessionContext,
};
use datafusion_dist::{
    cluster::NodeId, physical_plan::ProxyExec, planner::TaskId, runtime::DistRuntime,
};
use datafusion_proto::{
    convert_required,
    physical_plan::{PhysicalExtensionCodec, from_proto::parse_protobuf_partitioning},
};
use datafusion_proto::{physical_plan::to_proto::serialize_partitioning, protobuf::proto_error};
use prost::Message;

use crate::{
    protobuf::{
        self, DistPhysicalPlanNode, ProxyExecNode, dist_physical_plan_node::DistPhysicalPlanType,
    },
    serde::{parse_stage_id, parse_task_id, serialize_stage_id, serialize_task_id},
};

#[derive(Debug)]
pub struct DistPhysicalExtensionEncoder {
    pub app_extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl PhysicalExtensionCodec for DistPhysicalExtensionEncoder {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "DistPhysicalExtensionEncoder::try_decode is not implemented".to_string(),
        ))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        if let Some(exec) = node.as_any().downcast_ref::<ProxyExec>() {
            let proto_stage_id = serialize_stage_id(exec.delegated_stage_id);
            let proto_partitioning = serialize_partitioning(
                &exec.delegated_plan_properties.partitioning,
                self.app_extension_codec.as_ref(),
            )?;
            let proto_task_distribution =
                serialize_task_distribution(&exec.delegated_task_distribution);

            let proto = DistPhysicalPlanNode {
                dist_physical_plan_type: Some(DistPhysicalPlanType::Proxy(ProxyExecNode {
                    delegated_stage_id: Some(proto_stage_id),
                    delegated_plan_name: exec.delegated_plan_name.clone(),
                    delegated_task_distribution: Some(proto_task_distribution),
                    schema: Some(exec.schema().as_ref().try_into()?),
                    partitioning: Some(proto_partitioning),
                })),
            };
            proto.encode(buf).map_err(|e| {
                DataFusionError::Internal(format!("failed to encode proxy exec plan: {e:?}"))
            })?;

            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "DistPhysicalExtensionEncoder does not support plan {}",
                node.name()
            )))
        }
    }
}

pub struct DistPhysicalExtensionDecoder {
    pub runtime: DistRuntime,
    pub ctx: SessionContext,
    pub app_extension_codec: Arc<dyn PhysicalExtensionCodec>,
}

impl Debug for DistPhysicalExtensionDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DistPhysicalCodec")
            .field("runtime", &self.runtime)
            .field("app_extension_codec", &self.app_extension_codec)
            .finish()
    }
}

impl PhysicalExtensionCodec for DistPhysicalExtensionDecoder {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let dist_node = DistPhysicalPlanNode::decode(buf).map_err(|e| {
            DataFusionError::Internal(format!("Failed to decode dist physical plan node: {e:?}"))
        })?;
        let dist_plan = dist_node.dist_physical_plan_type.ok_or_else(|| {
            DataFusionError::Internal(
                "Failed to decode dist physical plan node due to physical plan type is none"
                    .to_string(),
            )
        })?;

        match dist_plan {
            DistPhysicalPlanType::Proxy(proto) => {
                let delegated_stage_id = parse_stage_id(
                    proto
                        .delegated_stage_id
                        .expect("stage_id should not be null"),
                );
                let delegated_task_distribution = parse_task_distribution(
                    proto
                        .delegated_task_distribution
                        .expect("task_distribution is none"),
                );
                let delegated_plan_schema: SchemaRef = Arc::new(convert_required!(proto.schema)?);
                let partitioning = parse_protobuf_partitioning(
                    proto.partitioning.as_ref(),
                    &self.ctx,
                    &delegated_plan_schema,
                    self.app_extension_codec.as_ref(),
                )?
                .expect("partition is none");

                // Todo EmissionType / Boundedness protobuf
                let delegated_plan_properties = PlanProperties::new(
                    EquivalenceProperties::new(delegated_plan_schema),
                    partitioning,
                    EmissionType::Incremental,
                    Boundedness::Bounded,
                );

                let proxy_exec = ProxyExec {
                    delegated_stage_id,
                    delegated_plan_name: proto.delegated_plan_name,
                    delegated_plan_properties,
                    delegated_task_distribution,
                    runtime: self.runtime.clone(),
                };

                Ok(Arc::new(proxy_exec))
            }
        }
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        Err(DataFusionError::NotImplemented(
            "DistPhysicalExtensionDecoder::try_encode is not implemented".to_string(),
        ))
    }
}

fn parse_task_distribution(proto: protobuf::TaskDistribution) -> HashMap<TaskId, NodeId> {
    let mut task_distribution = HashMap::new();
    for task in proto.distribution {
        let (task_id, node_id) = parse_task_node(task);
        task_distribution.insert(task_id, node_id);
    }
    task_distribution
}

fn parse_task_node(proto: protobuf::TaskNode) -> (TaskId, NodeId) {
    let task_id = parse_task_id(proto.task_id.expect("task_id is none"));
    let node_id = parse_node_id(proto.node_id.expect("node_id is none"));
    (task_id, node_id)
}

fn parse_node_id(proto: protobuf::NodeId) -> NodeId {
    NodeId {
        host: proto.host,
        port: proto.port as u16,
    }
}

fn serialize_task_distribution(
    task_distribution: &HashMap<TaskId, NodeId>,
) -> protobuf::TaskDistribution {
    let mut task_nodes = Vec::new();
    for (task_id, node_id) in task_distribution {
        let task_node = serialize_task_node(*task_id, node_id.clone());
        task_nodes.push(task_node);
    }
    protobuf::TaskDistribution {
        distribution: task_nodes,
    }
}

fn serialize_node_id(node_id: NodeId) -> protobuf::NodeId {
    protobuf::NodeId {
        host: node_id.host,
        port: node_id.port as u32,
    }
}

fn serialize_task_node(task_id: TaskId, node_id: NodeId) -> protobuf::TaskNode {
    protobuf::TaskNode {
        task_id: Some(serialize_task_id(task_id)),
        node_id: Some(serialize_node_id(node_id)),
    }
}
