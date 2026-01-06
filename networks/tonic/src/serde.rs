//! Serialization and parsing functions for protobuf types.

use std::collections::HashMap;

use datafusion_dist::{
    cluster::NodeId,
    network::{StageInfo, TaskSetInfo},
    planner::{StageId, TaskId},
    runtime::TaskMetrics,
};
use uuid::Uuid;

use crate::protobuf;

// ============================================================================
// StageId serialization/parsing
// ============================================================================

pub fn serialize_stage_id(stage_id: StageId) -> protobuf::StageId {
    protobuf::StageId {
        job_id: stage_id.job_id.to_string(),
        stage: stage_id.stage,
    }
}

pub fn parse_stage_id(proto: protobuf::StageId) -> StageId {
    let job_id = Uuid::parse_str(&proto.job_id)
        .unwrap_or_else(|_| panic!("Failed to parse job id {} as uuid", proto.job_id));
    StageId {
        job_id,
        stage: proto.stage,
    }
}

// ============================================================================
// TaskId serialization/parsing
// ============================================================================

pub fn serialize_task_id(task_id: TaskId) -> protobuf::TaskId {
    protobuf::TaskId {
        job_id: task_id.job_id.to_string(),
        stage: task_id.stage,
        partition: task_id.partition,
    }
}

pub fn parse_task_id(proto: protobuf::TaskId) -> TaskId {
    let job_id = Uuid::parse_str(&proto.job_id)
        .unwrap_or_else(|_| panic!("Failed to parse job id {} as uuid", proto.job_id));
    TaskId {
        job_id,
        stage: proto.stage,
        partition: proto.partition,
    }
}

// ============================================================================
// StageInfo serialization/parsing
// ============================================================================

pub fn parse_stage_info(proto: protobuf::StageInfo) -> StageInfo {
    let assigned_partitions = proto
        .assigned_partitions
        .into_iter()
        .map(|p| p as usize)
        .collect();

    let task_set_infos = proto
        .task_set_infos
        .into_iter()
        .map(parse_task_set_info)
        .collect();

    StageInfo {
        create_at_ms: proto.created_at_ms,
        assigned_partitions,
        task_set_infos,
    }
}

pub fn serialize_stage_info(stage_id: StageId, stage_info: StageInfo) -> protobuf::StageInfo {
    let proto_stage_id = serialize_stage_id(stage_id);

    let proto_assigned_partitions = stage_info
        .assigned_partitions
        .into_iter()
        .map(|p| p as u32)
        .collect();

    let proto_task_set_infos = stage_info
        .task_set_infos
        .into_iter()
        .map(serialize_task_set_info)
        .collect();

    protobuf::StageInfo {
        stage_id: Some(proto_stage_id),
        created_at_ms: stage_info.create_at_ms,
        assigned_partitions: proto_assigned_partitions,
        task_set_infos: proto_task_set_infos,
    }
}

// ============================================================================
// TaskSetInfo serialization/parsing
// ============================================================================

pub fn parse_task_set_info(proto: protobuf::TaskSetInfo) -> TaskSetInfo {
    let mut dropped_partitions = HashMap::new();
    for proto_dropped_partition in proto.dropped_partitions {
        let partition = proto_dropped_partition.partition as usize;
        let metrics = parse_task_metrics(
            proto_dropped_partition
                .metrics
                .expect("task metrics is none"),
        );
        dropped_partitions.insert(partition, metrics);
    }
    TaskSetInfo {
        running_partitions: proto
            .running_partitions
            .into_iter()
            .map(|p| p as usize)
            .collect(),
        dropped_partitions,
    }
}

pub fn serialize_task_set_info(task_set_info: TaskSetInfo) -> protobuf::TaskSetInfo {
    let mut dropped_partitions = Vec::new();
    for (dropped_partition, task_metrics) in task_set_info.dropped_partitions {
        let serialized_metrics = serialize_task_metrics(task_metrics);
        dropped_partitions.push(protobuf::DroppedPartition {
            partition: dropped_partition as u32,
            metrics: Some(serialized_metrics),
        });
    }
    protobuf::TaskSetInfo {
        running_partitions: task_set_info
            .running_partitions
            .into_iter()
            .map(|p| p as u32)
            .collect(),
        dropped_partitions,
    }
}

// ============================================================================
// TaskMetrics serialization/parsing
// ============================================================================
pub fn parse_task_metrics(proto: protobuf::TaskMetrics) -> TaskMetrics {
    TaskMetrics {
        output_rows: proto.output_rows as usize,
        output_bytes: proto.output_bytes as usize,
        completed: proto.completed,
    }
}

pub fn serialize_task_metrics(task_metrics: TaskMetrics) -> protobuf::TaskMetrics {
    protobuf::TaskMetrics {
        output_rows: task_metrics.output_rows as u64,
        output_bytes: task_metrics.output_bytes as u64,
        completed: task_metrics.completed,
    }
}

// ============================================================================
// Task distribution HashMap<TaskId, NodeId> serialization/parsing
// ============================================================================

pub fn serialize_task_distribution(
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

pub fn parse_task_distribution(proto: protobuf::TaskDistribution) -> HashMap<TaskId, NodeId> {
    let mut task_distribution = HashMap::new();
    for task in proto.distribution {
        let (task_id, node_id) = parse_task_node(task);
        task_distribution.insert(task_id, node_id);
    }
    task_distribution
}

// ============================================================================
// NodeId serialization/parsing
// ============================================================================

pub fn parse_node_id(proto: protobuf::NodeId) -> NodeId {
    NodeId {
        host: proto.host,
        port: proto.port as u16,
    }
}

pub fn serialize_node_id(node_id: NodeId) -> protobuf::NodeId {
    protobuf::NodeId {
        host: node_id.host,
        port: node_id.port as u32,
    }
}

// ============================================================================
// (TaskId, NodeId) serialization/parsing
// ============================================================================

pub fn parse_task_node(proto: protobuf::TaskNode) -> (TaskId, NodeId) {
    let task_id = parse_task_id(proto.task_id.expect("task_id is none"));
    let node_id = parse_node_id(proto.node_id.expect("node_id is none"));
    (task_id, node_id)
}

pub fn serialize_task_node(task_id: TaskId, node_id: NodeId) -> protobuf::TaskNode {
    protobuf::TaskNode {
        task_id: Some(serialize_task_id(task_id)),
        node_id: Some(serialize_node_id(node_id)),
    }
}
