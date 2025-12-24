//! Serialization and parsing functions for protobuf types.

use datafusion::arrow::{
    self,
    array::RecordBatch,
    error::ArrowError,
    ipc::{reader::StreamReader, writer::StreamWriter},
};
use datafusion_dist::{
    DistError, DistResult,
    network::{StageInfo, TaskSetInfo},
    planner::{StageId, TaskId},
};
use tonic::Status;
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
        assigned_partitions: proto_assigned_partitions,
        task_set_infos: proto_task_set_infos,
    }
}

// ============================================================================
// TaskSetInfo serialization/parsing
// ============================================================================

pub fn parse_task_set_info(proto: protobuf::TaskSetInfo) -> TaskSetInfo {
    TaskSetInfo {
        running_partitions: proto
            .running_partitions
            .into_iter()
            .map(|p| p as usize)
            .collect(),
        completed_partitions: proto
            .completed_partitions
            .into_iter()
            .map(|p| p as usize)
            .collect(),
    }
}

pub fn serialize_task_set_info(task_set_info: TaskSetInfo) -> protobuf::TaskSetInfo {
    protobuf::TaskSetInfo {
        running_partitions: task_set_info
            .running_partitions
            .into_iter()
            .map(|p| p as u32)
            .collect(),
        completed_partitions: task_set_info
            .completed_partitions
            .into_iter()
            .map(|p| p as u32)
            .collect(),
    }
}

// ============================================================================
// Result<RecordBatch, Status> serialization/parsing
// ============================================================================

pub fn parse_record_batch_res(
    proto_res: Result<protobuf::RecordBatch, Status>,
) -> DistResult<RecordBatch> {
    let proto_batch = proto_res.map_err(|e| DistError::network(Box::new(e)))?;
    let reader = StreamReader::try_new(proto_batch.data.as_slice(), None)?;
    let mut batches = reader.into_iter().collect::<Result<Vec<_>, ArrowError>>()?;
    if batches.len() == 1 {
        return Ok(batches.remove(0));
    }
    let first_batch = batches
        .first()
        .ok_or_else(|| DistError::internal("No batch found in stream reader"))?;
    let batch = arrow::compute::concat_batches(first_batch.schema_ref(), &batches)?;
    Ok(batch)
}

#[allow(clippy::result_large_err)]
pub fn serialize_record_batch_result(
    batch_res: DistResult<RecordBatch>,
) -> Result<protobuf::RecordBatch, Status> {
    let batch = batch_res.map_err(|e| Status::from_error(Box::new(e)))?;

    let mut data = vec![];
    let mut writer = StreamWriter::try_new(&mut data, batch.schema_ref())
        .map_err(|e| Status::internal(format!("Failed to build stream writer: {e}")))?;
    writer.write(&batch).map_err(|e| {
        Status::internal(format!("Failed to write batch through stream writer: {e}"))
    })?;
    writer
        .finish()
        .map_err(|e| Status::internal(format!("Failed to finish stream writer: {e}")))?;
    Ok(protobuf::RecordBatch { data })
}
