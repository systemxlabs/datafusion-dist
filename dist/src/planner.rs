use std::{collections::HashMap, sync::Arc};

use datafusion::physical_plan::ExecutionPlan;
use uuid::Uuid;

use crate::{DistResult, network::StageId};

pub trait DistPlanner {
    fn plan_stages(
        &self,
        job_id: Uuid,
        plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<HashMap<StageId, Arc<dyn ExecutionPlan>>>;
}

pub struct DefaultPlanner;

impl DistPlanner for DefaultPlanner {
    fn plan_stages(
        &self,
        _job_id: Uuid,
        _plan: Arc<dyn ExecutionPlan>,
    ) -> DistResult<HashMap<StageId, Arc<dyn ExecutionPlan>>> {
        todo!()
    }
}
