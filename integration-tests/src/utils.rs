use std::{collections::BTreeMap, fmt::Display, sync::Arc};

use datafusion::physical_plan::{ExecutionPlan, display::DisplayableExecutionPlan};
use datafusion_dist::planner::{DefaultPlanner, DistPlanner, StageId};
use uuid::Uuid;

use crate::data::build_session_context;

pub async fn assert_planner_output(sql: &str, expected_stage_plans: &str) {
    let ctx = build_session_context();
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let dist_planner = DefaultPlanner {};
    let stage_plans = dist_planner.plan_stages(Uuid::new_v4(), plan).unwrap();
    let stage_plans = stage_plans.into_iter().collect::<BTreeMap<_, _>>();
    let actual = TestDisplayableStagePlans(&stage_plans).to_string();
    println!("Planner output: {actual}");
    assert_eq!(actual, expected_stage_plans);
}

pub struct TestDisplayableStagePlans<'a>(pub &'a BTreeMap<StageId, Arc<dyn ExecutionPlan>>);

impl Display for TestDisplayableStagePlans<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (stage_id, plan) in self.0.iter() {
            writeln!(f, "===============Stage {}===============", stage_id.stage)?;
            write!(
                f,
                "{}",
                DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
            )?;
        }
        Ok(())
    }
}
