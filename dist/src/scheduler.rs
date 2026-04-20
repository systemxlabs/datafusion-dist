use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
};

use datafusion_catalog::memory::{DataSourceExec, MemorySourceConfig};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_physical_plan::{
    ExecutionPlan, ExecutionPlanProperties,
    coalesce_partitions::CoalescePartitionsExec,
    joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode},
    repartition::RepartitionExec,
};
use itertools::Itertools;

use crate::{
    DistError, DistResult,
    cluster::{NodeId, NodeState, NodeStatus},
    planner::{StageId, TaskId},
};

#[async_trait::async_trait]
pub trait DistScheduler: Debug + Send + Sync {
    async fn schedule(
        &self,
        local_node: &NodeId,
        node_states: &HashMap<NodeId, NodeState>,
        stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>>;
}

pub type AssignSelfFn = Box<dyn Fn(&Arc<dyn ExecutionPlan>) -> bool + Send + Sync>;

pub struct DefaultScheduler {
    assign_self: Option<AssignSelfFn>,
    memory_datasource_size_threshold: usize,
}

impl DefaultScheduler {
    pub fn new() -> Self {
        DefaultScheduler {
            assign_self: None,
            memory_datasource_size_threshold: 1024 * 1024,
        }
    }

    pub fn with_assign_self(mut self, assign_self: Option<AssignSelfFn>) -> Self {
        self.assign_self = assign_self;
        self
    }

    pub fn with_memory_datasource_size_threshold(mut self, threshold: usize) -> Self {
        self.memory_datasource_size_threshold = threshold;
        self
    }
}

impl Debug for DefaultScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultScheduler").finish()
    }
}

impl Default for DefaultScheduler {
    fn default() -> Self {
        DefaultScheduler::new()
    }
}

#[async_trait::async_trait]
impl DistScheduler for DefaultScheduler {
    async fn schedule(
        &self,
        local_node: &NodeId,
        node_states: &HashMap<NodeId, NodeState>,
        stage_plans: &HashMap<StageId, Arc<dyn ExecutionPlan>>,
    ) -> DistResult<HashMap<TaskId, NodeId>> {
        if let Some(state) = node_states.get(local_node)
            && matches!(state.status, NodeStatus::Terminating)
        {
            return Err(DistError::schedule(
                "Local node is in Terminating status, cannot schedule tasks",
            ));
        }

        // Filter out nodes that are in Terminating status
        let available_nodes: HashMap<NodeId, NodeState> = node_states
            .iter()
            .filter(|(_, state)| matches!(state.status, NodeStatus::Available))
            .map(|(id, state)| (id.clone(), state.clone()))
            .collect();

        if available_nodes.is_empty() {
            return Err(DistError::schedule("No nodes available for scheduling"));
        }

        // Maintain virtual loads across stages within a single schedule call
        let mut virtual_loads: HashMap<NodeId, u32> = available_nodes
            .iter()
            .map(|(id, state)| {
                (
                    id.clone(),
                    state.num_running_tasks + state.num_pending_tasks,
                )
            })
            .collect();

        let mut assignments = HashMap::new();

        for (stage_id, plan) in stage_plans.iter().sorted_by_key(|entry| entry.0) {
            if let Some(assign_self) = &self.assign_self
                && assign_self(plan)
            {
                assignments.extend(assign_stage_tasks_to_self(
                    stage_id.clone(),
                    plan,
                    local_node,
                ));
                continue;
            }

            if contains_large_memory_datasource(plan, self.memory_datasource_size_threshold) {
                assignments.extend(assign_stage_tasks_to_self(
                    stage_id.clone(),
                    plan,
                    local_node,
                ));
                continue;
            }

            if is_plan_fully_pipelined(plan) {
                let assignment =
                    assign_stage_tasks_to_all_nodes(stage_id.clone(), plan, &mut virtual_loads);
                assignments.extend(assignment);
            } else {
                let assignment =
                    assign_stage_all_tasks_to_node(stage_id.clone(), plan, &mut virtual_loads);
                assignments.extend(assignment);
            }
        }
        Ok(assignments)
    }
}

pub fn contains_large_memory_datasource(plan: &Arc<dyn ExecutionPlan>, threshold: usize) -> bool {
    let mut result = false;

    plan.apply(|node| {
        if let Some(datasource) = node.as_any().downcast_ref::<DataSourceExec>()
            && let Some(memory) = datasource
                .data_source()
                .as_any()
                .downcast_ref::<MemorySourceConfig>()
        {
            let size = memory
                .partitions()
                .iter()
                .map(|partition| {
                    partition
                        .iter()
                        .map(|batch| batch.get_array_memory_size())
                        .sum::<usize>()
                })
                .sum::<usize>();
            if size > threshold {
                result = true;
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("plan traversal should not fail");

    result
}

pub fn is_plan_fully_pipelined(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let mut fully_pipelined = true;
    plan.apply(|node| {
        let any = node.as_any();
        if any.is::<RepartitionExec>()
            || any.is::<CoalescePartitionsExec>()
            || any.is::<NestedLoopJoinExec>()
        {
            fully_pipelined = false;
        }
        if let Some(hash_join) = any.downcast_ref::<HashJoinExec>()
            && hash_join.partition_mode() == &PartitionMode::CollectLeft
        {
            fully_pipelined = false;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("plan traversal should not fail");

    fully_pipelined
}

fn assign_stage_tasks_to_self(
    stage_id: StageId,
    plan: &Arc<dyn ExecutionPlan>,
    local_node: &NodeId,
) -> HashMap<TaskId, NodeId> {
    let mut assignments = HashMap::new();
    let partition_count = plan.output_partitioning().partition_count();

    for partition in 0..partition_count {
        let task_id = stage_id.task_id(partition as u32);
        assignments.insert(task_id, local_node.clone());
    }

    assignments
}

fn assign_stage_tasks_to_all_nodes(
    stage_id: StageId,
    plan: &Arc<dyn ExecutionPlan>,
    virtual_loads: &mut HashMap<NodeId, u32>,
) -> HashMap<TaskId, NodeId> {
    let mut assignments = HashMap::new();
    let partition_count = plan.output_partitioning().partition_count();

    for partition in 0..partition_count {
        let task_id = stage_id.task_id(partition as u32);

        // Find node with min load and tie-break by NodeId
        let node_id = virtual_loads
            .iter()
            .min_by(|a, b| a.1.cmp(b.1).then_with(|| a.0.cmp(b.0)))
            .map(|(id, _)| id.clone())
            .expect("available nodes should not be empty");

        assignments.insert(task_id, node_id.clone());

        // Increment virtual load
        *virtual_loads.get_mut(&node_id).unwrap() += 1;
    }

    assignments
}

fn assign_stage_all_tasks_to_node(
    stage_id: StageId,
    plan: &Arc<dyn ExecutionPlan>,
    virtual_loads: &mut HashMap<NodeId, u32>,
) -> HashMap<TaskId, NodeId> {
    // Find node with min load and tie-break by NodeId
    let node_id = virtual_loads
        .iter()
        .min_by(|a, b| a.1.cmp(b.1).then_with(|| a.0.cmp(b.0)))
        .map(|(id, _)| id.clone())
        .expect("available nodes should not be empty");

    let mut assignments = HashMap::new();
    let partition_count = plan.output_partitioning().partition_count();

    for partition in 0..partition_count {
        let task_id = stage_id.task_id(partition as u32);
        assignments.insert(task_id, node_id.clone());
    }

    // Increment virtual load for the entire stage (considering all partitions as load)
    *virtual_loads.get_mut(&node_id).unwrap() += partition_count as u32;

    assignments
}

pub struct DisplayableTaskDistribution<'a>(pub &'a HashMap<TaskId, NodeId>);

impl Display for DisplayableTaskDistribution<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut node_tasks = HashMap::new();

        for (task_id, node_id) in self.0.iter() {
            node_tasks
                .entry(node_id)
                .or_insert_with(Vec::new)
                .push(task_id);
        }

        let mut node_dist = Vec::new();
        for (node_id, tasks) in node_tasks
            .into_iter()
            .sorted_by_key(|(node_id, _)| *node_id)
        {
            let stage_groups = tasks.into_iter().into_group_map_by(|task_id| task_id.stage);
            let stage_groups_display = stage_groups
                .into_iter()
                .sorted_by_key(|(stage, _)| *stage)
                .map(|(stage, tasks)| {
                    format!(
                        "{stage}/{}",
                        if tasks.len() == 1 {
                            format!("{}", tasks[0].partition)
                        } else {
                            format!(
                                "{{{}}}",
                                tasks
                                    .into_iter()
                                    .sorted()
                                    .map(|t| t.partition.to_string())
                                    .collect::<Vec<String>>()
                                    .join(",")
                            )
                        }
                    )
                })
                .collect::<Vec<String>>()
                .join(",");
            node_dist.push(format!("{stage_groups_display}->{node_id}",));
        }

        write!(f, "{}", node_dist.join(", "))
    }
}
