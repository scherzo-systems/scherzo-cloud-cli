use std::collections::BTreeMap;

use super::StepProjection;

pub(super) const MAX_GRAPH_LANES: usize = 6;

const OVERFLOW_LANE: usize = MAX_GRAPH_LANES - 1;
const UP: u8 = 1 << 0;
const DOWN: u8 = 1 << 1;
const LEFT: u8 = 1 << 2;
const RIGHT: u8 = 1 << 3;
const VERTICAL: u8 = UP | DOWN;
const HORIZONTAL: u8 = LEFT | RIGHT;
const TOP_LEFT: u8 = DOWN | RIGHT;
const TOP_RIGHT: u8 = DOWN | LEFT;
const BOTTOM_LEFT: u8 = UP | RIGHT;
const BOTTOM_RIGHT: u8 = UP | LEFT;
const VERTICAL_RIGHT: u8 = UP | DOWN | RIGHT;
const VERTICAL_LEFT: u8 = UP | DOWN | LEFT;
const HORIZONTAL_DOWN: u8 = DOWN | LEFT | RIGHT;
const HORIZONTAL_UP: u8 = UP | LEFT | RIGHT;
const CROSS: u8 = UP | DOWN | LEFT | RIGHT;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct DagLayout {
    rows: Vec<DagRow>,
    gutter_width: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct DagRow {
    pub(super) before_node: String,
    pub(super) after_node: String,
    pub(super) below_node: String,
}

#[derive(Clone, Copy)]
struct ActiveLane {
    target: usize,
}

#[derive(Clone, Copy)]
enum Segment {
    Upper { from: usize, to: usize },
    Lower { from: usize, to: usize },
}

struct LogicalRow {
    node_lane: usize,
    segments: Vec<Segment>,
}

impl DagLayout {
    pub(super) fn for_steps<Step: StepProjection>(steps: &[Step]) -> Self {
        Self::new(
            steps
                .iter()
                .map(|step| (step.id(), step.definition().direct_dependencies())),
        )
    }

    fn new<'a>(nodes: impl IntoIterator<Item = (&'a str, &'a [String])>) -> Self {
        let nodes = nodes.into_iter().collect::<Vec<_>>();
        let indexes = nodes
            .iter()
            .enumerate()
            .map(|(index, (id, _))| (*id, index))
            .collect::<BTreeMap<_, _>>();
        let direct_parents = nodes
            .iter()
            .map(|(_, dependencies)| {
                dependencies
                    .iter()
                    .filter_map(|dependency| indexes.get(dependency.as_str()).copied())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let mut ancestors = vec![vec![false; nodes.len()]; nodes.len()];
        for (node, parents) in direct_parents.iter().enumerate() {
            let (earlier, current_and_later) = ancestors.split_at_mut(node);
            let current = &mut current_and_later[0];
            for parent in parents {
                debug_assert!(
                    *parent < node,
                    "DAG presentation order must place dependencies before dependents"
                );
                if let Some(parent_ancestors) = earlier.get(*parent) {
                    for (reachable, parent_reachable) in
                        current.iter_mut().zip(parent_ancestors.iter())
                    {
                        *reachable |= *parent_reachable;
                    }
                }
                current[*parent] = true;
            }
        }
        let mut children = vec![Vec::new(); nodes.len()];
        for (child, parents) in direct_parents.iter().enumerate() {
            for parent in parents {
                let redundant = parents
                    .iter()
                    .any(|other| other != parent && ancestors[*other].get(*parent) == Some(&true));
                if !redundant {
                    children[*parent].push(child);
                }
            }
        }

        let mut active = Vec::<Option<ActiveLane>>::new();
        let mut overflow_targets = BTreeMap::<usize, usize>::new();
        let mut overflow = false;
        let mut maximum_lane_count = 1_usize;
        let mut rows = Vec::with_capacity(nodes.len());
        for (node, outgoing) in children.iter().enumerate() {
            let incoming = active
                .iter()
                .enumerate()
                .filter_map(|(lane, active)| {
                    active
                        .is_some_and(|active| active.target == node)
                        .then_some(lane)
                })
                .collect::<Vec<_>>();
            let overflow_incoming = overflow_targets.remove(&node).is_some();
            let mut node_lane = incoming
                .first()
                .copied()
                .or_else(|| overflow_incoming.then_some(OVERFLOW_LANE))
                .or_else(|| active.iter().position(Option::is_none))
                .unwrap_or(active.len());
            if node_lane >= MAX_GRAPH_LANES {
                collapse_overflow_lane(&mut active, &mut overflow_targets);
                overflow = true;
                node_lane = OVERFLOW_LANE;
            }
            maximum_lane_count = maximum_lane_count.max(node_lane.saturating_add(1));

            let mut segments = Vec::new();
            for (lane, active_lane) in active.iter().enumerate() {
                let Some(active_lane) = active_lane else {
                    continue;
                };
                if active_lane.target == node {
                    segments.push(Segment::Upper {
                        from: lane,
                        to: node_lane,
                    });
                } else {
                    segments.push(Segment::Upper {
                        from: lane,
                        to: lane,
                    });
                    segments.push(Segment::Lower {
                        from: lane,
                        to: lane,
                    });
                }
            }
            if overflow && (overflow_incoming || !overflow_targets.is_empty()) {
                segments.push(Segment::Upper {
                    from: OVERFLOW_LANE,
                    to: if overflow_incoming {
                        node_lane
                    } else {
                        OVERFLOW_LANE
                    },
                });
            }

            let mut next = active.clone();
            for lane in incoming {
                next[lane] = None;
            }
            if node_lane < overflow_lane_start(overflow) {
                while next.len() <= node_lane {
                    next.push(None);
                }
            }
            let mut search_from = node_lane;
            let mut hidden_outgoing = false;
            for (outgoing_index, target) in outgoing.iter().copied().enumerate() {
                let regular_lane_count = overflow_lane_start(overflow);
                let lane = if outgoing_index == 0 {
                    node_lane
                } else {
                    next.iter()
                        .enumerate()
                        .take(regular_lane_count)
                        .skip(search_from.saturating_add(1))
                        .find_map(|(lane, active)| active.is_none().then_some(lane))
                        .or_else(|| {
                            (next.len() < regular_lane_count && next.len() > search_from)
                                .then_some(next.len())
                        })
                        .unwrap_or(regular_lane_count)
                };
                if lane < regular_lane_count {
                    while next.len() <= lane {
                        next.push(None);
                    }
                    next[lane] = Some(ActiveLane { target });
                    segments.push(Segment::Lower {
                        from: node_lane,
                        to: lane,
                    });
                    search_from = lane;
                } else {
                    if !overflow {
                        collapse_overflow_lane(&mut next, &mut overflow_targets);
                        overflow = true;
                    }
                    add_overflow_target(&mut overflow_targets, target);
                    hidden_outgoing = true;
                    search_from = OVERFLOW_LANE;
                }
            }
            if overflow && !overflow_targets.is_empty() {
                segments.push(Segment::Lower {
                    from: if hidden_outgoing {
                        node_lane
                    } else {
                        OVERFLOW_LANE
                    },
                    to: OVERFLOW_LANE,
                });
            }
            while next.last().is_some_and(Option::is_none) {
                next.pop();
            }
            maximum_lane_count = maximum_lane_count.max(next.len());
            if overflow {
                maximum_lane_count = MAX_GRAPH_LANES;
            }
            debug_assert!(
                segments.len() <= MAX_GRAPH_LANES.saturating_mul(2).saturating_add(1),
                "DAG rows must retain only bounded connector segments"
            );

            rows.push(LogicalRow {
                node_lane,
                segments,
            });
            active = next;
        }

        let physical_lane_count = maximum_lane_count.min(MAX_GRAPH_LANES);
        let gutter_width = physical_lane_count.saturating_mul(2).saturating_sub(1);
        let rows = rows
            .into_iter()
            .map(|row| project_row(row, physical_lane_count, overflow))
            .collect();
        Self { rows, gutter_width }
    }

    pub(super) fn rows(&self) -> &[DagRow] {
        &self.rows
    }

    pub(super) const fn gutter_width(&self) -> usize {
        self.gutter_width
    }
}

const fn overflow_lane_start(overflow: bool) -> usize {
    if overflow {
        OVERFLOW_LANE
    } else {
        MAX_GRAPH_LANES
    }
}

fn collapse_overflow_lane(
    active: &mut Vec<Option<ActiveLane>>,
    overflow_targets: &mut BTreeMap<usize, usize>,
) {
    let collapsed = active.get(OVERFLOW_LANE).copied().flatten();
    active.truncate(OVERFLOW_LANE);
    if let Some(active_lane) = collapsed {
        add_overflow_target(overflow_targets, active_lane.target);
    }
}

fn add_overflow_target(overflow_targets: &mut BTreeMap<usize, usize>, target: usize) {
    let count = overflow_targets.entry(target).or_default();
    *count = count.saturating_add(1);
}

fn project_row(row: LogicalRow, physical_lane_count: usize, overflow: bool) -> DagRow {
    let physical_lane = |lane: usize| {
        if overflow {
            lane.min(physical_lane_count.saturating_sub(1))
        } else {
            lane
        }
    };
    let gutter_width = physical_lane_count.saturating_mul(2).saturating_sub(1);
    let mut upper_cells = vec![0_u8; gutter_width];
    let mut lower_cells = vec![0_u8; gutter_width];
    let overflow_boundary = physical_lane_count.saturating_sub(1);
    let mut upper_overflow_active = overflow && row.node_lane >= overflow_boundary;
    let mut lower_overflow_active = false;
    for segment in row.segments {
        let (from, to) = match segment {
            Segment::Upper { from, to } | Segment::Lower { from, to } => (from, to),
        };
        let overflow_segment = overflow && (from >= overflow_boundary || to >= overflow_boundary);
        let from = physical_lane(from);
        let to = physical_lane(to);
        match segment {
            Segment::Upper { .. } => {
                upper_overflow_active |= overflow_segment;
                draw_upper(&mut upper_cells, from, to);
            }
            Segment::Lower { .. } => {
                lower_overflow_active |= overflow_segment;
                draw_lower(&mut lower_cells, from, to);
            }
        }
    }

    let upper_connectors = connector_row(upper_cells, physical_lane_count, upper_overflow_active);
    let lower_connectors = connector_row(lower_cells, physical_lane_count, lower_overflow_active);
    let node_column = physical_lane(row.node_lane).saturating_mul(2);
    DagRow {
        before_node: upper_connectors[..node_column].iter().collect(),
        after_node: upper_connectors[node_column.saturating_add(1)..]
            .iter()
            .collect(),
        below_node: lower_connectors.iter().collect(),
    }
}

fn connector_row(cells: Vec<u8>, physical_lane_count: usize, overflow_active: bool) -> Vec<char> {
    let mut connectors = cells.into_iter().map(connector_glyph).collect::<Vec<_>>();
    if overflow_active {
        let overflow_column = physical_lane_count.saturating_sub(1).saturating_mul(2);
        connectors[overflow_column] = '┊';
        if overflow_column != 0 {
            connectors[overflow_column - 1] = '…';
        }
    }
    connectors
}

fn draw_upper(cells: &mut [u8], from_lane: usize, to_lane: usize) {
    let from = from_lane.saturating_mul(2);
    let to = to_lane.saturating_mul(2);
    if from == to {
        cells[from] |= UP;
        return;
    }
    draw_horizontal(cells, from, to);
    cells[from] |= UP;
}

fn draw_lower(cells: &mut [u8], from_lane: usize, to_lane: usize) {
    let from = from_lane.saturating_mul(2);
    let to = to_lane.saturating_mul(2);
    if from == to {
        cells[to] |= DOWN;
        return;
    }
    draw_horizontal(cells, from, to);
    cells[to] |= DOWN;
}

fn draw_horizontal(cells: &mut [u8], from: usize, to: usize) {
    let (left, right) = if from < to { (from, to) } else { (to, from) };
    cells[left] |= RIGHT;
    cells[right] |= LEFT;
    for cell in &mut cells[left.saturating_add(1)..right] {
        *cell |= LEFT | RIGHT;
    }
}

fn connector_glyph(directions: u8) -> char {
    match directions {
        0 => ' ',
        VERTICAL => '│',
        HORIZONTAL => '─',
        TOP_LEFT => '┌',
        TOP_RIGHT => '┐',
        BOTTOM_LEFT => '└',
        BOTTOM_RIGHT => '┘',
        VERTICAL_RIGHT => '├',
        VERTICAL_LEFT => '┤',
        HORIZONTAL_DOWN => '┬',
        HORIZONTAL_UP => '┴',
        CROSS => '┼',
        _ if directions & VERTICAL != 0 => '│',
        _ => '─',
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout(nodes: &[(&str, &[&str])]) -> DagLayout {
        let dependencies = nodes
            .iter()
            .map(|(id, dependencies)| {
                (
                    *id,
                    dependencies
                        .iter()
                        .map(|dependency| (*dependency).to_owned())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        DagLayout::new(
            dependencies
                .iter()
                .map(|(id, dependencies)| (*id, dependencies.as_slice())),
        )
    }

    fn node_row(layout: &DagLayout, index: usize) -> String {
        let row = &layout.rows[index];
        format!("{}●{}", row.before_node, row.after_node)
    }

    fn connector_row(layout: &DagLayout, index: usize) -> &str {
        &layout.rows[index].below_node
    }

    #[test]
    fn chains_and_independent_roots_keep_quiet_lanes() {
        let chain = layout(&[("a", &[]), ("b", &["a"]), ("c", &["b"])]);
        assert_eq!(chain.gutter_width, 1);
        assert_eq!(
            (0..3)
                .map(|index| node_row(&chain, index))
                .collect::<Vec<_>>(),
            ["●", "●", "●"]
        );
        assert_eq!(connector_row(&chain, 0), "│");
        assert_eq!(connector_row(&chain, 1), "│");
        assert_eq!(connector_row(&chain, 2), " ");

        let independent = layout(&[("root", &[]), ("other", &[]), ("child", &["root"])]);
        assert_eq!(independent.gutter_width, 3);
        assert_eq!(node_row(&independent, 1), "│ ●");
        assert_eq!(connector_row(&independent, 1), "│  ");
        assert_eq!(node_row(&independent, 2), "●  ");
    }

    #[test]
    fn transitively_redundant_edges_do_not_open_lanes() {
        let layout = layout(&[
            ("prepare", &[]),
            ("decompose", &["prepare"]),
            ("validate", &["prepare", "decompose"]),
            ("publish", &["prepare", "validate"]),
        ]);

        assert_eq!(layout.gutter_width, 1);
        assert_eq!(
            (0..4)
                .map(|index| node_row(&layout, index))
                .collect::<Vec<_>>(),
            ["●", "●", "●", "●"]
        );
        assert_eq!(connector_row(&layout, 0), "│");
        assert_eq!(connector_row(&layout, 1), "│");
        assert_eq!(connector_row(&layout, 2), "│");
        assert_eq!(connector_row(&layout, 3), " ");
    }

    #[test]
    fn fan_out_uses_presentation_order_for_lane_ties() {
        let layout = layout(&[
            ("root", &[]),
            ("first", &["root"]),
            ("second", &["root"]),
            ("third", &["root"]),
        ]);

        assert_eq!(node_row(&layout, 0), "●    ");
        assert_eq!(connector_row(&layout, 0), "┌─┬─┐");
        assert_eq!(node_row(&layout, 1), "● │ │");
        assert_eq!(connector_row(&layout, 1), "  │ │");
        assert_eq!(node_row(&layout, 2), "  ● │");
        assert_eq!(connector_row(&layout, 2), "    │");
        assert_eq!(node_row(&layout, 3), "    ●");
    }

    #[test]
    fn fan_in_and_diamond_close_into_the_dependent_lane() {
        let fan_in = layout(&[("left", &[]), ("right", &[]), ("join", &["right", "left"])]);
        assert_eq!(node_row(&fan_in, 0), "●  ");
        assert_eq!(node_row(&fan_in, 1), "│ ●");
        assert_eq!(node_row(&fan_in, 2), "●─┘");

        let diamond = layout(&[
            ("root", &[]),
            ("left", &["root"]),
            ("right", &["root"]),
            ("join", &["right", "left"]),
        ]);
        assert_eq!(node_row(&diamond, 0), "●  ");
        assert_eq!(connector_row(&diamond, 0), "┌─┐");
        assert_eq!(node_row(&diamond, 1), "● │");
        assert_eq!(connector_row(&diamond, 1), "│ │");
        assert_eq!(node_row(&diamond, 2), "│ ●");
        assert_eq!(node_row(&diamond, 3), "●─┘");
    }

    #[test]
    fn excess_fan_out_collapses_into_an_explicit_bounded_lane() {
        let layout = layout(&[
            ("root", &[]),
            ("one", &["root"]),
            ("two", &["root"]),
            ("three", &["root"]),
            ("four", &["root"]),
            ("five", &["root"]),
            ("six", &["root"]),
            ("seven", &["root"]),
        ]);

        assert_eq!(layout.gutter_width, MAX_GRAPH_LANES * 2 - 1);
        assert!(connector_row(&layout, 0).contains("…┊"));
        assert_eq!(node_row(&layout, 6).chars().count(), layout.gutter_width);
        assert!(node_row(&layout, 6).ends_with('●'));
        assert!(node_row(&layout, 7).ends_with('●'));
    }

    #[test]
    fn maximum_dense_graph_reduces_to_a_bounded_chain() {
        const STEP_COUNT: usize = 256;

        let nodes = (0..STEP_COUNT)
            .map(|node| {
                (
                    format!("step-{node}"),
                    (0..node)
                        .map(|dependency| format!("step-{dependency}"))
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let layout = DagLayout::new(
            nodes
                .iter()
                .map(|(id, dependencies)| (id.as_str(), dependencies.as_slice())),
        );

        assert_eq!(layout.rows.len(), STEP_COUNT);
        assert_eq!(layout.gutter_width, 1);
        assert!(layout.rows.iter().all(|graph_row| {
            graph_row.before_node.chars().count() + 1 + graph_row.after_node.chars().count()
                == layout.gutter_width
                && graph_row.below_node.chars().count() == layout.gutter_width
        }));
        assert!(
            layout.rows[..STEP_COUNT - 1]
                .iter()
                .all(|row| row.below_node == "│")
        );
        assert_eq!(layout.rows[STEP_COUNT - 1].below_node, " ");
    }
}
