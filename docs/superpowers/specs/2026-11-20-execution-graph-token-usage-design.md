# Execution Graph Harness Token Usage Design

## Intent

Every Primary, Child and Reviewer Harness node must expose its own model token
usage without making graph nodes dense. Usage changes should be visibly gradual
for the same running Harness.

## Placement

- A Harness node appends one compact total to its existing state/time line.
- Selecting the node shows four metrics above the execution timeline: ordinary
  input, cache read, cache write and output.
- Exact counts are available as titles; visible values use compact units.

## Data Contract

Gateway model activity already persists the four normalized usage dimensions.
Execution Graph groups PLAN Items by `(turn_id, job_id)`: empty `job_id` belongs
to the Primary Harness, while a Child or Reviewer uses its durable Job ID. No
database schema changes are required.

The projector bounds usage rows. If the bound is exceeded, affected graph
nodes expose `token_usage_truncated=true`; the UI never labels a partial value
as complete.

## Animation

For one selected node, each counter interpolates from its currently rendered
value to the next durable aggregate over 600ms with ease-out. Switching node ID
sets the new node's current value immediately. Reduced-motion users receive an
immediate update. Animation is presentation-only and never changes accounting.
