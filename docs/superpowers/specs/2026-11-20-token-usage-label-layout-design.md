# Token Usage Label Layout Design

## Intent

The four Harness model-usage labels must remain fully readable in the execution
graph details panel instead of collapsing to one character plus an ellipsis.

## Root Cause

The metric cells use `<article>` inside `.execution-details`. The older
`.execution-details article` activity rule has greater specificity than the
metric class and replaces each cell with a three-column grid whose first column
is 12px wide.

## Design

Use ordinary `<div>` elements for the four non-document metric cells. This
removes them from the activity-row selector without increasing CSS specificity
or coupling the two components. Keep the existing four-column desktop and
two-column mobile layouts. Browser coverage must prove that all four exact
labels fit without horizontal overflow.

The rejected alternative is an escalating CSS override for every property set
by `.execution-details article`; that preserves the invalid structural overlap
and is easier to regress when activity styling changes.

