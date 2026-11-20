# Execution Role Title Alignment Design

## Intent

Execution Graph role icons and role names must read as one title row. The name
needs slightly more horizontal separation from the icon without changing node
size, graph layout, dragging or status presentation.

## Approach

Keep the existing node DOM and three-row CSS grid. Stop spanning the role icon
across all rows; place it in row one, place the role name in row one/column two,
and add 3px left padding to the name. Title and status remain in rows two and
three. A browser geometry assertion protects desktop alignment.
