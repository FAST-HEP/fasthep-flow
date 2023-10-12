# Operators

Operators are used here as a general term of callable code blocks that operate
on data. In Airflow, operators are used to define tasks in a
[DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph). In `fasthep-flow`,
operators are used to define stages in a workflow. The operators are defined in
the YAML file, and then mapped to Airflow operators when the DAG is generated.
One `fasthep-flow` operator can map to multiple Airflow operators.

## Operator Types

There are four types of operators:

1. **Data Input**: These are a special set that does not require any input data,
   and instead generates data. These are used to start a workflow.
2. **Data Output**: These are a special set that does not require any output
   data, and instead consumes data. These are used to end a workflow.
3. **Data Transform**: These are the most common operators, and are used to
   transform data or add data to the workflow.
4. **Filter**: These are used to filter data. They are similar to data transform
   operators, but instead of adding data, they restrict part of the data to
   continue in the workflow.
