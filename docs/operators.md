# Operators

Operators are used here as a general term of callable code blocks that operate
on data. These are similar to Ansible's modules, or Airflow's operators. In
`fasthep-flow`, operators are used to define stages in a workflow. The operators
are defined in the YAML file, and then integrated into the workflow when the DAG
is generated. The defined operators can be used to transform data, filter data,
or generate data. Operators defined in the YAML file are expected to be
callables supporting a specific protocol. When constructing the workflow,
`fasthep-flow` will try to import the module first, e.g.
`fasthep_flow.operators.bash.BashOperator`, which gives the user the flexibility
to define their own operators.

## Operator Types

There are five types of operators:

1. **Data Input**: These are a special set that does not require any input data,
   and instead generates data. These are used to start a workflow.
2. **Data Output**: These are a special set that does not require any output
   data, and instead consumes data. These are used to end a workflow.
3. **Data Transform**: These are the most common operators, and are used to
   transform data or add data to the workflow.
4. **Filter**: These are used to filter data. They are similar to data transform
   operators, but instead of adding data, they restrict part of the data to
   continue in the workflow.
5. **Passive**: These are used to monitor the workflow, and are do not change
   the data. Examples of such modules are the `ProvenanceOperator` and the
   `MonitoringOperator`.

## Custom operators

Documentation on how to create custom operators can be found in the
[developer's corner](devcon/operators.md).
