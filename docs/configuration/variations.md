# Variations of existing stages

Often it will be necessary to clone a stage or re-run the whole workflow with a
to one variable. These might be due to new calibration constants, new
systematics, or updated or new algorithms. In these cases it might be useful to
have a way to define a new stage that is almost identical to an existing stage,
but with a few changes.

`fasthep-flow` provides a way to do this by using the `variations` key in the
YAML file. This key is a dictionary, where the keys are the names of the
variations, and the values are the changes to the stage. The changes are defined
in the same way as the stage itself, but only the changes are needed. The
changes are applied to the stage, and the new stage is then added to the
workflow.

Here's an example of a variation:

```yaml

stages:
  - name: my_stage
    type: fasthep_flow.operators.BashOperator
    kwargs:
      bash_command: echo "Hello World!"
variations:
    - name: my_stage [variation]
        changes:
          kwargs:
            bash_command: echo "Hello Universe!"
```

In this example, the `my_stage [variation]` stage is a variation of the
`my_stage` stage. The only change is that the `bash_command` argument is changed
from `echo "Hello World!"` to `echo "Hello Universe!"`. Since the workflow has
only one stage, the new stage will be added to the workflow in parallel to the
original stage.

## Changing data

Let's say you are measuring the invariant mass of two particles, and you have a
stage that calculates the mass. Since the invariant mass depends on the momenta
and energies of the two particles, they are likely to have their own
calibrations. Luckily, these up- and down-systematics are already included in
the data, but under different names.

let's take the example from the CMS Public Tutorial, where we have a stage that
calculates the invariant mass of two muons. The stage looks like this:

```yaml
- name: Muon Invariant Mass
  type: fasthep_carpenter.operators.DiObjectMass
  kwargs:
    four_momenta: ["Muon_Px", "Muon_Py", "Muon_Pz", "Muon_E"]
    output: "DiMuonMass"
    when:
      all:
        - "NIsoMuon >= 2"
        - "Muon_Charge[0] == -Muon_Charge[1]"
```

The systematics are included in the data as `Muon_Px_up` and `Muon_Px_down`,
`Muon_Py_up` and `Muon_Py_down`, and so on. We can use the `variations` key to
define a new stage that uses the up-systematics:

```yaml
variations:
  - name: Muon Invariant Mass [up]
    changes:
      kwargs:
        four_momenta: ["Muon_Px_up", "Muon_Py_up", "Muon_Pz_up", "Muon_E_up"]
  - name: Muon Invariant Mass [down]
    changes:
      kwargs:
        four_momenta:
          ["Muon_Px_down", "Muon_Py_down", "Muon_Pz_down", "Muon_E_down"]
```

Since this is a full analysis example, this stage is not in isolation. Before
this stage we have the `Input data` and `Create variables` stages, and after it
we have the `Creating histograms` stages, and after it we have the
`Creating histograms`, `Select events`, `Creating histograms after selection`,
and `Output data` stages. As previously, a new stage will be added to the
workflow in parallel to the original stage, but fasthep-flow will also create
new stages for all subsequent stages, and add them to the workflow in parallel
to the original stages.

Alternatively, if you have lots of variations, you might want to use the
`source` key to define the location of the variations:

```yaml
variations:
  source: /path/to/variation_*.yaml
```
