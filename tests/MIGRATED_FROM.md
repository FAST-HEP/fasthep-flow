# Migrated From Pre-Split Tests

This checklist records how `hepflow/tests` was triaged for the native
`fasthep-flow` test suite. Flow tests use `tests.toy_components` instead of
HEP-specific carpenter, curator, or render components.

| Original test area | Classification | Destination / status |
| --- | --- | --- |
| `architecture/test_no_legacy_imports.py` | flow-owned | Adapted as `test_architecture.py`. |
| `backends/test_backend_loader.py` | flow-owned | Adapted as `test_backend_loader.py`. |
| `backends/test_local_backend.py` | flow-owned | Covered by `test_runtime_toy.py` and public API run tests. |
| `backends/test_dask_local_backend.py` | flow-owned, optional runtime | Loader coverage kept; full dask execution deferred until optional dependency CI. |
| `compiler/test_includes.py` | flow-owned | Covered by include normalization smoke in `test_normalize_plan_api.py`. |
| `compiler/test_normalize.py` | flow-owned plus source defaults | Adapted for generic toy sources in `test_normalize_plan_api.py`; ROOT defaults remain existing flow behavior. |
| `compiler/test_profiles.py` | flow-owned | Adapted as package-qualified profile tests in `test_profiles_registry.py`. |
| `compiler/test_lower_graph.py` | flow-owned plus HEP ops | Generic lowering covered in `test_normalize_plan_api.py`; HEP cases moved to owning packages. |
| `compiler/test_make_plan.py`, `test_plan_context.py`, `test_plan_diff.py` | flow-owned | Plan creation/context covered in `test_normalize_plan_api.py`; diff remains public API surface for later expansion. |
| `compiler/test_data_flow.py`, `test_symbols.py`, `test_parsers.py`, `test_routing.py` | flow-owned generic mechanics | Toy dependency parser and source inference covered in `test_hooks_data_flow.py`; HEP expression specifics deferred. |
| `runtime/test_handlers.py`, `test_sink_execution.py`, `test_sink_timing.py`, `test_lifecycle.py` | flow-owned | Covered by toy runtime, lifecycle, and sink timing tests. |
| `runtime/test_execution_hooks.py` | flow-owned hook manager | Adapted with toy hook in `test_hooks_data_flow.py`. |
| `runtime/test_observer_execution.py` | flow-owned mechanism, curator component ownership | Toy observer component added; full curator observers stay in `fasthep-curator`. |
| `runtime/test_records.py`, `test_merge.py` | flow-owned where generic | Deferred for focused follow-up; current toy runtime covers non-awkward flow path. |
| `model/test_*`, `registry/test_merge.py`, `test_api.py`, `test_imports.py`, `test_streams.py` | flow-owned | Partially covered by current smoke tests; deeper model tests are candidates for direct port. |
| `carpenter/**`, `runtime/test_*define*`, `*hist*`, `*cutflow*`, `*di_object_mass*`, `*match_l1t*`, `test_zip_join.py`, ROOT source/writer tests | carpenter-owned | Do not migrate to flow; use `fasthep-carpenter` tests. |
| `runtime/test_schema_snapshot_observer.py`, `test_warning_capture_hook.py`, `test_node_error_context.py`, schema/source inspection tests | curator-owned | Do not migrate to flow; use `fasthep-curator` tests. |
| `render/**`, `test_render_spec.py`, render/style/multi-input render tests | render-owned | Do not migrate to flow; use `fasthep-render` tests. |
| `cli/**` | CLI-owned | Do not migrate to flow; use `fasthep-cli` tests. |
| `end2end/**`, `golden/**`, public example runs | integration | Belongs in `fasthep-workshop` / workspace smoke tests. |
| `legacy/**` and old workflow cache files | legacy | Do not migrate. |
