## Summary

Brief description of the change and why it was made.

## Type of change

- [ ] Bug fix (data quality, logic error, pipeline failure)
- [ ] New feature / notebook (within scope of v2.3 plan)
- [ ] KPI formula change (requires versioned SQL function + business sign-off)
- [ ] Configuration change (`configs/`, `rules_config.yaml`)
- [ ] Documentation update
- [ ] Schema change (requires data contract update)

## Scope check

> [!IMPORTANT]
> This project has a scope lock. Confirm before merging:

- [ ] This change is within the scope of the v2.3 implementation plan
- [ ] If a KPI formula changed: new versioned SQL function created (old version preserved)
- [ ] If a schema changed: data contract updated
- [ ] If a weight changed: `kpi_weights.yaml` updated with new version + commercial sign-off documented
- [ ] If a recommendation rule changed: `rules_config.yaml` version incremented + commercial sign-off documented

## Tests

- [ ] Unit tests pass locally (`pytest tests/unit/`)
- [ ] YAML configs valid (`python -c "import yaml; yaml.safe_load(open('configs/....yaml'))"`)
- [ ] No plaintext credentials introduced

## Edge cases considered

List any edge cases from §5 of the implementation plan that are relevant to this change and how they are handled.

## Reviewer checklist

- [ ] Logic is correct
- [ ] Edge cases handled per §5 registry
- [ ] No hardcoded credentials, thresholds, or environment names
- [ ] If schema change: downstream impact assessed
