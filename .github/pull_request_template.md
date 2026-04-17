## Summary

<!-- What does this PR change and why? Keep this focused. -->

## Design notes

<!-- Anything subtle worth calling out: trade-offs, alternatives considered, consistency or concurrency implications. -->

## Testing

- [ ] `make test` passes locally (`-race`)
- [ ] `make vet` clean
- [ ] `make lint` clean (if applicable)
- [ ] Added/updated tests for new behavior

## Checklist

- [ ] No `fmt.Println` / `log.Printf` in production paths
- [ ] New metrics registered on `observability.Registry.Prom`
- [ ] New tunables added to `internal/config` with defaults + validation
- [ ] README / docs updated if user-facing behavior changed
