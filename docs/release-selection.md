<!-- SPDX-License-Identifier: Apache-2.0 -->
# Deployment-owned releases

The deployment independently selects and locks ClickHouse images, Keeper and Meridian distributions.
The adapter does not accept or reject a release because it appears in a historical table.
A passed probe establishes the checked features; complete conformance evidence establishes only the
exact tested combination. Unlisted or untested combinations remain unverified.

## Complete gate inventory

| Surface / variants | Classification | Rule |
| --- | --- | --- |
| `ClickHouseSettings.from_binding`, standalone/replicated; managed/external endpoints | Release metadata | No server-release membership predicate. Closed topology identities remain enforced. |
| `adapter_descriptor` and legacy `SUPPORTED_ENGINE_VERSIONS` | Historical evidence | The V1 wire name `supportedEngineVersions` and legacy Python alias remain, but contain historical 25.3 evidence, independent of the selected version. New runs live in CI evidence, not a compiled acceptance table. |
| `capability_manifest` | Deployment integrity | `engineVersion` records the selection; changing it changes the canonical manifest hash and requires an explicit Binding update. |
| `probe_adapter` / runtime open | Deployment integrity | Authenticated `version()` must match the declared release or its dotted patch. This checks drift against deployment input, never against a compiled release. |
| `AdapterProbe.observed_engine_version` and evidence | Observation | Actual authenticated server value, separate from the selected release; no configured value is presented as an observation. |
| Binding Adapter contract, runtime operations, bounded Query and append | Real contract | SPI 1.0.0 and advertised operation versions remain closed; unsupported write modes, unbounded scans and resource violations fail. |
| Probe functions, UTC, table engines, replicas/Keeper | Required features | Required SQL functions, timezone, ReplacingMergeTree/ReplicatedReplacingMergeTree, writable replicas, active Keeper session and consistent healthy replica count remain mandatory. |
| Physical verification, Schema/layout metadata, columns, capability and physical pins | Deployment integrity | Canonical like-for-like hashes and physical Schema evidence must match. No hash is synthesized from installed package releases. |
| Endpoint, driver client, identity, TLS | Security / provider constraint | Credential-free origins, TLS policy, authenticated access and driver errors remain enforced; managed/external ownership does not bypass them. |
| Schema/migration and backup helpers | Real contract / ownership | Layout and migration fingerprints remain unchanged. Only explicit external deployment jobs mutate Schema or perform recovery. Runtime never provisions or backs up an engine. |
| `pyproject.toml` | Public API bounds | Core >=1.1,<2 is needed for the released observation SPI and non-gating manifests. Query >=1.0.3,<2 and Semantics >=2.0.1,<3 provide a normally resolvable Core-compatible API closure. Driver 0.15 APIs retain their existing bounds. These bounds do not claim every future combination is verified. |
| `requirements.lock`, compose images, CI matrix | Reproducible examples | Exact distribution hashes and immutable image digests select tested environments, not runtime compatibility rules. Both standalone and replicated suites run independently selected 25.3 and 25.8 images. |

## Migration and evidence

Adapter distribution 1.1.0 retains Adapter, layout and operation wire contracts. Core >=1.1 is
required for the additive authenticated observation SPI. Historical 25.3 manifest bytes remain
stable; a new selected release produces its own manifest fingerprint without falsely adding itself
to descriptor history. Regenerate and explicitly deploy Binding fingerprints when selection changes.

The released driver receives Schema-directed binary column formats; JSON fields are decoded
from their declared logical type, including projected aliases. This preserves the existing
logical contract for telemetry identifiers, attributes, events, links, histogram and exemplar
payloads. It adds no service publication or Collector responsibility.

CI uploads every engine suite's JUnit result and exact combination report, including selected images,
observed image IDs, actual server observation, installed library releases and the hashed dependency
lock. Existing checked-in 1.0.0 reports are historical. Release workflow publishes immutable wheel
and sdist hashes and build attestations. Resolve the public release plus its ordinary dependencies;
do not use sibling sources or installation overrides.

The fixtures use isolated Docker Compose projects and persistent disposable volumes. Standalone
restarts its actual server and compares stored rows after recovery. Replicated tests stop one member,
verify the degraded topology fails its probe, append on the surviving member using the adapter's
batch executor, restart the peer and verify catch-up. Both perform native BACKUP and RESTORE and compare complete normalized rows. Standalone restores
a separate table; replicated recovery drops the disposable source table on both members, restores
its backup on one member and re-creates the second replica to verify recovery without Keeper-path
collisions or weakening Schema validation. The scripts remove only their disposable
project volumes on exit. These tests do not establish managed-provider or object-store backup
certification; external deployments must supply their own provider-specific evidence.
