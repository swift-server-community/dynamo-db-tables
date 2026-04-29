# DynamoDBTables — Improvements Backlog

This document tracks public-API surface decisions and potential improvements for
DynamoDBTables. Each entry notes whether the change is source-breaking (and therefore
worth doing before 1.0) or purely additive (and can be added at any time).

## 1.0 Considerations

Before tagging 1.0, the public API surface gets locked in. Changes that would alter:

- The shape or behavior of the table protocols (`DynamoDBCompositePrimaryKeyTable`,
  `DynamoDBCompositePrimaryKeysProjection`, the `Generic*` and `InMemory*` family)
- Public types like `WriteEntry`, `TransactionConstraintEntry`,
  `TypedTTLDatabaseItem`, `CompositePrimaryKey`, `RowStatus`
- The `PrimaryKeyAttributes` / `TimeToLiveAttributes` protocols and their `Standard*`
  conformers
- The `@PolymorphicWriteEntry` / `@PolymorphicTransactionConstraintEntry` /
  `@PolymorphicOperationReturnType` macro signatures or emitted shape
- The encoder/decoder behavior for round-tripping `Codable` rows
- The `AWSSDK` / `SOTOSDK` package traits and their target products

become source-breaking after 1.0.

## Before 1.0 — completed

- **Public API surface reduction.** `Reduce public api surface.` (#136) collapsed
  several types and methods that were `public` only because of the historical
  smoke-dynamodb fork's broader surface. `Remove DynamoDBCompositePrimaryKeyGSILogic.`
  (#146) dropped a leaked-from-the-fork type that no consumer relied on. The
  resulting surface is closer to "what does a user need to integrate" rather than
  "everything the implementation happens to expose".

- **Polymorphic enum macros generic in attribute and TTL types.** The three macros
  (`@PolymorphicWriteEntry`, `@PolymorphicTransactionConstraintEntry`,
  `@PolymorphicOperationReturnType`) previously emitted `StandardCompositePrimaryKey`
  / `StandardPrimaryKeyAttributes` / `StandardTimeToLiveAttributes` directly,
  locking conforming enums to the Standard attribute types even though the
  underlying protocols were already polymorphic. They now derive `AttributesType`
  (and `TimeToLiveAttributesType` for the return-type macro) from the first case's
  parameter via internal `_Polymorphic*CaseParameter` protocols, with per-case
  assertion helpers verifying cross-case consistency at the user's case
  declaration. (#152, building on #150 and #151.)

- **`@PolymorphicOperationReturnType(databaseItemType:)` parameter removed.** The
  argument existed purely as a workaround for the macro not seeing through
  typealiases; replaced with a `_PolymorphicReturnTypeCaseParameter` protocol
  conformance on `TypedTTLDatabaseItem` so the macro extracts the row type via
  Swift's type checker. Typealiases — module-qualified, generic, or fully
  concrete — resolve transparently. (#151.)

- **Diagnostics for malformed polymorphic-enum case parameters anchored at the
  user's case line.** The macros emit per-case `_assertCase_*` helpers wrapped in
  `#sourceLocation` directives, so a wrong-shape or attribute/TTL-mismatched case
  produces a `cannot convert value of type 'X.Type' to expected argument type
  '<Concrete>.Type'` error at exactly the offending case declaration, instead of
  surfacing inside the synthetic macro-expansion buffer. Closes the long-standing
  issue #38 without waiting for language-level type-conformance checking in
  macros. (#150 + #151 + #152.)

- **Macro expansion test target.** `DynamoDBTablesMacrosTests` covers expansion
  goldens and the existing diagnostic paths for all three macros, giving
  regression coverage that didn't exist before. (#148.)

- **Encoder/decoder hardening.** Fixed FloatingPoint encoding (#140), error
  behavior on decode failures (#141), special handling for `Data` and
  `Decimal` (#143), and added round-trip property tests (#142). Added
  `reverseAttributeNameTransform` for non-default attribute name conventions
  (#144).

- **Static Linux SDK CI.** Cross-compiles core + Soto trait against the static
  Linux SDK on every push (#147), so Linux-only Sendable/macro-expansion
  divergences surface in CI rather than at consumer build time.

- **Integration tests against LocalStack.** Real-DynamoDB integration coverage
  via swift-local-containers + LocalStack, including index queries
  (#128, #135, #138). The previously test-only behavior (in-memory table) is
  now corroborated by behavior against an actual DynamoDB-compatible engine.

- **Soto SDK trait.** Optional Soto integration via the `SOTOSDK` package trait
  (#123), so consumers that prefer Soto over aws-sdk-swift have a first-class
  path. The default trait (`AWSSDK`) leaves the existing aws-sdk-swift path
  unchanged.

- **swift-configuration integration.** Configurable behavior pulled from
  `swift-configuration` (#125), so consumers that already use it get
  consistent config plumbing.

## Improvement Backlog

The pre-1.0 cleanup landed everything we knew about. Items below are speculative
or would only become real if surfaced by downstream usage.

### 1. Argument capture API for retrying operations

**Status:** Idea — not surfaced by any consumer

**Source-breaking?** No. Pure addition.

**Description:** The `retryingTransactWrite` / `retryingUpdateItem` / `retryingUpsertItem`
families currently accept a payload-providing closure that gets re-invoked on
conflict. The signature does not expose how many attempts ran, what attempt the
final success was, or what the intermediate states looked like. For users that
want to instrument retry behavior (metrics, traces) without wrapping the table
implementation, an explicit hook would help.

**Possible API:** A `retryContext: RetryContext` parameter to the closure (or an
overload) carrying attempt index and elapsed time.

**Open questions:**
- Is the retry instrumentation actually needed at this level, or do users add
  their own metrics layer above the table API?
- Would a swift-metrics integration be a better fit?

## Considered and Dropped

Items that were on the backlog but, after concrete analysis, are not worth
pursuing. Documented here so future readers don't re-litigate the same ground.

### Wait for language-level type-conformance checking in macros (issue #38)

**Original plan:** Leave the polymorphic-enum macro check for "case parameter
type conforms to `WriteEntry` / `TransactionConstraintEntry`" as a TODO until
Swift macros gain access to the type checker, since macros operate on syntax
and can't query conformances directly.

**Why dropped:** The check turned out to be expressible *today* via two existing
mechanisms: a per-case `_assertCase_*` helper that emits a generically
constrained inline `_check` function whose call-site triggers the type checker,
combined with `#sourceLocation` directives that anchor the resulting diagnostic
at the user's case declaration. The macro never has to query a conformance
itself; it generates code that the compiler type-checks normally, and the
compiler's normal "cannot convert value of type X to expected argument type Y"
diagnostic is more informative than a custom message would be. No language
changes required.

### `@PolymorphicOperationReturnType(databaseItemType:)` configurability

**Original design:** A `databaseItemType: String` argument let users specify the
syntactic name of their typealias for `StandardTypedDatabaseItem` so the macro's
syntactic name check would pass.

**Why dropped:** The argument existed purely because the macro couldn't see
through typealiases — a `_PolymorphicReturnTypeCaseParameter` protocol
conformance on `TypedTTLDatabaseItem` lets the macro emit `<paramType>.RowType.self`
and rely on Swift's type checker for typealias resolution. Once that landed
(#151), the parameter was redundant. Removed pre-1.0 to avoid carrying a
no-longer-needed configurability knob through the stable API.

### Hardcoded `StandardCompositePrimaryKey` in polymorphic-write-entry macros

**Original status:** Considered "fine for 1.0" because no real-world use case
required non-Standard attributes.

**Why reversed:** The protocols (`PolymorphicWriteEntry`,
`PolymorphicTransactionConstraintEntry`, `PolymorphicOperationReturnType`) were
already polymorphic in `AttributesType`, so the macros were the only thing
forcing `Standard*`. The change to derive the typealias from the first case
parameter via the same protocol-conformance mechanism used for `RowType` was
mechanical (~150 LOC) and additive at the protocol level — existing
Standard-only enums keep working unchanged. Pre-1.0 is the right time to
remove the asymmetry between the protocol's flexibility and the macro's
emission. (#152.)
