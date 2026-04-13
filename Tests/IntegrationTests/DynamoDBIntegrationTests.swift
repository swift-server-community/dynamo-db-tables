import ContainerMacrosLib
import ContainerTestSupport
import DynamoDBTables
import Foundation
import Testing

// ContainerizationContainerRuntime is not yet fully implemented,
// so integration tests only run where PlatformRuntime uses Docker.
#if !canImport(ContainerizationRuntime)

@Containers
struct DynamoDBContainers {
    @LocalStackContainer(stackName: "dynamo-integration")
    var integrationStack: DynamodbTableOutputs
}

@Suite(
    DynamoDBContainers.containerTrait,
    .tags(.integration),
    .enabled(if: containerRuntimeAvailable, "Container runtime is required"),
    .enabled(
        if: localStackAuthTokenAvailable,
        "LOCALSTACK_AUTH_TOKEN is required (set it in the environment or in .local-containers/env)"
    )
)
struct DynamoDBIntegrationTests {
    let containers = DynamoDBContainers()

    private func makeTable() throws -> any DynamoDBCompositePrimaryKeyTable {
        let stack = containers.integrationStack
        return try createDynamoDBTable(
            tableName: stack.tableName,
            endpoint: stack.awsEndpoint
        )
    }

    private func makeProjection() throws -> any DynamoDBCompositePrimaryKeysProjection {
        let stack = containers.integrationStack
        return try createDynamoDBProjection(
            tableName: stack.tableName,
            endpoint: stack.awsEndpoint
        )
    }

    private func uniqueKey(sortKey: String = "sk") -> StandardCompositePrimaryKey {
        CompositePrimaryKey(partitionKey: UUID().uuidString, sortKey: sortKey)
    }

    // MARK: - Tests

    @Test("Insert and get item round-trips correctly")
    func insertAndGet() async throws {
        let table = try makeTable()
        let key = uniqueKey()
        let payload = TestPayload(message: "hello", count: 42)
        let item = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(item)

        let retrieved: StandardTypedDatabaseItem<TestPayload>? = try await table.getItem(forKey: key)
        let unwrapped = try #require(retrieved)
        #expect(unwrapped.rowValue == payload)
        #expect(unwrapped.rowStatus.rowVersion == 1)
    }

    @Test("Inserting duplicate key throws conditionalCheckFailed")
    func insertDuplicate() async throws {
        let table = try makeTable()
        let key = uniqueKey()
        let payload = TestPayload(message: "original", count: 1)
        let item = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(item)

        let thrown = await #expect(throws: DynamoDBTableError.self) {
            try await table.insertItem(item)
        }
        guard case .conditionalCheckFailed = thrown else {
            Issue.record("Expected .conditionalCheckFailed, got \(String(describing: thrown))")
            return
        }
    }

    @Test("Update item increments version")
    func updateItem() async throws {
        let table = try makeTable()
        let key = uniqueKey()
        let payload = TestPayload(message: "v1", count: 1)
        let original = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(original)

        let updatedPayload = TestPayload(message: "v2", count: 2)
        let updatedItem = original.createUpdatedItem(withValue: updatedPayload)
        try await table.updateItem(newItem: updatedItem, existingItem: original)

        let retrieved: StandardTypedDatabaseItem<TestPayload>? = try await table.getItem(forKey: key)
        let unwrapped = try #require(retrieved)
        #expect(unwrapped.rowValue == updatedPayload)
        #expect(unwrapped.rowStatus.rowVersion == 2)
    }

    @Test("Update with stale version throws conditionalCheckFailed")
    func updateWithStaleVersion() async throws {
        let table = try makeTable()
        let key = uniqueKey()
        let payload = TestPayload(message: "v1", count: 1)
        let original = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(original)

        // First update succeeds
        let updated = original.createUpdatedItem(
            withValue: TestPayload(message: "v2", count: 2)
        )
        try await table.updateItem(newItem: updated, existingItem: original)

        // Second update with stale version fails
        let staleUpdate = original.createUpdatedItem(
            withValue: TestPayload(message: "v3", count: 3)
        )
        let thrown = await #expect(throws: DynamoDBTableError.self) {
            try await table.updateItem(newItem: staleUpdate, existingItem: original)
        }
        guard case .conditionalCheckFailed = thrown else {
            Issue.record("Expected .conditionalCheckFailed, got \(String(describing: thrown))")
            return
        }
    }

    @Test("Delete item removes it from the table")
    func deleteItem() async throws {
        let table = try makeTable()
        let key = uniqueKey()
        let payload = TestPayload(message: "to-delete", count: 0)
        let item = StandardTypedDatabaseItem.newItem(withKey: key, andValue: payload)

        try await table.insertItem(item)
        try await table.deleteItem(forKey: key)

        let retrieved: StandardTypedDatabaseItem<TestPayload>? = try await table.getItem(forKey: key)
        #expect(retrieved == nil)
    }

    @Test("Clobber item overwrites without condition check")
    func clobberItem() async throws {
        let table = try makeTable()
        let key = uniqueKey()
        let first = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: TestPayload(message: "first", count: 1)
        )
        let second = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: TestPayload(message: "second", count: 2)
        )

        try await table.clobberItem(first)
        try await table.clobberItem(second)

        let retrieved: StandardTypedDatabaseItem<TestPayload>? = try await table.getItem(forKey: key)
        let unwrapped = try #require(retrieved)
        #expect(unwrapped.rowValue.message == "second")
    }

    @Test("retryingUpsertItem inserts when no item exists")
    func retryingUpsertInsertPath() async throws {
        let table = try makeTable()
        let key = uniqueKey()

        let written = try await table.retryingUpsertItem(
            forKey: key,
            newItemProvider: {
                StandardTypedDatabaseItem.newItem(
                    withKey: key,
                    andValue: TestPayload(message: "fresh", count: 1)
                )
            },
            updatedItemProvider: { existing in
                existing.createUpdatedItem(
                    withValue: TestPayload(message: "should-not-run", count: 99)
                )
            }
        )

        #expect(written.rowValue.message == "fresh")
        #expect(written.rowStatus.rowVersion == 1)

        let retrieved: StandardTypedDatabaseItem<TestPayload>? = try await table.getItem(forKey: key)
        let unwrapped = try #require(retrieved)
        #expect(unwrapped.rowValue.message == "fresh")
    }

    @Test("retryingUpsertItem updates when item already exists")
    func retryingUpsertUpdatePath() async throws {
        let table = try makeTable()
        let key = uniqueKey()
        let original = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: TestPayload(message: "v1", count: 1)
        )
        try await table.insertItem(original)

        let written = try await table.retryingUpsertItem(
            forKey: key,
            newItemProvider: {
                StandardTypedDatabaseItem.newItem(
                    withKey: key,
                    andValue: TestPayload(message: "should-not-run", count: 0)
                )
            },
            updatedItemProvider: { existing in
                existing.createUpdatedItem(
                    withValue: TestPayload(message: "v2", count: 2)
                )
            }
        )

        #expect(written.rowValue.message == "v2")
        #expect(written.rowStatus.rowVersion == 2)
    }

    @Test("retryingUpdateItem updates an existing item via the payload provider")
    func retryingUpdateExistingItem() async throws {
        let table = try makeTable()
        let key = uniqueKey()
        let original = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: TestPayload(message: "v1", count: 1)
        )
        try await table.insertItem(original)

        let updated: StandardTypedDatabaseItem<TestPayload> = try await table.retryingUpdateItem(
            forKey: key,
            updatedPayloadProvider: { existing in
                TestPayload(message: existing.message + "-updated", count: existing.count + 10)
            }
        )

        #expect(updated.rowValue.message == "v1-updated")
        #expect(updated.rowValue.count == 11)
        #expect(updated.rowStatus.rowVersion == 2)
    }

    @Test("Query returns items for a partition key")
    func queryByPartitionKey() async throws {
        let table = try makeTable()
        let partitionKey = UUID().uuidString

        for index in 1...3 {
            let key = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
                partitionKey: partitionKey,
                sortKey: "item-\(index)"
            )
            let item = StandardTypedDatabaseItem.newItem(
                withKey: key,
                andValue: TestPayload(message: "item-\(index)", count: index)
            )
            try await table.insertItem(item)
        }

        let results: [StandardTypedDatabaseItem<TestPayload>] = try await table.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: nil
        )
        #expect(results.count == 3)

        let messages = results.map(\.rowValue.message)
        #expect(messages == ["item-1", "item-2", "item-3"])
    }

    // MARK: - batchGetItem coverage

    @Test("getItems returns all requested items via batchGetItem")
    func getItemsBatch() async throws {
        let table = try makeTable()
        let partitionKey = UUID().uuidString

        var keys: [StandardCompositePrimaryKey] = []
        for index in 1...3 {
            let key = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
                partitionKey: partitionKey,
                sortKey: "sk-\(index)"
            )
            let item = StandardTypedDatabaseItem.newItem(
                withKey: key,
                andValue: TestPayload(message: "msg-\(index)", count: index)
            )
            try await table.insertItem(item)
            keys.append(key)
        }

        let results: [StandardCompositePrimaryKey: StandardTypedDatabaseItem<TestPayload>] =
            try await table.getItems(forKeys: keys)
        #expect(results.count == 3)
        for key in keys {
            let unwrapped = try #require(results[key])
            #expect(unwrapped.rowValue.message.hasPrefix("msg-"))
        }
    }

    // MARK: - executeTransaction coverage

    @Test("transactWrite inserts a batch of items in a single transaction")
    func transactWriteInsertsBatch() async throws {
        let table = try makeTable()
        let partitionKey = UUID().uuidString

        let entries: [StandardWriteEntry<TestPayload>] = (1...3).map { index in
            let key = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
                partitionKey: partitionKey,
                sortKey: "tx-\(index)"
            )
            return .insert(
                new: StandardTypedDatabaseItem.newItem(
                    withKey: key,
                    andValue: TestPayload(message: "tx-\(index)", count: index)
                )
            )
        }

        try await table.transactWrite(entries)

        let written: [StandardTypedDatabaseItem<TestPayload>] = try await table.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: nil
        )
        #expect(written.count == 3)
        let messages = written.map(\.rowValue.message)
        #expect(messages == ["tx-1", "tx-2", "tx-3"])
    }

    // MARK: - batchExecuteStatement coverage

    @Test("bulkWrite inserts a batch of items via batchExecuteStatement")
    func bulkWriteInsertsBatch() async throws {
        let table = try makeTable()
        let partitionKey = UUID().uuidString

        let entries: [StandardWriteEntry<TestPayload>] = (1...3).map { index in
            let key = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
                partitionKey: partitionKey,
                sortKey: "bw-\(index)"
            )
            return .insert(
                new: StandardTypedDatabaseItem.newItem(
                    withKey: key,
                    andValue: TestPayload(message: "bw-\(index)", count: index)
                )
            )
        }

        try await table.bulkWrite(entries)

        let written: [StandardTypedDatabaseItem<TestPayload>] = try await table.query(
            forPartitionKey: partitionKey,
            sortKeyCondition: nil
        )
        #expect(written.count == 3)
        let messages = written.map(\.rowValue.message)
        #expect(messages == ["bw-1", "bw-2", "bw-3"])
    }

    // MARK: - executeStatement coverage

    @Test("execute returns items for the requested partition keys via PartiQL")
    func executePartiQL() async throws {
        let table = try makeTable()
        let partitionKey = UUID().uuidString

        for index in 1...3 {
            let key = CompositePrimaryKey<StandardPrimaryKeyAttributes>(
                partitionKey: partitionKey,
                sortKey: "ex-\(index)"
            )
            let item = StandardTypedDatabaseItem.newItem(
                withKey: key,
                andValue: TestPayload(message: "ex-\(index)", count: index)
            )
            try await table.insertItem(item)
        }

        let results: [StandardTypedDatabaseItem<TestPayload>] = try await table.execute(
            partitionKeys: [partitionKey],
            attributesFilter: nil,
            additionalWhereClause: nil
        )
        #expect(results.count == 3)
        let messages = results.map(\.rowValue.message)
        #expect(messages == ["ex-1", "ex-2", "ex-3"])
    }

    // MARK: - GSI projection coverage

    @Test("GSI projection returns keys for items with matching GSI partition key")
    func gsiProjectionQuery() async throws {
        let table = try makeTable()
        let projection = try makeProjection()
        let gsiPartitionKey = UUID().uuidString

        for index in 1...3 {
            let key = uniqueKey(sortKey: "gsi-item-\(index)")
            let item = StandardTypedDatabaseItem.newItem(
                withKey: key,
                andValue: GSITestPayload(
                    GSI1PK: gsiPartitionKey,
                    GSI1SK: "gsi-sk-\(index)",
                    message: "msg-\(index)"
                )
            )
            try await table.insertItem(item)
        }

        let keys: [CompositePrimaryKey<GSI1PrimaryKeyAttributes>] =
            try await projection.query(
                forPartitionKey: gsiPartitionKey,
                sortKeyCondition: nil
            )
        #expect(keys.count == 3)
        let sortKeys = keys.map(\.sortKey).sorted()
        #expect(sortKeys == ["gsi-sk-1", "gsi-sk-2", "gsi-sk-3"])
    }

    @Test("GSI projection returns empty results for non-existent partition key")
    func gsiProjectionEmptyResults() async throws {
        let projection = try makeProjection()

        let keys: [CompositePrimaryKey<GSI1PrimaryKeyAttributes>] =
            try await projection.query(
                forPartitionKey: UUID().uuidString,
                sortKeyCondition: nil
            )
        #expect(keys.isEmpty)
    }
}

#endif

// MARK: - Test Types

struct TestPayload: Codable, Equatable, Sendable {
    let message: String
    let count: Int
}

/// Payload type whose fields include GSI key attributes. DynamoDB projects
/// these into the GSI automatically because the CloudFormation template
/// declares `GSI1PK` and `GSI1SK` as attribute definitions on the table.
struct GSITestPayload: Codable, Sendable {
    let GSI1PK: String
    let GSI1SK: String
    let message: String
}

/// `PrimaryKeyAttributes` for the GSI1 index. The `indexName` tells
/// DynamoDBTables to include `IndexName: "GSI1"` in the query request.
struct GSI1PrimaryKeyAttributes: PrimaryKeyAttributes {
    static var partitionKeyAttributeName: String { "GSI1PK" }
    static var sortKeyAttributeName: String { "GSI1SK" }
    static var indexName: String? { "GSI1" }
}
