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
    .enabled(if: dockerAvailable, "Docker is required")
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

        await #expect(throws: DynamoDBTableError.self) {
            try await table.insertItem(item)
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
        await #expect(throws: DynamoDBTableError.self) {
            try await table.updateItem(newItem: staleUpdate, existingItem: original)
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

        let messages = results.map(\.rowValue.message).sorted()
        #expect(messages == ["item-1", "item-2", "item-3"])
    }
}

#endif

// MARK: - Test Payload

struct TestPayload: Codable, Equatable, Sendable {
    let message: String
    let count: Int
}
