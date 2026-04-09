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

    private func uniqueKey(sortKey: String = "sk") -> StandardCompositePrimaryKey {
        CompositePrimaryKey(partitionKey: UUID().uuidString, sortKey: sortKey)
    }

    // MARK: - Tests

}

#endif

// MARK: - Test Payload

struct TestPayload: Codable, Equatable, Sendable {
    let message: String
    let count: Int
}
