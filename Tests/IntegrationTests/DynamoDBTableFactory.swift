import DynamoDBTables

#if AWSSDK
import AWSDynamoDB
import DynamoDBTablesAWS
import SmithyIdentity

func createDynamoDBTable(
    tableName: String,
    endpoint: String
) throws -> any DynamoDBCompositePrimaryKeyTable {
    let client = try createDynamoDBClient(endpoint: endpoint)
    return AWSDynamoDBCompositePrimaryKeyTable(
        tableName: tableName,
        client: client
    )
}

func createDynamoDBProjection(
    tableName: String,
    endpoint: String
) throws -> any DynamoDBCompositePrimaryKeysProjection {
    let client = try createDynamoDBClient(endpoint: endpoint)
    return AWSDynamoDBCompositePrimaryKeysProjection(
        tableName: tableName,
        client: client
    )
}

private func createDynamoDBClient(endpoint: String) throws -> DynamoDBClient {
    let credentials = AWSCredentialIdentity(accessKey: "test", secret: "test")
    let config = try DynamoDBClient.DynamoDBClientConfig(
        awsCredentialIdentityResolver: StaticAWSCredentialIdentityResolver(credentials),
        region: "us-east-1",
        endpoint: endpoint
    )
    return DynamoDBClient(config: config)
}
#elseif SOTOSDK
import DynamoDBTablesSoto
import SotoDynamoDB

/// Wraps `AWSClient` so that `syncShutdown()` is called in deinit,
/// satisfying Soto's assertion that the client is shut down before deallocation.
private final class ManagedAWSClient: @unchecked Sendable {
    let client: AWSClient

    init() {
        self.client = AWSClient(
            credentialProvider: .static(
                accessKeyId: "test",
                secretAccessKey: "test"
            )
        )
    }

    deinit {
        try? client.syncShutdown()
    }
}

private let sharedClient = ManagedAWSClient()

func createDynamoDBTable(
    tableName: String,
    endpoint: String
) throws -> any DynamoDBCompositePrimaryKeyTable {
    let dynamoDB = createSotoDynamoDB(endpoint: endpoint)
    return SotoDynamoDBCompositePrimaryKeyTable(
        tableName: tableName,
        client: dynamoDB
    )
}

func createDynamoDBProjection(
    tableName: String,
    endpoint: String
) throws -> any DynamoDBCompositePrimaryKeysProjection {
    let dynamoDB = createSotoDynamoDB(endpoint: endpoint)
    return SotoDynamoDBCompositePrimaryKeysProjection(
        tableName: tableName,
        client: dynamoDB
    )
}

private func createSotoDynamoDB(endpoint: String) -> DynamoDB {
    DynamoDB(
        client: sharedClient.client,
        region: .useast1,
        endpoint: endpoint
    )
}
#endif
