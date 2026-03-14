import DynamoDBTables

#if AWSSDK
import AWSDynamoDB
import DynamoDBTablesAWS
import SmithyIdentity

func createDynamoDBTable(
    tableName: String,
    endpoint: String
) throws -> any DynamoDBCompositePrimaryKeyTable {
    let credentials = AWSCredentialIdentity(accessKey: "test", secret: "test")
    let config = try DynamoDBClient.DynamoDBClientConfig(
        awsCredentialIdentityResolver: StaticAWSCredentialIdentityResolver(credentials),
        region: "us-east-1",
        endpoint: endpoint
    )
    return AWSDynamoDBCompositePrimaryKeyTable(
        tableName: tableName,
        client: DynamoDBClient(config: config)
    )
}
#elseif SOTOSDK
import DynamoDBTablesSoto
import SotoDynamoDB

func createDynamoDBTable(
    tableName: String,
    endpoint: String
) throws -> any DynamoDBCompositePrimaryKeyTable {
    let awsClient = AWSClient(
        credentialProvider: .static(
            accessKeyId: "test",
            secretAccessKey: "test"
        )
    )
    let dynamoDB = DynamoDB(
        client: awsClient,
        region: .useast1,
        endpoint: endpoint
    )
    return SotoDynamoDBCompositePrimaryKeyTable(
        tableName: tableName,
        client: dynamoDB
    )
}
#endif
