//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeyTableGenerator.swift
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Changes specified by
// https://github.com/swift-server-community/dynamo-db-tables/compare/6fec4c8..main
// Copyright (c) 2024 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  AWSDynamoDBCompositePrimaryKeyTableGenerator.swift
//  DynamoDBTables
//

import Foundation
import Logging
import DynamoDBClient
import DynamoDBModel
import SmokeAWSCore
import SmokeAWSHttp
import SmokeHTTPClient
import AsyncHTTPClient
import NIO

public class AWSDynamoDBCompositePrimaryKeyTableGenerator {
    internal let dynamodbGenerator: _AWSDynamoDBClientGenerator
    internal let targetTableName: String
    internal let escapeSingleQuoteInPartiQL: Bool

    public init(accessKeyId: String, secretAccessKey: String,
                region: AWSRegion,
                endpointHostName: String, endpointPort: Int = 443,
                requiresTLS: Bool? = nil, tableName: String,
                connectionTimeoutSeconds: Int64 = 10,
                retryConfiguration: HTTPClientRetryConfiguration = .default,
                eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew,
                reportingConfiguration: SmokeAWSCore.SmokeAWSClientReportingConfiguration<DynamoDBModel.DynamoDBModelOperations>
                    = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>(),
                escapeSingleQuoteInPartiQL: Bool = false) {
        let staticCredentials = StaticCredentials(accessKeyId: accessKeyId,
                                                  secretAccessKey: secretAccessKey,
                                                  sessionToken: nil)

        self.dynamodbGenerator = _AWSDynamoDBClientGenerator(credentialsProvider: staticCredentials,
                                                             awsRegion: region,
                                                             endpointHostName: endpointHostName,
                                                             endpointPort: endpointPort, requiresTLS: requiresTLS,
                                                             connectionTimeoutSeconds: connectionTimeoutSeconds,
                                                             retryConfiguration: retryConfiguration,
                                                             eventLoopProvider: eventLoopProvider,
                                                             reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
    }

    public init(credentialsProvider: CredentialsProvider,
                region: AWSRegion,
                endpointHostName: String, endpointPort: Int = 443,
                requiresTLS: Bool? = nil, tableName: String,
                connectionTimeoutSeconds: Int64 = 10,
                retryConfiguration: HTTPClientRetryConfiguration = .default,
                eventLoopProvider: HTTPClient.EventLoopGroupProvider = .createNew,
                reportingConfiguration: SmokeAWSCore.SmokeAWSClientReportingConfiguration<DynamoDBModel.DynamoDBModelOperations>
                    = SmokeAWSClientReportingConfiguration<DynamoDBModelOperations>(),
                escapeSingleQuoteInPartiQL: Bool = false) {
        self.dynamodbGenerator = _AWSDynamoDBClientGenerator(credentialsProvider: credentialsProvider,
                                                             awsRegion: region,
                                                             endpointHostName: endpointHostName,
                                                             endpointPort: endpointPort, requiresTLS: requiresTLS,
                                                             connectionTimeoutSeconds: connectionTimeoutSeconds,
                                                             retryConfiguration: retryConfiguration,
                                                             eventLoopProvider: eventLoopProvider,
                                                             reportingConfiguration: reportingConfiguration)
        self.targetTableName = tableName
        self.escapeSingleQuoteInPartiQL = escapeSingleQuoteInPartiQL
    }

    /**
     Gracefully shuts down the client behind this table. This function is idempotent and
     will handle being called multiple times. Will block until shutdown is complete.
     */
    public func syncShutdown() throws {
        try self.dynamodbGenerator.syncShutdown()
    }

    // renamed `syncShutdown` to make it clearer this version of shutdown will block.
    @available(*, deprecated, renamed: "syncShutdown")
    public func close() throws {
        try self.dynamodbGenerator.close()
    }

    /**
     Gracefully shuts down the client behind this table. This function is idempotent and
     will handle being called multiple times. Will return when shutdown is complete.
     */
    #if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    public func shutdown() async throws {
        try await self.dynamodbGenerator.shutdown()
    }
    #endif
    
    public func with<NewInvocationReportingType: HTTPClientCoreInvocationReporting>(
            reporting: NewInvocationReportingType,
            tableMetrics: AWSDynamoDBTableMetrics = .init())
    -> AWSDynamoDBCompositePrimaryKeyTable<NewInvocationReportingType> {
        return AWSDynamoDBCompositePrimaryKeyTable<NewInvocationReportingType>(
            dynamodb: self.dynamodbGenerator.with(reporting: reporting),
            targetTableName: self.targetTableName,
            escapeSingleQuoteInPartiQL: self.escapeSingleQuoteInPartiQL,
            logger: reporting.logger,
            tableMetrics: tableMetrics)
    }
    
    public func with<NewTraceContextType: InvocationTraceContext>(
            logger: Logging.Logger,
            internalRequestId: String = "none",
            traceContext: NewTraceContextType,
            eventLoop: EventLoop? = nil,
            tableMetrics: AWSDynamoDBTableMetrics = .init())
    -> AWSDynamoDBCompositePrimaryKeyTable<StandardHTTPClientCoreInvocationReporting<NewTraceContextType>> {
        let reporting = StandardHTTPClientCoreInvocationReporting(
            logger: logger,
            internalRequestId: internalRequestId,
            traceContext: traceContext,
            eventLoop: eventLoop)

        return with(reporting: reporting, tableMetrics: tableMetrics)
    }

    public func with(
            logger: Logging.Logger,
            internalRequestId: String = "none",
            eventLoop: EventLoop? = nil,
            tableMetrics: AWSDynamoDBTableMetrics = .init())
    -> AWSDynamoDBCompositePrimaryKeyTable<StandardHTTPClientCoreInvocationReporting<AWSClientInvocationTraceContext>> {
        let reporting = StandardHTTPClientCoreInvocationReporting(
            logger: logger,
            internalRequestId: internalRequestId,
            traceContext: AWSClientInvocationTraceContext(),
            eventLoop: eventLoop)

        return with(reporting: reporting, tableMetrics: tableMetrics)
    }
}
