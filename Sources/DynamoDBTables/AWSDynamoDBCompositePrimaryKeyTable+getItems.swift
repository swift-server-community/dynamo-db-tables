//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/AWSDynamoDBCompositePrimaryKeyTable+monomorphicGetItems.swift
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Changes specified by
// https://github.com/swift-server-community/dynamo-db-tables/compare/9ab0e7a..main
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
//  AWSDynamoDBCompositePrimaryKeyTable+getItems.swift
//  DynamoDBTables
//

import AWSDynamoDB
import Foundation
import Logging

// BatchGetItem has a maximum of 100 of items per request
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
private let maximumKeysPerGetItemBatch = 100
private let millisecondsToNanoSeconds: UInt64 = 1_000_000

/// DynamoDBTable conformance getItems function
public extension AWSDynamoDBCompositePrimaryKeyTable {
    /**
     Helper type that manages the state of a getItems request.

     As suggested here - https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html - this helper type
     monitors the unprocessed items returned in the response from DynamoDB and uses an exponential backoff algorithm to retry those items using
     the same retry configuration as the underlying DynamoDB client.
     */
    private class GetItemsRetriable<AttributesType: PrimaryKeyAttributes, ItemType: Codable, TimeToLiveAttributesType: TimeToLiveAttributes> {
        typealias OutputType = [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]

        let dynamodb: AWSDynamoDB.DynamoDBClient
        let retryConfiguration: RetryConfiguration
        let logger: Logging.Logger

        var retriesRemaining: Int
        var input: BatchGetItemInput
        var outputItems: OutputType = [:]

        init(initialInput: BatchGetItemInput,
             dynamodb: AWSDynamoDB.DynamoDBClient,
             retryConfiguration: RetryConfiguration,
             logger: Logging.Logger)
        {
            self.dynamodb = dynamodb
            self.retryConfiguration = retryConfiguration
            self.retriesRemaining = retryConfiguration.numRetries
            self.input = initialInput
            self.logger = logger
        }

        func batchGetItem() async throws -> OutputType {
            // submit the asynchronous request
            let output = try await self.dynamodb.batchGetItem(input: self.input)

            let errors = output.responses?.flatMap { _, itemList -> [Error] in
                return itemList.compactMap { values -> Error? in
                    do {
                        let attributeValue = DynamoDBClientTypes.AttributeValue.m(values)

                        let decodedValue: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>
                            = try DynamoDBDecoder().decode(attributeValue)
                        let key = decodedValue.compositePrimaryKey

                        self.outputItems[key] = decodedValue
                        return nil
                    } catch {
                        return error
                    }
                }
            } ?? []

            if !errors.isEmpty {
                throw DynamoDBTableError.multipleUnexpectedErrors(cause: errors)
            }

            if let requestItems = output.unprocessedKeys, !requestItems.isEmpty {
                self.input = BatchGetItemInput(requestItems: requestItems)

                return try await self.getMoreResults()
            }

            return self.outputItems
        }

        func getMoreResults() async throws -> OutputType {
            // if there are retries remaining
            if self.retriesRemaining > 0 {
                // determine the required interval
                let retryInterval = Int(self.retryConfiguration.getRetryInterval(retriesRemaining: self.retriesRemaining))

                let currentRetriesRemaining = self.retriesRemaining
                self.retriesRemaining -= 1

                let remainingKeysCount = self.input.requestItems?.count ?? 0

                self.logger.warning(
                    "Request retried for remaining items: \(remainingKeysCount). Remaining retries: \(currentRetriesRemaining). Retrying in \(retryInterval) ms.")
                try await Task.sleep(nanoseconds: UInt64(retryInterval) * millisecondsToNanoSeconds)

                self.logger.trace("Reattempting request due to remaining retries: \(currentRetriesRemaining)")
                return try await self.batchGetItem()
            }

            throw DynamoDBTableError.batchAPIExceededRetries(retryCount: self.retryConfiguration.numRetries)
        }
    }

    func getItems<AttributesType, ItemType, TimeToLiveAttributesType>(
        forKeys keys: [CompositePrimaryKey<AttributesType>]) async throws
        -> [CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<AttributesType, ItemType, TimeToLiveAttributesType>]
    {
        let chunkedList = keys.chunked(by: maximumKeysPerGetItemBatch)

        let maps = try await chunkedList.concurrentMap { chunk in
            let input = try self.getInputForBatchGetItem(forKeys: chunk)

            let retriable = GetItemsRetriable<AttributesType, ItemType, TimeToLiveAttributesType>(
                initialInput: input,
                dynamodb: self.dynamodb,
                retryConfiguration: self.retryConfiguration,
                logger: self.logger)

            return try await retriable.batchGetItem()
        }

        // maps is of type [[CompositePrimaryKey<AttributesType>: TypedTTLDatabaseItem<some Any, some Any, some Any>]]
        // with each map coming from each chunk of the original key list
        return maps.reduce([:]) { partialMap, chunkMap in
            // reduce the maps from the chunks into a single map
            partialMap.merging(chunkMap) { _, new in new }
        }
    }
}
