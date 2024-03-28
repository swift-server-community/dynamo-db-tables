//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from https://github.com/amzn/smoke-dynamodb. Any commits
// prior to February 2024
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Subsequent commits
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
//  AWSDynamoDBCompositePrimaryKeysProjection+DynamoDBKeysProjectionAsync.swift
//  DynamoDBTables
//

import Foundation
import SmokeAWSCore
import DynamoDBModel
import SmokeHTTPClient
import Logging
import NIO

/// DynamoDBKeysProjection conformance async functions
public extension AWSDynamoDBCompositePrimaryKeysProjection {
    
    func query<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?) -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]>
            where AttributesType: PrimaryKeyAttributes {
        return partialQuery(forPartitionKey: partitionKey,
                            sortKeyCondition: sortKeyCondition,
                            exclusiveStartKey: nil)
    }
    
    // function to return a future with the results of a query call and all future paginated calls
    private func partialQuery<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            exclusiveStartKey: String?) -> EventLoopFuture<[CompositePrimaryKey<AttributesType>]>
            where AttributesType: PrimaryKeyAttributes {
        let queryFuture: EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)> =
            query(forPartitionKey: partitionKey,
                  sortKeyCondition: sortKeyCondition,
                  limit: nil,
                  scanIndexForward: true,
                  exclusiveStartKey: exclusiveStartKey)
        
        return queryFuture.flatMap { paginatedItems in
            // if there are more items
            if let lastEvaluatedKey = paginatedItems.1 {
                // returns a future with all the results from all later paginated calls
                return self.partialQuery(forPartitionKey: partitionKey,
                                         sortKeyCondition: sortKeyCondition,
                                         exclusiveStartKey: lastEvaluatedKey)
                    .map { partialResult in
                        // return the results from 'this' call and all later paginated calls
                        return paginatedItems.0 + partialResult
                    }
            } else {
                // this is it, all results have been obtained
                let promise = self.eventLoop.makePromise(of: [CompositePrimaryKey<AttributesType>].self)
                promise.succeed(paginatedItems.0)
                return promise.futureResult
            }
        }
    }
    
    func query<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?, exclusiveStartKey: String?) -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        return query(forPartitionKey: partitionKey,
                     sortKeyCondition: sortKeyCondition,
                     limit: limit,
                     scanIndexForward: true,
                     exclusiveStartKey: exclusiveStartKey)
        
    }
    
    func query<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            limit: Int?,
            scanIndexForward: Bool,
            exclusiveStartKey: String?) -> EventLoopFuture<([CompositePrimaryKey<AttributesType>], String?)>
            where AttributesType: PrimaryKeyAttributes {
        let queryInput: DynamoDBModel.QueryInput
        do {
            queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(partitionKey: partitionKey, targetTableName: targetTableName,
                                                                          primaryKeyType: AttributesType.self,
                                                                          sortKeyCondition: sortKeyCondition, limit: limit,
                                                                          scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                                          consistentRead: false)
        } catch {
            let promise = self.eventLoop.makePromise(of: ([CompositePrimaryKey<AttributesType>], String?).self)
            promise.fail(error)
            return promise.futureResult
        }
        
        let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
            "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.trace("\(logMessage)")
        
        return dynamodb.query(input: queryInput).flatMapThrowing { queryOutput in
            let lastEvaluatedKey: String?
            if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                let encodedLastEvaluatedKey: Data
                
                do {
                    encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
                } catch {
                    throw error.asUnrecognizedDynamoDBTableError()
                }
                
                lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
            } else {
                lastEvaluatedKey = nil
            }
            
            if let outputAttributeValues = queryOutput.items {
                let items: [CompositePrimaryKey<AttributesType>]
                
                do {
                    items = try outputAttributeValues.map { values in
                        let attributeValue = DynamoDBModel.AttributeValue(M: values)
                        
                        return try DynamoDBDecoder().decode(attributeValue)
                    }
                } catch {
                    throw error.asUnrecognizedDynamoDBTableError()
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
        } .flatMapErrorThrowing { error in
            if let typedError = error as? DynamoDBError {
                throw typedError.asDynamoDBTableError()
            }
            
            throw error.asUnrecognizedDynamoDBTableError()
        }
    }
    
#if (os(Linux) && compiler(>=5.5)) || (!os(Linux) && compiler(>=5.5.2)) && canImport(_Concurrency)
    func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?) async throws
    -> [CompositePrimaryKey<AttributesType>] {
        return try await partialQuery(forPartitionKey: partitionKey,
                                      sortKeyCondition: sortKeyCondition,
                                      exclusiveStartKey: nil)
    }
    
    // function to return a future with the results of a query call and all future paginated calls
    private func partialQuery<AttributesType>(
            forPartitionKey partitionKey: String,
            sortKeyCondition: AttributeCondition?,
            exclusiveStartKey: String?) async throws
    -> [CompositePrimaryKey<AttributesType>] {
        let paginatedItems: ([CompositePrimaryKey<AttributesType>], String?) =
            try await query(forPartitionKey: partitionKey,
                            sortKeyCondition: sortKeyCondition,
                            limit: nil,
                            scanIndexForward: true,
                            exclusiveStartKey: exclusiveStartKey)
        
        // if there are more items
        if let lastEvaluatedKey = paginatedItems.1 {
            // returns a future with all the results from all later paginated calls
            let partialResult: [CompositePrimaryKey<AttributesType>] = try await self.partialQuery(
                forPartitionKey: partitionKey,
                sortKeyCondition: sortKeyCondition,
                exclusiveStartKey: lastEvaluatedKey)
                
            // return the results from 'this' call and all later paginated calls
            return paginatedItems.0 + partialResult
        } else {
            // this is it, all results have been obtained
            return paginatedItems.0
        }
    }
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      exclusiveStartKey: String?) async throws
    -> ([CompositePrimaryKey<AttributesType>], String?)  {
        return try await query(forPartitionKey: partitionKey,
                               sortKeyCondition: sortKeyCondition,
                               limit: limit,
                               scanIndexForward: true,
                               exclusiveStartKey: exclusiveStartKey)
        
    }
    
    func query<AttributesType>(forPartitionKey partitionKey: String,
                                      sortKeyCondition: AttributeCondition?,
                                      limit: Int?,
                                      scanIndexForward: Bool,
                                      exclusiveStartKey: String?) async throws
    -> ([CompositePrimaryKey<AttributesType>], String?) {
        let queryInput = try DynamoDBModel.QueryInput.forSortKeyCondition(partitionKey: partitionKey, targetTableName: targetTableName,
                                                                          primaryKeyType: AttributesType.self,
                                                                          sortKeyCondition: sortKeyCondition, limit: limit,
                                                                          scanIndexForward: scanIndexForward, exclusiveStartKey: exclusiveStartKey,
                                                                          consistentRead: false)
        
        let logMessage = "dynamodb.query with partitionKey: \(partitionKey), " +
            "sortKeyCondition: \(sortKeyCondition.debugDescription), and table name \(targetTableName)."
        self.logger.trace("\(logMessage)")
        
        do {
            let queryOutput = try await self.dynamodb.query(input: queryInput)
            
            let lastEvaluatedKey: String?
            if let returnedLastEvaluatedKey = queryOutput.lastEvaluatedKey {
                let encodedLastEvaluatedKey: Data
                
                do {
                    encodedLastEvaluatedKey = try JSONEncoder().encode(returnedLastEvaluatedKey)
                } catch {
                    throw error.asUnrecognizedDynamoDBTableError()
                }
                
                lastEvaluatedKey = String(data: encodedLastEvaluatedKey, encoding: .utf8)
            } else {
                lastEvaluatedKey = nil
            }
            
            if let outputAttributeValues = queryOutput.items {
                let items: [CompositePrimaryKey<AttributesType>]
                
                do {
                    items = try outputAttributeValues.map { values in
                        let attributeValue = DynamoDBModel.AttributeValue(M: values)
                        
                        return try DynamoDBDecoder().decode(attributeValue)
                    }
                } catch {
                    throw error.asUnrecognizedDynamoDBTableError()
                }
                
                return (items, lastEvaluatedKey)
            } else {
                return ([], lastEvaluatedKey)
            }
        } catch {
            if let typedError = error as? DynamoDBError {
                throw typedError.asDynamoDBTableError()
            }
            
            throw error.asUnrecognizedDynamoDBTableError()
        }
    }
#endif
}
