// swiftlint:disable cyclomatic_complexity
//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/main/Sources/SmokeDynamoDB/InMemoryDynamoDBCompositePrimaryKeyTableStore+execute.swift
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
//  InMemoryDynamoDBCompositePrimaryKeyTableStore+execute.swift
//  DynamoDBTables
//

import Foundation
import AWSDynamoDB

extension InMemoryDynamoDBCompositePrimaryKeyTableStore {
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?) throws
    -> [ReturnedType] {
        let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
           
        let returnedItems: [ReturnedType] = try items.map { item in
            return try self.convertToQueryableType(input: item)
        }
        
        return returnedItems
    }
    
    func execute<ReturnedType: PolymorphicOperationReturnType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?, nextToken: String?) throws
    -> (items: [ReturnedType], lastEvaluatedKey: String?)  {
        let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
           
        let returnedItems: [ReturnedType] = try items.map { item in
            return try self.convertToQueryableType(input: item)
        }
        
        return (returnedItems, nil)
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
            partitionKeys: [String],
            attributesFilter: [String]?,
            additionalWhereClause: String?) throws
    -> [TypedDatabaseItem<AttributesType, ItemType>] {
        let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
           
        let returnedItems: [TypedDatabaseItem<AttributesType, ItemType>] = try items.map { item in
            guard let typedItem = item as? TypedDatabaseItem<AttributesType, ItemType> else {
                let foundType = type(of: item)
                let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                    
                throw error
            }
                
            return typedItem
        }
        
        return returnedItems
    }
    
    func monomorphicExecute<AttributesType, ItemType>(
        partitionKeys: [String],
        attributesFilter: [String]?,
        additionalWhereClause: String?, nextToken: String?) throws
    -> (items: [TypedDatabaseItem<AttributesType, ItemType>], lastEvaluatedKey: String?) {
        let items = self.getExecuteItems(partitionKeys: partitionKeys, additionalWhereClause: additionalWhereClause)
           
        let returnedItems: [TypedDatabaseItem<AttributesType, ItemType>] = try items.map { item in
            guard let typedItem = item as? TypedDatabaseItem<AttributesType, ItemType> else {
                let foundType = type(of: item)
                let description = "Expected to decode \(TypedDatabaseItem<AttributesType, ItemType>.self). Instead found \(foundType)."
                let context = DecodingError.Context(codingPath: [], debugDescription: description)
                let error = DecodingError.typeMismatch(TypedDatabaseItem<AttributesType, ItemType>.self, context)
                    
                throw error
            }
                
            return typedItem
        }
        
        return (returnedItems, nil)
    }
    
    func getExecuteItems(partitionKeys: [String],
                         additionalWhereClause: String?) -> [PolymorphicOperationReturnTypeConvertable] {
        var items: [PolymorphicOperationReturnTypeConvertable] = []
        partitionKeys.forEach { partitionKey in
            guard let partition = self.store[partitionKey] else {
                // no such partition, continue
                return
            }
            
            partition.forEach { (sortKey, databaseItem) in
                // if there is an additional where clause
                if let additionalWhereClause = additionalWhereClause {
                    // there must be an executeItemFilter
                    if let executeItemFilter = self.executeItemFilter {
                        if executeItemFilter(partitionKey, sortKey, additionalWhereClause, databaseItem) {
                            // add if the filter says yes
                            items.append(databaseItem)
                        }
                    } else {
                        fatalError("An executeItemFilter must be provided when an excute call includes an additionalWhereClause")
                    }
                } else {
                    // otherwise just add the item
                    items.append(databaseItem)
                }
            }
        }
        
        return items
    }
}
