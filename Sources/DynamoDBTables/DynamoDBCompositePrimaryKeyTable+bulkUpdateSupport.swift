//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Sources/SmokeDynamoDB/DynamoDBCompositePrimaryKeyTable+bulkUpdateSupport.swift
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
//  DynamoDBCompositePrimaryKeyTable+bulkUpdateSupport.swift
//  DynamoDBTables
//

import Foundation
import Logging
import AWSDynamoDB

internal enum AttributeDifference: Equatable {
    case update(path: String, value: String)
    case remove(path: String)
    case listAppend(path: String, value: String)
    
    var path: String {
        switch self {
        case .update(path: let path, value: _):
            return path
        case .remove(path: let path):
            return path
        case .listAppend(path: let path, value: _):
            return path
        }
    }
}

extension DynamoDBCompositePrimaryKeyTable {
    
    func getAttributes<AttributesType, ItemType>(forItem item: TypedDatabaseItem<AttributesType, ItemType>) throws
        -> [String: DynamoDBClientTypes.AttributeValue] {
            let attributeValue = try DynamoDBEncoder().encode(item)

            let attributes: [String: DynamoDBClientTypes.AttributeValue]
            if case .m(let itemAttributes) = attributeValue {
                attributes = itemAttributes
            } else {
                throw DynamoDBTableError.unexpectedResponse(reason: "Expected a map.")
            }

            return attributes
    }
    
    func getUpdateExpression<AttributesType, ItemType>(tableName: String,
                                                       newItem: TypedDatabaseItem<AttributesType, ItemType>,
                                                       existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> String {
        let attributeDifferences = try diffItems(newItem: newItem,
                                                 existingItem: existingItem)
        
        // according to https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.update.html
        let elements = attributeDifferences.map { attributeDifference -> String in
            switch attributeDifference {
            case .update(path: let path, value: let value):
                return "SET \(path)=\(value)"
            case .remove(path: let path):
                return "REMOVE \(path)"
            case .listAppend(path: let path, value: let value):
                return "SET \(path)=list_append(\(path),\(value))"
            }
        }
        
        let combinedElements = elements.joined(separator: " ")
        
        return "UPDATE \"\(tableName)\" \(combinedElements) "
            + "WHERE \(AttributesType.partitionKeyAttributeName)='\(sanitizeString(newItem.compositePrimaryKey.partitionKey))' "
            + "AND \(AttributesType.sortKeyAttributeName)='\(sanitizeString(newItem.compositePrimaryKey.sortKey))' "
            + "AND \(RowStatus.CodingKeys.rowVersion.rawValue)=\(existingItem.rowStatus.rowVersion)"
    }
    
    func getInsertExpression<AttributesType, ItemType>(tableName: String,
                                                       newItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> String {
        let newAttributes = try getAttributes(forItem: newItem)
        let flattenedAttribute = try getFlattenedMapAttribute(attribute: newAttributes)
        
        // according to https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.insert.html
        return "INSERT INTO \"\(tableName)\" value \(flattenedAttribute)"
    }
    
    func getDeleteExpression<ItemType: DatabaseItem>(tableName: String,
                                                     existingItem: ItemType) throws -> String {
        return "DELETE FROM \"\(tableName)\" "
            + "WHERE \(ItemType.AttributesType.partitionKeyAttributeName)='\(sanitizeString(existingItem.compositePrimaryKey.partitionKey))' "
            + "AND \(ItemType.AttributesType.sortKeyAttributeName)='\(sanitizeString(existingItem.compositePrimaryKey.sortKey))' "
            + "AND \(RowStatus.CodingKeys.rowVersion.rawValue)=\(existingItem.rowStatus.rowVersion)"
    }
    
    func getDeleteExpression<AttributesType>(tableName: String,
                                             existingKey: CompositePrimaryKey<AttributesType>) throws -> String {
        return "DELETE FROM \"\(tableName)\" "
            + "WHERE \(AttributesType.partitionKeyAttributeName)='\(sanitizeString(existingKey.partitionKey))' "
            + "AND \(AttributesType.sortKeyAttributeName)='\(sanitizeString(existingKey.sortKey))'"
    }
    
    func getExistsExpression<AttributesType, ItemType>(tableName: String,
                                                       existingItem: TypedDatabaseItem<AttributesType, ItemType>) -> String {
        // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-functions.exists.html
        return "EXISTS("
            + "SELECT * FROM \"\(tableName)\" "
            + "WHERE \(AttributesType.partitionKeyAttributeName)='\(sanitizeString(existingItem.compositePrimaryKey.partitionKey))' "
            + "AND \(AttributesType.sortKeyAttributeName)='\(sanitizeString(existingItem.compositePrimaryKey.sortKey))' "
            + "AND \(RowStatus.CodingKeys.rowVersion.rawValue)=\(existingItem.rowStatus.rowVersion)"
            + ")"
    }
    
    /*
     Function to return the differences between two items. This is used to then create an UPDATE
     query that just specifies the values that are changing.
     */
    func diffItems<AttributesType, ItemType>(
                newItem: TypedDatabaseItem<AttributesType, ItemType>,
                existingItem: TypedDatabaseItem<AttributesType, ItemType>) throws -> [AttributeDifference] {
        let newAttributes = try getAttributes(forItem: newItem)
        let existingAttributes = try getAttributes(forItem: existingItem)
        
        return try diffMapAttribute(path: nil, newAttribute: newAttributes, existingAttribute: existingAttributes)
    }
    
    private func diffAttribute(path: String,
                               newAttribute: DynamoDBClientTypes.AttributeValue,
                               existingAttribute: DynamoDBClientTypes.AttributeValue) throws -> [AttributeDifference] {
        switch (newAttribute, existingAttribute) {
        case (.b, .b):
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle Binary types.")
        case (.bool(let newTypedAttribute), .bool(let existingTypedAttribute)):
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: String(newTypedAttribute))]
            }
        case (.bs, .bs):
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle Binary Set types.")
        case (.l(let newTypedAttribute), .l(let existingTypedAttribute)):
            return try diffListAttribute(path: path, newAttribute: newTypedAttribute, existingAttribute: existingTypedAttribute)
        case (.m(let newTypedAttribute), .m(let existingTypedAttribute)):
            return try diffMapAttribute(path: path, newAttribute: newTypedAttribute, existingAttribute: existingTypedAttribute)
        case (.n(let newTypedAttribute), .n(let existingTypedAttribute)):
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: String(newTypedAttribute))]
            }
        case (.ns, .ns):
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle Number Set types.")
        case (.null, .null):
            // always equal
            return []
        case (.s(let newTypedAttribute), .s(let existingTypedAttribute)):
            if newTypedAttribute != existingTypedAttribute {
                return [.update(path: path, value: "'\(sanitizeString(newTypedAttribute))'")]
            }
        case (.ss, .ss):
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle String Set types.")
        default:
            // new value is a different type and could be replaced
            return try updateAttribute(newPath: path, attribute: newAttribute)
        }
        
        // no change
        return []
    }
    
    private func diffListAttribute(path: String,
                                   newAttribute: [DynamoDBClientTypes.AttributeValue],
                                   existingAttribute: [DynamoDBClientTypes.AttributeValue]) throws -> [AttributeDifference] {
        let maxIndex = max(newAttribute.count, existingAttribute.count)
        
        return try (0..<maxIndex).flatMap { index -> [AttributeDifference] in
            let newPath = "\(path)[\(index)]"

            // if both new and existing attributes are present
            if index < newAttribute.count && index < existingAttribute.count {
                return try diffAttribute(path: newPath, newAttribute: newAttribute[index], existingAttribute: existingAttribute[index])
            } else if index < existingAttribute.count {
                return [.remove(path: newPath)]
            } else if index < newAttribute.count {
                return try updateAttribute(newPath: newPath, attribute: newAttribute[index])
            }
            
            return []
        }
    }
    
    private func diffMapAttribute(path: String?,
                                  newAttribute: [String: DynamoDBClientTypes.AttributeValue],
                                  existingAttribute: [String: DynamoDBClientTypes.AttributeValue]) throws -> [AttributeDifference] {
        var combinedMap: [String: (new: DynamoDBClientTypes.AttributeValue?, existing: DynamoDBClientTypes.AttributeValue?)] = [:]

        newAttribute.forEach { (key, attribute) in
            var existingEntry = combinedMap[key] ?? (nil, nil)
            existingEntry.new = attribute
            combinedMap[key] = existingEntry
        }
        
        existingAttribute.forEach { (key, attribute) in
            var existingEntry = combinedMap[key] ?? (nil, nil)
            existingEntry.existing = attribute
            combinedMap[key] = existingEntry
        }
        
        return try combinedMap.flatMap { (key, attribute) -> [AttributeDifference] in
            let newPath = combinePath(basePath: path, newComponent: key)
            
            // if both new and existing attributes are present
            if let new = attribute.new, let existing = attribute.existing {
                return try diffAttribute(path: newPath, newAttribute: new, existingAttribute: existing)
            } else if attribute.existing != nil {
                return [.remove(path: newPath)]
            } else if let new = attribute.new {
                return try updateAttribute(newPath: newPath, attribute: new)
            } else {
                return []
            }
        }
    }
    
    private func combinePath(basePath: String?, newComponent: String) -> String {
        if let basePath = basePath {
            return "\(basePath).\"\(newComponent)\""
        } else {
            return "\"\(newComponent)\""
        }
    }
    
    private func updateAttribute(newPath: String, attribute: DynamoDBClientTypes.AttributeValue) throws -> [AttributeDifference] {
        if let newValue = try getFlattenedAttribute(attribute: attribute) {
            return [.update(path: newPath, value: newValue)]
        } else {
            return [.remove(path: newPath)]
        }
    }
    
    func getFlattenedAttribute(attribute: DynamoDBClientTypes.AttributeValue) throws -> String? {
        switch attribute {
        case .b:
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle Binary types.")
        case .bool(let typedAttribute):
            return String(typedAttribute)
        case .bs:
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle Binary Set types.")
        case .l(let typedAttribute):
            return try getFlattenedListAttribute(attribute: typedAttribute)
        case .m(let typedAttribute):
            return try getFlattenedMapAttribute(attribute: typedAttribute)
        case .n(let typedAttribute):
            return String(typedAttribute)
        case .ns:
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle Number Set types.")
        case .null:
            return nil
        case .s(let typedAttribute):
            return "'\(sanitizeString(typedAttribute))'"
        case .ss:
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle String Set types.")
        case .sdkUnknown(let payload):
            throw DynamoDBTableError.unableToUpdateError(reason: "Unable to handle unknown type: '\(payload)'.")
        }
    }
    
    private func getFlattenedListAttribute(attribute: [DynamoDBClientTypes.AttributeValue]) throws -> String {
        let elements: [String] = try attribute.compactMap { nestedAttribute in
            return try getFlattenedAttribute(attribute: nestedAttribute)
        }
        
        let joinedElements = elements.joined(separator: ", ")
        return "[\(joinedElements)]"
    }
    
    private func getFlattenedMapAttribute(attribute: [String: DynamoDBClientTypes.AttributeValue]) throws -> String {
        let elements: [String] = try attribute.compactMap { (key, nestedAttribute) in
            guard let flattenedNestedAttribute = try getFlattenedAttribute(attribute: nestedAttribute) else {
                return nil
            }
            
            return "'\(key)': \(flattenedNestedAttribute)"
        }
        
        let joinedElements = elements.joined(separator: ", ")
        return "{\(joinedElements)}"
    }

    /// In PartiQL single quotes indicate start and end of a string attribute.
    /// If, however, the string itself contains a single quote then the database
    /// does not know where the string should end. Therefore, need to escape
    /// single quote by doubling it. E.g. 'foo'bar' becomes 'foo''bar'.
    private func sanitizeString(_ string: String) -> String {
        if self.escapeSingleQuoteInPartiQL {
            return string.replacingOccurrences(of: "'", with: "''")
        } else {
            return string
        }
    }
}
