//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// Copyright (c) 2026 the DynamoDBTables authors
// Licensed under Apache License v2.0
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  DynamoDBModel.swift
//  DynamoDBTables
//

import Foundation

// swiftlint:disable type_body_length
public enum DynamoDBModel {
    // MARK: - Core Types

    // swiftlint:disable identifier_name
    public indirect enum AttributeValue: Sendable, Equatable, Encodable, Decodable {
        case b(Data)
        case bool(Bool)
        case bs([Data])
        case l([AttributeValue])
        case m([String: AttributeValue])
        case n(String)
        case ns([String])
        case null(Bool)
        case s(String)
        case ss([String])
        case sdkUnknown(String)

        enum CodingKeys: Swift.String, Swift.CodingKey {
            case b = "B"
            case bool = "BOOL"
            case bs = "BS"
            case l = "L"
            case m = "M"
            case n = "N"
            case ns = "NS"
            case null = "NULL"
            case s = "S"
            case ss = "SS"
            case sdkUnknown
        }

        public func encode(to encoder: Swift.Encoder) throws {
            var container = encoder.container(keyedBy: CodingKeys.self)
            switch self {
            case let .b(b):
                try container.encode(b.base64EncodedString(), forKey: .b)
            case let .bool(bool):
                try container.encode(bool, forKey: .bool)
            case let .bs(bs):
                var bsContainer = container.nestedUnkeyedContainer(forKey: .bs)
                for binaryattributevalue0 in bs {
                    try bsContainer.encode(binaryattributevalue0.base64EncodedString())
                }
            case let .l(l):
                var lContainer = container.nestedUnkeyedContainer(forKey: .l)
                for attributevalue0 in l {
                    try lContainer.encode(attributevalue0)
                }
            case let .m(m):
                var mContainer = container.nestedContainer(keyedBy: Key.self, forKey: .m)
                for (dictKey0, mapAttributeValue0) in m {
                    try mContainer.encode(mapAttributeValue0, forKey: Key(stringValue: dictKey0))
                }
            case let .n(n):
                try container.encode(n, forKey: .n)
            case let .ns(ns):
                var nsContainer = container.nestedUnkeyedContainer(forKey: .ns)
                for numberattributevalue0 in ns {
                    try nsContainer.encode(numberattributevalue0)
                }
            case let .null(null):
                try container.encode(null, forKey: .null)
            case let .s(s):
                try container.encode(s, forKey: .s)
            case let .ss(ss):
                var ssContainer = container.nestedUnkeyedContainer(forKey: .ss)
                for stringattributevalue0 in ss {
                    try ssContainer.encode(stringattributevalue0)
                }
            case let .sdkUnknown(sdkUnknown):
                try container.encode(sdkUnknown, forKey: .sdkUnknown)
            }
        }

        // swiftlint:disable cyclomatic_complexity function_body_length
        public init(from decoder: Swift.Decoder) throws {
            let values = try decoder.container(keyedBy: CodingKeys.self)
            let sDecoded = try values.decodeIfPresent(Swift.String.self, forKey: .s)
            if let s = sDecoded {
                self = .s(s)
                return
            }
            let nDecoded = try values.decodeIfPresent(Swift.String.self, forKey: .n)
            if let n = nDecoded {
                self = .n(n)
                return
            }
            let bDecoded = try values.decodeIfPresent(Foundation.Data.self, forKey: .b)
            if let b = bDecoded {
                self = .b(b)
                return
            }
            let ssContainer = try values.decodeIfPresent([Swift.String?].self, forKey: .ss)
            var ssDecoded0: [Swift.String]? = nil
            if let ssContainer {
                ssDecoded0 = [Swift.String]()
                for string0 in ssContainer {
                    if let string0 {
                        ssDecoded0?.append(string0)
                    }
                }
            }
            if let ss = ssDecoded0 {
                self = .ss(ss)
                return
            }
            let nsContainer = try values.decodeIfPresent([Swift.String?].self, forKey: .ns)
            var nsDecoded0: [Swift.String]? = nil
            if let nsContainer {
                nsDecoded0 = [Swift.String]()
                for string0 in nsContainer {
                    if let string0 {
                        nsDecoded0?.append(string0)
                    }
                }
            }
            if let ns = nsDecoded0 {
                self = .ns(ns)
                return
            }
            let bsContainer = try values.decodeIfPresent([Foundation.Data?].self, forKey: .bs)
            var bsDecoded0: [Foundation.Data]? = nil
            if let bsContainer {
                bsDecoded0 = [Foundation.Data]()
                for blob0 in bsContainer {
                    if let blob0 {
                        bsDecoded0?.append(blob0)
                    }
                }
            }
            if let bs = bsDecoded0 {
                self = .bs(bs)
                return
            }
            let mContainer = try values.decodeIfPresent(
                [Swift.String: AttributeValue?].self,
                forKey: .m
            )
            var mDecoded0: [Swift.String: AttributeValue]? = nil
            if let mContainer {
                mDecoded0 = [Swift.String: AttributeValue]()
                for (key0, attributevalue0) in mContainer {
                    if let attributevalue0 {
                        mDecoded0?[key0] = attributevalue0
                    }
                }
            }
            if let m = mDecoded0 {
                self = .m(m)
                return
            }
            let lContainer = try values.decodeIfPresent([AttributeValue?].self, forKey: .l)
            var lDecoded0: [AttributeValue]? = nil
            if let lContainer {
                lDecoded0 = [AttributeValue]()
                for union0 in lContainer {
                    if let union0 {
                        lDecoded0?.append(union0)
                    }
                }
            }
            if let l = lDecoded0 {
                self = .l(l)
                return
            }
            let nullDecoded = try values.decodeIfPresent(Swift.Bool.self, forKey: .null)
            if let null = nullDecoded {
                self = .null(null)
                return
            }
            let boolDecoded = try values.decodeIfPresent(Swift.Bool.self, forKey: .bool)
            if let bool = boolDecoded {
                self = .bool(bool)
                return
            }
            self = .sdkUnknown("")
        }
        // swiftlint:enable cyclomatic_complexity function_body_length identifier_name
    }

    package struct KeysAndAttributes: Sendable, Equatable {
        package let keys: [[String: AttributeValue]]
        package let consistentRead: Bool?
        package let attributesToGet: [String]?
        package let projectionExpression: String?
        package let expressionAttributeNames: [String: String]?

        package init(
            attributesToGet: [String]? = nil,
            consistentRead: Bool? = nil,
            expressionAttributeNames: [String: String]? = nil,
            keys: [[String: AttributeValue]] = [],
            projectionExpression: String? = nil
        ) {
            self.keys = keys
            self.consistentRead = consistentRead
            self.attributesToGet = attributesToGet
            self.projectionExpression = projectionExpression
            self.expressionAttributeNames = expressionAttributeNames
        }
    }

    package struct BatchStatementRequest: Sendable, Equatable {
        package let statement: String?
        package let consistentRead: Bool?
        package let parameters: [AttributeValue]?

        package init(
            consistentRead: Bool? = nil,
            parameters: [AttributeValue]? = nil,
            statement: String? = nil
        ) {
            self.statement = statement
            self.consistentRead = consistentRead
            self.parameters = parameters
        }
    }

    package struct BatchStatementResponse: Sendable {
        package let error: BatchStatementError?
        package let item: [String: AttributeValue]?
        package let tableName: String?

        package init(
            error: BatchStatementError? = nil,
            item: [String: AttributeValue]? = nil,
            tableName: String? = nil
        ) {
            self.error = error
            self.item = item
            self.tableName = tableName
        }
    }

    package struct BatchStatementError: Sendable {
        package let code: BatchStatementErrorCode?
        package let message: String?

        package init(
            code: BatchStatementErrorCode? = nil,
            message: String? = nil
        ) {
            self.code = code
            self.message = message
        }
    }

    package enum BatchStatementErrorCode: Sendable, Equatable {
        case accessdenied
        case conditionalcheckfailed
        case duplicateitem
        case internalservererror
        case itemcollectionsizelimitexceeded
        case provisionedthroughputexceeded
        case requestlimitexceeded
        case resourcenotfound
        case throttlingerror
        case transactionconflict
        case validationerror
        case sdkUnknown(String)
    }

    package struct ParameterizedStatement: Sendable, Equatable {
        package let statement: String?
        package let parameters: [AttributeValue]?

        package init(
            parameters: [AttributeValue]? = nil,
            statement: String? = nil
        ) {
            self.statement = statement
            self.parameters = parameters
        }
    }

    package struct CancellationReason: Sendable {
        package let code: String?
        package let item: [String: AttributeValue]?
        package let message: String?

        package init(
            code: String? = nil,
            item: [String: AttributeValue]? = nil,
            message: String? = nil
        ) {
            self.code = code
            self.item = item
            self.message = message
        }
    }

    // MARK: - Input Types

    package struct PutItemInput: Sendable, Equatable {
        package let conditionExpression: String?
        package let expressionAttributeNames: [String: String]?
        package let expressionAttributeValues: [String: AttributeValue]?
        package let item: [String: AttributeValue]
        package let tableName: String

        package init(
            conditionExpression: String? = nil,
            expressionAttributeNames: [String: String]? = nil,
            expressionAttributeValues: [String: AttributeValue]? = nil,
            item: [String: AttributeValue],
            tableName: String
        ) {
            self.conditionExpression = conditionExpression
            self.expressionAttributeNames = expressionAttributeNames
            self.expressionAttributeValues = expressionAttributeValues
            self.item = item
            self.tableName = tableName
        }
    }

    package struct GetItemInput: Sendable, Equatable {
        package let consistentRead: Bool?
        package let key: [String: AttributeValue]
        package let tableName: String

        package init(
            consistentRead: Bool? = nil,
            key: [String: AttributeValue],
            tableName: String
        ) {
            self.consistentRead = consistentRead
            self.key = key
            self.tableName = tableName
        }
    }

    package struct DeleteItemInput: Sendable, Equatable {
        package let conditionExpression: String?
        package let expressionAttributeNames: [String: String]?
        package let expressionAttributeValues: [String: AttributeValue]?
        package let key: [String: AttributeValue]
        package let tableName: String

        package init(
            conditionExpression: String? = nil,
            expressionAttributeNames: [String: String]? = nil,
            expressionAttributeValues: [String: AttributeValue]? = nil,
            key: [String: AttributeValue],
            tableName: String
        ) {
            self.conditionExpression = conditionExpression
            self.expressionAttributeNames = expressionAttributeNames
            self.expressionAttributeValues = expressionAttributeValues
            self.key = key
            self.tableName = tableName
        }
    }

    package struct QueryInput: Sendable, Equatable {
        package let consistentRead: Bool?
        package let exclusiveStartKey: [String: AttributeValue]?
        package let expressionAttributeNames: [String: String]?
        package let expressionAttributeValues: [String: AttributeValue]?
        package let indexName: String?
        package let keyConditionExpression: String?
        package let limit: Int?
        package let scanIndexForward: Bool?
        package let tableName: String

        package init(
            consistentRead: Bool? = nil,
            exclusiveStartKey: [String: AttributeValue]? = nil,
            expressionAttributeNames: [String: String]? = nil,
            expressionAttributeValues: [String: AttributeValue]? = nil,
            indexName: String? = nil,
            keyConditionExpression: String? = nil,
            limit: Int? = nil,
            scanIndexForward: Bool? = nil,
            tableName: String
        ) {
            self.consistentRead = consistentRead
            self.exclusiveStartKey = exclusiveStartKey
            self.expressionAttributeNames = expressionAttributeNames
            self.expressionAttributeValues = expressionAttributeValues
            self.indexName = indexName
            self.keyConditionExpression = keyConditionExpression
            self.limit = limit
            self.scanIndexForward = scanIndexForward
            self.tableName = tableName
        }
    }

    package struct BatchGetItemInput: Sendable, Equatable {
        package let requestItems: [String: KeysAndAttributes]?

        package init(
            requestItems: [String: KeysAndAttributes]? = nil
        ) {
            self.requestItems = requestItems
        }
    }

    package struct BatchExecuteStatementInput: Sendable, Equatable {
        package let statements: [BatchStatementRequest]?

        package init(
            statements: [BatchStatementRequest]? = nil
        ) {
            self.statements = statements
        }
    }

    package struct ExecuteStatementInput: Sendable, Equatable {
        package let consistentRead: Bool?
        package let nextToken: String?
        package let statement: String

        package init(
            consistentRead: Bool? = nil,
            nextToken: String? = nil,
            statement: String
        ) {
            self.consistentRead = consistentRead
            self.nextToken = nextToken
            self.statement = statement
        }
    }

    package struct ExecuteTransactionInput: Sendable, Equatable {
        package let transactStatements: [ParameterizedStatement]?

        package init(
            transactStatements: [ParameterizedStatement]? = nil
        ) {
            self.transactStatements = transactStatements
        }
    }

    // MARK: - Output Types

    package struct GetItemOutput: Sendable {
        package let item: [String: AttributeValue]?

        package init(
            item: [String: AttributeValue]? = nil
        ) {
            self.item = item
        }
    }

    package struct QueryOutput: Sendable {
        package let items: [[String: AttributeValue]]?
        package let lastEvaluatedKey: [String: AttributeValue]?

        package init(
            items: [[String: AttributeValue]]? = nil,
            lastEvaluatedKey: [String: AttributeValue]? = nil
        ) {
            self.items = items
            self.lastEvaluatedKey = lastEvaluatedKey
        }
    }

    package struct BatchGetItemOutput: Sendable {
        package let responses: [String: [[String: AttributeValue]]]?
        package let unprocessedKeys: [String: KeysAndAttributes]?

        package init(
            responses: [String: [[String: AttributeValue]]]? = nil,
            unprocessedKeys: [String: KeysAndAttributes]? = nil
        ) {
            self.responses = responses
            self.unprocessedKeys = unprocessedKeys
        }
    }

    package struct BatchExecuteStatementOutput: Sendable {
        package let responses: [BatchStatementResponse]?

        package init(
            responses: [BatchStatementResponse]? = nil
        ) {
            self.responses = responses
        }
    }

    package struct ExecuteStatementOutput: Sendable {
        package let items: [[String: AttributeValue]]?
        package let nextToken: String?

        package init(
            items: [[String: AttributeValue]]? = nil,
            nextToken: String? = nil
        ) {
            self.items = items
            self.nextToken = nextToken
        }
    }
}
// swiftlint:enable type_body_length

package struct Key: CodingKey {
    package let stringValue: String
    package init(stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    package init(_ stringValue: String) {
        self.stringValue = stringValue
        self.intValue = nil
    }

    package let intValue: Int?
    package init?(intValue _: Int) {
        nil
    }
}
