//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/smoke-dynamodb-3.x/Tests/SmokeDynamoDBTests/DynamoDBEncoderDecoderTests.swift
// Copyright 2018-2022 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// Licensed under Apache License v2.0
//
// Changes specified by
// https://github.com/swift-server-community/dynamo-db-tables/compare/9ab0e7a..main
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
//  DynamoDBEncoderDecoderTests.swift
//  DynamoDBTablesTests
//

import Foundation
import Testing

@testable import DynamoDBTables

struct CoreAccountAttributes: Codable {
    var description: String
    var mappedValues: [String: String]
    var notificationTargets: NotificationTargets
}

extension CoreAccountAttributes: Equatable {
    static func == (lhs: CoreAccountAttributes, rhs: CoreAccountAttributes) -> Bool {
        lhs.description == rhs.description && lhs.notificationTargets == rhs.notificationTargets
            && lhs.mappedValues == rhs.mappedValues
    }
}

struct NotificationTargets: Codable {
    var currentIDs: [String]
    var maximum: Int
}

extension NotificationTargets: Equatable {
    static func == (lhs: NotificationTargets, rhs: NotificationTargets) -> Bool {
        lhs.currentIDs == rhs.currentIDs && lhs.maximum == rhs.maximum
    }
}

typealias DatabaseItemType = StandardTypedDatabaseItem<CoreAccountAttributes>

struct DynamoDBEncoderDecoderTests {
    let partitionKey = "partitionKey"
    let sortKey = "sortKey"
    let attributes = CoreAccountAttributes(
        description: "Description",
        mappedValues: ["A": "one", "B": "two"],
        notificationTargets: NotificationTargets(currentIDs: [], maximum: 20)
    )

    @Test
    func encoderDecoder() throws {
        // create key and database item to create
        let key = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: sortKey)
        let newDatabaseItem: DatabaseItemType = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: self.attributes
        )

        let encodedAttributeValue = try DynamoDBEncoder().encode(newDatabaseItem)

        let output: DatabaseItemType = try DynamoDBDecoder().decode(encodedAttributeValue)

        #expect(newDatabaseItem.rowValue == output.rowValue)
        #expect(self.partitionKey == output.compositePrimaryKey.partitionKey)
        #expect(self.sortKey == output.compositePrimaryKey.sortKey)
        #expect(self.attributes == output.rowValue)
        #expect(output.timeToLive == nil)
    }

    @Test
    func encoderDecoderWithTimeToLive() throws {
        let timeToLiveTimestamp: Int64 = 123_456_789
        let timeToLive = StandardTimeToLive(timeToLiveTimestamp: timeToLiveTimestamp)

        // create key and database item to create
        let key = StandardCompositePrimaryKey(partitionKey: partitionKey, sortKey: sortKey)
        let newDatabaseItem: DatabaseItemType = StandardTypedDatabaseItem.newItem(
            withKey: key,
            andValue: self.attributes,
            andTimeToLive: timeToLive
        )

        let encodedAttributeValue = try DynamoDBEncoder().encode(newDatabaseItem)

        let output: DatabaseItemType = try DynamoDBDecoder().decode(encodedAttributeValue)

        #expect(newDatabaseItem.rowValue == output.rowValue)
        #expect(self.partitionKey == output.compositePrimaryKey.partitionKey)
        #expect(self.sortKey == output.compositePrimaryKey.sortKey)
        #expect(self.attributes == output.rowValue)
        #expect(timeToLiveTimestamp == output.timeToLive?.timeToLiveTimestamp)
    }

    @Test(
        "NaN and Infinity are rejected for Double and Float, at top level and nested",
        arguments: [
            Double.nan, .infinity, -.infinity,
        ]
    )
    func rejectsNonFiniteDouble(value: Double) throws {
        struct DoubleBox: Codable { let value: Double }
        struct DoubleListBox: Codable { let values: [Double] }

        #expect(throws: EncodingError.self) {
            try DynamoDBEncoder().encode(value)
        }
        #expect(throws: EncodingError.self) {
            try DynamoDBEncoder().encode(DoubleBox(value: value))
        }
        #expect(throws: EncodingError.self) {
            try DynamoDBEncoder().encode(DoubleListBox(values: [1.0, value]))
        }
    }

    @Test(
        "NaN and Infinity are rejected for Float, at top level and nested",
        arguments: [
            Float.nan, .infinity, -.infinity,
        ]
    )
    func rejectsNonFiniteFloat(value: Float) throws {
        struct FloatBox: Codable { let value: Float }
        struct FloatListBox: Codable { let values: [Float] }

        #expect(throws: EncodingError.self) {
            try DynamoDBEncoder().encode(value)
        }
        #expect(throws: EncodingError.self) {
            try DynamoDBEncoder().encode(FloatBox(value: value))
        }
        #expect(throws: EncodingError.self) {
            try DynamoDBEncoder().encode(FloatListBox(values: [1.0, value]))
        }
    }

    @Test
    func finiteFloatingPointStillEncodes() throws {
        struct Box: Codable, Equatable {
            let d: Double
            let f: Float
            let list: [Double]
        }
        let box = Box(d: 1.5, f: -2.25, list: [0.0, 3.14, -1.0])

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    // MARK: - Optional round-trip

    @Test
    func optionalFieldsRoundTrip() throws {
        struct Box: Codable, Equatable {
            let present: String?
            let absent: String?
            let presentInt: Int?
            let absentInt: Int?
        }
        let box = Box(present: "hello", absent: nil, presentInt: 42, absentInt: nil)

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    @Test
    func topLevelOptionalRoundTrip() throws {
        let some: String? = "hello"
        let encodedSome = try DynamoDBEncoder().encode(some)
        let decodedSome: String? = try DynamoDBDecoder().decode(encodedSome)
        #expect(decodedSome == some)

        let none: String? = nil
        let encodedNone = try DynamoDBEncoder().encode(none)
        let decodedNone: String? = try DynamoDBDecoder().decode(encodedNone)
        #expect(decodedNone == none)
    }

    // Known limitation: double-Optional `.some(.none)` collapses to `.none` on
    // decode because the outer wrapper emits `encodeNil()`, which is
    // indistinguishable from the outer value being absent. This matches the
    // behavior of `JSONEncoder` and is a `Codable` framework constraint, not a
    // bug in this encoder. Non-collapsing cases still round-trip.
    @Test
    func doubleOptionalRoundTrip() throws {
        struct Box: Codable, Equatable {
            let outerSomeInnerSome: String??
            let outerNone: String??
        }
        let box = Box(outerSomeInnerSome: .some(.some("hi")), outerNone: .none)

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    @Test
    func nestedOptionalStructRoundTrip() throws {
        struct Inner: Codable, Equatable { let value: Int }
        struct Outer: Codable, Equatable {
            let inner: Inner?
            let missing: Inner?
        }
        let box = Outer(inner: Inner(value: 7), missing: nil)

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Outer = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    // MARK: - Empty containers

    @Test
    func emptyStructRoundTrip() throws {
        struct Empty: Codable, Equatable {}
        let encoded = try DynamoDBEncoder().encode(Empty())
        let decoded: Empty = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == Empty())
    }

    @Test
    func emptyCollectionsRoundTrip() throws {
        struct Box: Codable, Equatable {
            let list: [Int]
            let map: [String: Int]
        }
        let box = Box(list: [], map: [:])

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    // MARK: - Date / Data / Decimal

    @Test
    func dateRoundTrip() throws {
        struct Box: Codable, Equatable {
            let date: Date
        }
        // Round to whole seconds so ISO8601 stringification is lossless.
        let box = Box(date: Date(timeIntervalSince1970: 1_700_000_000))

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    @Test
    func dataRoundTrip() throws {
        struct Box: Codable, Equatable {
            let payload: Data
        }
        let box = Box(payload: Data([0x00, 0x01, 0x02, 0xFF]))

        let encoded = try DynamoDBEncoder().encode(box)

        // Verify Data encodes to the DynamoDB binary type (B), not a list of bytes.
        if case let .m(attrs) = encoded {
            if case .b = attrs["payload"] {
                // expected
            } else {
                Issue.record("Expected .b for Data, got \(String(describing: attrs["payload"]))")
            }
        }

        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    @Test
    func dataEmptyRoundTrip() throws {
        struct Box: Codable, Equatable {
            let payload: Data
        }
        let box = Box(payload: Data())

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    @Test
    func decimalRoundTrip() throws {
        struct Box: Codable, Equatable {
            let amount: Decimal
        }
        let box = Box(amount: Decimal(string: "123.456")!)

        let encoded = try DynamoDBEncoder().encode(box)

        // Verify Decimal encodes to the DynamoDB number type (N), not a keyed struct.
        if case let .m(attrs) = encoded {
            if case let .n(numString) = attrs["amount"] {
                #expect(numString == "123.456")
            } else {
                Issue.record("Expected .n for Decimal, got \(String(describing: attrs["amount"]))")
            }
        }

        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    @Test
    func decimalLargeValueRoundTrip() throws {
        struct Box: Codable, Equatable {
            let big: Decimal
            let precise: Decimal
            let negative: Decimal
        }
        let box = Box(
            big: Decimal(string: "99999999999999999999999999999")!,
            precise: Decimal(string: "0.000000000000001")!,
            negative: Decimal(string: "-42.5")!
        )

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    // MARK: - Int64 / UInt64 precision

    @Test
    func int64BoundariesRoundTrip() throws {
        struct Box: Codable, Equatable {
            let max: Int64
            let min: Int64
            let umax: UInt64
        }
        let box = Box(max: .max, min: .min, umax: .max)

        let encoded = try DynamoDBEncoder().encode(box)
        let decoded: Box = try DynamoDBDecoder().decode(encoded)
        #expect(decoded == box)
    }

    // MARK: - attributeNameTransform symmetry

    @Test
    func attributeNameTransformRoundTrip() throws {
        struct Box: Codable, Equatable {
            let firstName: String
            let lastName: String
            let age: Int
        }
        let box = Box(firstName: "Ada", lastName: "Lovelace", age: 36)

        let transform: (String) -> String = { $0.uppercased() }
        let encoded = try DynamoDBEncoder(attributeNameTransform: transform).encode(box)

        // Confirm the transform was actually applied on the wire.
        if case let .m(attrs) = encoded {
            #expect(attrs["FIRSTNAME"] != nil)
            #expect(attrs["firstName"] == nil)
        } else {
            Issue.record("Expected map attribute value, got \(encoded)")
        }

        let decoded: Box = try DynamoDBDecoder(attributeNameTransform: transform).decode(encoded)
        #expect(decoded == box)
    }
}
