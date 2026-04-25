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
//  PolymorphicOperationReturnTypeMacroTests.swift
//  DynamoDBTablesMacrosTests
//

import SwiftSyntax
import SwiftSyntaxMacroExpansion
import SwiftSyntaxMacrosTestSupport
import XCTest

@testable import DynamoDBTablesMacros

final class PolymorphicOperationReturnTypeMacroTests: XCTestCase {
    private let macroSpecs: [String: MacroSpec] = [
        "PolymorphicOperationReturnType": MacroSpec(
            type: PolymorphicOperationReturnTypeMacro.self,
            conformances: ["PolymorphicOperationReturnType", "BatchCapableReturnType"]
        ),
    ]

    func testExpansionWithDefaultDatabaseItemType() {
        assertMacroExpansion(
            """
            @PolymorphicOperationReturnType
            enum TestQueryableTypes {
                case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
                case testTypeB(StandardTypedDatabaseItem<TestTypeB>)
            }
            """,
            expandedSource: """
            enum TestQueryableTypes {
                case testTypeA(StandardTypedDatabaseItem<TestTypeA>)
                case testTypeB(StandardTypedDatabaseItem<TestTypeB>)
            }

            extension TestQueryableTypes: PolymorphicOperationReturnType {
                typealias AttributesType = StandardPrimaryKeyAttributes
                typealias TimeToLiveAttributesType = StandardTimeToLiveAttributes
                static let types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self, TimeToLiveAttributesType>)] =
                [(
                    TestTypeA.self, .init {
                            .testTypeA($0)
                        }
                    ), (
                    TestTypeB.self, .init {
                            .testTypeB($0)
                        }
                    ),]
            }

            extension TestQueryableTypes: BatchCapableReturnType {
                func getItemKey() -> CompositePrimaryKey<AttributesType> {
                    switch self {
                    case let .testTypeA(databaseItem):
                        return databaseItem.compositePrimaryKey
                    case let .testTypeB(databaseItem):
                        return databaseItem.compositePrimaryKey
                    }
                }
            }
            """,
            macroSpecs: macroSpecs
        )
    }

    func testExpansionWithCustomDatabaseItemType() {
        assertMacroExpansion(
            """
            @PolymorphicOperationReturnType(databaseItemType: "CustomTypedDatabaseItem")
            enum TestQueryableTypes {
                case testTypeA(CustomTypedDatabaseItem<TestTypeA>)
            }
            """,
            expandedSource: """
            enum TestQueryableTypes {
                case testTypeA(CustomTypedDatabaseItem<TestTypeA>)
            }

            extension TestQueryableTypes: PolymorphicOperationReturnType {
                typealias AttributesType = StandardPrimaryKeyAttributes
                typealias TimeToLiveAttributesType = StandardTimeToLiveAttributes
                static let types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self, TimeToLiveAttributesType>)] =
                [(
                    TestTypeA.self, .init {
                            .testTypeA($0)
                        }
                    ),]
            }

            extension TestQueryableTypes: BatchCapableReturnType {
                func getItemKey() -> CompositePrimaryKey<AttributesType> {
                    switch self {
                    case let .testTypeA(databaseItem):
                        return databaseItem.compositePrimaryKey
                    }
                }
            }
            """,
            macroSpecs: macroSpecs
        )
    }

    func testDiagnosticWhenAttachedToStruct() {
        assertMacroExpansion(
            """
            @PolymorphicOperationReturnType
            struct NotAnEnum {
            }
            """,
            expandedSource: """
            struct NotAnEnum {
            }
            """,
            diagnostics: [
                DiagnosticSpec(
                    message: "@PolymorphicOperationReturnType must be attached to an enum declaration.",
                    line: 1,
                    column: 1
                ),
            ],
            macroSpecs: macroSpecs
        )
    }

    func testDiagnosticWhenEnumHasNoCases() {
        assertMacroExpansion(
            """
            @PolymorphicOperationReturnType
            enum Empty {
            }
            """,
            expandedSource: """
            enum Empty {
            }
            """,
            diagnostics: [
                DiagnosticSpec(
                    message: "@PolymorphicOperationReturnType decorated enum must be have at least a singe case.",
                    line: 1,
                    column: 1
                ),
            ],
            macroSpecs: macroSpecs
        )
    }

    func testDiagnosticWhenCaseParameterIsWrongType() {
        assertMacroExpansion(
            """
            @PolymorphicOperationReturnType
            enum BadTypes {
                case bad(SomeOtherType<TestTypeA>)
            }
            """,
            expandedSource: """
            enum BadTypes {
                case bad(SomeOtherType<TestTypeA>)
            }
            """,
            diagnostics: [
                DiagnosticSpec(
                    message:
                    "PolymorphicOperationReturnTypeMacro decorated enum cases parameter must be of StandardTypedDatabaseItem type.",
                    line: 3,
                    column: 10
                ),
            ],
            macroSpecs: macroSpecs
        )
    }

    func testDiagnosticWhenCaseHasMultipleParameters() {
        assertMacroExpansion(
            """
            @PolymorphicOperationReturnType
            enum BadTypes {
                case bad(StandardTypedDatabaseItem<TestTypeA>, String)
            }
            """,
            expandedSource: """
            enum BadTypes {
                case bad(StandardTypedDatabaseItem<TestTypeA>, String)
            }
            """,
            diagnostics: [
                DiagnosticSpec(
                    message: "@PolymorphicOperationReturnType decorated enum can only have case entries with a single parameter.",
                    line: 3,
                    column: 10
                ),
            ],
            macroSpecs: macroSpecs
        )
    }
}
