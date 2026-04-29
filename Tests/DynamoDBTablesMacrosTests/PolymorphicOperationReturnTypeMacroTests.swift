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
        )
    ]

    // The expansion derives both `AttributesType` and `TimeToLiveAttributesType` from the first
    // case's parameter type and emits per-case `_assertCase_*` helpers that pin each case
    // parameter to `TypedTTLDatabaseItem<AttributesType, _, TimeToLiveAttributesType>`. The pin
    // catches both "wrong parameter shape" and "case attributes/TTL don't match the enum's" at
    // the user's case declaration. In real builds the helpers wrap their assertion in
    // `#sourceLocation(file:, line:)`; `BasicMacroExpansionContext` (used by `assertMacroExpansion`)
    // returns nil from `location(of:)` for detached nodes, so the test goldens see the
    // fallback (no `#sourceLocation` directives) path.
    func testExpansionWithStandardTypedDatabaseItem() {
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
                    typealias AttributesType = StandardTypedDatabaseItem<TestTypeA>.AttributesType
                    typealias TimeToLiveAttributesType = StandardTypedDatabaseItem<TestTypeA>.TimeToLiveAttributesType
                    static let types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self, TimeToLiveAttributesType>)] =
                    [(
                        StandardTypedDatabaseItem<TestTypeA>.RowType.self, .init {
                                .testTypeA($0)
                            }
                        ), (
                        StandardTypedDatabaseItem<TestTypeB>.RowType.self, .init {
                                .testTypeB($0)
                            }
                        ),]
                    private static func _assertCase_testTypeA() {
                        func _check<R: Codable & Sendable>(
                        _: TypedTTLDatabaseItem<AttributesType, R, TimeToLiveAttributesType>.Type
                        ) {
                        }
                        _check(StandardTypedDatabaseItem<TestTypeA>.self)
                    }
                    private static func _assertCase_testTypeB() {
                        func _check<R: Codable & Sendable>(
                        _: TypedTTLDatabaseItem<AttributesType, R, TimeToLiveAttributesType>.Type
                        ) {
                        }
                        _check(StandardTypedDatabaseItem<TestTypeB>.self)
                    }
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

    // The macro no longer performs a syntactic name check; typealiases that resolve to a
    // `TypedTTLDatabaseItem<...>` are handled transparently by the type checker via the
    // `_PolymorphicReturnTypeCaseParameter` protocol. The expanded source mirrors the case
    // parameter's syntactic form — the row-type is recovered at compile time via `.RowType`.
    func testExpansionWithUserTypealias() {
        assertMacroExpansion(
            """
            @PolymorphicOperationReturnType
            enum TestQueryableTypes {
                case testTypeA(MyAlias<TestTypeA>)
            }
            """,
            expandedSource: """
                enum TestQueryableTypes {
                    case testTypeA(MyAlias<TestTypeA>)
                }

                extension TestQueryableTypes: PolymorphicOperationReturnType {
                    typealias AttributesType = MyAlias<TestTypeA>.AttributesType
                    typealias TimeToLiveAttributesType = MyAlias<TestTypeA>.TimeToLiveAttributesType
                    static let types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self, TimeToLiveAttributesType>)] =
                    [(
                        MyAlias<TestTypeA>.RowType.self, .init {
                                .testTypeA($0)
                            }
                        ),]
                    private static func _assertCase_testTypeA() {
                        func _check<R: Codable & Sendable>(
                        _: TypedTTLDatabaseItem<AttributesType, R, TimeToLiveAttributesType>.Type
                        ) {
                        }
                        _check(MyAlias<TestTypeA>.self)
                    }
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

    // A typealias that resolves to a fully-specialised `TypedTTLDatabaseItem<...>` with no
    // remaining generic arguments. The old syntactic check rejected this shape because there
    // was no `genericArgumentClause` to extract a row type from; the new mechanism resolves
    // `.RowType` via the protocol conformance on the underlying `TypedTTLDatabaseItem`, so it
    // works transparently.
    func testExpansionWithFullyConcreteTypealias() {
        assertMacroExpansion(
            """
            @PolymorphicOperationReturnType
            enum TestQueryableTypes {
                case testTypeA(ConcreteAlias)
            }
            """,
            expandedSource: """
                enum TestQueryableTypes {
                    case testTypeA(ConcreteAlias)
                }

                extension TestQueryableTypes: PolymorphicOperationReturnType {
                    typealias AttributesType = ConcreteAlias.AttributesType
                    typealias TimeToLiveAttributesType = ConcreteAlias.TimeToLiveAttributesType
                    static let types: [(Codable.Type, PolymorphicOperationReturnOption<AttributesType, Self, TimeToLiveAttributesType>)] =
                    [(
                        ConcreteAlias.RowType.self, .init {
                                .testTypeA($0)
                            }
                        ),]
                    private static func _assertCase_testTypeA() {
                        func _check<R: Codable & Sendable>(
                        _: TypedTTLDatabaseItem<AttributesType, R, TimeToLiveAttributesType>.Type
                        ) {
                        }
                        _check(ConcreteAlias.self)
                    }
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
                )
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
                )
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
                    message:
                        "@PolymorphicOperationReturnType decorated enum can only have case entries with a single parameter.",
                    line: 3,
                    column: 10
                )
            ],
            macroSpecs: macroSpecs
        )
    }
}
