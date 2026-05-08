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
//  PolymorphicWriteEntryMacroTests.swift
//  DynamoDBTablesMacrosTests
//

import SwiftSyntax
import SwiftSyntaxMacroExpansion
import SwiftSyntaxMacrosTestSupport
import XCTest

@testable import DynamoDBTablesMacros

final class PolymorphicWriteEntryMacroTests: XCTestCase {
    private let macroSpecs: [String: MacroSpec] = [
        "PolymorphicWriteEntry": MacroSpec(
            type: PolymorphicWriteEntryMacro.self,
            conformances: ["PolymorphicWriteEntry"]
        )
    ]

    // The expansion derives `AttributesType` from the first case's parameter type and emits
    // per-case `_assertCase_*` helpers that pin each case parameter to
    // `WriteEntry<AttributesType, _, _>`. The pin catches both "wrong parameter shape" and
    // "case attributes don't match the enum's" at the user's case declaration. The helpers
    // wrap their assertion in `#sourceLocation(file:, line:)` so a diagnostic surfaces at the
    // user's enum case rather than the macro-generated buffer. swift-syntax 602+'s
    // `BasicMacroExpansionContext` (used by `assertMacroExpansion`) returns a synthesized
    // `TestModule/test.swift` location for inputs, so the goldens reflect the production path.
    func testExpansionWithTwoCases() {
        assertMacroExpansion(
            """
            @PolymorphicWriteEntry
            enum TestEntry {
                case testTypeA(TestTypeAWriteEntry)
                case testTypeB(TestTypeBWriteEntry)
            }
            """,
            expandedSource: """
                enum TestEntry {
                    case testTypeA(TestTypeAWriteEntry)
                    case testTypeB(TestTypeBWriteEntry)
                }

                extension TestEntry: PolymorphicWriteEntry {
                    typealias AttributesType = TestTypeAWriteEntry.AttributesType
                    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteEntryTransformType {
                        switch self {
                        case let .testTypeA(writeEntry):
                            return try context.transform(writeEntry)
                        case let .testTypeB(writeEntry):
                            return try context.transform(writeEntry)
                        }
                    }
                    var compositePrimaryKey: CompositePrimaryKey<AttributesType> {
                        switch self {
                        case let .testTypeA(writeEntry):
                            return writeEntry.compositePrimaryKey
                        case let .testTypeB(writeEntry):
                            return writeEntry.compositePrimaryKey
                        }
                    }
                    private static func _assertCase_testTypeA() {
                        func _check<R: Codable & Sendable, T: TimeToLiveAttributes>(
                        _: WriteEntry<AttributesType, R, T>.Type
                        ) {
                        }
                        #sourceLocation(file: "TestModule/test.swift", line: 3)
                        _check(TestTypeAWriteEntry.self)
                        #sourceLocation()
                    }
                    private static func _assertCase_testTypeB() {
                        func _check<R: Codable & Sendable, T: TimeToLiveAttributes>(
                        _: WriteEntry<AttributesType, R, T>.Type
                        ) {
                        }
                        #sourceLocation(file: "TestModule/test.swift", line: 4)
                        _check(TestTypeBWriteEntry.self)
                        #sourceLocation()
                    }
                }
                """,
            macroSpecs: macroSpecs
        )
    }

    func testDiagnosticWhenAttachedToStruct() {
        assertMacroExpansion(
            """
            @PolymorphicWriteEntry
            struct NotAnEnum {
            }
            """,
            expandedSource: """
                struct NotAnEnum {
                }
                """,
            diagnostics: [
                DiagnosticSpec(
                    message: "@PolymorphicWriteEntry must be attached to an enum declaration.",
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
            @PolymorphicWriteEntry
            enum Empty {
            }
            """,
            expandedSource: """
                enum Empty {
                }
                """,
            diagnostics: [
                DiagnosticSpec(
                    message: "@PolymorphicWriteEntry decorated enum must be have at least a singe case.",
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
            @PolymorphicWriteEntry
            enum BadEntry {
                case bad(TestTypeAWriteEntry, String)
            }
            """,
            expandedSource: """
                enum BadEntry {
                    case bad(TestTypeAWriteEntry, String)
                }
                """,
            diagnostics: [
                DiagnosticSpec(
                    message:
                        "@PolymorphicWriteEntry decorated enum can only have case entries with a single parameter.",
                    line: 3,
                    column: 10
                )
            ],
            macroSpecs: macroSpecs
        )
    }

    func testDiagnosticWhenCaseHasNoParameter() {
        assertMacroExpansion(
            """
            @PolymorphicWriteEntry
            enum BadEntry {
                case bad
            }
            """,
            expandedSource: """
                enum BadEntry {
                    case bad
                }
                """,
            diagnostics: [
                DiagnosticSpec(
                    message:
                        "@PolymorphicWriteEntry decorated enum can only have case entries with a single parameter.",
                    line: 3,
                    column: 10
                )
            ],
            macroSpecs: macroSpecs
        )
    }
}
