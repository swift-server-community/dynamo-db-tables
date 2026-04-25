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
//  PolymorphicTransactionConstraintEntryMacroTests.swift
//  DynamoDBTablesMacrosTests
//

import SwiftSyntax
import SwiftSyntaxMacroExpansion
import SwiftSyntaxMacrosTestSupport
import XCTest

@testable import DynamoDBTablesMacros

final class PolymorphicTransactionConstraintEntryMacroTests: XCTestCase {
    private let macroSpecs: [String: MacroSpec] = [
        "PolymorphicTransactionConstraintEntry": MacroSpec(
            type: PolymorphicTransactionConstraintEntryMacro.self,
            conformances: ["PolymorphicTransactionConstraintEntry"]
        )
    ]

    func testExpansionWithTwoCases() {
        assertMacroExpansion(
            """
            @PolymorphicTransactionConstraintEntry
            enum TestConstraint {
                case testTypeA(TestTypeAStandardTransactionConstraintEntry)
                case testTypeB(TestTypeBStandardTransactionConstraintEntry)
            }
            """,
            expandedSource: """
                enum TestConstraint {
                    case testTypeA(TestTypeAStandardTransactionConstraintEntry)
                    case testTypeB(TestTypeBStandardTransactionConstraintEntry)
                }

                extension TestConstraint: PolymorphicTransactionConstraintEntry {
                    func handle<Context: PolymorphicWriteEntryContext>(context: Context) throws -> Context.WriteTransactionConstraintType {
                        switch self {
                        case let .testTypeA(writeEntry):
                            return try context.transform(writeEntry)
                        case let .testTypeB(writeEntry):
                            return try context.transform(writeEntry)
                        }
                    }
                    var compositePrimaryKey: StandardCompositePrimaryKey {
                        switch self {
                        case let .testTypeA(writeEntry):
                            return writeEntry.compositePrimaryKey
                        case let .testTypeB(writeEntry):
                            return writeEntry.compositePrimaryKey
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
            @PolymorphicTransactionConstraintEntry
            struct NotAnEnum {
            }
            """,
            expandedSource: """
                struct NotAnEnum {
                }
                """,
            diagnostics: [
                DiagnosticSpec(
                    message: "@PolymorphicTransactionConstraintEntry must be attached to an enum declaration.",
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
            @PolymorphicTransactionConstraintEntry
            enum Empty {
            }
            """,
            expandedSource: """
                enum Empty {
                }
                """,
            diagnostics: [
                DiagnosticSpec(
                    message:
                        "@PolymorphicTransactionConstraintEntry decorated enum must be have at least a singe case.",
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
            @PolymorphicTransactionConstraintEntry
            enum BadConstraint {
                case bad(TestTypeAStandardTransactionConstraintEntry, String)
            }
            """,
            expandedSource: """
                enum BadConstraint {
                    case bad(TestTypeAStandardTransactionConstraintEntry, String)
                }
                """,
            diagnostics: [
                DiagnosticSpec(
                    message:
                        "@PolymorphicTransactionConstraintEntry decorated enum can only have case entries with a single parameter.",
                    line: 3,
                    column: 10
                )
            ],
            macroSpecs: macroSpecs
        )
    }
}
