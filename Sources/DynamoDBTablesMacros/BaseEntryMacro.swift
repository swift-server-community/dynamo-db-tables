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
//  BaseEntryMacro.swift
//  DynamoDBTablesMacros
//

import SwiftDiagnostics
import SwiftSyntax
import SwiftSyntaxMacros

protocol CoreMacroAttributes {
    static var macroName: String { get }

    static var protocolName: String { get }
}

protocol MacroAttributes: CoreMacroAttributes {
    static var transformType: String { get }

    static var contextType: String { get }

    /// The name of the concrete case-parameter base type (e.g. `"WriteEntry"`). Used inside the
    /// generated per-case assertion to pin the parameter type's `AttributesType` to the enum's
    /// derived `AttributesType` typealias.
    static var caseParameterBaseTypeName: String { get }
}

enum BaseEntryDiagnostic<Attributes: CoreMacroAttributes>: String, DiagnosticMessage {
    case notAttachedToEnumDeclaration
    case enumMustNotHaveZeroCases
    case enumCasesMustHaveASingleParameter

    var diagnosticID: MessageID {
        MessageID(domain: "\(Attributes.macroName)Macro", id: rawValue)
    }

    var severity: DiagnosticSeverity { .error }

    var message: String {
        switch self {
        case .notAttachedToEnumDeclaration:
            "@\(Attributes.macroName) must be attached to an enum declaration."
        case .enumMustNotHaveZeroCases:
            "@\(Attributes.macroName) decorated enum must be have at least a singe case."
        case .enumCasesMustHaveASingleParameter:
            "@\(Attributes.macroName) decorated enum can only have case entries with a single parameter."
        }
    }
}

struct CaseExpansionResult {
    var hasDiagnostics: Bool
    var firstParameterType: String?
    var handleCases: SwitchCaseListSyntax
    var compositePrimaryKeyCases: SwitchCaseListSyntax
    var assertions: [DeclSyntax]
}

enum BaseEntryMacro<Attributes: MacroAttributes>: ExtensionMacro {
    private static func getCases(
        caseMembers: [EnumCaseDeclSyntax],
        context: some MacroExpansionContext
    ) -> CaseExpansionResult {
        var result = CaseExpansionResult(
            hasDiagnostics: false,
            firstParameterType: nil,
            handleCases: [],
            compositePrimaryKeyCases: [],
            assertions: []
        )
        for caseMember in caseMembers {
            for element in caseMember.elements {
                // ensure that the enum case only has one parameter
                guard let parameterClause = element.parameterClause, parameterClause.parameters.count == 1,
                    let parameter = parameterClause.parameters.first
                else {
                    context.diagnose(
                        .init(node: element, message: BaseEntryDiagnostic<Attributes>.enumCasesMustHaveASingleParameter)
                    )
                    result.hasDiagnostics = true
                    // do nothing for this case
                    continue
                }

                if result.firstParameterType == nil {
                    result.firstParameterType = parameter.type.trimmedDescription
                }

                result.handleCases.append(
                    SwitchCaseListSyntax.Element(
                        """
                        case let .\(element.name)(writeEntry):
                            return try context.transform(writeEntry)
                        """
                    )
                )

                result.compositePrimaryKeyCases.append(
                    SwitchCaseListSyntax.Element(
                        """
                        case let .\(element.name)(writeEntry):
                            return writeEntry.compositePrimaryKey
                        """
                    )
                )

                result.assertions.append(
                    self.assertionDecl(for: element, parameter: parameter, in: context)
                )
            }
        }

        return result
    }

    /// Emits a per-case assertion helper that forces a compile-time check that the case parameter type
    /// is a `<BaseType><AttributesType, _, _>` — both confirming the parameter shape and that the
    /// case's attributes match the enum's derived `AttributesType` typealias. When the case has a
    /// known source location, wraps the call in `#sourceLocation` directives so the diagnostic
    /// surfaces at the user's enum case declaration rather than at the macro-generated buffer.
    private static func assertionDecl(
        for element: EnumCaseElementListSyntax.Element,
        parameter: EnumCaseParameterListSyntax.Element,
        in context: some MacroExpansionContext
    ) -> DeclSyntax {
        let paramType = parameter.type.trimmedDescription
        // The pretty-printer splits multi-line declarations across lines, so the
        // `#sourceLocation` directive is placed immediately before the `_check` call site
        // (where any diagnostic actually fires) rather than at the top of the body — that
        // way the directive's line offset doesn't drift through the function declaration.
        let assertionBody: String
        if let location = context.location(of: element) {
            assertionBody = """
                func _check<R: Codable & Sendable, T: TimeToLiveAttributes>(
                    _: \(Attributes.caseParameterBaseTypeName)<AttributesType, R, T>.Type
                ) {}
                #sourceLocation(file: \(location.file), line: \(location.line))
                _check(\(paramType).self)
                #sourceLocation()
                """
        } else {
            assertionBody = """
                func _check<R: Codable & Sendable, T: TimeToLiveAttributes>(
                    _: \(Attributes.caseParameterBaseTypeName)<AttributesType, R, T>.Type
                ) {}
                _check(\(paramType).self)
                """
        }
        return DeclSyntax(
            stringLiteral: """
                private static func _assertCase_\(element.name.text)() {
                    \(assertionBody)
                }
                """
        )
    }

    static func expansion(
        of _: AttributeSyntax,
        attachedTo declaration: some DeclGroupSyntax,
        providingExtensionsOf type: some TypeSyntaxProtocol,
        conformingTo protocols: [TypeSyntax],
        in context: some MacroExpansionContext
    ) throws -> [ExtensionDeclSyntax] {
        // make sure this is attached to an enum
        guard let enumDeclaration = declaration as? EnumDeclSyntax else {
            context.diagnose(
                .init(node: declaration, message: BaseEntryDiagnostic<Attributes>.notAttachedToEnumDeclaration)
            )

            return []
        }

        let requiresProtocolConformance = protocols.reduce(false) { partialResult, protocolSyntax in
            if let identifierTypeSyntax = protocolSyntax.as(IdentifierTypeSyntax.self),
                identifierTypeSyntax.name.text == Attributes.protocolName
            {
                return true
            }

            return partialResult
        }

        let memberBlock = enumDeclaration.memberBlock.members

        let caseMembers: [EnumCaseDeclSyntax] = memberBlock.compactMap { member in
            if let caseMember = member.decl.as(EnumCaseDeclSyntax.self) {
                return caseMember
            }

            return nil
        }

        // make sure this is attached to an enum
        guard !caseMembers.isEmpty else {
            context.diagnose(
                .init(node: declaration, message: BaseEntryDiagnostic<Attributes>.enumMustNotHaveZeroCases)
            )

            return []
        }

        let cases = self.getCases(caseMembers: caseMembers, context: context)

        if cases.hasDiagnostics {
            return []
        }

        // The enum-level `AttributesType` is derived from the first case's parameter type. Subsequent
        // cases are verified to have the same `AttributesType` via the per-case assertion helpers.
        guard let firstParameterType = cases.firstParameterType else {
            return []
        }

        let type = TypeSyntax(
            extendedGraphemeClusterLiteral: requiresProtocolConformance
                ? "\(type.trimmed): \(Attributes.protocolName) "
                : "\(type.trimmed) "
        )
        let extensionDecl = try ExtensionDeclSyntax(
            extendedType: type,
            memberBlockBuilder: {
                try TypeAliasDeclSyntax(
                    "typealias AttributesType = \(raw: firstParameterType).AttributesType"
                )

                try FunctionDeclSyntax(
                    "func handle<Context: \(raw: Attributes.contextType)>(context: Context) throws -> Context.\(raw: Attributes.transformType)"
                ) {
                    SwitchExprSyntax(subject: ExprSyntax(stringLiteral: "self"), cases: cases.handleCases)
                }

                try VariableDeclSyntax("var compositePrimaryKey: CompositePrimaryKey<AttributesType>") {
                    SwitchExprSyntax(subject: ExprSyntax(stringLiteral: "self"), cases: cases.compositePrimaryKeyCases)
                }

                for assertion in cases.assertions {
                    assertion
                }
            }
        )

        return [extensionDecl]
    }
}
