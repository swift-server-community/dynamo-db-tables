//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// See LICENSE.txt for license information
// See CONTRIBUTORS.txt for the list of DynamoDBTables authors
//
// SPDX-License-Identifier: Apache-2.0
//
//===----------------------------------------------------------------------===//

//
//  Macros.swift
//  DynamoDBTables
//

@attached(extension, conformances: PolymorphicWriteEntry, names: named(handle(context:)), named(compositePrimaryKey))
public macro PolymorphicWriteEntry(passCompositePrimaryKey: Bool = true) =
    #externalMacro(
        module: "DynamoDBTablesMacros",
        type: "PolymorphicWriteEntryMacro")
