//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked CollectionConcurrencyKit
// Copyright (c) John Sundell 2021
// MIT license, see LICENSE.md file for details
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
//  Sequence+concurrency.swift
//  DynamoDBTables
//
//
//
//

// MARK: - ForEach

extension Sequence {
    /// Run an async closure for each element within the sequence.
    ///
    /// The closure calls will be performed in order, by waiting for
    /// each call to complete before proceeding with the next one. If
    /// any of the closure calls throw an error, then the iteration
    /// will be terminated and the error rethrown.
    ///
    /// - parameter operation: The closure to run for each element.
    /// - throws: Rethrows any error thrown by the passed closure.
    func asyncForEach(
        _ operation: @Sendable (Element) async throws -> Void) async rethrows
    {
        for element in self {
            try await operation(element)
        }
    }
    /*
     /// Run an async closure for each element within the sequence.
     ///
     /// The closure calls will be performed concurrently, but the call
     /// to this function won't return until all of the closure calls
     /// have completed.
     ///
     /// - parameter priority: Any specific `TaskPriority` to assign to
     ///   the async tasks that will perform the closure calls. The
     ///   default is `nil` (meaning that the system picks a priority).
     /// - parameter operation: The closure to run for each element.
     func concurrentForEach(
         withPriority priority: TaskPriority? = nil,
         _ operation: @Sendable @escaping (Element) async -> Void
     ) async {
         await withTaskGroup(of: Void.self) { group in
             for element in self {
                 group.addTask(priority: priority) {
                     await operation(element)
                 }
             }
         }
     }

     /// Run an async closure for each element within the sequence.
     ///
     /// The closure calls will be performed concurrently, but the call
     /// to this function won't return until all of the closure calls
     /// have completed. If any of the closure calls throw an error,
     /// then the first error will be rethrown once all closure calls have
     /// completed.
     ///
     /// - parameter priority: Any specific `TaskPriority` to assign to
     ///   the async tasks that will perform the closure calls. The
     ///   default is `nil` (meaning that the system picks a priority).
     /// - parameter operation: The closure to run for each element.
     /// - throws: Rethrows any error thrown by the passed closure.
     func concurrentForEach(
         withPriority priority: TaskPriority? = nil,
         _ operation: @Sendable @escaping (Element) async throws -> Void
     ) async throws {
         try await withThrowingTaskGroup(of: Void.self) { group in
             for element in self {
                 group.addTask(priority: priority) {
                     try await operation(element)
                 }
             }

             // Propagate any errors thrown by the group's tasks:
             for try await _ in group {}
         }
     }*/
}

// MARK: - Map

extension Sequence where Element: Sendable {
    /*    /// Transform the sequence into an array of new values using
     /// an async closure.
     ///
     /// The closure calls will be performed in order, by waiting for
     /// each call to complete before proceeding with the next one. If
     /// any of the closure calls throw an error, then the iteration
     /// will be terminated and the error rethrown.
     ///
     /// - parameter transform: The transform to run on each element.
     /// - returns: The transformed values as an array. The order of
     ///   the transformed values will match the original sequence.
     /// - throws: Rethrows any error thrown by the passed closure.
     func asyncMap<T>(
         _ transform: @Sendable (Element) async throws -> T
     ) async rethrows -> [T] {
         var values = [T]()

         for element in self {
             try await values.append(transform(element))
         }

         return values
     }
     */
    /// Transform the sequence into an array of new values using
    /// an async closure.
    ///
    /// The closure calls will be performed concurrently, but the call
    /// to this function won't return until all of the closure calls
    /// have completed.
    ///
    /// - parameter priority: Any specific `TaskPriority` to assign to
    ///   the async tasks that will perform the closure calls. The
    ///   default is `nil` (meaning that the system picks a priority).
    /// - parameter transform: The transform to run on each element.
    /// - returns: The transformed values as an array. The order of
    ///   the transformed values will match the original sequence.
    func concurrentMap<T: Sendable>(
        withPriority priority: TaskPriority? = nil,
        _ transform: @Sendable @escaping (Element) async -> T) async -> [T]
    {
        await withTaskGroup(of: (offset: Int, value: T).self) { group in
            var taskCount = 0
            for (idx, element) in enumerated() {
                taskCount += 1

                group.addTask(priority: priority) {
                    await (idx, transform(element))
                }
            }

            var res = [T?](repeating: nil, count: taskCount)
            while let next = await group.next() {
                res[next.offset] = next.value
            }
            return res as! [T]
        }
    }

    /// Transform the sequence into an array of new values using
    /// an async closure.
    ///
    /// The closure calls will be performed concurrently, but the call
    /// to this function won't return until all of the closure calls
    /// have completed. If any of the closure calls throw an error,
    /// then the first error will be rethrown once all closure calls have
    /// completed.
    ///
    /// - parameter priority: Any specific `TaskPriority` to assign to
    ///   the async tasks that will perform the closure calls. The
    ///   default is `nil` (meaning that the system picks a priority).
    /// - parameter transform: The transform to run on each element.
    /// - returns: The transformed values as an array. The order of
    ///   the transformed values will match the original sequence.
    /// - throws: Rethrows any error thrown by the passed closure.
    func concurrentMap<T: Sendable>(
        withPriority priority: TaskPriority? = nil,
        _ transform: @Sendable @escaping (Element) async throws -> T) async throws -> [T]
    {
        try await withThrowingTaskGroup(of: (offset: Int, value: T).self) { group in
            var taskCount = 0
            for (idx, element) in enumerated() {
                taskCount += 1

                group.addTask(priority: priority) {
                    try await (idx, transform(element))
                }
            }

            var res = [T?](repeating: nil, count: taskCount)
            while let next = try await group.next() {
                res[next.offset] = next.value
            }
            return res as! [T]
        }
    }
}

// MARK: - CompactMap

extension Sequence where Element: Sendable {
    /// Transform the sequence into an array of new values using
    /// an async closure that returns optional values. Only the
    /// non-`nil` return values will be included in the new array.
    ///
    /// The closure calls will be performed in order, by waiting for
    /// each call to complete before proceeding with the next one. If
    /// any of the closure calls throw an error, then the iteration
    /// will be terminated and the error rethrown.
    ///
    /// - parameter transform: The transform to run on each element.
    /// - returns: The transformed values as an array. The order of
    ///   the transformed values will match the original sequence,
    ///   except for the values that were transformed into `nil`.
    /// - throws: Rethrows any error thrown by the passed closure.
    func asyncCompactMap<T>(
        _ transform: @Sendable (Element) async throws -> T?) async rethrows -> [T]
    {
        var values = [T]()

        for element in self {
            guard let value = try await transform(element) else {
                continue
            }

            values.append(value)
        }

        return values
    }

    /// Transform the sequence into an array of new values using
    /// an async closure that returns optional values. Only the
    /// non-`nil` return values will be included in the new array.
    ///
    /// The closure calls will be performed concurrently, but the call
    /// to this function won't return until all of the closure calls
    /// have completed.
    ///
    /// - parameter priority: Any specific `TaskPriority` to assign to
    ///   the async tasks that will perform the closure calls. The
    ///   default is `nil` (meaning that the system picks a priority).
    /// - parameter transform: The transform to run on each element.
    /// - returns: The transformed values as an array. The order of
    ///   the transformed values will match the original sequence,
    ///   except for the values that were transformed into `nil`.
    func concurrentCompactMap<T: Sendable>(
        withPriority priority: TaskPriority? = nil,
        _ transform: @Sendable @escaping (Element) async -> T?) async -> [T]
    {
        await withTaskGroup(of: (offset: Int, value: T?).self) { group in
            var taskCount = 0
            for (idx, element) in enumerated() {
                taskCount += 1

                group.addTask(priority: priority) {
                    await (idx, transform(element))
                }
            }

            var res = [T??](repeating: nil, count: taskCount)
            while let next = await group.next() {
                res[next.offset] = next.value
            }
            return (res as! [T?]).compactMap(\.self)
        }
    }

    /// Transform the sequence into an array of new values using
    /// an async closure that returns optional values. Only the
    /// non-`nil` return values will be included in the new array.
    ///
    /// The closure calls will be performed concurrently, but the call
    /// to this function won't return until all of the closure calls
    /// have completed. If any of the closure calls throw an error,
    /// then the first error will be rethrown once all closure calls have
    /// completed.
    ///
    /// - parameter priority: Any specific `TaskPriority` to assign to
    ///   the async tasks that will perform the closure calls. The
    ///   default is `nil` (meaning that the system picks a priority).
    /// - parameter transform: The transform to run on each element.
    /// - returns: The transformed values as an array. The order of
    ///   the transformed values will match the original sequence,
    ///   except for the values that were transformed into `nil`.
    /// - throws: Rethrows any error thrown by the passed closure.
    func concurrentCompactMap<T: Sendable>(
        withPriority priority: TaskPriority? = nil,
        _ transform: @Sendable @escaping (Element) async throws -> T?) async throws -> [T]
    {
        try await withThrowingTaskGroup(of: (offset: Int, value: T?).self) { group in
            var taskCount = 0
            for (idx, element) in enumerated() {
                taskCount += 1

                group.addTask(priority: priority) {
                    try await (idx, transform(element))
                }
            }

            var res = [T??](repeating: nil, count: taskCount)
            while let next = try await group.next() {
                res[next.offset] = next.value
            }
            return (res as! [T?]).compactMap(\.self)
        }
    }
}

// MARK: - FlatMap

extension Sequence where Element: Sendable {
    /*    /// Transform the sequence into an array of new values using
     /// an async closure that returns sequences. The returned sequences
     /// will be flattened into the array returned from this function.
     ///
     /// The closure calls will be performed in order, by waiting for
     /// each call to complete before proceeding with the next one. If
     /// any of the closure calls throw an error, then the iteration
     /// will be terminated and the error rethrown.
     ///
     /// - parameter transform: The transform to run on each element.
     /// - returns: The transformed values as an array. The order of
     ///   the transformed values will match the original sequence,
     ///   with the results of each closure call appearing in-order
     ///   within the returned array.
     /// - throws: Rethrows any error thrown by the passed closure.
     func asyncFlatMap<T: Sequence>(
         _ transform: @Sendable (Element) async throws -> T
     ) async rethrows -> [T.Element] {
         var values = [T.Element]()

         for element in self {
             try await values.append(contentsOf: transform(element))
         }

         return values
     }
     */
    /// Transform the sequence into an array of new values using
    /// an async closure that returns sequences. The returned sequences
    /// will be flattened into the array returned from this function.
    ///
    /// The closure calls will be performed concurrently, but the call
    /// to this function won't return until all of the closure calls
    /// have completed.
    ///
    /// - parameter priority: Any specific `TaskPriority` to assign to
    ///   the async tasks that will perform the closure calls. The
    ///   default is `nil` (meaning that the system picks a priority).
    /// - parameter transform: The transform to run on each element.
    /// - returns: The transformed values as an array. The order of
    ///   the transformed values will match the original sequence,
    ///   with the results of each closure call appearing in-order
    ///   within the returned array.
    func concurrentFlatMap<T: Sequence & Sendable>(
        withPriority priority: TaskPriority? = nil,
        _ transform: @Sendable @escaping (Element) async -> T) async -> [T.Element]
    {
        await withTaskGroup(of: (offset: Int, value: T).self) { group in
            var taskCount = 0
            for (idx, element) in enumerated() {
                taskCount += 1

                group.addTask(priority: priority) {
                    await (idx, transform(element))
                }
            }

            var res = [T?](repeating: nil, count: taskCount)
            while let next = await group.next() {
                res[next.offset] = next.value
            }
            return (res as! [T]).flatMap(\.self)
        }
    }

    /// Transform the sequence into an array of new values using
    /// an async closure that returns sequences. The returned sequences
    /// will be flattened into the array returned from this function.
    ///
    /// The closure calls will be performed concurrently, but the call
    /// to this function won't return until all of the closure calls
    /// have completed. If any of the closure calls throw an error,
    /// then the first error will be rethrown once all closure calls have
    /// completed.
    ///
    /// - parameter priority: Any specific `TaskPriority` to assign to
    ///   the async tasks that will perform the closure calls. The
    ///   default is `nil` (meaning that the system picks a priority).
    /// - parameter transform: The transform to run on each element.
    /// - returns: The transformed values as an array. The order of
    ///   the transformed values will match the original sequence,
    ///   with the results of each closure call appearing in-order
    ///   within the returned array.
    /// - throws: Rethrows any error thrown by the passed closure.
    func concurrentFlatMap<T: Sequence & Sendable>(
        withPriority priority: TaskPriority? = nil,
        _ transform: @Sendable @escaping (Element) async throws -> T) async throws -> [T.Element]
    {
        try await withThrowingTaskGroup(of: (offset: Int, value: T).self) { group in
            var taskCount = 0
            for (idx, element) in enumerated() {
                taskCount += 1

                group.addTask(priority: priority) {
                    try await (idx, transform(element))
                }
            }

            var res = [T?](repeating: nil, count: taskCount)
            while let next = try await group.next() {
                res[next.offset] = next.value
            }
            return (res as! [T]).flatMap(\.self)
        }
    }
}
