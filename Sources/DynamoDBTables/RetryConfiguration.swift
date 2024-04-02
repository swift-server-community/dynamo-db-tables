//===----------------------------------------------------------------------===//
//
// This source file is part of the DynamoDBTables open source project
//
// This file is forked from
// https://github.com/amzn/smoke-dynamodb/tree/aws-sdk-swift-main/Sources/SmokeDynamoDB/RetryConfiguration.swift
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
//  RetryConfiguration.swift
//  DynamoDBTables
//

import Foundation

/// Type alias for a retry interval.
public typealias RetryInterval = UInt32

/**
 Retry configuration for the requests made by a table..
 */
public struct RetryConfiguration {
    // Number of retries to be attempted
    public let numRetries: Int
    // First interval of retry in millis
    public let baseRetryInterval: RetryInterval
    // Max amount of cumulative time to attempt retries in millis
    public let maxRetryInterval: RetryInterval
    // Exponential backoff for each retry
    public let exponentialBackoff: Double
    // Ramdomized backoff
    public let jitter: Bool
 
    /**
     Initializer.
 
     - Parameters:
         - numRetries: number of retries to be attempted.
         - baseRetryInterval: first interval of retry in millis.
         - maxRetryInterval: max amount of cumulative time to attempt retries in millis
         - exponentialBackoff: exponential backoff for each retry
         - jitter: ramdomized backoff
     */
    public init(numRetries: Int, baseRetryInterval: RetryInterval, maxRetryInterval: RetryInterval,
                exponentialBackoff: Double, jitter: Bool = true) {
        self.numRetries = numRetries
        self.baseRetryInterval = baseRetryInterval
        self.maxRetryInterval = maxRetryInterval
        self.exponentialBackoff = exponentialBackoff
        self.jitter = jitter
    }
    
    public func getRetryInterval(retriesRemaining: Int) -> RetryInterval {
        let msInterval = RetryInterval(Double(baseRetryInterval) * pow(exponentialBackoff, Double(numRetries - retriesRemaining)))
        let boundedMsInterval = min(maxRetryInterval, msInterval)
        
        if jitter {
            if boundedMsInterval > 0 {
                return RetryInterval.random(in: 0 ..< boundedMsInterval)
            } else {
                return 0
            }
        }
        
        return boundedMsInterval
    }
 
    /// Default try configuration with 5 retries starting at 500 ms interval.
    public static var `default` = RetryConfiguration(numRetries: 5, baseRetryInterval: 500,
                                                     maxRetryInterval: 10000, exponentialBackoff: 2)
 
    /// Retry Configuration with no retries.
    public static var noRetries = RetryConfiguration(numRetries: 0, baseRetryInterval: 0,
                                                     maxRetryInterval: 0, exponentialBackoff: 0)
}
