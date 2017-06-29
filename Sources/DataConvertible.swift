//
//  DataConvertible.swift
//  SwiftLMDB
//
//  Created by August Heegaard on 02/10/2016.
//  Copyright © 2016 August Heegaard. All rights reserved.
//

import Foundation

/// Any type conforming to the DataConvertible protocol can be used as both key and value in LMDB.
/// The protocol provides a default implementation, which will work for most Swift value types.
/// For other types, including reference counted ones, you may want to implement the conversion yourself.
public protocol DataConvertible {
    init?(data: UnsafeRawBufferPointer)
    func read<ResultType>(_ body: (UnsafeRawBufferPointer) throws -> ResultType) rethrows -> ResultType
}

public extension DataConvertible {
    init?(data: UnsafeRawBufferPointer) {
        guard data.count == MemoryLayout<Self>.size else { return nil }
        
        // I don't know if its necessary to bind the memory, or if we can use
        // assumingMemoryBound.
        self = data.baseAddress!.bindMemory(to: Self.self, capacity: data.count).pointee
    }
    
    func read<ResultType>(_ body: (UnsafeRawBufferPointer) throws -> ResultType) rethrows -> ResultType {
        // The copy here is strictly unnecessary, but I'm not sure how to write
        // this function without it. Thankfully this is cheap for trivial types,
        // and classes will ref count.
        var val = self
        return try withUnsafeBytes(of: &val, body)
    }
}

extension Data: DataConvertible {
    public init?(data: UnsafeRawBufferPointer) {
        // This copies the bytes out immediately.
        self = Data.init(bytes: data.baseAddress!, count: data.count)
    }

    public func read<ResultType>(_ body: (UnsafeRawBufferPointer) throws -> ResultType) rethrows -> ResultType {
        return try self.withUnsafeBytes() { (typedPtr: UnsafePointer<UInt8>) -> ResultType in
            return try body(UnsafeRawBufferPointer.init(start: typedPtr, count: self.count))
        }
    }
}

extension String: DataConvertible {
    public init?(data: UnsafeRawBufferPointer) {
        self.init(bytes: data, encoding: .utf8)
    }

    public func read<ResultType>(_ body: (UnsafeRawBufferPointer) throws -> ResultType) rethrows -> ResultType {
        // This will allocate a temporary array for reading, but I think thats
        // unavoidable. Strings should be short anyway - large objects should be
        // encoded into Data before we hit this method, and then they'll be read
        // directly.
        return try self.data(using: .utf8)!.read(body)
    }
}

extension Array: DataConvertible {
    // This uses the fact that we know the data length to figure out the number
    // of elements.
    public init?(data: UnsafeRawBufferPointer) {
        let first = UnsafeRawPointer(data.baseAddress)?.bindMemory(to: Element.self, capacity: data.count)
        self = [Element](UnsafeBufferPointer(start: first, count: data.count / MemoryLayout<Element>.stride))
//        UnsafeBufferPointer.init(start: <#T##UnsafePointer<Element>?#>, count: <#T##Int#>)
//        guard let result = [Element].init(data: data) else { return nil }
//        self = result
    }

    public func read<ResultType>(_ body: (UnsafeRawBufferPointer) throws -> ResultType) rethrows -> ResultType {
        return try self.withUnsafeBytes(body)
    }
}

extension Bool: DataConvertible {}
extension Int: DataConvertible {}
extension Int8: DataConvertible {}
extension Int16: DataConvertible {}
extension Int32: DataConvertible {}
extension Int64: DataConvertible {}
extension UInt: DataConvertible {}
extension UInt8: DataConvertible {}
extension UInt16: DataConvertible {}
extension UInt32: DataConvertible {}
extension UInt64: DataConvertible {}
extension Float: DataConvertible {}
extension Double: DataConvertible {}
extension Date: DataConvertible {}
extension URL: DataConvertible {}
