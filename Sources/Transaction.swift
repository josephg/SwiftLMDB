//
//  Transaction.swift
//  SwiftLMDB
//
//  Created by August Heegaard on 30/09/2016.
//  Copyright © 2016 August Heegaard. All rights reserved.
//

import Foundation
import CLMDB

internal func bufferToMdb(_ buf: UnsafeRawBufferPointer) -> MDB_val {
    return MDB_val(
        mv_size: buf.count,
        mv_data: UnsafeMutableRawPointer(mutating: buf.baseAddress)
    )
}

internal func mdbToBuffer(_ mdb: MDB_val) -> UnsafeRawBufferPointer {
    return UnsafeRawBufferPointer(start: mdb.mv_data, count: mdb.mv_size)
}

/// All read and write operations on the database happen inside a Transaction.
public struct Transaction {
    
    public enum Result {
        case abort, commit
    }
    
    public struct Flags: OptionSet {
        public let rawValue: Int32
        public init(rawValue: Int32) { self.rawValue = rawValue}
        
        public static let readOnly = Flags(rawValue: MDB_RDONLY)
    }
    
    internal private(set) var handle: OpaquePointer?
    
    /// Creates a new instance of Transaction and runs the closure provided.
    /// Depending on the result returned from the closure, the transaction will either be comitted or aborted.
    /// If an error is thrown from the transaction closure, the transaction is aborted.
    /// - parameter environment: The environment with which the transaction will be associated.
    /// - parameter parent: Transactions can be nested to unlimited depth. (WARNING: Not yet tested)
    /// - parameter flags: A set containing flags modifying the behavior of the transaction.
    /// - parameter closure: The closure in which database interaction should occur. When the closure returns, the transaction is ended.
    /// - throws: an error if operation fails. See `LMDBError`.
    internal init(environment: Environment,
                  parent: Transaction? = nil,
                  flags: Flags = []) throws {
        
        // http://lmdb.tech/doc/group__mdb.html#gad7ea55da06b77513609efebd44b26920
        let txnStatus = mdb_txn_begin(environment.handle, parent?.handle, UInt32(flags.rawValue), &handle)
        try LMDBError.check(txnStatus)
    }
    
    internal mutating func run<R>(_ body: ((Transaction) throws -> R)) throws -> R {
        // Run the closure inside a do/catch block, so we can abort the transaction if an error is thrown from the closure.
        // This consumes the transaction.
        guard handle != nil else { throw LMDBError.badTransaction }
        defer { handle = nil }
        
        do {
            let userResult = try body(self)
            try LMDBError.check(mdb_txn_commit(handle))
            return userResult
        } catch {
            mdb_txn_abort(handle)
            throw error
        }
    }
    
    public mutating func commit() throws {
        guard handle != nil else { throw LMDBError.badTransaction }
        defer { handle = nil }
        try LMDBError.check(mdb_txn_commit(handle))
    }
    
    /// Returns a value from the database instantiated as type `V` for a key of type `K`.
    /// - parameter type: A type conforming to `DataConvertible` that you want to be instantiated with the value from the database.
    /// - parameter key: A key conforming to `DataConvertible` for which the value will be looked up.
    /// - returns: Returns the value as an instance of type `V` or `nil` if no value exists for the key or the type could not be instatiated with the data.
    /// - note: You can always use `Foundation.Data` as the type. In such case, `nil` will only be returned if there is no value for the key.
    /// - throws: an error if operation fails. See `LMDBError`.
    public func get<V: DataConvertible, K: DataConvertible>(type: V.Type, forKey key: K, fromDb dbi: MDB_dbi) throws -> V? {
        return try key.read() { (key: UnsafeRawBufferPointer) in
            // The database will manage the memory for the returned value.
            // The memory is valid until a subsequent update operation, or the
            // end of the transaction.
            // http://104.237.133.194/doc/group__mdb.html#ga8bf10cd91d3f3a83a34d04ce6b07992d
            var keyVal = bufferToMdb(key)
            var dataVal = MDB_val()
            let result = mdb_get(self.handle, dbi, &keyVal, &dataVal)
            
            guard result != MDB_NOTFOUND else { return nil }
            try LMDBError.check(result)

            return V(data: mdbToBuffer(dataVal))
        }
    }

    public func put<V: DataConvertible, K: DataConvertible>(key: K, toVal val: V, fromDb dbi: MDB_dbi, flags: Database.PutFlags = []) throws {
        try key.read() { (key: UnsafeRawBufferPointer) in
            try val.read() { (val: UnsafeRawBufferPointer) in
                var key = bufferToMdb(key)
                var val = bufferToMdb(val)
                
                try LMDBError.check(mdb_put(self.handle, dbi, &key, &val, UInt32(flags.rawValue)))
            }
        }
    }
    
    
    public func delete<K: DataConvertible>(_ key: K, fromDb dbi: MDB_dbi) throws {
        try key.read() { key in
            var keyVal = bufferToMdb(key)
            try LMDBError.check(mdb_del(self.handle, dbi, &keyVal, nil))
        }
    }
}
