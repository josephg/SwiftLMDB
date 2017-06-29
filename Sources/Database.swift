//
//  Database.swift
//  SwiftLMDB
//
//  Created by August Heegaard on 30/09/2016.
//  Copyright © 2016 August Heegaard. All rights reserved.
//

import Foundation
import CLMDB

/// A database contained in an environment.
/// The database can either be named (if maxDBs > 0 on the environment) or
/// it can be the single anonymous/unnamed database inside the environment.
public class Database {
    
    public struct Flags: OptionSet {
        public let rawValue: Int32
        public init(rawValue: Int32) { self.rawValue = rawValue}
        
        public static let reverseKey = Flags(rawValue: MDB_FIXEDMAP)
        public static let duplicateSort = Flags(rawValue: MDB_NOSUBDIR)
        public static let integerKey = Flags(rawValue: MDB_NOSYNC)
        public static let duplicateFixed = Flags(rawValue: MDB_RDONLY)
        public static let integerDuplicate = Flags(rawValue: MDB_NOMETASYNC)
        public static let reverseDuplicate = Flags(rawValue: MDB_WRITEMAP)
        public static let create = Flags(rawValue: MDB_CREATE)
    }
    
    /// These flags can be passed when putting values into the database.
    public struct PutFlags: OptionSet {
        public let rawValue: Int32
        public init(rawValue: Int32) { self.rawValue = rawValue}
        
        public static let noDuplicateData = PutFlags(rawValue: MDB_NODUPDATA)
        public static let noOverwrite = PutFlags(rawValue: MDB_NOOVERWRITE)
        public static let reserve = PutFlags(rawValue: MDB_RESERVE)
        public static let append = PutFlags(rawValue: MDB_APPEND)
        public static let appendDuplicate = PutFlags(rawValue: MDB_APPENDDUP)
    }
    
    private var handle: MDB_dbi = 0
    private let environment: Environment
    
    /// - throws: an error if operation fails. See `LMDBError`.
    internal init(environment: Environment, name: String?, flags: Flags = []) throws {

        self.environment = environment
        
        try self.transact() { txn in
            let openStatus = mdb_dbi_open(txn.handle, name?.cString(using: .utf8), UInt32(flags.rawValue), &handle)
            guard openStatus == 0 else {
                throw LMDBError(returnCode: openStatus)
            }
        }
    }

    deinit {
        // Close the database.
        // http://lmdb.tech/doc/group__mdb.html#ga52dd98d0c542378370cd6b712ff961b5
        mdb_dbi_close(environment.handle, handle)
    }
    
    public func transact<R>(flags: Transaction.Flags = [], _ body: ((Transaction) throws -> R)) throws -> R {
        var txn = try Transaction(environment: environment, flags: flags)
        return try txn.run(body)
    }

    /// Returns a value from the database instantiated as type `V` for a key of type `K`.
    /// - parameter type: A type conforming to `DataConvertible` that you want to be instantiated with the value from the database.
    /// - parameter key: A key conforming to `DataConvertible` for which the value will be looked up.
    /// - returns: Returns the value as an instance of type `V` or `nil` if no value exists for the key or the type could not be instatiated with the data.
    /// - note: You can always use `Foundation.Data` as the type. In such case, `nil` will only be returned if there is no value for the key.
    /// - throws: an error if operation fails. See `LMDBError`.
    public func get<V: DataConvertible, K: DataConvertible>(type: V.Type, forKey key: K) throws -> V? {
        return try self.transact(flags: .readOnly) { txn in
            return try txn.get(type: type, forKey: key, fromDb: handle)
        }
    }
    
    /// Check if a value exists for the given key.
    /// - parameter key: The key to check for.
    /// - returns: `true` if the database contains a value for the key. `false` otherwise.
    /// - throws: an error if operation fails. See `LMDBError`.
    public func hasValue<K: DataConvertible>(forKey key: K) throws -> Bool {
        // TODO: Avoid the allocation and cast to Data here.
        return try get(type: Data.self, forKey: key) != nil
    }

    /// Inserts a value into the database.
    /// - parameter value: The value to be put into the database. The value must conform to `DataConvertible`.
    /// - parameter key: The key which the data will be associated with. The key must conform to `DataConvertible`. Passing an empty key will cause an error to be thrown.
    /// - parameter flags: An optional set of flags that modify the behavior if the put operation. Default is [] (empty set).
    /// - throws: an error if operation fails. See `LMDBError`.
    public func put<V: DataConvertible, K: DataConvertible>(value: V, forKey key: K, flags: PutFlags = []) throws {
        try self.transact() { txn in
            try txn.put(key: key, toVal: value, fromDb: handle)
        }
    }

    /// Deletes a value from the database.
    /// - parameter key: The key identifying the database entry to be deleted. The key must conform to `DataConvertible`. Passing an empty key will cause an error to be thrown.
    /// - throws: an error if operation fails. See `LMDBError`.
    public func deleteValue<K: DataConvertible>(forKey key: K) throws {
        return try self.transact() { txn in
            try txn.delete(key, fromDb: handle)
        }
    }
    
    /// Empties the database, removing all key/value pairs.
    /// The database remains open after being emptied and can still be used.
    /// - throws: an error if operation fails. See `LMDBError`.
    public func empty() throws {
        return try self.transact() { txn in
            let result = mdb_drop(txn.handle, handle, 0)
            guard result == 0 else { throw LMDBError(returnCode: result) }
        }
    }

    /// Drops the database, deleting it (along with all it's contents) from the environment.
    /// - warning: Dropping a database also closes it. You may no longer use the database after dropping it.
    /// - seealso: `empty()`
    /// - throws: an error if operation fails. See `LMDBError`.
    public func drop() throws {
        return try self.transact() { txn in
            let result = mdb_drop(txn.handle, handle, 1)
            guard result == 0 else { throw LMDBError(returnCode: result) }
        }
    }
}
