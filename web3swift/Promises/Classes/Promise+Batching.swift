//
//  Promise+Batching.swift
//  web3swift
//
//  Created by Alexander Vlasov on 17.06.2018.
//  Copyright © 2018 Bankex Foundation. All rights reserved.
//

import Foundation
import PromiseKit

public class JSONRPCrequestDispatcher {
    public var MAX_WAIT_TIME: TimeInterval = 0.1
    public var policy: DispatchPolicy
    public var queue: DispatchQueue

    private var provider: Web3Provider
    private var lockQueue: DispatchQueue
    private lazy var batches: Store<Batch> = .init(queue: lockQueue)

    public init(provider: Web3Provider, queue: DispatchQueue, policy: DispatchPolicy) {
        self.provider = provider
        self.queue = queue
        self.policy = policy
        self.lockQueue = DispatchQueue(label: "batchingQueue", qos: .userInitiated)

        createBatch()
    }

    private func getBatch() throws -> Batch {
        guard case .Batch(let batchLength) = policy else {
            throw Web3Error.inputError("Trying to batch a request when policy is not to batch")
        }

        let currentBatch = batches.last() ?? createBatch()

        if currentBatch.requests.count() % batchLength == 0 || currentBatch.triggered {
            let newBatch = Batch(capacity: Int(batchLength), queue: queue, lockQueue: lockQueue)
            newBatch.delegate = self

            batches.append(newBatch)

            return newBatch
        }

        return currentBatch
    }

    public enum DispatchPolicy {
        case Batch(Int)
        case NoBatching
    }

    func addToQueue(request: JSONRPCrequest) -> Promise<JSONRPCresponse> {
        switch policy {
        case .NoBatching:
            return provider.sendAsync(request, queue: queue)
        case .Batch:
            let (promise, seal) = Promise<JSONRPCresponse>.pending()

            do {
                let batch = try getBatch()
                let internalPromise = try batch.add(request, maxWaitTime: MAX_WAIT_TIME)
                internalPromise.done(on: queue) { resp in
                    seal.fulfill(resp)
                }.catch(on: queue) { err in
                    seal.reject(err)
                }
            } catch {
                seal.reject(error)
            }

            return promise
        }
    }

    internal final class Batch: NSObject {
        var capacity: Int
        lazy var promises: DictionaryStore<UInt64, (promise: Promise<JSONRPCresponse>, resolver: Resolver<JSONRPCresponse>)> = .init(queue: lockQueue)
        lazy var requests: Store<JSONRPCrequest> = .init(queue: lockQueue)

        private var pendingTrigger: Guarantee<Void>?
        private let queue: DispatchQueue
        private let lockQueue: DispatchQueue
        private (set) var triggered: Bool = false
        weak var delegate: BatchDelegate?

        fileprivate func add(_ request: JSONRPCrequest, maxWaitTime: TimeInterval) throws -> Promise<JSONRPCresponse> {
            guard !triggered else {
                throw Web3Error.nodeError("Batch is already in flight")
            }

            let requestID = request.id
            let promiseToReturn = Promise<JSONRPCresponse>.pending()
            var shouldAddPromise = true

            if promises[requestID] != nil {
                shouldAddPromise = false
                promiseToReturn.resolver.reject(Web3Error.processingError("Request ID collision"))
            }

            if shouldAddPromise {
                promises[requestID] = promiseToReturn
            }

            requests.append(request)

            if pendingTrigger == nil {
                pendingTrigger = after(seconds: maxWaitTime).done(on: queue) { [weak self] in
                    guard let strongSelf = self else {
                        return
                    }
                    strongSelf.trigger()
                }
            }

            if requests.count() == capacity {
                trigger()
            }

            return promiseToReturn.promise
        }

        func trigger() {
            guard !triggered else { return }
            triggered = true

            delegate?.didTrigger(id: self)
        }

        init(capacity: Int, queue: DispatchQueue, lockQueue: DispatchQueue) {
            self.capacity = capacity
            self.queue = queue
            self.lockQueue = lockQueue
        }
    }

}

extension JSONRPCrequestDispatcher: BatchDelegate {

    func didTrigger(id batch: Batch) {
        let requestsBatch = JSONRPCrequestBatch(requests: batch.requests.allValues())

        provider.sendAsync(requestsBatch, queue: queue).done(on: queue, { batchResponse in
            for response in batchResponse.responses {
                if batch.promises[UInt64(response.id)] == nil {
                    for k in batch.promises.keys() {
                        batch.promises[k]?.resolver.reject(Web3Error.nodeError("Unknown request id"))
                    }
                    return
                }
            }

            for response in batchResponse.responses {
                batch.promises[UInt64(response.id)]?.resolver.fulfill(response)
            }
        }).catch(on: queue, { err in
            for k in batch.promises.keys() {
                batch.promises[k]?.resolver.reject(err)
            }
        }).finally { [weak self] in
            self?.batches.removeAll(where: { $0 == batch })
        }
    }

    @discardableResult func createBatch() -> Batch {
        switch policy {
        case .NoBatching:
            let batch = Batch(capacity: 32, queue: queue, lockQueue: lockQueue)
            batch.delegate = self

            batches.append(batch)

            return batch
        case .Batch(let count):
            let batch = Batch(capacity: count, queue: queue, lockQueue: lockQueue)
            batch.delegate = self

            batches.append(batch)

            return batch
        }
    }
}

protocol BatchDelegate: class {
    func didTrigger(id batch: JSONRPCrequestDispatcher.Batch)
}

class DictionaryStore<K: Hashable, V> {
    private var values: [K: V] = [:]
    private let queue: DispatchQueue

    public init(queue: DispatchQueue = DispatchQueue(label: "RealmStore.syncQueue", qos: .background)) {
        self.queue = queue
    }

    public subscript(key: K) -> V? {
        get {
            var element: V?
            dispatchPrecondition(condition: .notOnQueue(queue))
            queue.sync { [unowned self] in
                element = self.values[key]
            }
            return element
        }
        set {
            dispatchPrecondition(condition: .notOnQueue(queue))
            queue.sync { [unowned self] in
                self.values[key] = newValue
            }
        }
    }

    func keys() -> Dictionary<K, V>.Keys {
        var keys: Dictionary<K, V>.Keys?
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [unowned self] in
            keys = self.values.keys
        }
        return keys!
    }

}

class Store<T> {
    private var values: [T] = []
    private let queue: DispatchQueue

    public init(queue: DispatchQueue = DispatchQueue(label: "RealmStore.syncQueue", qos: .background)) {
        self.queue = queue
    }

    func append(_ element: T) {
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [unowned self] in
            self.values.append(element)
        }
    }

    func last() -> T? {
        var element: T?
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [unowned self] in
            element = self.values.last
        }

        return element
    }

    func removeAll(where closure: (T) -> Bool) {
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [unowned self] in
            self.values.removeAll(where: closure)
        }
    }

    func count() -> Int {
        var count: Int = 0
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [unowned self] in
            count = self.values.count
        }
        return count
    }

    func allValues() -> [T] {
        var values: [T] = []
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [unowned self] in
            values = self.values
        }
        return values
    }
}
