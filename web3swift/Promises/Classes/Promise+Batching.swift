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
    private var batches: Store<Batch>

    public init(provider: Web3Provider, queue: DispatchQueue, policy: DispatchPolicy) {
        self.provider = provider
        self.queue = queue
        self.policy = policy
        self.lockQueue = DispatchQueue(label: "batchingQueue", qos: .userInitiated)
        self.batches = .init(queue: lockQueue)

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
            do {
                let batch = try getBatch()
                return try batch.add(request, maxWaitTime: MAX_WAIT_TIME)
            } catch {
                let returnPromise = Promise<JSONRPCresponse>.pending()
                queue.async {
                    returnPromise.resolver.reject(error)
                }
                return returnPromise.promise
            }
        }
    }

    internal final class Batch: NSObject {
        private var pendingTrigger: Guarantee<Void>?
        private let queue: DispatchQueue
        private (set) var triggered: Bool = false

        var capacity: Int
        var promises: DictionaryStore<UInt64, (promise: Promise<JSONRPCresponse>, seal: Resolver<JSONRPCresponse>)>
        var requests: Store<JSONRPCrequest>

        weak var delegate: BatchDelegate?

        fileprivate func add(_ request: JSONRPCrequest, maxWaitTime: TimeInterval) throws -> Promise<JSONRPCresponse> {
            guard !triggered else {
                throw Web3Error.nodeError("Batch is already in flight")
            }

            let (promise, seal) = Promise<JSONRPCresponse>.pending()

            if let value = promises[request.id] {
                value.seal.reject(Web3Error.processingError("Request ID collision"))
            } else {
                promises[request.id] = (promise, seal)
            }

            requests.append(request)

            if pendingTrigger == nil {
                pendingTrigger = after(seconds: maxWaitTime)
                    .done(on: queue) { [weak self] in
                        self?.trigger()
                    }
            }

            if requests.count() == capacity {
                trigger()
            }

            return promise
        }

        func trigger() {
            guard !triggered else { return }
            triggered = true

            delegate?.didTrigger(id: self)
        }

        init(capacity: Int, queue: DispatchQueue, lockQueue: DispatchQueue) {
            self.capacity = capacity
            self.queue = queue
            self.promises = .init(queue: lockQueue)
            self.requests = .init(queue: lockQueue)
        }
    }

}

extension JSONRPCrequestDispatcher: BatchDelegate {

    func didTrigger(id batch: Batch) {
        let requestsBatch = JSONRPCrequestBatch(requests: batch.requests.allValues())

        provider
            .sendAsync(requestsBatch, queue: queue)
            .done(on: queue, { [weak batches] batchResponse in
                for response in batchResponse.responses {
                    let id = UInt64(response.id)
                    guard let value = batch.promises[id] else {
                        guard let keys = batch.promises.keys() else { return }
                        for key in keys {
                            guard let value = batch.promises[key] else { continue }
                            value.seal.reject(Web3Error.nodeError("Unknown request id"))
                        }
                        return
                    }
                    value.seal.fulfill(response)
                }

                batches?.removeAll(batch)
            }).catch(on: queue, { [weak batches] err in
                guard let keys = batch.promises.keys() else { return }

                for key in keys {
                    guard let value = batch.promises[key] else { continue }
                    value.seal.reject(err)
                }

                batches?.removeAll(batch)
            })
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

protocol BatchDelegate: AnyObject {
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
            queue.sync { [weak self] in
                element = self?.values[key]
            }
            return element
        }
        set {
            dispatchPrecondition(condition: .notOnQueue(queue))
            queue.sync { [weak self] in
                self?.values[key] = newValue
            }
        }
    }

    func keys() -> Dictionary<K, V>.Keys? {
        var keys: Dictionary<K, V>.Keys?
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [weak self] in
            keys = self?.values.keys
        }
        return keys
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
        queue.sync { [weak self] in
            self?.values.append(element)
        }
    }

    func last() -> T? {
        var element: T?
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [weak self] in
            element = self?.values.last
        }

        return element
    }

    func count() -> Int {
        var count: Int = 0
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [weak self] in
            count = self?.values.count ?? 0
        }
        return count
    }

    func allValues() -> [T] {
        var values: [T] = []
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [weak self] in
            values = self?.values ?? []
        }
        return values
    }
}

extension Store where T: Equatable & AnyObject {
    func removeAll(_ elem: T) {
        dispatchPrecondition(condition: .notOnQueue(queue))
        queue.sync { [weak self] in
            self?.values.removeAll(where: { $0 === elem })
        }
    }
}
