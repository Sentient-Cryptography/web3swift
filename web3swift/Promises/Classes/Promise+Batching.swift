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
    private lazy var batches: [Batch] = []

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

        let currentBatch = batches.last ?? createBatch()

        if currentBatch.requests.count % batchLength == 0 || currentBatch.triggered {
            let newBatch = Batch(capacity: Int(batchLength), queue: queue)
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
            lockQueue.async { [weak self] in
                guard let strongSelf = self else { return }

                do {
                    let batch = try strongSelf.getBatch()
                    let internalPromise = try batch.add(request, maxWaitTime: strongSelf.MAX_WAIT_TIME)
                    internalPromise.done(on: strongSelf.queue) { resp in
                        seal.fulfill(resp)
                    }.catch(on: strongSelf.queue) { err in
                        seal.reject(err)
                    }
                } catch {
                    seal.reject(error)
                }
            }

            return promise
        }
    }

    internal final class Batch: NSObject {
        var capacity: Int
        var promises: [UInt64: (promise: Promise<JSONRPCresponse>, resolver: Resolver<JSONRPCresponse>)] = [:]
        var requests: [JSONRPCrequest] = []

        private var pendingTrigger: Guarantee<Void>?
        private let queue: DispatchQueue
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

            if requests.count == capacity {
                trigger()
            }

            return promiseToReturn.promise
        }

        func trigger() {
            guard !triggered else {
                return
            }
            triggered = true

            delegate?.didTrigger(id: self)
        }

        init(capacity: Int, queue: DispatchQueue) {
            self.capacity = capacity
            self.queue = queue
        }
    }

}

extension JSONRPCrequestDispatcher: BatchDelegate {

    func didTrigger(id batch: Batch) {
        let requestsBatch = JSONRPCrequestBatch(requests: batch.requests)

        provider.sendAsync(requestsBatch, queue: queue).done(on: queue, { [weak self] batchResponse in
            for response in batchResponse.responses {
                if batch.promises[UInt64(response.id)] == nil {
                    for k in batch.promises.keys {
                        batch.promises[k]?.resolver.reject(Web3Error.nodeError("Unknown request id"))
                    }
                    return
                }
            }

            for response in batchResponse.responses {
                let promise = batch.promises[UInt64(response.id)]!
                promise.resolver.fulfill(response)
            }
            self?.batches.removeAll(where: { $0 == batch })
        }).catch(on: queue, { [weak self] err in
            for k in batch.promises.keys {
                batch.promises[k]?.resolver.reject(err)
            }
            self?.batches.removeAll(where: { $0 == batch })
        })
    }

    @discardableResult func createBatch() -> Batch {
        switch policy {
        case .NoBatching:
            let batch = Batch(capacity: 32, queue: queue)
            batch.delegate = self

            batches.append(batch)

            return batch
        case .Batch(let count):
            let batch = Batch(capacity: count, queue: queue)
            batch.delegate = self

            batches.append(batch)

            return batch
        }
    }
}

protocol BatchDelegate: class {
    func didTrigger(id batch: JSONRPCrequestDispatcher.Batch)
}
