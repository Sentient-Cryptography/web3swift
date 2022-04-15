//
//  Web3+Instance.swift
//  web3swift
//
//  Created by Alexander Vlasov on 19.12.2017.
//  Copyright © 2017 Bankex Foundation. All rights reserved.
//

import Foundation
import BigInt
import PromiseKit

//NOTE: use alias to avoid some misunderstanding when var called in same way as type
typealias SafeWeb3 = web3

public class web3: Web3OptionsInheritable {
    public var provider : Web3Provider
    public var options : Web3Options = Web3Options.defaultOptions()
    public var defaultBlock = "latest"
    public var queue: OperationQueue
    public var requestDispatcher: JSONRPCrequestDispatcher

    var dispatcher: OperationDispatcher

    public func send(request: JSONRPCrequest) -> [String: Any]? {
        return self.provider.send(request: request)
    }
    public func dispatch(_ request: JSONRPCrequest) -> Promise<JSONRPCresponse> {
        return requestDispatcher.addToQueue(request: request)
    }

    public init(provider prov: Web3Provider, queue: OperationQueue? = nil, dispatcher: OperationDispatcher? = nil, requestDispatcher: JSONRPCrequestDispatcher? = nil) {
        provider = prov
        if queue == nil {
            self.queue = OperationQueue.init()
            self.queue.maxConcurrentOperationCount = 32
            self.queue.underlyingQueue = DispatchQueue.global(qos: .userInteractive)

        } else {
            self.queue = queue!
        }
        if dispatcher == nil {
            self.dispatcher = OperationDispatcher(provider: provider, queue: self.queue, policy: .Batch(16))
        } else {
            self.dispatcher = dispatcher!
        }
        if requestDispatcher == nil {
            self.requestDispatcher = JSONRPCrequestDispatcher(provider: provider, queue: self.queue.underlyingQueue!, policy: .Batch(32))
        } else {
            self.requestDispatcher = requestDispatcher!
        }
    }


    public func addKeystoreManager(_ manager: KeystoreManager?) {
        self.provider.attachedKeystoreManager = manager
    }

    public class Eth: Web3OptionsInheritable {
        var web3: web3
        public var options: Web3Options {
            return self.web3.options
        }

        public init(provider prov: Web3Provider, web3 web3instance: web3) {
            web3 = web3instance
        }
    }

    public class Personal:Web3OptionsInheritable {
        var web3: web3
        public var options: Web3Options {
            return self.web3.options
        }

        public init(provider prov: Web3Provider, web3 web3instance: web3) {
            web3 = web3instance
        }
    }

    public class Web3Wallet {
        var web3: web3

        public init(provider prov: Web3Provider, web3 web3instance: web3) {
            web3 = web3instance
        }
    }

    public class BrowserFunctions: Web3OptionsInheritable {
        var web3: web3
        public var options: Web3Options {
            return self.web3.options
        }

        public init(provider prov: Web3Provider, web3 web3instance: web3) {
            web3 = web3instance
        }
    }

}
