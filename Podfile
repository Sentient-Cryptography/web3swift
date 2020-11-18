def import_pods
  pod 'Alamofire', '~> 4.7'
  pod 'Alamofire-Synchronous', '~> 4.0'
  pod "PromiseKit", "~> 6.0"
  pod 'BigInt', '~> 3.0'
  pod 'CryptoSwift', '~> 1.0'
  pod 'Result', '~> 3.0.0'
  pod 'libsodium', :modular_headers => true
  pod 'secp256k1_ios', :git => 'https://github.com/shamatar/secp256k1_ios.git', :modular_headers => true
end

target 'web3swift-macOS' do
  platform :osx, '10.13'
#  use_frameworks!
  use_modular_headers!
  import_pods
  
  target 'web3swift-macOS_Tests' do
    inherit! :search_paths
  end
end

target 'web3swift-iOS' do
  platform :ios, '12.0'
#  use_frameworks!
  use_modular_headers! 
  import_pods
  
  target 'web3swift-iOS_Tests' do
    inherit! :search_paths
  end
end

post_install do |installer|
  installer.pods_project.targets.each do |target|
    if ['Result-iOS'].include? target.name
      target.build_configurations.each do |config|
        config.build_settings['SWIFT_VERSION'] = '4.2'
      end
    end
  end
end
