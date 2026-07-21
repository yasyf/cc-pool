import Darwin
import FuseKit

do {
    if try await CatalogBroker.runChildIfRequested(
        configuration: CCPoolFileProviderConfiguration.brokerConfiguration
    ) {
        exit(0)
    }
} catch {
    fputs("CCPoolStatus: FuseKit broker child failed: \(error)\n", stderr)
    exit(1)
}

let childStatus = CCPoolFuseKitDispatchChild()
if childStatus >= 0 {
    exit(childStatus)
}

guard CCPoolFuseKitStart() == 0 else {
    exit(1)
}

guard CCPoolFuseKitReady() == 0 else {
    _ = CCPoolFuseKitStop()
    exit(1)
}

Task.detached {
    exit(CCPoolFuseKitWait())
}

CCPoolStatusApp.main()
