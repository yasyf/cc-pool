import SwiftUI
import WidgetKit

// LSUIElement host app for the widget extension. Its only jobs: exist so the
// widget appears in the gallery after first launch and, while daemonkit's
// fixed-app service keeps it running, watch ~/.cc-pool for the daemon's
// status.json rewrites so the widget tracks the 3-minute poll cadence instead
// of WidgetKit's lazy refresh budget. The widget still works without the app
// running — just less fresh. The fixed app also owns the embedded FuseKit
// runtime and its exact broker child mode.

struct CCPoolStatusApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) var delegate

    var body: some Scene {
        Settings { EmptyView() } // agent app: no windows
    }
}

@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate {
    private let watcher = StatusWatcher()
    private var terminationTask: Task<Void, Never>?

    func applicationDidFinishLaunching(_: Notification) {
        watcher.start() // fires an immediate reload once the watch is armed
    }

    func applicationShouldTerminate(_ sender: NSApplication) -> NSApplication.TerminateReply {
        guard terminationTask == nil else { return .terminateLater }
        terminationTask = Task { [weak sender] in
            if CCPoolFuseKitStop() != 0 {
                NSLog("CCPoolStatus: FuseKit runtime shutdown failed")
            }
            sender?.reply(toApplicationShouldTerminate: true)
        }
        return .terminateLater
    }
}

final class StatusWatcher {
    private var source: DispatchSourceFileSystemObject?
    private var lastReload = Date.distantPast

    /// Watches the directory, not the file: the daemon's atomic temp+rename
    /// replaces the inode, which would kill a file-fd vnode watcher.
    func start() {
        let dir = StatusFile.url.deletingLastPathComponent().path // ~/.cc-pool
        let fd = open(dir, O_EVTONLY)
        guard fd >= 0 else {
            // ~/.cc-pool missing (pre-`ccp init`): retry once a minute.
            DispatchQueue.main.asyncAfter(deadline: .now() + 60) { [weak self] in
                self?.start()
            }
            return
        }
        let src = DispatchSource.makeFileSystemObjectSource(
            fileDescriptor: fd, eventMask: [.write, .rename, .delete], queue: .main
        )
        src.setEventHandler { [weak self] in self?.handleEvent() }
        src.setCancelHandler { close(fd) }
        src.resume()
        source = src
        changed() // freshen now — covers re-arming onto a recreated dir
    }

    private func handleEvent() {
        guard let src = source else { return }
        // ~/.cc-pool itself deleted or replaced (rm -rf + re-`ccp init`): the
        // fd references the dead vnode and will never fire again. Re-arm on
        // the new dir; start()'s retry loop covers the not-yet-recreated window.
        if !src.data.isDisjoint(with: [.delete, .rename]) {
            src.cancel() // cancel handler closes the fd
            source = nil
            start()
            return
        }
        changed()
    }

    private func changed() {
        // Coalesce to >=5min: the daemon rewrites status.json on a ~3-min cadence,
        // and WidgetKit's refresh budget punishes a reload per change.
        guard Date().timeIntervalSince(lastReload) > 300 else { return }
        lastReload = Date()
        WidgetCenter.shared.reloadAllTimelines()
    }
}
