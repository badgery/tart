import Foundation
import Darwin
import System

// Avoid Tart's Darwin type shadowing the system functions
private let systemBind = bind
private let systemWrite = write

/// Serves Softnet's control channel on a Unix socket in the VM's directory.
///
/// Softnet answers each request line with exactly one response line, so clients are served one
/// at a time and only complete lines are relayed. A client that disconnects mid-request therefore
/// cannot leave a partial request or an unread response for the next one.
class SoftnetControlSocket {
  // Softnet's request limit plus the newline
  static let maxLineBytes = 1024 * 1024 + 1

  let socketURL: URL
  private let listenFD: Int32
  private let softnetFD: Int32

  /// Starts serving and returns the descriptor to pass to Softnet as its control channel.
  static func start(_ socketURL: URL) throws -> Int32 {
    var fds: [Int32] = [-1, -1]
    guard socketpair(AF_UNIX, SOCK_STREAM, 0, &fds) == 0 else {
      throw SoftnetError.InitializationFailed(why: "socketpair() failed: \(Errno(rawValue: errno))")
    }

    do {
      // Keep the relay's end out of Softnet, so its exit is seen as end of file
      guard fcntl(fds[1], F_SETFD, FD_CLOEXEC) == 0 else {
        throw SoftnetError.InitializationFailed(why: "fcntl() failed: \(Errno(rawValue: errno))")
      }

      let controlSocket = try SoftnetControlSocket(socketURL, softnetFD: fds[1])
      controlSocket.serve()
    } catch {
      close(fds[0])
      close(fds[1])
      throw error
    }

    return fds[0]
  }

  init(_ socketURL: URL, softnetFD: Int32) throws {
    self.socketURL = socketURL
    self.softnetFD = softnetFD
    try Self.disableSIGPIPE(softnetFD)

    // Remove the socket file from previous "tart run" invocations, if any,
    // otherwise we may get the "address already in use" error
    try? FileManager.default.removeItem(at: socketURL)

    // Bind relative to the VM's base directory to work around the Unix domain
    // socket 104 byte limitation, as ControlSocket does
    if let baseURL = socketURL.baseURL {
      FileManager.default.changeCurrentDirectoryPath(baseURL.absoluteURL.path(percentEncoded: false))
    }

    listenFD = socket(AF_UNIX, SOCK_STREAM, 0)
    guard listenFD >= 0, fcntl(listenFD, F_SETFD, FD_CLOEXEC) == 0 else {
      throw SoftnetError.InitializationFailed(why: "socket() failed: \(Errno(rawValue: errno))")
    }

    do {
      // Accepted connections inherit this, so a client that has already gone can't raise SIGPIPE
      try Self.disableSIGPIPE(listenFD)
      try Self.bindSocket(listenFD, path: socketURL.relativePath)

      // Restrict who can connect before accepting any connections
      guard chmod(socketURL.relativePath, 0o600) == 0, listen(listenFD, 1) == 0 else {
        throw SoftnetError.InitializationFailed(why: "failed to listen on \(socketURL.relativePath): \(Errno(rawValue: errno))")
      }
    } catch {
      close(listenFD)
      throw error
    }
  }

  private func serve() {
    let thread = Thread {
      let softnet = LineReader(self.softnetFD)

      while true {
        let clientFD = accept(self.listenFD, nil, nil)
        if clientFD < 0 {
          if errno == EINTR || errno == ECONNABORTED {
            continue
          }

          return
        }

        let softnetAlive = self.relay(clientFD, softnet)
        close(clientFD)

        if !softnetAlive {
          close(self.listenFD)
          try? FileManager.default.removeItem(at: self.socketURL)

          return
        }
      }
    }
    thread.name = "softnet-control-socket"
    thread.start()
  }

  /// Relays the client's requests until it disconnects. Returns false once Softnet has gone away.
  private func relay(_ clientFD: Int32, _ softnet: LineReader) -> Bool {
    // Writing to a client without this could raise SIGPIPE, and it fails when the client has already gone
    guard (try? Self.disableSIGPIPE(clientFD)) != nil else {
      return true
    }

    let client = LineReader(clientFD)

    while let request = client.next() {
      guard Self.writeAll(softnetFD, request) else {
        return false
      }

      guard let response = softnet.next() else {
        return false
      }

      // The response is read even if the client has gone, so the next client starts in sync
      guard Self.writeAll(clientFD, response) else {
        return true
      }
    }

    return true
  }

  private static func bindSocket(_ fd: Int32, path: String) throws {
    var address = sockaddr_un()
    address.sun_family = sa_family_t(AF_UNIX)

    let pathBytes = Array(path.utf8)
    let capacity = MemoryLayout.size(ofValue: address.sun_path)
    guard pathBytes.count < capacity else {
      throw SoftnetError.InitializationFailed(why: "socket path \(path) is too long")
    }

    withUnsafeMutableBytes(of: &address.sun_path) { buffer in
      buffer.copyBytes(from: pathBytes)
      buffer[pathBytes.count] = 0
    }

    let result = withUnsafePointer(to: &address) { pointer in
      pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
        systemBind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
      }
    }
    guard result == 0 else {
      throw SoftnetError.InitializationFailed(why: "failed to bind \(path): \(Errno(rawValue: errno))")
    }
  }

  private static func disableSIGPIPE(_ fd: Int32) throws {
    var enabled: Int32 = 1
    guard setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, socklen_t(MemoryLayout<Int32>.size)) == 0 else {
      throw SoftnetError.InitializationFailed(why: "setsockopt(SO_NOSIGPIPE) failed: \(Errno(rawValue: errno))")
    }
  }

  private static func writeAll(_ fd: Int32, _ data: Data) -> Bool {
    data.withUnsafeBytes { buffer in
      var offset = 0

      while offset < buffer.count {
        let written = systemWrite(fd, buffer.baseAddress! + offset, buffer.count - offset)
        if written < 0 {
          if errno == EINTR {
            continue
          }

          return false
        }

        offset += written
      }

      return true
    }
  }
}

/// Reads newline-terminated lines from a descriptor.
private class LineReader {
  private let fd: Int32
  private var buffer = Data()

  init(_ fd: Int32) {
    self.fd = fd
  }

  /// Returns the next line including its newline, or nil on end of file, an error,
  /// or a line longer than SoftnetControlSocket.maxLineBytes.
  func next() -> Data? {
    var chunk = [UInt8](repeating: 0, count: 64 * 1024)

    while true {
      if let newline = buffer.firstIndex(of: UInt8(ascii: "\n")) {
        let line = buffer[buffer.startIndex...newline]
        buffer = Data(buffer[buffer.index(after: newline)...])

        return Data(line)
      }

      if buffer.count >= SoftnetControlSocket.maxLineBytes {
        return nil
      }

      let count = read(fd, &chunk, chunk.count)
      if count < 0 && errno == EINTR {
        continue
      }
      if count <= 0 {
        return nil
      }

      buffer.append(contentsOf: chunk[0..<count])
    }
  }
}
