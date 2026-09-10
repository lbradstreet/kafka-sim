use crate::provider::{
    self, Flags, Owner, ReadinessListener, ReadinessStream, Response, Shared, VectoredResponse,
};
use kr_runtime::CompletionError;
use kr_runtime_io::network::{
    ConnectRequest, ListenRequest, NetworkError, NetworkFailure, NetworkOperationKind, ReadRequest,
    ReadResult, VectoredWriteFailure, VectoredWriteRequest, VectoredWriteResult, WriteRequest,
    WriteResult,
};
use rustix::{
    event::epoll,
    fd::OwnedFd,
    io::Errno,
    net::{self, AddressFamily, SocketFlags, SocketType},
};
use std::{
    collections::{BTreeMap, VecDeque},
    io::IoSlice,
    net::SocketAddr,
    sync::{Arc, Weak, atomic::Ordering},
    time::{Duration, Instant},
};

#[derive(Clone, Copy)]
pub(crate) enum ResourceKind {
    Stream,
    Listener,
}
pub(crate) struct Resource {
    pub(crate) shared: Weak<Shared>,
    pub(crate) kind: ResourceKind,
    pub(crate) id: u64,
}
impl Drop for Resource {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            let mut q = provider::lock(&shared.queue);
            match self.kind {
                ResourceKind::Stream => q.streams -= 1,
                ResourceKind::Listener => q.listeners -= 1,
            }
        }
    }
}
pub(crate) enum Command {
    Listen {
        request: ListenRequest<SocketAddr>,
        resource: Resource,
        owner: Arc<Owner>,
        responder: Response<ReadinessListener>,
    },
    Connect {
        request: ConnectRequest<SocketAddr>,
        deadline: Instant,
        resource: Resource,
        owner: Arc<Owner>,
        responder: Response<ReadinessStream>,
    },
    Accept {
        id: u64,
        resource: Resource,
        owner: Arc<Owner>,
        responder: Response<ReadinessStream>,
    },
    Read {
        id: u64,
        request: ReadRequest,
        responder: Response<ReadResult>,
    },
    Write {
        id: u64,
        request: WriteRequest,
        responder: Response<WriteResult>,
    },
    Vectored {
        id: u64,
        request: VectoredWriteRequest,
        responder: VectoredResponse,
    },
    Shutdown {
        id: u64,
        responder: Response<()>,
    },
    Close {
        id: u64,
        responder: Option<Response<()>>,
    },
}
impl Command {
    pub(crate) fn fail(self, error: NetworkError) {
        match self {
            Self::Listen { responder, .. } => fail(responder, error),
            Self::Connect { responder, .. } | Self::Accept { responder, .. } => {
                fail(responder, error)
            }
            Self::Read {
                request, responder, ..
            } => fail_buffer(responder, error, request.buffer),
            Self::Write {
                request, responder, ..
            } => fail_buffer(responder, error, request.buffer),
            Self::Vectored {
                request, responder, ..
            } => responder.complete(Err(CompletionError::not_applied(
                VectoredWriteFailure::new(error, request.segments, 0),
            ))),
            Self::Shutdown { responder, .. } => fail(responder, error),
            Self::Close {
                responder: Some(responder),
                ..
            } => fail(responder, error),
            Self::Close {
                responder: None, ..
            } => {}
        }
    }
}
struct ReadWork {
    request: ReadRequest,
    responder: Response<ReadResult>,
}
enum WriteWork {
    Bytes(WriteRequest, Response<WriteResult>),
    Vectored(VectoredWriteRequest, VectoredResponse),
    Shutdown(Response<()>),
}
impl WriteWork {
    fn fail(self, error: NetworkError) {
        match self {
            Self::Bytes(request, responder) => fail_buffer(responder, error, request.buffer),
            Self::Vectored(request, responder) => responder.complete(Err(
                CompletionError::not_applied(VectoredWriteFailure::new(error, request.segments, 0)),
            )),
            Self::Shutdown(responder) => fail(responder, error),
        }
    }
}
struct AcceptWork {
    resource: Resource,
    owner: Arc<Owner>,
    responder: Response<ReadinessStream>,
}
enum Pending {
    Connecting {
        deadline: Instant,
        owner: Arc<Owner>,
        responder: Response<ReadinessStream>,
    },
    Listener {
        accepts: VecDeque<AcceptWork>,
    },
    Stream {
        reads: VecDeque<ReadWork>,
        writes: VecDeque<WriteWork>,
        write_closed: bool,
    },
}
struct Node {
    fd: OwnedFd,
    resource: Resource,
    flags: Arc<Flags>,
    registered: bool,
    pending: Pending,
}
impl Node {
    fn interest(&self) -> epoll::EventFlags {
        match &self.pending {
            Pending::Connecting { .. } => epoll::EventFlags::OUT,
            Pending::Listener { accepts } => {
                if accepts.is_empty() {
                    epoll::EventFlags::empty()
                } else {
                    epoll::EventFlags::IN
                }
            }
            Pending::Stream { reads, writes, .. } => {
                let mut flags = epoll::EventFlags::empty();
                if !reads.is_empty() {
                    flags |= epoll::EventFlags::IN;
                }
                if !writes.is_empty() {
                    flags |= epoll::EventFlags::OUT;
                }
                flags
            }
        }
    }
    fn fail(self, error: NetworkError) {
        let Self {
            fd,
            resource,
            flags,
            mut pending,
            ..
        } = self;
        flags.write_closed.store(true, Ordering::Release);
        let _ = net::shutdown(&fd, net::Shutdown::Both);
        drop(fd);
        drop(resource);
        flags.closed.store(true, Ordering::Release);
        // The socket and resource are retired before any caller waker can run.
        match &mut pending {
            Pending::Stream { reads, writes, .. } => {
                for work in reads.drain(..) {
                    fail_buffer(work.responder, error.clone(), work.request.buffer);
                }
                for work in writes.drain(..) {
                    work.fail(error.clone());
                }
            }
            Pending::Listener { accepts } => {
                for work in accepts.drain(..) {
                    fail(work.responder, error.clone());
                }
            }
            Pending::Connecting { .. } => {}
        }
        if let Pending::Connecting { responder, .. } = pending {
            responder.complete(Err(CompletionError::may_have_applied(
                NetworkFailure::without_buffer(error),
            )));
        }
    }
}
struct Reactor {
    epoll: OwnedFd,
    shared: Arc<Shared>,
    nodes: BTreeMap<u64, Node>,
}

pub(crate) fn run(epoll: OwnedFd, shared: Arc<Shared>) {
    let mut reactor = Reactor {
        epoll,
        shared,
        nodes: BTreeMap::new(),
    };
    // Any unexpected internal panic must still close descriptors and complete
    // every retained response. Caller wakers already use panic containment.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| reactor.drive()));
    reactor.shared.stopped.store(true, Ordering::Release);
    for (_, node) in std::mem::take(&mut reactor.nodes) {
        node.fail(NetworkError::DriverStopped);
    }
    while let Some(command) = reactor.shared.pop() {
        command.fail(NetworkError::DriverStopped);
    }
}
impl Reactor {
    fn drive(&mut self) {
        let mut events = Vec::new();
        let mut expired = Vec::new();
        if events.try_reserve_exact(128).is_err()
            || expired
                .try_reserve_exact(self.shared.config.max_streams)
                .is_err()
        {
            return;
        }
        while !self.shared.stopped.load(Ordering::Acquire) {
            // Command count is bounded at admission. Drain only one bounded
            // round so readiness on another socket cannot be starved by ingress.
            for _ in 0..128 {
                match self.shared.pop() {
                    Some(command) => self.command(command),
                    None => break,
                }
            }
            let immediate = !provider::lock(&self.shared.queue).commands.is_empty();
            let now = Instant::now();
            self.expire_connects(now, &mut expired);
            let deadline = self
                .nodes
                .values()
                .filter_map(|node| match node.pending {
                    Pending::Connecting { deadline, .. } => Some(deadline),
                    _ => None,
                })
                .min();
            let timeout = wait_timeout(now, deadline, immediate);
            events.clear();
            match epoll::wait(
                &self.epoll,
                rustix::buffer::spare_capacity(&mut events),
                timeout.as_ref(),
            ) {
                Ok(_) => {}
                Err(Errno::INTR) => continue,
                Err(_) => return,
            }
            self.expire_connects(Instant::now(), &mut expired);
            for event in events.drain(..) {
                let id = event.data.u64();
                if id == 0 {
                    let mut buffer = [0u8; 8];
                    loop {
                        match rustix::io::read(&*self.shared.wake, &mut buffer) {
                            Ok(_) => {}
                            Err(Errno::INTR) => continue,
                            Err(_) => break,
                        }
                    }
                } else {
                    self.progress(id);
                }
            }
        }
    }
    fn expire_connects(&mut self, now: Instant, expired: &mut Vec<u64>) {
        expired.clear();
        expired.extend(
            self.nodes
                .iter()
                .filter_map(|(&id, node)| match node.pending {
                    Pending::Connecting { deadline, .. } if now >= deadline => Some(id),
                    _ => None,
                }),
        );
        for id in expired.drain(..) {
            if let Some(node) = self.nodes.remove(&id) {
                if node.registered {
                    let _ = epoll::delete(&self.epoll, &node.fd);
                }
                node.fail(provider::backend(
                    NetworkOperationKind::Connect,
                    Errno::TIMEDOUT,
                ));
            }
        }
    }
    fn command(&mut self, command: Command) {
        match command {
            Command::Listen {
                request,
                resource,
                owner,
                responder,
            } => {
                let result = (|| {
                    let fd = socket(request.address, self.shared.config.socket_buffer_bytes)?;
                    net::bind(&fd, &request.address)
                        .map_err(|e| provider::backend(NetworkOperationKind::Listen, e))?;
                    net::listen(&fd, request.backlog as i32)
                        .map_err(|e| provider::backend(NetworkOperationKind::Listen, e))?;
                    let address = SocketAddr::try_from(
                        net::getsockname(&fd)
                            .map_err(|e| provider::backend(NetworkOperationKind::Listen, e))?,
                    )
                    .map_err(|_| NetworkError::InvalidRequest {
                        reason: "non-IP TCP address",
                    })?;
                    Ok((fd, address))
                })();
                match result {
                    Err(error) => fail(responder, error),
                    Ok((fd, address)) => {
                        let id = resource.id;
                        let flags = Flags::new();
                        self.nodes.insert(
                            id,
                            Node {
                                fd,
                                resource,
                                flags: flags.clone(),
                                registered: false,
                                pending: Pending::Listener {
                                    accepts: VecDeque::new(),
                                },
                            },
                        );
                        responder.complete(Ok(ReadinessListener {
                            owner,
                            id,
                            flags,
                            address,
                        }));
                    }
                }
            }
            Command::Connect {
                request,
                deadline,
                resource,
                owner,
                responder,
            } => {
                if Instant::now() >= deadline {
                    drop(resource);
                    fail(
                        responder,
                        provider::backend(NetworkOperationKind::Connect, Errno::TIMEDOUT),
                    );
                    return;
                }
                let result = (|| {
                    let fd = socket(request.local, self.shared.config.socket_buffer_bytes)?;
                    net::bind(&fd, &request.local)
                        .map_err(|e| provider::backend(NetworkOperationKind::Connect, e))?;
                    match net::connect(&fd, &request.remote) {
                        Ok(()) | Err(Errno::INPROGRESS) => Ok(fd),
                        Err(e) => Err(provider::backend(NetworkOperationKind::Connect, e)),
                    }
                })();
                match result {
                    Err(error) => fail(responder, error),
                    Ok(fd) => {
                        let node = Node {
                            fd,
                            resource,
                            flags: Flags::new(),
                            registered: false,
                            pending: Pending::Connecting {
                                owner,
                                responder,
                                deadline,
                            },
                        };
                        self.store(node);
                        // Completion is evaluated only after writable/error
                        // readiness. SO_ERROR==0 before that is not connected.
                    }
                }
            }
            Command::Close { id, responder } => {
                if let Some(node) = self.nodes.remove(&id) {
                    if node.registered {
                        let _ = epoll::delete(&self.epoll, &node.fd);
                    }
                    let error = if matches!(node.pending, Pending::Listener { .. }) {
                        NetworkError::ListenerClosed
                    } else {
                        NetworkError::ConnectionClosed
                    };
                    node.fail(error);
                }
                if let Some(responder) = responder {
                    responder.complete(Ok(()));
                }
            }
            other => {
                let id = match &other {
                    Command::Accept { id, .. }
                    | Command::Read { id, .. }
                    | Command::Write { id, .. }
                    | Command::Vectored { id, .. }
                    | Command::Shutdown { id, .. } => *id,
                    _ => unreachable!("control variants handled above"),
                };
                let Some(node) = self.nodes.get_mut(&id) else {
                    let error = if matches!(other, Command::Accept { .. }) {
                        NetworkError::ListenerClosed
                    } else {
                        NetworkError::ConnectionClosed
                    };
                    other.fail(error);
                    return;
                };
                match (&mut node.pending, other) {
                    (
                        Pending::Listener { accepts },
                        Command::Accept {
                            resource,
                            owner,
                            responder,
                            ..
                        },
                    ) => {
                        if accepts.try_reserve(1).is_err() {
                            fail(
                                responder,
                                provider::exhausted(
                                    "accept queue allocation",
                                    self.shared.config.max_control_operations,
                                ),
                            );
                        } else {
                            accepts.push_back(AcceptWork {
                                resource,
                                owner,
                                responder,
                            });
                        }
                    }
                    (
                        Pending::Stream { reads, .. },
                        Command::Read {
                            request, responder, ..
                        },
                    ) => {
                        if reads.try_reserve(1).is_err() {
                            fail_buffer(
                                responder,
                                provider::exhausted(
                                    "read queue allocation",
                                    self.shared.config.max_read_operations,
                                ),
                                request.buffer,
                            );
                        } else {
                            reads.push_back(ReadWork { request, responder });
                        }
                    }
                    (
                        Pending::Stream { writes, .. },
                        Command::Write {
                            request, responder, ..
                        },
                    ) => push_write(
                        writes,
                        WriteWork::Bytes(request, responder),
                        self.shared.config.max_write_operations,
                    ),
                    (
                        Pending::Stream { writes, .. },
                        Command::Vectored {
                            request, responder, ..
                        },
                    ) => push_write(
                        writes,
                        WriteWork::Vectored(request, responder),
                        self.shared.config.max_write_operations,
                    ),
                    (Pending::Stream { writes, .. }, Command::Shutdown { responder, .. }) => {
                        push_write(
                            writes,
                            WriteWork::Shutdown(responder),
                            self.shared.config.max_control_operations,
                        )
                    }
                    (_, command) => command.fail(NetworkError::ConnectionClosed),
                }
                self.progress(id);
            }
        }
    }
    fn store(&mut self, mut node: Node) {
        let interest = node.interest();
        let result = if interest.is_empty() {
            if node.registered {
                epoll::delete(&self.epoll, &node.fd)
            } else {
                Ok(())
            }
        } else if node.registered {
            epoll::modify(
                &self.epoll,
                &node.fd,
                epoll::EventData::new_u64(node.resource.id),
                interest,
            )
        } else {
            epoll::add(
                &self.epoll,
                &node.fd,
                epoll::EventData::new_u64(node.resource.id),
                interest,
            )
        };
        match result {
            Ok(()) => {
                node.registered = !interest.is_empty();
                self.nodes.insert(node.resource.id, node);
            }
            Err(error) => node.fail(provider::backend(NetworkOperationKind::Connect, error)),
        }
    }
    fn progress(&mut self, id: u64) {
        let Some(mut node) = self.nodes.remove(&id) else {
            return;
        };
        match node.pending {
            Pending::Connecting {
                owner, responder, ..
            } => {
                match net::sockopt::socket_error(&node.fd).and_then(|result| result) {
                    Ok(()) => {
                        node.pending = Pending::Stream {
                            reads: VecDeque::new(),
                            writes: VecDeque::new(),
                            write_closed: false,
                        };
                        let flags = node.flags.clone();
                        self.store(node);
                        if self.nodes.contains_key(&id) {
                            responder.complete(Ok(ReadinessStream { owner, id, flags }));
                        } else {
                            fail(responder, NetworkError::DriverStopped);
                        }
                    }
                    Err(error) => {
                        if node.registered {
                            let _ = epoll::delete(&self.epoll, &node.fd);
                        }
                        node.flags.closed.store(true, Ordering::Release);
                        fail(
                            responder,
                            provider::backend(NetworkOperationKind::Connect, error),
                        );
                    }
                }
                return;
            }
            Pending::Listener { ref mut accepts } => {
                // Bound accepts per readiness notification; level readiness
                // schedules another notification for work left behind.
                for _ in 0..64 {
                    let Some(work) = accepts.pop_front() else {
                        break;
                    };
                    match net::accept_with(&node.fd, SocketFlags::CLOEXEC | SocketFlags::NONBLOCK) {
                        Ok(fd) => {
                            if let Err(error) =
                                configure_socket(&fd, self.shared.config.socket_buffer_bytes)
                            {
                                fail(work.responder, error);
                                continue;
                            }
                            let stream_id = work.resource.id;
                            let flags = Flags::new();
                            self.nodes.insert(
                                stream_id,
                                Node {
                                    fd,
                                    resource: work.resource,
                                    flags: flags.clone(),
                                    registered: false,
                                    pending: Pending::Stream {
                                        reads: VecDeque::new(),
                                        writes: VecDeque::new(),
                                        write_closed: false,
                                    },
                                },
                            );
                            work.responder.complete(Ok(ReadinessStream {
                                owner: work.owner,
                                id: stream_id,
                                flags,
                            }));
                        }
                        Err(Errno::AGAIN | Errno::INTR) => {
                            accepts.push_front(work);
                            break;
                        }
                        Err(error) => fail(
                            work.responder,
                            provider::backend(NetworkOperationKind::Accept, error),
                        ),
                    }
                }
            }
            Pending::Stream {
                ref mut reads,
                ref mut writes,
                ref mut write_closed,
            } => {
                for _ in 0..64 {
                    let Some(work) = reads.pop_front() else {
                        break;
                    };
                    if let Some(work) = read(&node.fd, work, self.shared.config.max_chunk_bytes) {
                        reads.push_front(work);
                        break;
                    }
                }
                for _ in 0..64 {
                    let Some(work) = writes.pop_front() else {
                        break;
                    };
                    if let WriteWork::Shutdown(responder) = work {
                        if *write_closed {
                            responder.complete(Ok(()));
                        } else {
                            match net::shutdown(&node.fd, net::Shutdown::Write) {
                                Ok(()) => {
                                    *write_closed = true;
                                    responder.complete(Ok(()));
                                }
                                Err(error) => fail(
                                    responder,
                                    provider::backend(NetworkOperationKind::ShutdownWrite, error),
                                ),
                            }
                        }
                    } else if *write_closed {
                        work.fail(NetworkError::WriteClosed);
                    } else if let Some(work) = write(
                        &node.fd,
                        work,
                        self.shared.config.max_chunk_bytes,
                        #[cfg(test)]
                        &self.shared,
                    ) {
                        writes.push_front(work);
                        break;
                    }
                }
            }
        }
        self.store(node);
    }
}
fn socket(address: SocketAddr, buffer_bytes: usize) -> Result<OwnedFd, NetworkError> {
    let family = if address.is_ipv4() {
        AddressFamily::INET
    } else {
        AddressFamily::INET6
    };
    let fd = net::socket_with(
        family,
        SocketType::STREAM,
        SocketFlags::CLOEXEC | SocketFlags::NONBLOCK,
        None,
    )
    .map_err(|e| provider::backend(NetworkOperationKind::Connect, e))?;
    configure_socket(&fd, buffer_bytes)?;
    Ok(fd)
}
fn configure_socket(fd: &OwnedFd, bytes: usize) -> Result<(), NetworkError> {
    // Reuse permits an explicit local address after an orderly prior connection
    // entered TIME_WAIT; it does not enable SO_REUSEPORT or duplicate listeners.
    net::sockopt::set_socket_reuseaddr(fd, true)
        .map_err(|e| provider::backend(NetworkOperationKind::Connect, e))?;
    net::sockopt::set_socket_recv_buffer_size(fd, bytes)
        .map_err(|e| provider::backend(NetworkOperationKind::Connect, e))?;
    net::sockopt::set_socket_send_buffer_size(fd, bytes)
        .map_err(|e| provider::backend(NetworkOperationKind::Connect, e))?;
    net::sockopt::set_tcp_nodelay(fd, true)
        .map_err(|e| provider::backend(NetworkOperationKind::Connect, e))
}
fn push_write(queue: &mut VecDeque<WriteWork>, work: WriteWork, limit: usize) {
    if queue.try_reserve(1).is_err() {
        work.fail(provider::exhausted("write queue allocation", limit));
    } else {
        queue.push_back(work);
    }
}
fn read(fd: &OwnedFd, mut work: ReadWork, max_chunk: usize) -> Option<ReadWork> {
    let count = work.request.max_bytes.min(max_chunk);
    let prefix = work.request.buffer.len();
    if count == 0 {
        work.responder.complete(Ok(ReadResult {
            buffer: work.request.buffer,
            bytes_read: 0,
            end_of_stream: false,
        }));
        return None;
    }
    if work.request.buffer.try_reserve_exact(count).is_err() {
        fail_buffer(
            work.responder,
            provider::exhausted("read buffer allocation", prefix + count),
            work.request.buffer,
        );
        return None;
    }
    work.request.buffer.resize(prefix + count, 0);
    match net::recv(
        fd,
        &mut work.request.buffer[prefix..],
        net::RecvFlags::DONTWAIT,
    ) {
        Ok((bytes, _)) => {
            work.request.buffer.truncate(prefix + bytes);
            work.responder.complete(Ok(ReadResult {
                buffer: work.request.buffer,
                bytes_read: bytes,
                end_of_stream: bytes == 0,
            }));
            None
        }
        Err(Errno::AGAIN | Errno::INTR) => {
            work.request.buffer.truncate(prefix);
            Some(work)
        }
        Err(error) => {
            work.request.buffer.truncate(prefix);
            fail_buffer(
                work.responder,
                provider::backend(NetworkOperationKind::Read, error),
                work.request.buffer,
            );
            None
        }
    }
}
fn write(
    fd: &OwnedFd,
    work: WriteWork,
    max_chunk: usize,
    #[cfg(test)] shared: &Shared,
) -> Option<WriteWork> {
    match work {
        WriteWork::Bytes(request, responder) => {
            if request.buffer.is_empty() {
                responder.complete(Ok(WriteResult {
                    buffer: request.buffer,
                    bytes_written: 0,
                }));
                return None;
            }
            match net::send(
                fd,
                &request.buffer[..request.buffer.len().min(max_chunk)],
                net::SendFlags::DONTWAIT | net::SendFlags::NOSIGNAL,
            ) {
                Ok(bytes_written) => {
                    responder.complete(Ok(WriteResult {
                        buffer: request.buffer,
                        bytes_written,
                    }));
                    None
                }
                Err(Errno::AGAIN | Errno::INTR) => Some(WriteWork::Bytes(request, responder)),
                Err(error) => {
                    fail_buffer(
                        responder,
                        provider::backend(NetworkOperationKind::Write, error),
                        request.buffer,
                    );
                    None
                }
            }
        }
        WriteWork::Vectored(request, responder) => {
            // IoSlice descriptors borrow retained immutable owners only for the
            // synchronous nonblocking sendmsg syscall. Payloads are not flattened.
            let result = (|| {
                let mut slices = Vec::new();
                slices
                    .try_reserve_exact(request.segments.len())
                    .map_err(|_| provider::exhausted("iovec allocation", request.segments.len()))?;
                let mut remaining = max_chunk;
                for segment in &request.segments {
                    if remaining == 0 {
                        break;
                    }
                    let start = segment.range.start as usize;
                    let count = (segment.range.end as usize - start).min(remaining);
                    slices.push(IoSlice::new(
                        &segment.bytes.as_slice()[start..start + count],
                    ));
                    remaining -= count;
                }
                Ok(net::sendmsg(
                    fd,
                    &slices,
                    &mut net::SendAncillaryBuffer::default(),
                    net::SendFlags::DONTWAIT | net::SendFlags::NOSIGNAL,
                ))
            })();
            match result {
                Ok(Ok(bytes_written)) => {
                    responder.complete(Ok(VectoredWriteResult {
                        segments: request.segments,
                        bytes_written,
                    }));
                    None
                }
                Ok(Err(_error @ (Errno::AGAIN | Errno::INTR))) => {
                    #[cfg(test)]
                    if _error == Errno::AGAIN {
                        shared.vectored_eagain_for_test();
                    }
                    Some(WriteWork::Vectored(request, responder))
                }
                other => {
                    let error = match other {
                        Err(error) => error,
                        Ok(Err(error)) => provider::backend(NetworkOperationKind::Write, error),
                        _ => unreachable!("success handled above"),
                    };
                    responder.complete(Err(CompletionError::not_applied(
                        VectoredWriteFailure::new(error, request.segments, 0),
                    )));
                    None
                }
            }
        }
        WriteWork::Shutdown(_) => unreachable!("shutdown handled before write syscall"),
    }
}
fn fail<T>(responder: Response<T>, error: NetworkError) {
    responder.complete(Err(CompletionError::not_applied(
        NetworkFailure::without_buffer(error),
    )));
}
fn fail_buffer<T>(responder: Response<T>, error: NetworkError, buffer: Vec<u8>) {
    responder.complete(Err(CompletionError::not_applied(
        NetworkFailure::with_buffer(error, buffer, 0),
    )));
}

fn wait_timeout(
    now: Instant,
    deadline: Option<Instant>,
    immediate: bool,
) -> Option<rustix::event::Timespec> {
    let duration = if immediate {
        Duration::ZERO
    } else {
        deadline?.saturating_duration_since(now)
    };
    Some(rustix::event::Timespec {
        tv_sec: duration.as_secs().min(i64::MAX as u64) as i64,
        tv_nsec: i64::from(duration.subsec_nanos()),
    })
}

#[cfg(test)]
mod timeout_tests {
    use super::*;
    use crate::{ReadinessConfig, ReadinessNet};
    use kr_runtime::CompletionCertainty;
    use kr_runtime_io::completion::SyncOperation;
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::AtomicUsize,
        task::{Context, Poll, Wake, Waker},
    };
    #[test]
    fn nearest_connect_deadline_bounds_epoll_without_sleeping_in_tests() {
        let now = Instant::now();
        assert!(wait_timeout(now, None, false).is_none());
        let deadline = now.checked_add(Duration::new(2, 17)).unwrap();
        let timeout = wait_timeout(now, Some(deadline), false).unwrap();
        assert_eq!((timeout.tv_sec, timeout.tv_nsec), (2, 17));
        let timeout = wait_timeout(deadline, Some(now), false).unwrap();
        assert_eq!((timeout.tv_sec, timeout.tv_nsec), (0, 0));
        let timeout = wait_timeout(now, Some(deadline), true).unwrap();
        assert_eq!((timeout.tv_sec, timeout.tv_nsec), (0, 0));
    }

    #[test]
    fn injected_deadline_retires_socket_credit_before_waking_connect_response() {
        struct Observe {
            network: ReadinessNet,
            streams_at_wake: Arc<AtomicUsize>,
        }
        impl Wake for Observe {
            fn wake(self: Arc<Self>) {
                self.streams_at_wake
                    .store(self.network.status().streams, Ordering::SeqCst);
            }
        }
        let network = ReadinessNet::new(ReadinessConfig::default()).unwrap();
        let mut reactor = Reactor {
            epoll: epoll::create(epoll::CreateFlags::CLOEXEC).unwrap(),
            shared: network.owner.shared.clone(),
            nodes: BTreeMap::new(),
        };
        let resource = network.owner.resource(ResourceKind::Stream).unwrap();
        let id = resource.id;
        let (mut response, responder) = SyncOperation::channel();
        let now = Instant::now();
        let deadline = now.checked_add(Duration::from_secs(1)).unwrap();
        reactor.nodes.insert(
            id,
            Node {
                fd: socket("127.0.0.1:0".parse().unwrap(), 4096).unwrap(),
                resource,
                flags: Flags::new(),
                registered: false,
                pending: Pending::Connecting {
                    deadline,
                    owner: network.owner.clone(),
                    responder,
                },
            },
        );
        let streams = Arc::new(AtomicUsize::new(usize::MAX));
        let waker = Waker::from(Arc::new(Observe {
            network: network.clone(),
            streams_at_wake: streams.clone(),
        }));
        assert!(
            Pin::new(&mut response)
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let mut expired = Vec::with_capacity(network.owner.shared.config.max_streams);
        reactor.expire_connects(now, &mut expired);
        assert_eq!(network.status().streams, 1);
        assert_eq!(reactor.nodes.len(), 1);
        reactor.expire_connects(deadline, &mut expired);
        assert_eq!(network.status().streams, 0);
        assert!(reactor.nodes.is_empty());
        assert_eq!(streams.load(Ordering::SeqCst), 0);
        let Poll::Ready(Err(failure)) =
            Pin::new(&mut response).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("expired connect must be terminal")
        };
        assert_eq!(failure.certainty(), CompletionCertainty::MayHaveApplied);
        assert_eq!(
            failure.error().error(),
            &provider::backend(NetworkOperationKind::Connect, Errno::TIMEDOUT)
        );
    }

    #[test]
    fn queued_connect_expiry_is_not_applied_and_invalid_timeouts_reject_at_startup() {
        for timeout in [Duration::ZERO, Duration::from_secs(86_401)] {
            assert!(
                ReadinessConfig {
                    connect_timeout: timeout,
                    ..ReadinessConfig::default()
                }
                .validate()
                .is_err()
            );
        }
        let network = ReadinessNet::new(ReadinessConfig::default()).unwrap();
        let mut reactor = Reactor {
            epoll: epoll::create(epoll::CreateFlags::CLOEXEC).unwrap(),
            shared: network.owner.shared.clone(),
            nodes: BTreeMap::new(),
        };
        let resource = network.owner.resource(ResourceKind::Stream).unwrap();
        let (mut response, responder) = SyncOperation::channel();
        reactor.command(Command::Connect {
            request: ConnectRequest {
                local: "127.0.0.1:0".parse().unwrap(),
                remote: "127.0.0.1:9".parse().unwrap(),
            },
            deadline: Instant::now(),
            resource,
            owner: network.owner.clone(),
            responder,
        });
        let Poll::Ready(Err(failure)) =
            Pin::new(&mut response).poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("expired queued connect must complete")
        };
        assert_eq!(failure.certainty(), CompletionCertainty::NotApplied);
        assert_eq!(network.status().streams, 0);
        assert!(reactor.nodes.is_empty());
    }
}
