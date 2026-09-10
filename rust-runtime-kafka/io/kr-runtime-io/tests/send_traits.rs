use std::cell::Cell;
use std::cmp::Ordering as CmpOrdering;
use std::future::{Ready, ready};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::time::Instant;

use kr_runtime::CompletionResult;
use kr_runtime_io::datagram::{
    DatagramBindRequest, DatagramFailure, DatagramProviderSubmit, DatagramSocketSubmit,
    DatagramTruncation, RecvFromRequest, RecvFromResult, SendDatagramProviderSubmit,
    SendDatagramSocketSubmit, SendToRequest, SendToResult,
};
use kr_runtime_io::network::{
    ByteStreamSubmit, ConnectRequest, ListenRequest, NetworkFailure, NetworkListenerSubmit,
    NetworkProviderSubmit, ReadRequest, ReadResult, SendByteStreamSubmit,
    SendNetworkListenerSubmit, SendNetworkProviderSubmit, WriteRequest, WriteResult,
};
use kr_runtime_io::storage::{
    FileIoSubmit, FileLength, ReadAtFailure, ReadAtRequest, ReadAtSuccess, SendFileIoSubmit,
    SetLenSuccess, StorageError, SyncSuccess, WriteAtFailure, WriteAtRequest, WriteAtSuccess,
};

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}

fn assert_send_file_contract<T: SendFileIoSubmit>() {
    assert_send::<T>();
    assert_sync::<T>();
    assert_send::<<T as FileIoSubmit>::ReadAtResponse>();
    assert_send::<<T as FileIoSubmit>::WriteAtResponse>();
    assert_send::<<T as FileIoSubmit>::SetLenResponse>();
    assert_send::<<T as FileIoSubmit>::LenResponse>();
    assert_send::<<T as FileIoSubmit>::SyncResponse>();
}

fn assert_send_stream_contract<T: SendByteStreamSubmit>() {
    assert_send::<T>();
    assert_sync::<T>();
    assert_send::<<T as ByteStreamSubmit>::ReadResponse>();
    assert_send::<<T as ByteStreamSubmit>::WriteResponse>();
    assert_send::<<T as ByteStreamSubmit>::ControlResponse>();
}

fn assert_send_listener_contract<T: SendNetworkListenerSubmit>() {
    assert_send::<T>();
    assert_sync::<T>();
    assert_send::<<T as NetworkListenerSubmit>::Address>();
    assert_send_stream_contract::<<T as NetworkListenerSubmit>::Stream>();
    assert_send::<<T as NetworkListenerSubmit>::AcceptResponse>();
    assert_send::<<T as NetworkListenerSubmit>::CloseResponse>();
}

fn assert_send_network_contract<T: SendNetworkProviderSubmit>() {
    assert_send::<T>();
    assert_sync::<T>();
    assert_send::<<T as NetworkProviderSubmit>::Address>();
    assert_send_stream_contract::<<T as NetworkProviderSubmit>::Stream>();
    assert_send_listener_contract::<<T as NetworkProviderSubmit>::Listener>();
    assert_send::<<T as NetworkProviderSubmit>::ListenResponse>();
    assert_send::<<T as NetworkProviderSubmit>::ConnectResponse>();
}

fn assert_send_datagram_socket_contract<T: SendDatagramSocketSubmit>() {
    assert_send::<T>();
    assert_sync::<T>();
    assert_send::<<T as DatagramSocketSubmit>::Address>();
    assert_send::<<T as DatagramSocketSubmit>::Instant>();
    assert_send::<<T as DatagramSocketSubmit>::SendResponse>();
    assert_send::<<T as DatagramSocketSubmit>::RecvResponse>();
    assert_send::<<T as DatagramSocketSubmit>::ControlResponse>();
}

fn assert_send_datagram_contract<T: SendDatagramProviderSubmit>() {
    assert_send::<T>();
    assert_sync::<T>();
    assert_send::<<T as DatagramProviderSubmit>::Address>();
    assert_send::<<T as DatagramProviderSubmit>::Instant>();
    assert_send_datagram_socket_contract::<<T as DatagramProviderSubmit>::Socket>();
    assert_send::<<T as DatagramProviderSubmit>::BindResponse>();
}

#[derive(Clone, Copy)]
struct ThreadSafeFile;

impl FileIoSubmit for ThreadSafeFile {
    type ReadAtResponse = Ready<CompletionResult<ReadAtSuccess, ReadAtFailure>>;
    type WriteAtResponse = Ready<CompletionResult<WriteAtSuccess, WriteAtFailure>>;
    type SetLenResponse = Ready<CompletionResult<SetLenSuccess, StorageError>>;
    type LenResponse = Ready<CompletionResult<FileLength, StorageError>>;
    type SyncResponse = Ready<CompletionResult<SyncSuccess, StorageError>>;

    fn submit_read_at(&self, request: ReadAtRequest) -> Self::ReadAtResponse {
        ready(Ok(ReadAtSuccess {
            buffer: request.buffer,
            bytes_read: 0,
        }))
    }

    fn submit_write_at(&self, request: WriteAtRequest) -> Self::WriteAtResponse {
        ready(Ok(WriteAtSuccess {
            bytes_written: request.buffer.len(),
            buffer: request.buffer,
        }))
    }

    fn submit_set_len(&self, len: u64) -> Self::SetLenResponse {
        ready(Ok(SetLenSuccess { len }))
    }

    fn submit_len(&self) -> Self::LenResponse {
        ready(Ok(FileLength { len: 0 }))
    }

    fn submit_sync(&self) -> Self::SyncResponse {
        ready(Ok(SyncSuccess { durable_len: 0 }))
    }
}

#[derive(Clone, Copy)]
struct ThreadSafeStream;

impl ByteStreamSubmit for ThreadSafeStream {
    type ReadResponse = Ready<CompletionResult<ReadResult, NetworkFailure>>;
    type WriteResponse = Ready<CompletionResult<WriteResult, NetworkFailure>>;
    type ControlResponse = Ready<CompletionResult<(), NetworkFailure>>;

    fn submit_read(&self, request: ReadRequest) -> Self::ReadResponse {
        ready(Ok(ReadResult {
            buffer: request.buffer,
            bytes_read: 0,
            end_of_stream: false,
        }))
    }

    fn submit_write(&self, request: WriteRequest) -> Self::WriteResponse {
        ready(Ok(WriteResult {
            bytes_written: request.buffer.len(),
            buffer: request.buffer,
        }))
    }

    fn submit_shutdown_write(&self) -> Self::ControlResponse {
        ready(Ok(()))
    }

    fn submit_close(&self) -> Self::ControlResponse {
        ready(Ok(()))
    }
}

#[derive(Clone, Copy)]
struct ThreadSafeListener(SocketAddr);

impl NetworkListenerSubmit for ThreadSafeListener {
    type Address = SocketAddr;
    type Stream = ThreadSafeStream;
    type AcceptResponse = Ready<CompletionResult<Self::Stream, NetworkFailure>>;
    type CloseResponse = Ready<CompletionResult<(), NetworkFailure>>;

    fn local_address(&self) -> Self::Address {
        self.0
    }

    fn submit_accept(&self) -> Self::AcceptResponse {
        ready(Ok(ThreadSafeStream))
    }

    fn submit_close(&self) -> Self::CloseResponse {
        ready(Ok(()))
    }
}

#[derive(Clone, Copy)]
struct ThreadSafeNetwork;

impl NetworkProviderSubmit for ThreadSafeNetwork {
    type Address = SocketAddr;
    type Stream = ThreadSafeStream;
    type Listener = ThreadSafeListener;
    type ListenResponse = Ready<CompletionResult<Self::Listener, NetworkFailure>>;
    type ConnectResponse = Ready<CompletionResult<Self::Stream, NetworkFailure>>;

    fn submit_listen(&self, request: ListenRequest<Self::Address>) -> Self::ListenResponse {
        ready(Ok(ThreadSafeListener(request.address)))
    }

    fn submit_connect(&self, _request: ConnectRequest<Self::Address>) -> Self::ConnectResponse {
        ready(Ok(ThreadSafeStream))
    }
}

#[derive(Clone, Copy)]
struct ThreadSafeDatagramSocket(SocketAddr);

impl DatagramSocketSubmit for ThreadSafeDatagramSocket {
    type Address = SocketAddr;
    type Instant = Instant;
    type SendResponse = Ready<CompletionResult<SendToResult, DatagramFailure>>;
    type RecvResponse = Ready<CompletionResult<RecvFromResult<Self::Address>, DatagramFailure>>;
    type ControlResponse = Ready<CompletionResult<(), DatagramFailure>>;

    fn local_addr(&self) -> Self::Address {
        self.0
    }

    fn submit_send_to(&self, request: SendToRequest<Self::Address>) -> Self::SendResponse {
        let bytes_sent = request.buffer.len();
        ready(Ok(SendToResult {
            buffer: request.buffer,
            bytes_sent,
        }))
    }

    fn submit_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        self.empty_receive(request)
    }

    fn submit_try_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        self.empty_receive(request)
    }

    fn submit_recv_from_until(
        &self,
        request: RecvFromRequest,
        _deadline: Self::Instant,
    ) -> Self::RecvResponse {
        self.empty_receive(request)
    }

    fn submit_close(&self) -> Self::ControlResponse {
        ready(Ok(()))
    }
}

impl ThreadSafeDatagramSocket {
    fn empty_receive(
        self,
        request: RecvFromRequest,
    ) -> <Self as DatagramSocketSubmit>::RecvResponse {
        ready(Ok(RecvFromResult {
            buffer: request.buffer,
            bytes_received: 0,
            datagram_len: 0,
            source: self.0,
            truncation: DatagramTruncation::Complete,
        }))
    }
}

#[derive(Clone, Copy)]
struct ThreadSafeDatagramProvider;

impl DatagramProviderSubmit for ThreadSafeDatagramProvider {
    type Address = SocketAddr;
    type Instant = Instant;
    type Socket = ThreadSafeDatagramSocket;
    type BindResponse = Ready<CompletionResult<Self::Socket, DatagramFailure>>;

    fn submit_bind(&self, request: DatagramBindRequest<Self::Address>) -> Self::BindResponse {
        ready(Ok(ThreadSafeDatagramSocket(request.address)))
    }
}

/// Deliberately `Send + !Sync`: operation values move into owned futures but
/// are never shared by reference through the provider contract.
#[derive(Clone)]
struct SendOnlyAddress(Cell<u16>);

#[derive(Clone)]
struct SendOnlyInstant(Cell<u64>);

impl PartialEq for SendOnlyInstant {
    fn eq(&self, other: &Self) -> bool {
        self.0.get() == other.0.get()
    }
}

impl Eq for SendOnlyInstant {}

impl PartialOrd for SendOnlyInstant {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for SendOnlyInstant {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.0.get().cmp(&other.0.get())
    }
}

#[derive(Clone, Copy)]
struct SendOnlyListener(u16);

impl NetworkListenerSubmit for SendOnlyListener {
    type Address = SendOnlyAddress;
    type Stream = ThreadSafeStream;
    type AcceptResponse = Ready<CompletionResult<Self::Stream, NetworkFailure>>;
    type CloseResponse = Ready<CompletionResult<(), NetworkFailure>>;

    fn local_address(&self) -> Self::Address {
        SendOnlyAddress(Cell::new(self.0))
    }

    fn submit_accept(&self) -> Self::AcceptResponse {
        ready(Ok(ThreadSafeStream))
    }

    fn submit_close(&self) -> Self::CloseResponse {
        ready(Ok(()))
    }
}

#[derive(Clone, Copy)]
struct SendOnlyNetwork;

impl NetworkProviderSubmit for SendOnlyNetwork {
    type Address = SendOnlyAddress;
    type Stream = ThreadSafeStream;
    type Listener = SendOnlyListener;
    type ListenResponse = Ready<CompletionResult<Self::Listener, NetworkFailure>>;
    type ConnectResponse = Ready<CompletionResult<Self::Stream, NetworkFailure>>;

    fn submit_listen(&self, request: ListenRequest<Self::Address>) -> Self::ListenResponse {
        ready(Ok(SendOnlyListener(request.address.0.get())))
    }

    fn submit_connect(&self, _request: ConnectRequest<Self::Address>) -> Self::ConnectResponse {
        ready(Ok(ThreadSafeStream))
    }
}

#[derive(Clone, Copy)]
struct SendOnlyDatagramSocket(u16);

impl DatagramSocketSubmit for SendOnlyDatagramSocket {
    type Address = SendOnlyAddress;
    type Instant = SendOnlyInstant;
    type SendResponse = Ready<CompletionResult<SendToResult, DatagramFailure>>;
    type RecvResponse = Ready<CompletionResult<RecvFromResult<Self::Address>, DatagramFailure>>;
    type ControlResponse = Ready<CompletionResult<(), DatagramFailure>>;

    fn local_addr(&self) -> Self::Address {
        SendOnlyAddress(Cell::new(self.0))
    }

    fn submit_send_to(&self, request: SendToRequest<Self::Address>) -> Self::SendResponse {
        let bytes_sent = request.buffer.len();
        ready(Ok(SendToResult {
            buffer: request.buffer,
            bytes_sent,
        }))
    }

    fn submit_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        self.empty_receive(request)
    }

    fn submit_try_recv_from(&self, request: RecvFromRequest) -> Self::RecvResponse {
        self.empty_receive(request)
    }

    fn submit_recv_from_until(
        &self,
        request: RecvFromRequest,
        _deadline: Self::Instant,
    ) -> Self::RecvResponse {
        self.empty_receive(request)
    }

    fn submit_close(&self) -> Self::ControlResponse {
        ready(Ok(()))
    }
}

impl SendOnlyDatagramSocket {
    fn empty_receive(
        self,
        request: RecvFromRequest,
    ) -> <Self as DatagramSocketSubmit>::RecvResponse {
        ready(Ok(RecvFromResult {
            buffer: request.buffer,
            bytes_received: 0,
            datagram_len: 0,
            source: SendOnlyAddress(Cell::new(self.0)),
            truncation: DatagramTruncation::Complete,
        }))
    }
}

#[derive(Clone, Copy)]
struct SendOnlyDatagramProvider;

impl DatagramProviderSubmit for SendOnlyDatagramProvider {
    type Address = SendOnlyAddress;
    type Instant = SendOnlyInstant;
    type Socket = SendOnlyDatagramSocket;
    type BindResponse = Ready<CompletionResult<Self::Socket, DatagramFailure>>;

    fn submit_bind(&self, request: DatagramBindRequest<Self::Address>) -> Self::BindResponse {
        ready(Ok(SendOnlyDatagramSocket(request.address.0.get())))
    }
}

#[test]
fn send_companion_bounds_propagate_through_generic_code() {
    assert_send_file_contract::<ThreadSafeFile>();
    assert_send_stream_contract::<ThreadSafeStream>();
    assert_send_listener_contract::<ThreadSafeListener>();
    assert_send_network_contract::<ThreadSafeNetwork>();
    assert_send_datagram_socket_contract::<ThreadSafeDatagramSocket>();
    assert_send_datagram_contract::<ThreadSafeDatagramProvider>();
    assert_send_network_contract::<SendOnlyNetwork>();
    assert_send_datagram_contract::<SendOnlyDatagramProvider>();

    let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));
    assert_eq!(ThreadSafeListener(address).local_address(), address);
    assert_eq!(ThreadSafeDatagramSocket(address).local_addr(), address);
}
