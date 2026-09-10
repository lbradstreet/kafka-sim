use std::future::Future;

use kr_runtime::{RunOutcome, SimDuration, SimRuntime};
use kr_runtime_io::{SimDisk, SimLatencyModel, SimPipelineModel, SimStorage, SimStorageConfig};
use kr_runtime_ring::file::FileRing;
use kr_runtime_ring::{
    AppendRequest, AppendSuccess, ReadPage, ReadRequest, RingCursor, RingReader, RingWriter,
    SyncSuccess, TrimSuccess,
};

use super::{BenchRing, file_ring_config};

pub(super) struct SimBenchRing {
    ring: Option<FileRing<SimStorage>>,
    runtime: SimRuntime,
}

impl SimBenchRing {
    pub(super) fn new() -> Self {
        let config = file_ring_config();
        let max_file_bytes = usize::try_from(
            config
                .physical_file_bytes()
                .expect("benchmark file-ring geometry is valid"),
        )
        .expect("benchmark file-ring length fits usize");
        let io_request_bytes = config.max_io_request_bytes;
        let storage_config = SimStorageConfig {
            max_file_bytes,
            max_read_bytes: io_request_bytes,
            max_write_bytes: io_request_bytes,
            max_read_chunk: io_request_bytes,
            max_write_chunk: io_request_bytes,
            max_in_flight: config.command_queue_capacity,
            // The queue's worth of maximum-size requests: a benchmark measures
            // overhead, so no limit under test may bind unexpectedly.
            max_outstanding_bytes: config.command_queue_capacity * io_request_bytes,
            max_scripted_faults: 1,
            default_latency: SimDuration::ZERO,
            // A benchmark measures overhead, not exploration: keep completion
            // latency and completion order exactly reproducible.
            latency_model: SimLatencyModel::Fixed,
            pipeline_model: SimPipelineModel::Serial,
        };

        let mut runtime = SimRuntime::default();
        let handle = runtime.handle();
        let storage = SimDisk::default()
            .open(handle.clone(), storage_config)
            .expect("open simulated benchmark file");
        let ring = runtime
            .block_on(FileRing::create(handle, storage, config))
            .expect("simulation runtime completes file-ring creation")
            .expect("create simulated benchmark ring");

        Self {
            ring: Some(ring),
            runtime,
        }
    }

    fn ring(&self) -> &FileRing<SimStorage> {
        self.ring
            .as_ref()
            .expect("simulated benchmark ring is open")
    }

    fn block_on<T>(&mut self, future: impl Future<Output = T> + 'static) -> T
    where
        T: 'static,
    {
        self.runtime
            .block_on(future)
            .expect("simulation runtime completes benchmark operation")
    }
}

impl BenchRing for SimBenchRing {
    fn append(&mut self, records: Vec<Vec<u8>>) -> AppendSuccess {
        let future = self.ring().append(AppendRequest::new(records));
        self.block_on(future)
            .expect("append to simulated benchmark ring")
    }

    fn sync(&mut self) -> SyncSuccess {
        let future = self.ring().sync();
        self.block_on(future)
            .expect("sync simulated benchmark ring")
    }

    fn trim(&mut self, before: RingCursor) -> TrimSuccess {
        let future = self.ring().trim(before);
        self.block_on(future)
            .expect("trim simulated benchmark ring")
    }

    fn read(&mut self, request: ReadRequest) -> ReadPage {
        let future = self.ring().read(request);
        self.block_on(future)
            .expect("read simulated benchmark ring")
    }
}

impl Drop for SimBenchRing {
    fn drop(&mut self) {
        // The last handle closes admission. Drive both the file-ring actor and
        // the simulated storage worker until their normal termination before
        // stopping the runtime itself.
        drop(self.ring.take());
        let drain = self.runtime.run_until_stalled();
        let shutdown = self.runtime.shutdown();

        if std::thread::panicking() {
            return;
        }
        match drain {
            Ok(RunOutcome::Idle(_)) => {}
            Ok(RunOutcome::Stalled(_)) => panic!("simulated benchmark actors stalled at teardown"),
            Ok(RunOutcome::Stopped(_)) => {
                panic!("simulated benchmark runtime stopped before teardown")
            }
            Err(error) => panic!("could not drain simulated benchmark actors: {error}"),
        }
        shutdown.expect("shut down simulated benchmark runtime");
    }
}
