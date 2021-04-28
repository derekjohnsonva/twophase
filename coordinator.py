from concurrent import futures
import grpc
import logging
import twophase_pb2
import twophase_pb2_grpc

logger = logging.getLogger(__name__)

class MyCoordinator(twophase_pb2_grpc.CoordinatorServicer):
    def __init__(self, log, worker_stubs):
        self._log = log
        self._worker_stubs = worker_stubs
        # TODO: add recovery code here when two-phase commit is implemented

    def SetValue(self, request, context):
        for worker_stub in self._worker_stubs:
            worker_stub.SetValue(request)
        return twophase_pb2.Empty()


def create_coordinator(coordinator_log, worker_stubs, **extra_args):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1, **extra_args))
    twophase_pb2_grpc.add_CoordinatorServicer_to_server(MyCoordinator(coordinator_log, worker_stubs), server)
    return server


if __name__ == '__main__':
    import argparse
    import os
    import persistent_log
    import time
    parser = argparse.ArgumentParser(
        description='Run the coordinator server.'
    )
    parser.add_argument('server_address')
    parser.add_argument('log_file')
    parser.add_argument('worker_addresses', nargs='+')
    args = parser.parse_args()
    log = persistent_log.FilePersistentLog(args.log_file)
    worker_stubs = [
        twophase_pb2_grpc.WorkerStub(grpc.insecure_channel(address)) \
        for address in args.worker_addresses
    ]
    server = create_coordinator(log, worker_stubs)
    server.add_insecure_port(args.server_address)
    server.start()
    while True:
        time.sleep(3600)
