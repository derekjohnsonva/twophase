from concurrent import futures
import logging
import grpc
import twophase_pb2
import twophase_pb2_grpc

logger = logging.getLogger(__name__)
class MyWorker(twophase_pb2_grpc.WorkerServicer):
    def __init__(self, log):
        self._log = log
        self._value = None
        if self._log.get_last_entry() != None:
            self._value = self._log.get_last_entry()['value']

    def SetValue(self, request, context):
        self._log.set_last_entry({
            'value': request.content
        })
        self._value = request.content
        return twophase_pb2.Empty()

    def GetCommitted(self, request, context):
        return twophase_pb2.MaybeValue(
            available=True,
            content=self._value,
        )

def create_worker(worker_log, **extra_args):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1), **extra_args)
    twophase_pb2_grpc.add_WorkerServicer_to_server(MyWorker(worker_log), server)
    return server

if __name__ == '__main__':
    import argparse
    import os
    import persistent_log
    import time
    parser = argparse.ArgumentParser(
        description='Run a worker'
    )
    parser.add_argument('server_address')
    parser.add_argument('log_file')
    args = parser.parse_args()
    log = persistent_log.FilePersistentLog(args.log_file)
    server = create_worker(log)
    server.add_insecure_port(args.server_address)
    server.start()
    while True:
        time.sleep(3600)
