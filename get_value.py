import argparse
import grpc
import twophase_pb2
import twophase_pb2_grpc
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='get the currently committed value from a worker'
    )
    parser.add_argument('worker_address')
    args = parser.parse_args()
    coordinator_stub = twophase_pb2_grpc.WorkerStub(
        grpc.insecure_channel(args.worker_address)
    )
    result = coordinator_stub.GetCommitted(twophase_pb2.Empty())
    if result.available:
        print("value is AVAILABLE and", result.content)
    else:
        print("value is UNAVAILABLE")
