import argparse
import grpc
import twophase_pb2
import twophase_pb2_grpc
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Send the SetValue command to a coordinator'
    )
    parser.add_argument('coordinator_address')
    parser.add_argument('value')
    args = parser.parse_args()
    coordinator_stub = twophase_pb2_grpc.CoordinatorStub(
        grpc.insecure_channel(args.coordinator_address)
    )
    coordinator_stub.SetValue(
        twophase_pb2.MaybeValue(
            available=True,
            content=args.value
        )
    )
