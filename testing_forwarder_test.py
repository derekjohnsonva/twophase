from concurrent import futures
import itertools
import logging
import os.path
import random
import tempfile
import unittest

import grpc
import worker
import coordinator
import persistent_log

from testing_forwarder_test_pb2 import TestMessage
import testing_forwarder_test_pb2_grpc
from testing_forwarder_test_pb2_grpc import TestServiceServicer, TestServiceStub

from testing_forwarder import TestingForwarder
import testing_forwarder

class TestingService(TestServiceServicer):
    def __init__(self):
        self._seen = []
        self._to_return = TestMessage(value='dummy return value')
        self._force_failure_for = None

    @property
    def seen(self):
        return self._seen

    @property
    def to_return(self):
        return self._to_return

    @to_return.setter
    def to_return(self, value):
        self._to_return = value

    @property
    def force_failure_for(self):
        return self._force_failure

    @force_failure_for.setter
    def force_failure_for(self, value):
        self._force_failure_for = value

    def MethodA(self, request, context):
        self._seen.append(
            ('MethodA', request.value)
        )
        if self._force_failure_for == 'MethodA':
            context.abort(grpc.StatusCode.INTERNAL, 'test service failing')
        return self._to_return
        
    def MethodB(self, request, context):
        self._seen.append(
            ('MethodB', request.value)
        )
        if self._force_failure_for == 'MethodB':
            context.abort(grpc.StatusCode.INTERNAL, 'test service failing')
        return self._to_return

class TestingForwarderTest(unittest.TestCase):
    def _real_socket_file(self):
        return 'unix://' + os.path.join(self.tempdir.name, 'service')

    def _forwarder_socket_file(self):
        return 'unix://' + os.path.join(self.tempdir.name, 'forwarder')

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(prefix='2ppy')
        self.service = TestingService()
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
        testing_forwarder_test_pb2_grpc.add_TestServiceServicer_to_server(self.service, self.server)
        self.server.add_insecure_port(self._real_socket_file())
        self.server.start()
        real_channel = grpc.insecure_channel(self._real_socket_file())
        self.forwarder = TestingForwarder(real_channel)
        self.forwarder_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), [self.forwarder])
        self.forwarder_server.add_insecure_port(self._forwarder_socket_file())
        self.forwarder_server.start()
        self.stub = TestServiceStub(grpc.insecure_channel(self._forwarder_socket_file()))

    def tearDown(self):
        self.forwarder_server.stop(0)
        self.server.stop(0)
        self.tempdir.cleanup()
    
    def test_trivial_forward(self):
        self.assertEqual(
            self.stub.MethodA(TestMessage(value='testA')),
            TestMessage(value='dummy return value')
        )
        self.service.to_return = TestMessage(value='next dummy return value')
        self.assertEqual(
            self.stub.MethodB(TestMessage(value='testB')),
            TestMessage(value='next dummy return value')
        )
        self.assertEqual(
            self.service.seen,
            [('MethodA', 'testA'), ('MethodB', 'testB')]
        )

    def test_fail_immediately(self):
        self.forwarder.fail_after(0)
        with self.assertRaises(grpc.RpcError):
            self.stub.MethodA(TestMessage(value='testA'))
        self.assertEqual(
            self.service.seen,
            []
        )
        self.forwarder.stop_failing()
        self.service.to_return = TestMessage(value='next dummy return value')
        self.assertEqual(
            self.stub.MethodB(TestMessage(value='testB')),
            TestMessage(value='next dummy return value')
        )
        self.assertEqual(
            self.service.seen,
            [('MethodB', 'testB')]
        )

    def test_fail_immediately_but_send_once(self):
        self.forwarder.fail_after(0, but_send=True)
        with self.assertRaises(grpc.RpcError):
            self.stub.MethodA(TestMessage(value='testA1'))
        self.assertEqual(
            self.service.seen,
            [('MethodA', 'testA1')]
        )
        # second call should not be forwarded, as if communication problem happnened in between
        with self.assertRaises(grpc.RpcError):
            self.stub.MethodA(TestMessage(value='testA2'))
        self.assertEqual(
            self.service.seen,
            [('MethodA', 'testA1')]
        )
        self.forwarder.stop_failing()
        self.service.to_return = TestMessage(value='next dummy return value')
        self.assertEqual(
            self.stub.MethodB(TestMessage(value='testB')),
            TestMessage(value='next dummy return value')
        )
        self.assertEqual(
            self.service.seen,
            [('MethodA', 'testA1'), ('MethodB', 'testB')]
        )

    def test_delay_one_call(self):
        self.forwarder.delay_after(0)
        with self.assertRaises(grpc.RpcError):
            self.stub.MethodA(TestMessage(value='testA'))
        self.assertEqual(
            self.service.seen,
            []
        )
        self.forwarder.stop_delaying()
        self.service.to_return = TestMessage(value='return value B')
        self.assertEqual(
            self.stub.MethodB(TestMessage(value='testB')),
            TestMessage(value='return value B')
        )
        self.forwarder.replay_delayed()
        self.service.to_return = TestMessage(value='return value C')
        self.assertEqual(
            self.stub.MethodA(TestMessage(value='testC')),
            TestMessage(value='return value C')
        )
        self.assertEqual(
            self.service.seen,
            [('MethodB', 'testB'), ('MethodA', 'testA'), ('MethodA', 'testC')]
        )

    def test_delayed_call_does_not_propogate_failure(self):
        self.forwarder.delay_after(0)
        with self.assertRaises(grpc.RpcError):
            self.stub.MethodA(TestMessage(value='testA'))
        self.assertEqual(
            self.service.seen,
            []
        )
        self.forwarder.stop_delaying()
        self.service.force_failure_for = 'MethodA'
        self.service.to_return = TestMessage(value='return value B')
        self.assertEqual(
            self.stub.MethodB(TestMessage(value='testB')),
            TestMessage(value='return value B')
        )
        self.forwarder.replay_delayed()
        self.service.to_return = TestMessage(value='return value C')
        self.assertEqual(
            self.stub.MethodB(TestMessage(value='testC')),
            TestMessage(value='return value C')
        )
        self.assertEqual(
            self.service.seen,
            [('MethodB', 'testB'), ('MethodA', 'testA'), ('MethodB', 'testC')]
        )


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    testing_forwarder.logger.setLevel(logging.DEBUG)
    unittest.main(verbosity=2)
