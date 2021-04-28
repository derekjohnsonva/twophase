from concurrent import futures
from parameterized import parameterized
import itertools
import logging
import os.path
import random
import tempfile
import time
import unittest

import grpc
import worker
import coordinator
import persistent_log

import twophase_pb2
import twophase_pb2_grpc

import test_util_pb2
import test_util_pb2_grpc

from testing_forwarder import TestingForwarder

logger = logging.getLogger(__name__)

class TransitionChecker(object):
    def __init__(self, unittest, worker_stubs, prev_value, next_value):
        self._worker_stubs = worker_stubs
        self._next_value = next_value
        self._prev_value = prev_value
        self._on_next = False
        self._unittest = unittest
        self._failures = []

    def report_queued_failures(self):
        for failure in self._failures:
            self._unittest.fail(failure)

    def _fail(self, message):
        self._failures.append(message)
        self._unittest.fail(message)

    def __call__(self):
        for i, worker in enumerate(self._worker_stubs):
            try:
                result = worker.GetCommitted(twophase_pb2.Empty())
            except:
                import traceback
                self._fail(
                    'worker %s threw exception from GetCommitted:\n%s' % (i,
                        traceback.format_exc()))
            if result.available:
                if self._on_next:
                    if result.content != self._next_value:
                        self._fail(
                            ('worker %s has value %s (previous set value was %s) '
                            'even though some workers started returning new value %s') %
                            (i, result.content, self._prev_value, self._next_value))
                else:
                    if result.content == self._prev_value:
                        continue
                    elif result.content == self._next_value:
                        self._on_next = True
                    else:
                        self._fail('worker %s returned unexpected value %s' % (i, result.content))

class PingService(test_util_pb2_grpc.TestUtilPingServicer):
    def Ping(self, request, context):
      return request

class TestBase(unittest.TestCase):
    disable_recovery_consistency_check = False
    disable_recovery_consistency_queued = False
    disable_consistent_transition_check = False
    also_recover_workers = False

    def _start_and_wait_for(self, server, port):
        test_util_pb2_grpc.add_TestUtilPingServicer_to_server(PingService(), server)
        server.add_insecure_port(port)
        server.start()
        time.sleep(0.001)
        channel = grpc.insecure_channel(port)
        stub = test_util_pb2_grpc.TestUtilPingStub(channel)
        while True:
            try:
                stub.Ping(test_util_pb2.TestUtilEmpty())
                break
            except:
                pass
            time.sleep(0.001)

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(prefix='2ppy')
        self.worker_servers = []

    def _socket_file_for(self, worker_id):
        return 'unix://' + os.path.join(self.tempdir.name, 'worker-{}'.format(worker_id))
    
    def _forward_socket_file_for(self, worker_id):
        return 'unix://' + os.path.join(self.tempdir.name, 'worker-forward-{}'.format(worker_id))

    def _coordinator_socket_file(self):
        return 'unix://' + os.path.join(self.tempdir.name, 'coordinator')

    def tearDown(self):
        self._stop_workers()
        self._stop_coordinator()
        self._stop_worker_forwarders()
        self.tempdir.cleanup()
    
    def _stop_coordinator(self):
        self.coordinator_server.stop(0)
        self.coordinator_server._state.thread_pool.shutdown()
        self.coordinator_server.wait_for_termination()

    def _start_coordinator(self):
        self.coordinator_server = coordinator.create_coordinator(self.coordinator_log, self.worker_stubs)
        self._start_and_wait_for(self.coordinator_server, self._coordinator_socket_file())
        channel = grpc.insecure_channel(self._coordinator_socket_file())
        self.coordinator_stub = twophase_pb2_grpc.CoordinatorStub(channel)

    def _start_worker_forwarders(self):
        self.worker_forwarders = []
        self.worker_forwarder_servers = []
        self.worker_stubs = []
        for i in range(len(self.worker_logs)):
            socket_file = self._socket_file_for(i)
            raw_channel = grpc.insecure_channel(socket_file)
            raw_stub = twophase_pb2_grpc.WorkerStub(raw_channel)
            worker_forwarder = TestingForwarder(raw_channel)
            worker_forwarder_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), [worker_forwarder])
            forward_socket_file = self._forward_socket_file_for(i)
            self._start_and_wait_for(worker_forwarder_server, forward_socket_file)
            channel = grpc.insecure_channel(forward_socket_file)
            stub = twophase_pb2_grpc.WorkerStub(channel)
            self.worker_stubs.append(stub)
            self.worker_forwarders.append(worker_forwarder)
            self.worker_forwarder_servers.append(worker_forwarder_server)

    def _start_workers(self):
        self.worker_servers = [worker.create_worker(worker_log) for worker_log in self.worker_logs]
        self.raw_worker_stubs = []
        for i, worker_server in enumerate(self.worker_servers):
            socket_file = self._socket_file_for(i)
            self._start_and_wait_for(worker_server, socket_file)
            raw_channel = grpc.insecure_channel(socket_file)
            raw_stub = twophase_pb2_grpc.WorkerStub(raw_channel)
            self.raw_worker_stubs.append(raw_stub)

    def _stop_workers(self):
        for worker_server in self.worker_servers:
            worker_server.stop(0)
            worker_server.wait_for_termination()
            worker_server._state.thread_pool.shutdown()

    def _stop_worker_forwarders(self):
        for worker_forwarder_server in self.worker_forwarder_servers:
            worker_forwarder_server.stop(0)
            worker_forwarder_server.wait_for_termination()
            worker_forwarder_server._state.thread_pool.shutdown()
    
    def start_coordinator_and_workers(self, num_workers):
        logger.debug('test starting %s workers', num_workers)
        self.coordinator_log = persistent_log.TestingDummyPersistentLog()
        self.worker_logs = [persistent_log.TestingDummyPersistentLog() for i in range(num_workers)]
        self._start_workers()
        self._start_worker_forwarders()
        self._start_coordinator()

    def recover_coordinator(self, maybe_fail=False, expect_from=None, expect_to=None):
        if self.also_recover_workers:
            self.recover_workers()
        logger.debug('test restarting coordinator %s',
            '[allowed to fail]' if maybe_fail else '')
        self._stop_coordinator()
        if not self.disable_recovery_consistency_check and expect_from:
            checker = TransitionChecker(
                unittest=self,
                worker_stubs=self.raw_worker_stubs,
                prev_value=expect_from,
                next_value=expect_to,
            )
            for forwarder in self.worker_forwarders:
                forwarder.callback_before_call(checker)
                forwarder.callback_after_call(checker)
        if maybe_fail:
            try:
                self._start_coordinator()
            except:
                pass # ignore exception
        else:
            self._start_coordinator()
        if not self.disable_recovery_consistency_check and expect_from:
            if not self.disable_recovery_consistency_queued:
                checker.report_queued_failures()
            for forwarder in self.worker_forwarders:
                forwarder.callback_before_call(None)
                forwarder.callback_after_call(None)
    
    def recover_workers(self):
        logger.debug('test restarting workers')
        self._stop_workers()
        try:
            self._start_workers()
        except:
            import traceback
            logger.debug('exception (re)starting workers: %s', traceback.format_exc())

    def do_set_value(self, target_value, maybe_fail=False, expect_fail=False, expect_transition_from=False):
        if expect_transition_from and not self.disable_consistent_transition_check:
            checker = TransitionChecker(
                unittest=self,
                worker_stubs=self.raw_worker_stubs,
                prev_value=expect_transition_from,
                next_value=target_value,
            )
            for forwarder in self.worker_forwarders:
                forwarder.callback_before_call(checker)
                forwarder.callback_after_call(checker)
        logger.debug('test SetValue(%s) %s%s', target_value,
             '[allowed to fail]' if maybe_fail else '',
             '[expected to fail]' if expect_fail else '')
        to_set = twophase_pb2.MaybeValue(available=True,content=target_value)
        if expect_fail:
            with self.assertRaises(grpc.RpcError):
                self.coordinator_stub.SetValue(to_set)
        else:
            try:
                self.coordinator_stub.SetValue(to_set)
            except grpc.RpcError:
                if not maybe_fail:
                    self.fail('setting value to {} triggered an exception'.format(target_value))
        if expect_transition_from and not self.disable_consistent_transition_check:
            if not self.disable_recovery_consistency_queued:
                checker.report_queued_failures()
            for forwarder in self.worker_forwarders:
                forwarder.callback_before_call(None)
                forwarder.callback_after_call(None)

    def check_values(self, target_value, alternate_target_value=None, allow_unavailable=False):
        logger.debug('test checking value is %s%s%s',
            target_value,
            (' or ' + alternate_target_value) if alternate_target_value else '',
            ' or <unavailable>' if allow_unavailable else '')
        empty = twophase_pb2.Empty()
        first_value = None
        for i, worker_stub in enumerate(self.raw_worker_stubs):
            result = worker_stub.GetCommitted(empty)
            if not allow_unavailable:
                self.assertEqual(result.available, True)
            if not result.available:
                continue
            self.assertTrue(result.content != None)
            self.assertTrue(result.content == target_value or result.content == alternate_target_value,
                msg='expected stored value {} of worker {} to be either {} or {}'.format(result.content, i, target_value, alternate_target_value))
            if first_value == None and result.available:
                first_value = result.content
            elif result.available:
                self.assertEqual(result.content, first_value)

    def check_unavailable(self):
        logger.debug('test checking value is <unavailable>')
        empty = twophase_pb2.Empty()
        for i, worker_stub in enumerate(self.worker_stubs):
            result = worker_stub.GetCommitted(empty)
            self.assertEqual(result.available, False)
