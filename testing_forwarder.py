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

import twophase_pb2
import twophase_pb2_grpc

logger = logging.getLogger(__name__)

# via https://github.com/grpc/grpc/blob/master/examples/python/interceptors/headers/request_header_validator_interceptor.py
def _unary_unary_rpc_terminator(code, details):
    def terminate(ignored_request, context):
        context.abort(code, details)
    return grpc.unary_unary_rpc_method_handler(terminate)

class TestingForwarder(grpc.GenericRpcHandler):
    def __init__(self, target_channel):
        self._target_channel = target_channel
        self._fail = False
        self._fail_but_send = False
        self._fail_countdown = None
        self._delay = False
        self._delay_countdown = None
        self._replay_delay = False
        self._replay_delay_countdown = None
        self._delayed_calls = []
        self._call_before = None
        self._call_after = None

    def fail_after(self, count, but_send=False):
        self._fail_countdown = count
        self._fail_but_send = but_send

    def stop_failing(self):
        self._fail = False
        self._fail_countdown = None

    def delay_after(self, count):
        self._replay_delay = False
        self._delay_countdown = count

    def stop_delaying(self):
        self._delay = False
        self._delay_countdown = None

    def replay_delayed(self):
        self.stop_delaying()
        self._replay_delay = True

    def replay_delayed_after(self, when=0):
        self.stop_delaying()
        self._replay_delay_countdown = when

    def stop_replaying_delayed(self):
        self._replay_delay = False

    def callback_before_call(self, function):
        self._call_before = function

    def callback_after_call(self, function):
        self._call_after = function

    def _do_fail_countdown(self):
        if self._fail_countdown != None:
            if self._fail_countdown == 0:
                self._fail_countdown = None
                self._fail = True
            else:
                self._fail_countdown -= 1
        if self._delay_countdown != None:
            if self._delay_countdown == 0:
                self._delay_countdown = None
                self._delay = True
            else:
                self._delay_countdown -= 1
        if self._replay_delay_countdown != None:
            if self._replay_delay_countdown == 0:
                self._replay_delay_countdown = None
                self._replay_delay = True
            else:
                self._replay_delay_countdown -= 1

    def service(self, handler_call_details):
        if self._call_before:
            self._call_before()
        logger.debug('service with replay delay %s; %s delayed calls; countdowns %s fail / %s delay / %s replay delay ; fail %s; delay %s',
            self._replay_delay,
            len(self._delayed_calls),
            self._fail_countdown,
            self._delay_countdown,
            self._replay_delay_countdown,
            self._fail,
            self._delay
        )
        self._do_fail_countdown()
        if self._replay_delay and len(self._delayed_calls) > 0:
            delayed_index = random.randrange(len(self._delayed_calls))
            to_replay = self._delayed_calls[delayed_index]
            logger.info('making delayed and/or reordered call to %s', to_replay[0])
            replay_callable = self._target_channel.unary_unary(to_replay[0])
            try:
                replay_callable(to_replay[1])
            except:
                logger.info('reordered call to %s failed', to_replay[0])
                pass
        if self._fail:
            if self._fail_but_send:
                my_callable = self._target_channel.unary_unary(handler_call_details.method)
                def discard_call(request, context):
                    my_callable(request)
                    context.abort(grpc.StatusCode.UNAVAILABLE, 'injected failure')
                self._fail_but_send = False  # XXX: for now, always only do it for the first call
                return grpc.unary_unary_rpc_method_handler(discard_call)
            else:
                return _unary_unary_rpc_terminator(grpc.StatusCode.UNAVAILABLE, 'injected failure')
        elif self._delay:
            method = handler_call_details.method
            def delay_call(request, context):
                self._delayed_calls.append((method, request))
                context.abort(grpc.StatusCode.UNAVAILABLE, 'injected failure')
            return grpc.unary_unary_rpc_method_handler(delay_call)
        else:
            my_callable = self._target_channel.unary_unary(handler_call_details.method)
            def forward_call(request, context):
                response = my_callable(request)
                if self._call_after:
                    self._call_after()
                return response
            return grpc.unary_unary_rpc_method_handler(forward_call)
