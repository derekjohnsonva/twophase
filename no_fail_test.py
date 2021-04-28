import logging
import os.path
import tempfile
import unittest

import grpc
import worker
import coordinator
import persistent_log

import twophase_pb2
import twophase_pb2_grpc

import test_util

from parameterized import parameterized

class NoFailTest(test_util.TestBase):
    @parameterized.expand([
        (1,),
        (2,),
        (3,),
    ])
    def test_set_one_value_no_recover(self, num_workers):
        '''Set one value, check that it is there on all workers.'''
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
    
    @parameterized.expand([
        (1,),
        (2,),
        (3,),
    ])
    def test_set_one_value_recover_workers(self, num_workers):
        '''Set one value, check that it is there on all workers after restarting the workers.'''
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.recover_workers()
        self.check_values('ValueA')

    @parameterized.expand([
        (1,),
        (2,),
        (3,),
    ])
    def test_set_one_value_recover_coordinator(self, num_workers):
        '''Set one value, check that it is there on all workers after restarting the coordinator.'''
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.recover_coordinator(expect_from='ValueA', expect_to='ValueA')
        self.check_values('ValueA')

    @parameterized.expand([
        (1,),
        (2,),
        (3,),
    ])
    def test_change_value_once(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.do_set_value('ValueB', expect_transition_from='ValueA')
        self.check_values('ValueB')

    @parameterized.expand([
        (1,),
        (2,),
        (3,),
    ])
    def test_change_value_once_recover_workers(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.do_set_value('ValueB')
        self.check_values('ValueB')
        self.recover_workers()
        self.check_values('ValueB')

    @parameterized.expand([
        (1,),
        (2,),
        (3,),
    ])
    def test_change_value_once_recover_all(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.do_set_value('ValueB')
        self.check_values('ValueB')
        self.recover_coordinator(expect_from='ValueB', expect_to='ValueB')
        self.recover_workers()
        self.recover_coordinator(expect_from='ValueB', expect_to='ValueB')
        self.recover_workers()
        self.recover_coordinator(expect_from='ValueB', expect_to='ValueB')
        self.check_values('ValueB')


    @parameterized.expand([
        (1,),
        (2,),
        (3,),
    ])
    def test_change_values(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.do_set_value('ValueB')
        self.check_values('ValueB')
        self.do_set_value('ValueC')
        self.check_values('ValueC')
        self.do_set_value('ValueD')
        self.check_values('ValueD')
        self.do_set_value('ValueE')
        self.check_values('ValueE')
        self.do_set_value('ValueA')
        self.check_values('ValueA')

    @parameterized.expand([
        (1,),
        (2,),
        (3,),
    ])
    def test_change_values_with_consistent_transitions(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.do_set_value('ValueB', expect_transition_from='ValueA')
        self.check_values('ValueB')
        self.do_set_value('ValueC', expect_transition_from='ValueB')
        self.check_values('ValueC')
        self.do_set_value('ValueD', expect_transition_from='ValueC')
        self.check_values('ValueD')
        self.do_set_value('ValueE', expect_transition_from='ValueD')
        self.check_values('ValueE')
        self.check_values('ValueE')
        self.do_set_value('ValueA', expect_transition_from='ValueE')
        self.check_values('ValueA')

    @parameterized.expand([
        (1,),
        (3,),
    ])
    def test_change_values_with_consistent_transitions_recover_workers(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.check_values('ValueA')
        self.recover_workers()
        self.check_values('ValueA')
        self.do_set_value('ValueB', expect_transition_from='ValueA')
        self.check_values('ValueB')
        self.recover_workers()
        self.check_values('ValueB')
        self.do_set_value('ValueC', expect_transition_from='ValueB')
        self.check_values('ValueC')
        self.recover_workers()
        self.check_values('ValueC')
        self.do_set_value('ValueD', expect_transition_from='ValueC')
        self.check_values('ValueD')
        self.recover_workers()
        self.check_values('ValueD')
        self.do_set_value('ValueE', expect_transition_from='ValueD')
        self.check_values('ValueE')
        self.recover_workers()
        self.check_values('ValueE')
        self.do_set_value('ValueA', expect_transition_from='ValueE')
        self.check_values('ValueA')
        self.recover_workers()
        self.check_values('ValueA')

    @parameterized.expand([
        (1,),
        (3,),
    ])
    def test_change_values_recover_coordinator(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.recover_coordinator()
        self.check_values('ValueA')
        self.do_set_value('ValueB')
        self.recover_coordinator()
        self.check_values('ValueB')
        self.do_set_value('ValueC')
        self.recover_coordinator()
        self.check_values('ValueC')
        self.do_set_value('ValueD')
        self.recover_coordinator()
        self.check_values('ValueD')
        self.do_set_value('ValueE')
        self.recover_coordinator()
        self.check_values('ValueE')
        self.do_set_value('ValueA')
        self.recover_coordinator()
        self.check_values('ValueA')

    @parameterized.expand([
        (1,),
        (3,),
    ])
    def test_change_values_with_consistent_transitions_recover_coordinator(self, num_workers):
        self.start_coordinator_and_workers(num_workers)
        self.do_set_value('ValueA')
        self.recover_coordinator(expect_from='ValueA', expect_to='ValueA')
        self.check_values('ValueA')
        self.do_set_value('ValueB', expect_transition_from='ValueA')
        self.recover_coordinator(expect_from='ValueB', expect_to='ValueB')
        self.check_values('ValueB')
        self.do_set_value('ValueC', expect_transition_from='ValueB')
        self.recover_coordinator(expect_from='ValueC', expect_to='ValueC')
        self.check_values('ValueC')
        self.do_set_value('ValueD', expect_transition_from='ValueC')
        self.recover_coordinator(expect_from='ValueD', expect_to='ValueD')
        self.check_values('ValueD')
        self.do_set_value('ValueE', expect_transition_from='ValueD')
        self.recover_coordinator(expect_from='ValueE', expect_to='ValueE')
        self.check_values('ValueE')
        self.do_set_value('ValueA', expect_transition_from='ValueE')
        self.recover_coordinator(expect_from='ValueA', expect_to='ValueA')
        self.check_values('ValueA')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    unittest.main(verbosity=2)
