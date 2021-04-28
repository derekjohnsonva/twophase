import copy
import os
import pickle

class PersistentLog(object):
    def get_last_entry(self):
        raise NotImplementedError("PersistentLog implementation should implement get_last_entry")

    def set_last_entry(self, entry):
        raise NotImplementedError("PersistentLog implementation should implement set_last_entry")


class DummyPersistentLog(object):
    def __init__(self, value=None):
        self._last_entry = value

    def get_last_entry(self):
        return copy.deepcopy(self._last_entry)

    def set_last_entry(self, new_value):
        self._last_entry = copy.deepcopy(new_value)


class InjectedFailure(Exception):
    pass


class TestingDummyPersistentLog(DummyPersistentLog):
    def __init__(self, value=None):
        self._last_entry = value
        self._fail_after = None
        self._log_just_before_failure = None

    def fail_after(self, value, log_just_before_failure=False):
        self._fail_after = value
        self._log_just_before_failure = log_just_before_failure

    def stop_failing(self):
        self._fail_after = None

    def get_last_entry(self):
        return copy.deepcopy(self._last_entry)

    def set_last_entry(self, new_value):
        if self._fail_after == 0 and not self._log_just_before_failure:
            raise InjectedFailure()
        self._last_entry = copy.deepcopy(new_value)
        if self._fail_after == 0 and self._log_just_before_failure:
            raise InjectedFailure()
        if self._fail_after != None and self._fail_after > 0:
            self._fail_after -= 1

MAX_SIZE = 16384

class FilePersistentLog(PersistentLog):
    def __init__(self, filename):
        self._fd = os.open(filename, os.O_RDWR | os.O_CREAT)
        self._fh = os.fdopen(self._fd, 'r+b', buffering=0)
        raw_index = self._fh.read(1)
        if len(raw_index) == 0:
            self._index = 0
        else:
            self._index = int.from_bytes(raw_index[0:1], byteorder='little')
        self._read_raw_value()
    
    def _read_raw_value(self):
        self._fh.seek((self._index + 1) * MAX_SIZE)
        raw_bytes = self._fh.read(MAX_SIZE)
        if len(raw_bytes) == 0:
            self._current = None
        else:
            self._current = pickle.loads(raw_bytes)

    def _write_raw_value(self):
        raw_bytes = pickle.dumps(self._current, protocol=pickle.HIGHEST_PROTOCOL)
        if len(raw_bytes) > MAX_SIZE:
            raise RuntimeError("value {} is too large to store (serializes to {} bytes)".format(value, len(raw_bytes)))
        self._fh.seek((self._index + 1) * MAX_SIZE)
        padded_raw_bytes = raw_bytes + (b'\0' * (MAX_SIZE - len(raw_bytes)))
        self._fh.write(padded_raw_bytes)
        os.fsync(self._fh.fileno())

    def _write_index(self):
        self._fh.seek(0)
        self._fh.write(self._index.to_bytes(1, byteorder='little'))
        os.fsync(self._fh.fileno())

    def get_last_entry(self):
        return self._current

    def set_last_entry(self, new_value):
        self._current = new_value
        self._index ^= 1
        self._write_raw_value()
        self._write_index()
