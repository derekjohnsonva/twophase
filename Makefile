TEST_OPTIONS=-f -b

all: twophase_pb2.py twophase_pb2_grpc.py test_util_pb2.py test_util_pb2_grpc.py

twophase_pb2.py twophase_pb2_grpc.py: twophase.proto
	. bin/activate && python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. $^

test_util_pb2.py test_util_pb2_grpc.py: test_util.proto
	. bin/activate && python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. $^

testing_forwarder_test_pb2.py testing_forwarder_test_pb2_grpc.py: testing_forwarder_test.proto
	. bin/activate && python -m grpc_tools.protoc --proto_path=. --python_out=. --grpc_python_out=. $^

testing_forwarder_test: testing_forwarder_test_pb2_grpc.py
	. bin/activate && python -m testing_forwarder_test $(TEST_OPTIONS)

no_fail_test: twophase_pb2_grpc.py test_util_pb2_grpc.py
	. bin/activate && python -m no_fail_test $(TEST_OPTIONS)

failure_test: twophase_pb2_grpc.py test_util_pb2_grpc.py
	. bin/activate && python -m failure_test $(TEST_OPTIONS)

SUBMIT_FILENAME=twophase-$(shell date +%Y%m%d%H%M%S).tar.gz

archive:
	tar -zcf $(SUBMIT_FILENAME) $(wildcard *.py *.proto *.txt *.md *.pdf *.cc *.cpp *.C *.c) Makefile 
	@echo "Created $(SUBMIT_FILENAME); please upload and submit this file."

submit: archive
