STATE_BACKEND_TYPE="${2:-file}"
STATE_BACKEND_FILE_ASYNC="${3:-true}"
TTL="${4:-1000000}"
PARALLELISM="${5-3}"

JAR=./flink-end-to-end-tests/flink-stream-state-ttl-test/target/DataStreamStateTTLTestProgram.jar

cd flink-end-to-end-tests

FLINK_DIR=../${1} \
./run-single-test.sh test-scripts/test_stream_state_ttl.sh ${@:2}

#${1}/bin/flink run -d \
#      -p ${PARALLELISM} \
#      ${JAR} \
#      --test.semantics exactly-once \
#      --environment.parallelism ${PARALLELISM} \
#      --state_backend ${STATE_BACKEND_TYPE} \
#      --state_ttl_verifier.ttl_milli ${TTL} \
#      --state_backend.file.async ${STATE_BACKEND_FILE_ASYNC} \
#      --update_generator_source.sleep_time 10 \
#      --update_generator_source.sleep_after_elements 1 \
#      --state_backend.checkpoint_directory file://./chkp
