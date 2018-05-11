#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

END_TO_END_DIR="`dirname \"$0\"`" # relative
END_TO_END_DIR="`( cd \"$END_TO_END_DIR\" && pwd )`" # absolutized and normalized
if [ -z "$END_TO_END_DIR" ] ; then
    # error; for some reason, the path is not accessible
    # to the script (e.g. permissions re-evaled after suid)
    exit 1  # fail
fi

if [ -z "$FLINK_DIR" ] ; then
    echo "You have to export the Flink distribution directory as FLINK_DIR"
    exit 1
fi

FLINK_DIR="`( cd \"$FLINK_DIR\" && pwd )`" # absolutized and normalized

echo "flink-end-to-end-test directory: $END_TO_END_DIR"
echo "Flink distribution directory: $FLINK_DIR"

START_SCRIPT_TS_SEC=$(date +%s)sec

function elapsed {
    MESSAGE=$1
    FROM=$2
    echo "${MESSAGE} $(TZ=UTC date --date now-${FROM} +%H:%M:%S.%N)"
}

function elapsed_overall {
    elapsed "Elapsed overall:" ${START_SCRIPT_TS_SEC}
}

function run_test {
    DESC=$1
    CMD=$2
    printf "\n==============================================================================\n"
    printf "${DESC}\n"
    printf "==============================================================================\n"
    START_TEST_TS_SEC=$(date +%s)sec
    ${CMD}
    EXIT_CODE=$?
    elapsed "Test '${DESC}' took:" ${START_TEST_TS_SEC}
    elapsed_overall
}

EXIT_CODE=0

# Template for adding a test:

#if [ $EXIT_CODE == 0 ]; then
#    DESC="Running my fancy nightly end-to-end test"
#    CMD="$END_TO_END_DIR/test-scripts/test_something_very_fancy.sh"
#    ENV_VAR1=val1 ENV_VAR2=val2 run_test "${DESC}" "${CMD}"
#fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running HA end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_ha.sh"
    run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (file, async, no parallelism change) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2"
    STATE_BACKEND_TYPE=file STATE_BACKEND_FILE_ASYNC=true run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (file, sync, no parallelism change) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2"
    STATE_BACKEND_TYPE=file STATE_BACKEND_FILE_ASYNC=false run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (rocks, no parallelism change) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 2"
    STATE_BACKEND_TYPE=rocks run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (file, async, scale up) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4"
    STATE_BACKEND_TYPE=file STATE_BACKEND_FILE_ASYNC=true run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (file, sync, scale up) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4"
    STATE_BACKEND_TYPE=file STATE_BACKEND_FILE_ASYNC=false run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (rocks, scale up) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 2 4"
    STATE_BACKEND_TYPE=rocks run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (file, async, scale down) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2"
    STATE_BACKEND_TYPE=file STATE_BACKEND_FILE_ASYNC=true run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (ile, sync, scale down) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2"
    STATE_BACKEND_TYPE=file STATE_BACKEND_FILE_ASYNC=false run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Resuming Savepoint (rocks, scale down) end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_resume_savepoint.sh 4 2"
    STATE_BACKEND_TYPE=rocks run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
  DESC="Running Resuming Externalized Checkpoint (file, async) end-to-end test"
  CMD="$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh"
  STATE_BACKEND_TYPE=file STATE_BACKEND_FILE_ASYNC=true run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
  DESC="Running Resuming Externalized Checkpoint (file, sync) end-to-end test"
  CMD="$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh"
  STATE_BACKEND_TYPE=file STATE_BACKEND_FILE_ASYNC=false run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
  DESC="Running Resuming Externalized Checkpoint (rocks) end-to-end test"
  CMD="$END_TO_END_DIR/test-scripts/test_resume_externalized_checkpoints.sh"
  STATE_BACKEND_TYPE=rocks run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running DataSet allround nightly end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_batch_allround.sh"
    run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Streaming SQL nightly end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_streaming_sql.sh"
    run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running Streaming bucketing nightly end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_streaming_bucketing.sh"
    run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running stateful stream job upgrade nightly end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_stateful_stream_job_upgrade.sh 2 4"
    run_test "${DESC}" "${CMD}"
fi

if [ $EXIT_CODE == 0 ]; then
    DESC="Running connected components iterations with high parallelism nightly end-to-end test"
    CMD="$END_TO_END_DIR/test-scripts/test_high_parallelism_iterations.sh"
    PARALLELISM=25 run_test "${DESC}" "${CMD}"
fi

# Exit code for Travis build success/failure
exit $EXIT_CODE
