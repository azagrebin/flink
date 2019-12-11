FLINK_DIR=build-target
FLINK_DIR=${FLINK_DIR} ./mem_setup.sh
ec=$?
if [ $ec != 0 ]; then
    exit $ec;
fi

./run.sh ${FLINK_DIR} ${@}
