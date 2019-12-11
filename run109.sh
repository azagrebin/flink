FLINK_DIR=flink109
cp ./flink-conf-09.yaml ${FLINK_DIR}/conf/.
./run.sh ${FLINK_DIR} ${@}
