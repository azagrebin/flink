_FLINK_HOME_DETERMINED=1
FLINK_HOME=${FLINK_DIR:-build-target}

cp ./flink-conf.yaml ${FLINK_HOME}/conf/.

. ${FLINK_HOME}/bin/config.sh

class_path=`constructFlinkClassPath`
class_path=`manglePathList ${class_path}`

run() {
  ${JAVA_RUN} -classpath ${class_path} org.apache.flink.runtime.util.BashJavaUtils ${1} --configDir ${FLINK_HOME}/conf
  ec=$?
  if [ $ec != 0 ]; then
    exit $ec;
  fi
}

run GET_TM_RESOURCE_JVM_PARAMS
run GET_TM_RESOURCE_DYNAMIC_CONFIGS

unset _FLINK_HOME_DETERMINED
unset FLINK_HOME
