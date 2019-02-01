set FLINK=flink-dist\target\flink-1.8-SNAPSHOT-bin\flink-1.8-SNAPSHOT

if [%1] == [cl] goto start_cluster

if [%1] == [cancel] goto cancel

if [%1] == [sp] goto savep

if [%1] == [res] goto restore

if [%1] == [] goto nosp
::else goto spe

:spe
set SP=-s %1
goto cont

:nosp
set SP=

:cont
cd %FLINK%
if not exist tmp mkdir tmp
call bin\flink.bat run -d %SP%^
  d:\flink\flink-end-to-end-tests\flink-stream-state-ttl-test\target\DataStreamStateTTLTestProgram.jar^
  --test.semantics exactly-once^
  --environment.parallelism 1^
  --state_backend rocks^
  --state_ttl_verifier.ttl_milli 1000^
  --state_ttl_verifier.precision_milli 5^
  --state_backend.checkpoint_directory file:///tmp^
  --state_backend.file.async false^
  --update_generator_source.sleep_time 10^
  --update_generator_source.sleep_after_elements 1 && ^
cd ../../../..
timeout 30
goto exit_src

:cancel
:: %2 = <job-id>
cd %FLINK%
call bin\flink.bat cancel %2
cd ../../../..
goto exit_src

:savep
:: %2 = <job-id>
cd %FLINK%
if not exist tmp mkdir tmp
call bin\flink.bat savepoint %2 tmp
cd ../../../..
goto exit_src

:restore
:: %2 = <savepoint_id>
call test-frocksdb\run.bat tmp\%2
goto exit_src

:start_cluster
cd %FLINK%
call bin\start-cluster.bat
cd ../../../..
explorer "http://localhost:8081/#/running-jobs"
goto exit_src

:exit_src
exit /b