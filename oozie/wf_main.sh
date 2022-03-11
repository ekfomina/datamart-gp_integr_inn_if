echo "start_bash"

PATH_TO_PYTHON=/opt/cloudera/parcels/PYENV.ZNO20008661/bin/python

# Extremely useful option allowing access to Hive Metastore from Spark 2.2
export HADOOP_CONF_DIR=/etc/hive/conf

KEYTAB=$(basename $PATH_TO_KEYTAB)

v_wf_path="DataMartIlFGp/"
PATH_TO_PROJ_HDFS="$NAME_NODE$OOZIE_PATH$v_wf_path"

echo "start get files from hdfs"
hdfs dfs -get ${PATH_TO_PROJ_HDFS}/scripts
hdfs dfs -get ${PATH_TO_PROJ_HDFS}/configs

if [[ $FAKE_LOADING == "1" ]]
then
  exit 0
fi

echo "start spark submit"
spark2-submit \
--master yarn \
--keytab $KEYTAB \
--queue $YARN_QUEUE \
--principal ${PRINCIPAL}@${REALM} \
--conf spark.pyspark.driver.python=$PATH_TO_PYTHON \
--conf spark.pyspark.python=$PATH_TO_PYTHON \
--verbose \
scripts/main.py --loading_id $LOADING_ID \
                --etl_force_load $ETL_FORCE_LOAD \
                --ETL_SRC_TABLE_1 $ETL_SRC_TABLE_1  \
                --ETL_SRC_DIR_1 $ETL_SRC_DIR_1  \
                --ETL_SRC_TABLE_2 $ETL_SRC_TABLE_2  \
                --ETL_SRC_DIR_2 $ETL_SRC_DIR_2  \
                --etl_pa_table_1 $ETL_PA_TABLE_1  \
                --etl_pa_dir_1 $ETL_PA_DIR_1  \
                --etl_pa_table_2 $ETL_PA_TABLE_2  \
                --etl_pa_dir_2 $ETL_PA_DIR_2  \
                --fake_loading $FAKE_LOADING  \
                --name_node $NAME_NODE \
                --ctl_url $CTL \
                --ctl_entity_id $CTL_ENTITY_ID \

spark_submit_exit_code=$?
echo "spark-submit exit code: ${spark_submit_exit_code}"

# если была ошибка то возвращяем не 0 код возврата
if [[ $spark_submit_exit_code != 0 ]]
then
  exit $spark_submit_exit_code
fi

echo "end_bash"
