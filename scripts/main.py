import logging
import os
from argparse import ArgumentParser
from datetime import datetime, timedelta

from utils import args_parser as ARGS
from utils import etl_utils as SVETL
from utils import hdfs_shell_command as SHELL
from utils import logging_utils as LOG
from utils import repartition as PART

logger = logging.getLogger(__name__)


if __name__ == '__main__':
    LOG.setup_logger("configs/logger_config.yaml")

    logger.info("start")

    logger.ctl_info("path to log in HDFS: {}".format(os.environ.get("PATH_TO_LOG_IN_HDFS")))

    # Parse args
    parser = ArgumentParser()
    args = ARGS.parse_arguments(parser)
    logger.info(str(args))

    # Getting date and time of launch (first 10 letters is report date)
    ctl_validfrom = datetime.now().strftime('%Y-%m-%d')

    logger.info("start spark session")
    spark = SVETL.get_spark('start custom_salesntwrk_gp_integr_inn_individuals_features' + str(args.loading_id))

    logger.info("change spark/hive config")
    SVETL.change_hive_config_for_working_with_partition(spark.sparkContext)
    spark.conf.set("spark.sql.hive.convertMetastoreParquet", False)

    count_in_pa_uzp_data_payroll_m = SVETL.get_stat_value_from(spark, 'scripts/sql/count.sql', args.etl_pa_table_1)
    logger.info("count records in uzp_data_payroll_m  " + str(count_in_pa_uzp_data_payroll_m))

    count_in_pa_uzp_data_merch = SVETL.get_stat_value_from(spark, 'scripts/sql/count.sql', args.etl_pa_table_2)
    logger.info("count records in uzp_data_merch  " + str(count_in_pa_uzp_data_merch))


    src_loading_id_uzp_data_payroll_m = SVETL.get_stat_value_from(spark, 'scripts/sql/get_src_loading_id.sql', args.etl_src_table_1)
    logger.info("src loading id in " + str(args.etl_src_table_1) + "  " + str(src_loading_id_uzp_data_payroll_m))

    src_loading_id_uzp_data_merch = SVETL.get_stat_value_from(spark, 'scripts/sql/get_src_loading_id.sql', args.etl_src_table_2)
    logger.info("src loading id in " + str(args.etl_src_table_1) + "  " + str(src_loading_id_uzp_data_merch))




    if args.etl_force_load == 1:
        logger.info("CLEAR {}".format(args.etl_pa_dir))
        SHELL.run_cmd(['hdfs', 'dfs', '-rm', '-R', '-skipTrash', str(args.etl_pa_dir_1)+"*"])

        logger.info("CLEAR {}".format(args.etl_pa_dir))
        SHELL.run_cmd(['hdfs', 'dfs', '-rm', '-R', '-skipTrash', str(args.etl_pa_dir_2)+"*"])

    SVETL.recreate_table(spark, 'scripts/sql/ddl.sql')

    logger.info("START {}".format(args.etl_pa_table_1))
    query = SVETL.get_sql_query("scripts/sql/uzp_data_payroll_m.sql").format(args.etl_src_table_1, args.etl_pa_table_1, src_loading_id_uzp_data_payroll_m, args.loading_id, ctl_validfrom)
    logger.debug("SVETL:\n{}".format(query))
    spark.sql(query)
    logger.info('SUCCESS: main sql finished')


    logger.info("START {}".format(args.etl_pa_table_2))
    query = SVETL.get_sql_query("scripts/sql/uzp_data_merc.sql").format(args.etl_src_table_2, args.etl_pa_table_2, src_loading_id_uzp_data_merch, args.loading_id, ctl_validfrom)
    logger.debug("SVETL:\n{}".format(query))
    spark.sql(query)
    logger.info('SUCCESS: main sql finished')

    logger.info("Repart " + str(args.etl_pa_dir))
    # For interaction with partitions
    data_control = PART.PartitionController(spark)

    data_control.repart(str(args.etl_pa_dir), ctl_validfrom)



    SVETL.recreate_table(spark, 'scripts/sql/ddl.sql')

    count_in_pa_uzp_data_payroll_m = SVETL.get_stat_value_from(spark, 'scripts/sql/count.sql', args.etl_pa_table_1)
    logger.info("count records in uzp_data_payroll_m after loading" + str(count_in_pa_uzp_data_payroll_m))

    count_in_pa_uzp_data_merch = SVETL.get_stat_value_from(spark, 'scripts/sql/count.sql', args.etl_pa_table_2)
    logger.info("count records in uzp_data_merch after loading" + str(count_in_pa_uzp_data_merch))


    # Statistics
    ctl_url_stat = args.ctl_url + '/v1/api/statval/m'
    ctl_stat = LOG.CTLStatistics(ctl_url=ctl_url_stat,
                                 loading_id=args.loading_id,
                                 ctl_entity_id=args.ctl_entity_id)

    applicationId = spark._sc.applicationId
    ctl_stat.upload_statistic(stat_id=15, stat_value=applicationId)
    logger.info("YARN application id '{}' was published".format(applicationId))

    ctl_stat.upload_statistic(stat_id=11, stat_value=ctl_validfrom)
    logger.info("LAST_LOADED_TIME '{}' was published".format(ctl_validfrom))
    change = 1
    ctl_stat.upload_statistic(stat_id=2, stat_value=str(change))
    if change:
        logger.info("CHANGE was published")
    else:
        logger.warning("NO CHANGE")

    business_date = ctl_validfrom
    ctl_stat.upload_statistic(stat_id=5, stat_value=business_date)
    logger.info("BUSINESS_DATE '{}' was published".format(business_date))

    max_cdc_date = ctl_validfrom
    ctl_stat.upload_statistic(stat_id=1, stat_value=max_cdc_date)
    logger.info("MAX_CDC_DATE '{}' was published".format(max_cdc_date))

    logger.ctl_info("SUCCESS. YARN Application id: " +
                    "{0}. CHANGE = {1}. New MAX_CDC_DATE = {2}. New BUSINESS_DATE = {3}.".format(
                        applicationId, change, max_cdc_date, business_date))
    logger.info("End python")
