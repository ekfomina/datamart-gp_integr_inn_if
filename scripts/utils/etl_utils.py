"""
В данный модуль вынесен основной функционал

Практика показала, что под копирку в других проектах
данный могуль использовать не получится
"""
import glob
import logging
import re
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from pyspark.sql import HiveContext
from pyspark.sql import SparkSession
logger = logging.getLogger(__name__)


class DatesForLaunch:
    """
    Структура данных для работы с дадами для запуска
    """

    def __init__(self, report_date: datetime.date):
        dt_end = report_date.replace(day=1) - timedelta(days=1)
        dt_start = dt_end.replace(day=1)
        dt_prev = dt_start - timedelta(days=1)

        self.end_str = str(dt_end)  # последний день предидущего месяца
        self.start_str = str(dt_start)  # первый день предидущего месяца
        self.prev_str = str(dt_prev)  # последний день предидущего, предидущего месяца

        # если я не ошибаюсь, такие поля нужны тк они будут использоваться
        # в sql-ный запросах, к таблицам, в которых есть поля с датой
        # но формат даты там dicimal(<year><month><day>) а не timestamp
        self.start = dt_start.strftime('%Y%m%d')
        self.end = dt_end.strftime('%Y%m%d')
        self.prev = dt_prev.strftime('%Y%m%d')


class FileWithStep:
    """
    Класс в котором хранятся sql-ные файлы

    Которые должны запускаться в определенном порядке
    Файл с наименьшим step должен запускаться первым

    Например
    1) step = 1
    2) step = 2
    3) step = 13

    Значение step должно принадлежать [0; +infinity)
    """

    def __init__(self, path_to_file: str, step: int):

        assert isinstance(step, int)

        if step < 0:
            raise ValueError("step should be [0; +infinity)")

        self.path_to_file = path_to_file
        self.step = step

    def __lt__(self, other):
        if self.step < other.step:
            return True
        else:
            return False


def change_hive_config_for_working_with_partition(sc) -> None:
    """
    Измененение конфига Hive-а для партиционирования

    Нужно для того, чтобы можно было инсерты с партиционированием делать

    Args:
        sc: спарк контекст

    Returns: None

    """
    hc = HiveContext(sc)
    hc.setConf("hive.exec.dynamic.partition", "true")
    hc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hc.setConf("hive.metastore.try.direct.sql", "true")

    return None


def get_spark(app_name: str) -> SparkSession:
    """
    Получение спарк сессии

    Returns: pyspark.sql.SparkSession
    """
    spark = (SparkSession.builder
             .appName(app_name)
             .enableHiveSupport()
             .getOrCreate())

    blockSize = 1024 * 1024 * 128
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().setInt("df.blocksize", blockSize)
    sc._jsc.hadoopConfiguration().setInt("parquet.blocksize", blockSize)
    sc._jsc.hadoopConfiguration().setInt("dfs.blocksize", blockSize)
    sc._jsc.hadoopConfiguration().setInt("dfs.block.size", blockSize)

    return spark


def get_sql_query(fname: str) -> str:
    """
    Читает sql-ный запрос из файла

    Args:
        fname: путь до файла с sql Запросом

    Returns: sql-ный запрос

    """
    with open(fname) as f:
        query = f.read()
    return query


def get_stat_value(spark: SparkSession, query: str):
    """
    Получает статистические данные

    Благодоря тому, что исполняет переданный запрос
    Args:
        spark: спарк сессия
        query: sql-ный запрос

    Returns:

    """
    df = spark.sql(query)
    try:
        stat_value = df.collect()[0][0]
        return stat_value
    except Exception as e:
        logger.debug("ERROR: get state")
        return None


def get_stat_value_from(spark: SparkSession, path_to_query: str, *argv):
    """
    Получает статистические данные по запросу из указанного файла

    Благодоря тому, что исполняет переданный запрос
    Args:
        spark: спарк сессия
        path_to_query: адрес файла с sql-ным запросом
        argv: параметры для форматирования запроса

    Returns:
        Значение, которое выдал spark по форматированному запросу

    """
    query = get_sql_query(path_to_query).format(*argv)
    stat_value = get_stat_value(spark, query)
    return stat_value


def write_to_file(file_name: str, data) -> None:
    """
    Записывает данные в файл

    Args:
        file_name: пусть до файла
        data: данные которые нужно записать

    Returns: None

    """
    with open(file_name, 'w') as f:
        f.write(data)

    return None


def extract_all_files_by_pattern_with_step(path_to_scanning: str, pattern: str) -> list:
    """
    Па узазанному пути извлекаются все файы удовлетовряющие шаблону

    Важно, чтобы в назнвании самого файла было именно одно число - шаг

    Например:
        корректные названия: stat1.sql, 1stat.sql, stat12.sql
        не корректные названия: stat1_1.sql, 12stat1.sql

    Args:
        path_to_scanning: директория в которой нужно файлы искать (рекурсивно)
        pattern: шаблон интересующих файлов

    Returns: список файлов (обьекты типа SQLFileWithStep)

    Пример
        path_to_scanning:
            * "."
            * ".."
            * "/"
            * "/scripts"
            * "/user/home/scripts"

        pattern:
            * "step*.sql"
            * "*.sql"
    """
    all_files = []

    for file in Path(path_to_scanning).rglob(pattern):
        absolute_file_path = str(file.absolute())
        file_name = file.name

        step = re.findall(r"\d+", file_name)

        if len(step) != 1:
            raise ValueError("Expect file with one digit(step)")

        file_with_step = FileWithStep(absolute_file_path, int(step[0]))
        all_files.append(file_with_step)

    return all_files


def drop_all_partitions(spark: SparkSession, tbl_db: str, tbl_name: str, partition_name: str):
    query_partitions = "show partitions " + tbl_db + "." + tbl_name
    partitions_df = spark.sql(query_partitions)
    query_drop_partition = "alter table " + tbl_db + "." + tbl_name \
                           + " drop if exists partition (" + partition_name + "='{0}')"
    partitions = [str(row['partition']).split('=')[1] for row in partitions_df.collect()]
    for partition in partitions:
        query = query_drop_partition.format(partition)
        spark.sql(query)
    return True


def recreate_table(spark: SparkSession, path_to_query: str) -> None:
    for file_name in glob.glob(path_to_query):
        queries = get_sql_query(file_name)
        queries = queries.split(';')
        for query in queries:
            logger.info(query)
            spark.sql(query)
