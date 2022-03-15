"""
В данный модуль вынесен функционал, связанный управлением партициями
"""
import logging
import os

logger = logging.getLogger(__name__)


class PartitionController:
    def __init__(self, spark):
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs
        hadoopConfiguration = spark.sparkContext._jsc.hadoopConfiguration()

        self._spark = spark
        self._Path = fs.Path
        # self._parquet_filter = fs.GlobFilter("*.parquet")
        self._file_system = fs.FileSystem.get(hadoopConfiguration)

        self._block_size = self._file_system.getDefaultBlockSize()

    def _clear_directory(self, path_to_dir: str):
        if self._file_system.exists(self._Path(path_to_dir)):
            self._file_system.delete(self._Path(path_to_dir), True)
            logger.debug("Delete " + path_to_dir)

    @staticmethod
    def _filter_directories(path: str, min_date='', max_date=''):
        suit = min_date < path[-10:] and path != "_SUCCESS" and path[0] != "."
        if max_date != '':
            suit = path[-10:] < max_date and suit

        return suit

    def delete_partitions(self, source_dir: str, from_date='', to_date=''):
        """
        Удаляет устаревшие партиции, находящиеся в переданной директории

        Args:
            source_dir: путь до директории с партициями
            from_date: дата в конце имени директории вида '%Y-%m-%d', начиная с которой партиции удаляются
            to_date: дата в конце имени директории вида '%Y-%m-%d', до которой партиции удаляются
        """

        file_list = [str(file_status.getPath()) for file_status in self._file_system.listStatus(self._Path(source_dir))
                     if file_status.isDirectory() and self._filter_directories(str(file_status.getPath()),
                                                                               min_date=from_date,
                                                                               max_date=to_date)]
        logger.debug("Delete from date: " + str(from_date))
        logger.debug("Delete to date: " + str(to_date))
        logger.debug("File_list: " + str(file_list))

        for file in file_list:
            self._clear_directory(file)

    def repart(self, source_dir: str, start_date=''):
        """
        Укрупняет партиции, находящиеся в переданной директории, или саму директорию, если она является партицией.
        Все вложенные директории (только по переданному пути) рассматриваются как партции.
        Метод ловит исключения при чтении каждой отдельной партиции.

        Args:
            source_dir: путь до директории с партициями
            start_date: дата в конце имени директории вида '%Y-%m-%d', начиная с которой укрупняются партиции
        """
        tmp_dir = source_dir + "_temp"
        self._clear_directory(tmp_dir)

        file_list = [str(file_status.getPath()) for file_status in self._file_system.listStatus(self._Path(source_dir))
                     if file_status.isDirectory() and self._filter_directories(str(file_status.getPath()))]

        if len(file_list) == 0:
            # Для НЕпартиционированной таблицы
            logger.info("Укрупляем файлы в НЕпартиционированной таблице")
            self.repart_one_partition(source_dir, tmp_dir)

        else:
            # Для партицинированной таблицы
            file_list = list(filter(lambda path: path[-10:] >= start_date, file_list))
            logger.debug("Start date " + str(start_date))
            logger.debug("File list " + str(file_list))
            for file in file_list:
                logger.debug("File " + file)
                part_name = os.path.basename(file)
                logger.info("Укрупняем файлы в партции " + part_name)
                self.repart_one_partition(source_dir + "/" + part_name, tmp_dir + "/" + part_name)

    def repart_one_partition(self, source_dir: str, tmp_dir: str = None):
        """
        Укрупняет паркетники, находящиеся в переданной директории (не рассматривая вложенные директории).
        Метод ловит исключения при чтении parquet файлов и выводит их в логи.

        Args:
            source_dir: путь до директории с parquet файлами
            tmp_dir: путь до директории для временных файлов
        """
        if tmp_dir is None:
            tmp_dir = source_dir + "_temp"
            self._clear_directory(tmp_dir)

        size_in_bytes = self._file_system.getContentSummary(self._Path(source_dir)).getLength()
        if size_in_bytes == 0:
            logger.info("Current directory is empty")
            return None

        logger.debug("Size in bytes " + str(size_in_bytes))

        repart_factor = (size_in_bytes // self._block_size) + 1
        logger.info("Coalesce = " + str(repart_factor))

        logger.debug("Start reading from source")
        df = self._spark.read.parquet(source_dir)
        # except Exception as e:
        #     logger.error("Reading parquet failed on " + source_dir + ":\n" + str(e))
        #     return None

        logger.debug("Start coalesce")
        df.coalesce(repart_factor).write.mode("append").parquet(tmp_dir)

        self._clear_directory(source_dir)

        logger.debug("Move " + tmp_dir + " --> " + source_dir)
        self._file_system.rename(self._Path(tmp_dir), self._Path(source_dir))
