import requests
import gtfs_realtime_pb2
from pathlib import Path
import zipfile
from io import BytesIO
import logging
import click
import datetime
import pandas as pd


class ZTMVirtualMonitorAPI:
    def __init__(self):

        self.__logger = logging.getLogger("ZTMVirtualMonitorAPI")

        self.__gtfs_zip_url = 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile'
        self.__tmp_files_subdirectory = 'tmp'
        self.__stop_code = 'MOGI42'

        self.__stop_id = None
        self.__stop_times_df = pd.DataFrame()
        self.__trips_df = pd.DataFrame()

        self.__get_initial_gtfs()

    def __get_initial_gtfs(self):
        self.__logger.info("Downloading initial GTFS data...")
        tmp_files_absolute_directory = Path.cwd() / self.__tmp_files_subdirectory
        Path(tmp_files_absolute_directory).mkdir(exist_ok=True)
        r = requests.get(self.__gtfs_zip_url, allow_redirects=True)
        filebytes = BytesIO(r.content)
        myzipfile = zipfile.ZipFile(filebytes)
        myzipfile.extractall(tmp_files_absolute_directory)

        self.__logger.info('Extracting stop_id data...')
        stops_df = pd.read_csv(tmp_files_absolute_directory / 'stops.txt', delimiter=',')
        stop_searching_result = stops_df.loc[stops_df['stop_code'] == self.__stop_code]
        if not stop_searching_result.empty:
            stop_id = stop_searching_result['stop_id'].iloc[0]
        else:
            raise Exception(f'Stop code "{self.__stop_code}" does not exist. Pass an existing stop code.')
        self.__stop_id = stop_id

        self.__logger.info('Extracting stop_times data...')
        all_stop_times_df = pd.read_csv(tmp_files_absolute_directory / 'stop_times.txt', delimiter=',')
        self.__stop_times_df = all_stop_times_df.loc[all_stop_times_df['stop_id'] == self.__stop_id]
        if not self.__stop_times_df.empty:
            self.__stop_times_df = self.__stop_times_df.sort_values(by=['arrival_time']).reset_index(drop=True)
        else:
            raise Exception(f'Stop ID "{self.__stop_id}" does not exist in stop_times.txt file.')

        self.__logger.info('Extracting trips data...')
        all_trips_df = pd.read_csv(tmp_files_absolute_directory / 'trips.txt', delimiter=',')
        self.__trips_df = all_trips_df[all_trips_df['trip_id'].isin(self.__stop_times_df['trip_id'])]

        # ToDo extract calendar

    def get_virtual_monitor(self, n_trips: int):

        feed_message = gtfs_realtime_pb2.FeedMessage()
        with open('trip_updates.pb', "rb") as f:
            feed_message.ParseFromString(f.read())
        print(feed_message)

        current_datetime = datetime.now()
        weekday = current_datetime.weekday()
        time_string = current_datetime.strftime('%H:%M:%S')

        # TODO prepare n_trips next trips





@click.command()
@click.option('-v', '--verbose', count=True, help='Logging level')
@click.option('-l', '--log', is_flag=True, default=False, help='Enable logging to file')
def main(verbose, log):
    log_handlers = [logging.StreamHandler()]
    log_level = {0: logging.INFO, 1: logging.DEBUG}.get(verbose, logging.INFO)

    if log:
        log_handlers.append(
            logging.FileHandler(datetime.datetime.now().strftime(f"%Y-%m-%d-%H-%M-%S.log"))
        )

    logging.basicConfig(
        handlers=log_handlers,
        encoding='utf-8',
        level=log_level,
        format='%(asctime)s|%(levelname)s|%(name)s|%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    vm = ZTMVirtualMonitorAPI()
    vm.download_virtual_monitor()


if __name__ == '__main__':
    main()
