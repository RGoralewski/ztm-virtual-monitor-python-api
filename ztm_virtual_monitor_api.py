import requests
import gtfs_realtime_pb2
from pathlib import Path
import shutil
import zipfile
from io import BytesIO
import logging
import click
from datetime import datetime
import pandas as pd
import numpy as np


class ZTMVirtualMonitorAPI:
    def __init__(self):

        self.__logger = logging.getLogger("ZTMVirtualMonitorAPI")

        self.__gtfs_zip_url = 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile'
        self.__gtfs_rt_trip_updates_url = 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGtfsRtFile?file=trip_updates.pb'
        self.__tmp_files_subdirectory = 'tmp'
        self.__stop_code = 'MOGI42'

        self.__stop_id = None
        self.__stop_times_df = pd.DataFrame()
        self.__trips_df = pd.DataFrame()

        self.__get_initial_gtfs()

    def __get_initial_gtfs(self) -> None:
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
        self.__trips_df = all_trips_df[all_trips_df['trip_id'].isin(self.__stop_times_df['trip_id'])].reset_index(drop=True)

        self.__logger.info('Extracting calendar data...')
        self.__calendar_df = pd.read_csv(tmp_files_absolute_directory / 'calendar.txt', delimiter=',')

        self.__logger.info('Removing tmp directory with all files inside...')
        shutil.rmtree(tmp_files_absolute_directory)

    def __get_next_trip(self, ignored_trips: list = None) -> str:
        current_datetime = datetime.now()
        weekday = current_datetime.strftime('%A').lower()
        time_string = current_datetime.strftime('%H:%M:%S')

        current_weekday_df = self.__calendar_df[self.__calendar_df[weekday] == 1]
        if current_weekday_df.empty:
            raise Exception("There's no service_id in calendar_df for current weekday!")
        current_weekday_service_ids = list(current_weekday_df['service_id'])
        current_weekday_trips_df = self.__trips_df[self.__trips_df['service_id'].isin(current_weekday_service_ids)]
        filtered_stop_times_df = self.__stop_times_df[self.__stop_times_df['trip_id'].isin(current_weekday_trips_df['trip_id'])]

        filtered_stop_times_df = filtered_stop_times_df[~filtered_stop_times_df['trip_id'].isin(ignored_trips)]
        filtered_stop_times_df = filtered_stop_times_df.reset_index(drop=True)

        next_stop_time_idx = np.searchsorted(filtered_stop_times_df['arrival_time'], time_string, side='left')
        if next_stop_time_idx > (len(self.__stop_times_df) - 1):
            next_stop_time_idx = 0

        return self.__stop_times_df.iloc[next_stop_time_idx]


    def get_virtual_monitor(self, n_trips: int):

        self.__logger.info("Downloading GTFS-RT data...")
        r = requests.get(self.__gtfs_rt_trip_updates_url, allow_redirects=True)
        feed_message = gtfs_realtime_pb2.FeedMessage()
        feed_message.ParseFromString(r.content)
        print(feed_message)

        # TODO test this function
        self.__get_next_trip(['4_5900741^N+', '2_5934101^B,N', '4_5900670^B,N'])




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
    vm.get_virtual_monitor(5)


if __name__ == '__main__':
    main()
