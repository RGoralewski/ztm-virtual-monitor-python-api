import requests
try:
    from . import gtfs_realtime_pb2
except ImportError:
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
import time
from google.protobuf.message import DecodeError


class ZTMVirtualMonitorAPI:
    def __init__(self, stop_code: str):

        self.__logger = logging.getLogger("ZTMVirtualMonitorAPI")

        self.__gtfs_zip_url = 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile'
        self.__gtfs_rt_trip_updates_url = 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGtfsRtFile?file=trip_updates.pb'
        self.__tmp_files_subdirectory = 'tmp'
        self.__stop_code = stop_code
        self.__gtfs_rt_decode_tries_limit = 3

        self.__stop_id = None
        self.__stop_times_df = pd.DataFrame()
        self.__trips_df = pd.DataFrame()
        self.__calendar_df = pd.DataFrame()

        self.update_initial_gtfs()

    def update_initial_gtfs(self) -> None:
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

    def __get_next_stop_time(self, ignored_trips: list = None) -> pd.Series:
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
        if next_stop_time_idx > (len(filtered_stop_times_df) - 1):
            next_stop_time_idx = 0

        return filtered_stop_times_df.iloc[next_stop_time_idx]

    def __get_gtfs_rt_feed_message(self) -> gtfs_realtime_pb2.FeedMessage:
        fails_counter = 0
        while True:
            try:
                self.__logger.info('Downloading GTFS-RT data...')
                r = requests.get(self.__gtfs_rt_trip_updates_url, allow_redirects=True)
                feed_message = gtfs_realtime_pb2.FeedMessage()
                feed_message.ParseFromString(r.content)
                self.__logger.info('GTFS-RT feed message downloaded and decoded.')
                return feed_message
            except DecodeError:
                self.__logger.info('Could not parse downloaded content! Retrying...')
                fails_counter += 1
                if fails_counter >= self.__gtfs_rt_decode_tries_limit:
                    self.__logger.warning(f'GTFS-RT downloading and decoding limit of {self.__gtfs_rt_decode_tries_limit} tries has been reached!')
                    break

        self.__logger.warning("Could not get GTFS-RT!")
        return None

    def generate_timetable(self, n_trips) -> pd.DataFrame:

        gtfs_rt_feed_message = self.__get_gtfs_rt_feed_message()

        trips_df = pd.DataFrame(columns=['arrival_time', 'trip_headsign', 'route_id', 'wheelchair_accessible', 'arrival_realtime'])
        ignored_trips = []
        while len(trips_df) < n_trips:
            current_stop_time = self.__get_next_stop_time(ignored_trips)
            ignored_trips.append(current_stop_time['trip_id'])

            arrival_delay = None
            if gtfs_rt_feed_message:
                for gtfs_rt_entity in gtfs_rt_feed_message.entity:
                    if gtfs_rt_entity.trip_update.trip.trip_id == current_stop_time['trip_id']:
                        arrival_delay = gtfs_rt_entity.trip_update.stop_time_update[0].arrival.delay

            current_trip_row_from_trips_df = self.__trips_df[self.__trips_df['trip_id'] == current_stop_time['trip_id']]
            route_id = current_trip_row_from_trips_df['route_id'].iloc[0]
            trip_headsing = current_trip_row_from_trips_df['trip_headsign'].iloc[0]
            wheelchair_accessible = current_trip_row_from_trips_df['wheelchair_accessible'].iloc[0]
            trips_df.loc[len(trips_df)] = [current_stop_time['arrival_time'], trip_headsing, route_id, wheelchair_accessible, arrival_delay]

        return trips_df


@click.command()
@click.option('-s', '--stop-code', type=str, required=True, help='Stop code in ZTM Poznan')
@click.option('-t', '--timetable-length', type=int, required=True,
              help='A number of rows in result dataframe with trips')
@click.option('-v', '--verbose', count=True, help='Logging level')
@click.option('-l', '--log', is_flag=True, default=False, help='Enable logging to file')
def main(stop_code, timetable_length, verbose, log):
    log_handlers = [logging.StreamHandler()]
    log_level = {0: logging.INFO, 1: logging.DEBUG}.get(verbose, logging.INFO)

    if log:
        log_handlers.append(
            logging.FileHandler(datetime.now().strftime(f"%Y-%m-%d-%H-%M-%S.log"))
        )

    logging.basicConfig(
        handlers=log_handlers,
        encoding='utf-8',
        level=log_level,
        format='%(asctime)s|%(levelname)s|%(name)s|%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    vm = ZTMVirtualMonitorAPI(stop_code)
    try:
        while True:
            with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                print(vm.generate_timetable(timetable_length))
            time.sleep(30)
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
