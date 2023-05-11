# ZTM Virtual Monitor API

## About
Python API that generates Poznan's public transport virtual monitor records 
based on data made available by ZTM Poznań: https://www.ztm.poznan.pl/pl/dla-deweloperow/index

## Protobuf format
Protobuf format for GTFS Realtime is described on: https://developers.google.com/transit/gtfs-realtime?hl=en

Protocol buffer is compiled and attached in repo as [gtfs_realtime_pb2.py](gtfs_realtime_pb2.py) file.

## Usage

Run demo (virtual monitor updating every 30 seconds):
```commandline
python3.10 ztm_virtual_monitor_api.py -s RKAP71
```

Or add repo as a submodule and use *ZTMVirtualMonitorAPI* class in your code.


### Demo result
```console
2023-05-11 13:29:03|INFO|ZTMVirtualMonitorAPI|Downloading GTFS-RT data...
2023-05-11 13:29:04|INFO|ZTMVirtualMonitorAPI|GTFS-RT feed message downloaded and decoded.
  arrival_time    trip_headsign route_id  wheelchair_accessible   
0     13:30:00      Górczyn PKM        8                      1  \
1     13:31:00    Starołęka PKM       12                      1   
2     13:33:00  Unii Lubelskiej       11                      1   
3     13:33:00         Junikowo       15                      0   
4     13:34:00       Słowiańska        3                      1   
5     13:38:00          Franowo       18                      1   

   arrival_realtime  
0               -31  
1               -61  
2               -11  
3               -56  
4               -54  
5                -9  

```