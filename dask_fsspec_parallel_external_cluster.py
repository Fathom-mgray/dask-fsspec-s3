import time
import xarray as xr
import fsspec
from dask.distributed import Client, as_completed
from cluster import DASK_SCHEDULER_ADDRESS
from dotenv import load_dotenv
load_dotenv()


client = Client(DASK_SCHEDULER_ADDRESS)

so = dict(
    anon=True,
)

main_reader = fsspec.filesystem("s3", **so)
files = [
    'usgs-coawst/useast-archive/coawst_2009-08-21_0000.nc',
    'usgs-coawst/useast-archive/coawst_2009-08-28_0001.nc',
    'usgs-coawst/useast-archive/coawst_2009-09-04_0002.nc',
         ]


def test_reader(file_list: list, i: int):
    fs = fsspec.filesystem("s3", **so)
    return xr.open_dataset(fs.open(f"s3://{file_list[i]}", mode="rb"), engine="h5netcdf", chunks={})


def preprocessing(ds: xr.Dataset):
    time.sleep(5)
    return ds


start = time.time()
futures = []
for i in range(3):
    futures.append(client.submit(test_reader, *[files, i]))
preprocessing_futures = []
for future in as_completed(futures):
    dataset = future.result()
    preprocessing_futures.append(
        client.submit(
            preprocessing,
            *[dataset]
        )
    )
x = client.gather(preprocessing_futures)
print(f"time to process {i} files: {time.time()-start:.2f} seconds")