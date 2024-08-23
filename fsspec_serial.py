import time
import xarray as xr
import fsspec
from dotenv import load_dotenv
load_dotenv()

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
for i in range(3):
    x = test_reader(files, i)
    y = preprocessing(x)
print(f"time to process {i} files: {time.time()-start:.2f} seconds")