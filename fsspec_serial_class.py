import time
import xarray as xr
import fsspec
from dotenv import load_dotenv
load_dotenv()


class S3NCReader:
    def __init__(self):
        self.so = so
        self.file_list = files

    def s3_reader(self, file, i):
        fs = fsspec.filesystem("s3", **self.so)
        return xr.open_dataset(fs.open(f"s3://{file[i]}", mode="rb"), engine="h5netcdf", chunks={})

    def preprocessing(self, ds: xr.Dataset):
        time.sleep(5)
        return ds

    def generate_data(self):
        start = time.time()
        for i in range(3):
            x = self.s3_reader(self.file_list, i)
            y = self.preprocessing(x)
        print(f"time to process {i} files: {time.time() - start:.2f} seconds")


if __name__ == "__main__":
    so = dict(
        anon=True,
    )

    files = [
        'usgs-coawst/useast-archive/coawst_2009-08-21_0000.nc',
        'usgs-coawst/useast-archive/coawst_2009-08-28_0001.nc',
        'usgs-coawst/useast-archive/coawst_2009-09-04_0002.nc',
    ]

    test = S3NCReader()
    test.generate_data()
