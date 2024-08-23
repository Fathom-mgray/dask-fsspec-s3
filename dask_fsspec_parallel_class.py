import time
import xarray as xr
import fsspec
from dask.distributed import Client, LocalCluster, as_completed
from dotenv import load_dotenv
load_dotenv()


class S3NCReader:
    def __init__(self):
        self.so = so
        self.file_list = files

    def s3_reader(self, file):
        fs = fsspec.filesystem("s3", **self.so)
        return xr.open_dataset(fs.open(f"s3://{file}", mode="rb"), engine="h5netcdf", chunks={})

    def preprocessing(self, ds: xr.Dataset):
        time.sleep(5)
        return ds

    def generate_data(self):
        start = time.time()
        futures = []
        for i in range(3):
            futures.append(client.submit(self.s3_reader, *[self.file_list, i]))
        preprocessing_futures = []
        for future in as_completed(futures):
            dataset = future.result()
            preprocessing_futures.append(
                client.submit(
                    self.preprocessing,
                    *[dataset]
                )
            )
        x = client.gather(preprocessing_futures)
        print(f"time to process {i} files: {time.time()-start:.2f} seconds")


if __name__ == "__main__":
    client = Client(LocalCluster(n_workers=4))

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
