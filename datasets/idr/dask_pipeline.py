from dask.distributed import Client, as_completed
import dask.bag as db
from dask import delayed
import pandas as pd
import dask.dataframe as dd
from idr import connection
import omero
import asyncio
import logging
from dask_jobqueue import SLURMCluster
from dask.distributed import LocalCluster
from dask.cache import Cache
from distributed import progress
from dask.diagnostics import ProgressBar
import time 
from functools import wraps
from functools import partial

from dask import compute

import omero
import os
import json
import idr_funs
from idr import connection
from idr_funs import download_image
from dask import delayed, compute
from dask.distributed import Client, progress
import os
from tqdm import tqdm
import json
import omero
import logging
import pandas as pd
import dask
import dask.bag as db
from dask.distributed import Client, progress
import contextlib
import dask.dataframe as dd
from functools import partial
import idr
from functools import wraps

logger = logging.basicConfig(
    filename="app.log",
    filemode="w",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


data_dir = "data"


meta = {
    "screen_id": int,  # Assuming screen IDs are integers
    "z": int,  # Assuming 'z' is an integer
    "c": int,  # Assuming 'c' is an integer
    "t": int,  # Assuming 't' is an integer
    "image_id": int,  # Assuming image IDs are integers
    "well_sample": int,  # Assuming well sample IDs are integers
    "well_id": int,  # Assuming well IDs are integers
    "plate_id": int,  # Assuming plate IDs are integers
    "screen_name": str,  # Assuming screen names are strings
}


def download_image_cached(image_id, z, c, t, path, *args, **kwargs):
    if os.exists(path):
        return True
    conn = connection("idr.openmicroscopy.org", verbose=-1)
    try:
        download_image(
            conn,
            image_id,
            path,
            int(z),
            int(c),
            int(t),
        )
        result = f"Success: {image_id}"
    except Exception as e:
        result = f"Error: {image_id} - {str(e)}"
    finally:
        conn._closeSession()
    return result


def get_image_ids_from_screen(screen_name):
    from idr import connection
    conn = connection("idr.openmicroscopy.org", verbose=-1)
    image_ids = get_image_ids_from_screen_unconnected(screen_name, conn)
    conn._closeSession()
    return image_ids


def get_image_ids_from_screen_unconnected(screen_name, conn):
    # image_ids = get_image_ids
    screen = conn.getObject("Screen", attributes={"name": screen_name})
    # screens = conn.getObject('Screen', attributes={'name': wildcards.screen_name})
    print("Getting plates")
    plates = idr_funs.get_omero_children(screen)
    print("Getting wells")
    wells = idr_funs.get_children_from_list_of_objects(plates)
    print("Getting imageids")
    well_sampler = idr_funs.get_children_from_list_of_objects(wells)
    # print("Getting plates")
    image_ids = [x.image().id for x in well_sampler]
    print("generate_image_ids: Done")
    return image_ids


def get_image_ids_from_screen_dask(screen_name):
    conn = connection("idr.openmicroscopy.org", verbose=-1)
    image_ids = get_image_ids_from_screen_dask_unconnected(screen_name, conn=conn)
    conn._closeSession()
    return image_ids


def well_id_to_image_ids(well_id):
    conn = connection("idr.openmicroscopy.org", verbose=-1)
    image_ids = well_id_to_image_ids_unconnected(well_id, conn=conn)
    conn._closeSession()
    return image_ids


def well_id_to_image_ids_unconnected(well_id, conn):
    well = conn.getObject("Well", well_id)
    # Iterate over all WellSamples within the Well
    image_ids = []
    try:
        for well_sample in well.listChildren():
            image = well_sample.getImage()
            if image:
                image_ids.append(image.getId())
    except Exception as e:
        print(f"Error processing WellSamples for Well ID {well_id}: {str(e)}")
        return image_ids.append(None)

    return image_ids


def get_image_ids_from_screen_dask_unconnected(screen_name, conn):
    screen = conn.getObject("Screen", attributes={"name": screen_name})
    screen_id = db.from_sequence([screen.getId()])
    # screen_id = screen_id.repartition(256)
    plate_ids = screen_id.map(idr_funs.get_omero_children_id, field="Screen").flatten()
    wells_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate").flatten()
    image_ids = db.from_delayed(wells_ids).map(well_id_to_image_ids)
    return image_ids


def get_image_ids_from_screen_dask_unconnected_bag(screen_name, conn):
    screen = conn.getObject("Screen", attributes={"name": screen_name})
    screen_id = db.from_sequence([screen.getId()])
    plate_ids = screen_id.map(idr_funs.get_omero_children_id, field="Screen").flatten()
    wells_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate").flatten()
    well_ids = well_ids.repartition(npartitions=1)
    image_ids = wells_ids.map(well_id_to_image_ids)
    return image_ids


def get_hcs():
    # return ["idr0001-graml-sysgro/screenA", "idr0002-heriche-condensation/screenA"]
    conn = connection("idr.openmicroscopy.org", verbose=0)
    filtered_screens = get_hcs_unconnected(conn)
    conn.close()
    return filtered_screens


def get_hcs_unconnected(conn):
    screens = conn.getObjects("Screen")
    filtered_screens = []
    for screen in screens:
        # Get all the map annotation associated with the screen
        map_anns = [
            ann
            for ann in screen.listAnnotations()
            if isinstance(ann, omero.gateway.MapAnnotationWrapper)
        ]
        # Filter based on the map annotation values
        for ann in map_anns:
            values = dict(ann.getValue())
            if (
                values.get("Organism") == "Homo sapiens"
                and values.get("Study Type") == "high content screen"
            ):
                filtered_screens.append(screen.name)
    return filtered_screens


def get_metadata(image_id):
    try:
        conn = connection("idr.openmicroscopy.org", verbose=0)
        metadata = get_metadata_connected(image_id, conn)
        conn._closeSession()
    except:
        metadata = {}
    return metadata


def get_metadata_connected(image_id, conn) -> dict:
    image = conn.getObject("Image", image_id)
    well_sample = image.getParent()
    well = well_sample.getParent()
    plate = well.getParent()
    screen = plate.getParent()

    return {
        "screen_id": screen.getName(),
        "z": image.getSizeZ(),
        "c": image.getSizeC(),
        "t": image.getSizeT(),
        "image_id": image.getId(),
        "well_sample": well_sample.getId(),
        "well_id": well.getId(),
        "plate_id": plate.getId(),
        "screen_id": screen.getId(),
        "screen_name": screen.getName(),
    }


def populate_csv(csv="hcs.csv"):
    filtered_screens = get_hcs()
    image_ids = []
    rows = []
    try:
        df = dd.read_csv(csv, assume_missing=True)
    except:
        # df = pd.DataFrame()
        df = dd.from_pandas(pd.DataFrame(), npartitions=1)

    for screen in filtered_screens:
        ids = get_image_ids_from_screen(screen)
        image_ids.extend(ids)
        for image_id in image_ids:
            metadata = get_metadata(image_id, screen)
            metadata["image_id"] = image_id
            row = dd.from_pandas(pd.Series(metadata), npartitions=1)
            # row.set_index('image_id', sorted=False)
            rows.append(row)
    df = dd.concat(rows, axis=0)
    return df


# @delayed()
def screen_to_id(screen_name, field=None):
    conn = connection("idr.openmicroscopy.org", verbose=0)
    screen = conn.getObject("Screen", attributes={"name": screen_name})
    conn._closeSession()
    if screen:
        return screen.getId()
    else:
        return None



def dynamically_repartition(df, func, field=None):
    result = df.to_bag().map(func, field=field).flatten()
    df = result.to_dataframe()
    # df = df.repartition(npartitions=int(df.shape[0].compute()))
    future = client.persist(df)
    progress(future)
    return df


def dynamic_repartion(bag):
    bag = client.persist(bag)
    progress(bag)
    npartitions = bag.count().compute()
    bag = bag.repartition(npartitions=npartitions)
    return bag



def populate_csv_dask(csv="hcs.csv"):
    # try:
    #     df = dd.read_csv(csv, assume_missing=True)
    # except:

    # filtered_screens = db.from_delayed(delayed(get_hcs)())
    filtered_screens = delayed(get_hcs)()

    screens = db.from_delayed(filtered_screens)
    screens = cache_bag(screens, "screens.csv")
    screens = dynamic_repartion(screens)
    screens.to_dataframe().to_csv("screens.csv")

    screen_ids = screens.map(screen_to_id)
    screen_ids = cache_bag(screen_ids, "screen_ids.csv")
    screen_ids = dynamic_repartion(screen_ids)
    screen_ids.to_dataframe().to_csv("screen_ids.csv")

    plate_ids = screen_ids.map(idr_funs.get_omero_children_id, field="Screen").flatten()
    plate_ids = cache_bag(plate_ids, "plate_ids.csv")
    plate_ids = dynamic_repartion(plate_ids)
    plate_ids.to_dataframe().to_csv("plate_ids.csv")

    wells_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate").flatten()
    wells_ids = cache_bag(wells_ids, "wells_ids.csv")
    wells_ids = dynamic_repartion(wells_ids)
    wells_ids.to_dataframe().to_csv("wells_ids.csv")

    ids = wells_ids.map(well_id_to_image_ids)
    ids = cache_bag(ids, "ids.csv")
    ids = dynamic_repartion(ids)
    ids.to_dataframe().to_csv("ids.csv")

    ids.take(1)

    metadata = ids.map(get_metadata)
    metadata = cache_bag(metadata, "metadata.csv")
    metadata = dynamically_repartition(ids, get_metadata)
    metadata.to_dataframe().to_csv("metadata.csv")



    df = metadata.to_dataframe(meta=meta)



def process():
    df = populate_csv_dask()
    for row in df:
        path = f"{data_dir}/{row.image_id}_t-{row.t}_c-{row.c}_z-{row.z}"
        try:
            download_image_cached(**row, path=path)
            df["path"] = path
        except:
            print(f"Failed on {row.image_id}")



@delayed(pure=True)
def to_df(x, path):
    print(f"Caching to {path}")
    db.from_sequence(x).to_dataframe().to_csv(path, single_file=True)
    print("Success")
    return x


@delayed(pure=True)
def bag_to_df(bag, path):
    print(f"Caching to {path}")
    bag.to_dataframe().to_csv(path, single_file=True)
    print("Success")
    return bag


@delayed()
def cache_bag(file):

    print(f"Caching from {file}")
    df = dd.read_csv(file).iloc[:, 1]
    df = df.repartition(npartitions=1)
    x = df.to_bag().compute()

    return x


@delayed(pure=True)
def save_bag(x, file):
    bag = db.from_sequence(x)
    bag.to_dataframe().to_csv(file, single_file=True)
    print(f"Caching to {file}")
    return x


def get_screens():
    filtered_screens = delayed(get_hcs)()
    screens = db.from_delayed(filtered_screens)
    return screens


async def get_screen_ids(screens):
    screen_ids = screens.map(screen_to_id)
    return screen_ids


async def get_plate_ids(screen_ids):
    screen_ids = db.from_sequence(screen_ids)
    plate_ids = screen_ids.map(idr_funs.get_omero_children_id, field="Screen")
    plate_ids = plate_ids.flatten()
    return plate_ids


async def get_well_ids(plate_ids):
    plate_ids = db.from_sequence(plate_ids)
    well_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate")
    well_ids = well_ids.flatten()
    return well_ids


async def image_ids(well_ids):
    well_ids = db.from_sequence(well_ids)
    ids = well_ids.map(well_id_to_image_ids)
    ids = ids.flatten()
    return ids


async def metadata(ids):
    ids = db.from_sequence(ids)
    metadata = ids.map(get_metadata)
    return metadata

async def get_client():
    cluster = LocalCluster(
        memory_limit="32GB", threads_per_worker=128, asynchronous=True
    )
    client = Client(cluster)
    return client

def read_csv(filename):
    return pd.read_csv(filename).iloc[:,1].to_list()
    return list(dd.read_csv(filename).iloc[:, 1].to_bag())


def write_csv(x, file):
    if x != []:
        # print("Writing to cache")
        db.from_sequence(x).repartition(1).to_dataframe().to_csv(file, single_file=True)
        # x = read_csv(file)
    return x


def cache(file, bag, client):
    if os.path.isfile(file):
        print("Reading from cache")
        x = delayed(read_csv)(file)
    if not os.path.isfile(file):
        print(f"Processing {file}")
        x = delayed(write_csv)(bag,file)
    x = client.persist(x)
    x = client.compute(x).result()
    return db.from_sequence(x)


async def a_write_csv(file, bag, client):
    x = write_csv(bag, file)
    x = await client.compute(x)
    return db.from_sequence(x)


async def a_read_csv(file, col, bag, client):
    x = read_csv(bag, file, col)
    x = await client.compute(x)
    return db.from_sequence(x)



# The sleep and retry is to handle the case where the file is being written to disk and the read_csv function is called before the file is written, causing an error, race condition
@delayed
def dask_read_csv(filename: str):
    for _ in range(10):
        try:
            return read_csv(filename)
        except:
            time.sleep(0.01)
    raise Exception("Failed to read file")

# @delayed
# def delayed_cache(result,filename):
#     if not os.path.isfile(filename):
#         return delayed(write_csv)(result, filename)
#     else:
#         return delayed(read_csv)(filename)
#     return result

def cache_to_disk(folder):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            filename = f"{folder}/{args[0]}.csv"
            result = delayed(func)(*args, **kwargs)
            if not os.path.isfile(filename):
                result = delayed(func)(*args, **kwargs)
                result = delayed(write_csv)(result, filename)
            else:
                result = dask_read_csv(filename)
            return db.from_delayed(result)
        
        return wrapper
    return decorator


@cache_to_disk("cache/screen_id.csvs")
def screen2screen_id(screen):
    return [screen_to_id(screen)]


@cache_to_disk("cache/plate_id.csvs")
def screen2plate(screen_id):
    return idr_funs.get_omero_children_id(screen_id, field="Screen")


@cache_to_disk("cache/well.csvs")
def plate2well(plate):
    return idr_funs.get_omero_children_id(plate, field="Plate")

@cache_to_disk("cache/image.csvs")
def well2image(well):
    return well_id_to_image_ids(well)

@cache_to_disk("cache/metadata.csvs")
def image2metadata(image_id):
    return [get_metadata(image_id)]


async def a_process():
    if "SLURM_JOB_ID" in os.environ:
        cluster = SLURMCluster(
            memory="16GB", walltime="24:00:00", cores=32, asynchronous=False
        )
        cluster.scale(64)
    else:
        cluster = LocalCluster(
            memory_limit="32GB", threads_per_worker=32, asynchronous=True
        )
    print(cluster)

    async with Client(cluster, asynchronous=True) as client:
        print(client)
        screens = await get_screens()
        screens = await client.compute(screens, on_error="skip")
        screens = db.from_sequence(screens)
        
        print("screens_ids")
        screen_ids = screens.map(screen2screen_id)
        screen_ids = screen_ids.flatten()
        screen_ids = await cache("cache/screen_ids.csv", screen_ids, client)

        print("plate_ids")
        plate_ids = screen_ids.map(screen2plate)
        plate_ids = plate_ids.flatten()
        plate_ids = await cache("cache/plate_ids.csv", plate_ids, client)

        print("well_ids")
        well_ids = plate_ids.map(plate2well)
        well_ids = well_ids.flatten()
        well_ids = await cache("cache/well_ids.csv", well_ids, client)

        print("ids")
        ids = well_ids.map(well2image)
        ids = ids.flatten()
        ids = await cache("cache/ids.csv", ids, client)

        print("metadata")
        metadata = ids.map(image2metadata)
        metadata = metadata.flatten()
        metadata = await cache("cache/metadata.csv", metadata, client)
        result = metadata

    return result



def process():
    if "SLURM_JOB_ID" in os.environ:
        cluster = SLURMCluster(
            memory="16GB", walltime="24:00:00", cores=32,
        )
        cluster.scale(64)
    else:
        cluster = LocalCluster(
            memory_limit="32GB", threads_per_worker=32,
        )
    print(cluster)

    with Client(cluster) as client:
        print(client)
        screens = get_screens()
        screens = client.persist(screens)
        screens = db.from_sequence(screens)
        
        print("screens_ids")
        screen_ids = screens.map(screen2screen_id)
        screen_ids = screen_ids.flatten()
        screen_ids = cache("cache/screen_ids.csv", screen_ids, client)

        print("plate_ids")
        plate_ids = screen_ids.map(screen2plate)
        plate_ids = plate_ids.flatten()
        plate_ids = cache("cache/plate_ids.csv", plate_ids, client)

        print("well_ids")
        well_ids = plate_ids.map(plate2well)
        well_ids = well_ids.flatten()
        well_ids = cache("cache/well_ids.csv", well_ids, client)

        print("ids")
        ids = well_ids.map(well2image)
        ids = ids.flatten()
        ids = cache("cache/ids.csv", ids, client)

        print("metadata")
        metadata = ids.map(image2metadata)
        metadata = metadata.flatten()
        metadata = cache("cache/metadata.csv", metadata, client)
        result = metadata

    return result

if __name__ == "__main__":
    
    result = process()
    # result = asyncio.run(process())
    print(result)
