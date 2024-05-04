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

cache = Cache(2e9)
cache.register()

# from scripts.get_image_metadata
# Import necessary modules
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


from dask import compute


def get_image_ids_from_screen_dask_unconnected(screen_name, conn):
    screen = conn.getObject("Screen", attributes={"name": screen_name})
    screen_id = db.from_sequence([screen.getId()])
    # screen_id = screen_id.repartition(256)
    plate_ids = screen_id.map(idr_funs.get_omero_children_id, field="Screen").flatten()
    wells_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate").flatten()
    image_ids = db.from_delayed(wells_ids).map(well_id_to_image_ids)
    return image_ids


# def get_screen_ids(scree_name):


def get_image_ids_from_screen_dask_unconnected_bag(screen_name, conn):
    screen = conn.getObject("Screen", attributes={"name": screen_name})
    screen_id = db.from_sequence([screen.getId()])
    plate_ids = screen_id.map(idr_funs.get_omero_children_id, field="Screen").flatten()
    wells_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate").flatten()
    well_ids = well_ids.repartition(npartitions=1)
    image_ids = wells_ids.map(well_id_to_image_ids)
    return image_ids


def get_hcs():
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


from functools import partial


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


# def cache_bag(bag, file):
#     try:
#         df = dd.read_csv(file)
#         return df.to_bag()
#     except:
#         # df = bag.to_dataframe()
#         # df.to_csv(file)
#         return bag


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

    # with dask.config.set(scheduler="sync", delayed_optimize=__cached__):
    # full = client.persist(metadata)
    # progress(full)

    df = metadata.to_dataframe(meta=meta)
    # df = client.persist(df)
    # progress(df)
    # df.to_csv(csv, index=False,).compute()
    # progress(df.to_csv(csv, index=False))
    # return df


def process():
    df = populate_csv_dask()
    # sub_df = df.iloc[0:1000]
    for row in df:
        path = f"{data_dir}/{row.image_id}_t-{row.t}_c-{row.c}_z-{row.z}"
        try:
            download_image_cached(**row, path=path)
            df["path"] = path
        except:
            print(f"Failed on {row.image_id}")


# async def get_hcs():
#     return ["a"]


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
    # try:
    #     assert os.path.isfile(file)
    print(f"Caching from {file}")
    df = dd.read_csv(file).iloc[:, 1]
    df = df.repartition(npartitions=1)
    x = df.to_bag().compute()
    # return x
    # x = df.to_dask_array(lengths=True)
    # print(f"Success")
    # except:
    # bag = db.from_sequence(x)
    # bag.to_dataframe().to_csv(file, single_file=True)
    # print(f"Caching to {file}")
    return x


@delayed(pure=True)
def save_bag(x, file):
    bag = db.from_sequence(x)
    bag.to_dataframe().to_csv(file, single_file=True)
    print(f"Caching to {file}")
    return x


async def get_screens():
    filtered_screens = delayed(get_hcs)()

    screens = db.from_delayed(filtered_screens)
    # screens = await client.compute(screens)
    return screens


async def get_screen_ids(screens):
    # screens = db.from_sequence(screens)
    screen_ids = screens.map(screen_to_id)
    # screen_ids = await client.compute(screen_ids)
    return screen_ids


async def get_plate_ids(screen_ids):
    screen_ids = db.from_sequence(screen_ids)
    plate_ids = screen_ids.map(idr_funs.get_omero_children_id, field="Screen")
    plate_ids = plate_ids.flatten()
    # plate_ids = await client.compute(plate_ids)
    return plate_ids


async def get_well_ids(plate_ids):
    plate_ids = db.from_sequence(plate_ids)
    well_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate")
    well_ids = well_ids.flatten()
    # well_ids = await client.compute(well_ids)
    return well_ids


async def image_ids(well_ids):
    well_ids = db.from_sequence(well_ids)
    ids = well_ids.map(well_id_to_image_ids)
    ids = ids.flatten()
    # ids = await client.compute(ids)
    return ids


async def metadata(ids):
    ids = db.from_sequence(ids)
    metadata = ids.map(get_metadata)
    # metadata = await client.compute(metadata)
    return metadata


# async def a_process(cluster):
#     # print("OK")
#     print(cluster)
#     async with Client(cluster, asynchronous=True) as client:
#         # with client
#         print(client)
#         filtered_screens = delayed(get_hcs)()
#         # filtered_screens = delayed(lambda: [1, 2, 3])()
#         print("Screens")

#         screens = db.from_delayed(filtered_screens)
#         screens = await client.persist(screens)

#         # if os.path.isfile("screens.csv"):
#         #     screens = cache_bag("screens.csv")
#         # else:
#         # save_bag(screens, "screens.csv")

#         # screens = await client.compute(screens)
#         # screens = db.from_sequence(screens)

#         screen_ids = screens.map(screen_to_id)
#         print("screen_ids")
#         screen_ids = await client.persist(screen_ids)
#         # progress(screen_ids)
#         # screen_ids = screen_ids.repartition(1)
#         # if os.path.isfile("screen_ids.csv"):
#         #     screen_ids = cache_bag("screen_ids.csv")
#         # else:
#         # screen_ids = save_bag(screen_ids, "screen_ids.csv")

#         # screen_ids = await client.compute(screen_ids)
#         # screen_ids = db.from_sequence(screen_ids)

#         print("plate_ids")
#         plate_ids = screen_ids.map(idr_funs.get_omero_children_id, field="Screen")
#         # plate_ids = plate_ids.repartition(1)
#         plate_ids = plate_ids.flatten()
#         plate_ids = await client.persist(plate_ids)
#         # progress(plate_ids)
#         # if os.path.isfile("plate_ids.csv"):
#         #     plate_ids = cache_bag("plate_ids.csv")
#         # else:
#         # plate_ids = save_bag(plate_ids, "plate_ids.csv")
#         # plate_ids = await client.compute(plate_ids)
#         # plate_ids = db.from_sequence(plate_ids)

#         print("well_ids")
#         well_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate")
#         well_ids = well_ids.flatten()
#         well_ids = client.persist(well_ids)
#         # progress(well_ids)

#         # if os.path.isfile("well_ids.csv"):
#         #     well_ids = cache_bag("well_ids.csv")
#         # else:
#         # save_bag(well_ids, "well_ids.csv")
#         #     # cache = well_ids

#         # well_ids = await client.compute(well_ids)
#         # well_ids = db.from_sequence(well_ids)
#         print("image_ids")
#         ids = well_ids.map(well_id_to_image_ids)
#         # ids = ids.repartition(1)
#         ids = ids.flatten()
#         ids = client.persist(ids)

#         # if os.path.isfile("ids.csv"):
#         #     ids = cache_bag("ids.csv")
#         # else:
#         #     save_bag(ids, "ids.csv")
#         # ids = await client.compute(ids)
#         # ids = db.from_sequence(ids)

#         metadata = ids.map(get_metadata)
#         metadata = client.persist(metadata)

#         # metadata = metadata.repartition(1)

#         # if os.path.isfile("metadata.csv"):
#         #     metadata = cache_bag("metadata.csv")
#         # else:
#         #     metadata = save_bag(metadata, "metadata.csv")

#         # metadata.take(1)
#         # metadata = await client.compute(metadata)
#         # metadata = db.from_sequence(metadata)
#         # breakpoint()

#         metadata = client.compute(metadata)

#         progress(metadata)
#         metadata = await metadata
#     return metadata


async def get_client():
    # try:
    #     cluster = await SLURMCluster(
    #         memory="16GB", walltime="12:00:00", cores=1, asynchronous=True
    #     )
    #     cluster.scale(8)
    # except:
    cluster = LocalCluster(
        memory_limit="32GB", threads_per_worker=32, asynchronous=True
    )
    client = Client(cluster)
    return client


# # @delayed
# async def cache(data, file):
#     if not os.path.isfile(file):
#         db.from_sequence(data).to_dataframe().to_csv(file)
#         return data
#     else:
#         return dd.read_csv(file).to_bag().compute()


# @delayed
# def read_csv(file):
#     return dd.read_csv(file).iloc[:, 1].to_bag()


@delayed
def read_csv(x, file, col):
    bag = db.from_sequence(x)
    bag = x
    # file = name + ".csv"

    if os.path.exists(file + "/"):
        cache_bag = dd.read_csv(file + "/*")[col].to_bag()
        diff = db.from_sequence(set(x) - set(cache_bag))
        bag = diff
        bag = list(set(x) - set(cache_bag))
    return bag
    return dd.read_csv(file).iloc[:, 1].to_bag()


@delayed
def write_csv(x, file):
    if x != []:
        # TODO Check logic
        bag = db.from_sequence(x)
        bag.to_dataframe().to_csv(file)

    return list(dd.read_csv(file + "/*")[file].to_bag())

    # bag = db.from_sequence(x)
    # bag.to_dataframe().to_csv(file)
    # return bag


# async def cache(file, bag, client):
#     if os.path.isfile(file):
#         bag = read_csv(file + "*.csv")
#     if not os.path.isfile(file):
#         bag = write_csv(bag, file)
#     return await client.compute(bag)


async def cache(file, bag, client):
    # if os.path.isfile(file):
    bag = read_csv(bag, file)
    # if not os.path.isfile(file):
    bag = write_csv(bag, file)
    return await client.compute(bag)


async def a_write_csv(file, bag, client):
    x = write_csv(bag, file)
    x = await client.compute(x)
    return db.from_sequence(x)


async def a_read_csv(file, col, bag, client):
    x = read_csv(bag, file, col)
    x = await client.compute(x)
    return db.from_sequence(x)
    # return await read_csv(bag, file).compute()


# def mapper(index, index_name, var_name, fn, *args, **kwargs):
#     var = fn(index, *args, **kwargs)
#     return [{index: x} for x in var]


def screen2plate(screen):
    col = "screen_id"
    folder = "plate.csv"
    filename = f"{folder}/{screen}.csv"

    if not os.path.isfile(filename):
        x = idr_funs.get_omero_children_id(screen, field="Screen")
        # pd.DataFrame({col:x}).to_csv(filename)
        db.from_sequence(x).to_dataframe().to_csv(filename, single_file=True)
    else:
        x = dd.read_csv(filename).iloc[:, 1].to_bag().compute()
    return db.from_sequence(x)


def screen2screen_id(screen):
    col = "screen"
    folder = "screen_id.csv"
    filename = f"{folder}/{screen}.csv"

    if not os.path.isfile(filename):
        x = [screen_to_id(screen)]
        db.from_sequence(x).to_dataframe().to_csv(filename, single_file=True)
    else:
        x = dd.read_csv(filename).iloc[:, 1].to_bag().compute()
    return db.from_sequence(x)


def plate2well(plate):

    col = "plate_id"
    folder = "well.csv"
    filename = f"{folder}/{plate}.csv"

    if not os.path.isfile(filename):
        x = idr_funs.get_omero_children_id(plate, field="Plate")
        # pd.DataFrame({col:x}).to_csv(filename)
        db.from_sequence(x).to_dataframe().to_csv(filename, single_file=True)
    else:
        x = dd.read_csv(filename).iloc[:, 1].to_bag().compute()
    return db.from_sequence(x)


def well2image(well):
    col = "well_id"
    folder = "image.csv"
    filename = f"{folder}/{well}.csv"

    if not os.path.isfile(filename):
        x = well_id_to_image_ids(well)
        db.from_sequence(x).to_dataframe().to_csv(filename, single_file=True)
    else:
        x = dd.read_csv(filename).iloc[:, 1].to_bag().compute()
    return db.from_sequence(x)


def image2metadata(image_id):
    col = "image_id"
    folder = "metadata.csv"
    filename = f"{folder}/{image_id}.csv"

    if not os.path.isfile(filename):
        x = [get_metadata(image_id)]
        db.from_sequence(x).to_dataframe().to_csv(filename, single_file=True)
    else:
        x = dd.read_csv(filename).iloc[:, 1].to_bag().compute()
    return db.from_sequence(x)


# def cache2disk(fn):
#     @wraps(fn)
#     def inner(*args, **kwargs):
#         if os.path.isfile(id):
#             return dd.read_csv(id).to_bag().compute()
#         else:
#             return fn(*args, **kwargs)

#     return inner


async def process():

    if "SLURM_JOB_ID" in os.environ:
        cluster = SLURMCluster(
            memory="16GB", walltime="24:00:00", cores=1, asynchronous=True
        )
        # cluster.adapt(maximum_jobs=64)
        cluster.scale(64)
    else:
        cluster = LocalCluster(
            memory_limit="32GB", threads_per_worker=64, asynchronous=True
        )
    print(cluster)
    # The logic should be that the read_csv culling needs to happen before the mapping operations.
    # This means the CSVs need the screen_name and the screen_id
    #

    async with Client(cluster, asynchronous=True) as client:
        print(client)
        screens = await get_screens()
        screens = await client.compute(screens, on_error='skip')
        screens = db.from_sequence(screens)
        
        print("screens_ids")
        screen_ids = screens.map(screen2screen_id)
        screen_ids = screen_ids.flatten()
        screen_ids = await client.compute(screen_ids, on_error='skip')
        screen_ids = db.from_sequence(screen_ids)
        
        print("plate_ids")
        plate_ids = screen_ids.map(screen2plate)
        plate_ids = plate_ids.flatten()
        with ProgressBar():
            plate_ids = await client.compute(plate_ids, on_error='skip')
        plate_ids = db.from_sequence(plate_ids)
        
        print("well_ids")
        well_ids = plate_ids.map(plate2well)
        well_ids = well_ids.flatten()
        with ProgressBar():
            well_ids = await client.compute(well_ids, on_error='skip')
        well_ids = db.from_sequence(well_ids)

        print("ids")
        ids = well_ids.map(well2image)
        ids = ids.flatten()
        with ProgressBar():
            ids = await client.compute(ids, on_error='skip')
        ids = db.from_sequence()
        
        print("metadata")
        metadata = ids.map(image2metadata)
        metadata = metadata.flatten()
        result = await client.compute(metadata, on_error='skip').result()

        # print(client)
        # screens = await get_screens()
        # # screens = await a_read_csv("screen_ids", "screens", screens, client)

        # print("screens_ids")
        # screen_ids = screens.map(
        #     lambda x: {"screens": x, "screen_ids": screen_to_id(x)}
        # )
        # screen_ids = await a_write_csv("screen_ids", screen_ids, client)

        # print("plate_ids")
        # # screen_ids = await a_read_csv("plate_ids", "screen_ids", screen_ids, client)
        # print(await client.compute(screen_ids).result())
        # plate_ids = screen_ids.map(
        #     lambda x: [
        #         {"screen_ids": x, "plate_ids": var}
        #         for var in idr_funs.get_omero_children_id(x, field="Screen")
        #     ],
        # )
        # # print(await client.compute(plate_ids).result())
        # plate_ids = plate_ids.flatten()
        # # print(await client.compute(plate_ids).result())
        # plate_ids = await a_write_csv("plate_ids", plate_ids, client)

        # print("well_ids")
        # # plate_ids = await a_read_csv("well_ids", "plate_ids", well_ids, client)
        # well_ids = plate_ids.map(
        #     lambda x: [
        #         {"plate_ids": x, "well_ids": var}
        #         for var in idr_funs.get_omero_children_id(x, field="Plate")
        #     ]
        # )
        # well_ids = well_ids.flatten(compute=False)
        # well_ids = await a_write_csv("well_ids.csv", well_ids, client)

        # print("ids")
        # # well_ids = await a_read_csv("ids", "well_ids", well_ids, client)
        # ids = well_ids.map(
        #     lambda x: [{"well_ids": x, "ids": var} for var in well_id_to_image_ids(x)]
        # )
        # ids = ids.flatten()
        # ids = await a_write_csv("ids.csv", ids, client)

        # print("metadata")
        # # ids = await a_read_csv("metadata", "ids", ids, client)
        # metadata = ids.map(get_metadata)
        # metadata = await a_write_csv("metadata.csv", metadata, client)

    return result


if __name__ == "__main__":

    result = asyncio.run(process())
    print(result)
    # screens = asyncio.run(screens())
    # screen_ids = asyncio.run(screen_ids(screens))
    # plate_ids = asyncio.run(plate_ids(screen_ids))
    # well_ids = asyncio.run(well_ids(plate_ids))
    # image_ids = asyncio.run(image_ids(well_ids))
    # metadata = asyncio.run(metadata(image_ids))

    # pd.DataFrame(metadata).to_csv("metadata.csv")


# if __name__ == "__main__":
#     # client = Client()  # This will start a local scheduler and workers
#     client = Client(
#         n_workers=8,
#         threads_per_worker=64,
#     )

#     # print(client)  # This will provide the dashboard link and other details
#     process()
#     # client.close()
