from dask.distributed import Client, as_completed
import dask.bag as db
from dask import delayed
import pandas as pd
import dask.dataframe as dd
from idr import connection
import omero
import asyncio
import logging

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
import graphchain
from functools import partial
import idr
from functools import wraps

logger = logging.basicConfig(
    filename="app.log",
    filemode="w",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

cache = partial(graphchain.optimize, location="cache")


# def suppress_print(func):
#     @wraps(func)
#     def wrapped(*args, **kwargs):
#         with open(os.devnull, 'w') as f, contextlib.redirect_stdout(f):
#             return func(*args, **kwargs)
#     return wrapped

# # Assuming idr.connection is a function you want to wrap
# @suppress_print
# def connection(url="idr.openmicroscopy.org"):
#     return idr.connection(url)

# screen_names = get_hcs()

data_dir = "data"
# Delayed function for downloading images


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
    conn = connection("idr.openmicroscopy.org", verbose=0)
    metatdata = get_metadata_connected(image_id, conn)
    conn._closeSession()
    return metatdata


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


def cache_bag(bag, file):
    try:
        df = dd.read_csv(file)
        return df.to_bag()
    except:
        # df = bag.to_dataframe()
        # df.to_csv(file)
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


@delayed(pure=True)
def cache_bag(file):
    # try:
    #     assert os.path.isfile(file)
    print(f"Caching from {file}")
    df = dd.read_csv(file).iloc[:, 1]
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


async def a_process():
    async with Client(n_workers=1, threads_per_worker=64, asynchronous=True) as client:
        print(client)
        filtered_screens = delayed(get_hcs)()
        print("Screens")
        screens = db.from_delayed(filtered_screens)
        if os.path.isfile("screens.csv"):
            screens = cache_bag("screens.csv")
        else:
            screens = save_bag(screens, "screens.csv")

        # screens = db.from_delayed(screens)
        # screens = screens.repartition(16)
        # a = screens.take(1)
        screens = await client.compute(screens)
        # logger.info(screens[0:10])
        screens = await db.from_sequence(screens)

        # return screens
        print("screen_ids")

        screen_ids = screens.map(screen_to_id)
        # screen_ids = screen_ids.repartition(1)

        if os.path.isfile("screen_ids.csv"):
            screen_ids = cache_bag("screen_ids.csv")
        else:
            screen_ids = save_bag(plate_ids, "screen_ids.csv")

        # screen_ids = db.from_delayed(screen_ids)
        screen_ids = await client.compute(screen_ids)
        # logger.info(screen_ids[0:10])
        screen_ids = db.from_sequence(screen_ids)

        print("plate_ids")
        plate_ids = screen_ids.map(idr_funs.get_omero_children_id, field="Screen")
        plate_ids = plate_ids.repartition(1)
        plate_ids = plate_ids.flatten()
        # plate_ids = await cache_bag(plate_ids, "plate_ids.csv")

        if os.path.isfile("plate_ids.csv"):
            plate_ids = cache_bag("plate_ids.csv")
        else:
            plate_ids = save_bag(plate_ids, "plate_ids.csv")
        # plate_ids = db.from_delayed(plate_ids)
        plate_ids = await client.compute(plate_ids)
        # logger.info(plate_ids[0:10])
        plate_ids = db.from_sequence(plate_ids)

        # plate_ids = await client.compute(plate_ids)
        print("well_ids")
        well_ids = plate_ids.map(idr_funs.get_omero_children_id, field="Plate")
        well_ids = well_ids.repartition(1)
        well_ids = well_ids.flatten()
        # well_ids.take(1)

        if os.path.isfile("well_ids.csv"):
            well_ids = cache_bag("well_ids.csv")
        else:
            well_ids = save_bag(well_ids, "well_ids.csv")

        # well_ids = db.from_delayed(well_ids)
        # well_ids = well_ids.repartition(16)
        well_ids = await client.compute(well_ids)
        # logger.info(well_ids[0:10])
        well_ids = db.from_sequence(well_ids)

        # well_ids = await client.compute(well_ids)

        print("image_ids")
        ids = well_ids.map(well_id_to_image_ids)
        ids = ids.repartition(1)
        ids = ids.flatten()
        if os.path.isfile("ids.csv"):
            ids = cache_bag("ids.csv")
        else:
            ids = save_bag(ids, "ids.csv")
        ids = await client.compute(ids)
        ids = db.from_sequence(ids)
        # ids.take(1)

        ids = await client.compute(ids)

        metadata = ids.map(get_metadata)
        metadata = metadata.repartition(1)
        metadata = metadata.flatten(1)

        if os.path.isfile("metadata.csv"):
            metadata = cache_bag("metadata.csv")
        else:
            metadata = save_bag(ids, "metadata.csv")
        metadata = await client.compute(metadata)
        metadata = metadata.from_sequence(metadata)

        return metadata

        # plate_ids = screen_ids.map(
        #     idr_funs.get_omero_children_id, field="Screen"
        # ).flatten()
        # plate_ids = await cache_bag(plate_ids, "plate_ids.csv")
        # plate_ids = await to_df(plate_ids, "plate_ids.csv")
        # plate_ids = await client.compute(plate_ids)
        # plate_ids = db.from_sequence(plate_ids)

        # well_ids = plate_ids.map(
        #     idr_funs.get_omero_children_id, field="Plate"
        # ).flatten()
        # well_ids = await cache_bag(well_ids, "well_ids.csv")
        # well_ids = await to_df(well_ids, "well_ids.csv")
        # well_ids = await client.compute(well_ids)
        # well_ids = db.from_sequence(well_ids)

        # ids = well_ids.map(well_id_to_image_ids)
        # ids = await cache_bag(ids, "ids.csv")
        # ids = await to_df(ids, "ids.csv")
        # ids = await client.compute(ids)
        # ids = db.from_sequence(ids)

    return True

    #     metadata = ids.map(get_metadata)

    #     # screens = await client.compute(screens)
    #     df = await to_df(screens)
    #     df = await client.compute(df)
    #     # df = await to_df(screens)

    # return metadata


if __name__ == "__main__":
    result = asyncio.run(a_process())

    # screens = delayed(a_process)()
    # screens.compute()
    # # screens = db.from_sequence(screens)
    # screens.to_dataframe("screens.csv")

    print(result)


# if __name__ == "__main__":
#     # client = Client()  # This will start a local scheduler and workers
#     client = Client(
#         n_workers=8,
#         threads_per_worker=64,
#     )

#     # print(client)  # This will provide the dashboard link and other details
#     process()
#     # client.close()
