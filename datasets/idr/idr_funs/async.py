from dask.distributed import Client, as_completed
import dask.bag as db
from dask import delayed
import pandas as pd
import dask.dataframe as dd
from idr import connection
import omero
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dask.distributed import Client, as_completed
from dask import delayed
import dask.bag as db

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

import dask.dataframe as dd
import graphchain
from functools import partial
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

import dask.dataframe as dd
import graphchain
from functools import partial



# Helper functions and configurations

async def get_screens_async(screen_name):
    async with connection("idr.openmicroscopy.org", verbose=0) as conn:
        screen = await conn.getObject("Screen", attributes={"name": screen_name})
        return screen.getName()

async def get_children_async(parent_id, child_type, conn):
    parent = await conn.getObject(child_type, parent_id)
    return [child.getId() for child in await parent.listChildren()]

# Function to process image metadata
async def get_metadata_async(image_id, conn):
    image = await conn.getObject("Image", image_id)
    well_sample = await image.getParent()
    well = await well_sample.getParent()
    plate = await well.getParent()
    screen = await plate.getParent()
    return {
        "screen_id": screen.getId(),
        "z": image.getSizeZ(),
        "c": image.getSizeC(),
        "t": image.getSizeT(),
        "image_id": image.getId(),
        "well_sample": well_sample.getId(),
        "well_id": well.getId(),
        "plate_id": plate.getId(),
        "screen_name": screen.getName(),
    }

# Async function to fetch screen names that meet certain criteria
async def get_hcs_async():
    async with connection("idr.openmicroscopy.org", verbose=0) as conn:
        return await get_hcs_unconnected_async(conn)

async def get_hcs_unconnected_async(conn):
    screens = await conn.getObjects("Screen")  # Assuming async support
    filtered_screens = []
    for screen in screens:
        map_anns = await screen.listAnnotations()  # Assuming this returns an async iterable
        for ann in map_anns:
            if isinstance(ann, omero.gateway.MapAnnotationWrapper):
                values = dict(ann.getValue())
                if values.get("Organism") == "Homo sapiens" and values.get("Study Type") == "high content screen":
                    filtered_screens.append(screen.name)
    return filtered_screens


async def cache_bag(bag, file):
    try:
        print("")
        df = await dd.read_csv(file)
        return df.to_bag()
    except:
        # df = bag.to_dataframe()
        # df.to_csv(file)
        return bag

async def get_screen_ids_async(screen_names, conn):
    return [await conn.getObject("Screen", attributes={"name": name}).getId() for name in screen_names]


# async def get_image_ids_from_screen_dask_unconnected(screen_name, conn):
#     screen = await conn.getObject("Screen", attributes={"name": screen_name})
#     screen_id = screen.getId()
#     plate_ids = await idr_funs.get_omero_children_id(screen_id, 'Screen', conn)
#     wells_ids = []
#     for plate_id in plate_ids:
#         wells_ids.extend(await idr_funs.get_omero_children_id(plate_id, 'Plate', conn))
    
#     image_ids = []
#     for well_id in wells_ids:
#         image_ids.extend(await well_id_to_image_ids(well_id, conn))
    
#     return image_ids


# Main pipeline function
async def process():
    screens = await get_hcs_async()
    screens = await db.from_delayed(delayed(get_screens_async)())
    screen_ids = delayed(get_screens_async)()

    # screens = await cache_bag(screens, "screens.csv")
    await screens.to_dataframe().to_csv("screens.csv")













def get_hcs():
    conn = connection("idr.openmicroscopy.org", verbose=0)
    filtered_screens = get_hcs_unconnected(conn)
    conn.close()
    return filtered_screens


executor = ThreadPoolExecutor(max_workers=10)


async def async_connection(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(pool, func, *args, **kwargs)


async def get_image_ids_from_screen_async(screen_name):
    conn = await async_connection(connection, "idr.openmicroscopy.org")
    try:
        image_ids = await async_connection(
            get_image_ids_from_screen_unconnected, screen_name, conn
        )
    finally:
        conn.close()
    return image_ids


async def download_image_async(image_id, z, c, t, path):
    if not os.path.exists(path):
        conn = await async_connection(connection, "idr.openmicroscopy.org")
        try:
            await async_connection(download_image, conn, image_id, path, z, c, t)
            result = f"Success: {image_id}"
        except Exception as e:
            result = f"Error: {image_id} - {str(e)}"
        finally:
            conn._closeSession()
    else:
        result = True
    return result


# async def main():
#     client = Client(asynchronous=True)
#     # Assume filtered_screens is a list of screen names fetched asynchronously
#     filtered_screens = await async_connection(get_hcs)
#     tasks = [
#         delayed(get_image_ids_from_screen_async)(screen) for screen in filtered_screens
#     ]
#     image_ids = await client.compute(tasks, sync=False)
#     results = [await download_image_async(*args) for args in image_ids]
#     print(results)


# if __name__ == "__main__":
#     asyncio.run(main())




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