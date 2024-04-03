from idr import connection

# Import necessary modules
import omero
import os
from tqdm import tqdm
import json


import dask
from dask import delayed, compute
import dask.bag as db
from tqdm import tqdm
from dask.diagnostics import ProgressBar
from dask.distributed import Client  # Import the Dask Distributed Client


def get_metadata(image_id, screen_name, conn):
    conn = connection("idr.openmicroscopy.org", verbose=0)
    metadata = get_metadata_connected(image_id, screen_name, conn)
    conn._closeSession()
    return metadata


def get_metadata_connected(image_id,screen_name,conn):
    image = conn.getObject("Image", image_id)
    z = image.getSizeZ()
    c = image.getSizeC()
    t = image.getSizeT()
    image_id = int(image_id)
    metadata = {
        "screen_id": screen_name,
        "z": z,
        "c": c,
        "t": t,
    }
    return metadata

# def get_metadata_api(image_ids, screen_name,conn):
#     images = conn.getObjects("Image",image_ids)
#     for image in images:


def get_metadata_unconnected(image_id, screen_name):
    conn = connection("idr.openmicroscopy.org", verbose=0)
    metatdata = get_metadata_connected(image_id,screen_name,conn)
    conn._closeSession()
    return metatdata


def get_all_metadata(screen_name, image_ids,conn):
    # breakpoint()
    get_metadata_delayed = delayed(get_metadata_connected)
    # get_metadata_delayed = delayed(get_metadata_unconnected)
    tasks = [get_metadata_delayed(image_id, screen_name, conn) for image_id in image_ids]

    # Use ProgressBar with Dask to compute tasks in parallel and show progress
    client = Client("127.0.0.1:8786")
 
    with ProgressBar():
        results = compute(*tasks, scheduler="distributed")

    # Organize results into a dictionary
    metadata = {str(image_id): result for image_id, result in zip(image_ids, results)}
    return metadata



def get_metadata_sm(snakemake):
    conn = connection("idr.openmicroscopy.org")
    screen_name = snakemake.wildcards.screen_name
    with open(snakemake.input.image_ids, "r") as f:
        image_ids = json.load(f)
    metadata = get_all_metadata(screen_name, image_ids,conn)
    conn._closeSession()
    with open(snakemake.output.metadata, "w") as f:
        json.dump(metadata, f)
    print("get_image_metadata: Done")

# if __name__ == "main":
get_metadata_sm(snakemake)
