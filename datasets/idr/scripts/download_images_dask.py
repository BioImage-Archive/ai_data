from idr_funs import download_image
from idr import connection

# Import necessary modules

from dask import delayed, compute
from dask.distributed import Client, progress
import os
from tqdm import tqdm
import json
import omero


# Delayed function for downloading images
@delayed
def download_image_delayed(image_id, z, c, t, output_path):
    conn = connection("idr.openmicroscopy.org")
    try:
        download_image(
            conn,
            image_id,
            output_path,
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


def download_images_dask(image_ids, metadata):
    conn = connection("idr.openmicroscopy.org", verbose=0)
    tasks = []
    for image_id in image_ids:
        if image_id in metadata:
            zct = metadata[image_id]
            task = download_image_delayed(
                image_id,
                z=range(zct["z"]),
                c=range(zct["c"]),
                t=range(zct["t"]),
            )
            tasks.append(task)
    conn.close()
    conn._closeSession()

    # Use Dask to compute tasks in parallel
    client = Client("127.0.0.1:8786")
    with progress.ProgressBar():
        results = compute(*tasks, scheduler="distributed")

    return results


def download_images_sm(snakemake):
    with open(snakemake.input.image_ids, "r") as f:
        image_ids = json.load(f)
    with open(snakemake.input.metadata, "r") as f:
        metadata = json.load(f)

    return download_images_dask(image_ids, metadata)


download_images_sm(snakemake)
# Placeholder for a function that processes or downloads data based on file paths
# @delayed
# def download_image_delayed(image_id, zct):
#     # Actual logic to process or download the file should go here
#     # print(f"Processing {image_id}")
#     try:
#         conn = connection("idr.openmicroscopy.org")
#         download_image(
#             image_id=image_id,
#             conn=conn,
#             z=range(zct["z"]),
#             c=range(zct["c"]),
#             t=range(zct["t"]),
#         )
#         conn._closeSession()
#         # Return some result or confirmation
#     except:

#         return False
#     return True
