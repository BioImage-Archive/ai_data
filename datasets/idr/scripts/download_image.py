from idr_funs import download_image
from idr import connection
# Import necessary modules
import omero
import os
from tqdm import tqdm
import json
conn = connection("idr.openmicroscopy.org")
download_image(
    conn,
    snakemake.wildcards.image_id,
    snakemake.output.tiff,
    int(snakemake.wildcards.z),
    int(snakemake.wildcards.c),
    int(snakemake.wildcards.t),
)
conn._closeSession()