from idr import connection
# Import necessary modules
import omero
import os
from tqdm import tqdm
import json 

conn = connection("idr.openmicroscopy.org")
with open(snakemake.input.image_ids, 'r') as f:
    image_ids = json.load(f)
# with open(input.image_ids) as f:
    # breakpoint()
    # image_ids = [int(x.strip()) for x in f.readlines()]
metadata = {}
for image_id in tqdm(image_ids):
    image = conn.getObject("Image", image_id)
    z = image.getSizeZ()
    c = image.getSizeC()
    t = image.getSizeT()
    metadata[str(image_id)] = {"z": z, "c": c, "t": t}

with open(snakemake.output.metadata, "w") as f:
    json.dump(metadata, f)
conn._closeSession()
print("get_image_metadata: Done")
