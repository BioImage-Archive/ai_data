# Import necessary modules
import omero
import os
import json 
import idr_funs 
from idr import connection
conn = connection("idr.openmicroscopy.org")
# image_ids = get_image_ids
screen = conn.getObject('Screen', attributes={'name': snakemake.wildcards.screen_name})
# screens = conn.getObject('Screen', attributes={'name': wildcards.screen_name})
print("Getting plates")
plates = idr_funs.get_omero_children(screen)
print("Getting wells")
wells = idr_funs.get_children_from_list_of_objects(plates)
print("Getting imageids")
well_sampler = idr_funs.get_children_from_list_of_objects(wells)
# print("Getting plates")
image_ids = [x.image().id for x in well_sampler]
with open(snakemake.output.image_ids, "w") as f:
    json.dump(image_ids, f)
# with open(output.image_ids, "w") as f:
#     f.write("\n".join(str(x) for x in image_ids))
print("generate_image_ids: Done")
conn._closeSession()