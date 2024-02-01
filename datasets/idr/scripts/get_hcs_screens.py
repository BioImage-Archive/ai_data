# Import necessary modules
import omero
import os
import json 
import idr_funs 
from idr import connection

screen_names = idr_funs.get_hcs()
with open(snakemake.output.screen_names, "w") as f:
    json.dump(screen_names, f)