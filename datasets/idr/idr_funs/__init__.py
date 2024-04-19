from pathlib import Path
from idr import connection, images
from tqdm import tqdm
from PIL import Image
import omero

IDR_PATH = "idr.openmicroscopy.org"


def get_omero_children(blitz_object):
    return [child for child in blitz_object.listChildren()]


def get_children_from_list_of_objects(blitz_objects):
    children = []
    for obj in blitz_objects:
        children.extend(get_omero_children(obj))
    return children


def get_omero_children_id(omero_id, field):
    conn = connection("idr.openmicroscopy.org",verbose=-1)
    child_id = get_omero_child_id_unconnected(omero_id, field, conn)
    conn._closeSession()
    return child_id


def get_omero_id(omero_id, field, conn):
    return conn.getObject(field, omero_id)


def get_omero_child_id_unconnected(omero_id, field, conn):
    blitz_object = conn.getObject(field, omero_id)
    ids = []
    for child in blitz_object.listChildren():
        ids.append(child.getId())
    return ids


# def get_plates_by_screen(conn,screen_name):
#     # Get the screen object
#     screen = conn.getObject('Screen', attributes={'name': screen_name})

#     # Get all the image objects in the screen
#     return get_omero_children(screen)

# def get_image_ids_by_screen(conn,screen_name):
#     screen = conn.getObject('Screen', attributes={'name': screen_name})
#     plates = get_children_from_list_of_objects([screen])
#     wells = get_children_from_list_of_objects(plates)
#     image_ids = get_children_from_list_of_objects(wells)
#     # for plate in tqdm(plate):
#     #     # plate = conn.getObject('Plate', plate_id)
#     #     get_omero_children(plate)
#     #     image_ids.extend()
#     return image_ids


# screen = conn.getObject('Screen', attributes={'name': screen_name})

# Create the output directory if it doesn't exist
# Path(output_dir).mkdir(parents=True, exist_ok=True)


# Define a function to download images
def download_image(conn, image_id, output_file, z=0, c=0, t=0):
    image = conn.getObject("Image", image_id)
    pixels = image.getPrimaryPixels()
    plane = pixels.getPlane(z, c, t)
    print(f"Saving image {image_id} to {output_file}")
    im = Image.fromarray(plane)
    im.save(output_file)
    return im
    # images.download_image(conn, image_id, download_path=output_dir)


def get_image_dimensions(conn, image_id):
    image = conn.getObject("Image", image_id)
    z = image.getSizeZ()
    c = image.getSizeC()
    t = image.getSizeT()
    return z, c, t


def get_hcs(idr_path=IDR_PATH):
    # breakpoint()
    conn = connection(idr_path, verbose=0)
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
    conn.close()
    return filtered_screens


def get_metadata_unconnected(image_id, screen_name):
    conn = connection("idr.openmicroscopy.org", verbose=0)
    metatdata = get_metadata_connected(image_id, screen_name, conn)
    conn._closeSession()
    return metatdata
