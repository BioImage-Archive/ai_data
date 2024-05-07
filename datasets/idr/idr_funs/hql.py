import dask
import dask.bag as db
from dask.distributed import Client, progress
from omero.gateway import BlitzGateway
import os

# Assume connection details and functions are correctly set up
def get_image_ids(conn, query, params):
    """Fetch image IDs using OMERO query."""
    results = conn.getQueryService().projection(query, params)
    return [r[0]._val for r in results] if results else []

def download_image(conn, image_id, download_path):
    """Download an image by ID using OMERO tools."""
    # Placeholder for the actual download function
    # Implement downloading logic here
    pass

def batch_download_images(image_ids, batch_size=1024):
    """Download images in batches."""
    for i in range(0, len(image_ids), batch_size):
        batch = image_ids[i:i + batch_size]
        yield db.from_sequence(batch).map(download_image, conn=conn, download_path='path/to/save')

# Define the main function to be run with Dask
def main():
    client = Client()
    
    # Connection to OMERO
    conn = connection("idr.openmicroscopy.org")
    conn.connect()
    
    # Define the query and parameters
    query = """
            SELECT img.id FROM Image img
            JOIN img.wellSamples ws
            JOIN ws.well w
            JOIN w.plate p
            JOIN p.screenLinks sl
            JOIN sl.parent s
            JOIN s.annotationLinks sal
            JOIN sal.child mapAnn
            JOIN mapAnn.mapValue mv1 
            JOIN mapAnn.mapValue mv2 
            WHERE mv1.name = 'Organism' AND mv1.value = 'Homo sapiens'
            AND mv2.name = 'Study Type' AND mv2.value = 'high content screen'
            """
    params = omero.sys.ParametersI()
    params.addString('organism', 'Homo sapiens')
    params.addString('study_type', 'high content screen')
    conn.getQueryService().projection(query, params)
    # Fetch image IDs
    image_ids = get_image_ids(conn, query, params)
    
    # Create a Dask bag for batch processing
    image_batches = batch_download_images(image_ids)
    results = list(image_batches)  # Execute batch downloads

    # Wait for all downloads to complete and monitor the progress
    progress(results)

    # Close the connection
    conn.close()

    # Shutdown Dask client
    client.close()




def get_image_ids(conn, query, params):
    """Fetch image IDs using OMERO query."""
    results = conn.getQueryService().projection(query, params)
    return [r[0]._val for r in results] if results else []

def download_image(conn, image_id, download_path):
    """Download an image by ID using OMERO tools."""
    # Placeholder for the actual download function
    # Implement downloading logic here
    pass

def download_homo_sapiens_images(conn, download_path, batch_size=1024):
    """Download a random sample of Homo sapiens images from high-content screens."""
    # Define the query and parameters
    query = """
            SELECT img.id FROM Image img
            JOIN img.wellSamples ws
            JOIN ws.well w
            JOIN w.plate p
            JOIN p.screenLinks sl
            JOIN sl.parent s
            JOIN s.annotationLinks sal
            JOIN sal.child mapAnn
            JOIN mapAnn.mapValue mv1 
            JOIN mapAnn.mapValue mv2 
            WHERE mv1.name = 'Organism' AND mv1.value = 'Homo sapiens'
            AND mv2.name = 'Study Type' AND mv2.value = 'high content screen'
            """
    params = omero.sys.ParametersI()
    params.addString('organism', 'Homo sapiens')
    params.addString('study_type', 'high content screen')

    # Fetch image IDs based on the query
    image_ids = get_image_ids(conn, query, params)
    return image_ids

def setup_connection_and_download(host, username, password, download_path):
    """Sets up the connection to the OMERO server and initiates image download."""
    conn = BlitzGateway(host=host, username=username, passwd=password, secure=True)
    conn.connect()

    # Initiate the download process
    download_homo_sapiens_images(conn, download_path)

    # Clean up: close the connection after operations
    conn.close()
