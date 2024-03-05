from dask.distributed import Client, LocalCluster
# This will automatically create a LocalCluster and client
# Workers are equal to the number of cores
def main():
    cluster = LocalCluster(n_workers=32, scheduler_port=8786)
    client = Client(cluster)
    print(cluster)
    print(client)
    
    # Wait for user input to close
    input("Press Enter to exit...")
    
    # Close client and cluster upon exiting
    client.close()
    cluster.close()

if __name__ == "__main__":
    main()
