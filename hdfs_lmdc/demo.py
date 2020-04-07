from hdfs import HDFSWrapper

def main():
    hdfs_name_services = "Namenode1" 
    user = "hdfs"
    hdfs_replication ="1" 
    hdfs_host_services = "sandbox-hdp.hortonworks.com:8020"
    
    my_client = HDFSWrapper.hdfs_connect_withoutlogin(hdfs_name_services, user, hdfs_replication, hdfs_host_services)
    print("Status conection:",my_client.getClient())

# Os testes estan feitos utilizando a imagen do docker do  Pdf-Extractor
if __name__=="__main__":
    main()