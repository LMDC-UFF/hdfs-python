import sys
sys.path.append('../')
from hdfs import HDFSWrapper

# Os testes estan feitos utilizando a imagen do docker do  Pdf-Extractor
if __name__=="__main__":
    hdfs_name_services = "Namenode1" 
    user = "hdfs"
    hdfs_replication ="1" 
    hdfs_host_services = "sandbox-hdp.hortonworks.com:8020"
    
    my_client = HDFSWrapper.hdfs_connect_withoutlogin(hdfs_name_services, user, hdfs_replication, hdfs_host_services)
    print("Status conection:",my_client.getClient())
    
    p, res = my_client.download("/DataLakeFiles/Files/PDFMANGO/2_10.pdf_paper_6.pdf", "/SSD_inference/Docker")
    print("Download:", res.erro, res.success_msg)

    res = my_client.upload("/SSD_inference/README.md", "/DataLakeFiles/Files/PDFMANGO/")
    print("Upload:", res.erro, res.success_msg)
    print("finish.")