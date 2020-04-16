import os
import fnmatch
import subprocess
from PIL import Image
from hdfs3 import HDFileSystem

class RequestResult:

    def __init__(self, status:bool, success_msg:str, erro:str):
        self.status =  status
        self.erro =  erro
        self.success_msg = success_msg

    @staticmethod
    def ofOk(success_msg:str = None):
        return RequestResult(True, success_msg, None)
    
    @staticmethod
    def ofError(err_msg:str = None):
        return RequestResult(False, None, err_msg)
    

class HDFSWrapper:

    def __init__(self, hdfsclient: HDFileSystem):
        self._hdfsClient  = hdfsclient
    
    def getClient(self):
        return self._hdfsClient

    def upload(self, local_path: str, hdfs_path: str)-> RequestResult:
        try:
            file_name = os.path.basename(local_path)
            self._hdfsClient.put(local_path,os.path.join(hdfs_path, file_name))
            return RequestResult.ofOk("File Uploaded")
        except:
            return RequestResult.ofError("Error upload file...! {}".format(local_path))

    def download(self, hdfs_file_path: str, local_save_path: str=None):
        try:
            if self._hdfsClient.exists(hdfs_file_path) is False:
                return None, RequestResult.ofError("File {} not exist.".format(hdfs_file_path))
            
            _, local_file_name = os.path.split(hdfs_file_path)
            local_file_name, ext = os.path.splitext(local_file_name)
            
            local_folder_path = local_save_path
            if local_save_path is None:
                local_folder_path = os.path.join((os.sep + "tmp"), local_file_name)
            
            local_file_path = os.path.join(local_folder_path, local_file_name + ext)
            os.makedirs(local_folder_path, exist_ok=True)
            self._hdfsClient.get(hdfs_file_path, local_file_path)
            
            return local_file_path, RequestResult.ofOk("File downloaded")
        except:
            return None, RequestResult.ofError("Download File {} failure.".format(hdfs_file_path))

    def read_image(self, hdfs_image_path: str):
        """Read an image from HDFS
 
        Parameters
        ---------- 
        hdfs_image_path : str
            HDFS full path, including file's name
        
        Returns
        -------
        doc: Any
            Image loaded in memory by the pillow package
        """
        try:
            if self._hdfsClient.exists(hdfs_image_path) is False:
                return (
                    None,
                    RequestResult.ofError(
                        "File {} not exist.".format(hdfs_image_path)
                    ),
                )
            with self._hdfsClient.open(hdfs_image_path) as reader:
                img = Image.open(reader)
                return (
                    img.convert('RGB'),
                    RequestResult.ofOk(
                        "File {} readed and converted to RGB.".format(hdfs_image_path)
                    ),
                )
        except:
            return (
                None,
                RequestResult.ofError(
                    "Could not open file {}.".format(hdfs_image_path)
                ),
            )


    @staticmethod
    def create_hdfs3_conf(use_kerberos: bool, hdfs_name_services: str, hdfs_replication: str,
                           hdfs_host_services: str) -> dict:

        conf={"dfs.nameservices": hdfs_name_services,
            "dfs.client.use.datanode.hostname": "true",
            "dfs.replication": hdfs_replication
        }
        
        if use_kerberos:
            conf["hadoop.security.authentication"] = "kerberos"

        name_nodes = []
        urls = hdfs_host_services
        list_urls = urls.split(",")
        for i in range(len(list_urls)):
            name_nodes.append('nn' + str(i+1))
        conf["dfs.ha.namenodes." + hdfs_name_services] = ",".join(name_nodes)
        for i in range(len(list_urls)):
            conf['dfs.namenode.rpc-address.' + hdfs_name_services + '.' + 'nn' + str(i+1)] = list_urls[i]
        
        return conf

    @staticmethod
    def generate_ticket_cache(hdfs_kbr5_user_keytab_path: str, hdfs_krb5_username: str) -> bool:
       
        #Status is 0 when the subprocess is succeful!
       
        kt_cmd = 'kinit -kt ' + hdfs_kbr5_user_keytab_path + ' ' + hdfs_krb5_username
        status = subprocess.call([kt_cmd], shell=True)

        if status != 0:
            print("kinit ERROR:")
            print(subprocess.call([kt_cmd], shell=True))
        return status==0

    @staticmethod
    def get_ticket_cache():
        path = '/tmp'
        ticket = 'krb5cc_*'
        res = fnmatch.filter(os.listdir(path), ticket)
        res_ = res[0] if len(res)>0 else None
        return res_

    @staticmethod
    def renew_ticket_cache(conf: dict, hdfs_name_services: str, user: str, hdfs_kbr5_user_keytab_path: str, hdfs_krb5_username: str, message: str=""):
        hdfs_host = hdfs_name_services
        status = HDFSWrapper.generate_ticket_cache(hdfs_kbr5_user_keytab_path, hdfs_krb5_username)
        if status:
            ticket_cache = HDFSWrapper.get_ticket_cache()
            return HDFileSystem(host=hdfs_host, port=None, user=user, pars=conf, ticket_cache=ticket_cache)
        else:
            RequestResult.ofError(message)
            return None

    @staticmethod
    def hdfs_connect_kerberos(hdfs_name_services: str, hdfs_replication: str, user: str, hdfs_host_services: str,
                               hdfs_kbr5_user_keytab_path: str, hdfs_krb5_username: str):
        host = hdfs_name_services
        print("Usando KerberosClient...")
        conf = HDFSWrapper.create_hdfs3_conf(True, hdfs_name_services, hdfs_replication, hdfs_host_services)
        try:
            ticket_cache = HDFSWrapper.get_ticket_cache()
            if ticket_cache is not None:
                hdfs_client = HDFileSystem(host=host, port=None, user=user, pars = conf, ticket_cache=ticket_cache)
            else: 
                hdfs_client = HDFSWrapper.renew_ticket_cache(conf, hdfs_name_services, user, hdfs_kbr5_user_keytab_path,
                                                  hdfs_krb5_username, message="ERROR: Problems to generate Ticket Cache!")
        except:
            hdfs_client = HDFSWrapper.renew_ticket_cache(conf, hdfs_name_services, user, hdfs_kbr5_user_keytab_path,
                                                   hdfs_krb5_username, message="ERROR: Problems to renew Ticket Cache!")

        return HDFSWrapper(hdfs_client)

    @staticmethod
    def hdfs_connect_withoutlogin(hdfs_name_services: str, user: str, hdfs_replication: str, hdfs_host_services: str):
        host = hdfs_name_services
        print("Usando InsecureClient...")
        conf = HDFSWrapper.create_hdfs3_conf(False, hdfs_name_services, hdfs_replication, hdfs_host_services)
        hdfs_client = HDFileSystem(host=host, port=None, user=user, pars=conf)
        return HDFSWrapper(hdfs_client)
