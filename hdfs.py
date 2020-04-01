import os
import fnmatch
import subprocess
from hdfs3 import HDFileSystem

class ResquestResult:

    def __init__(self, status:bool, sucess_msg:str, erro:str):
        self.status =  status
        self.erro =  erro
        self.sucess_msg = sucess_msg

    @staticmethod
    def ofOk(sucess_msg:str = None):ResquestResult
        return ResquestResult(True, sucess_msg, None)
    
    @staticmethod
    def ofError(err_msg:str = None):ResquestResult
        return ResquestResult(False, sucess_msg, None)
    

class HDFSWrapper:

    def __init__(self, hdfsClient:HDFileSystem):
        self._hdfsClient  = hdfsClient
    
    def getClient(self):
        return self.hdfs_client

    def upload(self, hdfs_path: str, local_path: str):ResquestResult
        try:
            file_name = os.path.basename(local_path)
            self.hdfsClient.put(local_path,os.path.join(hdfs_path, file_name))
        except:
            print("Error upload file.....!" + local_path)

    def download(self, hdfs_file_path: str, local_save_path: str=None):
        try:
            if self.hdfsClient.exists(hdfs_file_path) is False:
                return None, "File {} not exist.".format(hdfs_file_path)
            
            local_file_name = hdfs_file_path
            local_file_name, ext = os.path.splitext(local_file_name)
            
            local_folder_path = local_save_path
            if local_save_path is None:
                local_folder_path = os.path.join((os.sep + "tmp"), local_file_name)
            
            local_file_path = os.path.join(local_folder_path, local_file_name + ext)
            os.makedirs(local_folder_path, exist_ok=True)
            
            self.hdfsClient.get(hdfs_file_path, local_file_path)
            return local_file_path, None
        except:
            return None, "Download File {} failure.".format(hdfs_file_path)


    @staticmethod
    def _create_hdfs3_conf(use_kerberos: bool, hdfs_name_services: str, hdfs_replication: str, hdfs_host_services: str):

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
    def _generate_ticket_cache(hdfs_kbr5_user_keytab_path: str, hdfs_krb5_username: str):
        """
        Status is 0 when the subprocess is succeful!
        """
        kt_cmd = 'kinit -kt ' + hdfs_kbr5_user_keytab_path + ' ' + hdfs_krb5_username
        status = subprocess.call([kt_cmd], shell=True)

        if status != 0:
            print("kinit ERROR:")
            print(subprocess.call([kt_cmd], shell=True))
        return status==0

    @staticmethod
    def _get_ticket_cache():
        path = '/tmp'
        ticket = 'krb5cc_*'
        res = fnmatch.filter(os.listdir(path), ticket)
        res_ = res[0] if len(res)>0 else None
        return res_

    @staticmethod
    def _renew_ticket_cache(conf: dict, hdfs_name_services: str, user: str, hdfs_kbr5_user_keytab_path: str, hdfs_krb5_username: str, message: str=""):
        hdfs_host = hdfs_name_services
        status = _generate_ticket_cache(hdfs_kbr5_user_keytab_path, hdfs_krb5_username)
        if status:
            ticket_cache = _get_ticket_cache()
            return HDFileSystem(host=hdfs_host, port=None, user=user, pars=conf, ticket_cache=ticket_cache)
        else:
            print(message)
        return None

    @staticmethod
    def hdfs_connect_kerberos(hdfs_name_services: str, hdfs_replication: str, user: str, hdfs_host_services: str,
                               hdfs_kbr5_user_keytab_path: str, hdfs_krb5_username: str):
        host = hdfs_name_services
        print("Usando KerberosClient...")
        conf = _create_hdfs3_conf(True, hdfs_name_services, hdfs_replication, hdfs_host_services)
        try:
            ticket_cache = _get_ticket_cache()
            if ticket_cache is not None:
                hdfs_client = HDFileSystem(host=host, port=None, user=user, pars = conf, ticket_cache=ticket_cache)
            else: 
                hdfs_client = _renew_ticket_cache(conf, hdfs_name_services, user, hdfs_kbr5_user_keytab_path,
                                                  hdfs_krb5_username, message="ERROR: Problems to generate Ticket Cache!")
        except:
            hdfs_client = _renew_ticket_cache(conf, hdfs_name_services, user, hdfs_kbr5_user_keytab_path,
                                                   hdfs_krb5_username, message="ERROR: Problems to renew Ticket Cache!")

        return HDFSWrapper(hdfs_client)

    @staticmethod
    def hdfs_connect_withoutlogin(hdfs_name_services: str, user: str, hdfs_replication: str, hdfs_host_services: str):HDFSWrapper
        host = hdfs_name_services
        print("Usando InsecureClient...")
        conf = _create_hdfs3_conf(False, hdfs_name_services, hdfs_replication, hdfs_host_services)
        hdfs_client = HDFileSystem(host=host, port=None, user=user, pars=conf)
        return HDFSWrapper(hdfs_client)

