import os
import fnmatch
import subprocess
from hdfs3 import HDFileSystem


class LocalHDFS:

    def __init__(self, hdfs_name_services: str, hdfs_host_services: str, user: str, 
                 auth_mechanism: str, hdfs_replication: str, **kwargs):
        self.hdfs_name_services = hdfs_name_services
        self.hdfs_host_services = hdfs_host_services
        self.user = user
        self.auth_mechanism = auth_mechanism
        self.hdfs_replication = hdfs_replication
        self.hdfs_kbr5_user_keytab_path = kwargs.get('hdfs_kbr5_user_keytab_path')
        self.hdfs_krb5_username = kwargs.get('hdfs_krb5_username')
        self.client = self._hdfs3Connect()
    
    def upload(self, hdfs_path: str, local_path: str, overwrite: bool=True):
        try:
            file_name = os.path.basename(local_path)
            self.client.put(local_path,os.path.join(hdfs_path, file_name))
        except:
            print("Error upload file.....!" + local_path)

    def download(self, hdfs_file_path: str, local_save_path: str=None):
        try:
            if self.client.exists(hdfs_file_path) is False:
                return None, "File {} not exist.".format(hdfs_file_path)
            
            local_file_name = hdfs_file_path
            local_file_name, ext = os.path.splitext(local_file_name)
            
            local_folder_path = local_save_path
            if local_save_path is None:
                local_folder_path = os.path.join((os.sep + "tmp"), local_file_name)
            
            local_file_path = os.path.join(local_folder_path, local_file_name + ext)
            os.makedirs(local_folder_path, exist_ok=True)
            
            self.client.get(hdfs_file_path, local_file_path)
            return local_file_path, None
        except:
            return None, "Download File {} failure.".format(hdfs_file_path)
    
    def _hdfs3Connect(self):
        try:
            myclient = None
            if self.auth_mechanism in ['GSSAPI', 'LDAP']:
                myclient = self._hdfs_connect_kerberos()
            else:
                myclient = self._hdfs_connect_WithoutLogin()
            print("STATUS HDFS:", myclient)
            return myclient
        except:
            print("Conecction failure...!")
            return None
    
    def _create_hdfs3_conf(self, use_kerberos: bool):
        # nameservices: str
        # namenodes: str ("nn1,nn2,...")
        # url: str ("url1,url2,..") 
        # dfs_replication: str(number)
        # use_kerberos: bool
        conf={"dfs.nameservices": self.hdfs_name_services,
            "dfs.client.use.datanode.hostname": "true",
            "dfs.replication": self.hdfs_replication
        }
        
        if use_kerberos:
            conf["hadoop.security.authentication"] = "kerberos"

        name_nodes = []
        urls = self.hdfs_host_services
        list_urls = urls.split(",")
        for i in range(len(list_urls)):
            name_nodes.append('nn' + str(i+1))
        conf["dfs.ha.namenodes." + self.hdfs_name_services] = ",".join(name_nodes)
        for i in range(len(list_urls)):
            conf['dfs.namenode.rpc-address.' + self.hdfs_name_services + '.' + 'nn' + str(i+1)] = list_urls[i]
        
        return conf

    def _generate_ticket_cache(self):
        """
        Status is 0 when the subprocess is succeful!
        """
        kt_cmd = 'kinit -kt ' + self.hdfs_kbr5_user_keytab_path + ' ' + self.hdfs_krb5_username
        status = subprocess.call([kt_cmd], shell=True)

        if status != 0:
            print("kinit ERROR:")
            print(subprocess.call([kt_cmd], shell=True))
        return status==0

    def _get_ticket_cache(self):
        path = '/tmp'
        ticket = 'krb5cc_*'
        res = fnmatch.filter(os.listdir(path), ticket)
        res_ = res[0] if len(res)>0 else None
        return res_  

    def _renew_ticket_cache(self, conf: dict, message: str=""):
        hdfs_host = self.hdfs_name_services
        status = self._generate_ticket_cache()
        if status:
            ticket_cache = self._get_ticket_cache()
            return HDFileSystem(host=hdfs_host, port=None, user=self.user, pars=conf, ticket_cache=ticket_cache)
        else:
            print(message)
        return None

    def _hdfs_connect_kerberos(self):
        host = self.hdfs_name_services
        print("Usando KerberosClient...")
        conf = self._create_hdfs3_conf(use_kerberos=True)
        try:
            ticket_cache = self._get_ticket_cache()
            if ticket_cache is not None:
                hdfs_client = HDFileSystem(host=host, port=None, user=user, pars = conf, ticket_cache=ticket_cache)
            else: 
                hdfs_client = self._renew_ticket_cache(conf, message="ERROR: Problems to generate Ticket Cache!")
        except:
            hdfs_client = self._renew_ticket_cache(conf, message="ERROR: Problems to renew Ticket Cache!")

        return hdfs_client
    
    def _hdfs_connect_withoutlogin(self):
        host = self.hdfs_name_services
        print("Usando InsecureClient...")
        conf = self._create_hdfs3_conf(use_kerberos=False)
        hdfs_client = HDFileSystem(host=host, port=None, user=self.user, pars=conf)
        return hdfs_client