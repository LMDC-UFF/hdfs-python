import fnmatch
import os
import subprocess
from hdfs3 import HDFileSystem

class LocalHDFS:

    def __init__(self, hdfs_name_services: str, auth_mechanism: str, user: str, hdfs_host_services: str, hdfs_replication: str, **kwargs):
        try:
            #host = os.environ['HDFS_NAMESERVICES'].split(",")[0]
            if auth_mechanism in ['GSSAPI', 'LDAP']:
                self.client = self.hdfs_connect_kerberos(hdfs_name_services, user, hdfs_host_services,
                                                         hdfs_replication, kwargs.get(hdfs_kbr5_huser_keytab_path), kwargs.get(hdfs_krb5_username))
            else:
                self.client = self.hdfs_connect_WithoutLogin(hdfs_name_services, user, hdfs_host_services, hdfs_replication)
            print("STATUS HDFS:", self.client)
        except:
            print("Conecction failure...!")
            raise
    
    def create_hdfs3_conf(self, nameservices: str, urls: str, dfs_replication: str, use_kerberos: bool):
        # nameservices: str
        # namenodes: str ("nn1,nn2,...")
        # url: str ("url1,url2,..") 
        # dfs_replication: str(number)
        # use_kerberos: bool
        conf={"dfs.nameservices": nameservices,
            "dfs.client.use.datanode.hostname": "true",
            "dfs.replication": dfs_replication
        }
        
        if use_kerberos:
            conf["hadoop.security.authentication"] = "kerberos"

        name_nodes = []
        list_urls = urls.split(",")
        for i in range(len(list_urls)):
            name_nodes.append('nn' + str(i+1))
        conf["dfs.ha.namenodes." + nameservices] = ",".join(name_nodes)
        for i in range(len(list_urls)):
            conf['dfs.namenode.rpc-address.' + nameservices + '.' + 'nn' + str(i+1)] = list_urls[i]
        
        return conf

    def generate_ticket_cache(self, hdfs_kbr5_huser_keytab_path: str, hdfs_krb5_username: str):
        """
        Status is 0 when the subprocess is succeful!
        """
        kt_cmd = 'kinit -kt ' + hdfs_kbr5_huser_keytab_path + ' ' + hdfs_krb5_username
        status = subprocess.call([kt_cmd], shell=True)

        if status != 0:
            print("kinit ERROR:")
            print(subprocess.call([kt_cmd], shell=True))
        return status==0

    def get_ticket_cache(self):
        path = '/tmp'
        ticket = 'krb5cc_*'
        res = fnmatch.filter(os.listdir(path), ticket)
        res_ = res[0] if len(res)>0 else None
        return res_  

    def renew_ticket_cache(self, hdfs_host: str, user: str, conf: dict, hdfs_kbr5_huser_keytab_path: str, hdfs_krb5_username: str, message=""):
        status = generate_ticket_cache(hdfs_kbr5_huser_keytab_path, hdfs_krb5_username)
        if status:
            ticket_cache = get_ticket_cache()
            return HDFileSystem(host=hdfs_host, port=None, user=user, pars = conf, ticket_cache=ticket_cache)
        else:
            print(message)
        return None


    def hdfs_connect_kerberos(self, hdfs_name_services: str, user: str, hdfs_host_services: str, hdfs_replication: str, hdfs_kbr5_huser_keytab_path: str, hdfs_krb5_username: str):

        host = hdfs_name_services
        # host, port = str(url).split(":")
        # port = int(port)

        print("Usando KerberosClient...")
        conf = create_hdfs3_conf( host,
                                  hdfs_host_services,
                                  hdfs_replication,
                                    True)
        try:
            ticket_cache = get_ticket_cache()
            if ticket_cache is not None:
                hdfs_client = HDFileSystem(host=host, port=None, user=user, pars = conf, ticket_cache=ticket_cache)
            else: 
                hdfs_client = renew_ticket_cache(host, user, conf, hdfs_kbr5_huser_keytab_path, hdfs_krb5_username, message="ERROR: Problems to generate Ticket Cache!")
        except:
            hdfs_client = renew_ticket_cache(host, user, conf, message="ERROR: Problems to renew Ticket Cache!")

        return hdfs_client
    
    def hdfs_connect_withoutlogin(self, hdfs_name_services: str, user: str, hdfs_host_services: str, hdfs_replication: str):

        host = hdfs_name_services
        # host, port = str(url).split(":")
        # port = int(port)

        print("Usando InsecureClient...")
        conf = create_hdfs3_conf(host,
                                 hdfs_host_services,
                                 hdfs_replication,
                                False)
        hdfs_client = HDFileSystem(host=host, port=None, user=user, pars=conf)

        return hdfs_client

    
    def upload(self, hdfs_path: str, local_path: str, overwrite: bool):
        try:
            # self.client.upload(hdfs_path, local_path, overwrite=overwrite)
            file_name = os.path.basename(local_path)
            self.client.put(local_path,os.path.join(hdfs_path, file_name))
        except:
            print("Error upload file.....!" + local_path)


    def download(self, hdfs_file_path: str, local_save_path=None):
        try:
            if self.client.exists(hdfs_file_path) is False:
                return None, "File {} not exist.".format(hdfs_file_path)
            # doc = self.read(hdfs_file_path)
            local_file_name = hdfs_file_path
            local_file_name, ext = os.path.splitext(local_file_name)
            
            local_folder_path = local_save_path
            if local_save_path is None:
                local_folder_path = os.path.join(join(os.sep + "tmp"), local_file_name)
            
            local_file_path = os.path.join(local_folder_path, local_file_name + ext)
            os.makedirs(local_folder_path, exist_ok=True)
            # local_file_path = self.save_local(doc, local_file_path)
            self.client.get(hdfs_file_path, local_file_path)
            return local_file_path, None
        except:
            return None, "Download File {} failure.".format(hdfs_file_path)