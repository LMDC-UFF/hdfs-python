import io
import os
from typing import Tuple, List
from PIL import Image
from py4j.java_gateway import JavaGateway

from hdfs_lmdc.HDFSWrapperBase import HDFSWrapperBase, T
from hdfs_lmdc.hdfs import RequestResult

globals()['gateway'] = None


class HadoopPythonServiceDef:

    def existsPath(self, path: str) -> bool:
        pass

    def upload(self, local_path: str, hdfs_path: str) -> bool:
        pass

    def download(self, hdfs_file_path: str, local_save_path: str = None) -> bool:
        pass

    def readAllBytes(self, path) -> bytearray:
        pass

    def mkdir(self, path) -> bool:
        pass

    def glob(self, path) -> List[str]:
        pass


class HDFSWrapperJava(HDFSWrapperBase[HadoopPythonServiceDef]):

    def getClient(self) -> T:
        gateway: JavaGateway = globals()["gateway"]
        if gateway is None:
            print("Abrindo conexão com o hadoop no java")
            gateway = JavaGateway()
            print("Métodos disponívies")
            print(gateway.help(gateway.entry_point))
            globals()["gateway"] = gateway
        return gateway

    def exist_path(self, path: str) -> bool:
        return self.getClient().existsPath(path)

    def upload(self, local_path: str, hdfs_path: str) -> RequestResult:
        result = self.getClient().upload(local_path, hdfs_path)
        if result:
            return RequestResult.ofOk("File Uploaded")
        else:
            return RequestResult.ofError("Error upload file...! {}".format(local_path))

    def download(self, hdfs_file_path: str, local_save_path: str = None) -> Tuple[str, RequestResult]:
        try:
            if self.exist_path(hdfs_file_path) is False:
                return None, RequestResult.ofError("File {} not exist.".format(hdfs_file_path))

            _, local_file_name = os.path.split(hdfs_file_path)
            local_file_name, ext = os.path.splitext(local_file_name)

            local_folder_path = local_save_path
            if local_save_path is None:
                local_folder_path = os.path.join((os.sep + "tmp"), local_file_name)

            local_file_path = os.path.join(local_folder_path, local_file_name + ext)
            os.makedirs(local_folder_path, exist_ok=True)
            self.getClient().download(hdfs_file_path, local_file_path)

            return local_file_path, RequestResult.ofOk("File downloaded")
        except:
            return None, RequestResult.ofError("Download File {} failure.".format(hdfs_file_path));

    def read_txt(self, hdfs_text_path: str) -> Tuple[str, RequestResult]:
        try:
            if self.getClient().existsPath(hdfs_text_path) is False:
                return (
                    None,
                    RequestResult.ofError(
                        "File {} not exist.".format(hdfs_text_path)
                    ),
                )

            return (
                self.getClient().readAllBytes(hdfs_text_path).decode("utf-8"),
                RequestResult.ofOk(
                    "File {} read successfully.".format(hdfs_text_path)
                ),
            )
        except:
            pass

        return (
            None,
            RequestResult.ofError(
                "Could not open file {}.".format(hdfs_text_path)
            ),
        )

    def read_image(self, hdfs_image_path: str):
        try:
            if self.getClient().existsPath(hdfs_image_path) is False:
                return (
                    None,
                    RequestResult.ofError(
                        "File {} not exist.".format(hdfs_image_path)
                    ),
                )
            import time
            start = time.time()
            content = self.getClient().readAllBytes(hdfs_image_path)
            end = time.time()
            print(end - start)
            img = Image.open(io.BytesIO(content))
            return (
                img.convert('RGB'),
                RequestResult.ofOk(
                    "File {} readed and converted to RGB.".format(hdfs_image_path)
                ),
            )
        except Exception as e:
            print(e)
            pass

        return (
            None,
            RequestResult.ofError(
                "Could not open file {}.".format(hdfs_image_path)
            ),
        )

    def mkdir(self, path: str) -> bool:
        return self.getClient().mkdir(path)

    def glob(self, path) -> List[str]:
        resultQuery = self.getClient().glob(path)
        result = [path for path in resultQuery]
        return result
