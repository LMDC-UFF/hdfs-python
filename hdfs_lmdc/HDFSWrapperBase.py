import abc

from typing import Tuple, TypeVar, Generic, List

from PIL.Image import Image

from hdfs_lmdc.hdfs import RequestResult

T = TypeVar('T')


# class HDFSWrapperBase(Generic[T], metaclass=abc.ABCMeta):
class HDFSWrapperBase(Generic[T]):

    @abc.abstractmethod
    def getClient(self) -> T:
        pass

    @abc.abstractmethod
    def upload(self, local_path: str, hdfs_path: str) -> RequestResult:
        pass

    @abc.abstractmethod
    def download(self, hdfs_file_path: str, local_save_path: str = None) -> Tuple[str, RequestResult]:
        pass

    @abc.abstractmethod
    def read_image(self, hdfs_image_path: str) -> Tuple[Image, str]:
        pass

    @abc.abstractmethod
    def exist_path(self, path: str) -> bool:
        pass

    @abc.abstractmethod
    def read_txt(self, path: str) -> Tuple[str, RequestResult]:
        pass

    @abc.abstractmethod
    def mkdir(self, path: str) -> bool:
        pass

    @abc.abstractmethod
    def glob(self, path) -> List[str]:
        pass