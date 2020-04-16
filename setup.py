from setuptools import setup

def readme():
    with open('README.md') as f:
        README = f.read()
    return README

# This call to setup() does all the work
setup(
    name="hdfs-lmdc",
    version="1.0.4",
    description="Esta biblioteca tem como objetivo generalizar funções da integração entre HDFS e Python utilizando HDFS3",
    long_description=readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/LMDC-UFF/hdfs-python",
    author="LMDC-UFF",
    author_email="opensource@lmdc.uff.br",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    packages=["hdfs_lmdc"],
    include_package_data=True,
    install_requires=["hdfs3", "pillow==6.2.2"],
    entry_points={
        "console_scripts": [
            "hdfs-lmdc=hdfs_lmdc.demo:main",
        ]
    },
)