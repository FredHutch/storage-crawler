from setuptools import setup

__version__ = "0.0.1"

try:
    from pypandoc import convert
    read_md = lambda f: convert(f, 'rst')
except ImportError:
    print("warning: pypandoc module not found, could not convert Markdown to RST")
    read_md = lambda f: open(f, 'r').read()

CLASSIFIERS = [
    "Development Status :: 5 - Production/Stable",
    "Environment :: Console",
    "Intended Audience :: Customer Service",
    "Intended Audience :: Developers",
    "Intended Audience :: Education",
    "Intended Audience :: End Users/Desktop",
    "Intended Audience :: Healthcare Industry",
    "Intended Audience :: Information Technology",
    "Intended Audience :: Science/Research",
    "Intended Audience :: System Administrators",
    "License :: OSI Approved :: Apache Software License",
    "Natural Language :: English",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: POSIX",
    "Operating System :: POSIX :: Linux",
    "Operating System :: POSIX :: Other",
    "Operating System :: Unix",
    "Programming Language :: Python :: 2",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Python :: Implementation :: PyPy",
    "Programming Language :: Unix Shell",
    "Topic :: Desktop Environment :: File Managers",
    "Topic :: Internet",
    "Topic :: Scientific/Engineering :: Bio-Informatics",
    "Topic :: System :: Archiving",
    "Topic :: System :: Archiving :: Backup",
    "Topic :: System :: Filesystems",
    "Topic :: Software Development :: Libraries :: Python Modules",
    "Topic :: System :: Systems Administration",
    "Topic :: Utilities"
]

setup(
    name='storage-crawler',
    version=__version__,
    description='''\
       parallel file system crawler that manages file metadata in a postgres database''',
    long_description=read_md('README.md'),
    packages=['storage_crawler'],
    scripts=['storage_crawler/storcrawl'],
    author = 'Some Person',
    author_email = 'dp@nowhere.com',
    url = 'https://github.com/FredHutch/storage-crawler', 
    download_url = 'https://github.com/FredHutch/storage-crawler/tarball/%s' % __version__,
    keywords = ['openstack', 'swift', 'cloud storage'], # arbitrary keywords
    classifiers = CLASSIFIERS,
    # 'python-swiftclient>=2.5,<3','python-keystoneclient>=1.5,<2'
    install_requires=[
        'psutil>=4',
        'scandir>=1'
        ],
    entry_points={
        # we use console_scripts here to allow virtualenv to rewrite shebangs
        # to point to appropriate python and allow experimental python 2.X
        # support.
        'console_scripts': [
            'storcrawl.py=storage_crawler.storcrawl:main',
        ]
    }
)
