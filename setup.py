from setuptools import setup, find_packages

setup(
    name='pymongo-helper',
    version='0.0.3',
    author='ambroisehdn',
    author_email='contact@ambroisehdn.me',
    description='The pymongo helper package is a Python library designed to simplify and streamline interactions with MongoDB databases using the PyMongo driver.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/ambroisehdn/pymongo-helper',
    project_urls={
        'Bug Tracker': 'https://github.com/ambroisehdn/pymongo-helper/issues'
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
    ],
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    python_requires='>=3.6',
    install_requires=[
        'pymongo==4.7.0',
        'pytest==8.2.0',
    ],
)
