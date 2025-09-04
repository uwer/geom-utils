from setuptools import setup, find_packages

setup(
    name='pygeom',
    version='0.9',
    package_dir = {"": "src"},
    python_requires=">=3.8",
    
    url='https://github.com/uwer/geom-utils',
    author='UR',
    author_email='ur@gmail.com',
    install_requires=[
        "shapely",
        "pyproj",
        "geographiclib"   
    ],
    
    packages=find_packages(where='src'),
    
)