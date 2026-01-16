from setuptools import setup, find_packages

setup(
    name='pygeom',
    version='0.9.1',
    package_dir = {"": "src"},
    python_requires=">=3.9",
    
    url='https://github.com/uwer/geom-utils',
    author='UR',
    author_email='ur@gmail.com',
    install_requires=[
        "shapely",
        "pyproj",
        "geographiclib"   ,
        "pandas>=2.3",
        "numpy",
        "fiona",
        "concave_hull"
    ],
    
    packages=find_packages(where='src'),
    
)