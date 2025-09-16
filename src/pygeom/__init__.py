from datetime import datetime, date,timezone
import os, sys, re, json
DEBUG = os.getenv("DEBUG", 'False').lower() in ('true', '1', 't')

from typing import List, Dict
NM2KM=1.852
HOUR2SEC=3600.
DAYS2SEC=HOUR2SEC*24.

import contextlib, time

@contextlib.contextmanager
def stopwatch(context):
    """Context manager to print how long a block of code took."""
    t0 = time.time()
    try:
        yield
    finally:
        t1 = time.time()
        print('Total elapsed time for %s : %.3f sec' % (context, t1 - t0))
        

LOG_DFORMAT = '%Y-%m-%dT%H:%M:%S%Z - '
def formatNow():
    return datetime.now(tz=timezone.utc).strftime(LOG_DFORMAT)


def logme(msg):
    print(f"{formatNow()}: {msg}", file=sys.stderr, flush=True)
    
    
def mergeJson(dict0, dict1):
    
    if not isinstance(dict1,dict):
        return True
    
    for k in dict1:
        if k in dict0:
            if mergeJson(dict0[k],dict1[k]):
                dict0[k]= dict1[k]
        else:
            dict0[k] = dict1[k]
            
    return False

def mergeJsonDicts(jsondicts):        
    primdict = jsondicts[0]
    for i in range(1,len(jsondicts)):
        mergeJson(primdict,jsondicts[i])
    return primdict


    
def mergeCSV(fileroot, wildcard):
    """
    Merge a list of csv files as a result of the wildcard list,
    The wildcard has to be complete based on the Path.glob paradigm (shell script resolving, not regex)
    It is a vertical merge! 
    """
    
    from pathlib import Path
    import pandas as pd
    proot = Path(fileroot)
    frames  = []
    flist = list(proot.glob(wildcard))
    print("have n files {}".format(len(flist)))
    for f in flist:
        frames.append(pd.read_csv(str(f)))
        
        
    return pd.concat(frames), len(flist)

      
def encodeDumpPath(dtime = datetime.utcnow()):
    '''
    Build a path split by year/year-month/year-month-day
    '''
    return [dtime.strftime("%Y"),dtime.strftime("%Y-%m"),dtime.strftime("%Y-%m-%d")]



mmsi_regex = re.compile(r"[2-7]\d{8}")
mmsi_indo_regex = re.compile(r"^525\d{6}$")

def validMMSI(mmsi, repattern = mmsi_regex):
    '''
    valid mmsi must 
    start with 2 to 7
    0 Ship group, coast station, or group of coast stations
    1 For use by SAR aircraft (111MIDaxx)[note 1][2]
2-7 MMSI's used by individual ships, beginning with an MID:
    2 Europe (e.g., Italy has MID 247; Denmark has MIDs 219 and 220)
    3 North and Central America and Caribbean (e.g., Canada, 316; Greenland, 331; Panama, 351 through 357, plus 370 through 373; United States, 303(Alaska), 338(domestic), plus 366 through 369)
    4 Asia (not the southeast) (e.g., PRC, 412, 413, and 414; Maldives, 455; Japan, 431)
    5 Oceania (Australia, 503; New Zealand, 512), and Southeast Asia (Philippines, 548; Indonesia, 525)
    6 Africa (Eritrea, 625)
    7 South America (Peru, 760)
    8 Handheld VHF transceiver with DSC and GNSS[3]
    9 Devices using a free-form number identity:[2]
    Search and Rescue Transponders (970yyzzzz)[note 2][4][note 3][5]
    Man overboard DSC and/or AIS devices (972yyzzzz)[note 2]
    406 MHz EPIRBs fitted with an AIS transmitter (974yyzzzz)[note 2]
    craft associated with a parent ship (98MIDxxxx)[note 4]
    navigational aids (AtoNs; 99MIDaxxx)[note 5]

    '''
    return not repattern.match(str(mmsi)) is None
    
    
    
def toTS(dtt):
    return dtt.timestamp()

def _nowISOString():
    return datetime.utcnow().isoformat()

def _nowUTCOrdinal():
    return int(datetime.utcnow().timestamp())

def _nowTIMESTAMPString():
    return str(datetime.utcnow().timestamp())

def isValidStr(strval):
    if strval is None:
        return False
    return len(str(strval).strip()) > 0

'''
A collection of convenience classes to treat 'containers' of geometry collections the same way, 
sometimes avoiding the overhead of 'heavier' classes

'''

class Attributed():
    '''
    Simple base class to provide access to attributes in a otherwise stale class.
    FIXME - add iterator
    '''
    
    def __init__(self, properties = None):
        if properties is None:
            self._attr = {}
        elif isinstance(properties, dict):
            self._attr = properties

            
        
    def get_attr(self, key):
        if key in self.properties:
            return self.properties[key] 
        
        return None

    def set_attr(self,key, val):
        self.properties[key] = val
        
        
    def del_attr(self,key):
        del self.properties[key]
        
        
    def contains_attr(self,key):
        return key in self.properties
    
    
    def __str__(self):
        return str(self.properties)
    
    @property
    def properties(self):
        return self._attr
    
    
    
    @properties.setter
    def properties(self, props):
        if props is None:
            self._attr = {}
        elif isinstance(props, dict):
            self._attr = props
        else:
            raise ValueError('properties must be a dict.')

    # emulating QGis Feature and provide dict like access to properties , aka fields in QGis 
    def __getitem__(self,key):
        return self.get_attr(key)
    
    def __setitem__(self,key, value):
        return self.set_attr(key,value)


    def keys(self):
        return self._attr.keys()
    
    
    
        
    


class PrintGeom():
    @property
    def __geo_interface__(self):
        raise NotImplementedError()
    


class PrintLineString(PrintGeom):
    
    
    
    def __init__(self, geometry):
        #from shapely.geometry import Point,LineString
        if geometry is None :
            raise ValueError("This class PrintLineString needs to be initialised correctly with list of corrdinates")
        if  len(geometry) < 2:
            raise ValueError("This class PrintLineString needs to be initialised correctly with list of corrdinates: "+str(len(geometry)))
        
        # so to avoid having to load the geos libs 
        #if isinstance(geometry[0],Point):
        if geometry[0].__class__.__name__ == 'Point' :
            #if hasattr(geometry[0],'x'):
            self._geom = [[p.x,p.y] for p in geometry]
        
        elif isinstance(geometry[0],(tuple,list)):
            self._geom = geometry
        
        #elif isinstance(geometry[0],LineString):
        elif geometry[0].__class__.__name__ == 'LineString' :
            # we should not need this if we got here
            #elif hasattr(geometry,'coords'):
            self._geom =geometry.coords
            
        else:
            raise ValueError("This class PrintLineString needs to be initialised correctly with list of corrdinates of (tuple,Point)")
        

    #@property
    #def properties(self):
    #    return self._properties

    @property
    def __geo_interface__(self):
        vals=  {
            'type': 'LineString',
            'coordinates': self._geom
        }
        return vals
    
    def asWkt(self):
        return self._geom.wkt
    
    
class PrintPoint(PrintGeom):
    
    
    def __init__(self, geometry):
        if geometry is None :
            raise ValueError("This class PrintPoint needs to be initialised correctly with list of corrdinates")
        if  len(geometry) < 2:
            raise ValueError("This class PrintPoint needs to be initialised correctly with list of corrdinates: "+str(len(geometry)))
        
        if hasattr(geometry,'x'):
            
            self._geom = [geometry.x,geometry.y] 
        elif isinstance(geometry,(tuple,list)):
            self._geom = geometry
        else:
            raise ValueError("This class PrintPoint needs to be initialised correctly with corrdinates of (tuple,Point)")
        
    
    @property
    def __geo_interface__(self):
        vals=  {
            'type': 'Point',
            'coordinates': self._geom
        }
        return vals
            
            
    
class FakeGeometry(PrintGeom):
    
    def __init__(self,gjson):
        self._gjson = gjson
        
    @property
    def __geo_interface__(self):
        return self._gjson
        
        

class Feature(Attributed):
    def __init__(self, geometry = None, properties=None, feature_id=None):
        Attributed.__init__(self, properties=properties)
        self._geometry = None
        if not geometry is None:
            self.setGeometry( geometry)
        self._id = feature_id
        
        if feature_id is None:
            if self.contains_attr("id"):
                self._id = self.get_attr("id")
        elif not self.contains_attr("id"):
            self.set_attr("id", feature_id)
            

        
    


    def bounds(self):
        if not self._geometry is None:
            return self._geometry.bounds
        return None

    def boundary(self):
        if not self._geometry is None:
            try:
                return self._geometry.boundary
            except:
                pass
        return None

    def hasGeometry(self):
        return not self._geometry is None

    def geometry(self):
        return self._geometry

    def setGeometry(self, geometry):
        self._geometry = geometry
        if  isinstance(geometry,(PrintGeom)):
            return

        from shapely.geometry.base import BaseGeometry
        if isinstance(geometry, BaseGeometry):
            
            ## through the back of the head to the eye, force compatibility with qgis.geometry 
            
            def asWkt(self):
                return self.wkt
            setattr(geometry, 'asWkt', asWkt)
            
            def asPoint(self):
                return self.centroid
            setattr(geometry, 'asPoint', asPoint)
            
            def asJson(self):
                return json.dumps(self.__geo_interface__)
            
            setattr(geometry, 'asJson', asJson)
            
            
            return
                       
        raise ValueError('geometry must be a shapely geometry or print version.')

        
        
    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, idd):
        self._id = idd
        

    #@property
    #def properties(self):
    #    return self._properties

    @property
    def __geo_interface__(self):
        vals=  {
            'type': 'Feature',
            'geometry': self._geometry.__geo_interface__,
            #'id':self.id,
            'properties': self.properties,
        }
        if not self._id is None:
            vals['id'] = self._id
        return vals

    def __eq__(self, other):
        return self.__geo_interface__ == other.__geo_interface__
    


class FeatureCollection(Attributed):
    
    def __init__(self, objects, properties = None):
        Attributed.__init__(self, properties=properties)
        '''
        Add another feature to this collection,
        objects - iterable instances of Feature or shapely geometries, error is raised otherwise
        '''
        self.features = list(objects)
        self._mpoints = None
        

    @property
    def features(self):
        return self._features

    @features.setter
    def features(self, objects):
        all_are_features = all(
            isinstance(feature, Feature)
            for feature in objects
        )
        if all_are_features:
            self._features = objects
        else:
            try:
                self._features = [
                    Feature(geometry)
                    for geometry in objects
                ]
            except ValueError:
                raise ValueError(
                    'features can only be either a Feature or shapely geometry.')

    def append(self,feature):
        '''
        Add another feature to this collection,
        feature - an instance of Feature, error is raised otherwise
        
        '''
        if isinstance(feature, Feature):
            self._features.append(feature)
            self._mpoints =  None
        else:
            try:
                self._features.append(Feature(feature))
            except ValueError:
                raise ValueError(
                    'features can only be either a Feature or shapely geometry.')
            

    def __iter__(self):
        return iter(self.features)

    def geometries_iterator(self):
        for feature in self.features:
            yield feature.geometry()
            
    def geometries_properties_itertor(self):
        for feature in self.features:
            yield feature.geometry(), feature.properties


    def __extractBounds(self,geom):
        from shapely.geometry.base import BaseMultipartGeometry
        pplist = list()
        if isinstance(geom,BaseMultipartGeometry):
            for ff in geom.geoms:
                pplist.extend(self.__extractBounds(ff))
        
        else:
            fbb = geom.boundary
            if fbb is None:
                fbb = geom.bounds
            else:
                fbb = fbb.bounds
            pplist.append(fbb[0:2])
            pplist.append(fbb[2:])
            '''
            for fb in zip(*fbb.coords.xy):
            #for fb in fbb:
                pplist.append(fb)
            '''
        return pplist
    

    def __extractMPoints(self):
        
        if self._mpoints is None:
            from shapely.geometry import MultiPoint
            plist = list()
            for f in self.geometries_iterator():
                plist.extend(self.__extractBounds(f))
                
            '''
                if isinstance(f,BaseMultipartGeometry):
                    for ff in f.geoms:
                        fbb = ff.boundary
                    if fbb is None:
                        fbb = ff.bounds
                    for fb in zip(*fbb.coords.xy):
                    #for fb in fbb:
                        plist.append(fb)
                else:
                    fbb = f.boundary
                    if fbb is None:
                        fbb = f.bounds
                    for fb in zip(*fbb.coords.xy):
                    #for fb in fbb:
                        plist.append(fb)
            '''
            self._mpoints = MultiPoint(plist)

    @property
    def bounds(self):
        self.__extractMPoints()
        return self._mpoints.bounds
    
    
    def boundary(self):
        self.__extractMPoints()
        return self._mpoints.convex_hull.boundary

    def convex_hull(self):
        self.__extractMPoints()
        return self._mpoints.convex_hull


    @property
    def __geo_interface__(self):
        return {
            'type': 'FeatureCollection',
            'properties': self.properties,
            'features': [
                feature.__geo_interface__
                for feature in self.features
            ],
        }

    def __eq__(self, other):
        return self.__geo_interface__ == other.__geo_interface__
    
    

## Export functions 

def _exportJsonProperties(props):
    if props:
        for k in props:
            if isinstance(props[k],(list, tuple) ):
                if len(props[k]) > 0:
                    if isinstance(props[k][0],(date, datetime) ):
                        props[k] = ",".join([d.isoformat() for d in props[k]])
                    else:
                        props[k] = ",".join([str(d) for d in props[k]])
                else:
                    props[k] = ""
                        
            elif isinstance(props[k],(date, datetime) ):
                props[k] = props[k].isoformat()
    return props

def _createFeature(coords, props):
    return { "type": "Feature",
        "geometry": {
          "type": "Polygon",
          "coordinates": [coords]
          },
        "properties": props
        }
    
    
def _createFeatureShapely(geom, props):
    '''
    if props:
        for k in props:
            if isinstance(props[k],(list, tuple) ):
                if len(props[k]) > 0:
                    if isinstance(props[k][0],(date, datetime) ):
                        props[k] = ",".join([d.isoformat() for d in props[k]])
                    else:
                        props[k] = ",".join([str(d) for d in props[k]])
                else:
                    props[k] = ""
                        
            elif isinstance(props[k],(date, datetime) ):
                props[k] = props[k].isoformat()
    '''
    
    return { "type": "Feature",
        "geometry": geom.__geo_interface__ ,
        "properties": _exportJsonProperties(props)
        }

def _createFeaturCollection(feats):
    return { "type": "FeatureCollection",
    "features":feats}
    
    
    
def bufferFromPoint(x:float,y:float,buffer:float):
    from shapely import Point
    return Point(x,y).buffer(buffer,20)
    
    
def bufferFeatureFromPoint(x:float,y:float,buffer:float):
    return  Feature(bufferFromPoint(x,y,buffer),{})
    
def toGeoJson(shapelygeoms : List, props: List):
    
    if props:
        return json.dumps({
                'type': 'FeatureCollection',
                'features': [{
                'type': 'Feature',
                'properties':p if p else {},
                'geometry': buffer.__geo_interface__
            } for buffer, p in zip(shapelygeoms,props)]})
        
    else:
        return json.dumps({
                'type': 'FeatureCollection',
                'features': [{
                'type': 'Feature',
                'geometry': buffer.__geo_interface__
            } for buffer in shapelygeoms]})
    
    
    
def toGeoPackage(fetcolection , outfile, epsg = '4326'):
    import geopandas as gpd
    import pandas as pd
    
    geoms = []
    properties = []
    for g,p in fetcolection.geometries_properties_itertor():
        properties.append(p)
        geoms.append(g)
        
    df = pd.DataFrame(properties)
    df["geometry"] = geoms
    gdf  = gpd.GeoDataFrame(df,crs=f"EPSG:{epsg}")
    
        
    gdf.to_file(outfile, driver='GPKG',mode='a')


'''
def appendToGPKG(infile, outfile, layer = None):    
    if layer is None:
        import os
        layer = os.path.splitext(os.path.basename(infile))[0] 
    writeToGPKG(infile, str(outfile), mode ='a',layer=layer)
''' 
    
def writeToGPKG(infile, outfile, layer =None):
    import fiona
    from pathlib import Path
    from pygeom.utils import ensureSuffix
    outfilep = Path(ensureSuffix(outfile,"gpkg")).as_uri()
    
    # Open a file for reading. We'll call this the source.
    with fiona.open(infile) as src:
        # The file we'll write to must be initialized with a coordinate
        # system, a format driver name, and a record schema. We can get
        # initial values from the open source's profile property and then
        # modify them as we need.
        profile = src.profile
        profile["driver"] = "GPKG"
        # ignored anyway
        #if not layer is None:    
        #    profile["layer"] = None
    
        # Open an output file, using the same format driver and coordinate
        # reference system as the source. The profile mapping fills in the
        with fiona.open(outfilep, "w",**profile) as dst:
            for feat in src:
                dst.write(feat)
                
def writeToSHP(infile, outfile, layer =None):
    import fiona
    from pygeom.utils import ensureSuffix
    from pathlib import Path
    outfilep = Path(ensureSuffix(outfile,"shp")).as_uri()
    
    # Open a file for reading. We'll call this the source.
    with fiona.open(infile) as src:
        # The file we'll write to must be initialized with a coordinate
        # system, a format driver name, and a record schema. We can get
        # initial values from the open source's profile property and then
        # modify them as we need.
        profile = src.profile
        profile["driver"] = "ESRI Shapefile"
        # ignored anyway
        #if not layer is None:    
        #    profile["layer"] = None
    
        # Open an output file, using the same format driver and coordinate
        # reference system as the source. The profile mapping fills in the
        with fiona.open(outfilep, "w",**profile) as dst:
            for feat in src:
                dst.write(feat)
                
                
                
        
        
    
    
    
    

