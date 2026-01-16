import json,os, sys,math
from copy import deepcopy
from shapely import wkt,wkb
import shapely
from shapely.geometry import (
     shape,Point,
     LineString, MultiPoint, 
     MultiPolygon, Polygon
     )
from shapely.ops import transform
import pyproj
from typing import List, Dict
from random import randrange
from collections.abc import Iterable


from pygeom import logme, _createFeatureShapely, Feature

from datetime import datetime, date
from _datetime import timedelta
import numpy as np


epsg4326Proj = pyproj.CRS('epsg:4326')

METRIC_PROCEJTION=pyproj.CRS("EPSG:3995")

DOTRACK = False

PROFILES = {"geojson":{"driver":"GeoJSON","properties":{},"crs":None,"schema":{"geometry":""}},
            "json":{"driver":"GeoJSON","properties":{},"crs":None,"schema":{"geometry":""}},
            "shp":{"driver":"ESRI Shapefile","properties":{},"crs":None,"schema":{"geometry":""}},
            "fgb":{"driver":"FlatGeobuf","properties":{},"crs":None,"schema":{"geometry":""}}}

SUFFIX4DRIVER = {"GeoJSON":"geojson","ESRI Shapefile":"shp","FlatGeobuf":"fgb"}

def createTransferProj(src_crs,target_csr):
    '''
    Create the projection transform function for geometries,
    call by 
    from shapely.ops import transform
    transform(proj_func,geometry)
    
    
    '''
    from functools import partial
    
    if type(src_crs) is pyproj.Proj:
        psrc_crs = src_crs
    elif type(src_crs) is dict:
        psrc_crs = pyproj.Proj(src_crs)
    else:
        psrc_crs = pyproj.Proj(init=str(src_crs))
    
    if type(target_csr) is pyproj.Proj:
        ptarget_crs = target_csr
    elif type(target_csr) is dict:
        ptarget_crs = pyproj.Proj(target_csr)
    else:
        ptarget_crs = pyproj.Proj(init=str(target_csr))
                                                          
        
    if psrc_crs.srs == ptarget_crs.srs:
        return None
    
    project = partial(
     pyproj.transform,
     psrc_crs,
     ptarget_crs
    )
    
    return project


def createTransformer(src_crs,target_csr):
    '''
    Create the projection transform function for geometries,
    call by 
    from shapely.ops import transform
    transform(proj_func,geometry)
    
    
    '''
    
    if type(src_crs) is pyproj.Proj:
        psrc_crs = src_crs
    elif type(src_crs) is dict:
        psrc_crs = pyproj.Proj(src_crs)
    else:
        psrc_crs = pyproj.Proj(init=str(src_crs))
    
    if type(target_csr) is pyproj.Proj:
        ptarget_crs = target_csr
    elif type(target_csr) is dict:
        ptarget_crs = pyproj.Proj(target_csr)
    else:
        ptarget_crs = pyproj.Proj(init=str(target_csr))
                                                          
        
    if psrc_crs.srs == ptarget_crs.srs:
        return None
    
    
    transformer = pyproj.Transformer.from_crs(psrc_crs.srs, ptarget_crs.srs, always_xy=True)
    return transformer

    
def _mapping(ob):
    return ob.__geo_interface__ 
    
def geo_json_dump(obj, fp, *args, **kwargs):
    import json
    #from shapely.geometry.geo import mapping
    """Dump shapely geometry object  :obj: to a file :fp:. as string """
    strobj = json.dumps(_mapping(obj), *args, **kwargs)
    fp.write(strobj)
    

def distance (p1,p2):
    from geographiclib.geodesic import Geodesic
    #return  Geodesic.WGS84.Inverse(39.435307, -76.799614, 39.43604, -76.79989)
    return  Geodesic.WGS84.Inverse(p1.y, p1.x, p2.y, p2.x)['s12']

def distanceList(points):
    dlist = []
    for i in range(1,len(points)):
        dlist.append(distance(points[i-1],points[i]))
        
    return dlist
    
    

def calculate_initial_compass_bearing(pointA, pointB):
    """
    Calculates the bearing between two points.
    The formulae used is the following:
        θ = atan2(sin(Δlong).cos(lat2),
                  cos(lat1).sin(lat2) − sin(lat1).cos(lat2).cos(Δlong))
    :Parameters:
      - `pointA: The tuple representing the latitude/longitude for the
        first point. Latitude and longitude must be in decimal degrees
      - `pointB: The tuple representing the latitude/longitude for the
        second point. Latitude and longitude must be in decimal degrees
    :Returns:
      The bearing in degrees
    :Returns Type:
      float
    """
    
    """
    We are doing the same as Geodesci.bearing ...
    """
    
    
    if (type(pointA) != tuple) or (type(pointB) != tuple):
        raise TypeError("Only tuples are supported as arguments")

    lat1 = math.radians(pointA[0])
    lat2 = math.radians(pointB[0])

    diffLong = math.radians(pointB[1] - pointA[1])

    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1)
            * math.cos(lat2) * math.cos(diffLong))

    initial_bearing = math.atan2(x, y)

    # Now we have the initial bearing but math.atan2 return values
    # from -180° to + 180° which is not what we want for a compass bearing
    # The solution is to normalize the initial bearing as shown below
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing


def bearing (p1,p2):
    from geographiclib.geodesic import Geodesic
    return Geodesic.WGS84.Inverse( p1.y, p1.x, p2.y, p2.x)['azi1']

def bearingProb(p1,p2,p3):
    '''
    Calculate the probability that a bearing is heading to point 3 over point 2 from point 1
    we normalise to 360 deg, and try to cater for going into the opposite direction 
    '''
    b1 = bearing(p1,p2)
    b3 = bearing(p1,p3)
    
    if b1 < 0:
        b1 += 360.
    if b3 < 0:
        b3 += 360.    
    # we multiply the deduction by 2 to move back to percent of 180 and not 360 as forced above
    # otherwise an opposed direction will still result in 50%
    try:
        bprob = abs(1. -  (abs(b1-b3) / 180.))
    except:
        bprob = np.nan
        
    return b1, b3 , bprob



def distanceAtBearing (p1, bearing, distance_m):
    from geographiclib.geodesic import Geodesic
    resdict = Geodesic.WGS84.Direct( p1.y, p1.x, bearing, distance_m) 
    return Point(resdict ['lon2'],resdict ['lat2'])

'''
def distanceSpeed (p1, p2, ts1, ts2):
    dist = distance(p1, p2)
    td = ts2 - ts1
    if dist == 0. or td.total_seconds() == 0:
        return 0. , 0. 
    return dist / td.total_seconds() , dist
'''
def distanceSpeed (p1, p2, ts1, ts2):
    return distanceSpeed2(distance(p1, p2),ts1, ts2)


def distanceSpeed2 (dist, ts1, ts2):
    
    td = ts2 - ts1
    if dist == 0. or td.total_seconds() == 0:
        return 0. , 0. 
    return dist / td.total_seconds() , dist


def calcSpeedDistance(dt: timedelta, p1: Point,p2:Point):
    '''
    return tsec, speed in m/sec , distance
    '''
    tsec  = dt.total_seconds()
    dist = distance(p1, p2)
    if dist == 0. or tsec == 0:
        return 0. , 0. , 0.
    return tsec , dist, dist / tsec
    
def point(coords):
    return Point(*coords)



def bufferSpherical(geom, distance_m): 
    # this should be less than 10 km, otherwise too much distortion
    centroid = geom.centroid
    epsg = findEPSGFromZone(lookupUTM(centroid.x,centroid.y))
    proj = pyproj.CRS('epsg:{}'.format(epsg))
    try:
        project = pyproj.Transformer.from_crs(epsg4326Proj, proj, always_xy=True).transform
        p1p = transform(project, geom)
        ppbuffer = p1p.buffer(distance_m)
        project2 = pyproj.Transformer.from_crs(proj,epsg4326Proj, always_xy=True).transform
        buffer =  transform(project2,ppbuffer)
        
        return buffer
    except Exception as e:
        logme(e)
        raise e
    
    
def centroidFromBBox(bbox):
    return Point(bbox[0]+(bbox[2]-bbox[0])/2,bbox[1]+(bbox[3]-bbox[1])/2)

def scaleFromPoint(point):
    '''
    figure out a generalised scale factor for this location
    we take the mean from x and y direction
    
    '''
    geom = bufferSpherical(point, 1000.)
    bbox = geom.bounds
    xdiff = bbox[2]-bbox[0]
    ydiff = bbox[3]-bbox[1]
    
    return (ydiff+xdiff)/2000.
    
    
    
    
    

def calcSinuosity(points):
    '''
    calculate the relationship between direct distance and distance traveled
    as distance-direct/distance traveled
    if not traveled - return NaN
    we ignore what projection the points are in, it has little influence on the relationship calculation
     
    '''
    ## we have geo points, they know how to define distance correctly
    ## and its implementation independent (shapeley,Qgs) 
    
    distd = math.fabs(points[-1].distance(points[0]))
    dista = 0.
    for i  in range(len(points)-1):
        dista += points[i].distance(points[i+1])
        
    '''
    distd = math.sqrt(math.pow(points[-1].x - points[0].x, 2)+ math.pow(points[-1].y - points[0].y, 2))
    dista = 0.
    for i in range(len(points)-1):
        dista += math.sqrt(math.pow(points[i+1].x - points[i].x, 2)+ math.pow(points[i+1].y - points[i].y, 2))
    '''  
    if dista == 0 or distd == 0.:
        return float('nan')
    return  distd / dista


def _assignRandomId():
    return randrange(1,sys.maxsize)






example = {"command":"set","group":"64520a257926140001438910",
           "detect":"inside",
           "hook":"PP.Kejawanan.2000.0m",
           "key":"vessel",
           "time":"2023-05-03T07:23:47.195723791Z",
           "id":"4299702",
           "object":{"type":"Point","coordinates":[108.5843,-6.7338]},"fields":{"ts":1640995320.0}
           }

    
def lookupUTM(lon, lat) :
    # find the UTM zone for a coordinate
    if lat >= 72.0 and lat < 84.0:
        if lon >= 0.0 and lon < 9.0:
            return 31
        if lon >= 9.0 and lon < 21.0:
            return 33
        if lon >= 21.0 and lon < 33.0:
            return 35
        if lon >= 33.0 and lon < 42.0:
            return 37
    if lat >= 56 and lat < 64.0 and lon >= 3 and lon <= 12:
        return 32
    
    zone = round((lon + 180) / 6) 
    return zone if lat >= 0 else zone  * -1
    
def findEPSGFromZone(zone) :
    
    #zone = getZones(longitude, latitude)   
    epsg_code = 32600
    epsg_code += int(abs(zone))
    if (zone < 0): # South
        epsg_code += 100    
    return epsg_code
    

def lineStringFeatureFromWKTPoints(wktkist, props = None):
    
    plist = [wkt.loads(p) for p in wktkist]
    
    return {'type': 'Feature',"properties":props if props else {}, 'geometry': LineString(plist).__geo_interface__ }
    

class GeometryMixin():
    
    def asWkt(self):
        return self.wkt
                
    def asPoint(self):
        self.centroid
                    
    def asJson(self):
        return json.dumps(self.__geo_interface__)
    
    

class Geom():
    '''
    A container holding one geometry to test against if a vessel trajectory potentially intersects with it.
    It in essence represents a working container of a Feature in  a FeaturesStore
    '''
    
    def __init__(self, geom, properties):
        
        self.geom = geom
        self.properties = properties#{k:v for k, v in properties.items()}
        self._name = properties.get("name",int(datetime.utcnow().timestamp()))
        


    def clone(self):
        return Geom(wkb.loads(wkb.dumps(self.geom)),deepcopy(self.properties))
        
    def __repr__(self):
        return "[{},{}]".format(self.geom.wkt,json.dumps(self.properties))
    
    @property
    def __geo_interface__(self):
        from pygeom import _exportJsonProperties
        return {"geometry": self.geom.__geo_interface__ ,
        "properties": _exportJsonProperties(self.properties)
        }
    
    @property
    def _feature(self):
        return _createFeatureShapely(self.geom,self.properties)
    
    @property
    def feature(self):
        return Feature(self.geom,self.properties)
    
    @property
    def centroid(self):
        return self.geom.centroid
    
    @property
    def geometry(self):
        return self.geom
    
    @property
    def type(self):
        return self.geom.geom_type
    
    
    def inside(self, point):
        return self.geom.contains(point)
    
    @property
    def name(self):
        return self._name
    
    
    
    def attribute(self, key):
        return self.properties.get(key,None)
    
    
    
from pygeom.utils import FeaturesStore
class Geometries(FeaturesStore):
    '''
    Collection of Geoms
    
    '''
    operations = ('intersects', 'within', 'contains', 'overlaps', 'crosses','touches', 'covers', 'covered_by', 'contains_properly')
    def __init__(self, name = "Geometries"):
        self._geoms = []
        self._tree = None
        self._name = name
        
        self._meta = None
        
        
    def setName(self,name):
        self._name = name
        
    def setMeta(self,metadata):
        if not self._meta is None:
            if str(self._meta.get("crs",None)) != str(metadata.get("crs",None)):
                raise ValueError("Attempting to merge geometries with different projections")
            
        else:
            self._meta = metadata
            
    def getMetaSchema(self, key):
        '''
        return the schema entry or None if not exists
        '''
        if self._meta is None:
            return None
        if key is None:
            return self._meta.get("schema",{})
        return self._meta.get("schema",{}).get(key,None)
        
    def getMetaDriver(self):
        if self._meta is None:
            return None
        return self._meta.get("driver",None)
    
    def name(self):
        return self._name 
    
    def CRS(self):
        if not self._meta is None :
            return self._meta.get("crs",None)
        return None
        
    def isValid(self):
        return len(self._geoms) > 0 and not self._tree is None
    
        
    def append(self, geom : Geom):
        if self._tree:
            return
        self._geoms.append(geom)
        
    def close(self):
        self._geoms = None
        self._tree = None
        
    def clone(self, empty = False):
        # return a deep copy of this collection without calling init
        ngeom  = Geometries()
        if not empty:
            ngeom._geoms = [g.clone() for g in self._geoms]
        ngeom._meta = {**self._meta}
        return ngeom
        
        
    def initIndex(self, reassignIDFunc= None ):
        if self._tree:
            return
        
        if not reassignIDFunc is None:
            for g in self._geoms:
                g.properties['id'] = reassignIDFunc()
        
        from shapely import STRtree
        self._tree = STRtree([g.geom for g in self._geoms])
        #print("stree  count {}".format(len(self._tree)))
        
        
    def testValid(self,predicate):
        if not predicate in Geometries.operations:
            raise ValueError(f"Unsupported operation '{predicate}'")
        
        if not self._tree:
            raise ValueError("Geometries not initialised")
        

    def operation(self, predicate, geom0):
        '''
        this should be done once not every time
        if not predicate in Geometries.operations:
            raise ValueError(f"Unsupported operation '{predicate}'")
        
        if not self._tree:
            raise ValueError("Geometries not initialised")
        '''
        indices = self._tree.query(geom0,predicate=predicate)
        return [self._geoms[i] for i in indices]
    
            
    def intersections(self, geom0):
        '''
        if not self._tree:
            raise ValueError("Geometries not initialised")
    
        indices = self._tree.query(geom0,predicate='intersects')
        return [self._geoms[i] for i in indices]
        '''
    
        return self.operation('intersects', geom0)
    
    
    def mergeAttribute(self, attr,attr2, funclist):
        if len(funclist) > 1:
            for g in self._geoms:
                vattr = g.properties[attr]
                vattrn = []
                for f in funclist:
                    if vattr:
                        vattrn.append(f(vattr))
                g.properties[attr2] = vattrn
        else:
            for g in self._geoms:
                vattr = g.properties[attr]
                g.properties[attr2] = funclist[0](vattrn)
                
                
            
    def applyAttributes(self, attr, func):
        for g in self._geoms:
            g.properties[attr] = func(g.properties)
            
        Geometries._applyAttributes( self._geoms, attr, func)
            
            
    def features(self):
        return [ g.feature for g in self._geoms]
    
    
    def hasGeomId(self,gid,attr='id'):
        #test if the geometry with this id already exists 
        for g in self._geoms:
            if g.attribute(attr) == gid:
                return True
            
        return False
        
    
    def geoms(self):
        return  self._geoms
        
    def __iter__(self):
        return iter(self._geoms)
        
    @staticmethod
    def _applyAttributes(geoms, attr, func):
        for g in geoms:
            g.properties[attr] = func(g.properties)
            
        
        
    @staticmethod
    def collectGeoms(geoms,geomspath, add_properties = {}):
        import fiona
        '''
        if not isinstance(geomspath,(list,tuple)):
            geomspath = [geomspath]
        ''' 
        if isinstance(geomspath,(list,tuple)):
            gg = fiona.open(geomspath[0],layer=geomspath[1])
            
        else:
            if str(os.path.split(geomspath)[1]).lower() == 'gpkg':
                geomlayers = fiona.listlayers(geomspath)
                for l in geomlayers:
                    gg = fiona.open(str(geomspath),layer=l)
                    geoms.setMeta(gg.profile)
                    for c in iter(gg):
                        props = c["properties"]
                        geoms._geoms.append(Geom(shape(c["geometry"]),{**props,**add_properties}))
                
            else:
                gg = fiona.open(geomspath)
                geoms.setMeta(gg.profile)
                for c in iter(gg):
                    props = c["properties"]
                    geoms._geoms.append(Geom(shape(c["geometry"]),{**props,**add_properties}))
        
    @staticmethod
    def buildInit(geomspath, name = 'Geometries',add_properties = {} ):
        geoms = Geometries(name)
        Geometries.collectGeoms(geoms, geomspath, add_properties)
        geoms.initIndex()
        return geoms          
    
    
    @staticmethod
    def collectMetadata(geomspath):
        import fiona
        
        with fiona.Env():
            try:
                if isinstance(geomspath,(list,tuple)):
                    gg = fiona.open(geomspath[0],layer=geomspath[1])
                else:
                    gg = fiona.open(geomspath)
                    
                    
                return gg.profile
                    
            except Exception as e:
                logme(f"Failed loading: {geomspath} {e}")

            
            
            
    def filterByAttributeFunc(self, func):
        '''
        filter the geometries by applying the func to the properties of aa feature
        return the list of geometries that match
        
        param - func, a function reference that takes a dict as argument ad returns a boolean like value
        
        '''
        filtered = []
        for g in self._geoms:
            if func(g.properties):
                filtered.append(g)
            
        return filtered
    
    
    def closest(self,geom, bufferm, attrs = None,doprojected = False):
        from shapely import shortest_line
        qppb = bufferSpherical(geom, bufferm)
        
        distances = list()
        fitems = list()
        
        # in case geom is not a point
        cppp = qppb.centroid
        
        intersectins = self.intersections(qppb)
        
        if not attrs is None:
            for g in intersectins:
                if doprojected:
                    distances.append(g.distance(cppp))
                else:
                    sline = shortest_line(g.geometry,cppp)
                    xy = list(sline.coordinates)
                    dd = distance(Point(xy[0]),Point(xy[1]))
                    distances.append(dd)
    
                fitems.append(",".join(["{}={}".format(a,g.properits(a)) for a in attrs  if a in g.properties ]))
            if len(distances) == 0:
                return None,None
            
            # return  whole metres, let the recipient deal with conversions
            minentry = min(distances)
            return round(minentry,3),fitems[distances.index(minentry)]
        
        else:
            for g in intersectins:
                if doprojected:
                    distances.append(g.distance(cppp))
                else:
                    sline = shortest_line(g.geometry,cppp)
                    xy = list(sline.coordinates)
                    dd = distance(Point(xy[0]),Point(xy[1]))
                    distances.append(dd)
    
            if len(distances) == 0:
                return None
            
            # return  whole metres, let the recipient deal with conversions
            minentry = min(distances)
            return round(minentry,3)
    
    def doWithin(self,p,doprojected = False):
        '''
        find the geometry a given geometry falls within and extracts the associated feature value (but not the  geometry)
        
        geom - the Geometry to relate to
        w_geom  - the list of Features
        
        The feature needs to have either a name atttrib or a name in the properties to assign the context correctly 
        
        '''
    
        ## epsg:4326, lat lon
        qbb = p.buffer(0.0001,8) # this will make a octagon - a very small one, it adds up to ~10 metre buffer around it
        
        reslist = []
        intersectins = self.intersections(qbb)
        for g in intersectins:

            name_key = g.name
            reslist.append("{}:{}".format(name_key,str(g.properties(name_key))))
                            
        return ",".join(reslist)
    
    
    def save(self, pathname, additionalProperties = None):
        import fiona
        #with fiona.Env():
        profile = {**self._meta}
        if additionalProperties:
            if isinstance(additionalProperties,dict):
                for a in additionalProperties:
                    profile['schema']['properties'][a]=additionalProperties[a]
            else:
                for a in additionalProperties:
                    profile['schema']['properties'][a]='str'
                    
                    
        with fiona.open(pathname, "w", **profile) as dst:
            ngeoms = len(self._geoms)
            print(f"Saving nfeatures {ngeoms}")
            for i,g in enumerate(self._geoms):
                dst.write(g.__geo_interface__)
                
                if i % 1000 == 0:
                    print(f"saved n features: {i}")
         


def find_min_y_point(list_of_points):
    """
    Returns that point of *list_of_points* having minimal y-coordinate
    :param list_of_points: list of tuples
    :return: tuple (x, y)
    """
    min_y_pt = list_of_points[0]
    for point in list_of_points[1:]:
        if point[1] < min_y_pt[1] or (point[1] == min_y_pt[1] and point[0] < min_y_pt[0]):
            min_y_pt = point
    return min_y_pt


def add_point(vector, element):
    """
    Returns vector with the given element append to the right
    """
    vector.append(element)
    return vector


def remove_point(vector, element):
    """
    Returns a copy of vector without the given element
    """
    vector.pop(vector.index(element))
    return vector


def euclidian_distance(point1, point2):
    """
    Returns the euclidian distance of the 2 given points.
    :param point1: tuple (x, y)
    :param point2: tuple (x, y)
    :return: float
    """
    return math.sqrt(math.pow(point1[0] - point2[0], 2) + math.pow(point1[1] - point2[1], 2))


def nearest_points(list_of_points, point, k):
    """
    gibt eine Liste mit den Indizes der k nächsten Nachbarn aus list_of_points zum angegebenen Punkt zurück.
    Das Maß für die Nähe ist die euklidische Distanz. Intern wird k auf das Minimum zwischen dem gegebenen Wert
    für k und der Anzahl der Punkte in list_of_points gesetzt
    :param list_of_points: list of tuples
    :param point: tuple (x, y)
    :param k: integer
    :return: list of k tuples
    """
    # build a list of tuples of distances between point *point* and every point in *list_of_points*, and
    # their respective index of list *list_of_distances*
    list_of_distances = []
    for index in range(len(list_of_points)):
        list_of_distances.append(( euclidian_distance(list_of_points[index], point), index))

    # sort distances in ascending order
    list_of_distances.sort()

    # get the k nearest neighbors of point
    nearest_list = []
    for index in range(min(k, len(list_of_points))):
        nearest_list.append((list_of_points[list_of_distances[index][1]]))
    return nearest_list


def angle(from_point, to_point):
    """
    Returns the angle of the directed line segment, going from *from_point* to *to_point*, in radians. The angle is
    positive for segments with upward direction (north), otherwise negative (south). Values ranges from 0 at the
    right (east) to pi at the left side (west).
    :param from_point: tuple (x, y)
    :param to_point: tuple (x, y)
    :return: float
    """
    return math.atan2(to_point[1] - from_point[1], to_point[0] - from_point[0])


def angle_difference(angle1, angle2):
    """
    Calculates the difference between the given angles in clockwise direction as radians.
    :param angle1: float
    :param angle2: float
    :return: float; between 0 and 2*Pi
    """
    if (angle1 > 0 and angle2 >= 0) and angle1 > angle2:
        return abs(angle1 - angle2)
    elif (angle1 >= 0 and angle2 > 0) and angle1 < angle2:
        return 2 * math.pi + angle1 - angle2
    elif (angle1 < 0 and angle2 <= 0) and angle1 < angle2:
        return 2 * math.pi + angle1 + abs(angle2)
    elif (angle1 <= 0 and angle2 < 0) and angle1 > angle2:
        return abs(angle1 - angle2)
    elif angle1 <= 0 < angle2:
        return 2 * math.pi + angle1 - angle2
    elif angle1 >= 0 >= angle2:
        return angle1 + abs(angle2)
    else:
        return 0


def intersect(line1, line2):
    """
    Returns True if the two given line segments intersect each other, and False otherwise.
    :param line1: 2-tuple of tuple (x, y)
    :param line2: 2-tuple of tuple (x, y)
    :return: boolean
    """
    a1 = line1[1][1] - line1[0][1]
    b1 = line1[0][0] - line1[1][0]
    c1 = a1 * line1[0][0] + b1 * line1[0][1]
    a2 = line2[1][1] - line2[0][1]
    b2 = line2[0][0] - line2[1][0]
    c2 = a2 * line2[0][0] + b2 * line2[0][1]
    tmp = (a1 * b2 - a2 * b1)
    if tmp == 0:
        return False
    sx = (c1 * b2 - c2 * b1) / tmp
    if (sx > line1[0][0] and sx > line1[1][0]) or (sx > line2[0][0] and sx > line2[1][0]) or\
            (sx < line1[0][0] and sx < line1[1][0]) or (sx < line2[0][0] and sx < line2[1][0]):
        return False
    sy = (a1 * c2 - a2 * c1) / tmp
    if (sy > line1[0][1] and sy > line1[1][1]) or (sy > line2[0][1] and sy > line2[1][1]) or\
            (sy < line1[0][1] and sy < line1[1][1]) or (sy < line2[0][1] and sy < line2[1][1]):
        return False
    return True


def point_in_polygon_q(point, list_of_points):
    """
    Return True if given point *point* is laying in the polygon described by the vertices *list_of_points*,
    otherwise False
    Based on the "Ray Casting Method" described by Joel Lawhead in this blog article:
    http://geospatialpython.com/2011/01/point-in-polygon.html
    """
    x = point[0]
    y = point[1]
    poly = [(pt[0], pt[1]) for pt in list_of_points]
    n = len(poly)
    inside = False

    p1x, p1y = poly[0]
    for i in range(n + 1):
        p2x, p2y = poly[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xints = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xints:
                        inside = not inside
        p1x, p1y = p2x, p2y

    return inside


def write_wkt(point_list, file_name):
    """
    Writes the geometry described by *point_list* in Well Known Text format to file
    :param point_list: list of tuples (x, y)
    :return: None
    """
    if file_name is None:
        file_name = 'hull2.wkt'
    if os.path.isfile(file_name):
        outfile = open(file_name, 'a')
    else:
        outfile = open(file_name, 'w')
        outfile.write('%s\n' % 'WKT')
    wkt = 'POLYGON((' + str(point_list[0][0]) + ' ' + str(point_list[0][1])
    for p in point_list[1:]:
        wkt += ', ' + str(p[0]) + ' ' + str(p[1])
    wkt += '))'
    outfile.write('%s\n' % wkt)
    outfile.close()
    return None


def as_wkt(point_list):
    """
    Returns the geometry described by *point_list* in Well Known Text format
    Example: hull = self.as_wkt(the_hull)
             feature.setGeometry(QgsGeometry.fromWkt(hull))
    :param point_list: list of tuples (x, y)
    :return: polygon geometry as WTK
    """
    wkt = 'POLYGON((' + str(point_list[0][0]) + ' ' + str(point_list[0][1])
    for p in point_list[1:]:
        wkt += ', ' + str(p[0]) + ' ' + str(p[1])
    wkt += '))'
    return wkt
    

def points_as_wktPolygon(point_list):
    """
    Returns the geometry described by *point_list* in Well Known Text format
    Example: hull = self.as_wkt(the_hull)
             feature.setGeometry(QgsGeometry.fromWkt(hull))
    :param point_list: list of tuples (x, y)
    :return: polygon geometry as WTK
    """
    wkt = 'POLYGON((' + str(point_list[0][0]) + ' ' + str(point_list[0][1])
    for p in point_list[1:]:
        wkt += ', ' + str(p[0]) + ' ' + str(p[1])
    wkt += '))'
    return wkt

def edges_as_wktMultiLine(edge_list):
    """
    Returns the geometry described by *point_list* in Well Known Text format
    Example: hull = self.as_wkt(the_hull)
             feature.setGeometry(QgsGeometry.fromWkt(hull))
    :param point_list: list of tuples (x, y)
    MULTILINESTRING ((30 20, 45 40, 10 40), (15 5, 40 10, 10 20))"
    :return: polygon geometry as WTK
    """
    
    wkt = 'MULTILINESTRING('
    edges = list()
    for e in edge_list:
        edges.append("( {} {} ".format(*e[0])+", {} {} )".format(*e[1]))
    
    wkt += ",".join(edges) 
    wkt += ')'
    return wkt

def edges_as_wktPoints(edge_list):
    """
    Returns the geometry described by *point_list* in Well Known Text format
    Example: hull = self.as_wkt(the_hull)
             feature.setGeometry(QgsGeometry.fromWkt(hull))
    :param point_list: list of tuples (x, y)
    MULTILINESTRING ((30 20, 45 40, 10 40), (15 5, 40 10, 10 20))"
    :return: polygon geometry as WTK
    """
    
    wkt = 'MULTIPOINT('
    edges = list()
    for e in edge_list:
        edges.append("({} {} )".format(*e[0])+",( {} {} )".format(*e[1]))
    
    wkt += ",".join(edges) 
    wkt += ')'
    return wkt

def points_as_wktPoints(pointlist):
    """
    Returns the geometry described by *point_list* in Well Known Text format
    Example: hull = self.as_wkt(the_hull)
             feature.setGeometry(QgsGeometry.fromWkt(hull))
    :param point_list: list of tuples (x, y)
    MULTIPOINT ((30 20), (15 5 ),( 40 10),( 10 20))"
    :return: multi point geometry as WTK
    """
    
    wkt = 'MULTIPOINT('
    edges = list()
    for e in pointlist:
        edges.append("({} {} )".format(*e))
    
    wkt += ",".join(edges) 
    wkt += ')'
    return wkt
    

def as_polygon(pointlist):
    from shapely import Polygon
    return Polygon(pointlist)

def sort_by_angle(list_of_points, last_point, last_angle):
    """
    gibt die Punkte in list_of_points in absteigender Reihenfolge des Winkels zum letzten Segment der Hülle zurück,
    gemessen im Uhrzeigersinn. Es wird also immer der rechteste der benachbarten  Punkte ausgewählt. Der erste
    Punkt dieser Liste wird der nächste Punkt der Hülle.
    """
    def getkey(item):
        return angle_difference(last_angle, angle(last_point, item))

    vertex_list = sorted(list_of_points, key=getkey, reverse=True)
    return vertex_list

def clean_list(list_of_points):
    """
    Deletes duplicate points in list_of_points
    """
    import numpy as np
    if type(list_of_points) is np.ndarray:
        return list(set(list_of_points.tolist()))
    s = set(list_of_points)
    l = list(s)
    return l
    #return list(set(list_of_points))

'''

way to slow
def concave_hull(points_list, k=3):
    """
    Calculates a valid concave hull polygon containing all given points. The algorithm searches for that
    point in the neighborhood of k nearest neighbors which maximizes the rotation angle in clockwise direction
    without intersecting any previous line segments.
    This is an implementation of the algorithm described by Adriano Moreira and Maribel Yasmina Santos:
    CONCAVE HULL: A K-NEAREST NEIGHBOURS APPROACH FOR THE COMPUTATION OF THE REGION OCCUPIED BY A SET OF POINTS.
    GRAPP 2007 - International Conference on Computer Graphics Theory and Applications; pp 61-68.
    :param points_list: list of tuples (x, y)
    :param k: integer
    :return: list of tuples (x, y)
    """
    # return an empty list if not enough points are given
    if k > len(points_list):
        return None

    # the number of nearest neighbors k must be greater than or equal to 3
    # kk = max(k, 3)
    kk = max(k, 2)

    # delete duplicate points
    point_set = clean_list(points_list)
    #point_set = list(points_list)

    # if point_set has less then 3 points no polygon can be created and an empty list will be returned
    if len(point_set) < 3:
        return None

    # if point_set has 3 points then these are already vertices of the hull. Append the first point to
    # close the hull polygon
    if len(point_set) == 3:
        return add_point(point_set, point_set[0])

    # make sure that k neighbours can be found
    kk = min(kk, len(point_set))

    # start with the point having the smallest y-coordinate (most southern point)
    first_point = find_min_y_point(point_set)

    # add this points as the first vertex of the hull
    hull = [first_point]

    # make the first vertex of the hull to the current point
    current_point = first_point

    # remove the point from the point_set, to prevent him being among the nearest points
    point_set = remove_point(point_set, first_point)
    previous_angle = math.pi

    # step counts the number of segments
    step = 2

    # as long as point_set is not empty or search is returning to the starting point
    while (current_point != first_point) or (step == 2) and (len(point_set) > 0):

        # after 3 iterations add the first point to point_set again, otherwise a hull cannot be closed
        if step == 5:
            point_set = add_point(point_set, first_point)

        # search the k nearest neighbors of the current point
        k_nearest_points = nearest_points(point_set, current_point, kk)

        # sort the candidates (neighbors) in descending order of right-hand turn. This way the algorithm progresses
        # in clockwise direction through as many points as possible
        c_points = sort_by_angle(k_nearest_points, current_point, previous_angle)

        its = True
        i = -1

        # search for the nearest point to which the connecting line does not intersect any existing segment
        while its is True and (i < len(c_points)-1):
            i += 1
            if c_points[i] == first_point:
                last_point = 1
            else:
                last_point = 0
            j = 2
            its = False

            while its is False and (j < len(hull) - last_point):
                its = intersect((hull[step-2], c_points[i]), (hull[step-2-j], hull[step-1-j]))
                j += 1

        # there is no candidate to which the connecting line does not intersect any existing segment, so the
        # for the next candidate fails. The algorithm starts again with an increased number of neighbors
        if its is True:
            return concave_hull(points_list, kk + 1)

        # the first point which complies with the requirements is added to the hull and gets the current point
        current_point = c_points[i]
        hull = add_point(hull, current_point)

        # calculate the angle between the last vertex and his precursor, that is the last segment of the hull
        # in reversed direction
        previous_angle = angle(hull[step - 1], hull[step - 2])

        # remove current_point from point_set
        point_set = remove_point(point_set, current_point)

        # increment counter
        step += 1

    all_inside = True
    i = len(point_set)-1

    # check if all points are within the created polygon
    while (all_inside is True) and (i >= 0):
        all_inside = point_in_polygon_q(point_set[i], hull)
        i -= 1

    # since at least one point is out of the computed polygon, try again with a higher number of neighbors
    if all_inside is False :#and MAX_K > kk:
        return concave_hull(points_list, kk + 1)

    # a valid hull has been constructed
    return hull
    


def concave(xys, alpha):
    """
    calculate the concave hill over points in xys
    return Feature object
    
    """
    import numpy as np
    from pygeom import Feature
    if type(xys) is np.ndarray:
        xys = xys.tolist()
    
    hullpoints = concave_hull(xys)
        
    class Fakelayer():
        def __init__(self,features):
            self.features = features
            
            
        def getFeatures(self):
            return self.features

    geom = as_polygon(hullpoints)
    return [Feature(geom)]

    
'''

def convex (xys, alpha):
    geom = as_polygon(xys)
    return geom.convex_hull
    
    
def concave(xy2, alpha):

    from concave_hull import (  # noqa: F401
        concave_hull
    )
    
    hull = concave_hull(xy2,length_threshold=40)
    # close the polygon
    hull.append(hull[0])
    #from pygeom.geom import as_polygon
    from pygeom import Feature
    
    return [Feature(as_polygon(hull))]




def geometryFromWkt(wkt):
    from shapely.wkt import loads
    return loads(wkt)


    
       
def createEmptyGeometry(name):
    geom = None
    if 'multipoint' in name:
        geom = MultiPoint()
    elif 'point' in name:
        geom = Point()
    elif 'linestring' in name:
        geom = LineString()
    elif 'multipolygon' in name:
        geom = MultiPolygon()
    elif 'polygon' in name:
        geom = Polygon()
    
    return geom
        

def createGeometry(name,coords,properties= None):
    
    geom= None
    if len(coords)  < 2:
        print ("wrong type of point coordinate: "+str(type(coords))+" "+str(coords))
    
    if 'multipoint' in name:
        geom = MultiPoint(coords)
    elif 'point' in name:
        geom = Point(*coords)
    elif 'linestring' in name:
        geom = LineString(coords)
    elif 'multipolygon' in name:
        geom =MultiPolygon(coords)
    elif 'polygon' in name:
        geom = Polygon(coords)
    
    if 'feature' in name:
        from pygeom import Feature
        feat = Feature()
        feat.setGeometry(geom)
        
        if not properties is None:
            feat.properties(properties)
        return feat
    
    return geom




##### Functions to calculate relationships


def closestAny(geom,target_geoms,buffer=5000,doprojected=False):
    
    '''
    find the closest geometry to a given geometry and extract the associated feature values  
    
    geom - the Geometry (point) to relate to
    buffer - the buffer to pad with, essentially the allowable max distance to search within
    target_geoms  - the list of Features or geometry containers that have  properties 
    attr - the attributes to collect from the properties
    
    returns - distance and dict representing the key/ values requested if available - 'None,None' if nothing is found
    
    '''
    
    attrs = {}
    
    if isinstance(target_geoms,(list,tuple)) and isinstance(target_geoms[0],(list,tuple)):        
        target_geoms = target_geoms[0]
    
    
    if isinstance(target_geoms,(list,tuple)):
        fstore = target_geoms[0]
        # the second is an instance of CacheManager
        attrs = target_geoms[-1].subsetAttr(target_geoms[-1].attrKeys())
    elif isinstance(target_geoms, FeaturesStore):
        fstore = target_geoms
    else:
        raise ValueError(f"expecting list or single instance of 'FeaturesStore' don't know what to do with {target_geoms}")
      
      
    if hasattr(fstore, "closestAny"):
        return fstore.closestAny(geom,buffer,attrs,doprojected)
      
    #from shapely.ops import nearest_points
    from shapely import shortest_line
    if doprojected:
        qppb = shapely.buffer(geom,buffer)
    else:
        qppb = bufferSpherical(geom, buffer)
    
    distances = list()
    fitems = list()
    
    # in case geom is not a point
    cppp = qppb.centroid
    
    inter_geoms = fstore.intersections(qppb)
    
    for feat in inter_geoms:
  
        if doprojected:
            distances.append(feat.distance(cppp))
        else:
            sline = shortest_line(feat.geometry,cppp)
            #dd = distance(*sline.coordinates)
            x,y = sline.xy
            dd = distance(Point(x[0],y[0]), Point(x[-1],y[-1]))
            distances.append(dd)
            
        #fitems.append(",".join(["{}={}".format(a,feat.attribute(a)) for a in attrs  if a in feat.properties ]))
        fitems.append({a:feat.attribute(attrs[a]) for a in attrs  if isinstance(attrs[a],str) and attrs[a] in feat.properties })
            
    if len(distances) == 0:
        return None,None
    
    # return  whole metres, let the recipient deal with conversions
    minentry = min(distances)
    return round(minentry,3),fitems[distances.index(minentry)]
            

    return None,None


def closestDistance(geom,target_geoms,buffer = 5000,doprojected= False):
        
    '''
    find the closest geometry to a given geometry and extract the associated feature values  
    
    geom - the Geometry (point) to relate to
    buffer - the buffer to pad with, essentially the allowable max distance to search within
    target_geoms  - the list of Features or geometry containers that have  properties 
    
    '''
    if hasattr(target_geoms, "closest"):
        return target_geoms.closest(geom,buffer,None,doprojected)
        
    if isinstance(target_geoms,(list,tuple)):
        fstore = target_geoms[0]
    elif isinstance(target_geoms, FeaturesStore):
        fstore = target_geoms
    else:
        raise ValueError(f"expecting list or single instance of 'FeaturesStore' don't know what to do with {target_geoms}")
      
      
    #from shapely.ops import nearest_points
    from shapely import shortest_line
    if doprojected:
        qppb = shapely.buffer(geom,buffer)
    else:
        qppb = bufferSpherical(geom, buffer)
        
    distances = list()
    fitems = list()
    
    
    inter_geoms = fstore.intersections(qppb)
    
        
    # in case geom is not a point
    cppp = qppb.centroid
    
    if len(inter_geoms) > 0:
        for feat in inter_geoms:

            if doprojected:
                distances.append(feat.distance(cppp))
            else:
                sline = shortest_line(feat.geometry,cppp)
                #first, last = sline.boundary
                x,y = sline.xy
                dd = distance(Point(x[0],y[0]), Point(x[-1],y[-1]))
                distances.append(dd)
                    
                    
        if len(distances) == 0:
            return None
        
        # return  whole metres, let the recipient deal with conversions
        minentry = min(distances)
        return round(minentry,3)
            
    return None



def closestSegmentDistance(geom_ls,target_geoms,buffer = 5000,doprojected= False):
    '''
    find the closest geometry to a given linestring geometry and extract the associated feature values  
    
    geom - the Geometry (point) to relate to
    buffer - the buffer to pad with, essentially the allowable max distance to search within
    target_geoms  - the list of Features or geometry containers that have  properties 
    
    '''
    
    #from shapely.ops import nearest_points
    from shapely import shortest_line
    from shapely.ops import nearest_points
    distances = list()
    fitems = list()
    
    if isinstance(target_geoms,(list,tuple)):
        fstore = target_geoms[0]
    elif isinstance(target_geoms, FeaturesStore):
        fstore = target_geoms
    else:
        raise ValueError(f"expecting list or single instance of 'FeaturesStore' don't know what to do with {target_geoms}")
    
    if doprojected:
        qppb = shapely.buffer(geom_ls,buffer)
    else:
        qppb = bufferSpherical(geom_ls, buffer)
        
    inter_geoms = fstore.intersections(qppb)
    if len(inter_geoms) > 0:
                
        # we do expect a single store , not a potential list of stores
        for feat in inter_geoms:
            nseg = nearest_points(feat.geometry,geom_ls)
    
            if doprojected:
                distances.append(nseg[0].distance(nseg[1]))
            else:
                dd = distance(nseg[0],nseg[1])
                distances.append(dd)
                    
                    
        if len(distances) == 0:
            return None
    
        # return  whole metres, let the recipient deal with conversions
        minentry = min(distances)
        return round(minentry,3)
    
    
            
    return None
    

def doWithin(geom,w_geom,doprojected=False):
    '''
    find the geometry of a given geometry falls within and extracts the associated feature value (but not the  geometry)
    
    w_geom - the Geometry and optionally a cache manager to relate to as list of lists
    geom  - the geometry to test
    
    The feature needs to have either a name atttrib or a name in the properties to assign the context correctly
    The cache manager alternative may have an id and name enrry 
    
    '''
    from pygeom.utils import CacheManager
    
    
    if isinstance(w_geom,(list,tuple)) :
        if not isinstance(w_geom[0],(list,tuple)):
            fstores = [w_geom]
        else:
            fstores = w_geom
    elif isinstance(w_geom, FeaturesStore):
        fstores = [w_geom, None]
    else:
        raise ValueError(f"expecting list or single instance of 'FeaturesStore' and CacheManager - don't know what to do with {w_geom}")
        

    #this is to create a small polygon
    if doprojected:
        qbb = bufferSpherical(geom, 10)
    else:
        ## epsg:4326, lat lon
        qbb = geom.buffer(0.0001,8) # this will make a octagon - a very small one, it adds up to ~10 metre buffer around it
    
    reslist = []
    
        
    for tg in fstores:
        #tg is a FeatureStore
        inter_geoms = tg[0].intersections(geom)
        if len(inter_geoms) > 0:
            if not tg[1] is None and isinstance(tg[1],CacheManager) :
                name_key = tg[1].attrMap("name")               
            elif hasattr(tg, 'attrname'):
                if callable(tg.attrname):
                    name_key = tg.attrname()
                else:
                    name_key = tg.attrname

            else:
                raise ValueError(f"Feature has no accessible name {tg}")
            
            for g in inter_geoms:
                reslist.append("{}:{}".format(name_key,str(g.attribute(name_key))))
                        
    return ",".join(reslist)
        
        
        
        
        
def findFirst(p,w_geom_collection , buffer = 5000, reverse = True, doprojected = False):
    '''
    There may be more than one dataset, so iterate through starting from the back if reversed is true
    Every entry in the collection may have its own set of attributes to be mapped
    
    '''  

    if reverse:
        #for i in reversed(range(len(w_geom_collection))):
        for wg in reversed(w_geom_collection):
            
            #rs = closestAny(p,w_geom_collection[i],collection_attrs[i],buffer,doprojected)
            dist,rs = closestAny(p,wg,buffer,doprojected)
            if not rs is None:
                return dist,rs
    else:
        for wg in w_geom_collection:
            dist,rs = closestAny(p,wg,buffer,doprojected)
            if not rs is None:
                return dist,rs 
            
            
    return None, None


def union(inputgeom, overlaygeoms, outputgeom, buffer = 0.005, id_attrib= 'id',migrateAttr = []):
    '''
    
    select all geometries in input geom that overlap with overlay, in essence a union of 2 geometries.
    If overlay is a point geometry we add a tiny buffer or 'buffer' if not negative
    
    migrateAttr - if not none/empty, copythe attributes from overlay to input
    
    '''
    
    ingeoms = Geometries.buildInit(inputgeom)
    overgeoms = Geometries.buildInit(overlaygeoms)
    
    outputgeoms = ingeoms.clone(True)
    count = 0
    reported = 0
    totallen = len(overgeoms.geoms())
    
    
    migrateAttrTypes = None
    # extract the additional property dtypes to pass to the save call
    if migrateAttr:
        migrateAttrTypes = {}
        proptypes  = overgeoms.getMetaSchema("properties")
        # blows if None
        for a in migrateAttr:
            migrateAttrTypes[a] = str(proptypes[a])
    
    if overgeoms.getMetaSchema('geometry') == 'Point' and buffer > 0:
        # 0.005 deg assume epsg:4326
        print (f"Union with buffer {buffer}")
        for go in overgeoms.geoms():
            intersectgeoms = ingeoms.intersections(go.geometry.buffer(buffer))
            for igeo in intersectgeoms:
                if not outputgeoms.hasGeomId(igeo.attribute(id_attrib), id_attrib):
                    clobeigo = igeo.clone()
                    for a in migrateAttr:
                        clobeigo.properties[a] = go.attribute(a)
                    outputgeoms.append(clobeigo)
            count+=1
            percent =count/totallen*100
            if int(percent) % 5 == 0 and reported < int(percent):
                reported= int(percent)
                print(f"Tested {int(percent)}% of {totallen}")
    else:
        print (f"Union without buffer")
        for go in overgeoms.geoms():
            intersectgeoms = ingeoms.intersections(go.geometry)
            for igeo in intersectgeoms:
                if not outputgeoms.hasGeomId(igeo.attribute(id_attrib), id_attrib):
                    clobeigo = igeo.clone()
                    for a in migrateAttr:
                        clobeigo.properties[a] = go.attribute(a)
                    outputgeoms.append(clobeigo)
        
            count+=1
            percent =count/totallen*100
            if int(percent) % 5 == 0  and reported < int(percent):
                reported= int(percent)
                print(f"Tested {int(percent)}% of {totallen}")

    
    outputgeoms.save(outputgeom,migrateAttrTypes)
    
    


