import json,os, sys,math

from shapely import wkt
from shapely.geometry import (
     shape,Point,
     LineString, MultiPoint, 
     MultiPolygon, Polygon
     )
from shapely.ops import transform
import pyproj
from typing import List, Dict
from random import randrange


from pygeom import logme

from datetime import datetime, date

epsg4326Proj = pyproj.CRS('epsg:4326')

from geographiclib.geodesic import Geodesic

DOTRACK = False


def distance (p1,p2):
    #return  Geodesic.WGS84.Inverse(39.435307, -76.799614, 39.43604, -76.79989)
    return  Geodesic.WGS84.Inverse(p1.y, p1.x, p2.y, p2.x)['s12']

def distanceList(points):
    dlist = []
    for i in range(1,len(points)):
        dlist.append(distance(points[i-1],points[i]))
        
    return dlist
    
    

def bearing (p1,p2):
    return Geodesic.WGS84.Inverse( p1.y, p1.x, p2.y, p2.x)['azi1']

def bearingProb(p1,p2,p3):
    b1 = bearing(p1,p2)
    b3 = bearing(p1,p3)
    return b1, b3 , b1 / b3



def distanceAtBearing (p1, bearing, distance_m):
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
    
def point(coords):
    return Point(*coords)

def bufferSpherical(p1, distance_m): 
    # this should be less than 10 km, otherwise too much distortion
    epsg = findEPSGFromZone(lookupUTM(p1.x,p1.y))
    proj = pyproj.CRS('epsg:{}'.format(epsg))
    try:
        project = pyproj.Transformer.from_crs(epsg4326Proj, proj, always_xy=True).transform
        p1p = transform(project, p1)
        ppbuffer = p1p.buffer(distance_m)
        project2 = pyproj.Transformer.from_crs(proj,epsg4326Proj, always_xy=True).transform
        buffer =  transform(project2,ppbuffer)
        
        return buffer
    except Exception as e:
        logme(e)
        raise e
    

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
    

    
    

class Geom():
    '''
    A container holding one geometry to test against if a vessel trajectory potentially intersects with it
    '''
    
    def __init__(self, geom, properties):
        
        self.geom = geom
        self.properties = properties#{k:v for k, v in properties.items()}
        self._name = properties.get("name",int(datetime.utcnow().timestamp()))
        

        
    def __repr__(self):
        return "[{},{}]".format(self.geom.wkt,json.dumps(self.properties)) 
    
    @property
    def _feature(self):
        return _createFeatureShapely(self.geom,self.properties)
    
    @property
    def centroid(self):
        return self.geom.centroid
    
    @property
    def geometry(self):
        return self.geom
    
    
    def inside(self, point):
        return self.geom.contains(point)
    
        
    
    
    
class Geometries():
    '''
    Collection of Geoms
    
    '''
    def __init__(self):
        self._geoms = []
        self._tree = None
        
    def append(self, geom : Geom):
        if self._tree:
            return
        self._geoms.append(geom)
        

        
        
    def init(self, reassignIDFunc= None ):
        if self._tree:
            return
        
        if not reassignIDFunc is None:
            for g in self._geoms:
                g.properties['id'] = reassignIDFunc()
        
        from shapely import STRtree
        self._tree = STRtree([g.geom for g in self._geoms])
        #print("stree  count {}".format(len(self._tree)))
        
        
    def intersections(self, geom0):
        
        if not self._tree:
            raise ValueError("ApproachGeometries not initialised")
    
        indices = self._tree.query(geom0,predicate='intersects')
        return [self._geoms[i] for i in indices]
    
    
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
            
    @staticmethod
    def _applyAttributes(geoms, attr, func):
        for g in geoms:
            g.properties[attr] = func(g.properties)
        
            
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
    
    def features(self):
        return [ g._feature for g in self._geoms]
        
    
    
    @staticmethod
    def collectGeoms(geoms,geomspath, add_properties = {}):
        import fiona
        if not isinstance(geomspath,(list,tuple)):
            geomspath = [geomspath]
        for g in geomspath:
            for c in iter(fiona.open(str(g),'r')):
                props = c["properties"]
                geoms._geoms.append(Geom(shape(c["geometry"]),{**props,**add_properties}))
        
    
    

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

def convex (xys, alpha):
    geom = as_polygon(xys)
    return geom.convex_hull()
    

def concave(xys, alpha):
    '''
    calculate the concave hill over points in xys
    return Feature object
    
    '''
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

