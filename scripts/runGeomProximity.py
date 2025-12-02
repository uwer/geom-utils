import os,sys,argparse

packagef= os.path.dirname(os.path.dirname(__file__))
if not packagef in sys.path:
    sys.path.append(packagef)


    
from pygeom.process.tools import filterByProximity

from pygeom.geom import Geometries, createTransferProj,geo_json_dump,METRIC_PROCEJTION
from pygeom import Feature,FeatureCollection

def __createArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('source', type=str, default='', help='File reference to source vector geometry')
    parser.add_argument('target', type=str, default='', help='File reference to target vector geometry')
    parser.add_argument('distance', type=float, help='Distance of the geometries to be included, note that the unit must correspond to the projection of the source')
    parser.add_argument('output', type=str, default='', help='File reference to write the output geometry')
    parser.add_argument('--store-projected',action='store_true', help='Save the filtered geometry in the source projection')
    parser.add_argument('--test-only',action='store_true', help='Perform a test run, only the first 1000 geometries are processed, the output is undefined')
    
    return parser

def main(opt):
    
    sourceGeom = Geometries()
    Geometries.buildInit(sourceGeom,opt.source)
    from pyproj import Transformer
    from pyproj.enums import TransformDirection
    
    def createBuffer(distance, transformer, geometry):
        if transformer:
            g2 = transforms.transform(geometry)
            g3 = g2.buffer(distance)
            return transforms.transform(g3,direction=TransformDirection.INVERSE)
        return geometry.buffer(distance)
        
        
        
    archGeom = Geometries()
    Geometries.buildInit(archGeom,opt.target)#)
    #archGeom.optimised()
    toSourceProj = None
    
    if  str(sourceGeom.CRS()) != str(archGeom.CRS()):
        toSourceProj = createTransferProj(sourceGeom.CRS(),archGeom.CRS())
        
    if not "+units=m" in str(archGeom.CRS()):
        distanceFunc = partial(createBuffer,opt.distance,createTransformer(archGeom.CRS(),METRIC_PROCEJTION))
    else:
        distanceFunc = partial(createBuffer,opt.distance,None)
    
    reslist = filterByProximity(archGeom.features(),sourceGeom,distanceFunc,toSourceProj,opt.store_projected,opt.test_only)
    
    
    os.makedirs(os.path.dirname(opt.output), exist_ok=True)
    
    if len(reslist) < 1:
        print ("no results")
        return
    
    if isinstance(reslist[0],Feature):
        fcres = reslist
    else:
        fcres= list()
        for g in reslist: 
            fcres.append(Feature(g))
            
    fc2 = FeatureCollection(fcres)
    
    
    with open(opt.output,'w') as fp2:
        if toSourceProj:
            geo_json_dump(fc2,fp2,**{'crs':sourceGeom.CRS.definition_string()})
        else:
            geo_json_dump(fc2)
        
        
        
if __name__ == "__main__":
    
    if len(sys.argv) < 4:
        print('require source and test geometries, output as well as proximity distance in the source unit')
        sys.exit(1)
    '''
    outfile = '/Users/ros260/projects/tmp/geoprocessing/process/indonesia_roads_epsg3857.geojson'
    infile = '/Users/ros260/projects/data/backGroundData/shapefiles/indonesia_infra/gis_osm_roads_free_1.shp'
    '''
    parser = __createArgs()
    opt = parser.parse_args()
    main(opt)
    