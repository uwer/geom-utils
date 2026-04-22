

def testFGB(inpath):
    
    from pygeom.geom import Geometries
    from pathlib import Path
    geoms = Geometries()
    Geometries.collectGeoms(geoms, inpath)
    geoms.setMetaDriver('FlatGeobuf')
    geoms.save(Path(inpath).with_suffix(".fgb"))
    
    
#testFGB("/var/folders/f6/xzsk_6d11sq92rxzrk6km7qh0000gp/T/1fe02fc8-4e8/de79f7/Vessel_503555300_pointgeom.json")

testFGB("/var/folders/f6/xzsk_6d11sq92rxzrk6km7qh0000gp/T/f6ffe648-ce0/29ad7b/Vessel_503555300_linegeom.json")