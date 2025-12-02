


attributemap= {"man_made":"pier","code":"5303|5302|5301","fclass":"pier|slipway|marina","leisure":"slipway|marina"}

def batch(pdir,extensions):
        
    import fiona
    from pygeom.process.tools import FilterByAtrribute
    outfile_pattern="{}-{}marine_infra.json"
    flists = []
    plist = []
    
    for e in extensions:
        flists.extend(pdir.glob(e))
        
    # expecting list of Path objects
    for f in flists:
        bname = f.stem
        print(f.suffix.lower())
        if f.suffix.lower() == '.gpkg':
            geomlayers = fiona.listlayers(str(f))
            for l in geomlayers:
                filter = FilterByAtrribute([str(f),l],f.parent.joinpath(outfile_pattern.format(bname,l)),attributemap)
                filter.start()
                plist.append(filter)
            
            
        else:
            filter = FilterByAtrribute(str(f),f.parent.joinpath(outfile_pattern.format(bname,"")),attributemap)
            filter.start()
            plist.append(filter)
        
        
        
    for p in plist:
        p.join()
        
    
    
    



if __name__ == "__main__":
    from pathlib import Path
    import sys, os
    
    
    if len(sys.argv) < 2:
        print("need at least parent directory")
        sys.exit(1)
        
    localpath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    if not localpath in sys.path:
        sys.path.append(localpath)
        
        
    pdir = Path(sys.argv[1])
    dlist = pdir.glob("*")
    
    scanfiles = ["gis_osm_traffic_free_1.shp","gis_osm_transport_free_1.shp","*.gpkg"]
    if len(sys.argv) > 2:
        scanfiles= sys.argv[2].split(',')
    
    
    print("applying batch on {}".format(scanfiles))
    for d in dlist:
        if not d.is_dir():
            continue
        
        batch(d,scanfiles)
        
    
        
    
    