

def main(flist,gtypes,outformat,transferPojection = None):
    #from csiro.geom import GeometryWrapper
    #from csiro.geom.processing import DumpGeoJsonQueueThread
    
    from pygeom.process.tools import DumpGeoJsonQueueThread
    from pygeom.geom import Geometries
    import fiona
    from queue import Queue
        
    typedict = {}
    typedumps = []
    typedumpfiles = []
    for k in gtypes:
        outqueue = Queue(1000)
        typedict[k] = outqueue
        dumpthread = DumpGeoJsonQueueThread(outqueue,outformat.format(k))
        if transferPojection:
            dumpthread.setInvProjection(transferPojection)
        typedumps.append(dumpthread)
        typedumpfiles.append(dumpthread.outf)
        dumpthread.start()
        
    
    
    
    for f in flist:
        if str(f) in typedumpfiles:
            continue
        if f.suffix.lower() == ".gpkg":
            geomlayers = fiona.listlayers(str(f))
            for l in geomlayers:
                print("processing {} from {} ".format(l,str(f)))
                geom = Geometries()
                Geometries.collectGeoms(geom, [str(f),l])

                for g in geom.geoms():
                    if g.type in typedict:
                        typedict[g.type].put([g.geometry,g.feature()]) 
                
                geom.close()
                
                
        else:            
            print("processing {} ".format(str(f)))
            geom = Geometries()
            Geometries.collectGeoms(geom,str(f))
            for g in geom.geoms():
                    if g.type in typedict:
                        typedict[g.type].put([g.geometry,g.feature()]) 
            geom.close()
    
    
    for d in typedumps:
        d.close()
    
    



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
       
    if len(sys.argv) > 2:
        dlist = list(pdir.glob(sys.argv[2]))
    else:
        dlist = list(pdir.glob("**/*.json"))
    
    print("processing {} files from dir {} ".format(len(dlist),str(pdir)))
    
    geotypes = ["point",'linestring',"polygon"]
    if len(sys.argv) > 3:
        geotypes = sys.argv[3].split(',')
    
    
        
    #"type": "LineString"
    main(dlist,geotypes,str(pdir.joinpath("aggregated_types-{}.json")))