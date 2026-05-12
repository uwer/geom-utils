
import sys,json,os
from pathlib import Path

from pygeom import stopwatch, mergeCSV

def scanDF(df, columnsDefs, groupby , geompath = None , outpath = None, options = {}):
    groups = {}
    geometriestest = None
    
    from pygeom.geom import createGeometry
    
    if not  geompath is None:
        from pygeom.geom import Geometries
        geometriestest = Geometries.buildInitMerge(geompath)
        
    
    
    dfgroupby = df.groupby(groupby)
    
    for dfgroup in dfgroupby:
        dfgmeta = {}
        groups[dfgroup[0]] = dfgmeta
        dfg = dfgroup[1]
        
        print(dfgroup[0])
        
        dstat = dfg[columnsDefs].describe()
        dfgmeta['statistics'] = json.loads(dstat.to_json())
        print(dstat.to_json())
        
        lonstats = dfgmeta['statistics'][columnsDefs[0]]
        lonbetween  = dfg[columnsDefs[0]].between(lonstats["25%"],lonstats["75%"])
        #print(lonbetween[lonbetween == True].count())
        
        latstats = dfgmeta['statistics'][columnsDefs[1]]
        latbetween  = dfg[columnsDefs[1]].between(latstats["25%"],latstats["75%"])
        #print(latbetween[latbetween == True].count())
        
        overlapping  = dfg[lonbetween &  latbetween]
        d2stat = overlapping.describe()
        dfgmeta['statistics-50%'] = json.loads(d2stat.to_json())
        
        if not geometriestest is None:
            bbox  = [[d2stat[columnsDefs[0]]["min"],d2stat[columnsDefs[1]]["min"]],[d2stat[columnsDefs[0]]["max"],d2stat[columnsDefs[1]]["max"]]]
            bbox.append(bbox[0])
            intersects = geometriestest.intersections(createGeometry( "polygon",bbox).buffer(0.1))
            dfgmeta["proximity"] = []
            for g in intersects:
                dfgmeta["proximity"].append(g.properties)
            
    
    if not outpath is None:
        outpp = Path(outpath)
        outpp.mkdir(parents = True, exist_ok = True)
            
        with outpp.joinpath("stats.json").open('w') as fp:
            json.dump(groups, fp)
        


def collectMeta( dfpaths,columnsDefs,groupby, wildcard = None,  geompath = None , outpath = None, options = {}):

    with stopwatch(f"Process subset on filter {filter}"):
            
        if not wildcard is None:
            with stopwatch("merging files"):
                df,nf  = mergeCSV(dfpaths,wildcard)
                df.info()
                
            if not outpath is None:
                bname = os.path.basename(dfpaths)
                outpath = os.path.join(outpath,bname)
        else:
            # assuming a file
            df = pd.read_csv(dfpaths)
    

    return scanDF(df, columnsDefs, groupby,geompath=geompath,outpath = outpath , options=options )


if __name__ == "__main__":
    
    
    if len(sys.argv) > 1:
        with open(sys.argv[1],'r') as fp:
            config = json.load(fp)
            
        config = config["params"]
        collectMeta(**config)
    
    
    else:
        ports = ["/Users/ros260/projects/data/backGroundData/qgis/v2/userdata/nsw/port_nsw/port_nsw.fgb",
                 "/Users/ros260/projects/data/backGroundData/qgis/v2/userdata/oz_ports/oz_ports.v2.fgb",
                 "/Users/ros260/projects/data/backGroundData/qgis/v2/userdata/qld/ports_qld/qld_ports.fgb"]
        
        collectMeta("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/2024all",
                    ["Lon","Lat","MsgTime"],
                    "MMSI",
                    wildcard = "5*.csv",
                    outpath = "/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/stats",
                    geompath = ports,
                    options = {})