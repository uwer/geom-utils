"""
Extract weekly subsets of the data and count the occurrence of events (Points) within the provided geometries. Add the count and temporal stats as attribute to the geometry.

The process will produce multiple geometries at the same location for different weeks. Geometries that do have a count of 0 are removed.
We keep a track on the first and last match as well as calculate the mean timestamp for the occurrences
 
I.e. - point appears inside polygon, increment count by one for said polygon for that week.  

"""


from pygeom.geom import Geom, Geometries,_assignRandomId
from pygeom import stopwatch, mergeCSV,_createFeaturCollection
import pandas as pd
import numpy as np
from pathlib import Path
import json, sys, os
from functools import partial
from copy import deepcopy



def extractWeeks(geompath,df,column_defs,pdump = None, options={}):
    '''
    extract weekly subsets of the data and count the occurrences of events in the provided geometries.
    if we have a "matchid" we also produce a dataframe with the results per week, this will create a substantial time and memory increase
    
    geompath - a reference to the geometries we will match to
    df - the pandas dataframe with data to scan
    column_defs - a list of [x,y, timestamp] columns to in the dataframe
    pdump - out path
    
    result - a collection of geometries with attributes added to reflect the weekly count, there will be a geometry in the same location for every week!
    if we also produce a dataframe, there will be a column for every week matching the matchid as row indentifier. Additional summaries per 12 weeks and annual are added at the end
    
    '''

    dminmax = df[column_defs[-1]].agg(['min', 'max'])
    from dateutil import rrule
    mrange = list(rrule.rrule(rrule.WEEKLY, dtstart=dminmax["min"], until=dminmax['max']))
    
    featuregeom = None
    def matchByLength(attr, props):
        return len(props[attr])
    
      

    def filterByCount(attr, props):
        return props[attr] > 0
    
    def dtMean(attr, props):
        try:
            dtlist = props[attr]
            if len(dtlist) == 1:
                return dtlist[0]
            
            c = (dtlist[-1] - dtlist[0])/2
            return dtlist[0] + c
        except:
            return None 
        
    def apply(func, attr, props):
        return func(props[attr])
    
    applyFunc = partial(matchByLength,"times")  
    
    applyFilterFunc = partial(filterByCount,"count") 
    
    applyMinFunc = partial(apply,min,"times") 
    
    applyMaxFunc = partial(apply,max,"times")  
    
    applyMeanFunc = partial(dtMean,"times")    
    

    # if we have a "matchid" we also produce a dataframe with the results per week, 
    # this may create a substantial time and memory increase    
    matchid = options.get("matchid",None)
    dfall =  None


    with stopwatch("apply weekly scan"):
        nweeks = len(mrange)
        print(f"processing weeks {nweeks}")
        for i,md in  enumerate(mrange):
            dff = df[(df[column_defs[-1]].dt.year == md.year) & (df[column_defs[-1]].dt.isocalendar().week == md.isocalendar().week)]
    
            # returns a Geometries object with attributes amended according to count
            # the geometry is read anew for every round/week
            geoms, dfout = match2Geoms(dff,geompath,column_defs,["count","times"],add_properties = {"count":0,"times":[]}, geom_id = matchid)
            

            
            #geoms.applyAttributes("count2",applyFunc)
            fgeomlist = geoms.filterByAttributeFunc(applyFilterFunc)
            Geometries._applyAttributes(fgeomlist,"tmin",applyMinFunc)
            Geometries._applyAttributes(fgeomlist,"tmax",applyMaxFunc)
            Geometries._applyAttributes(fgeomlist,"tmean",applyMeanFunc)
            
            if not matchid is None and not dfout is None and len(fgeomlist) > 0:
                # build up a dataframe of the non-empty results, 
                # this could be done with concat, but the existence of the label as index confuses pandas
                if dfall is None:
                    dfout.rename(columns= {"count":f"{md.year}-{md.isocalendar().week}"}, inplace=True)
                    dfall = dfout
                else:
                    dfall[f"{md.year}-{md.isocalendar().week}"] = dfout["count"].to_numpy()
                    
             
            
            if featuregeom is None:
                featuregeom = geoms.clone(True)
            
            for g in fgeomlist:
                featuregeom.append(g)
                    
            print(f"processed {i} of {nweeks} added {len(fgeomlist)}")

    
    if pdump:
        pdump = Path(pdump)
        prefix = pdump.parent.name
        pdump.mkdir(parents=True, exist_ok =True)
        idx = prefix.find("_gr")
        if idx > 2:
            prefix = prefix[:idx]
            
        from pygeom.geom import SUFFIX4DRIVER,PROFILES, epsg4326Proj
        if featuregeom.getMetaDriver() is None:
            profile = deepcopyPROFILES["fgb"]
            profile["schema"]["geometry"] = "Polygon"
            profile["crs"] = epsg4326Proj
            featuregeom.setMeta(PROFILES["fgb"])
            
        else:
            if not featuregeom.getMetaDriver() in SUFFIX4DRIVER:
                profile = {**featuregeom._meta}
                profile["driver"] = PROFILES["fgb"]["driver"]
                
            
        outfile = pdump.joinpath(f"{prefix}-weeks.{SUFFIX4DRIVER[featuregeom.getMetaDriver()]}")
            
        # add teh additional attributes, otherwise some output formats such as FGB will fail
        featuregeom.save(outfile,{"count":"int","times":"int","tmin":"datetime","tmax":"datetime","tmean":"datetime"})
        
        if not matchid is None:
            dfall.info()
            print(dfall['2024-3'].sum())
            
            # all sum all
            md1 = mrange[0]
            md2 =  mrange[-1]
            label = f"{md1.year}-{md1.isocalendar().week}-{md2.year}-{md2.isocalendar().week}"
            dfall[label] = dfall.sum(axis=1)
            print(dfall[label].sum())
            print(dfall.columns)
            weekq = 0
            
            while weekq < len(mrange):
                weekq2 = weekq+12
                #w1 = mrange[weekq].isocalendar().week
                md1 = mrange[weekq]
                
                
                if weekq2 > len(mrange):
                    md2 =  mrange[-1]
                    msubrange = [f"{md.year}-{md.isocalendar().week}" for md in mrange[weekq:-1]]
                else:
                    md2 =  mrange[weekq2-1]
                    msubrange = [f"{md.year}-{md.isocalendar().week}" for md in mrange[weekq:weekq2]]
                    
                label = f"{md1.year}-{md1.isocalendar().week}-{md2.year}-{md2.isocalendar().week}"
                
                deletemdl = []
                for mdl in msubrange:
                    if not mdl in dfall.columns:
                        deletemdl.append(mdl)
                
                for mdl in deletemdl:
                    msubrange.remove(mdl)
                    
                if len(msubrange) < 2 :
                    weekq +=12
                    continue
                dfall[label] = dfall[msubrange].sum(axis=1)
                print(dfall[label].sum())
                weekq +=12
                

            dfall.info()
            
            outfiledf = pdump.joinpath(f"{prefix}-weeks.csv")
            dfall.to_csv(outfiledf)

            
    
    #plotKDE(df,coords,minmax)
    print("done for weeks")
    
    
    
def match2Geoms(df,geomspath,column_defs,keys,add_properties = {}, geom_id = None):
    '''
    we build a new set of geometries as we modify the properties
    '''
    from shapely import Point
    geoms = Geometries()
    Geometries.collectGeoms(geoms, geomspath,add_properties)
    geoms.initIndex(reassignIDFunc = _assignRandomId)
    
    geometries = [[Point(x,y),dt] for x, y, dt in zip(df[column_defs[0]], df[column_defs[1]],df[column_defs[-1]])]
    dfout = None
    if not geom_id is None:
       dfout = pd.DataFrame(columns=[geom_id,keys[0]])
       dfout[geom_id] = [str(g.properties[geom_id]) for g in  geoms.geoms()]
       dfout[keys[0]] = len(geoms.geoms()) * [0]
       dfout.set_index(geom_id, inplace=True)
       idx = dfout.columns.get_loc(keys[0])
       #print(f"{dfout.columns} - {len(dfout.index)}")
       
       
    for p, dt in geometries:
        intersectionsLP = geoms.intersections(p)
        for geom in intersectionsLP:
            if geom.inside(p):
                geom.properties[keys[0]] = 1 + geom.properties[keys[0]]
                geom.properties[keys[1]].append(dt)
                if not dfout is None:
                    #dfout.loc[dfout[geom_id] == geom.properties[geom_id],keys[0]] += 1
                    
                    dfout.loc[str(geom.properties[geom_id]),keys[0]] += 1
                break
            
    return geoms,dfout
    
    
def scanDFInside(geompath, df, column_defs, filter = None , outpath = None, options={}):
        
    
    df[column_defs[-1]] = pd.to_datetime(df[column_defs[-1]])
    if filter:
        with stopwatch("apply filter"):
            dff = df.query(filter)
            dff.info()
            nfilter_points = len(dff.index)
            print(f" filtered npoints {nfilter_points}")
    else:
        dff = df
        
    
    
    #minmaxlon = dff[column_defs[0]].agg(['min', 'max'])
    #minmaxlat = dff[column_defs[1]].agg(['min', 'max'])
    with stopwatch(f"Processing weeks .."):
        extractWeeks(geompath, dff, column_defs,pdump = outpath, options=options)
    
    
    
    
    
    
    

def combineDFscan(geompath, dfpaths,column_defs, wc = None, filter = None , outpath = None, options = {}):
    
    with stopwatch(f"Process subset on filter {filter}"):
            
        if not wc is None:
            with stopwatch("merging files"):
                df,nf  = mergeCSV(dfpaths,wc)
                df.info()
        else:
            # assuming a file
            df = pd.read_csv(froot)
        

    return scanDFInside(geompath,df, column_defs, filter,outpath = outpath , options=options )
        
        
        
def testConcat(indir, wc,outdir):
    dfall,_ = mergeCSV(indir,wc,setindex = "grid_id" )
    

    dfall.info()
    
    #dflist = [df.set_index('grid_id',inplace=True) for df in dflist]
    
    #dfall = pd.concat(dflist, ignore_index=True)
    #dfall2 = pd.concat(dflist, ignore_index=True,axis=1)
    #dfall.set_index('grid_id',inplace=True)
    
    
    print(dfall['2024-1'].sum())
    #print(dfall2[2].sum(axis=1))
    
    
        
if __name__ == "__main__":
    
    
    
    if len(sys.argv) > 1:
        with open(sys.argv[1],'r') as fp:
            config = json.load(fp)
            
        config = config["params"]
        combineDFscan(**config)
    
    
    else:
            
        '''
        testConcat("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/scan/ais/fc009e67-9cd",
                   "ais-week-*.csv",
                   "/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2024/fc009e67")
        '''
        
        combineDFscan(geompath="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_centre_frdc.sub.rect.exact2.fgb",
                  dfpaths="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2024/fc009e67-8fb",
                  column_defs=["lon","lat","timestamp"],
                  wc="**/calc_data.csv",
                  filter= "`score-activity` == 'fishing' or `hmm-activity` == 'fishing' ",
                  outpath="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/scan/ais/fc009e67-000",
                  options={"matchid":"grid_id"})
        
        '''
        combineDFscan("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_centre_frdc.sub.rect.fgb",
                  "/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2025",
                  ["Lon","Lat","MsgTime"],
                  wc="**/calc_data.csv",
                  filter= None,#" `score-activity` == 'fishing' or `hmm-activity` == 'fishing' ",
                  outpath="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/scan/ais")
        
        '''
    """
    #"/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2025-combined/density/scan",   
    
    combineDFscan("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2025-combined/density/grid2.json",
                  "/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2025-combined/qgis",
                  ["Lon","Lat","MsgTime"],
                  wc="503*/calc_data.csv",
                  filter= " `score-activity` == 'fishing' or `hmm-activity` == 'fishing' ",
                  outpath="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2025-combined/density/scan/ais")
                  

#[[152.,-32.5],[154.5,-27.0]],
"""