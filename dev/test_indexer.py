import geopandas as gp
from pathlib import Path

def test_shp(finp, fout):
    
    finpp = Path(finp)
    foutp = Path(fout)
    shpin = gp.read_file(finp)
    shpin.to_csv(foutp.joinpath(f"{finpp.stem}.csv"), index= False,)
    
    
 
 
def test_dbase(finp):     
    import pybase3
    
     
def test_spatialIndex(dirtiff, timeregex,datastorename, fileregex = "*.tif*",columns=["location","ingestion","geometry"]):
    import rasterio
    from datetime import datetime
    from shapely import Polygon
    finpp = Path(dirtiff)
    import re
    tdtmatch = re.compile(timeregex[0])
        
    def getdt(strn):
        dt = tdtmatch.findall(strn)
        if dt:
            return datetime.strptime(dt[0], timeregex[1]).date().strftime("%d/%m/%Y")
        raise ValueError(f"cannot find re {timeregex} in {strn}")
        
    flist = list(finpp.glob(fileregex))
    
    with rasterio.open(flist[0]) as fpraster:
        print(fpraster.profile)
        bounds = fpraster.bounds
        bounds  = Polygon([[bounds.left,bounds.top],
                           [bounds.right,bounds.top],
                           [bounds.right,bounds.bottom],
                           [bounds.left,bounds.bottom],
                           [bounds.left,bounds.top]])
        crs = fpraster.crs
    flist = sorted(flist)    
    
    
    ndata= []
    for i,f in enumerate(flist):
        ndata.append({columns[0]:f.name,columns[1]:getdt(f.name),columns[2]:bounds})
        
    
    df = gp.GeoDataFrame(ndata ,crs=crs)
    
    df.to_file(str(finpp.joinpath(f"{datastorename}.shp")))
    
     
    
#test_spatialIndex("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff",[r"[0-9]{8}","%Y%m%d"],"po_jra55npv2.0.W-anom",fileregex="*W.tif")
#test_shp("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff/po_jra55npv2.0.W-anom.shp", "/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff/")

#test_spatialIndex("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff",[r"[0-9]{8}","%Y%m%d"],"po_jra55npv2.0.E-anom",fileregex="*E.tif")
#test_shp("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff/po_jra55npv2.0.E-anom.shp", "/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff/")


test_shp("/Users/ros260/projects/data/ML/enso-ops/output/tmp/po_data_jra55npv20W_anom_2/po_data_jra55npv20W_anom_2.shp","/Users/ros260/projects/data/ML/enso-ops/output/tmp/po_data_jra55npv20W_anom_2")

#test_dbase("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff/po_jra55npv2.0-anom.W.dbf")