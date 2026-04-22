
from datetime import timedelta, datetime
from calendar import isleap
import numpy as np
def is_leap(year):
    return 1 if isleap(year) else 0

import os
def convert_partial_year(number):

    year = int(number)
    fractn = float(number)
    d = timedelta(days=(fractn - year)*(365 + is_leap(year)))
    day_one = datetime(year,1,1)
    date = d + day_one
    return date



def _is_monotonic(coord, axis=0):
    """
    >>> _is_monotonic(np.array([0, 1, 2]))
    np.True_
    >>> _is_monotonic(np.array([2, 1, 0]))
    np.True_
    >>> _is_monotonic(np.array([0, 2, 1]))
    np.False_
    """
    if coord.shape[axis] < 3:
        return True
    else:
        n = coord.shape[axis]
        delta_pos = coord.take(np.arange(1, n), axis=axis) >= coord.take(
            np.arange(0, n - 1), axis=axis
        )
        delta_neg = coord.take(np.arange(1, n), axis=axis) <= coord.take(
            np.arange(0, n - 1), axis=axis
        )
        return np.all(delta_pos) or np.all(delta_neg)


def _infer_interval_breaks(coord, axis=0, scale=None, check_monotonic=False):
    """
    >>> _infer_interval_breaks(np.arange(5))
    array([-0.5,  0.5,  1.5,  2.5,  3.5,  4.5])
    >>> _infer_interval_breaks([[0, 1], [3, 4]], axis=1)
    array([[-0.5,  0.5,  1.5],
           [ 2.5,  3.5,  4.5]])
    >>> _infer_interval_breaks(np.logspace(-2, 2, 5), scale="log")
    array([3.16227766e-03, 3.16227766e-02, 3.16227766e-01, 3.16227766e+00,
           3.16227766e+01, 3.16227766e+02])
    """
    coord = np.asarray(coord)

    if check_monotonic and not _is_monotonic(coord, axis=axis):
        raise ValueError(
            "The input coordinate is not sorted in increasing "
            f"order along axis {axis}. This can lead to unexpected "
            "results. Consider calling the `sortby` method on "
            "the input DataArray. To plot data with categorical "
            "axes, consider using the `heatmap` function from "
            "the `seaborn` statistical plotting library."
        )

    # If logscale, compute the intervals in the logarithmic space
    if scale == "log":
        if (coord <= 0).any():
            raise ValueError(
                "Found negative or zero value in coordinates. "
                "Coordinates must be positive on logscale plots."
            )
        coord = np.log10(coord)

    deltas = 0.5 * np.diff(coord, axis=axis)
    if deltas.size == 0:
        deltas = np.array(0.0)
    first = np.take(coord, [0], axis=axis) - np.take(deltas, [0], axis=axis)
    last = np.take(coord, [-1], axis=axis) + np.take(deltas, [-1], axis=axis)
    trim_last = tuple(
        slice(None, -1) if n == axis else slice(None) for n in range(coord.ndim)
    )
    interval_breaks = np.concatenate(
        [first, coord[trim_last] + deltas, last], axis=axis
    )
    if scale == "log":
        # Recovert the intervals into the linear space
        return np.power(10, interval_breaks)
    return interval_breaks


def calculateAnomaly(fpath, coordvars, valuevars , outdir,  isFractionalDate = False):
    import xarray
    import rioxarray as rio
    
    from pathlib import Path
    import os
    
    engine = "h5netcdf"
    #engine = "rasterio"

    xds = xarray.open_dataset(fpath,engine=engine)
    
    infilep = Path(fpath)
    outpath = Path(outdir)
    outpath.mkdir(parents=True, exist_ok=True)
    
    
    #xds.rio.write_crs("epsg:4326", inplace=True)    
    #rcoordvars = list(reversed(coordvars))
    
    xdsdtarray = [np.datetime64(convert_partial_year(dt)) for dt in xds[coordvars[-1]]]
    xdsdt= xds.assign_coords(time=xdsdtarray)
    
    xdsdt.to_netcdf(outpath.joinpath(infilep.name),format="NETCDF4",engine = "h5netcdf")
    
    
    
    for var in valuevars:
        varxds = xdsdt[var]
        dims = list(varxds.dims)
        varmean = varxds.groupby('time.month').mean()
        xdsdt[f"{varxds.name}-mean"] = varmean
        
        vardanom = np.empty(varxds.shape)
        
        for i,dt in enumerate(varxds[coordvars[-1]]):
            vard = varxds[i,:,:]
            m = dt.dt.month
            vardanom[i,:,:] = vard - varmean[m-1,:,:]
         
        print(f"{varxds.dims}, {varxds.shape}")   
        print(f"{vardanom.shape}")
        
        xdsdt[f"{varxds.name}-anom"] = xarray.DataArray(vardanom, coords=varxds.coords,name=f"{varxds.name}-anom")
    
    xdsdt.to_netcdf(outpath.joinpath(f"{infilep.stem}-means{infilep.suffix}"),format="NETCDF4",engine = "h5netcdf")
    
    
def rasterBounds(fpath,valuevars):
    import xarray
    import rioxarray as rio
    
    from pathlib import Path
    import os
    gnbset = os.environ.get("GDAL_NETCDF_BOTTOMUP","YES")
    #os.environ["GDAL_NETCDF_BOTTOMUP"] = "NO"
    
    
    #engine = "h5netcdf"
    engine = "rasterio"

    xds = xarray.open_dataset(fpath,engine=engine)
    
    for var in valuevars:
        xdsv = xds[var]
        m1 = xdsv.min()
        m2 = xdsv.max()
        print(f"{Path(fpath).stem} {var}: min {m1.values} max {m2.values}")
        print(xdsv.dims)
        print(xdsv.coords)
        
        
    


def rasterFromNetcdf(fpath, coordvars, valuevars , 
                     outdir,  isFractionalDate = False, reorder = True, 
                     minyear = -1, dimesion_order = -1, dimension_attr = None):
    
    import xarray
    import rioxarray as rio
    
    from pathlib import Path
    import os
    gnbset = os.environ.get("GDAL_NETCDF_BOTTOMUP","YES")
    #os.environ["GDAL_NETCDF_BOTTOMUP"] = "NO"
    
    
    #engine = "h5netcdf"
    engine = "rasterio"

    xds = xarray.open_dataset(fpath,engine=engine)
    
        
    Path(outdir).mkdir(parents=True, exist_ok=True)
    #os.environ["GDAL_NETCDF_BOTTOMUP"] = gnbset
    
    '''
    xds.rio.set_crs("epsg:4326", inplace=True)
    xds["Pressure"].rio.to_raster(test.tiff,tiled=True, windowed=True) # as cog
    '''
    
    xds.rio.write_crs("epsg:4326", inplace=True)    
    rcoordvars = list(reversed(coordvars))
    
    
    if engine == "rasterio":
        print(f"{xds.rio.bounds()}")
        x='x'
        y='y'
        
        

        
    else:
        x = coordvars[0]
        y = coordvars[1]
        
        
    if xds[x].max() > 180:
        xdima = xds[x]
        xdima  = (xdima + 180.) % 360. - 180.
        xds[x] = xdima
        print(f" min/max x {xds[x].min()} {xds[x].max()}")
        #xds0 = xds.where(x >= 0)
        #xds1 = xds.where(x < 0)
        #xds11 = xds1.T
        if reorder:
            xds = xds.sortby(x)
        #print(f" min/max x {xds[x].min()} {xds[x].max()}")
            
        #print(f" max x {xds[x].max()}")
        
        
 
    ilon = -1
    # test for continuity and breaks
    if not reorder:
        if not _is_monotonic(np.asarray(xdima)):
            ilon = list(xdima.values).index(xdima.sel(x=xdima.max(), method='nearest'))
            # xarray indexing starts at 1
            ilon +=1
            if ilon >= xdima.size:
                ilon = -1
                 
                 
    def processTile(vard, dt, subsec=""):
        if isFractionalDate:
            dtt = convert_partial_year(dt)
            if not dtt.year > minyear:
                return 
            dttgdal = dtt.isoformat()
            #dtt = dtt.strftime('%Y%m%dT%H%M')
            dtt = dtt.strftime('%Y%m%d')
            
        else:
            dtt = dt.dt
            if not dtt.year > minyear:
                return 
            dttgdal = str(dtt.strftime('%Y-%m-%dT%H:%M:%S').values)
            #dtt = str(dtt.strftime('%Y%m%dT%H%M').values)
            dtt = str(dtt.strftime('%Y%m%d').values)
            
        #vard.attrs["TIFFTAG_DATETIME"]= dtt
        varxdsp = vard.assign_attrs(**{"TIFFTAG_DATETIME": dttgdal,**varxds.attrs})
        if  ilon > 0:
            #vard[:,:ilon].plot()
            varxdsp[:,:ilon].rio.to_raster(f"{outdir}/{var}{subsec}-{dtt}W.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
            varde = varxdsp[:,ilon:]
            #varde.assign_coords(x=((varde[x] + 180) % 360) - 180)
            #varde.plot()
            varde.rio.to_raster(f"{outdir}/{var}{subsec}-{dtt}E.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
            #vard.plot()
            #vard.rio.to_raster(f"{outdir}/{var}-{dtt}.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
        
        
            
        else:
            #vard.rio.slice_xy(minx=120, miny=-20, maxx=-120, maxy=20).plot()
            #vard.plot()
            varxdsp.rio.to_raster(f"{outdir}/{var}{subsec}-{dtt}.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
            
    for var in valuevars:
        varxds = xds[var]
        dims = list(varxds.dims)
        
        
        
        if engine == "rasterio":
            varxds.rio.set_spatial_dims(x_dim=x, y_dim=y, inplace=True)
            
            
            
        elif engine == "h5netcdf":
            varxds.rio.set_spatial_dims(x_dim=coordvars[0], y_dim=coordvars[1], inplace=True)
            if not all([vd == cd for vd,cd in zip(dims,rcoordvars)]):
                varxds = varxds.transpose(*rcoordvars)
                
            #varxds = varxds.rename({coordvars[0],'x',coordvars[1],'y'})
            
        
        #print(varxds.attrs)
        if len(dims) == 3:
            for i,dt in enumerate(varxds[coordvars[-1]]):
                vard = varxds[i,:,:]
                #if swap_xy:
                #    vard.transpose()
                #vard.rio.set_spatial_dims(x_dim=coordvars[0], y_dim=coordvars[1], inplace=True)
                
                #print(vard.coords)
                #print(dt.dt.strftime('%Y%m%dT%H%M').values)
                #print(type(dt.dt.strftime('%Y%m%dT%H%M').values))
                
                #vard.transpose(coordvars[1],coordvars[0])
                '''
                if isFractionalDate:
                    dtt = convert_partial_year(dt)
                    if not dtt.year > minyear:
                        continue 
                    dttgdal = dtt.isoformat()
                    #dtt = dtt.strftime('%Y%m%dT%H%M')
                    dtt = dtt.strftime('%Y%m%d')
                    
                else:
                    dtt = dt.dt
                    if not dtt.year > minyear:
                        continue 
                    dttgdal = str(dtt.strftime('%Y-%m-%dT%H:%M:%S').values)
                    #dtt = str(dtt.strftime('%Y%m%dT%H%M').values)
                    dtt = str(dtt.strftime('%Y%m%d').values)
                    
                #vard.attrs["TIFFTAG_DATETIME"]= dtt
                varxdsp = vard.assign_attrs(**{"TIFFTAG_DATETIME": dttgdal,**varxds.attrs})
                if  ilon > 0:
                    #vard[:,:ilon].plot()
                    varxdsp[:,:ilon].rio.to_raster(f"{outdir}/{var}-{dtt}W.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
                    varde = varxdsp[:,ilon:]
                    #varde.assign_coords(x=((varde[x] + 180) % 360) - 180)
                    #varde.plot()
                    varde.rio.to_raster(f"{outdir}/{var}-{dtt}E.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
                    #vard.plot()
                    #vard.rio.to_raster(f"{outdir}/{var}-{dtt}.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
                
                
                    
                else:
                    #vard.rio.slice_xy(minx=120, miny=-20, maxx=-120, maxy=20).plot()
                    #vard.plot()
                    varxdsp.rio.to_raster(f"{outdir}/{var}-{dtt}.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
                '''
                processTile(vard,dt,"")
                
                
        elif len(dims) > 3:
            if dimesion_order is None:
                dimesion_order = [str(d) for d in varxds.dims]
                
            # figure out over which dim the time dim is iterating
            tidx = dimesion_order.index(coordvars[-2])
            edim = coordvars[-2]
            edimn = varxds.shape[tidx]
            
            if tidx == 0:
                for idx in range(edimn):
                    for i,dt in enumerate(varxds[coordvars[-1]]):
                        vard = varxds[idx,i,:,:]
                        processTile(vard,dt,f"{coordvars[-2]}-{idx}")
                    
                
            elif tidx == 1:
                for idx in range(edimn):
                    for i,dt in enumerate(varxds[coordvars[-1]]):
                        vard = varxds[i,idx,:,:]
                        processTile(vard,dt,f"{coordvars[-2]}-{idx}")
                '''
                if isFractionalDate:
                    dtt = convert_partial_year(dt)
                    if not dtt.year > minyear:
                        continue 
                    dttgdal = dtt.isoformat()
                    #dtt = dtt.strftime('%Y%m%dT%H%M')
                    dtt = dtt.strftime('%Y%m%d')
                    
                else:
                    dtt = dt.dt
                    if not dtt.year > minyear:
                        continue 
                    dttgdal = str(dtt.strftime('%Y-%m-%dT%H:%M:%S').values)
                    #dtt = str(dtt.strftime('%Y%m%dT%H%M').values)
                    dtt = str(dtt.strftime('%Y%m%d').values)
                    
                #vard.attrs["TIFFTAG_DATETIME"]= dtt
                varxdsp = vard.assign_attrs(**{"TIFFTAG_DATETIME": dttgdal,**varxds.attrs})
                if  ilon > 0:
                    #vard[:,:ilon].plot()
                    varxdsp[:,:ilon].rio.to_raster(f"{outdir}/{var}-{dtt}W.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
                    varde = varxdsp[:,ilon:]
                    #varde.assign_coords(x=((varde[x] + 180) % 360) - 180)
                    #varde.plot()
                    varde.rio.to_raster(f"{outdir}/{var}-{dtt}E.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
                    #vard.plot()
                    #vard.rio.to_raster(f"{outdir}/{var}-{dtt}.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
                
                
                    
                else:
                    #vard.rio.slice_xy(minx=120, miny=-20, maxx=-120, maxy=20).plot()
                    #vard.plot()
                    varxdsp.rio.to_raster(f"{outdir}/{var}-{dtt}.tif",tiled=True, windowed=True,overview_level=4,driver="COG")
                '''
                
                
            
            
            
def buildImageMosaic(indir,timeformat, params= {}, storename = None, wildcard = "*.tif"):
    
    from pathlib import Path
    from pyrest.geoserver import GSAuthClient, createCOGImageStoreTemporal,createAndPublishCOG,createAndPublishCoverage
    
    #gsclient = GSAuthClient({"url":"http://localhost:18080/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    gsclient = GSAuthClient({"url":"http://oa-gis.csiro.au/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    #print(gsclient.getWorkspaces())
    
    indirp = Path(indir)
    
       
    
    if storename is None:
        storename = indirp.name
        
    storename = storename.replace(' ','').replace(".","").replace("-","_")
    
        
    res = gsclient.testCoverageStore("enso",storename)
    if not res is None:
        return
    
    flist = list(indirp.glob(wildcard))
    if len(flist) ==0 :
        return
        
    createCOGImageStoreTemporal(gsclient,indirp.joinpath("tmp"),"enso",storename,flist,timeformat, params=params)        


def testPrepared(zipname, workspaceName):
    from pathlib import Path
    from pyrest.geoserver import GSAuthClient, createCOGImageStoreTemporal,createAndPublishCOG,createAndPublishCoverage,createCOGImageStorePrepared
    
    gsclient = GSAuthClient({"url":"http://localhost:18080/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    
    print(gsclient.getWorkspaces())
    
    inp = Path(zipname)
    
    createCOGImageStorePrepared(gsclient,workspaceName,inp.stem, inp.read_bytes(),{"time":"time"})
    
    res = gsclient.getCoverage(workspaceName,inp.stem,qfilter= {'list':'all'})
    print(res)
    
def test_connection():
    from pathlib import Path
    from pyrest.geoserver import GSAPIClient, createCOGImageStoreTemporal,createAndPublishCOG,createAndPublishCoverage,createCOGImageStorePrepared
    
    gsclient = GSAPIClient({"url":"http://oa-gis.csiro.au/geoserver/rest","username":"admin","password":"a2acccess"})
    print(gsclient.getWorkspaces())#**{"_dry_run":True}))
    
def test_connectionOA():
    from pathlib import Path
    from pyrest.geoserver import GSAPIClient,GSAuthClient, createCOGImageStoreTemporal,createAndPublishCOG,createAndPublishCoverage,createCOGImageStorePrepared
    #os.environ["DEBUG_CURL"]="T"
    from curlify import to_curl
    
    gsclient = GSAuthClient({"url":"http://oa-gis.csiro.au/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    #gsclient = GSAPIClient({"url":"http://oa-gis2.it.csiro.au:8600/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    #gsclient = GSAPIClient({"url":"http://localhost:18080/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    #gsclient = GSAuthClient({"url":"http://localhost:18080/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    gsclient._printConnection()
    
    
    print(gsclient.getWorkspaces())
    #requ = gsclient.last_response
    #print(requ)
    #print(to_curl(requ.urllib3_response.request))
    
    
#http://localhost:18080/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities
#http://hamster-hf.nexus.csiro.au:18080/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities

#https://oa-gis.csiro.au/geoserver/ows?REQUEST=GetCapabilities&VERSION=1.3.0&SERVICE=WMS

#rasterFromNetcdf("/Users/ros260/projects/data/ML/enso-ops/output/sst-anom-smoothed_1991-2020_remapped.nc",["lon","lat","time"],["sst_anom"],"/Users/ros260/projects/data/ML/enso-ops/output/tiff.r.4")


#rasterFromNetcdf("/Users/ros260/projects/git/enso-data/data/po_jra55npv2_2x30d_T_L1_1958_2022-2.nc",["lon","lat","time"],["data"],"/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0",True,False, 1978)


#calculateAnomaly("/Users/ros260/projects/git/enso-data/data/po_jra55npv2_2x30d_T_L1_1958_2022-2.nc",["lon","lat","time"],["data"],"/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom",True)


#rasterFromNetcdf("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/po_jra55npv2_2x30d_T_L1_1958_2022-2-means.nc",["lon","lat","time"],["data-anom"],"/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff-test",False,False, 1978)


#test_connectionOA()


#nemo_combined_vasrs= ["fbiom_bathy","fbiom_epi","fbiom_hmbathy","fbiom_mbathy","fbiom_meso","fbiom_mmeso","o2_l1","o2_l2","o2_l3","pp","sst","temp_l1","temp_l2","temp_l3"]
nemo_combined_vasrs= ["fbiom_hmbathy","fbiom_mbathy","fbiom_meso","fbiom_mmeso","o2_l1","o2_l2","o2_l3","pp","sst","temp_l1","temp_l2","temp_l3"]

nemo_combined_vect = [["u_l1","v_l1"],["u_l2","v_l2"],["u_l3","v_l3"]]
#rasterFromNetcdf("/Users/ros260/projects/data/ML/enso-ops/output/nemo_combined_1958_2022_detrended_1979-2022.nc",["lon","lat","time"],nemo_combined_vasrs,"/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined",False,False, 2001)

toskipE = [["fbiom_mbathy",8],["fbiom_meso",20],["temp_l2",20],["o2_l2",21]]
toskipW = [["fbiom_epi",20],["o2_l1",13]]
for v in nemo_combined_vasrs:
    for i in range(24):
        #print(f"{v} {i} {[v == skip[0] and i == skip[1]-1 for skip in toskipE ]}")
        if not any([v == skip[0] and i == skip[1]-1 for skip in toskipE ]):
            try:
                buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v}_lead_time{i+1}E",wildcard=f"{v}lead_time-{i+1}-*E.tif",params={"LevelsNum":4})
            except :
                pass
        if not any([v == skip[0] and i ==skip[1]-1 for skip in toskipW ]):
            try:
                buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v}_lead_time{i+1}W",wildcard=f"{v}lead_time-{i+1}-*W.tif",params={"LevelsNum":4})
            except:
                pass
            
            
            
for v in nemo_combined_vect:
    for i in range(24):
        try:
            buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted-uv",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v[0]}_lead_time{i+1}E",wildcard=f"speed_{v[0]}_direction_{v[1]}_lead_time-{i+1}-*E.tif",params={"LevelsNum":4})
        except:
            pass
        
        try:
            buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted-uv",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v[0]}_lead_time{i+1}W",wildcard=f"speed_{v[0]}_direction_{v[1]}_lead_time-{i+1}-*W.tif",params={"LevelsNum":4})
        except:
            pass
'''
v='fbiom_epi'
i=23
buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v}_lead_time{i+1}W",wildcard=f"{v}lead_time-{i+1}-*W.tif",params={"LevelsNum":4})
buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v}_lead_time{i+1}E",wildcard=f"{v}lead_time-{i+1}-*E.tif",params={"LevelsNum":4})
'''

#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff3",timeformat='[0-9]{8},format=yyyyMMdd',params={"LevelsNum":4})
#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff3",timeformat='[0-9]{8},format=yyyyMMdd',params={"LevelsNum":4})


#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/tiff.r.2",timeformat='[0-9]{8},format=yyyyMMdd',params={"LevelsNum":4})#"'%Y%m%dT%H%M')


#rasterBounds("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/po_jra55npv2_2x30d_T_L1_1958_2022-2-means.nc",["data-anom"])
#rasterBounds("/Users/ros260/projects/data/ML/enso-ops/output/sst-anom-smoothed_1991-2020_remapped.nc",["sst_anom"])


#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/tiff0",timeformat='[0-9]{8}T[0-9]{4},format=yyyyMMddTHHMM',params={"LevelsNum":4})#"'%Y%m%dT%H%M')

#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/tiff0",timeformat='[0-9]{8}',params={"LevelsNum":4})#"'%Y%m%dT%H%M')
#test_connection()

#testPrepared("/Users/ros260/projects/data/backGroundData/OZ/processing/wpop/package/pop3.zip", "test")


#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff3",timeformat='[0-9]{8},format=yyyyMMdd',params={"LevelsNum":4},storename= 'po_jra55npv2.0.E-anom',wildcard="*E.tif")#"'%Y%m%dT%H%M')



'''
    
    
PropertyCollectors=DateExtractorSPI(ingestion)
Schema=*the_geom:Polygon,location:String,ingestion:java.util.Date
TimeAttribute=ingestion
    
'''