
from datetime import timedelta, datetime
from calendar import isleap
import numpy as np
def is_leap(year):
    return 1 if isleap(year) else 0

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
        

    
def netcdfEnforceUNICODE(fpath):
    import xarray
    import rioxarray as rio
    
    from pathlib import Path
    import os
    from h5netcdf.legacyapi import Dataset 
    
    
    ds = Dataset(fpath,mode='a')
    
    for v in ds.variables:
        print(ds[v].attrs)
        ds[v].attrs['units'] = ds[v].attrs['units'].encode('utf-8', 'surrogateescape').decode('iso-8859-15')
        print(ds[v].attrs)
    '''
    
    engine = "h5netcdf"
    #engine = "rasterio"

    xds = xarray.open_dataset(fpath,engine=engine)
    
    for var in valuevars:
        xdsv = xds[var]
        m1 = xdsv.min()
        m2 = xdsv.max()
        print(f"{Path(fpath).stem} {var}: min {m1.values} max {m2.values}")
        print(xdsv.dims)
        print(xdsv.coords)
    '''
        
    


def rasterFromNetcdf(fpath, coordvars, valuevars , outdir,  isFractionalDate = False, reorder = True, minyear = -1, dimesion_order = None, dimension_process = None, additionalSub= ""):
    
    #from h5netcdf.legacyapi import Dataset as DS 
    #ds = DS(fpath,mode='r')
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
            if engine == "rasterio":
                ilon = list(xdima.values).index(xdima.sel(x=xdima.max(), method='nearest'))
            else:
                ilon = list(xdima.values).index(xdima.sel(lon=xdima.max(), method='nearest'))
            # xarray indexing starts at 1
            ilon +=1
            if ilon >= xdima.size:
                ilon = -1
                 
                 
    def processTile(vard, dtt, varnames, subsec="", tiffprops = {"tiled":True, "windowed":True,"overview_level":4,"driver":"COG"}):
        
            
        #vard.attrs["TIFFTAG_DATETIME"]= dtt
        varxdsp = vard.assign_attrs(**{"TIFFTAG_DATETIME": dttgdal,**vard.attrs})
        if  ilon > 0:
            if ismultiple: # from parent function
                vards = [varxdsp[v][:,:ilon] for v in varnames]
                vardw = xarray.merge(vards)
                vardw.rio.to_raster(f"{outdir}/{'_'.join(varnames)}{subsec}-{dtt}W.tif",**tiffprops)
                vards = [varxdsp[v][:,ilon:] for v in varnames]
                varde = xarray.merge(vards)
                varde.rio.to_raster(f"{outdir}/{'_'.join(varnames)}{subsec}-{dtt}E.tif",**tiffprops)
            else:
                varxdsp[:,:ilon].rio.to_raster(f"{outdir}/{var}{subsec}-{dtt}W.tif",**tiffprops)
                varde = varxdsp[:,ilon:]
                varde.rio.to_raster(f"{outdir}/{var}{subsec}-{dtt}E.tif",**tiffprops)
        else:
            #vard.rio.slice_xy(minx=120, miny=-20, maxx=-120, maxy=20).plot()
            #vard.plot()
            if ismultiple: # from parent function
                varxdsp.rio.to_raster(f"{outdir}/{'_'.join(varnames)}{subsec}-{dtt}.tif",**tiffprops)
            else:
                varxdsp.rio.to_raster(f"{outdir}/{var}{subsec}-{dtt}.tif",**tiffprops)
            
    if engine == "rasterio":
        xds.rio.set_spatial_dims(x_dim=x, y_dim=y, inplace=True)
        
            
            
    elif engine == "h5netcdf":
        #xds.rio.set_spatial_dims(x_dim=coordvars[0], y_dim=coordvars[1], inplace=True)
        
        xds.rename({coordvars[0]:"x",coordvars[1]:"y"})
        xds = xds.rename_dims({coordvars[0]:"x",coordvars[1]:"y"}) 
        #xds.rename({coordvars[0]:"x",coordvars[1]:"y"})
        xds.rio.set_spatial_dims(x_dim='x', y_dim='y', inplace=True)

        #if not all([vd == cd for vd,cd in zip(dims,rcoordvars)]):
        #    varxds = xds.transpose(*rcoordvars)
                
    for var in valuevars:
        #varxds = xds[var]
        dims = list(xds[var].dims)
        
        '''
        if engine == "h5netcdf":
            if not all([vd == cd for vd,cd in zip(dims,rcoordvars)]):
                varxds = xds.transpose(*rcoordvars)
        '''
        ismultiple = isinstance(var,(list,tuple))
        #print(varxds.attrs)
        if len(dims) == 3:
            for i,dt in enumerate(xds[coordvars[-1]]):
                
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
                
                
                if ismultiple:
                    vards = [xds[v][i,:,:] for v in var]
                    vard = xarray.merge(vards)
                else:
                    vard = xds[var][i,:,:]
                    
                if dimension_process and  str(var) in dimension_process:
                    vard, var2 = dimension_process[str(var)](vard)
        
                    processTile(vard,dtt,var2,additionalSub,tiffprops = {"tiled":False, "windowed":False,"overview_level":4,"driver":"COG"})
                else:
                    processTile(vard,dtt,var,additionalSub)
                
                
        elif len(dims) > 3:
            
            # grab the var or the first one to extract the dims, if a list they must share dims
            if ismultiple:
                varxds = xds[var[0]]
            else:
                varxds = xds[var]
                
            if dimesion_order is None:
                dimesion_order = [str(d) for d in varxds.dims]
                
            # figure out over which dim the time dim is iterating
            tidx = dimesion_order.index(coordvars[-2])
            edim = coordvars[-2]
            edimn = varxds.shape[tidx]
            
            
            if tidx == 0:
                for idx,idxval in enumerate(varxds[coordvars[-2]]):
                    for i,dt in enumerate(varxds[coordvars[-1]]):
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
                            
                        if ismultiple:
                            vards = [xds[v][idx,i,:,:] for v in var]
                            vard = xarray.merge(vards)
                        else:
                            vard = varxds[idx,i,:,:]
                    
                        
                        
                        
                        if dimension_process and  str(var) in dimension_process:
                            vard, var = dimension_process[str(var)](vard)
                            processTile(vard,dtt,var2,f"{additionalSub}_{coordvars[-2]}-{str(idxval.values)}")
                        else:
                    
                            processTile(vard,dtt,var,f"{additionalSub}_{coordvars[-2]}-{str(idxval.values)}")
                    
                
            elif tidx == 1:
                #for idx in range(edimn):
                for idx,idxval in enumerate(varxds[coordvars[-2]]):
                    for i,dt in enumerate(varxds[coordvars[-1]]):
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
                            
                            
                        if ismultiple:
                            vards = [xds[v][i,idx,:,:] for v in var]
                            vard = xarray.merge(vards)
                        else:
                            vard = varxds[i,idx,:,:]
                            
                       
                        
                        if dimension_process and  str(var) in dimension_process:
                            vard, var2 = dimension_process[str(var)](vard)
                            processTile(vard,dtt,var2,f"{additionalSub}_{coordvars[-2]}-{str(idxval.values)}")
                        else:
                            processTile(vard,dtt,var,f"{additionalSub}_{coordvars[-2]}-{str(idxval.values)}")
                
                
                
def rasterFromNetcdfProcesseduv2ws(fpath, coordvars, valuevars , outdir,  isFractionalDate = False, reorder = True, minyear = -1, dimesion_order = -1,additionalSub=""):
    from functools import partial
    import xarray
    
    def reshape(vars,varsin):
        from pygeom.geom import calcSpeedDirection
        varin1 = varsin[vars[0]]
        varin2 = varsin[vars[1]]
        
        direction,speed = calcSpeedDirection(varin1,varin2)
        speed.name = f'speed_{varin1.name}'
        
        speeed0 = speed.assign_attrs(**{**varin1.attrs,"units": "m.s⁻¹"})
        speed.attrs["units"] = "m.s⁻¹"
        direction.name =f'direction_{varin2.name}'
        direction.attrs["units"] = 'deg'
        return xarray.merge([speed,direction]), [speed.name,direction.name]
    
    dimension_process = {}
    for var in valuevars:
        if isinstance (var,(tuple, list)):
            if any(['u' in v for v in var]):
                 dimension_process[str(var)] = partial(reshape, var)
    
    
    return rasterFromNetcdf(fpath, coordvars, valuevars , outdir,  
                            isFractionalDate = isFractionalDate, reorder = reorder, 
                            minyear = minyear, dimesion_order = dimesion_order, dimension_process=dimension_process,additionalSub=additionalSub)
            
def buildImageMosaic(indir,timeformat, params= {}, storename = None, wildcard = "*.tif", defaultStyle = None):
    
    from pathlib import Path
    from pyrest.geoserver import GSAuthClient, createCOGImageStoreTemporal,createAndPublishCOG,createAndPublishCoverage
    
    #gsclient = GSAuthClient({"url":"http://localhost:18080/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    gsclient = GSAuthClient({"url":"http://oa-gis.csiro.au/geoserver/rest","auth-key":"364c9181-6e30-4cb6-b1ab-e67177a5024c","auth":"gs-auth"})
    print(gsclient.getWorkspaces())
    
    indirp = Path(indir)
    
    flist = list(indirp.glob(wildcard))
    '''
    reflist = []
    for f in flist:
        ref = createAndPublishCoverage(gsclient,"test",f.stem,f.stem,f,**{"isCOG":True})
        reflist.append(ref)
    '''
    
    #tempdir, workspaceName,storeName, imagelist,baseurl,
    
    if storename is None:
        storename = indirp.name
    createCOGImageStoreTemporal(gsclient,indirp.joinpath("tmp"),"test",storename,flist,timeformat, params=params,defaultStyle=defaultStyle)        


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
    
    
    
#http://localhost:18080/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities
#http://hamster-hf.nexus.csiro.au:18080/geoserver/ows?service=WMS&version=1.3.0&request=GetCapabilities

#https://oa-gis.csiro.au/geoserver/ows?REQUEST=GetCapabilities&VERSION=1.3.0&SERVICE=WMS

#rasterFromNetcdf("/Users/ros260/projects/data/ML/enso-ops/output/sst-anom-smoothed_1991-2020_remapped.nc",["lon","lat","time"],["sst_anom"],"/Users/ros260/projects/data/ML/enso-ops/output/tiff.r.4")


#rasterFromNetcdf("/Users/ros260/projects/git/enso-data/data/po_jra55npv2_2x30d_T_L1_1958_2022-2.nc",["lon","lat","time"],["data"],"/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0",True,False, 1978)


#calculateAnomaly("/Users/ros260/projects/git/enso-data/data/po_jra55npv2_2x30d_T_L1_1958_2022-2.nc",["lon","lat","time"],["data"],"/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom",True)


#rasterFromNetcdf("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/po_jra55npv2_2x30d_T_L1_1958_2022-2-means.nc",["lon","lat","time"],["data-anom"],"/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff-test",False,False, 1978)




nemo_combined_vasrs= ["fbiom_bathy","fbiom_epi","fbiom_hmbathy","fbiom_mbathy","fbiom_meso","fbiom_mmeso","o2_l1","o2_l2","o2_l3","pp","sst","temp_l1","temp_l2","temp_l3"]
nemo_combined_vect = [["u_l1","v_l1"],["u_l2","v_l2"],["u_l3","v_l3"]]
'''
## reference ds
#rasterFromNetcdf("/Users/ros260/projects/data/ML/enso-ops/output/nemo_combined_1958_2022_detrended_1979-2022.nc",["lon","lat","time"],nemo_combined_vect,"/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-test",False,False, 2001)

#rasterFromNetcdfProcesseduv2ws("/Users/ros260/projects/data/ML/enso-ops/output/nemo_combined_1958_2022_detrended_1979-2022.nc",["lon","lat","time"],nemo_combined_vect,"/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-uv",False,False, 2001)

#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff3",timeformat='[0-9]{8},format=yyyyMMdd',params={"LevelsNum":4})
#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/po_jra55npv2.0-anom/tiff3",timeformat='[0-9]{8},format=yyyyMMdd',params={"LevelsNum":4})

#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-uv",timeformat='[0-9]{8},format=yyyyMMdd',storename="ref_wind_l1E",wildcard="speed_u_l1_direction_v_l1-*E.tif",params={"LevelsNum":4})
#buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined",timeformat='[0-9]{8},format=yyyyMMdd',storename="ref_temp_l1E",wildcard="temp_l1-*E.tif",params={"LevelsNum":4})

for v in nemo_combined_vasrs:
    buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"ref_{v}1E",wildcard=f"{v}-*E.tif",params={"LevelsNum":4})
    buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"ref_{v}1W",wildcard=f"{v}-*W.tif",params={"LevelsNum":4})


for v in nemo_combined_vect:
    buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-uv",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"ref_{v[0]}E",wildcard=f"speed_{v[0]}_direction_{v[1]}-*E.tif",params={"LevelsNum":4})
    buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-uv",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"ref_{v[0]}W",wildcard=f"speed_{v[0]}_direction_{v[1]}-*W.tif",params={"LevelsNum":4})
'''

# predicted
#netcdfEnforceUNICODE("/Users/ros260/projects/data/ML/enso-ops/output/predicted_nemo_fields_rescaled.nc")

#rasterFromNetcdf("/Users/ros260/projects/data/ML/enso-ops/output/predicted_nemo_fields_rescaled.nc",["lon","lat","lead_time","target_date"],nemo_combined_vasrs,"/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-test",False,False, 2001)
'''
for i in range(24):
    rasterFromNetcdf(f"/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/predicted/predicted_nemo_fields_rescaled.{i}nc",["lon","lat","target_date"],nemo_combined_vasrs,"/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted",reorder = False, minyear = 2001,additionalSub=f"_lead_time-{i+1}")
'''

for i in range(24):
    rasterFromNetcdfProcesseduv2ws(f"/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/predicted/predicted_nemo_fields_rescaled.{i}nc",["lon","lat","target_date"],nemo_combined_vect,"/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted-uv",reorder = False, minyear = 2001,additionalSub=f"_lead_time-{i+1}")
'''

for v in nemo_combined_vasrs:
    for i in range(24):
        buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v}_lead_time{i+1}E",wildcard=f"{v}lead_time-{i+1}-*E.tif",params={"LevelsNum":4})
        buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v}_lead_time{i+1}W",wildcard=f"{v}lead_time-{i+1}-*W.tif",params={"LevelsNum":4})
'''
'''
for v in nemo_combined_vect:
    for i in range(24):
        buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted-uv",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v[0]}_lead_time{i+1}E",wildcard=f"speed_{v[0]}_direction_{v[1]}_lead_time-{i+1}-*E.tif",params={"LevelsNum":4})
        buildImageMosaic("/Users/ros260/projects/data/ML/enso-ops/output/analysisMarch2026SPC/nemo_combined-predicted-uv",timeformat='[0-9]{8},format=yyyyMMdd',storename=f"pred_{v[0]}_lead_time{i+1}W",wildcard=f"speed_{v[0]}_direction_{v[1]}_lead_time-{i+1}-*W.tif",params={"LevelsNum":4})
'''

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