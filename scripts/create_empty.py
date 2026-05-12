from pyproj import proj
import json


def createNETCDF(crs,vars,dims,outfile):
    '''
    expects dict with atribute names and extents for both vars and dims 
    
    i.e.
    
    "epsg:4326",
                 {"lon":{"dims":["lon"],"dtype":float,"attributes":{"long_name":"Longitude","units":"degrees_east","axis":"X"}},
                  "lat":{"dims":["lat"],"dtype":float,"attributes":{"long_name":"Latitude","units":"degrees_north","axis":"Y"}},
                  "count":{"dims":["lat","lon"],"dtype":float,"attributes":{"long_name":"count"}}},
                 {"lat":1261,"lon":350},
                 <outputfile>
                 
    
    
    '''
    
    import h5netcdf
    import numpy as np
    
    with h5netcdf.File(outfile, "w") as f:
        # set dimensions with a dictionary as dim:size
        f.dimensions = dims
        
        if not crs is None:
            v = f.create_variable("crs", (),'|S1')
            from pyproj import CRS
            pcrs  =CRS(crs)
            cfdict = pcrs.to_cf()
            #print(json.dumps(cfdict))
            for kk in cfdict:
                if kk == "crs_wkt":
                    continue
                v.attrs[kk] = cfdict[kk]
            '''
            if "grid_mapping_name" in cfdict:
                v.attrs["grid_mapping_name"] = cfdict["grid_mapping_name"]
                v.attrs["epsg_code"] = str(pcrs)
                v.attrs["inverse_flattening"] = pcrs.ellipsoid.inverse_flattening
                v.attrs["semi_major_axis"] = pcrs.ellipsoid.semi_major_metre
            '''
            
        
        for var in vars:
            vardict = vars[var]
            dtype = vardict.get("dtype",float)
            vdims = vardict["dims"]
            v = f.create_variable(var, vdims,dtype)
            for va in vardict["attributes"]:
                v.attrs[va] = vardict["attributes"][va]
                
            dimsn = []
            for d in vdims:
                dimsn.append(dims[d])
                  
            fillvalue = np.nan  
            if "fillvalue" in vardict:
                fillvalue = vardict["fillvalue"]
                
            if len(dimsn) == 1:
                v[:] = np.full(dimsn,fillvalue,dtype=dtype)
            elif len(vardict["dims"]) == 2:
                v[:,:] = np.full(dimsn,fillvalue,dtype=dtype)
            elif len(vardict["dims"]) == 3:
                v[:,:,:] = np.full(dimsn,fillvalue,dtype=dtype)
            elif len(vardict["dims"]) == 3:
                v[:,:,:] = np.full(dimsn,fillvalue,dtype=dtype)
                
                    
        # you don't need to create groups first
        # you also don't need to create dimensions first if you supply data
        # with the new variable
        #v = f.create_variable("/grouped/data", ("y",), data=np.arange(10))

    
    
def createFeatureGrid(crs,vars,dims,outfile):
    """
    create a regular polygon space given origin spacing etc
    
    """
    
    
    
    
    
if __name__ == "__main__":
    
    
    
    createNETCDF("epsg:4326",
                 {"lon":{"dims":["lon"],"dtype":float,"attributes":{"long_name":"Longitude","units":"degrees_east","axis":"X"}},
                  "lat":{"dims":["lat"],"dtype":float,"attributes":{"long_name":"Latitude","units":"degrees_north","axis":"Y"}},
                  "count":{"dims":["lat","lon"],"dtype":float,"attributes":{"long_name":"count"}}},
                 {"lat":1261,"lon":350},
                 "/Users/ros260/projects/data/AIS_VMS/AIS/nsw/grid.nc")
    