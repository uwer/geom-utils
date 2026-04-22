

def skimGrid(infile, overlaygeoms, outfile):
    from pygeom.geom import union
    from pathlib import Path
    
    outp = Path(outfile)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp = outp.with_name(f"{outp.stem}.tmp{outp.suffix}")
    
    union(infile, overlaygeoms[0], str(outp), inverse=True) # masking out land
    union(str(outp), overlaygeoms[1],outfile) # masking out off shore buffer


def test(infile, overlaygeoms, outfile, buffer = 0.005):
    
    
    from pygeom.geom import union
    
    union(infile, overlaygeoms, outfile,buffer=buffer,migrateAttr={"grid_id"})
    
def trim(infile, overlaygeom, outfile):
    from pygeom.geom import union
    union(str(infile), overlaygeom,outfile) # masking out off shore buffer
    
    
if __name__ == "__main__":
    '''
    run as 
    skimGrid(<INGRID>,[<LANDMASK - to cut>,<offshore boundary - to keep>],<output file>)
    
    '''
    
    
    #test("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2025-combined/density/grid_frdc.rect.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/study_area.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2025-combined/density/grid_frdc.sub.rect.fgb")
    #test("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_frdc.06.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/study_area_clean.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_frdc.clean.sub.hex06.fgb")
    #test("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_clean.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/study_area_clean.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_centre_frdc.sub.rect.exact2.fgb", buffer = -1)
    #skimGrid("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_clean.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/study_area_clean.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_centre_frdc.sub.rect.exact2.fgb", buffer = -1)
    #skimGrid("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/gridnsw_raw.fgb",["/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/oz_simple.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026tests/oz_simple_buffered0.4.fgb"],"/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/gridnsw.fgb")
    #skimGrid("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/gridnsw_raw2.fgb",["/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/oz_simple.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026tests/oz_simple_buffered_reporting.fgb"],"/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/gridnsw-reporting.fgb")
    #trim("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/gridnsw-reporting.tmp.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/oz_simple_buffered_reporting.geojson","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/gridnsw-reporting.fgb")
    trim("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/gridnsw-reporting-raw.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/background/gridcells-reporting.fgb","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/2026-NEW/gridnsw-reporting.fgb")
    