

def skimGrid(infile, overlaygeoms, outfile):
    from pygeom.geom import union
    from pathlib import Path
    
    outp = Path(outfile)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp = outp.with_name(f"{outp.stem}.tmp{outp.suffix}")
    
    trim(infile, overlaygeoms[0], str(outp))
    contain(str(outp), overlaygeoms[1],outfile)
    
    #union(infile, overlaygeoms[0], str(outp), inverse=True) # masking out land
    #union(str(outp), overlaygeoms[1],outfile) # masking out off shore buffer



def trim(infile, overlaygeom, outfile):
    from pygeom.geom import union
    union(str(infile), overlaygeom,outfile, inverse=True) # masking out off shore buffer
    
def contain(infile, overlaygeom, outfile):
    from pygeom.geom import union
    union(str(infile), overlaygeom,outfile) # masking out off shore buffer
    
    

if __name__ == "__main__":
    
    '''
    run as 
    skimGrid(<INGRID>,[<LANDMASK - to cut>,<offshore boundary - to keep>],<output file>)
    
    '''
    print("no defs ....")