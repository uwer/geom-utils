

import pathlib, sys,os,pandas as pd

def merge(basedir, outdir, suffix="csv", sortcolumn = None, name_re=None):
    import re
    
    
    f = pathlib.Path(basedir)
    opath = pathlib.Path(outdir)
    opath.mkdir(parents =True, exist_ok=True)
    
    
    if not f.exists():
        path = str(f)
        wci = path.find('*')
        wci2 = path.find('[')
        if wci < 0 and wci2 < 0:
            raise ValueError(f"this doesn't seem to be  a valid path {f} - does not exist and has no wildcard")
        
        if wci < 0:
            wci = wci2
        elif wci2 > 0:
            wci = min(wci, wci2) 
        
        wpi = path.rfind("/",0,wci)
        if wpi < 0:
            raise ValueError(f"this doesn't seem to be  a valid path {f} - does not have a parent directory before wildcard {path[:wci]}")
        
        f2 = pathlib.Path(path[:wpi])
        if not f2.exists():
            raise ValueError(f"this doesn't seem to be  a valid path {f2}")
        
        wc = f"{path[wpi+1:]}/"
        bpath = f2
        print(f"found path {f2} with wildcard {wc}")
    
    
    
    else:
        wc = ""
        bpath = f
        
    flist = list(bpath.glob(f"{wc}*.{suffix}"))
    print(f"nfiles = {len(flist)}")
    if not name_re is None:
        print(f"on name reg ex {name_re}")
        mare = re.compile(name_re)
        names = []
        for f in flist:
            mg = mare.search(f.name)
            if not mg is None:
                names.append(mg.group())
     
                
    else:
        names = [f.name for f in flist]
    namesunique = list(set(names))
    print (f"unique names {len(namesunique)}")
    multiples = []
    for fn in namesunique:
        if names.count(fn) > 1:
            multiples.append(fn)
    
    for fn in multiples:
        collected = [f for f in flist if fn in f.name]
        print(f"merging nfiles {len(fn)} for {len(collected)}")
        dfs = [pd.read_csv(str(f)) for f in collected]
        #df = dfs[0]
        #df= dfs[0].append(dfs[1:])
        #for dd in dfs[1:]:
        #    df = df.concat(dd)
        df = pd.concat(dfs, ignore_index=True)
        if sortcolumn:
            df.sort_values(sortcolumn,inplace = True)
        if not name_re is None:
            df.to_csv(os.path.join(outdir,fn+".csv"),index=False)
        else:
            df.to_csv(os.path.join(outdir,fn),index=False)
        
        
        
    
    
'''
run as 
python merge_csv.py '${INDIR}/2024/2024-1[0,1,2]' ${OUTDIR}/2024/2024-10_12 MsgTime "5\d\d\d\d\d\d\d\d"

<indir with wildcards> in single quotes!!!  outdir 'column header too sort by' 'regex wildcard to filter file names by'  

'''
            
            
            
if __name__ == "__main__":
    
    if len(sys.argv) < 3:
        print("need at least 2 args")
        sys.exit(1)
    
    sortc = None
    matchre = None
    
    if len(sys.argv) > 3:
        sortc = sys.argv[3]
    
    if len(sys.argv) > 4:
        matchre = sys.argv[4]    
        
    merge(os.path.expandvars(sys.argv[1]),sys.argv[2],sortcolumn=sortc,name_re = matchre)
    