import cProfile, os
print(os.environ["PATH"])
if not 'opt/local/bin' in os.environ["PATH"]:
    os.environ["PATH"] = f'{os.environ["PATH"]}:/opt/local/bin'


def test_log():
    
    from pygeom.process import LogHandler
    
    
    lhandler = LogHandler()
    
    lhandler.logMessage("test")
    lhandler.logMessage("test","test")
    lhandler.logMessage("test","more",LogHandler.Warning)
    lhandler.logMessage("test","more",LogHandler.Error)
    lhandler.logMessage("test","more",LogHandler.Connect)
    lhandler.logMessage("test","more",LogHandler.Lock)
    lhandler.logMessage("test","more",LogHandler.Warning)
    lhandler.logMessage("test","more",LogHandler.Time)
    lhandler.logMessage("test","more",LogHandler.Tooling)
    lhandler.setProgress(1)



def test_load_geom(plugin_dir="/Users/ros260/projects/git/qgis-trips/src/trip_analytics"):
    from pygeom.process.load import ExecuteGeomTasks
    from pygeom.process import LogHandler
    import time, os

    
    

    SCAN_USER_DATA = True
    
    datapath = os.path.join(plugin_dir,'data')
    additional = []
    if SCAN_USER_DATA:
        additional.append('user')
            
    loghandler =  LogHandler()
    datadict = ExecuteGeomTasks.loadDataParams(loghandler,datapath,additionalpath=additional)
  
    loadgeom = ExecuteGeomTasks(datapath,datadict,loghandler)
    
    loadgeom.execute()           
    while not loadgeom.isEnded():
        time.sleep(1)
        
        
    print(loadgeom.isSuccessful())


def make_callgrap(callfunc,outputfile ):    
    
    from pycallgraph import PyCallGraph
    from pycallgraph.output import GraphvizOutput
    
    output=GraphvizOutput()
    output.output_file = outputfile
    with PyCallGraph(output=output):
        callfunc()
        
            
#cProfile.run("test_load_geom()")



def test_fioanGPKG(infile, outfile,append = False):
    from pygeom import writeToGPKG,writeToSHP
    '''
    ,appendToGPKG
    if append:
        appendToGPKG(infile, outfile)
    else:
        writeToGPKG(infile, outfile)
    '''
    writeToGPKG(infile, outfile)
    writeToSHP(infile, outfile)


#test_fioanGPKG("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/test-pygeom/v0-4-2025.v2/4591b9/Vessel_45626_pointgeom.json","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/test-pygeom/v0-4-2025.v2/4591b9/Vessel_45626_pointgeom-v3.gpkg")
test_fioanGPKG("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/test-pygeom/v0-4-2025.v2/4591b9/Vessel_45626_linegeom.json","/Users/ros260/projects/data/AIS_VMS/AIS/nsw/test-pygeom/v0-4-2025.v2/4591b9/Vessel_45626_pointgeom-v4.gpkg")


