import os, sys, json

DEBUG_GEOM_LOADING=False

from pygeom.utils import timeout
from pygeom import stopwatch
from pathlib import Path
from pygeom.process import LogHandler

from pygeom.utils import FeaturesCacheManager
from pygeom.geom import Geometries

LoadGeoms_MESSAGE_CATEGORY = 'Load-Geometries-Task'

def loadGeometry(actualpath,params,cancelHandler,layernameprefix="trip_seg_",progress = 20.):
    if "layername" in params:
        fn = params["layername"]
    else:
        fn = os.path.basename(actualpath)
        fn = os.path.splitext(fn)[0]
    
    istimeout = None
    def timeoutCancel(fname):
        istimeout = fname
        print("Called timeout on {}".format(fname))
    
    
    geoms = timeout(10, Geometries.buildInit, timeoutCancel ,*[actualpath,fn])
    
    #geoms = timeout(10,loadLayer,timeoutCancel,*[actualpath,layernameprefix+fn,driver])
    
    if geoms:
        validl = geoms.isValid()
        cancelHandler.logMessage("Loaded layer {} {}".format(fn,validl) )
    else:
        if istimeout:
            cancelHandler.logMessage("Loaded layer {} failed, did not load in due time ".format(actualpath) )
        else:
            cancelHandler.logMessage("Loaded layer {} failed ".format(actualpath) )
        return None,None
    
    if cancelHandler and cancelHandler.isCanceled():
        return None,None,None
    
    cancelHandler.increment(progress)
    if cancelHandler.isCanceled():
        return None,None,None
    ncaches = 4
    if 'nreplicas' in params:
        ncaches = int(float(params['nreplicas']))
        
    if not validl:
        ncaches = 0
    inmemory = True
    if "in-memory-clone" in params:
        inmemory = 'f' in params["in-memory-clone"].lower() or '0' == params["in-memory-clone"]
    
    if cancelHandler and DEBUG_GEOM_LOADING:
        cancelHandler.loginfo("Creating CacheManager for {} ".format(params))
        
    cres = FeaturesCacheManager(geoms,ncaches,cancelHandler=cancelHandler,
                        inmemory=inmemory,
                        attrmap = {k[5:]:v for k,v in params.items() if k.startswith("attr-")})
    
    if cancelHandler:
        cancelHandler.setProgress(90.)
    return [geoms ,cres ]
    
    

def loadPortGeometry(parentpath,params,cancelHandler,layernameprefix="trip_seg_"):
    actualpath = os.path.join(parentpath,params['filename'])
    if not cancelHandler is None and cancelHandler.isCanceled():
        return None, None
    
    layer,cManager = loadGeometry(actualpath,params,cancelHandler,layernameprefix=layernameprefix)
    
    
    return  [layer,cManager]#,[cManager.attrMap("id","id"),cManager.attrMap("name","name")]]



def loadGeometryCache(parentpath,params,cancelHandler,layernameprefix):
    if 'noload' in params:
        params["filename"] = os.path.join(parentpath,params["filename"])
        return params
    
    actualpath = os.path.join(parentpath,params['filename'])
    if not cancelHandler is None and cancelHandler.isCanceled():
        return None
    if DEBUG_GEOM_LOADING:
        cancelHandler.logMessage(f"Loading from path '{actualpath}'" ,LoadGeoms_MESSAGE_CATEGORY)
    layer,cManager = loadGeometry(actualpath,params,cancelHandler,layernameprefix)
    return [layer,cManager]



def loadPortsGeometryCacheDict(parentpath,params,cancelHandler,layernameprefix="ports_"):
    ports= list()
    pincrement = 20./len(params)
    for p in params:
        reslist = loadPortGeometry(parentpath,params[p],cancelHandler,p)
        if not cancelHandler is None and cancelHandler.isCanceled():
            return None
        ports.append(reslist[:3])
        
        cancelHandler.setProgress(10.+pincrement)
        
    return ports


def loadPortsGeometryCacheList(parentpath,params,cancelHandler,layernameprefix="ports_"):
    ports= list()
    pincrement = 20./len(params)
    for p in params:
        reslist = loadPortGeometry(parentpath,p,cancelHandler,p["layername"])
        if not cancelHandler is None and cancelHandler.isCanceled():
            return None
        ports.append(reslist[:3])
        cancelHandler.setProgress(10.+pincrement)
        
    return ports


def loadMultipleGeometryCache3(parentpath,paramlist,cancelHandler,layernameprefix=""):
    geoms = list()
    #layerIndex = list()
    #layerCaches= list()
    pincrement = 20./len(paramlist)

    for i,p in enumerate(paramlist):
        if 'noload' in p:
            p["filename"] = os.path.join(parentpath,p["filename"])
            geoms.append(p)
            continue
        
        actualpath = os.path.join(parentpath,p['filename'])
        if DEBUG_GEOM_LOADING:
            cancelHandler.logMessage("Loading layer from path '{}'".format(actualpath) ,LoadGeoms_MESSAGE_CATEGORY,cancelHandler.Debug)
            
        reslist = loadGeometry(actualpath,p,cancelHandler,"",progress = pincrement)
        if not cancelHandler is None and cancelHandler.isCanceled():
            return None
        ll = reslist[0]
        if not ll.isValid():
            continue
        geoms.append(reslist)
        
    return geoms


def loadPortsGeometryCache3(parentpath,params,cancelHandler,layernameprefix="ports_"):
    
    for p in params:
        if isinstance(p,(list,tuple)):
            return loadPortsGeometryCacheList(parentpath,params,cancelHandler,layernameprefix="ports_")
        else:
            return loadPortsGeometryCacheDict(parentpath,params,cancelHandler,layernameprefix="ports_")
    
    raise ValueError("No data for ports")





class LoadGeomTask(LogHandler):

    
    def __init__(self, parentPath,callablefunc,params, callname,bbox = None):
        #super().__init__("Load Geometry "+callname)
        LogHandler.__init__(self,f"Load Geometry - {callname}")
        self._parentPath = parentPath
        self._params = params
        self._bbox = bbox
        self._result = None
        self.exception = None
        self.callablefunc = callablefunc
        self.callname = callname
        
        self.cancelSimple = None
        
    def description(self):
        return self.callname
    
    def run(self):

        """
        Should periodically test for isCanceled() to gracefully
        abort.
        This method MUST return True or False.
        Raising exceptions will crash QGIS, so we handle them
        internally and raise them in self.finished
        """
        self.logMessage(f'Started task "{self.callname}"',
                                 LoadGeoms_MESSAGE_CATEGORY)
        
        
    
        try:
            
            if self.isCanceled():
                return False
            
            self._result = self.callablefunc(self._parentPath,self._params,self,self.callname)
            
            if self._result is None:
                self.logMessage("Failed Loading Geometry: "+self.callname,LoadGeoms_MESSAGE_CATEGORY,LogHandler.Error)
                print("Failed Loading Geometry: "+self.callname)
                return False
            self.logMessage(f"Loaded Geometry: {self.callname}",LoadGeoms_MESSAGE_CATEGORY)
            self.setProgress(100.)
            
            #self.doneSimple.emit(self.callname)
            return self.finished(True)
        except Exception as e:
            self.exception = e
            return False

            
    def finished(self, result):
        """
        This function is automatically called when the task has
        completed (successfully or not).
        You implement finished() to do whatever follow-up stuff
        should happen after the task is complete.
        finished is always called from the main thread, so it's safe
        to do GUI operations and raise Python exceptions here.
        result is the return value from self.run.
        """
        if result:
            self.logMessage(
                'Task "{name}" completed\n'.format(
                  name=self.description()),
              LoadGeoms_MESSAGE_CATEGORY)
            ## so we move this into the run call as finished is not always called across the versions if instantiated from another process 
            #self.doneSimple.emit(self.callname)
        
            
        else:
            if self.exception is None:
                self.logMessage(
                    'Task "{name}" on "{callname}" not successful but without '\
                    'exception (probably the task was manually '\
                    'canceled by the user)'.format(
                        name=self.description(),callname=self.callname),
                     LoadGeoms_MESSAGE_CATEGORY, LogHandler.Warning)
            else:
                self.logMessage(
                    'Task "{name}" on "{callname}" Exception: {exception}'.format(
                        name=self.description(),callname=self.callname,
                        exception=self.exception),
                    LoadGeoms_MESSAGE_CATEGORY, LogHandler.Critical)
                raise self.exception
            
        return self._result

    def cancel(self):
        self.cancelSimple(self.callname)
        
        self.logMessage(
            'Task "{name}" on "{callname}" was canceled [{cancel}]'.format(
                name=self.description(),callname=self.callname,cancel=str(self.isCanceled())),
            LoadGeoms_MESSAGE_CATEGORY, LogHandler.Info)
        super().cancel()
        
    def increment(self,progressInncr):
        self.setProgress(self.progress() + progressInncr)
        
        
def _testGeoPackageClean(striplayer, cancelHandler):
    #from osgeo import ogr
    
    if os.path.exists(striplayer+"-wal"):
        try:
            cancelHandler.logMessage("Removing left over wal for {}".format(striplayer),LoadGeoms_MESSAGE_CATEGORY,LogHandler.Warning)
            #print("removing left over wal for {}".format(striplayer))
            os.remove(striplayer+"-wal")
        except Exception as e:
            cancelHandler.logMessage("Error Removing left over wal for {}".format(e),LoadGeoms_MESSAGE_CATEGORY,LogHandler.Warning)
         
    if os.path.exists(striplayer+"-shm"):
        try:
            cancelHandler.logMessage("Removing left over shm for {}".format(striplayer),LoadGeoms_MESSAGE_CATEGORY,LogHandler.Warning)
            #print("removing left over shm for {}".format(striplayer))
            os.remove(striplayer+"-shm")
        except Exception as e:
            cancelHandler.logMessage("Error Removing left over shm for {}".format(e),LoadGeoms_MESSAGE_CATEGORY,LogHandler.Warning)
        
    

class ExecuteGeomTasks():
    
    
    def __init__(self,rootpath,params, cancelHandler):
        super().__init__()
        self._rpath = rootpath
        self._params = params
        self._results =  None
        self.load_tasks = {}
        self._cancelHandler =cancelHandler
        self._success = False
        self._finished = False
        
    @staticmethod 
    def hasDataFiles(datapath,datafile='geometry_*.v2.json'):
        dfilelist = list(Path(datapath).glob(datafile))
        if len(dfilelist) > 0:
            return dfilelist[0]
        return None
   
    @staticmethod
    def prependRelativePath(relpath, datadict):
        
        for k in datadict:
            entry = datadict[k]
            if isinstance(entry,(list,tuple)):
                for e in entry:
                    if 'filename' in e:
                        e['filename'] = os.path.join(relpath,e['filename'])
                        
                
            elif isinstance(entry,dict):
                if 'filename' in entry:
                    entry['filename'] = os.path.join(relpath,entry['filename'])
                else:
                    for e in entry:
                        if 'filename' in entry[e]:
                            entry[e]['filename'] = os.path.join(relpath,entry[e]['filename'])
        if "include" in datadict:
            includes =  datadict["include"]
            del datadict["include"]
            return includes
        
        return []
                   
        
    @staticmethod        
    def loadDataParams(cancelHandler, datapath,datafile='geometry_*.v2.json',additionalpath= [] ):
        import pathlib
        #datapath = os.path.join(app_path,'data')
        
        dfilelist = list(Path(datapath).glob(datafile))
        if  len (dfilelist) < 1:
            cancelHandler.logMessage(f"Not found data files for  {datafile} in {datapath}",LoadGeoms_MESSAGE_CATEGORY, LogHandler.Critical)
            return
        # grab the first one 
        with dfilelist[0].open() as fp:
            datadict = json.load(fp)
        
        additionalpath.extend(ExecuteGeomTasks.prependRelativePath(datadict["path"],datadict))
    
    
        for adir in additionalpath:
            userp = pathlib.Path(datapath,adir)
            if userp.exists():
                userdatalist = list(userp.glob("*.json"))

                cancelHandler.logMessage(f"found user data files {len(userdatalist)} ",LoadGeoms_MESSAGE_CATEGORY, LogHandler.Info)
                for ud in userdatalist:
                    if not ud.exists() :
                        continue
                    try:
                        with ud.open() as fp:
                            udatadict = json.load(fp)
                            # we ignoring further nests here 
                            ExecuteGeomTasks.prependRelativePath(adir,udatadict)
                            cancelHandler.logMessage(f"Adding from user path to {udatadict.keys()}",LoadGeoms_MESSAGE_CATEGORY)
                            if DEBUG_GEOM_LOADING:
                                cancelHandler.logMessage("Adding from user path  {}".format(udatadict.keys()),LoadGeoms_MESSAGE_CATEGORY)
                            for k in udatadict:
                                if k in datadict:
                                    if isinstance(datadict[k], list):
                                        datadict[k].extend(udatadict[k])
                                        if DEBUG_GEOM_LOADING:
                                            cancelHandler.logMessage("extended {} {} ".format(k,len(udatadict[k])),LoadGeoms_MESSAGE_CATEGORY)
                                    elif isinstance(datadict[k], dict):
                                        # this means the user defined stuff is also organised as dict with children has dicts
                                        for uk in udatadict[k]:
                                            datadict[k][uk] = udatadict[k][uk]
                                            if DEBUG_GEOM_LOADING:
                                                cancelHandler.logMessage("added to {}  {}".format(k,uk),LoadGeoms_MESSAGE_CATEGORY)
                                        #datadict[k][ud.stem] = udatadict[k]
                                else: # this is for special requests then there needs to be someone to read it...
                                    if not isinstance(udatadict[k], list):
                                        datadict[k] = [udatadict[k]]
                                    else:
                                        datadict[k] = udatadict[k]
                    except Exception as e:
                        cancelHandler.logMessage(f"Failed opening {ud} - {e}",LoadGeoms_MESSAGE_CATEGORY,LogHandler.Error)
        
        return datadict
    
    
    
    def cleanGPKG(self,root_path,params):
        
        flist = []
        
        for k in self._params:
            pd = self._params[k]
            if isinstance(params[k],(list,tuple)):
                for pd in params[k]:
                    try:
                        if "filename" in pd:
                            flist.append(pd["filename"].split("|")[0])
                    except Exception as e:
                        self._cancelHandler.logMessage(f"failed substring of filename  {pd} {e}",LoadGeoms_MESSAGE_CATEGORY, LogHandler.Info)
            elif isinstance(params[k],dict):
                for kk in  params[k]:
                    pd = params[k][kk]
                    try:
                        if "filename" in pd:
                            flist.append(pd["filename"].split("|")[0])
                    except Exception as e:
                        self._cancelHandler.logMessage(f"failed substring of filename  {pd} {e}",LoadGeoms_MESSAGE_CATEGORY, LogHandler.Info)
                        
        #logMessage("filelist {}".format(flist),LoadGeoms_MESSAGE_CATEGORY, Qgis.Info)
        flist = list(set(flist))
        #logMessage(" actual filelist {}".format(flist),LoadGeoms_MESSAGE_CATEGORY, Qgis.Info)
        
        for f in flist:
            _testGeoPackageClean(os.path.join(root_path,f),self._cancelHandler)
    
    
    '''
    def __assignResource(self,key):
        #logMessage("got signal "+key,LoadGeoms_MESSAGE_CATEGORY, Qgis.Info)
        self._results[key] = self.load_tasks[key]._result
    ''' 
        
        
    def __assignCancel(self,key):
        self._cancelHandler.logMessage(f"got cancel signal {key}",LoadGeoms_MESSAGE_CATEGORY)
        self._results[key] = False
        self.load_tasks[key] = None
        self.cancel()
        
        
    def clone(self):
        return ExecuteGeomTasks(self._rpath,self._params)
        
    def execute(self):

        ## If this is not the first time how do we release resources
        self._results = {}
        tasks = []
        with stopwatch(f"{self.__class__.__name__}: cleaning gpkg references"):
            self.cleanGPKG(self._rpath,self._params)
        
        with stopwatch(f"{self.__class__.__name__}: collecting context data async"):
            from .collect import createTask,waitForRequests
            import asyncio
            loop = asyncio.new_event_loop()
            try:
                
                
                
                    
                for k in self._params:
                    if DEBUG_GEOM_LOADING:
                        self._cancelHandler.logMessage(f"starting geom task {k}",LoadGeoms_MESSAGE_CATEGORY,LogHandler.Debug)
                    
                        
                    if "ports" == k:
                        tt = LoadGeomTask(self._rpath,loadPortsGeometryCache3,self._params[k],k)
                        tasks.append(createTask(tt.run,k,[],loop= loop))
                    elif isinstance(self._params[k],(list,tuple)):
                        tt = LoadGeomTask(self._rpath,loadMultipleGeometryCache3,self._params[k],k)
                        tasks.append(createTask(tt.run,k,[],loop= loop))
                    elif not isinstance(self._params[k],dict):
                        continue
                    else:
                        tt = LoadGeomTask(self._rpath,loadGeometryCache,self._params[k],k)
                        tasks.append(createTask(tt.run,k,[],loop= loop))
                        
                    
                    
                    tt.cancelSimple = self.__assignCancel
                    
                asyncio.set_event_loop(loop)
                res, failed = loop.run_until_complete(waitForRequests(tasks))
                
                self._cancelHandler.logMessage(f" return contxt len {len(res.keys())}")
                self._cancelHandler.logMessage(f" errors {failed}")
                #self._cancelHandler.logMessage((f"response  {res}")
                self._results = res
                self._success = True
            except Exception as e:
                self._cancelHandler.logMessage(f"Failed loading Geometries {e}",LoadGeoms_MESSAGE_CATEGORY,LogHandler.Error)
                self._success = False
            finally:
                loop.close()
                self._finished = True
        
                
    def isEnded(self):
        return  self._finished
    
    def isSuccessful(self):
        #self.getResults()
        return self._success
    
    def cancel(self):
        for k in self.load_tasks:
            tt = self.load_tasks[k]
            if not tt is None:
                try:
                    if tt.cancel():
                        return False
                except: # we lost tt so have ended..
                    pass
            
        
    def getResults(self):
        return self._results
    
    
    
 
                
                