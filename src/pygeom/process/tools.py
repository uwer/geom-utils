
from pygeom.process import LoggingDelegate


logger =  LoggingDelegate("pygeom.process.tools")


def filterByProximity(geom1,geom2wrapper,distanceBufferFunc=None,prjoj1transfomrmFunc=None, resultasProjected=False,testonly=False, attributelist=[]):
    '''
    test all elements of geom1 if they are in the proximity of geom2 given distance
    if prjoj1transfomrmFunc is not None, geom1 is projected using the transform function.
    the transform has to be initialised and be a valid argument to shapely.op.transform  
    (the result of csiro.geom.createProjTransform) - if this is the case the distance parameter must reflect the respetive unit
    
    distanceBufferFunc - instead of providing a distance , we expect a prepared partial 
            function that will produce a buffered geometry matching the target geometry.
     
    
    '''
    from pygeom import Feature

    resultList = []
    gcount = 0
    if isinstance(geom1[0],Feature):
        
        if prjoj1transfomrmFunc:
            from shapely.ops import transform
            if resultasProjected :
                for f in geom1:
                    g2 = transform(prjoj1transfomrmFunc,f.geometry)
                    g3 = distanceBufferFunc(g2)
                    #g3 = g2.buffer(distance,5)
                    interlist = geom2wrapper.operation('overlaps',g3)
                    if interlist and len(interlist) > 0:
                        resultList.append(Feature(g2,properties=f.properties,feature_id=f.id)) # return the projected geometry
                        
                    gcount+=1
                    if gcount % 100 == 0:
                        print("processed {} features".format(gcount))
                        if testonly and gcount >= 1000:
                            break

            else:
                for f in geom1:
                    g2 = transform(prjoj1transfomrmFunc,f.geometry)
                    g3 = distanceBufferFunc(g2)
                    interlist = geom2wrapper.operation('overlaps',g3)
                    if interlist and len(interlist) > 0:
                        resultList.append(f)
                        
                    gcount+=1
                    if gcount % 100 == 0:
                        print("processed {} features".format(gcount))
                        if testonly and gcount >= 1000:
                            break
        else:
            for f in geom1:
                g3 = distanceBufferFunc(f.geometry)
                #g3 = f.geometry.buffer(distance,5)
                interlist = geom2wrapper.operation('overlaps',g3)
                if interlist and len(interlist) > 0:
                    resultList.append(f)
                
                gcount+=1
                if gcount % 100 == 0:
                    print("processed {} features".format(gcount))
                    if testonly and gcount >= 1000:
                            break
    else: 
        
        if prjoj1transfomrmFunc:
            from shapely.ops import transform
            if resultasProjected :
                for g1 in geom1:
                    g2 = transform(prjoj1transfomrmFunc,g1)
                    g3 = distanceBufferFunc(g2)
                    #g3 = g2.buffer(distance,5)
                    interlist = geom2wrapper.operation('overlaps',g3)
                    if interlist and len(interlist) > 0:
                        resultList.append(g2) # return the projected geometry
                    
                    gcount+=1
                    if gcount % 100 == 0:
                        print("processed {} features".format(gcount))
                        if testonly and gcount >= 1000:
                            break
            else:
                for g1 in geom1:
                    g2 = transform(prjoj1transfomrmFunc,g1)
                    g3 = distanceBufferFunc(g2)
                    #g3 = g2.buffer(distance,5)
                    interlist = geom2wrapper.operation('overlaps',g3)
                    if interlist and len(interlist) > 0:
                        resultList.append(g1)
                    
                    gcount+=1
                    if gcount % 100 == 0:
                        print("processed {} features".format(gcount))
                        if testonly and gcount >= 1000:
                            break
        else:
            for g1 in geom1:
                g3 = distanceBufferFunc(g1)
                #g3 = g1.buffer(distance,5)
                interlist = geom2wrapper.operation('overlaps',g3)
                if interlist and len(interlist) > 0:
                    resultList.append(g1)
                
                gcount+=1
                if gcount % 100 == 0:
                    print("processed {} features".format(gcount))
                    if testonly and gcount >= 1000:
                        break
    
    return resultList




from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import ALL_COMPLETED,as_completed, wait as w
import threading
#from profilehooks import timecall
import csv,queue,faulthandler
from shapely.geometry import Point, Polygon
from shapely.ops import transform

import multiprocessing,json,logging

        

    
    
    
''' 
now use processing to dispatch the pickled parts
this relies on the fact that we need to use base shapely geometries to be pickled for transfer to different cpu
and a queue to push and receive results


'''
    
    
    
class QueueThread(threading.Thread):
    # not used as such anymore
    def __init__(self,delegtequeue,funccall,processcall = None,asbatch=False,swapGeometry = False):
        threading.Thread.__init__(self)
        self.lqueue = delegtequeue
        self.callf = funccall
        self.do_run = True
        self.swapGeometry = swapGeometry
        self.asbatch = asbatch
        self.inverseProj = None
        if processcall:
            self._doprocess = processcall
        else:
            self._doprocess = self.__process
        
    def run(self):
        import queue
        while self.do_run:
            try:
                args  = self.lqueue.get(block=True, timeout=1.)
                if self.asbatch:
                    for b in args:
                        self._doprocess(b,self.inverseProj)
                else:
                    self._doprocess(args,self.inverseProj)
            except queue.Empty:
                continue
            
        self.close_dump()
          
    def __process(self,arg,inverseProj):
        
        if self.swapGeometry:
            arg[-1].geometry = arg[-2]
            
        elif inverseProj:
            arg[-1].geometry = transform(inverseProj,arg[-2])
        
        arg2 = arg[-1]   
        
        self.callf(arg2)
        
          
    def setInvProjection(self,proj):
        self.inverseProj = proj
        self.swapGeometry = False
        
    def close_dump(self):
        pass
    
    def cancel(self):
        self.do_run = False
          
    def close(self):
        self.cancel()
        
        
          
class DumpQueueThread(QueueThread):
    
    def __init__(self,delegtequeue,outf,procescall = None,asbatch=False,swapGeometry = False):
        
        QueueThread.__init__(self, delegtequeue, self.dump, procescall,asbatch,swapGeometry)
        self.outf = outf
        
        
        
    def dump(self,arg):
        with open(self.outf,'a') as fp:
            fp.write(str(arg)+'\n')  
            fp.flush()

class DumpGeoJsonQueueThread(QueueThread):
    
    def __init__(self,delegtequeue,outf,procescall = None,asbatch=False,swapGeometry = False):
        
        QueueThread.__init__(self, delegtequeue, self.dump,procescall, asbatch,swapGeometry)
        self.outf = outf
        
        with open(self.outf,'w') as fp:
            fp.write('{"type": "FeatureCollection","features": [\n')
            
        self._stack = []
    
        
    def dump(self,arg):
        self._stack.append(json.dumps(arg.__geo_interface__))
        
        if len(self._stack) > 100:
            with open(self.outf,'a') as fp:
                for g in self._stack[:-1]:
                    fp.write(g+',\n')
                fp.flush()
            ## keep the last to avoid any miss formatting on close
            rec = self._stack[-1]
            self._stack.clear()
            self._stack.append(rec)
    
    def close_dump(self):
        if len(self._stack) >0:
            try:
                with open(self.outf,'a') as fp:
                    for g in self._stack[:-1]:
                        fp.write(g+',\n') 
                    fp.write(self._stack[-1]+"]}")
                    fp.flush()
            except Exception as e:
                print(e)
                with open(self.outf,'a') as fp:
                    fp.write("]}")
        else:
            with open(self.outf,'a') as fp:
                    fp.write("]}")


class ApplyBuffer():
    def __init__(self,distance):
        self.dist = distance
        
        
    def __call__(self,geoms):
        return [geoms[0].buffer(self.dist,5),*geoms]
    
    
    
class ApplyProj():
    
    def __init__(self,projtransform):
        self.projtransform = projtransform
        
        
    def __call__(self,geoms):
        from shapely.ops import transform
        geoms[0] = transform(self.projtransform,geoms[0])
        return geoms




class _ProcessEvalGeom(multiprocessing.Process):
    
    
    def __init__(self,ids,loadgeomf,geomfuncname,inqueue,outqueue,cancelstate = None, asbatch=False, applylist=[]):
        multiprocessing.Process.__init__(self)
        '''
        Process geometries retrieved from the inqueue, if valid return value, the output with the input is added to the out queue  
        inqueue - a Manager.Queue()
        outqueue - a Manager.Queue()
        loadgeomf - the path to the geometry to test against
        geomfuncname - the function to call in csiro.geom.GeometryWrapper
        cancelstate - the shared Manager.Value as 'i' to indicate cancel state
        asbatch - if we are expecting batches to be received from the queue
        
        
        '''
        self.inqueue = inqueue 
        self.outqueue = outqueue
        self.laodf = loadgeomf
        self.do_run = cancelstate# this is a Manager.Value - read only
        self.geomfuncname = geomfuncname
        self.asbatch = asbatch
        self.id = ids
        self.applies = applylist
        
    def setApplies(self,applylist):
        '''
        a list of callables that take an array of geometries representing one process, 
        the calls either modify the first element or prepend another
        this allows for adding projected versions and buffers
        the worker will always use the first in the list to execute the geometry test according to the operation passed  
        '''
        if self.is_alive():
            raise ValueError("setting these will not be propagated to the running process, already started!")
        self.applies = applylist
        
        
    def run(self,*args):
        import queue
        from pygeom.geom import Geometries
        ## so we do all this in the run func to make sure we have a local init
        logger.logMessage("process {} starting ".format(self.id))
        print("process {} starting ".format(self.id))
        self.geomwapper = Geometries()
        Geometries.buildInit(self.geomwapper,self.laodf)
        
        self.geomwapper.testValid(self.geomfuncname)
        '''
        #self.geomwapper.optimised()
        if not self.geomfuncname in Geometries.operations:
            raise ValueError("Geometries has no func "+self.geomfuncname)
        '''

        print("process {} started, finished setup ".format(self.id))
        
        
        emptyqueucalls = 0
        while self.do_run.value: # this is a Manager.Value
            try:
                qargs  = self.inqueue.get(block=True, timeout=1.)
                if self.asbatch:
                    for a in qargs:
                        obj = a
                        for apply  in self.applies:
                            obj = apply(obj)
                        res = self.geomwapper.operation(self.geomfuncname,obj[0]) 
                        if res :
                            if isinstance(res,(list,tuple)) :
                                if len(res) > 0:
                                    self.outqueue.put([*res,*obj])
                            else:
                                self.outqueue.put([res,*obj])
                else:
                    obj = qargs
                    for apply  in self.applies:
                        obj = apply(obj)
                    res = self.geomwapper.operation(self.geomfuncname,obj[0]) 
                    if res :
                        if isinstance(res,(list,tuple)) :
                            if len(res) > 0:
                                self.outqueue.put([*res,*obj])
                        else:
                            self.outqueue.put([res,*obj])
                
            except queue.Empty:
                emptyqueucalls += 1
                if emptyqueucalls % 100 == 0:
                    print("process {} timeout on empty queue {}".format(self.id,emptyqueucalls))
                #logger.logMessage("process {} timeout on empty queue ".format(self.id))
                continue
            
        
        
        
    def cancel(self):
        logger.logMessage("process {} cancelled ".format(self.id))
        
        

class RunGeomEval(threading.Thread):
    '''
    Run a gemetry filter operation over every element of target on source in multiple CPU, 
    this is heavily parallised and utilises the multiprocessing underlying MPI, hence items get serialised   
    '''
    
    def __init__(self,sourrcef,target,outf,geomop,nproc=4, preprocessfunc = None, dumpthread = DumpGeoJsonQueueThread, swapGeometry = False,test_only = False):
        '''
        sourrcef - the geometry collection to apply an operation on
        target - teh geometry collection to iterate over to apply to source
        outf - the output of the filtered geometries
        geomop - the operatin to apply, must be a valid attribute in csiro.geom.GeometryWrapper
        nproc - how many workers to start, these are processes working in a not necessarily shared memory
        preprocessfunc -  list of callables that take an array of geometries representing one process, 
            the calls either modify the first element or prepend another
            this allows for adding projected versions and buffers
            the worker will always use the first in the list to execute the geometry test according to the operation passed
        dumpthread - the listener class implementing a runnable to receive valid arrays of THE gemetry to store, by default its collected into a geojson FetureCollection
        swapGeometry - (default False), if True and source and target are of different projections, the output will be stored in the source projection
        test_only - int greater 0 as 'n' only process the first 'n' x 1000 entries
        '''
        if multiprocessing.cpu_count() < nproc or multiprocessing.cpu_count() -1 <= 0:
            raise ValueError("cannot do multiprocessing on single cpu machine or asking for too many cpu")
        
        threading.Thread.__init__(self)
        self.manager = multiprocessing.Manager()
        #self._inqueue = multiprocessing.Queue(10000)
        #self._outqueue = multiprocessing.Queue(10000)
        self._inqueue = self.manager.Queue(50000)
        self._outqueue = self.manager.Queue(10000)
        
        self.dumthread = dumpthread(self._outqueue,outf,swapGeometry=swapGeometry)
        self._target = target
        self._sourrcef = sourrcef
        # this is a Manager.Value - read only for the target processes
        self.do_run = self.manager.Value('i',1 ,lock=False)
        self.processpool = [_ProcessEvalGeom(str(i),sourrcef,geomop,self._inqueue,self._outqueue,cancelstate=self.do_run) for i in range(nproc)]
        self._outf = outf
        
        self.toSourceProj = None
        self.test_only= test_only
        self._preprocesFunc = [preprocessfunc]
        self._geomop = geomop
    
    def run(self):
        logger.logMessage("Parent Process Starting ")
        from pygeom.geom import Geometries, createTransferProj
        try:
            
            ## so we do all this in the run func to make sure we have a local init
            logger.logMessage("process {} starting ".format(self.id))
            print("process {} starting ".format(self.id))
            self.geomwapper = Geometries()
            Geometries.buildInit(self.geomwapper,self.laodf)
            
        
            sourcemeta = Geometries.collectMetadata(self._sourrcef)
            print("Parent loaded source")
            
            if not self._geomop in Geometries.operations:
                raise ValueError("GeometryWrapper has no func "+self._geomop)
            
            
            self.geomwapper = Geometries()
            Geometries.buildInit(self.geomwapper,self._target)
            
            
            if  'crs' in sourcemeta and str(sourcemeta['crs']) != str(self.geomwapper.CRS()):
                self.toSourceProj = createTransferProj(sourcemeta['crs'],self.geomwapper.CRS())
                self._preprocesFunc.insert(0,ApplyProj(self.toSourceProj))
                #if self._resultasProjected:
                #    self.dumthread.setInvProjection(souecgeom.createInverseProj(self.geomwapper.CRS()))
            
            
            for p in self.processpool:
                p.setApplies(self._preprocesFunc)
                p.start()
            
            self.dumthread.start()
            gcount = 0
            ### shifted projection and others to the workers by using an apply chain from above before starting the worker 
            for f in self.geomwapper.geoms():
                self._inqueue.put([f.geometry,f])
                
                gcount += 1
                if gcount % 1000 == 0:
                    logger.logMessage("Parent: committed n-elements: {}".format(gcount))
                    print("Parent: committed n-elements: {}".format(gcount))
                    if self.test_only and self.test_only < gcount/1000:
                        logger.logMessage("Parent ending prematurely on test {} with n-elements: {}".format(self.test_only,gcount))
                        print("Parent ending prematurely on test {} with n-elements: {}".format(self.test_only,gcount))
                        break
                if not self.do_run.value:# this is a Manager.Value
                    logger.logMessage("Parent ending with n-elements: {}".format(gcount))
                    print("Parent ending prematurely with n-elements: {}".format(gcount))
                    break
                
        except Exception as e:
            print(e)
            
        self.cancel()
        
        
    def cancel(self):
        import time
        #self.do_run = False
        
        logger.logMessage("Parent cancelling...")
        while not self._inqueue.empty():
            time.sleep(1.)
        logger.logMessage("Parent cancelling, ingueue empty...")
        time.sleep(2.) # this should cover the timeout of all the children, so at least double of the queue timeout  
        self.do_run.value = 0# this is a Manager.Value, the only place this should be changed
        #for p in self.processpool:
        #        p.cancel()
        while not self._outqueue.empty():
            time.sleep(1.)
        logger.logMessage("Parent cancelling, outgueue empty...")
        self.dumthread.cancel()
    
        
        
        
        
        
class FilterByAtrribute(threading.Thread):
    
    
    def __init__(self,sourrcef,outf,attrmap = {},dumpthread = DumpGeoJsonQueueThread, swapGeometry = False):
        threading.Thread.__init__(self)
        from queue import Queue
        
        
        self._outqueue = Queue(1000)
        self.geomwapper =  None
        self.dumthread = dumpthread(self._outqueue,outf,swapGeometry=swapGeometry)
        self._sourrcef = sourrcef
        self.attrmap = attrmap
        self.attrkeys = attrmap.keys()
        
        
    def run(self):
        import re
        from pygeom.geom import Geometries
        ## so we do all this in the run func to make sure we have a local init
        logger.logMessage("process {} starting ".format(self.attrkeys))
        print("process {} starting ".format(self.attrkeys))
        self.geomwapper = Geometries()
        Geometries.collectGeoms(self.geomwapper,self._sourrcef)
         
        #TODO   
        #### !!! Review this, geometries already supports this    


        def filterFuncAny(properties):
            for k in self.attrkeys:
                if k in properties and properties[k]:
                    try:
                        if  re.search(self.attrmap[k],str(properties[k])):
                            return True
                        
                    except Exception as e:
                        print("Testing {} against {} with value {} caused error".format(k,self.attrmap[k],properties[k]))
                    
            return False
        
        def filterFuncAll(properties):
            for k in self.attrkeys:
                if k in properties and properties[k]:
                    try:
                        if  not re.search(self.attrmap[k],str(properties[k])):
                            return False
                        
                    except Exception as e:
                        print("Testing {} against {} with value {} caused error".format(k,self.attrmap[k],properties[k]))
                else:
                    return False
            return True
        
        
        self.dumthread.start()
        
        filtered = self.geomwapper.filterByAttributeFunc(filterFuncAny)
        for f in filtered:
            self._outqueue.put([f.feature])
        
        logger.logMessage("filter process found matches {}".format(len(filtered)))
        print("filter process found matches {}".format(len(filtered)))
        self.dumthread.cancel()
        
        
        '''
        
        gcount = 0
        ### shifted projection and others to the workers by using an apply chain from above before starting the worker 
        for f in self.geomwapper.geoms():
            for k in self.attrkeys:
                if k in f.properties and f.properties[k]:
                    try:
                        if  re.search(self.attrmap[k],str(f.properties[k])):
                            self._outqueue.put([f])
                            gcount += 1
                    except Exception as e:
                        print("Testing {} against {} with value {} caused error".format(k,self.attrmap[k],f.properties[k]))
                    
        logger.logMessage("filter process found matches {}".format(gcount))
        print("filter process found matches {}".format(gcount))
        self.dumthread.cancel()
                
        '''
                
                
                
        