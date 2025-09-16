
from datetime import datetime as dt

from threading import Timer
from pygeom import (
    logme,NM2KM,
    DAYS2SEC,HOUR2SEC, 
    stopwatch
    )

class RepeatedTimer(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer     = None
        self.interval   = interval
        self.function   = function
        self.args       = args
        self.kwargs     = kwargs
        self._countdown = None
        self.is_running = False
        self.start()
        

    def _run(self):
        self.is_running = False
        self.start()
        self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.is_running:
            
            self._timer = Timer(self.interval, self._run)
            self._countdown = dt.utcnow().timestamp()
            self._timer.start()
            self.is_running = True

    def stop(self):
        self._timer.cancel()
        self.is_running = False
        
        
    def countdown(self):
        if self._countdown:
            return int(abs(self.interval - dt.utcnow().timestamp() + self._countdown ))
        return 0.
    
    


import threading
import ctypes
#import time

##  force killable Thread
class KillThread(threading.Thread):
    def __init__(self, func, *args, ** kwargs):
        threading.Thread.__init__(self)  
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self.exception = None
        self.result = None
        self.success = False
        
    def run(self):
        try:
            self.result = self._func(*self._args,**self._kwargs)
            self.success = True
                 
        except Exception as e:
            self.exception = e
              
    def running(self):
        return not self.success and not self.exception
                    
    def get_id(self):
        if hasattr(self, '_thread_id'):
            return self._thread_id
        return threading.get_ident()
        '''
        for id, thread in threading._active.items():
            if thread is self:
                return id
        '''
    def raise_exception(self):
        thread_id = self.get_id()
        resu = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id,
              ctypes.py_object(Exception))
        if resu > 1: 
            ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0)
            print('Failure in raising exception')     
            
            
    def cancel(self,*args):
        self.raise_exception()

def exit_after(s,quit_function):
    '''
    use as decorator to exit process if 
    function takes longer than s seconds
    '''
    def outer(fn):
        def inner(*args, **kwargs):
            kt = KillThread(fn,*args, **kwargs)
            timer = threading.Timer(s, kt.cancel)
            timer.start()
            
            try:
                kt.start()
                kt.join()
                    
                #result = fn(*args, **kwargs)
            except Exception as e:
                print (e)
            finally:
                timer.cancel()
                
            if kt.exception:
                raise kt.exception
            return kt.result
        return inner
    return outer

def timeout(s,fn,quit_function,*args, **kwargs):
    '''
    use as function to exit process if 
    function takes longer than s seconds
    '''
    #print("entered timeout {} for {} sec".format(fn.__name__,s))
    
    @exit_after(s,quit_function)
    def fcall(fn,*args,**kwargs):
        return fn(*args,**kwargs)
    
    return fcall(fn,*args,**kwargs)

    
def isValid(strval):
    if strval is None:
        return False
    return len(str(strval).strip()) > 0

isValidStr = isValid


def is_float(obj):
    try:
        float(obj)
        return True
    except:
        pass
    return False
    
    
    
def isTrue(strval):
    if isinstance(strval,bool):
        return strval
    return  "1" in strval or "t" in strval.lower()


def roundList(l,rd):
    return [round(x,rd) for x in l]

def factor4unit(unit):
    # returns scale factors to turn any unit value into base units of seconds and metres
    if not unit or len(unit) == 0:
        return 1
    
    u = unit.lower()
    if u == 'km':
        return 1000.
    if u == 'kmh' or u == 'km/h':
        return 1/3.6
    elif u == 'nm':
        return NM2KM * 1000.
    elif 'hr' in u or 'hour' in u:
        return HOUR2SEC
    elif 'day' in u:
        return DAYS2SEC
    elif 'min' in u:
        return 60.
    
    return 1.


def scale_unit(val,unit):
    if is_float(val):
        factor = factor4unit(unit)
        return val * factor
    return val


'''
not available on WIN
#import gc
import resource

def mem():
    print('Memory usage         : % 2.2f MB' % round(
        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024.0/1024.0,1)
    )
'''

### minimum of the dependencies from csiro_pylib copied here

def createTMPDir():
    import os, uuid
    td = os.curdir
    print("createTmp dir: "+td+" "+str(os is None)+ " " + str(os.environ is None))
    if not os.environ is None:
        try:
            if 'TEMP' in os.environ:
                td = os.getenv('TEMP',None)
                if td is None:
                    td = os.getenv('TMPDIR',os.curdir)
            elif 'TMPDIR' in os.environ:
                td = os.getenv('TMPDIR',os.curdir)
            else:
                td = os.path.abspath(td)
                
        except Exception as e:
            print(e)
            
    uniq = str(uuid.uuid4())
    return os.path.join(td,uniq[:12])
    
    
def ensureSuffix(filename, suffix):
    import os
    ss = list(os.path.splitext(filename))
    if not suffix in ss[1]:
        if "." in ss[1]:
            filename = filename.rsplit(".",1)[0]
            
        if suffix[0] == '.':
            return  filename+suffix
        elif filename[-1] == '.':
            return  filename+suffix
        return  filename+'.'+suffix
    return filename  
    
    
from abc import ABC, abstractmethod
from typing import List

class FeaturesStore(ABC):
    
    @abstractmethod
    def isValid(self) -> bool:
        pass
    
    @abstractmethod
    def name(self) -> str:
        pass
    
    @abstractmethod
    def close(self):
        # release all resources and close this
        pass
    
    @abstractmethod
    def clone(self) -> List:
        # create clone of this source store, they must not share any resources
        pass
    
    
    @abstractmethod
    def setName(self,name: str):
        pass
    
    
    @abstractmethod
    def initIndex(self):
        # initialise the store and an index
        pass
    
    @abstractmethod
    def intersections(self, geom0) -> List:
        """
        return a list of feature wrappers that are intersecting with the geometry provided
        geom0 - the shapely geometry to test (not the feature)
        return a list of features that intersect with geom0
        """
        pass  
    
    @abstractmethod
    def features(self) -> List:
        # a list of all features as dict
        pass
        
    @abstractmethod
    def __iter__(self):
        # a iterator of the store objects 
        pass
          
    
class CacheManager(ABC):
    
    @abstractmethod
    def attrMap(self, key, default = None) -> object: 
        pass
    @abstractmethod
    def setAttrMap(self,key,val):
        pass
    
    @abstractmethod
    def attrKeys(self):
        '''
        Return the available keys in the properties
        '''
        pass
    
    @abstractmethod
    def subsetAttr(self, keys: List):
        '''
        Return a subst of the properties according to the keys
        '''
        pass
    
    @abstractmethod
    def clone(self) -> List:
        # retrieve a clone of this resources form the pool or make a new one within the resource limits assigned
        # return a list with the number of source stores assigned
        pass
    
    @abstractmethod
    def name(self) -> str:
        pass
    
    @abstractmethod
    def releaseClone(self,l):
        # return clone back to pool, removing it from being locked
        pass
        
    @abstractmethod
    def nclones(self) -> int:
        # number of clones currently present
        pass
    
    @abstractmethod
    def nlocked(self) -> int:
        # number of clones currently locked
        pass
    
    @abstractmethod
    def release(self):
        # release all resources/clones
        pass
    
    @abstractmethod
    def close(self):
        # release all resources/clones and close this manager including the source of the clones
        pass

class FeaturesCacheManager(CacheManager):
    
    def __init__(self,featurecontainer, initCache = 1,cancelHandler=None,inmemory = True, attrmap= {"name":"name","id":"id"}):
        self._corefeatures = featurecontainer
        self._cacheStore = list()
        self._cacheLock = list()
        self._display_layer = None

        self.attrmap = attrmap
        with stopwatch(f"Cloning {self._corefeatures.name()} Feature sets {initCache} "):
            while initCache > 0:
                
                if not cancelHandler is None and cancelHandler.isCanceled():
                    return 
                if inmemory:
                    logme(f"Loading in-memory clone for layer {self._corefeatures.name()}")
                    #print("loading in memory")
                    l  = self.__createClone(str(initCache))
                else:
                    l = self.__loadAgain(str(initCache))
                    
                if not l.isValid():
                    logme(f"Failed Loading Geometry, clone produced invalid layer {self._corelayer.name()}")
                    return
                self._cacheStore.append(l)
                
                initCache -= 1

        
    def attrMap(self,key, default = None):
        return self.attrmap.get(key,default)
    
    def setAttrMap(self,key,val):
        self.attrmap[key] = val
        
        
    def attrKeys(self):
        return list(self.attrmap.keys())
    
    
    def subsetAttr(self, keys: List):
        return {k:self.attrmap[k] for k in keys if k in  self.attrmap}
    
    def name(self):
        return self._corefeatures.name()
        
    
    def __createClone(self,n_suffix):
        l = self._corefeatures.clone()
        l.setName(f"{self._corefeatures.name()}-{str(n_suffix)}")
        l.initIndex()
        return l
        
        
    def __loadAgain(self,n_suffix):
        # until there are alternative options we clone
        return self.__createClone(n_suffix)

    
    
    
    def clone(self):
        if len(self._cacheLock) == len(self._cacheStore):
            '''
            if qver[1] < 8:
                import processing
            else:
                from qgis import processing
            self._corelayer.selectAll()
            
            l = processing.run("native:saveselectedfeatures", {'INPUT': self._corelayer, 'OUTPUT': 'memory:'})['OUTPUT']
            l.setName(self._corelayer.name()+str(len(self._cacheStore)))
            #l = self.createMemoryLayer(self._corelayer.name()+str(len(self._cacheStore)),self._corelayer)
            '''
            l  = self.__createClone(str(len(self._cacheStore)))
            self._cacheStore.append(l)
            self._cacheLock.append(l)
            return [l]
        else:
            #print("caches {} ".format(self._cacheLock))
            for l in self._cacheStore:
                if not l in self._cacheLock:
                    self._cacheLock.append(l)
                    return [l]
        
        # missed our chance
        l  = self.__createClone(str(len(self._cacheStore)))
        self._cacheStore.append(l)
        self._cacheLock.append(l)
        return [l]
        
    def releaseClone(self,l):
        if not l in self._cacheStore:
            raise ValueError("This layer is not managed by this CacheManager")
        
        #print("release clone {}".format(l.name()))
        if l in self._cacheLock:
            self._cacheLock.remove(l)
            
            
    
    def nclones(self):
        return len(self._cacheStore)
    
    def nlocked(self):
        return len(self._cacheLock)
    
    def release(self):
        for l in  self._cacheLock:
            self._cacheLock.remove(l)
            self._cacheStore.remove(l)
            l.close()
        
    def close(self):
        self.release() 
        if hasattr(self._corefeatures,"release"):
            self._corefeatures.release()
        del self._corefeatures
        self._corefeatures = None
    
