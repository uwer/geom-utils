
from datetime import datetime as dt

from threading import Timer
from pygeom import *

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
    ss = os.path.splitext(filename)
    if not suffix in ss[1]:
        if suffix[0] == '.':
            return  filename+suffix
        elif filename[-1] == '.':
            return  filename+suffix
        return  filename+'.'+suffix
    return filename  
    
