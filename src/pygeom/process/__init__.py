


from pygeom import logme, stopwatch
from typing import overload
import time, os
from pygeom import isTrue

ASYNC_NEWLOOP=isTrue(os.getenv('ASYNC_NEWLOOP',True))


class LogHandler():
    
    Info = 6
    Warning = 3
    Error  = 2
    Time  = 1
    Connect = 5
    Lock = 7
    Tooling = 8
    Debug = 4
    Critical = 10
    Success = 0 
    
    
    
    ETIME = '🕐'
    ECANE = '🧑‍🦯‍➡️'
    EBULB = '💡'
    ENOTE = "📄"
    EPROGRESS =  '📈'
    EPIN = '📌'
    ELOCK = '🔐'
    ECROSS = '❌'
    ECONNECT = '🔌'
    EGREENTICK = '✅'
    EATTENTION = '⚠️'
    ENOTEFY = 'ℹ️'
    EMIND = '🧠'
    EERROR = '🚫'
    EFLAME = '🔥' 
    ETOOLS = '🛠️'
    ERECYCLE = '♻'
    EFLASH= '⚡'
    ERIBBON= '🎗'
    EGSTAR = '✳'
    EEYS= '👀'
    EGEAR = '⚙'
    ESHIELD = '🛡'
    
    
    
    def emoji4Code(self,code ):
        if code == LogHandler.Info:
            return LogHandler.EBULB
        elif code == LogHandler.Warning:
            return LogHandler.EATTENTION
        elif code == LogHandler.Error:
            return LogHandler.EERROR
        elif code == LogHandler.Connect:
            return LogHandler.ECONNECT
        elif code == LogHandler.Lock:
            return LogHandler.ELOCK
        elif code == LogHandler.Tooling:
            return LogHandler.EGEAR
        elif code == LogHandler.Time:
            return LogHandler.ETIME
        elif code == LogHandler.Debug:
            return LogHandler.ETOOLS
        elif code == LogHandler.Critical:
            return LogHandler.EFLAME
        elif code == LogHandler.Success:
            return LogHandler.ERIBBON
        return LogHandler.ENOTE
    
    
    def _report(self):
        self._logfunc(f'{self.__name}-{LogHandler.EPROGRESS} Progress: {round(self._progress,1)}')
        
    def __init__(self, name = "Log",logfunc = logme):
        self._progress = 0.
        self._logfunc = logfunc
        self.__name = name
        self._logfunc(f'{self.__name}-{LogHandler.ESHIELD} "Init" - Created handler {self.__name}')
        #self._doLogMessage(f"Created handler {self.__name}", "Init", LogHandler.Tooling)
         
    def isCanceled(self):
        return False
    
    def setProgress(self,p):
        if round(p,1) != round(self._progress,1):
            self._progress = p
            self._report()
        else:    
            self._progress = p
        
        
        
    def increment(self, progressIncrement: float):
        p = self._progress + progressIncrement
        self.setProgress(p)
    
    def cancel(self):
        pass
    
    def id(self):
        return self.__name
    
    def progress(self) -> float:
        return self._progress
    
    def _doLogMessage(self,msg,msg_context="Message",msg_type=Info):
        self._logfunc(f'{self.__name}-{self.emoji4Code(msg_type)} {msg_context} - {msg}')
    
    @overload
    def logMessage(self,msg,msg_context="Message"):
        self._doLogMessage(msg,msg_context,LogHandler.Info)
        
        
    @overload
    def logMessage(self,msg,msg_type=Info):
        self._doLogMessage(msg,msg_context="Message",msg_type=msg_type)
        
        
    def logMessage(self,msg,msg_context="Message",msg_type=Info):
        self._doLogMessage(msg,msg_context,msg_type)    
        
        
class LoggingDelegate(LogHandler):
    
    
    def __init__(self,contextpath):
        # ensure order
        import logging
        self.logger = logging.Logger(contextpath)
        LogHandler.__init__(self,contextpath,self.__message)
        
        
    def __message(self,msg):
        self._doLogMessage(msg)
        
    def _doLogMessage(self,msg,msg_context="Message",msg_type=LogHandler.Info):
        # remove match statement to meet py3.9 constraints
        if msg_type == LogHandler.Debug:
            self.logger.debug(f'{self.emoji4Code(msg_type)} {msg_context} - {msg}')
        elif msg_type ==  LogHandler.Warning:
            self.logger.warn(f'{self.emoji4Code(msg_type)} {msg_context} - {msg}')
        elif msg_type ==  LogHandler.Critical:
            self.logger.critical(f'{self.emoji4Code(msg_type)} {msg_context} - {msg}') 
        elif msg_type == LogHandler.Error:
            self.logger.error(f'{self.emoji4Code(msg_type)} {msg_context} - {msg}') 
        else:
            self.logger.info(f'{self.emoji4Code(msg_type)} {msg_context} - {msg}')
                        
        
     
from threading import Thread
class QueueManager(Thread):
    '''
    handle the parallel running of n jobs as a thread
    the controlled object only needs to implement 3 functions:
    start, isEnded, isSuccessful - latter 2 returning boolean
    
    
    '''
    
    def __init__(self, max_queue= 6):
        Thread.__init__(self)
        self.running = {}
        self.__max_nqueue = max_queue
        self.__testFinished = False
        self._processStore = {}
        from queue import Queue
        self.waiting = Queue()
        self.finished = {}
        self._setcancel = False
        
    def add(self, pid, execref):
        '''
        Add a job to be executed
        '''
        self._processStore[pid] = execref
        self.__start(pid)
        
    ## load management
    def nrunning(self):
        return len(self.running)
    
    def maxruning(self):
        return self.nrunning() >= self.__max_nqueue
    
    def nwaiting(self):
        return self.waiting.qsize()
    
    def isrunning(self):
        return len(self.finished) < len(self._processStore)
    
    def _testStatusRunning(self):
        '''
        test for job completion and start a new one 
        '''
        
        if self.__testFinished:
            return
        
        self.__testFinished = True
        try:
            deletepids = []
            for pid in self.running:
                ended = self._processStore[pid].isEnded()
                if ended:
                    deletepids.append(pid)
                    self.running[pid]['finished'] =True
                
            for pid in deletepids:
                self.finished[pid] = self.running[pid]
                del self.running[pid]
                self.finished[pid]['failed'] = not self._processStore[pid].isSuccessful()
                
            while not self.maxruning() and not self.waiting.empty():
                self._popWaiting()
                
        except Exception as e:
            print(e)
        finally:
            self.__testFinished = False
        
    def __start(self, pid):
        
        if self.maxruning():
            self.waiting.put(pid)
            return True
        
        self._processStore[pid].start()
        self.running[pid] = {'finished':False,"failed":False}
        
    
    def _popWaiting(self):
        if not self.waiting.empty():
            fif = self.waiting.get()
            self.__start(fif)
        
    def cancel(self):
        '''
        Indicate the jobs to be cancelled, it will not take place immediately 
        '''
        self._setcancel = True
        
    def run(self):
        '''
        Execute the designated number of executions to process the queue and wait for completion
        Monitor the progress of the execution with the management functions i.e. 
        'isrunning' ...
        
        '''
        with stopwatch("running queued tasks "):
            while not self._setcancel and self.isrunning():
                time.sleep(5)
                self._testStatusRunning()
        
        

class QueueManagerAsync():
    '''
    handle the parallel running of n jobs in asyncio
    the controlled object only needs to implement 3 functions:
    start, isEnded, isSuccessful - latter 2 returning boolean

    '''
    
    def __init__(self, max_queue= 6):
        import asyncio
        self.added = {}
        self.running = {}
        self.waiting  = None
        # Start 3 cashiers
        self.__max_nqueue = max_queue
        
        self.__testFinished = False
        self._processStore = {}
        
        self.finished = {}
        self._setcancel = False
        
       
    def cancel(self):
        '''
        Indicate the jobs to be cancelled, it will not take place immediately 
        '''
        self._setcancel = True
        
    async def _work(self,queue, _work_id):
        '''
        Worker implementation, retrieve a PID from the queue and execute its associated executable.
        we admit task done straight away for other workers to pick up a PID. 
        The 'executable ' only needs three functions to be used here
        'start', 'isEnded', 'isSuccessful'
        '''
        import asyncio
        while not self._setcancel:
            jobid = await queue.get()
            
            
            logme(f"job {_work_id} is starting ")
            self.running[jobid] = {'finished':False,"failed":False}
            job = self._processStore[jobid]
            job.start()
            await asyncio.sleep(0.5) 
            while not job.isEnded() :
                '''
                if asyncio.current_task().cancelling():
                    break
                '''
                await asyncio.sleep(2) 
                if self._setcancel:
                    job.cancel()
                    break
                
            logme(f"job {_work_id} is finished {job.isSuccessful()}")
            self.running[jobid]['finished'] = True
            self.finished[jobid] = self.running[jobid]
            self.finished[jobid]['failed'] = not job.isSuccessful()
            
            await asyncio.sleep(0.1)  
            del self.running[jobid]
            # we release the queue, so another can pick up work
            queue.task_done()
            
        
    def add(self, pid, execref):
        '''
        Add a job to be executed
        '''
        
        self._processStore[pid] = execref
        if not self.waiting is None:
            self.waiting.put(pid)
        
    
    ## load management
    def nrunning(self):
        return len(self.running)   
    
    def nwaiting(self):
        return self.waiting.qsize()
    
    def isrunning(self):
        return len(self.finished) < len(self._processStore)
    
    async def run(self):
        '''
        Execute the designated number of workers to process the queue and wait for completion
        
        '''
        with stopwatch("running queued async tasks "):
            import asyncio
            tasks = []
            self.waiting = asyncio.Queue()
            
            for i in range(self.__max_nqueue):
                tasks.append(asyncio.create_task(self._work(self.waiting , i + 1)))
                
            for pid in self._processStore:
                await self.waiting.put(pid)
                
            await self.waiting.join()
            
            # start the shutdown
            self.cancel()
            for t in tasks:
                t.cancel()
                
    def start(self):
        '''
        Start processing the queue of jobs 
        It will execute the management of the jobs in a thread and as such this call will return 
        Monitor the progress of the execution with the management functions i.e. 
        'isrunning' ...
        
        '''
        
        logme(f"starting queue manager have n {len(self._processStore)} jobs waiting")
        import asyncio
        _loop = asyncio.new_event_loop()
        Thread(target=_loop.run_forever, daemon=True).start()
        try:
            _loop.call_soon_threadsafe(asyncio.create_task, self.run())
        
        except Exception as e:
            logme(e)
        
                    