


from pygeom import logme, stopwatch
from typing import overload
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