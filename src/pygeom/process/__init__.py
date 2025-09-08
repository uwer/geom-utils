


from pygeom import logme, stopwatch

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
        self._logfunc(f'{LogHandler.EPROGRESS} Progress: {round(self._progress,3)}')
        
    def __init__(self, name = "Log",logfunc = logme):
        self._progress = 0.
        self._logfunc = logfunc
        self._name = name
        self.logMessage(f"Created handler {self._name}", "Init", LogHandler.Tooling)
         
    def isCanceled(self):
        return False
    
    def setProgress(self,p):
        self._progress = p
        self._report()
        
    def increment(self, progressIncrement: float):
        self._progress += progressIncrement
        self._report()
    
    def cancel(self):
        pass
    
    
    def progress(self) -> float:
        return self._progress
    
    
    def logMessage(self,msg,msg_context="Message",msg_type=Info):
        self._logfunc(f'{self._name}-{self.emoji4Code(msg_type)} {msg_context} - {msg}')
        
        
        