import asyncio, sys,os,json, time
from . import logme



from abc import ABC, abstractmethod
from typing import List

class APIClient(ABC):
    
    @abstractmethod
    def getStatus(self,pid):
        pass
    
    @abstractmethod
    def getProcessResults(self, pid):
        pass
    
    
async def issueRequest(call, key,args,kwargs):
    # a call that only returns once its finished
    rss = call(*args,**kwargs)
    
    return key,rss



async def _issueAPIRequest(client,call, key, args, kwargs):
    # this execution itself issues an async call on the other side,
    # hence we test if its finished and keep waiting
    rss = call(*args,**kwargs)
    if "process" in rss:
        pid = rss["process"]
        
        res = client.getStatus(pid)
        while res["running"]:
            await asyncio.sleep(2.)
            res = client.getStatus(pid)
            
        logme(f"done waiting for completion {key} - process success {res}")
        
        rss = client.getProcessResults(pid)
    if not rss is None:
        logme(f"done result keys {key}:    {rss.keys()}")
    else:
        logme(f"done result key {key}:  failed")
    return key,rss  
        

def nowTS():
    return time.perf_counter()

async def waitForRequests(tasks, now1 = nowTS()):
    
    await asyncio.gather(*tasks)
    now3 = nowTS()
    results = {}
    failed = {}
    for t in tasks:
        try:
            k,res = t.result()
            if res is None:
                failed[k] = None
            else:
                results[k] = res
        except Exception as e:
            failed[k] = e
    now4 = nowTS()
            
    print("times  gather {} collect {} ".format(now3-now1,now4-now3))
    
    return  results,failed
            
def createTask(call,key,args,kwargs = {}, loop= None):
    if loop:
        return loop.create_task(issueRequest(call,key,args,kwargs))
    return asyncio.create_task(issueRequest(call,key,args,kwargs))


def createAPITask(client,call,key,args,kwargs = {}, loop= None):
    if loop:
        return loop.create_task(_issueAPIRequest(client,call,key,args,kwargs))
    return asyncio.create_task(_issueAPIRequest(client,call,key,args,kwargs))
    
    
    
def runTasks(tasklist):
    loop = asyncio.new_event_loop()
    
    asyncio.set_event_loop(loop)
    res,failed = loop.run_until_complete(waitForRequests(tasklist))

    loop.close()
                    
    return res,failed
    
    
    
    