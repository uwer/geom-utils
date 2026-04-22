

from pygeom import findPathlibWCparent
import pathlib


def test():
    from pathlib import Path
    
    print(findPathlibWCparent(Path("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2024/*/calc_data.csv")))
    
    print(findPathlibWCparent(Path("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output202*/*/calc_data.csv")))
    print(findPathlibWCparent(Path("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2025/503160860-274b4b/*.csv")))
    print(findPathlibWCparent(Path("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2025/5*-*/calc_data.csv")))
    print(findPathlibWCparent(Path("/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2025/503160860-274b4b/calc_data.csv")))
    
    
test()