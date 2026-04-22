from scan import combineDFscan


def test_hmm():

    combineDFscan(geompath="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_centre_frdc.sub.rect.exact2.fgb",
                  dfpaths="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2024",
                  columnsDefs=["Lon","Lat","MsgTime"],
                  wildcard="**/calc_data.csv",
                  filterq= " `hmm-activity` == 'fishing' ",#`score-activity` == 'fishing' and
                  outpath="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/scan/ais/s2024hmm.4",
                  options={"matchid":"grid_id","save-source":True})
    
    
    
def test_clst():

    combineDFscan(geompath="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/grid_centre_frdc.sub.rect.exact2.fgb",
                  dfpaths="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/output2024",
                  columnsDefs=["Lon","Lat","MsgTime"],
                  wildcard="**/calc_data.csv",
                  filterq= " `clst-activity` == 'fishing' ",#`score-activity` == 'fishing' and
                  outpath="/Users/ros260/projects/data/AIS_VMS/AIS/nsw/frdc/scan/ais/s2024hmm.clst.1",
                  options={"matchid":"grid_id","save-source":True})
    
test_clst()