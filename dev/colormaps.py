import numpy as np


def createMonotonicCMAP1(colors = ["blue","white", "red"], N=24, name=""):
    import matplotlib.colors as mcolors
    
    cmap = mcolors.LinearSegmentedColormap.from_list(name, colors,N=N)
    #print(json.dumps(cmap))
    return cmap

def createMonotonicCMAP2(colors = ["blue","white", "red"], N=24, name=""):
    
    from pypalettes import create_cmap,show_cmap
    import matplotlib
    cmap = create_cmap(
    #colors=["#D57A6DFF", "#E8B762FF", "#9CCDDFFF", "#525052FF"],
    colors =colors,
    cmap_type="continuous",
    )
    '''
    print(cmap)
    for i in range(cmap.N):
        rgba = cmap(i)
        # rgb2hex accepts rgb or rgba
        print(matplotlib.colors.rgb2hex(rgba))
    '''
    return cmap

def createStyleDef(cmap,values , opacity = 1.0):
    import matplotlib
    template ='<ColorMapEntry quantity="{}" color="{}" label="{}" opacity="{}" />'
    
    
    if len(values) != cmap.N:
        values = np.linspace(min(values),max(values),num=cmap.N)
        
        
    for i , v in enumerate(values):
        rgba = cmap(i)
        # rgb2hex accepts rgb or rgba
    
        print(template.format(v,matplotlib.colors.rgb2hex(rgba),v,opacity))
    
    
    


#cmap = createMonotonicCMAP1(N=11)
#createStyleDef(cmap,[-3.0,3.0],opacity = 0.8)



print("fbiom")
cmap = createMonotonicCMAP1(colors = ["#3e356b","white", "#def5e5"],N=16)
createStyleDef(cmap,[-.7,0.7],opacity = 0.8)


print("02")
cmap = createMonotonicCMAP1(colors = ["#fb7e21","white", "#0941eb"],N=9)
createStyleDef(cmap,[-.4,0.4],opacity = 0.8)


print("pp")
cmap = createMonotonicCMAP1(colors = ["blue","white", "red"],N=19)
createStyleDef(cmap,[-18.,18],opacity = 0.8)


print("temp")
cmap = createMonotonicCMAP1(colors = ["blue","white", "red"],N=9)
createStyleDef(cmap,[-2.,2.],opacity = 0.8)


