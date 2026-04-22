from pylab import *
from numpy import *

from mpl_toolkits.mplot3d.art3d import Poly3DCollection
from scipy.interpolate import RectBivariateSpline as RBS
from scipy.interpolate import interpn
from skimage import measure

def makeMesh(xt,yt,zt,ct,isoSurface):
    '''
    for a ROMS data in the order the data is returned when reading a netCDF file, so (z,y,x)

    xt shape (ny,nx)     : x locations of data (e.g. x_rho)
    yt shape (ny,nx)     : y locations of data (e.g. y_rho)
    zt shape (nz,ny,nx)  : z locations of data (e.g. z_r)
    ct shape (nz,ny,nx)  : data
    isoSurface scaler    : isosurface to return

    returns verts,faces, normals, values as returned by skimage.measure.marching_cubes_lewiner
    
    For documentation of the returned feilds, and how to plot this output, see 
    https://scikit-image.org/docs/dev/api/skimage.measure.html#skimage.measure.marching_cubes_lewiner
    '''

    #make isosurface on rectangular grid
    verts, faces, normals, values = measure.marching_cubes_lewiner(ct, level=isoSurface)

    #ok, now alter locations in verts to make onto physical space. The
    #order of the indices is as in the arrays, so (z,y,x)
    oldVerts=verts+0.0
    xinterp=RBS(arange(xt.shape[0]),arange(xt.shape[1]),xt,kx=1,ky=1)
    verts[:,2]=xinterp(oldVerts[:,1],oldVerts[:,2],grid=False)
    yinterp=RBS(arange(yt.shape[0]),arange(yt.shape[1]),yt,kx=1,ky=1)
    verts[:,1]=yinterp(oldVerts[:,1],oldVerts[:,2],grid=False)
    #print('End re-gridding in 2D')
    
    verts[:,0]=interpn((arange(zt.shape[0]),arange(zt.shape[1]),arange(zt.shape[2])),zt,
                       oldVerts,method='linear',bounds_error=False,fill_value=None)
    #print('End re-gridding in 3D')

    return verts,faces,normals,values


if __name__=='__main__':
    
    import visvis as vv 

    #this section gives two examples. The first makes an oblate
    #spheroid, and changes the aspect ratio so it is a sphere
    #again. The second reads in some ROMS output and makes an
    #isosurface of a temperature surface, and shades it by the depth
    #of the surface.

    if True: #plot the sphere
        print('Plotting an oblate spheroid as a sphere, and changing')
        print('the aspect ratio so it looks spherical again.')

        #make domain grids. NOTE WELL: these are setup so that z varies in
        #first dimension, y in second, and x in the third dimension, so
        #that it mimics ROMS arrays which are ordered in python as (k,j,i)
        xvec=linspace(-500.0,500.0,300)
        zvec=linspace(-30.0,30.0,300)
        ym,zm,xm=meshgrid(xvec,zvec,xvec)

        #make data to get isosurface of -- it is distance from 0,0 in z,
        #and is stretched by 20 times in the x and y direction
        psi=sqrt((xm/20)**2+(ym/20)**2+zm**2)

        #define the aspect ration we want the data to have. This is the
        #amount to stretch each axis. This is choosen so that the image
        #looks like a sphere... and the vector is ordered (x,y,z)
        plotAspect=(1/20.0,1/20.0,1.0)


        #NOTE if you want to visualize this in mayavi2 look at comments in
        #https://scikit-image.org/docs/dev/api/skimage.measure.html#skimage.measure.marching_cubes_lewiner

        #make isosurface. Remember that makeMesh takes x and y as 2D
        #arrays, and z as a 3d array because that how ROMS does things.
        verts,faces,normals,values=makeMesh(xm[1,:,:],ym[1,:,:],zm,psi,20.0)
        a1=vv.subplot(111)
        vv.title('sphere, color indicates depth')
        vv.xlabel('x label')
        vv.ylabel('y label')
        vv.zlabel('z label')
        a1.daspect=plotAspect

        #the input for values sets the colors of the surface. By
        #setting it to verts[:,0], we are setting the color at the
        #vertices to the vertical position of the vertex -- so the
        #color represents the depth of the surface.
        jnk=vv.mesh(np.fliplr(verts), faces=faces,values=verts[:,0])
        jnk.colormap=vv.CM_JET
        vv.colorbar()
        vv.use().Run() 
    else:

        print('plot a temperature surface from a ROMS model run. Uses parts')
        print('of the pyroms toolkit to compute the vertical grid.')

        #import pyroms and netCDF4, and set up the model data
        import pyroms as p
        import netCDF4 as nc
        import os 
        dataRoot='~/workfiles/dPdy/slopeForceRoms/'
        histFile=os.path.expanduser(dataRoot+'slopeSpindown02_d2_vertMod/ocean_his.nc')

        #make roms grid object
        romsGrid=p.grid.get_ROMS_grid('jnks',hist_file=histFile,grid_file=histFile)

        #open data
        data=nc.Dataset(histFile,'r')

        #what time to plot
        nt=data['ocean_time'][:].shape[0]-1

        #make vertical grid
        h=data['h'][:]
        theta_b=data['theta_b'][:]
        theta_s=data['theta_s'][:]
        Tcline=data['Tcline'][:]
        Vstretching=data['Vstretching'][0]
        Vtransform=data['Vtransform'][0]
        N=len(data['s_rho'][:])

        #calculate vertical grid
        #ASSUMING Vtransform=2 and Vstretching=4!!!
        assert Vtransform==2, 'wrong Vtransform for calculation of s'
        assert Vstretching==4,'wrong Vstetching for calculation of s'
        s=p.vgrid.s_coordinate_4(h,theta_b,theta_s,Tcline,N)

        #and get vertical positions of w and rho points everywhere.
        z_r=s.z_r[0,:,:]
        z_w=s.z_w[0,:,:]

        #get horizontal locations of temperature grid
        x_rho=data['x_rho'][:]
        y_rho=data['y_rho'][:]

        #plot isosurface. NOTE WELL, you will see below that I scale
        #x_rho and y_rho to kilometers. The ocean is very thin; if we
        #plot the data in its actual aspect ratio, it would be too
        #thin to see.
        xt=x_rho/1e3
        yt=y_rho/1e3
        zt=z_r
        ct=data['temp'][nt,:,:,:]
        
        #If you want to plot only part of the data, this is a good
        #place to subset the data
        #clip regions to plot
        kvec=arange(ct.shape[0])
        jvec=arange(ct.shape[1]) #if for example, you wanted plot from ny=1..200, you could put arange[1,200]
        ivec=arange(150,ct.shape[2]) #plot from i=150 to maximum i

        #clipping to a depth is a tricky, since there is no one to one
        #mapping everywhere between depth and vertical index. So we
        #clip the data to a value far from the isosurface below the
        #depth zMax...
        zMax=-500.0
        ct[zt<zMax]=-9999.0

        #now subset the data
        xt=xt[ix_(jvec,ivec)]
        yt=yt[ix_(jvec,ivec)]
        zt=zt[ix_(kvec,jvec,ivec)]
        ct=ct[ix_(kvec,jvec,ivec)]

        #now make iso-surface at temperature isoVal
        isoVal=-2.0
        titleStr='temperature surface at T=%4.2f'%(isoVal,)
        verts,faces,normals,values=makeMesh(xt,yt,zt,ct,isoVal)

        #now make an iso-surface near the bottom by making a data set
        #that 10 at the bottom of the data set plotted, and then
        #showing the 9.9999 contour. Note I use z_w, because the w
        #surface forms the actual bottom. This means the iso-surface
        #CANNOT reach the bottom. Note Well, if you are plotting an
        #isosurface of something not on the rho grid, you will have to
        #adjust this code by providing a different xt and yt
        ctBot=z_w*0.0
        ctBot[0,:,:]=10.0
        ctBot[z_w<zMax]=0.0
        ctBot=ctBot[ix_(kvec,jvec,ivec)]
        ztBot=z_w[ix_(kvec,jvec,ivec)]
        vertsBot,facesBot,normalsBot,valuesBot=makeMesh(xt,yt,ztBot,ctBot,9.9999)

        #IMPORTANT -- OCEANIC DATA HAS EXTREME ASPECT RATIOS!  So for
        #the visualization to be any good, we need to plot the data
        #asymetrically. Above, I have already changed the aspect ratio
        #by making xt and yt into kilometers... but it is often useful
        #to distort it even more. The variable dataAspect is organized
        #as (x,y,z) (unlike the data!), and is the how much each axes
        #should be distorted. Values > 1 stretch, <1 shrink. 
        dataAspect=(1.0,1.0,1.0/5.0)
        
        #now show the isosurfaces
        a1=vv.subplot(111)
        vv.title(titleStr+' and time %4.2f days'%(data['ocean_time'][nt]/8.64e4))
        vv.xlabel('x in km')
        vv.ylabel('y in km')
        vv.zlabel('z in m')
        a1.daspect=dataAspect
        jnk2=vv.mesh(np.fliplr(vertsBot),faces=facesBot)
        jnk=vv.mesh(np.fliplr(verts), faces=faces,values=verts[:,0])
        jnk.colormap=vv.CM_JET
        vv.colorbar()
        vv.use().Run() 

