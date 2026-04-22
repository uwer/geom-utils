import numpy as np

def openFile(fpath, varname, idx_t = None ,coords = ['lon','lat','depth']):
    
    import xarray as xr
    import pyvista as pv
    
    # Load dataset
    ds = xr.open_dataset(fpath)
    temp_volume = ds[varname].values # 3/4D numpy array
    if not idx_t is None:
        temp_volume = temp_volume[idx_t,:,:,:]

        
    return ds[coords[0]].values, ds[coords[1]].values, ds[coords[2]].values , temp_volume


def _cell_bounds(points, bound_position=0.5):
    """
    Calculate coordinate cell boundaries.

    Parameters
    ----------
    points: numpy.ndarray
        One-dimensional array of uniformly spaced values of shape (M,).

    bound_position: bool, optional
        The desired position of the bounds relative to the position
        of the points.

    Returns
    -------
    bounds: numpy.ndarray
        Array of shape (M+1,)

    Examples
    --------
    >>> a = np.arange(-1, 2.5, 0.5)
    >>> a
    array([-1. , -0.5,  0. ,  0.5,  1. ,  1.5,  2. ])
    >>> _cell_bounds(a)
    array([-1.25, -0.75, -0.25,  0.25,  0.75,  1.25,  1.75,  2.25])

    """
    if points.ndim != 1:
        msg = 'Only 1D points are allowed.'
        raise ValueError(msg)
    diffs = np.diff(points)
    delta = diffs[0] * bound_position
    return np.concatenate([[points[0] - delta], points + delta])



def make_up_point_set():
    """Return an n by 3 numpy array of structured coordinates.

    The contents of this function can be ignored.
    """
    n, m = 29, 32
    x = np.linspace(-200, 200, num=n) + rng.uniform(-5, 5, size=n)
    y = np.linspace(-200, 200, num=m) + rng.uniform(-5, 5, size=m)
    xx, yy = np.meshgrid(x, y)
    A, b = 100, 100
    zz = A * np.exp(-0.5 * ((xx / b) ** 2.0 + (yy / b) ** 2.0))
    points = np.c_[xx.reshape(-1), yy.reshape(-1), zz.reshape(-1)]
    foo = pv.PolyData(points)
    foo.rotate_z(36.6, inplace=True)
    return foo.points

def make_point_set(x,y,z):
    """Return an n by 3 numpy array of structured coordinates.

    The contents of this function can be ignored.
    """
    import pyvista as pv
    pv.global_theme.allow_empty_mesh = True
    
    xx, yy , zz= np.meshgrid(x, y, z)
    points = np.c_[xx.reshape(-1), yy.reshape(-1), zz.reshape(-1)]
    foo = pv.PolyData(points)
    return foo


def calc_iso_surface(my_array, my_value, zs):
    '''Takes the average of the two closest zs
    from x,y,z value array
    it sorts the value array with the closest match to my_value first
    '''
    from numpy import argsort, take
    print(f"values {my_array.min()} to {my_array.max()} - {my_value}")
    dist = (my_array - my_value)**2
    arg = argsort(dist,axis=2)
    z0 = take(zs, arg[:,:,0])
    z1 = take(zs, arg[:,:,1])
    z = (z0+z1)/2
    return z

def test_iso(fpath, varname, idx_t = None,iso_value = None):
    import xarray as xr
    import pyvista as pv
    
    '''
    # Load dataset
    ds = xr.open_dataset(fpath)
    temp_volume = ds[varname].values # 3D numpy array
    if not idx_t is None:
        temp_volume = temp_volume[idx_t,:,:,:]
    ''' 
        
    xa, ya , za, vala = openFile(fpath, varname, idx_t)
    
    print(f" {vala.min()} {vala.max()}")
    vala_no_nan = np.nan_to_num(vala)
    print(f" {vala_no_nan.min()} {vala_no_nan.max()}")
    
    verts,faces,normals,values = makeMesh(vala, 0.)
    
    print(f" {values.min()} {values.max()}")
    
    '''
    points = make_point_set(xa,ya,za)
    
        
    points.plot(point_size =10)
    
    surf = points.delaunay_2d()
    surf.plot(show_edges=True)
    '''
    points = pv.PolyData(verts[faces])
    points.plot(point_size =10)
    surf = points.delaunay_2d()
    surf.plot(show_edges=True)
    
    # Create structured grid
    grid = pv.StructuredGrid()#ds['lon'].values, ds['lat'].values, ds['depth'].values)
    grid.points = verts
    #vala_no_nan = np.nan_to_num(vala)
    #grid[varname] = values
    grid.plot(smooth_shading=True)
    
    # Generate and plot isosurface
    iso = grid.contour([0.]) # Surface where value == 20
    iso.plot(opacity=0.5)
    
    

def test_iso21(fpath, varname, idx_t = None,iso_value = None):
    import xarray as xr
    import pyvista as pv
    from pyvista import examples
    
    doworld = True
    '''
    # Load dataset
    ds = xr.open_dataset(fpath)
    temp_volume = ds[varname].values # 3D numpy array
    if not idx_t is None:
        temp_volume = temp_volume[idx_t,:,:,:]
    ''' 
    # z,y,x order
    xa, ya , za, vala = openFile(fpath, varname, idx_t)
    
    RADIUS = 6371000.0 # metres

    # Longitudes and latitudes
    y_polar = 90.0 - ya  # grid_from_sph_coords() expects polar angle
    
    xx, yy = np.meshgrid(xa, ya)
    
    # Create arrays of grid cell boundaries, which have shape of (x.shape[0] + 1)
    xx_bounds = _cell_bounds(xa)
    yy_bounds = _cell_bounds(y_polar)
    
    levels = [RADIUS + za[1]]
    
    
    vala_nn =  np.nan_to_num(vala)
    scalar = vala_nn[1,:,:]
    
    
    grid_scalar = pv.grid_from_sph_coords(xx_bounds, yy_bounds, RADIUS)
    
    #neval = neval.T
    
    
    
    bottom = grid_scalar.points.copy()
    
    neval = calc_iso_surface(vala.T,0.1,za)
    neval = neval.T
    print(f"Z min max {neval.min()} {neval.max()}")
    #neval+=    RADIUS
    #print(f"Z min max {neval.min()} {neval.max()}")
    neval_boundsx =[]
    for i in range(len(neval)):
        neval_boundsx.append(_cell_bounds(neval[i,:]))
        
    neval_boundsx = np.array(neval_boundsx)
    neval_bounds = []
    for i in range(len(neval_boundsx[0])):
        neval_bounds.append(_cell_bounds(neval_boundsx[:,i]))
        
    neval_bounds = np.array(neval_bounds).T
        
    vol = pv.StructuredGrid()
    nevalflat = neval_bounds.flatten()
    #bottom = [0,0,1] * len (nevalflat)
    #bottom[:,-1] = nevalflat
    top = grid_scalar.points.copy()
    top[:,-1] = nevalflat
    vol.points = np.vstack((bottom,top))
    vol.dimensions = [*grid_scalar.dimensions[0:2], 2]
    vol.plot(show_edges=True,opacity=0.3)
    
    iso_values = np.ones(scalar.shape) * 0.1
    # discovery 
    # x- and y-components of the wind vector
    #u_vec = np.cos(np.radians(xx))  # zonal
    #v_vec = np.sin(np.radians(yy))  # meridional
    
    # Scalar data
    #scalar = u_vec**2 + v_vec**2
    example_scalar = np.array(scalar).swapaxes(-2, -1).ravel('C')
    #example_iso = np.array(values).swapaxes(-2, -1).ravel('C')
    
    # And fill its cell arrays with the scalar data
    vol.cell_data[varname] = example_scalar
    
    # Make a plot
    pl = pv.Plotter()
    
    if doworld:
        # And let's display it with a world map
        #tex = examples.load_globe_texture()
        
        sphere = pv.Sphere(radius=RADIUS-za.max(),
                           theta_resolution=120,
            phi_resolution=120,
            start_theta=270.001,
            end_theta=270)
        # Initialize the texture coordinates array
        #
        '''
        sphere.active_texture_coordinates = np.zeros((sphere.points.shape[0], 2))
        
        # Populate by manually calculating
        for i in range(sphere.points.shape[0]):
            sphere.active_texture_coordinates[i] = [
                0.5 + np.arctan2(-sphere.points[i, 0], sphere.points[i, 1]) / (2 * np.pi),
                0.5 + np.arcsin(sphere.points[i, 2]) / np.pi,
                ]
        '''     
        pl.add_mesh(sphere,opacity=0.3)#, texture = tex)
        
        
    pl.add_mesh(vol, clim=[scalar.min(),scalar.max()], opacity=0.5, cmap='plasma')
    #iso = grid_scalar.contour(0.)
    #pl.add_mesh(iso, clim=[scalar.min(),scalar.max()], opacity=0.5, cmap='plasma')
    
    pl.show()
    
    
    
    
def test_iso2(fpath, varname, idx_t = None,iso_value = None):
    import xarray as xr
    import pyvista as pv
    from pyvista import examples
    
    doworld = False
    '''
    # Load dataset
    ds = xr.open_dataset(fpath)
    temp_volume = ds[varname].values # 3D numpy array
    if not idx_t is None:
        temp_volume = temp_volume[idx_t,:,:,:]
    ''' 
    # z,y,x order
    xa, ya , za, vala = openFile(fpath, varname, idx_t)
    
    RADIUS = 6371000.0 # metres

    # Longitudes and latitudes
    y_polar = 90.0 - ya  # grid_from_sph_coords() expects polar angle
    
    xx, yy = np.meshgrid(xa, ya)
    
    # Create arrays of grid cell boundaries, which have shape of (x.shape[0] + 1)
    xx_bounds = _cell_bounds(xa)
    yy_bounds = _cell_bounds(y_polar)
    
    levels = [RADIUS + za[1]]
    
    
    vala_nn =  np.nan_to_num(vala)
    scalar = vala_nn[1,:,:]
    #verts,faces,normals,values = makeMesh(np.nan_to_num(vala), 0.)
    from skimage import measure
     
    # we need to walk through the rows and pass a slice at the time
    contour_idx_rows = []
    contour_z = [] 
    for i in range(vala_nn.shape[-1]):
        contour_idx = measure.find_contours(vala_nn[:,:,i], 0.0)
        contour_idx_rows.append(contour_idx)
        newz = [np.nan] * vala_nn.shape[-2]
        for cii in contour_idx:
            for c in cii:
                newz[int(c[1])] = za[int(c[0])]
        contour_z.append(newz)
        
    contour_z = np.array(contour_z)
    
    grid_scalar = pv.grid_from_sph_coords(xx_bounds, yy_bounds)#, RADIUS+contour_z)
    
    # discovery 
    # x- and y-components of the wind vector
    #u_vec = np.cos(np.radians(xx))  # zonal
    #v_vec = np.sin(np.radians(yy))  # meridional
    
    # Scalar data
    #scalar = u_vec**2 + v_vec**2
    example_scalar = np.array(contour_z).swapaxes(-2, -1).ravel('C')
    #example_iso = np.array(values).swapaxes(-2, -1).ravel('C')
    
        # And fill its cell arrays with the scalar data
    grid_scalar.cell_data[varname] = example_scalar
    
    # Make a plot
    pl = pv.Plotter()
    
    if doworld:
        # And let's display it with a world map
        tex = examples.load_globe_texture()
        
        sphere = pv.Sphere(radius=RADIUS-za.max(),
                           theta_resolution=120,
            phi_resolution=120,
            start_theta=270.001,
            end_theta=270)
        # Initialize the texture coordinates array
        sphere.active_texture_coordinates = np.zeros((sphere.points.shape[0], 2))
        
        # Populate by manually calculating
        for i in range(sphere.points.shape[0]):
            sphere.active_texture_coordinates[i] = [
                0.5 + np.arctan2(-sphere.points[i, 0], sphere.points[i, 1]) / (2 * np.pi),
                0.5 + np.arcsin(sphere.points[i, 2]) / np.pi,
                ]
                
        pl.add_mesh(sphere, texture = tex)
        
        
    pl.add_mesh(grid_scalar, clim=[scalar.min(),scalar.max()], opacity=0.5, cmap='plasma')
    #iso = grid_scalar.contour(0.)
    #pl.add_mesh(iso, clim=[scalar.min(),scalar.max()], opacity=0.5, cmap='plasma')
    
    pl.show()
    
    
def test_iso3(fpath, varname, idx_t = None,iso_value = None):
    import xarray as xr
    import pyvista as pv
    from pyvista import examples
    
    doworld = False
    
    xa, ya , za, vala = openFile(fpath, varname, idx_t)
    
    print(f"z values {za.min()} to {za.max()} ")
    RADIUS = 6371000.0 # metres

    # Longitudes and latitudes
    y_polar = 90.0 - ya  # grid_from_sph_coords() expects polar angle
    
    xx, yy = np.meshgrid(xa, ya, indexing='ij')
    
    vala_nn =  np.nan_to_num(vala)
    scalar = vala_nn[1,:,:]
    #verts,faces,normals,values = makeMesh(np.nan_to_num(vala), 0.)
    from skimage import measure
    '''
    # we need to walk through the rows and pass a slice at the time
    contour_idx_rows = []
    contour_z = [] 
    for i in range(vala_nn.shape[-1]):
        contour_idx = measure.find_contours(vala_nn[:,:,i], 0.)
        contour_idx_rows.append(contour_idx)
        newz = [np.nan] * vala_nn.shape[-2]
        for cii in contour_idx:
            for c in cii:
                newz[int(c[1])] = za[int(c[0])]
        contour_z.append(newz)
        
    contour_z = np.array(contour_z).T
    '''
    #vala_0_idx = np.where(vala < 0.1,vala)
    #vala_1_idx = np.where( vala > -0.1,vala)
    
    
    #  the function requres x,y,z - 
    # if indexing ij, disable transpose below
    neval = calc_iso_surface(vala.T,0.1,za)
    #neval = neval.T
    
    print(f"Z min max {neval.min()} {neval.max()}")
    neval+=    RADIUS
    print(f"Z min max {neval.min()} {neval.max()}")
    
    # Create and plot structured grid
    grid = pv.StructuredGrid(xx, yy,neval )
    
    #grid.plot(smooth_shading=True)
    #grid.plot_curvature( smooth_shading=True,show_edges=True, show_grid=True, cpos='xy')
    
    # Make a plot
    pl = pv.Plotter()
    
    sphere = pv.Sphere(radius=RADIUS-za.max())#,
    '''
                           theta_resolution=120,
            phi_resolution=120,
            start_theta=270.001,
            end_theta=270)
    '''         
    pl.add_mesh(sphere,opacity=0.3,)
        
    pl.add_mesh(grid, clim=[neval.min(),neval.max()],  cmap='plasma')
    
    pl.show()
    
    
def makeMesh(ct,isoSurface):
    '''
    ct shape (nz,ny,nx)  : data
    isoSurface scaler    : isosurface to return

    returns verts,faces, normals, values as returned by skimage.measure.marching_cubes_lewiner
    
    For documentation of the returned feilds, and how to plot this output, see 
    https://scikit-image.org/docs/dev/api/skimage.measure.html#skimage.measure.marching_cubes_lewiner
    '''
    #from mpl_toolkits.mplot3d.art3d import Poly3DCollection
    #from scipy.interpolate import RectBivariateSpline as RBS
    #from scipy.interpolate import interpn
    from skimage import measure
    print(f"values shape {ct.shape}")
    #make isosurface on rectangular grid
    verts, faces, normals, values = measure.marching_cubes(ct, level=isoSurface)
    print(f" surface shape {verts.shape} {faces.shape} {normals.shape} {values.shape} ")
    return verts,faces,normals,values

    
def test_mesh(fpath, varname, idx_t = None,iso_value = None):
    
    xa, ya , za, vala = openFile(fpath, varname, idx_t)
    
    verts,faces,normals,values = makeMesh(xa, ya , za, vala, 0.)
    
test_iso21("/Users/ros260/projects/data/ML/enso-ops/output/dtdz-anom-smoothed_1991-2020_remapped.nc", "dtdz",idx_t= 300)
#test_mesh("/Users/ros260/projects/data/ML/enso-ops/output/dtdz-anom-smoothed_1991-2020_remapped.nc", "dtdz",idx_t= 300)