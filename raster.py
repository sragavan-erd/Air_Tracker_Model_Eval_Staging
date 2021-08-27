import base64
import copy
import io
import tempfile
import textwrap

from netCDF4 import Dataset
import numpy as np


def mercator_transform(data, lat_bounds, origin='upper', height_out=None):
    """
    Transforms an image computed in (longitude,latitude) coordinates into
    the a Mercator projection image.
    Parameters
    ----------
    data: numpy array or equivalent list-like object.
        Must be NxM (mono), NxMx3 (RGB) or NxMx4 (RGBA)
    lat_bounds : length 2 tuple
        Minimal and maximal value of the latitude of the image.
        Bounds must be between -85.051128779806589 and 85.051128779806589
        otherwise they will be clipped to that values.
    origin : ['upper' | 'lower'], optional, default 'upper'
        Place the [0,0] index of the array in the upper left or lower left
        corner of the axes.
    height_out : int, default None
        The expected height of the output.
        If None, the height of the input is used.
    See https://en.wikipedia.org/wiki/Web_Mercator for more details.

    Originally from Folium
    Source: https://github.com/python-visualization/folium/blob/master/folium/utilities.py
    """
    import numpy as np

    def mercator(x):
        return np.arcsinh(np.tan(x*np.pi/180.))*180./np.pi

    array = np.atleast_3d(data).copy()
    height, width, nblayers = array.shape

    lat_min = max(lat_bounds[0], -85.051128779806589)
    lat_max = min(lat_bounds[1], 85.051128779806589)
    if height_out is None:
        height_out = height

    # Eventually flip the image
    if origin == 'upper':
        array = array[::-1, :, :]

    lats = (lat_min + np.linspace(0.5/height, 1.-0.5/height, height) *
            (lat_max-lat_min))
    latslats = (mercator(lat_min) +
                np.linspace(0.5/height_out, 1.-0.5/height_out, height_out) *
                (mercator(lat_max)-mercator(lat_min)))

    out = np.zeros((height_out, width, nblayers))
    for i in range(width):
        for j in range(nblayers):
            out[:, i, j] = np.interp(latslats, mercator(lats),  array[:, i, j])

    # Eventually flip the image.
    if origin == 'upper':
        out = out[::-1, :, :]
    return out


class Raster():
    def __init__(self, path: str = None, buffer: io.BytesIO = None):
        """Representation of gridded 2 or 3 dimensional data

        Args:
            path (str, optional): path to netcdf file conforming to CF metadata 
                conventions. Must have dimensions ordered as (x, y, layers)
                with a crs global attribute defining map projection
            buffer (io.BytesIO, optional): in memory buffer streaming file data
        """
        if not path and not buffer:
            raise ValueError('Must supply path or buffer')
        if buffer:
            memory = buffer.getvalue()
            buffer.close()
            path = tempfile.TemporaryFile()
        else:
            memory = None

        with Dataset(path, memory=memory) as nc:
            keys = list(nc.variables.keys())
            self.x = nc.variables[keys[0]][:].filled()
            self.y = nc.variables[keys[1]][:].filled()
            self.layers = nc.variables[keys[2]][:].filled()
            self.values = nc.variables[keys[3]][:].filled()
            self.crs = nc.crs

        if np.sign(np.diff(self.y).mean()) > 0:
            self.values = np.flip(self.values, axis=1)
            self.y = np.flip(self.y)

        if self.layers.dtype == np.float64 and self.layers.mean() > 1e9:
            self.layers = np.datetime64(
                '1970-01-01') + self.layers.astype('timedelta64[s]')

        self.dimensions = {
            'x': len(self.x),
            'y': len(self.y),
            'layers': len(self.layers)
        }
        self.resolution = {
            'x': round(np.abs(np.diff(self.x).mean()), 8),
            'y': round(np.abs(np.diff(self.y).mean()), 8)
        }
        self.extent = {
            'xmin': round(self.x.min() - self.resolution['x']/2, 8),
            'xmax': round(self.x.max() + self.resolution['x']/2, 8),
            'ymin': round(self.y.min() - self.resolution['y']/2, 8),
            'ymax': round(self.y.max() + self.resolution['y']/2, 8)
        }

    def copy(self):
        return copy.copy(self)

    def _mercator(self, y):
        return np.arcsinh(np.tan(y * np.pi/180)) * 180/np.pi

    def plot(self, path: str = None, log10: bool = False,
             mercator: bool = True, *args, **kwargs):
        """Plot visualization of gridded data

        If Raster is 3 dimensional, the layers axis is summed.

        Args:
            path (str, optional): location to output figure to disk. If not
                provided, a base64 representation of the image is returned.
            log10 (bool): log10 transform values prior to plotting
            *args, **kwargs: passed to plt.imshow()
        """
        import matplotlib.pyplot as plt

        image = self.values
        while len(image.shape) > 2:
            image = image.sum(axis=0)

        if mercator:
            ymin = max(self.extent['ymin'], -85)
            ymax = min(self.extent['ymax'], 85)
            image = mercator_transform(image, [ymin, ymax])

        image[image == 0] = np.nan

        if log10:
            image = np.log10(image)

        vmin = kwargs.get('vmin')
        if vmin:
            image[image < vmin] = np.nan

        fig = plt.imshow(image, *args, **kwargs)
        plt.axis('off')

        f = io.BytesIO()
        plt.savefig(f, bbox_inches='tight',
                    dpi=300, pad_inches=0, transparent=True)
        plt.close()

        b64 = base64.b64encode(f.getvalue())
        if not path:
            return {
                **self.extent,
                'image': b64
            }

        with open(path, 'wb') as f:
            f.write(base64.decodebytes(b64))

        return path

    def sum(self, axis: int = 0):
        """Sum values over given axis"""
        self.values = self.values.sum(axis=axis)
        self.layers = [None]
        return self.copy()

    def _validate_raster_attributes(self, x):
        """Ensure cell-by-cell operations are valid"""
        if self.extent != x.extent:
            raise ValueError('Extents do not match.')
        if self.resolution != x.resolution:
            raise ValueError('Resolutions do not match.')
        if not np.array_equal(self.x, x.x):
            raise ValueError('x attributes do not match.')
        if not np.array_equal(self.y, x.y):
            raise ValueError('y attributes do not match.')
        if len(self.layers) != len(x.layers):
            raise ValueError('layers lengths do not match.')
        if self.crs != x.crs:
            raise ValueError('crs attributes do not match.')

    def __add__(self, x):
        y = self.copy()
        if isinstance(x, Raster):
            self._validate_raster_attributes(x)
            y.values = y.values + x.values
            return y
        else:
            y.values = y.values + x
            return y

    def __sub__(self, x):
        y = self.copy()
        if isinstance(x, Raster):
            self._validate_raster_attributes(x)
            y.values = y.values - x.values
            return y
        else:
            y.values = y.values - x
            return y

    def __mul__(self, x):
        y = self.copy()
        if isinstance(x, Raster):
            self._validate_raster_attributes(x)
            y.values = y.values * x.values
            return y
        else:
            y.values = y.values * x
            return y

    def __truediv__(self, x):
        y = self.copy()
        if isinstance(x, Raster):
            self._validate_raster_attributes(x)
            y.values = y.values / x.values
            return y
        else:
            y.values = y.values / x
            return y

    def __rtruediv__(self, x):
        y = self.copy()
        if isinstance(x, Raster):
            self._validate_raster_attributes(x)
            y.values = x.values / y.values
            return y
        else:
            y.values = x / y.values
            return y

    def __repr__(self):
        return textwrap.dedent(f'''
            <Raster dimensions: {self.dimensions} 
                resolution: {self.resolution} 
                extent: {self.extent}>''')
