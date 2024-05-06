import healpy as hp

import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import Transformer
from shapely.ops import transform
try:
    import rasterio
except ImportError:
    print("rasterio not available")

from typing import Any
import time
import sys
import os
import matplotlib.pyplot as plt

sys.path.append('..')

from shapely.geometry import Polygon, Point, box
from pandas.core.common import flatten
from math import radians, sin, cos, asin, sqrt

#defaults
NEST = True

def __haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000


def _latlon2cellid(lat: Any, lon: Any, nside, nest=NEST) -> np.ndarray:
    return hp.ang2pix(nside, lon, lat, lonlat=True, nest=nest)

def _cellid2latlon(cell_ids: Any, nside, nest=NEST, as_geojson=False) -> tuple[np.ndarray, np.ndarray]:
    lon, lat = hp.pix2ang(nside, cell_ids, nest=nest, lonlat=True)
    # TODO: wrap for lon if lon > 180
    lon = np.where(lon > 180, lon - 360, lon)
    if as_geojson:
        return np.stack([lat, lon], axis=-1)
    return np.stack([lon, lat], axis=-1)

def _cellid2boundaries(cell_ids: Any, nside, nest=NEST, as_geojson=False) -> np.ndarray:
    boundary_vectors = hp.boundaries(
        nside, cell_ids, step=1, nest=nest
    )

    lon, lat = hp.vec2ang(np.moveaxis(boundary_vectors, 1, -1), lonlat=True)
    # TODO: wrap for lon if lon > 180
    lon = np.where(lon > 180, lon - 360, lon)
    if as_geojson:
        return np.reshape(np.stack((lat, lon), axis=-1), (-1, 4, 2))

    boundaries = np.reshape(np.stack((lon, lat), axis=-1), (-1, 4, 2))
    return boundaries

# resolution here is the order, not the nside!
# re extent: theta is  co-latitude, i.e. at the North Pole, at the Equator, at the South Pole
# phi is the longitude
def get_healpy_cells(resolution,extent=None) -> np.ndarray:
    nside = hp.order2nside(resolution)
    npix = hp.nside2npix(nside)
    m = np.arange(npix)
    # if extent is none we just work with the whole map

    if not extent is None:

        approx_res = hp.nside2resol(nside, arcmin=True) / 60
        xmin, ymin, xmax, ymax = extent
        # theta is (co-)latiude, phi is longitude when radians
        # BUT apparently the order is other way round when lonlat True
        ipix_corners = hp.ang2pix(nside=nside,
                                  phi=[ymin, ymin, ymax, ymax],
                                  theta=[xmin, xmax, xmin, xmax],
                                  nest=NEST, lonlat=True)
        # spaced coords from min to max
        theta = np.arange(ymin-approx_res, ymax+approx_res, approx_res)
        phi = np.arange(xmin-approx_res, xmax+approx_res, approx_res/2)
        # combinations to fill 2d space
        theta, phi = np.meshgrid(theta, phi)
        # flatten/unravel to 1d
        ipix_grid = hp.ang2pix(nside=nside,
                               phi=theta.flatten(),
                               theta=phi.flatten(),
                               nest=NEST, lonlat=True)
    
        m = np.unique(np.concatenate([ipix_corners, ipix_grid]))
    
    df = pd.DataFrame({'cell_id': m})
    return df


def create_healpy_geometry(df, resolution, as_geojson=False):
    cell_ids = df['cell_id'].values
    nside = hp.order2nside(resolution)
    bounds = _cellid2boundaries(cell_ids, nside, nest=NEST, as_geojson=as_geojson)
    # bounds need to be converted to shapely polygon, each bound is an array of 4 points
    # each point is a tuple of lon, lat
    polygons = [Polygon(bound) for bound in bounds]
    df['geometry'] = polygons
    gdf = gpd.GeoDataFrame(df.copy(), geometry='geometry', crs='EPSG:4326')
    return gdf


