import sys
import os
import math
import glob
import json

import argparse
import functools

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Polygon, shape, Point, mapping
from shapely.ops import transform
import pyproj


def get_conf(conf_path):
    with open(conf_path, 'r') as f:
        config = json.load(f)
    return config


def get_area_perimeter_from_lambert(geom):
    '''Area from cell's lambert azimutal projection'''
    perimeter = np.nan
    area = np.nan
    try:
        if (-180 <= geom.centroid.x <= 180) and (-90 <= geom.centroid.y <= 90):
            proj_str = f"+proj=laea +lat_0={geom.centroid.y} +lon_0={geom.centroid.x}"
            project = pyproj.Transformer.from_crs('EPSG:4236', proj_str, always_xy=True).transform
            perimeter = transform(project, geom).length
            area = transform(project, geom).area
        else:
            print(f'invalid centroid {geom.centroid}')
    except Exception as ex:
        print(f'invalid centroid Exception')
        print(ex)
    # return (area, perimeter)
    return pd.Series([area, perimeter])


def compactness_calculation_np(df, spherical=False):
    np_area = df['area'].values
    np_peri = df['perimeter'].values

    if not spherical:
        top = 4*np.pi*np_area
        bottom = np.square(np_peri)
        ipq = top / bottom
        
        return ipq

    else:
        t1 = 4*np.pi*np_area
        t2 = np.square(np_area)
        t3 = np.square(6378137.0)
        top = np.sqrt( t1 - t2/t3 )
        bottom = np_peri
        zsc_np = top / bottom

        # zsc_orig = df.apply(lambda row: math.sqrt(  4*math.pi*row['area'] - math.pow(row['area'],2) / math.pow(6378137,2)  )  /   row['perimeter'], axis=1).values
        # diffs = np.sum(zsc_np - zsc_orig)
        # ma = max_error(zsc_orig, zsc_np)
        # mae = mean_absolute_error(zsc_orig, zsc_np)
        # print(f"ma={ma} mae={mae} diffs={diffs}")

        return zsc_np


def zsc_calculation_np(df, spherical=False):
    np_area = df['area'].values
    np_peri = df['perimeter'].values

    if not spherical:
        top = 4*np.pi*np_area
        bottom = np.square(np_peri)
        c_orig = top / bottom

        return c_orig
    else:
        t1 = 4*np.pi*np_area
        t2 = np.square(np_area)
        t3 = np.square(6378137.0)
        top = np.sqrt( t1 - t2/t3 )
        bottom = np_peri
        zsc_np = top / bottom

        # zsc_orig = df.apply(lambda row: math.sqrt(  4*math.pi*row['area'] - math.pow(row['area'],2) / math.pow(6378137,2)  )  /   row['perimeter'], axis=1).values
        # diffs = np.sum(zsc_np - zsc_orig)
        # ma = max_error(zsc_orig, zsc_np)
        # mae = mean_absolute_error(zsc_orig, zsc_np)
        # print(f"ma={ma} mae={mae} diffs={diffs}")

        return zsc_np

def check_crossing(lon1: float, lon2: float, validate: bool = True):
    """
    Assuming a minimum travel distance between two provided longitude coordinates,
    checks if the 180th meridian (antimeridian) is crossed.
    """
    if validate and any(abs(x) > 180.0 for x in [lon1, lon2]):
        raise ValueError("longitudes must be in degrees [-180.0, 180.0]")
    return abs(lon2 - lon1) > 180.0


def check_for_geom(geom):
    crossed = False
    p_init = geom.exterior.coords[0]

    for p in range(1, len(geom.exterior.coords)):
        px = geom.exterior.coords[p]
        # print(px)
        try:
            if check_crossing(p_init[0], px[0]):
                crossed = True
        except ValueError:
            crossed = True

        p_init = px

    return crossed


def the_job(parquet_file_name, parquet_file_name_step2):

    if os.path.exists(parquet_file_name_step2):
        print(f"{parquet_file_name_step2} exists, skipping")
        return None
    else:
        df = pd.read_parquet(parquet_file_name)

        df['geometry'] = df['wkt'].apply(lambda s: wkt.loads(s) if not s == "error" else None)

        df = df.dropna(subset=['geometry'])

        df['crossed'] = df['geometry'].apply(check_for_geom)


        result = df['geometry'].apply(get_area_perimeter_from_lambert)
        result = result.rename(columns={0: 'area', 1: 'perimeter'})
        df = df.assign(area=result['area'], perimeter=result['perimeter'] )

        df['ipq'] = compactness_calculation_np(df, False)
        df['zsc'] = zsc_calculation_np(df, True)

        df = df[~df['area'].isna()]

        result = df['geometry'].apply(lambda g: pd.Series((g.centroid.x, g.centroid.y)))
        result = result.rename(columns={0: 'lon', 1: 'lat'})
        df = df.assign(lon=result['lon'], lat=result['lat'] )
 
        gdf = gpd.GeoDataFrame(df.drop(columns=['wkt']), geometry="geometry", crs=4326)
        gdf.to_file(parquet_file_name_step2.replace('.parquet', '.fgb'), driver="FlatGeobuf")

        df = df.drop(columns=["geometry", 'wkt'])

        df.to_parquet(parquet_file_name_step2, compression='gzip', index=False)
        return parquet_file_name_step2


def main():

    #Parsing arguments
    parser = argparse.ArgumentParser()

    #Specify item number
    parser.add_argument('-num', action='store', dest='num', default='-1',
                        help='Specify list work item number')

    #Specify work list
    parser.add_argument('-worklist', action='store', dest='worklist', default='empty',
                        help='Specify list work item number')

    #Specify input
    parser.add_argument('-infile', action='store', dest='infile', default='info',
                        help='infile')

    #Specify output
    parser.add_argument('-outfile', action='store', dest='outfile', default='outfile',
                        help='Specify output')

    #Specify workdir folder
    parser.add_argument('-workdir', action='store', dest='workdir', default=os.curdir,
                        help='Specify workdir folder')

    #Get the results of argument parsing
    results = parser.parse_args()

    #Store the results of argument parsing

    # run so: python compute_area_zsc_hpc.py -num 841 -worklist worklist.csv -workdir R:\kmoch\datacube_data\parquet_src

    infile = results.infile
    outfile = results.outfile
    num = int(results.num)
    worklist = results.worklist
    workdir = results.workdir


    if not worklist == 'empty':
        if num < 0:
            try:
                num = int(os.environ["SLURM_ARRAY_TASK_ID"])
            except:
                print("error, no task ID work item number possible")

        # load worklist and assign out
        df = pd.read_csv(os.path.join(workdir, worklist), index_col=0)
        ax = os.path.join(workdir, df.iloc[num]['infile'])
        bx = os.path.join(workdir, df.iloc[num]['outfile'])
        the_job(ax, bx)
    else:
        the_job(infile, outfile)


if __name__ == "__main__":
    main()
