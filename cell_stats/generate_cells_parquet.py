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
from shapely.geometry import Polygon, shape, Point, mapping
from shapely.ops import transform
import pyproj

import dask
import dask.dataframe as ddf

from dask.distributed import Client, LocalCluster
# from sklearn.metrics import max_error, mean_absolute_error

# python env conda daskgeo2020a

# OpenEAGGR via Python wheel egg

# Google S2 via sys.path.append('/usr/local/lib/python3/dist-packages')
sys.path.append('/usr/local/lib/python3.7/site-packages')

sys.path.append('..')

try:
    from h3_helper import *
except ImportError as ex:
    print("H3 modules not found")
    print(ex)

try:
    from s2_helper import *
except ImportError as ex:
    print("S2 modules not found")
    print(ex)

try:
    from rhealpix_helper import *
except ImportError as ex:
    print("rhealpix dggs modules not found")
    print(ex)

try:
    from eaggr_helper import *
except ImportError as ex:
    print("OpenEaggr modules not found")
    print(ex)

try:
    from dggrid4py import DGGRIDv7, Dggs, dgselect, dggs_types
except ImportError as ex:
    print("dggrid4py modules not found")
    print(ex)



def timer(func):

    @functools.wraps(func)
    def wrapper_timer(*args,**kwargs):
        start_time = time.perf_counter()
        value = func(*args, **kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"finished{func.__name__!r} in {run_time:.4f} sec")
        return value
    return wrapper_timer


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


@timer
def check_geom_dfp(gdf):
    gdf['crossed'] = gdf['geometry'].apply(check_for_geom)
    return gdf


@timer
def get_cells_area(gdf,crs):
    '''Get cells area for crs'''

    if crs =='LAEA':
        # gdf['area'],gdf['perimeter'] = zip(*gdf['geometry'].apply(get_area_perimeter_from_lambert))
        gdf[['area','perimeter']] = gdf['geometry'].apply(get_area_perimeter_from_lambert)

    else:
        gdf = gdf.to_crs(crs)
        gdf['area'] = gdf['geometry'].area
    return gdf


@timer
def create_cells(dggs, resolution, dggrid, extent=None):
    '''Creates cells for given DGGS on given resolution'''
    # extent is a shapely geometry

    if dggs[0] == 'h3':
        # extent (list): Extent as array of 2 lon lat pairs
        if extent:
            df = get_h3_cells(resolution, mapping(extent) )
        else:
            df = get_h3_cells(resolution,extent)

        gdf = create_h3_geometry(df)

    elif dggs[0] == 's2':
        # extent (list): Extent as array of 2 lon lat pairs
        if extent:
            df = get_s2_cells(resolution,extent.bounds)
        else:
            df = get_s2_cells(resolution,extent)

        gdf = create_s2_geometry(df)

    elif dggs[0] == 'DGGRID':
        # clip_geom is a shapely geometry
        if extent:
            extent = extent
            gdf = dggrid.grid_cell_polygons_for_extent(dggs[1], resolution, clip_geom=extent)
            gdf.crs = 'EPSG:4326'
        else:
            gdf = dggrid.grid_cell_polygons_for_extent(dggs[1], resolution)

        gdf = gdf.rename(columns={"Name": "cell_id", "name": "cell_id"})

    elif dggs[0] == 'eaggr':
        # extent (list): Extent as array of 2 lon lat pairs
        if extent:
            print("currently not implemented")
            # df = get_eaggr_cells(resolution,extent.bounds, dggs[1])
            df = pd.DataFrame({'cell_id': [], 'cell': [], 'geometry': []})
        else:
            df = get_eaggr_cells(resolution,extent, dggs[1])

        gdf = create_eaggr_geometry(df, dggs[1])

    elif dggs[0] == 'rhpix':
        # extent (list): Extent as array of 2 lon lat pairs
        if extent:
            df = get_rhpix_cells(resolution,extent.bounds)
        else:
            df = get_rhpix_cells(resolution,extent)

        gdf = create_rhpix_geometry(df)

    return gdf


@timer
def gen_cells(config, cpus, results_path, dggrid_exec, dggrid_work_dir, sample_polygons=None):
    for dggs in config['dggss']:

        print(f"Start processing {dggs['name']}")
        if dggs['name'][0] == 'DGGRID':
            dggrid_instance = DGGRIDv7(executable=dggrid_exec, working_dir=dggrid_work_dir, capture_logs=False, silent=False)
        else:
            dggrid_instance = None
        for res in dggs['global_res']:
            print(f"Start processing global resolution {res}")

            params = [dggs['name'], res, dggs['proj'], dggrid_instance]
            d_name, res, p_name = params[0], params[1], params[2]

            name = d_name[0]
            if len(d_name) > 1:
                name = '_'.join(d_name)

            name = f"{name}_{res}_{p_name}"
            parquet_file_name = os.path.join(results_path, f"{name}.parquet")

            if os.path.exists(parquet_file_name):
                print(f"{parquet_file_name} file exists, skipping")
                continue
            else:
                gdf = create_cells(dggs=params[0], resolution=params[1], dggrid=params[3], extent=None)
                rows = len(gdf.index)
                if rows > 0:
                    print(f"writing {parquet_file_name} with {rows} rows")
                    gdf['wkt'] = gdf['geometry'].apply(lambda g: g.wkt if not g is None else 'error')
                    pd.DataFrame(gdf.drop(columns="geometry")).to_parquet(parquet_file_name, compression='gzip', index=False)
                else:
                    print(f"Skipping {parquet_file_name} with {rows} rows")

        if not sample_polygons is None:

            for res in dggs['sample_res']:
                print(f"Start processing sample_res resolution {res}")

                params = [dggs['name'], res, dggs['proj'], dggrid_instance]
                d_name, res, p_name = params[0], params[1], params[2]

                for idx, row in sample_polygons.iterrows():
                    name = d_name[0]
                    if len(d_name) > 1:
                        name = '_'.join(d_name)

                    name = f"{name}_{res}_{p_name}_sample_id_{idx}"
                    parquet_file_name = os.path.join(results_path, f"{name}.parquet")

                    if os.path.exists(parquet_file_name):
                        print(f"{parquet_file_name} file exists, skipping")
                        continue
                    else:
                        extent = row['geometry']
                        gdf = create_cells(params[0], params[1], params[3], extent)

                        rows = len(gdf.index)
                        if rows > 0:
                            print(f"writing {parquet_file_name} with {rows} rows")
                            gdf['wkt'] = gdf['geometry'].apply(lambda g: g.wkt if not g is None else 'error')
                            pd.DataFrame(gdf.drop(columns="geometry")).to_parquet(parquet_file_name, compression='gzip', index=False)
                        else:
                            print(f"Skipping {parquet_file_name} with {rows} rows")
        else:
            print("no sampling polygons provided. skipping.")


@timer
def create_cell_stats_df(params, parquet_file_name, results_path):
    '''Create cell stats.'''
    d_name = params[0]

    name = d_name[0]
    if len(d_name) > 1:
        name = '_'.join(d_name)

    stats = []

    da = pd.read_parquet(parquet_file_name)
    num_cells_pre = len(da.index)
    # cell_id 	crossed 	area 	perimeter 	c_orig 	zsc 	lon 	lat

    geometry_errors = da['area'].isna().sum()
    da = da[~da['area'].isna()]
    date_line_cross_error_cells = len(da[da['crossed']].index)
    da2 = da[~da['crossed']]

    area_q_low = da['area'].quantile(0.005)
    area_q_high = da['area'].quantile(0.995)

    other_geom_anomalies = len( da[(da['area'] < area_q_low) & (da['area'] > area_q_high)] )

    area_min = da['area'].min()
    area_max = da['area'].max()
    area_std = da['area'].std()
    area_mean = da['area'].mean()

    da['norm_area'] = da['area'] / area_mean
    norm_area_std = da['norm_area'].std()
    norm_area_range = da['norm_area'].max() - da['norm_area'].min()

    zsc_min = da['zsc'].min()
    zsc_max = da['zsc'].max()
    zsc_std = da['zsc'].std()
    zsc_mean = da['zsc'].mean()

    da['norm_zsc'] = da['zsc'] / zsc_mean
    norm_zsc_std = da['norm_zsc'].std()
    norm_zsc_range = da['norm_zsc'].max() - da['norm_zsc'].min()

    c_orig_std = da['c_orig'].std()
    c_orig_std_range = da['c_orig'].max() - da['c_orig'].min()

    c_orig_min = da['c_orig'].min()
    c_orig_max = da['c_orig'].max()
    c_orig_std = da['c_orig'].std()
    c_orig_mean = da['c_orig'].mean()

    da['norm_c_orig'] = da['c_orig'] / c_orig_mean
    norm_c_orig_std = da['norm_c_orig'].std()
    norm_c_orig_range = da['norm_c_orig'].max() - da['norm_c_orig'].min()

    num_cells_used = len(da)

    area_stats = pd.DataFrame(
        {'name':[name],
         'resolution':[res],
         'min_area':[area_min],
         'max_area':[area_max],
         'std':[area_std],
         'mean':[area_mean],
         'norm_area_std':[norm_area_std],
         'norm_area_range':[norm_area_range],

         'min_zsc':[zsc_min],
         'max_zsc':[zsc_max],
         'zsc_std':[zsc_std],
         'zsc_mean':[zsc_mean],
         'norm_zsc_std':[norm_zsc_std],
         'norm_zsc_range':[norm_zsc_range],

         'min_c_orig':[c_orig_min],
         'max_c_orig':[c_orig_max],
         'c_orig_std':[c_orig_std],
         'c_orig_mean':[c_orig_mean],
         'norm_c_orig_std':[norm_c_orig_std],
         'norm_c_orig_range':[norm_c_orig_range],

         'num_cells_used':[num_cells_used],
         'num_cells_pre':[num_cells_pre],
         'date_line_cross_error_cells':[date_line_cross_error_cells],
         'other_geom_anomalies':[other_geom_anomalies],
         'geometry_errors':[geometry_errors]
         }
    )

    print(f"area_q_low {area_q_low}, area_q_high {area_q_high}, other_geom_anomalies {other_geom_anomalies}, geometry_errors {geometry_errors}, date_line_cross_error_cells {date_line_cross_error_cells} (num_cells_pre {num_cells_pre})")

    stats.append(area_stats)

    return pd.concat(stats)


def main():

    #Getting the directory of the script
    script_dir = os.getcwd()

    #Parsing arguments
    parser = argparse.ArgumentParser()

    #Specify base action cells, stats, cell_stats
    parser.add_argument('-action', action='store', dest='action', default='info',
                        help='base action cells, stats, cell_stats')

    #Specifies the config file
    parser.add_argument('-config', action='store', dest='config', default=os.path.join(script_dir,'config.json'),
                        help='Specifies the folder where config files are')

    #Specify base output name
    parser.add_argument('-name', action='store', dest='result_name', default='cell_stats',
                        help='Name for export to database or csv')

    #Specify number of paralell processes
    parser.add_argument('-cpu', action='store', dest='cpu', default='3',
                        help='Number of cpus for parallel processes to run')

    #Specify output folder
    parser.add_argument('-out', action='store', dest='out', default=os.path.join(script_dir,'results'),
                        help='Specify output folder')

    #Specify sample polygons path
    parser.add_argument('-sample', action='store', dest='sample', default=os.path.join(script_dir,'sample_polygons.geojson'),
                        help='Specify sample polygons path')

    #Specify dggrid path
    parser.add_argument('-dggrid', action='store', dest='dggrid', default=os.path.join(script_dir,'dggrid'),
                        help='Specify dggrid path')

    #Get the results of argument parsing
    results = parser.parse_args()

    #Store the results of argument parsing

    results_path = results.out
    config_path = results.config
    default_name = results.result_name
    cpus = int(results.cpu)
    sample = results.sample
    dggrid_exec = results.dggrid
    todo_command = results.action

    sample_polygons = gpd.read_file(sample,driver='GeoJSON')

    config = get_conf(config_path)

    dggrid_work_dir = os.path.join(script_dir,'dggrid_workdir')

    if not os.path.exists(dggrid_work_dir):
        os.makedirs(dggrid_work_dir)

    if not os.path.exists(results_path):
        os.makedirs(results_path)

    if dggrid_exec == 'julia':
        try:
            from julia.api import Julia

            jl = Julia(compiled_modules=False)
            jl.eval("using DGGRID7_jll")
            dirs = jl.eval("DGGRID7_jll.LIBPATH_list")

            # for Windows path separator is ";" and the variable is PATH
            # for linux the path separator is ":" and the variable is LD_LIBRARY_PATH
            if sys.platform.startswith('win'):

                path_update = ";".join(dirs)
                os.environ["PATH"] = os.environ["PATH"] + ";" + path_update

            else:
                path_update = ":".join(dirs)
                os.environ["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"] + ":" + path_update

            dggrid_exec = jl.eval("DGGRID7_jll.get_dggrid_path()")
        except Exception as err:
            print(err)

    if todo_command == "cells":
        gen_cells(config, cpus, results_path, dggrid_exec, dggrid_work_dir, sample_polygons)

    elif todo_command == "stats":

        counter = 0
        found_files = []

        for dggs in config['dggss']:

            stats_df_global = []
            for res in dggs['global_res']:

                params = [dggs['name'], res, dggs['proj'], None]
                d_name, res, p_name = params[0], params[1], params[2]

                name = d_name[0]
                if len(d_name) > 1:
                    name = '_'.join(d_name)

                name = f"{name}_{res}_{p_name}"
                parquet_file_name = os.path.join(results_path, f"{name}.parquet")
                parquet_file_name = f"{parquet_file_name.replace('.parquet', '_step2.parquet')}"


                if os.path.exists(parquet_file_name):
                    # print(f"{parquet_file_name} file exists, ok")
                    counter = counter + 1
                    found_files.append(parquet_file_name)

                    out_df = create_cell_stats_df([dggs['name'], res, dggs['proj'], "global"], parquet_file_name, results_path)
                    stats_df_global.append(out_df)

                else:
                    print(f"{parquet_file_name} file MISSING")

            stats_df_sample = []
            for res in dggs['sample_res']:

                params = [dggs['name'], res, dggs['proj'], None]
                d_name, res, p_name = params[0], params[1], params[2]

                for idx, row in sample_polygons.iterrows():
                    name = d_name[0]
                    if len(d_name) > 1:
                        name = '_'.join(d_name)

                    name = f"{name}_{res}_{p_name}_sample_id_{idx}"
                    parquet_file_name = os.path.join(results_path, f"{name}.parquet")
                    parquet_file_name = f"{parquet_file_name.replace('.parquet', '_step2.parquet')}"

                    if os.path.exists(parquet_file_name):
                        # print(f"{parquet_file_name} file exists, ok")
                        counter = counter + 1
                        found_files.append(parquet_file_name)

                        out_df = create_cell_stats_df([dggs['name'], res, dggs['proj'], "sample"], parquet_file_name, results_path)
                        stats_df_sample.append(out_df)

                    else:
                        print(f"{parquet_file_name} file MISSING")

            if len(stats_df_sample) > 0:
                final_stats = pd.concat([pd.concat(stats_df_global),pd.concat(stats_df_sample)])
            else:
                final_stats = pd.concat([pd.concat(stats_df_global)])


            if len(dggs['name'])>1:
                final_stats['dggs'] = dggs['name'][1]
            else:
                final_stats['dggs'] = dggs['name'][0]

            final_stats['proj'] = dggs['proj']


            if len(dggs['name']) == 1:
                name = dggs['name'][0]
            else:
                name = dggs['name'][0] + '_' + dggs['name'][1]

            res_file_name = os.path.join(results_path, default_name + f"_{name}.csv")

            final_stats.to_csv(res_file_name, index=False)

    else:
        print("no -action specified, you should be sure which one to chose")


if __name__ == "__main__":
    main()
