import pytest

import healpy as hp
import healpy_helper


NEST = True

def test_healpy_howto():
    # the lowest resolution possible with the HEALPix base partitioning of the sphere surface into 12 equal sized pixels.
    # Areas of all pixels at a given resolution are identical. 
    for nside in [1, 2, 4, 8]:
        npix = hp.nside2npix(nside)
        print(f"Number of pixels (nside={nside}): {npix}")

    for i in range(0, 10):
        # nside = 2**i
        nside = hp.order2nside(i)
        npix = hp.nside2npix(nside)
        print(f"Number (order={i}) of pixels (nside={nside}): {npix}")


        approx_res = hp.nside2resol(nside, arcmin=True) / 60
        print(f"Approximate resolution at NSIDE {nside} is {approx_res:.2} deg")

    # high res pixel nums:
    # 3145728 or even 12582912 pixels
    for pix in [3145728, 12582912]:
        nside = hp.npix2nside(pix)
        ord = hp.nside2order(nside)
        print(f"Number of pixels (pix={pix}): {nside} | order: {ord}")

    assert True

def test_latlon2cellid():
    lat = 59.5
    lon = 27.3
    nside = hp.order2nside(10)
    v = healpy_helper._latlon2cellid(lat, lon, nside, nest=NEST)
    # -> np.ndarray:
    print(v)
    assert 921402 == v

def test_cellid2latlon():
    cell_ids = [921402]
    nside = hp.order2nside(10)
    v = healpy_helper._cellid2latlon(cell_ids, nside, nest=NEST)
    #  -> tuple[np.ndarray, np.ndarray]:
    print(v)
    pass

def test_cellid2boundaries():
    cell_ids = [921402]
    nside = hp.order2nside(10)
    v = healpy_helper._cellid2boundaries(cell_ids, nside, nest=NEST) 
    # -> np.ndarray:
    print(v)
    pass

def test_get_healpy_cells():
    res = 10
    v = healpy_helper.get_healpy_cells(res)
    # -> np.ndarray:
    print(v)
    pass

def test_create_healpy_geometry():
    import pandas as pd
    res = 10
    df = pd.DataFrame({'cell_id': [921402]})
    v = healpy_helper.create_healpy_geometry(df, res, as_geojson=True)
    # -> np.ndarray:
    print(v)
    pass
