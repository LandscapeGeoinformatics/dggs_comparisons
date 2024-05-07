# code supplement for DGGS geometry comparisons

Our journal article: https://www.tandfonline.com/doi/full/10.1080/20964471.2022.2094926

  Kmoch, A., Vasilyev, I., Virro, H., & Uuemaa, E. (2022). Area and shape distortions in open-source discrete global grid systems. Big Earth Data, 6(3), 256–275. https://doi.org/10.1080/20964471.2022.2094926

The supplement for data and code is on Zenodo:

[![Zenodo DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.5905935.svg)](https://doi.org/10.5281/zenodo.5905935)

Assumptions are that the respective libraries are installed with their Python bindings:

 - [Google S2](https://s2geometry.io/) and https://github.com/google/s2geometry/tree/master/src/python
 - [Uber H3](https://h3geo.org/) and https://github.com/uber/h3-py
 - [OpenEaggr DGGS](https://github.com/riskaware-ltd/open-eaggr)
 - [rHEALPix](https://github.com/manaakiwhenua/rhealpixdggs-py)
 - [DGGRID](https://www.discreteglobalgrids.org/software/) and [dggrid4py](https://github.com/allixender/dggrid4py)
 - [HEALPIX](https://healpix.jpl.nasa.gov/) and the [Healpy](https://healpy.readthedocs.io/en/latest/) python binding


## generate cells to parquet

- S2 can currently only be built on Linux (potentially also MacOS/Unix)
- OpenEAGGR and DGGRID are possible to build on Windows as well
- rHEALPix, Healpy and H3 are available as multiplatform Python Pypi packages
- the experiment was originally run on a 2-CPU Intel XEON (each 8 cores*2 threads) server with 32 GB RAM and Ubuntu 18.04.3
- the 2024 HEALPIX update was run on an Apple M2 MacBook with 12 cores and 32 GB RAM


### creates `(cell_id, wkt_geom)` for the defined DGGS types and resolutions defined in config.json

```bash
#> python generate_cells_parquet.py -action cells -config config.json -cpu 16 -dggrid $(which dggrid) -out results_gen
```


## caveats

- EAGGR doesn't seem to have a predefined logic of hierarchical cell resolutions for ISEA3H
- EAGGR doesn't seem to have a region filling algorithm available, neither for ISEA4T nor ISEA3H
- rHEALPix is pure Python (with Numpy/Scipy support), but cell generation/conversion is slower than the other C/C++ based implementations
- Healpy bundles the HEALPIX C++ library
- DGGRID is a commandline tool and can predominantly only be used to generate a grid and fill with sampling data, the Python API is only a wrapper

## future potentials

- implement high-performance API in compiled language with C-API (C, C++, Rust, Fortran or similar) for rHEALPix
- implement region coverer/filler algorithm and fixed resolutions API for OpenEAGGR
- implement module that provides access DGGRID internal APIs for cell-wise logic (like dggridR or pydggrid), but as C compatible library, build on top and add spatial analysis functions like S2 or H3
