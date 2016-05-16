#!/usr/bin/env python
"""
Post-process the individual cell output files resulting from running the
OpenIFS single column model over a grid of cells.

The program takes a single input which is a configuration file in INI
format. This is the same configuration file used to run the model that
generated the column output files.

If you wish to exclude some variables from the processing, you may place
a file named 'dropvars.txt' in the current directory and write the names
of the variables you wish to exclude in there, one per line.

The program can use shared-memory parallelism, but has no support for MPI.

"""
# Copyright 2016 Andrew Dawson
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from argparse import ArgumentParser
from collections import OrderedDict
from datetime import datetime
from functools import partial
import logging
from multiprocessing import Pool
import os.path
import shutil
import sys

import numpy as np
from scmtiles import __version__ as scmtiles_version
from scmtiles.config import SCMTilesConfig
from scmtiles.exceptions import ConfigurationError
from scmtiles.grid_manager import GridManager
import xarray as xr


#: Version number of the post-processor.
__version__ = '1.0.alpha1'


#: Names of model output files.
SCM_OUT_FILES = ('diagvar.nc', 'diagvar2.nc', 'progvar.nc')


class Error(Exception):
    """Generic exception class for program errors."""
    pass


def load_coordinate_templates(config):
    """
    Loads the x and y grid coordinates from the SCM input file.

    **Arguments:**

    * config
        An `~scmtiles.config.SCMTilesConfg` instance describing the run to
        load coordinates from.

    **Returns:**

    * coord_dict
        A mapping from coordinate name to xarray Coordinate objects.

    """
    filename = config.input_file_pattern.format(time=config.start_time)
    filepath = os.path.join(config.input_directory, filename)
    try:
        ds = xr.open_dataset(filepath)
        x_coord = ds[config.xname]
        y_coord = ds[config.yname]
        x_coord.load()
        y_coord.load()
        ds.close()
    except RuntimeError as e:
        msg = 'Failed to open input file "{}": {!s}'
        raise Error(msg.format(filepath, e))
    except ValueError:
        msg = ("Failed to extract template coordinates, check grid "
               "dimensions in configuration match those in the files")
        raise Error(msg)
    return {config.xname: x_coord, config.yname: y_coord}


def pp_cell(cell, timestamp, coordinate_templates, drop_list, config):
    """
    Post-process an individual cell.

    **Aarguments:**

    * cell
        A `~scmtiles.grid_manager.Cell` instance identifying the cell.

    * timestamp
        A string timestamp used as part of the filename for the cell output
        file.

    * coordiate_templates
        A dictionary mapping coordinate names to xarray coordinate objects, as
        returned from `load_coorindate_templates`. This is used to lookup the
        latitude and longitude of the cell from its indices.

    * config
        A `~scmtiles.config.SCMTilesConfig` instance describing the run being
        post-processed.

    **Returns:**

    * (cell_ds, filepath)
        A 2-tuple containing the cell data in an `xarray.Dataset` and the full
        path to the file the cell data were loaded from.

    """
    cell_id = 'y{:04d}x{:04d}'.format(cell.y_global, cell.x_global)
    dirname = '{}.{}'.format(timestamp, cell_id)
    dirpath = os.path.join(config.output_directory, dirname)
    filepaths = [os.path.join(dirpath, filename) for filename in SCM_OUT_FILES]
    # Load the cell dataset from file into memory, then close the input
    # file to free the file handle.
    try:
        # Work-around for problem using open_mfdataset inside a
        # multiprocessing pool where the load just waits indefinitely.
        ds_list = [xr.open_dataset(fp, drop_variables=drop_list)
                   for fp in filepaths]
        cell_ds = xr.auto_combine(ds_list)
        cell_ds.load()
        cell_ds.close()
        for ds in ds_list:
            ds.close()
    except (OSError, RuntimeError):
        msg = 'The input files "{!s}" cannot be read, do they exist?'
        raise Error(msg.format(filepaths))
    # Add scalar latitude and longitude coordinates and return the
    # modified cell dataset:
    x_value = coordinate_templates[config.xname][cell.x_global]
    y_value = coordinate_templates[config.yname][cell.y_global]
    cell_ds.coords.update({config.yname: y_value, config.xname: x_value})
    return cell_ds, dirpath


def pp_tile(config, timestamp, coordinate_templates, drop_list, tile):
    """
    Post-process a rectangular tile of cells.

    **Arguments:**

    * config
        A `~scmtiles.config.SCMTilesConfig` instance describing the run being
        post-processed.

    * timestamp
        A string timestamp used as part of the filename for the cell output
        files.

    * coordiate_templates
        A dictionary mapping coordinate names to xarray coordinate objects, as
        returned from `load_coorindate_templates`. This is used to lookup the
        latitude and longitude of the cells from their indices.

    * tile
        A `~scmtiles.grid_manager.RectangularTile` instance describing the tile
        to process.

    **Returns:**

    * (tile_ds, filepaths)
        An `xarray.Dataset` representing the tile, and a list of paths to the
        files that were loaded to form the tile.

    """
    grid_rows = OrderedDict()
    filepaths = []
    for cell in tile.cells():
        cell_ds, cell_filepath = pp_cell(cell, timestamp, coordinate_templates,
                                         drop_list, config)
        try:
            grid_rows[cell.y_global].append(cell_ds)
        except KeyError:
            grid_rows[cell.y_global] = [cell_ds]
        filepaths.append(cell_filepath)
    for key, row in grid_rows.items():
        grid_rows[key] = xr.concat(row, dim=config.xname)
    if len(grid_rows) > 1:
        tile_ds = xr.concat(grid_rows.values(), dim=config.yname)
    else:
        tile_ds, = grid_rows.values()
    logger = logging.getLogger('PP')
    logger.info('processing of tile #{} completed'.format(tile.id))
    return tile_ds, filepaths


def post_process(config_file_path, num_processes, delete_cell_files=False):
    """
    Post-process an SCMTiles model run by combining individual cell output
    files into a single file for the whole grid.

    **Arguments:**

    * config_file_path
        Th path to an `~scmtiles.config.SCMTilesConfig` instance describing
        the run to be post-processed.

    * num_processes
        The number of processes to use to do the post-processing. You can
        choose any positive integer number, although it is advised to match
        against the resources you have available. One process per processor
        is a sensible choice, but depending on your I/O performance you may
        benefit from more or fewer processes than you have CPU cores available.

    **Keyword arguments:**

    * delete_cell_files
        If `True` the files containing data for the individual cells will be
        deleted once the whole grid file has been susccessfully to disk. If
        `False` then the individual cell files will remain after processing.
        The default is `False` (no files deleted).

    """
    if num_processes < 1:
        raise Error('number of processes must be positive')
    try:
        config = SCMTilesConfig.from_file(config_file_path)
    except ConfigurationError as e:
        raise Error(e)
    tiles = GridManager(config.xsize,
                        config.ysize,
                        config.ysize).decompose_by_rows()
    # Compute static data:
    logger = logging.getLogger('PP')
    logger.info('loading coordinate templates')
    coordinate_templates = load_coordinate_templates(config)
    timestamp = config.start_time.strftime('%Y%m%d_%H%M%S')
    # Load a list of variables to drop, if one exists:
    try:
        with open('dropvars.txt', 'r') as f:
            drop_list = [var.strip() for var in f.readlines()]
    except IOError:
        # If the file doesn't exist or we can't read it then we don't care,
        # we just won't drop any variables in post-processing.
        drop_list = []
    process_pool = Pool(num_processes)
    logger.info('dispatching tiles to {} workers'.format(num_processes))
    results = process_pool.map(
        partial(pp_tile, config, timestamp, coordinate_templates, drop_list),
        tiles)
    # The final dataset is formed by concatenating all the tiles along the
    # y-grid axis.
    dataset = xr.concat(sorted([ds for ds, _ in results],
                               key=lambda ds: ds[config.yname].values.max()),
                        dim=config.yname)
    # Ensure the time dimension has with CF compliant units:
    start_time = config.start_time.strftime('%FT%T')
    base_time = np.datetime64('{}+0000'.format(start_time))  # UTC
    time_unit = 'seconds since {}'.format(start_time)
    dataset.coords['time'].values = dataset.coords['time'].values + base_time
    dataset.coords['time'].encoding = {'units': time_unit}
    # We want to serialize using a conventional coordinate order of time first,
    # level second, and grid dimensions last in the order latitude then
    # longitude. This ordering can be supplied to the dataset transpose method.
    transposed_coords = ('time',
                         'nlev',
                         'nlevp1',
                         'nlevs',
                         'norg',
                         'ntiles',
                         'ncextr',
                         config.yname,
                         config.xname)
    # Write the output to a netcdf file.
    output_filename = 'scm_out.{}.nc'.format(timestamp)
    output_filepath = os.path.join(config.output_directory, output_filename)
    logger.info('writing combined output file: {!s}'.format(output_filepath))
    try:
        dataset.transpose(*transposed_coords).to_netcdf(output_filepath)
    except RuntimeError as e:
        Error('failed write grid to disk: {!s}'.format(e))
    if delete_cell_files:
        logger.info('deleting individual column files')
        for dlist in (dl for _, dl in results):
            for dp in dlist:
                shutil.rmtree(dp)
                logger.info('deleted: {!s}'.format(dp))


def main(argv=None):
    if argv is None:
        argv = sys.argv
    # Set up command line argument parsing.
    ap = ArgumentParser(description=__doc__)
    ap.add_argument('-n', '--num-processes', type=int, default=1,
                    help="Number of processes to use.")
    ap.add_argument('-d', '--delete', action='store_true', default=False,
                    help='delete input column files after processing')
    ap.add_argument('config_file_path', type=str,
                    help='path to the program configuration file')
    # Parse the given arguments, this will handle errors gracefully and print
    # a helpful message to the screen if an error occurs.
    argns = ap.parse_args(argv[1:])
    # Initialize the logging system:
    logger = logging.getLogger('PP')
    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler(sys.stdout)
    log_handler.setLevel(logging.DEBUG)
    log_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] (%(name)s) %(levelname)s %(message)s',
        datefmt='%Y-%m%d %H:%M:%S'))
    logger.addHandler(log_handler)
    try:
        # Run the post-processor.
        logger.info('Running {} at version {}'.format(argv[0], __version__))
        logger.info('Backend scmtiles is version {}'.format(scmtiles_version))
        post_process(argns.config_file_path, argns.num_processes,
                     delete_cell_files=argns.delete)
    except Error as e:
        logger.error('error: {!s}'.format(e))
        return 1
    else:
        return 0


if __name__ == '__main__':
    sys.exit(main())
