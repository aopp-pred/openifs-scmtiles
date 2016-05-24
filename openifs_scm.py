"""A TileRunner class for runnning the OpenIFS SCM over a tile."""
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

from datetime import datetime
import os
from os.path import join as pjoin
import shlex
import shutil
import subprocess

import numpy as np
import xarray as xr

from scmtiles.exceptions import TileRunError
from scmtiles.runner import CellResult, TileResult, TileRunner


# Patch NetCDF4DataStore from xray so that it will make the dimension named
# 'time' an unlimited dimension when saving to netCDF.
def _patched_set_dimension(self, dim, length):
    if dim == 'time':
        length = None
    self.ds.createDimension(dim, size=length)
xr.backends.netCDF4_.NetCDF4DataStore.set_dimension = _patched_set_dimension


class SCMError(TileRunError):
    """An error setting up or running the SCM."""
    pass


class SCMTileRunner(TileRunner):
    """Object in charge of running the SCM over a tile in serial."""

    __version__ = '1.0.alpha1'

    def __init__(self, config, tile, tile_in_memory=True,
                 archive_failed_runs=False):
        """
        Create a tile runner instance.

        **Arguments:**

        * config: `scmtiles.config.SCMTilesConfig`
            A configuration object defining the parameters for the tile.

        * tile: `scmtiles.grid_manager.Tile`
            A tile defining the work required of the runner.

        **Optional arguments:**

        * tile_in_memory
            If `True` then the data for the tile will be loaded from
            file into memory at initialization time. If `False` then
            data from the file will be read as needed from the file.
            Defaults to `True`.

        * archive_failed_runs
            If `False` then the run directories of runs that fail will
            be deleted. If `True` then any run that failes will have its
            entire run directory archive in the output directory as
            'failed.YYYYMMDD_HHMMSS.yNNNNxMMMM'. The default is `False`
            (delete all run directories regardless of run status).

        """
        # Initialize the parent class.
        super().__init__(config, tile, tile_in_memory=tile_in_memory)
        # Store extra instance variables.
        self.archive_failed_runs = archive_failed_runs
        # Modify the representation of time for OpenIFS compatibility.
        self._time_to_ifs()

    def run_cell(self, cell, logger):
        """
        Run a single OpenIFS SCM over a given cell.

        """
        try:
            # Build the SCM file structure if a working directory.
            run_directory = self.setup_openifs_scm(cell)
            # Run the SCM.
            self.run_openifs_scm(cell, run_directory)
            # Archive the results.
            output_files = self.archive_results(cell, run_directory)
            cell_result = CellResult(cell, output_files)
            logger.info('Run completed successfully for cell: '
                        '{!s}.'.format(cell))
            run_successful = True
        except (SCMError, TileRunError) as e:
            msg = 'Run failed for cell: {!s} ({!s}).'
            logger.error(msg.format(cell, e))
            cell_result = CellResult(cell, None)
            run_successful = False
        finally:
            if run_successful or not self.archive_failed_runs:
                try:
                    shutil.rmtree(run_directory)
                except UnboundLocalError:
                    pass
        return cell_result

    def setup_openifs_scm(self, cell):
        """
        Set-up for an OpenIFS SCM run.

        **Argument:**

        * cell
            An `scmtiles.grid_manager.Cell` instance representing the
            grid cell a model run should be set-up for.

        **Returns:**

        * run_directory
            Path to the directory the model run was set-up in.

        """
        run_directory = self.create_run_directory()
        self.link_template(run_directory)
        self.write_scm_input(cell, run_directory)
        return run_directory

    def run_openifs_scm(self, cell, run_directory):
        """
        Run the OpenIFS SCM in a directory where it has been set up.

        **Argument:**

        * run_directory
            Path to the directory the model should be run in.

        """
        try:
            command_result = self._scm_runner(run_directory)
            if command_result.returncode != 0:
                msg = 'SCM exited with non-zero status [{}].'
                raise SCMError(msg.format(exit_code))
            self._check_for_run_failures(run_directory)
        except SCMError:
            if self.archive_failed_runs:
                try:
                    # Dump the captured stdout and stderr (if any were
                    # captured) in the run directory:
                    with open(pjoin(run_directory, 'stdout.txt'), 'wb') as f:
                        f.write(command_result.stdout)
                    with open(pjoin(run_directory, 'stderr.txt'), 'wb') as f:
                        f.write(command_result.stderr)
                except UnboundLocalError:
                    # This will happen when command_result wasn't set due to
                    # an exception, in which case there is no stdout or stderr
                    # to write.
                    pass
                # Copy the whole run directory to the output directory:
                date_id = self.config.start_time.strftime('%Y%m%d_%H%M%S')
                cell_id = 'y{:04d}x{:04d}'.format(cell.y_global, cell.x_global)
                archive_name = 'failed.{}.{}'.format(date_id, cell_id)
                archive_directory = pjoin(self.config.output_directory,
                                          archive_name)
                try:
                    shutil.move(run_directory, archive_directory)
                except (PermissionError, FileNotFoundError):
                    pass
            # Re-raise the original exception.
            raise

    def archive_results(self, cell, run_directory):
        """
        Archive the results of an OpenIFS SCM run.

        **Arguments:**

        * cell
            The `scmtiles.grid_manager.Cell` instance representing the
            grid cell to archive.

        * run_directory
            Path to the directory the model was run in.

        **Returns:**

        * archived
            A list of paths to the archived files.

        """
        archive_files = ('diagvar.nc', 'diagvar2.nc', 'progvar.nc')
        date_id = self.config.start_time.strftime('%Y%m%d_%H%M%S')
        cell_id = 'y{:04d}x{:04d}'.format(cell.y_global, cell.x_global)
        archived = []
        for af in archive_files:
            source = pjoin(run_directory, af)
            af_name, af_ext = os.path.splitext(af)
            target_name = '{}.{}.{}{}'.format(af_name, date_id,
                                              cell_id, af_ext)
            target = pjoin(self.config.output_directory, target_name)
            try:
                shutil.move(source, target)
            except PermissionError:
                msg = 'Cannot archive data to "{}", permission denied.'
                raise SCMError(msg.format(self.config.output_directory))
            except FileNotFoundError:
                msg = 'Cannot archive data to "{}", it may not exist.'
                raise SCMError(msg.format(self.config.output_directory))
            archived.append(target)
        return archived

    def write_scm_input(self, cell, run_directory):
        """
        Write the input file for an OpenIFS SCM model run in the run
        directory.

        **Arguments:**

        * cell
            The `scmtiles.grid_manager.Cell` instance representing the
            grid cell to generate input for.

        * run_directory
            Path to the directory the model is run in.

        **Returns:**

        * status
            The path to the written input file.

        """
        scm_input_file_path = pjoin(run_directory, 'scm_in.nc')
        cell_ds = self.get_cell(cell)
        cell_ds['time'].encoding = {'units': 'seconds'}
        cell_ds.to_netcdf(scm_input_file_path, format='NETCDF3_CLASSIC')
        return scm_input_file_path

    def _scm_runner(self, run_directory):
        """
        Run the OpenIFS SCM model process in the run directory.

        **Argument:**

        * run_directory
            Path to the directory the model is run in. This model
            executable must already be present in the run directory.

        **Returns:**

        * status
            The exit code of the model process.

        """
        command = shlex.split('./master1c.exe')
        try:
            command_result = subprocess.run(command, cwd=run_directory,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE)
        except FileNotFoundError:
            msg = ('Cannot locate executable master1c.exe in the template '
                   'directory: {}')
            raise SCMError(msg.format(self.config.template_directory))
        except OSError:
            msg = ('Failed to run the executable master1c.exe, check the '
                   'program to ensure it is working')
            raise SCMError(msg)
        except PermissionError:
            msg = ('Cannot execute the master1c.exe program, check the file '
                   'has the exectuable bit set in: {}')
            raise SCMError(msg.format(self.config.template_directory))
        return command_result

    def _check_for_run_failures(self, run_directory):
        """
        Verify that the model process wrote the required files and that
        the files are not empty.

        **Argument:**

        * run_directory
            Path to the directory the model is run in. This model
            executable must already be present in the run directory.

        """
        expected_files = ('onecol.r', 'progvar.nc', 'diagvar.nc',
                          'diagvar2.nc')
        for file_name in expected_files:
            f_path = pjoin(run_directory, file_name)
            # Check if each required file exists, and that it is not empty.
            if not os.path.exists(f_path) or os.stat(f_path).st_size == 0:
                msg = ('SCM run did not complete correctly, '
                       '"{}" missing or empty.')
                raise SCMError(msg.format(f_path))

    def _time_to_ifs(self):
        """
        Convert a CF-compliant time coordinate into the special form
        required by the OpenIFS SCM.

        The OpenIFS SCM requires time to be specified by 3 coordinate
        variables 'time', 'date' and 'second':

        * The 'date' coordinate contains the date part of the forecast
          reference time encoded as an integer of the form YYYYMMDD.

        * The 'second' coordinate contains the seconds part of the reference
          time, encoded as an integer number of seconds since the date given
          by 'date'.

        * The 'time' coordinate contains the valid time of the forcing
          encoded as seconds since the reference time.

        Both 'date' and 'second' should be identical for all time points in
        a forcing set.

        """
        number_times = len(self.tile_ds.coords['time'])
        reference_time = self.config.start_time
        reference_date = datetime(reference_time.year, reference_time.month,
                                  reference_time.day)
        # Create the 'date' coordinate containing the date component of the
        # reference time.
        date_values = [int(reference_date.strftime('%Y%m%d'))] * number_times
        date_coord = xr.Coordinate('date', date_values)
        # Create the 'second' coordinate containing the seconds component of
        # the reference time.
        second_values = np.array(
            [(reference_time - reference_date).seconds] * number_times,
            dtype='<m8[s]')
        second_coord = xr.Coordinate('second', second_values)
        # Create the new 'time' coordinate containing the number of seconds
        # from the reference time. The times are first rounded to the
        # nearest second since OpenIFS cannot handle fractional seconds.
        time_ns = self.tile_ds['time'].values.astype('datetime64[ns]')
        time_int = time_ns.astype('int')
        time_rounded = np.round(time_int / 1.e9)
        time_rs = time_rounded.astype('datetime64[s]')
        time_values = time_rs - time_rs[0]
        time_coord = xr.Coordinate('time', time_values)
        # Update the dataset with the new coordinates.
        self.tile_ds.coords.update({'time': time_coord,
                                    'date': ('time', date_coord),
                                    'second': ('time', second_coord)})
        # Correctly set the encoding for time and second coordinates.
        self.tile_ds.coords['time'].encoding = {'units': 'seconds'}
        self.tile_ds.coords['second'].encoding = {'units': 'seconds'}
        # Set other attributes of the coordinate variables.
        self.tile_ds['date'].attrs = {'units': 'yyyymmdd',
                                      'long_name': 'Date'}
        self.tile_ds['second'].attrs = {'long_name': 'Second'}
        self.tile_ds['time'].attrs = {'long_name': 'Time'}
