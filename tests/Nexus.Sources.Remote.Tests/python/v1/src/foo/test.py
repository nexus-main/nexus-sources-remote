from abc import abstractmethod
import asyncio
import glob
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from nexus_extensibility import (CatalogRegistration, CatalogTimeRange,
                                 DataSourceContext, IDataSource,
                                 IUpgradableDataSource, LogLevel,
                                 NexusDataType, ReadDataHandler, ReadRequest,
                                 Representation, ResourceBuilder,
                                 ResourceCatalog, ResourceCatalogBuilder)


@dataclass(frozen=True)
class TestSettings():
    log_message: str

class TestBase(IUpgradableDataSource):

    @abstractmethod
    async def upgrade_source_configuration(self, configuration: Any) -> Any:
        configuration["foo"] = configuration["logMessage"]

class Test(TestBase, IDataSource[TestSettings]):
    
    _root: str

    async def upgrade_source_configuration(self, configuration: Any) -> Any:

        await super().upgrade_source_configuration(configuration)
        configuration["bar"] = configuration["logMessage"]
        
        return configuration

    async def set_context(self, context, logger):
        
        self._context: DataSourceContext[TestSettings] = context

        if (context.resource_locator is None or context.resource_locator.path is None):
            raise Exception(f"No resource locator provided.")

        if (context.resource_locator.scheme != "file"):
            raise Exception(f"Expected 'file' URI scheme, but got '{context.resource_locator.scheme}'.")

        self._root = context.resource_locator.path

        if self._root.startswith("//"):
            self._root = self._root[1:]

        logger.log(LogLevel.Information, self._context.source_configuration.log_message)

    async def get_catalog_registrations(self, path: str):

        if path == "/":
            return [
                CatalogRegistration("/A/B/C", "Test catalog /A/B/C."),
                CatalogRegistration("/D/E/F", "Test catalog /D/E/F.")
            ]

        else:
            return []

    async def enrich_catalog(self, catalog: ResourceCatalog):

        # TODO: return catalog.merge(new_catalog)

        if (catalog.id == "/A/B/C"):

            representation = Representation(NexusDataType.INT64, timedelta(seconds=1))

            resource1 = ResourceBuilder("resource1") \
                .with_unit("°C") \
                .with_groups(["group1"]) \
                .add_representation(representation) \
                .build()

            representation = Representation(NexusDataType.FLOAT64, timedelta(seconds=1))

            resource2 = ResourceBuilder("resource2") \
                .with_unit("bar") \
                .with_groups(["group2"]) \
                .add_representation(representation) \
                .build()

            catalog = ResourceCatalogBuilder("/A/B/C") \
                .with_property("a", "b") \
                .with_property("c", 1) \
                .add_resources([resource1, resource2]) \
                .build()

        elif (catalog.id == "/D/E/F"):

            representation = Representation(NexusDataType.FLOAT64, timedelta(seconds=1))

            resource = ResourceBuilder("resource1") \
                .with_unit("m/s") \
                .with_groups(["group1"]) \
                .add_representation(representation) \
                .build()

            resource2 = ResourceBuilder("resource2") \
                .with_unit("m/s") \
                .with_groups(["group1"]) \
                .add_representation(representation) \
                .build()

            catalog = ResourceCatalogBuilder("/D/E/F") \
                .add_resources([resource, resource2]) \
                .build()

        else:
            raise Exception("Unknown catalog identifier.")

        return catalog

    async def get_time_range(self, catalog_id: str):

        if catalog_id != "/A/B/C":
            raise Exception("Unknown catalog identifier.")

        file_paths = glob.glob(self._root + "/**/*.dat", recursive=True)
        file_names = [os.path.basename(file_path) for file_path in file_paths]
        date_times = sorted([datetime.strptime(fileName, '%Y-%m-%d_%H-%M-%S.dat') for fileName in file_names])
        begin = date_times[0].replace(tzinfo = timezone.utc)
        end = date_times[-1].replace(tzinfo = timezone.utc)

        return CatalogTimeRange(begin, end)

    async def get_availability(self, catalog_id: str, begin: datetime, end: datetime):

        if catalog_id != "/A/B/C":
            raise Exception("Unknown catalog identifier.")

        period_per_file = timedelta(minutes = 10)
        max_file_count = (end - begin).total_seconds() / period_per_file.total_seconds()
        file_paths = glob.glob(self._root + "/**/*.dat", recursive=True)
        file_names = [os.path.basename(file_path) for file_path in file_paths]
        date_times = [datetime.strptime(fileName, "%Y-%m-%d_%H-%M-%S.dat").replace(tzinfo=timezone.utc) for fileName in file_names]
        filtered_date_times = [current for current in date_times if current >= begin and current < end]
        actual_file_count = len(filtered_date_times)
        
        return actual_file_count / max_file_count

    def read(self, 
        begin: datetime, 
        end: datetime,
        requests: list[ReadRequest], 
        read_data: ReadDataHandler,
        report_progress: Callable[[float], None]):

        if (requests[0].catalog_item.catalog.id == "/A/B/C"):
            return self._read_local_files(begin, end, requests, report_progress)

        else:
            return self._read_and_modify_nexus_data(begin, end, requests, read_data, report_progress)

    async def _read_local_files(
        self, 
        begin: datetime, 
        end: datetime,
        requests: list[ReadRequest], 
        report_progress: Callable[[float], None]):

        # ############################################################################
        # Warning! This is a simplified implementation and not generally applicable! #
        # ############################################################################

        # The test files are located in a folder hierarchy where each has a length of 
        # 10-minutes (600 s):
        # ...
        # TESTDATA/2020-01/2020-01-02/2020-01-02_00-00-00.dat
        # TESTDATA/2020-01/2020-01-02/2020-01-02_00-10-00.dat
        # ...
        # The data itself is made up of progressing timestamps (unix time represented 
        # stored as 8 byte little-endian integers) with a sample rate of 1 Hz.
        for request in requests:
        
            samples_per_second = int(1 / request.catalog_item.representation.sample_period.total_seconds())
            seconds_per_file = 600
            type_size = 8

            # go
            current_begin = begin

            while current_begin < end:

                # find files
                search_pattern = self._root + \
                    f"/{current_begin.strftime('%Y-%m')}/{current_begin.strftime('%Y-%m-%d')}/*.dat"

                file_paths = glob.glob(search_pattern, recursive=True)

                # for each file found in target folder
                for file_path in file_paths:

                    fileName = os.path.basename(file_path)

                    file_begin = datetime \
                        .strptime(fileName, '%Y-%m-%d_%H-%M-%S.dat') \
                        .replace(tzinfo=timezone.utc)
                    
                    # if file date/time is within the limits
                    if file_begin >= current_begin and file_begin < end:

                        # compute target offset and file length
                        target_offset = int((file_begin - begin).total_seconds()) * samples_per_second
                        file_length = samples_per_second * seconds_per_file

                        # load and copy binary data
                        with open(file_path, "rb") as file:
                            file_data = file.read()

                        for i in range(0, file_length * type_size):    
                            request.data[(target_offset * type_size) + i] = file_data[i]

                        # set status to 1 for all written data
                        for i in range(0, file_length):    
                            request.status[target_offset + i] = 1

                current_begin += timedelta(days = 1)

            await request.complete()

    async def _read_and_modify_nexus_data(
        self, 
        begin: datetime, 
        end: datetime,
        requests: list[ReadRequest],
        read_data: ReadDataHandler,
        report_progress: Callable[[float], None]):
        
        async def handle_request(request: ReadRequest):
            data_from_nexus = await read_data("/need/more/data/1_s", begin, end)
            double_data = request.data.cast("d")

            for i in range(0, len(double_data)):
                double_data[i] = data_from_nexus[i] * 2

            for i in range(0, len(request.status)):
                request.status[i] = 1

            request.status[0] = len(requests)

            await request.complete()

        await asyncio.gather(*(handle_request(request) for request in requests))
