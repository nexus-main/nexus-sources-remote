import asyncio
import io
import json
import struct
import time
import typing
from datetime import datetime, timedelta
from typing import Any, Awaitable, Callable, Dict, Optional, cast
from urllib.parse import urlparse

from nexus_extensibility import (CatalogItem, DataSourceContext,
                                 ExtensibilityUtilities, IDataSource, ILogger,
                                 IUpgradableDataSource, LogLevel, NexusDataType,
                                 ReadRequest, ResourceCatalog)

from ._encoder import (JsonEncoder, JsonEncoderOptions, to_camel_case,
                       to_snake_case)
import pyarrow as pa
import pyarrow.ipc as pa_ipc

_json_encoder_options: JsonEncoderOptions = JsonEncoderOptions(
    property_name_encoder=to_camel_case,
    property_name_decoder=to_snake_case
)

#                                                                                               zfill(26) ensures leading zeros when year is < 1000
_json_encoder_options.encoders[datetime] = lambda value: value.strftime("%Y-%m-%dT%H:%M:%S.%f").zfill(26) + "0+00:00"

def _decode_nexus_data_type(type_cls: type[NexusDataType], value: Any) -> NexusDataType:
    if isinstance(value, int):
        return NexusDataType(value)

    if not isinstance(value, str):
        raise Exception(f"Unable to decode {value} into value of type NexusDataType.")

    name = value if value in NexusDataType.__members__ else to_snake_case(value).upper()
    return NexusDataType[name]

_json_encoder_options.decoders[NexusDataType] = _decode_nexus_data_type

_MAX_BATCH_STREAM_PAYLOAD_LENGTH = 4 * 1024 * 1024
_MAX_BATCH_STREAM_ERROR_MESSAGE_LENGTH = 64 * 1024

class _AsyncioArrowSink:

    def __init__(self, writer: asyncio.StreamWriter):
        self._writer = writer
        self._position = 0
        self.closed = False

    def write(self, data: bytes) -> int:
        self._writer.write(data)
        self._position += len(data)
        return len(data)

    def tell(self) -> int:
        return self._position

    def writable(self) -> bool:
        return True

    def close(self):
        self.closed = True

class _Logger(ILogger):

    _background_tasks = set[asyncio.Task]()

    def __init__(self, tcp_comm_socket: asyncio.StreamWriter):
        self._comm_writer = tcp_comm_socket

    def log(self, log_level: LogLevel, message: str):

        notification = {
            "jsonrpc": "2.0",
            "method": "log",
            "params": [log_level.name, message]
        }
        
        task = asyncio.create_task(_send_to_server(notification, self._comm_writer))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

class _ReadDataResponseBuilder:

    def __init__(self):
        self._data = bytearray()
        self.is_completed = False
        self.error_message: Optional[str] = None

    def add(self, data: bytes):
        if self.is_completed:
            raise Exception("The readData response received data after completion.")

        self._data.extend(data)

    def complete(self):
        if self.is_completed:
            raise Exception("The readData response completed more than once.")

        self.is_completed = True

    def fail(self, message: str):
        if self.is_completed:
            raise Exception("The readData response failed after completion.")

        self.error_message = message
        self.is_completed = True

    def to_bytes(self) -> bytes:
        return bytes(self._data)

class RemoteCommunicator:
    """A remote communicator."""

    _watchdog_timer = time.time()
    _logger: ILogger
    _source_type_name: str
    _data_source: IDataSource
    _frame_write_lock: asyncio.Lock

    def __init__(
        self, 
        comm_reader: asyncio.StreamReader,
        comm_writer: asyncio.StreamWriter,
        data_reader: asyncio.StreamReader, 
        data_writer: asyncio.StreamWriter,
        get_data_source_type: Callable[[str], type]
    ):
        """
        Initializes a new instance of the RemoteCommunicator.
        
            Args:
                comm_stream: The network stream for communications.
                data_stream: The network stream for data.
                get_data_source_type: A func to get a new data source instance by its type name.
        """

        self._comm_reader = comm_reader
        self._comm_writer = comm_writer
        self._data_reader = data_reader
        self._data_writer = data_writer
        self._get_data_source_type = get_data_source_type
        self._frame_write_lock = asyncio.Lock()
        self._read_data_response_read_lock = asyncio.Lock()
        self._read_data_request_id = 0
        self._read_data_responses: dict[int, _ReadDataResponseBuilder] = {}

    @property
    def last_communication(self) -> timedelta:
        now = time.time()
        return timedelta(seconds=now - self._watchdog_timer)

    async def run(self) -> Awaitable:
        """
        Starts the remoting operation.
        """

        # loop
        while (True):

            # https://www.jsonrpc.org/specification

            # get request message
            size = await self._read_size(self._comm_reader)
            json_request = await asyncio.wait_for(self._comm_reader.readexactly(size), timeout=60)

            request: Dict[str, Any] = json.loads(json_request)

            # process message
            response: Optional[Dict[str, Any]]

            if "jsonrpc" in request and request["jsonrpc"] == "2.0":

                if "id" in request:

                    try:

                        result = await self._process_invocation(request)

                        response = {
                            "result": result
                        }

                    except Exception as ex:
                        
                        response = {
                            "error": {
                                "code": -1,
                                "message": str(ex)
                            }
                        }

                else:
                    raise Exception(f"JSON-RPC 2.0 notifications are not supported.") 

            else:              
                raise Exception(f"JSON-RPC 2.0 message expected, but got something else.") 
            
            response["jsonrpc"] = "2.0"
            response["id"] = request["id"]

            # send response
            await _send_to_server(response, self._comm_writer)

    async def _process_invocation(self, request: dict[str, Any]) -> Optional[Any]:
        
        result: Optional[Any] = None

        method_name = request["method"]
        params = cast(list[Any], request["params"])

        if method_name == "initialize":

            self._source_type_name = params[0]

        elif method_name == "upgradeSourceConfiguration":

            if self._source_type_name is None:
                raise Exception("The connection must be initialized with a type before invoking other methods.")
            
            data_source_type = self._get_data_source_type(self._source_type_name)
            upgraded_configuration = params[0]

            if issubclass(data_source_type, IUpgradableDataSource):
                upgradable_data_source = cast(IUpgradableDataSource, data_source_type())
                upgraded_configuration = await upgradable_data_source.upgrade_source_configuration(params[0])

            result = upgraded_configuration

        elif method_name == "setContext":

            if self._source_type_name is None:
                raise Exception("The connection must be initialized with a type before invoking other methods.")

            raw_context = params[0]
            resource_locator_string = cast(str, raw_context["resourceLocator"]) if "resourceLocator" in raw_context else None
            resource_locator = None if resource_locator_string is None else urlparse(resource_locator_string)

            data_source_type = self._get_data_source_type(self._source_type_name)

            # TODO: Python 3.12: https://stackoverflow.com/a/78818079/1636629
            # typing.get_origin: https://stackoverflow.com/q/76494580/1636629
            data_source_base_type = next(base_type for base_type in data_source_type.__orig_bases__ if \
                issubclass(typing.get_origin(base_type) or base_type, IDataSource))
            
            configuration_type = data_source_base_type.__args__[0] # pyright: ignore

            encoded_source_configuration = raw_context["sourceConfiguration"] \
                if "sourceConfiguration" in raw_context else None

            source_configuration = JsonEncoder.decode(
                configuration_type, 
                encoded_source_configuration, 
                _json_encoder_options
            )

            request_configuration = raw_context["requestConfiguration"] \
                if "requestConfiguration" in raw_context else None

            self._logger = _Logger(self._comm_writer)

            context = DataSourceContext(
                resource_locator,
                source_configuration,
                request_configuration
            )

            self._data_source = cast(IDataSource, data_source_type())
            await self._data_source.set_context(context, self._logger)

        elif method_name == "getCatalogRegistrations":

            if self._data_source is None:
                raise Exception("The data source context must be set before invoking other methods.")

            path = cast(str, params[0])
            registrations = await self._data_source.get_catalog_registrations(path)

            result = registrations

        elif method_name == "enrichCatalog":

            if self._data_source is None:
                raise Exception("The data source context must be set before invoking other methods.")

            original_catalog = JsonEncoder().decode(ResourceCatalog, params[0])
            catalog = await self._data_source.enrich_catalog(original_catalog)
            
            result = catalog

        elif method_name == "getTimeRange":

            if self._data_source is None:
                raise Exception("The data source context must be set before invoking other methods.")

            catalog_id = params[0]
            time_range = await self._data_source.get_time_range(catalog_id)

            result = time_range

        elif method_name == "getAvailability":

            if self._data_source is None:
                raise Exception("The data source context must be set before invoking other methods.")

            catalog_id = params[0]
            begin = _json_encoder_options.decoders[datetime](datetime, params[1])
            end = _json_encoder_options.decoders[datetime](datetime, params[2])
            availability = await self._data_source.get_availability(catalog_id, begin, end)

            result = availability

        elif method_name == "read":

            if self._data_source is None:
                raise Exception("The data source context must be set before invoking other methods.")

            begin = _json_encoder_options.decoders[datetime](datetime, params[0])
            end = _json_encoder_options.decoders[datetime](datetime, params[1])
            remote_read_requests = cast(list[Any], params[2])

            if len(remote_read_requests) > 256:
                raise Exception("A remote batch read must not contain more than 256 resources.")

            read_requests: list[ReadRequest] = []
            streamed_indices: set[int] = set()
            self._read_data_responses.clear()
            schema = self._create_read_schema()
            sink = _AsyncioArrowSink(self._data_writer)
            writer = pa_ipc.new_stream(sink, schema)

            for index, remote_read_request in enumerate(remote_read_requests):
                original_resource_name = remote_read_request["originalResourceName"]
                catalog_item = JsonEncoder.decode(CatalogItem, remote_read_request["catalogItem"], _json_encoder_options)
                (data, status) = ExtensibilityUtilities.create_buffers(catalog_item.representation, begin, end)

                def _make_callback(idx: int, d: memoryview, s: memoryview, element_size: int):
                    async def on_completed():
                        await self._write_read_record_batch(writer, schema, idx, d, s, element_size)
                        streamed_indices.add(idx)
                    return on_completed

                on_completed = _make_callback(index, data, status, catalog_item.representation.element_size)
                read_requests.append(ReadRequest(original_resource_name, catalog_item, data, status, on_completed))

            await self._data_source.read(
                begin,
                end,
                read_requests,
                self._handle_read_data,
                self._handle_report_progress)

            for i in range(len(read_requests)):
                if i not in streamed_indices:
                    read_request = read_requests[i]
                    element_size = read_request.catalog_item.representation.element_size
                    await self._write_read_record_batch(writer, schema, i, read_request.data, read_request.status, element_size)
                    streamed_indices.add(i)

            writer.close()
            await self._data_writer.drain()

        # Add cancellation support?
        # https://github.com/microsoft/vs-streamjsonrpc/blob/main/doc/sendrequest.md#cancellation
        # https://github.com/Microsoft/language-server-protocol/blob/main/versions/protocol-2-x.md#cancelRequest
        elif method_name == "$/cancelRequest":
            pass

        # Add progress support?
        # https://github.com/microsoft/vs-streamjsonrpc/blob/main/doc/progresssupport.md
        elif method_name == "$/progress":
            pass

        # Add OOB stream support?
        # https://github.com/microsoft/vs-streamjsonrpc/blob/main/doc/oob_streams.md

        else:
            raise Exception(f"Unknown method '{method_name}'.")

        return result

    def _create_read_schema(self) -> pa.Schema:
        return pa.schema([
            pa.field("resourceIndex", pa.int32(), nullable=False),
            pa.field("offset", pa.int64(), nullable=False),
            pa.field("data", pa.binary(), nullable=False),
            pa.field("status", pa.binary(), nullable=False)
        ])

    async def _write_read_record_batch(
        self,
        writer: pa_ipc.RecordBatchStreamWriter,
        schema: pa.Schema,
        index: int,
        data: memoryview,
        status: memoryview,
        element_size: int
    ):
        async with self._frame_write_lock:
            writer.write_batch(self._create_read_record_batch(schema, index, 0, data, status, element_size))
            await self._data_writer.drain()

    def _create_read_record_batch(
        self,
        schema: pa.Schema,
        index: int,
        offset: int,
        data: memoryview,
        status: memoryview,
        element_size: int
    ) -> pa.RecordBatch:
        if len(data) % element_size != 0:
            raise Exception("The remote read data buffer length is not a multiple of the representation element size.")

        element_count = len(data) // element_size

        if len(status) != element_count:
            raise Exception("The remote read data and status buffers have different element counts.")

        return pa.record_batch([
            pa.array([index], type=pa.int32()),
            pa.array([offset], type=pa.int64()),
            pa.array([bytes(data)], type=pa.binary()),
            pa.array([bytes(status)], type=pa.binary())
        ], schema=schema)

    async def _handle_read_data(self, resource_path: str, begin: datetime, end: datetime) -> memoryview:

        self._logger.log(LogLevel.Debug, f"Read resource path {resource_path} from Nexus")
        self._read_data_request_id += 1
        request_id = self._read_data_request_id

        read_data_request = {
            "jsonrpc": "2.0",
            "method": "readData",
            "params": [
                request_id,
                resource_path, 
                begin, 
                end
            ]
        }

        await _send_to_server(read_data_request, self._comm_writer)

        data = await self._read_read_data_response(request_id)

        # 'cast' is required because of https://github.com/python/cpython/issues/126012
        # see also https://github.com/nexus-main/nexus/issues/184
        return cast(memoryview, memoryview(data).cast("d"))

    async def _read_read_data_response(self, request_id: int) -> bytes:
        async with self._read_data_response_read_lock:
            while True:
                completed_response = self._read_data_responses.get(request_id)

                if completed_response is not None and completed_response.is_completed:
                    del self._read_data_responses[request_id]

                    if completed_response.error_message is not None:
                        raise Exception(completed_response.error_message)

                    return self._read_read_data_arrow_response(completed_response.to_bytes())

                frame_type_bytes = await asyncio.wait_for(self._data_reader.readexactly(1), timeout=60)
                frame_type = struct.unpack("B", frame_type_bytes)[0]

                request_id_bytes = await asyncio.wait_for(self._data_reader.readexactly(4), timeout=60)
                frame_request_id = struct.unpack("<i", request_id_bytes)[0]
                builder = self._get_read_data_response_builder(frame_request_id)

                if frame_type == 0x01:
                    payload_length_bytes = await asyncio.wait_for(self._data_reader.readexactly(4), timeout=60)
                    payload_length = struct.unpack("<i", payload_length_bytes)[0]

                    if payload_length <= 0 or payload_length > _MAX_BATCH_STREAM_PAYLOAD_LENGTH:
                        raise Exception("The readData response returned an invalid payload length.")

                    payload = await asyncio.wait_for(self._data_reader.readexactly(payload_length), timeout=600)
                    builder.add(payload)

                elif frame_type == 0x02:
                    message_length_bytes = await asyncio.wait_for(self._data_reader.readexactly(4), timeout=60)
                    message_length = struct.unpack("<i", message_length_bytes)[0]

                    if message_length < 0 or message_length > _MAX_BATCH_STREAM_ERROR_MESSAGE_LENGTH:
                        raise Exception("The readData response returned an invalid error message length.")

                    message_bytes = await asyncio.wait_for(self._data_reader.readexactly(message_length), timeout=60)
                    builder.fail(message_bytes.decode("utf-8"))

                elif frame_type == 0x03:
                    builder.complete()

                else:
                    raise Exception(f"Unknown readData response frame type '{frame_type}'.")

    def _get_read_data_response_builder(self, request_id: int) -> _ReadDataResponseBuilder:
        builder = self._read_data_responses.get(request_id)

        if builder is None:
            builder = _ReadDataResponseBuilder()
            self._read_data_responses[request_id] = builder

        return builder

    def _read_read_data_arrow_response(self, data: bytes) -> bytes:
        reader = pa_ipc.open_stream(io.BytesIO(data))

        if len(reader.schema) != 2 or \
            reader.schema.field(0).name != "offset" or not pa.types.is_int64(reader.schema.field(0).type) or \
            reader.schema.field(1).name != "values" or not pa.types.is_list(reader.schema.field(1).type) or \
            not pa.types.is_float64(reader.schema.field(1).type.value_type):
            raise Exception("The readData response returned an invalid Arrow schema.")

        result = bytearray()

        for batch in reader:
            offset_array = batch.column(0)
            values_array = batch.column(1)

            for row_index in range(batch.num_rows):
                offset = offset_array[row_index].as_py()
                values = values_array[row_index].as_py()

                if offset is None:
                    raise Exception("The readData response returned a null offset.")

                if offset < 0 or offset != len(result) // 8:
                    raise Exception("The readData response returned values outside the requested range.")

                for value in values:
                    result.extend(struct.pack("<d", value))

        return bytes(result)

    def _handle_report_progress(self, progress_value: float):
        pass # not implemented

    async def _read_size(self, reader: asyncio.StreamReader) -> int:

        size_buffer = await asyncio.wait_for(reader.readexactly(4), timeout=60)
        size = struct.unpack(">I", size_buffer)[0]

        return size

async def _send_to_server(message: Any, writer: asyncio.StreamWriter):

    encoded = JsonEncoder.encode(message, _json_encoder_options)
    json_response = json.dumps(encoded)
    encoded_response = json_response.encode()

    writer.write(struct.pack(">I", len(encoded_response)))
    writer.write(encoded_response)

    await writer.drain()
