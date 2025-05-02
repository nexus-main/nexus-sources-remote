import asyncio
import time
import uuid
from datetime import timedelta
from logging import Logger
from typing import Any, Coroutine, Optional, cast

from apollo3zehn_package_management import ExtensionHive, PackageService
from nexus_extensibility import IDataSource
from nexus_remoting._remoting import RemoteCommunicator


class TcpClientPair:
    comm_reader: Optional[asyncio.StreamReader] = None
    comm_writer: Optional[asyncio.StreamWriter] = None
    data_reader: Optional[asyncio.StreamReader] = None
    data_writer: Optional[asyncio.StreamWriter] = None
    remote_communicator: Optional[RemoteCommunicator] = None
    watchdog_timer = time.time()
    task: Optional[asyncio.Task] = None

class AgentService:

    CLIENT_TIMEOUT = timedelta(minutes=1)

    _background_tasks = set[asyncio.Task]()
    _tcp_client_pairs: dict[uuid.UUID, TcpClientPair] = {}
    _lock = asyncio.Lock()

    def __init__(
            self, 
            extension_hive: ExtensionHive, 
            package_service: PackageService, 
            logger: Logger, 
            json_rpc_listen_address: str,
            json_rpc_listen_port: int
        ):
        
        self._extension_hive = extension_hive
        self._package_service = package_service
        self._logger = logger
        self._json_rpc_listen_address = json_rpc_listen_address
        self._json_rpc_listen_port = json_rpc_listen_port

    async def load_packages(self):

        self._logger.info("Load packages")
        package_reference_map = await self._package_service.get_all()
        await self._extension_hive.load_packages(package_reference_map)

    async def accept_clients(self):

        async def detect_and_remove_inactive_clients():

            while True:

                await asyncio.sleep(600)

                for key, pair in list(self._tcp_client_pairs.items()):

                    now = time.time()
                    watchdog_timer_elasped = timedelta(seconds=now - pair.watchdog_timer)

                    is_dead =\
                        (pair.comm_reader is None or pair.data_reader is None) and watchdog_timer_elasped >= self.CLIENT_TIMEOUT or \
                        pair.remote_communicator is not None and pair.remote_communicator.last_communication >= self.CLIENT_TIMEOUT

                    if is_dead:

                        if key in self._tcp_client_pairs:

                            # TODO: Add proper cancellation https://medium.com/nuculabs/cancellation-token-pattern-in-python-b549d894e244
                            # Otherwise sockets may stay open
                            if pair.task is not None:
                                pair.task.cancel()

                            del self._tcp_client_pairs[key]

        self._create_task(detect_and_remove_inactive_clients())
        
        server = await asyncio.start_server(
            self._handle_client, 
            host=self._json_rpc_listen_address,
            port=self._json_rpc_listen_port
        )

        self._logger.info(
            "Listening for JSON-RPC communication on %s:%d",
            self._json_rpc_listen_address,
            self._json_rpc_listen_port
        )

        async with server:
            await server.serve_forever()

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):

        # Get connection id
        buffer1 = await asyncio.wait_for(reader.readexactly(36), timeout=5)
        id_string = buffer1.decode("utf-8")

        # Get connection type
        buffer2 = await asyncio.wait_for(reader.readexactly(4), timeout=5)
        type_string = buffer2.decode("utf-8")

        try:
            id = uuid.UUID(id_string)

        except:
            writer.close()
            await writer.wait_closed()
            return

        self._logger.debug("Accept TCP client with connection ID %s and communication type %s", id_string, type_string)

        async with self._lock:

            # We got a "comm" tcp connection
            if type_string == "comm":

                if id not in self._tcp_client_pairs:
                    self._tcp_client_pairs[id] = TcpClientPair()

                self._tcp_client_pairs[id].comm_reader = reader
                self._tcp_client_pairs[id].comm_writer = writer

            # We got a "data" tcp connection
            elif type_string == "data":

                if id not in self._tcp_client_pairs:
                    self._tcp_client_pairs[id] = TcpClientPair()

                self._tcp_client_pairs[id].data_reader = reader
                self._tcp_client_pairs[id].data_writer = writer
                
            # Something went wrong, close the socket and return
            else:
                writer.close()
                await writer.wait_closed()
                return

            pair = self._tcp_client_pairs[id]

            if pair.comm_reader and \
               pair.comm_writer and \
               pair.data_reader and \
               pair.data_writer and \
               not pair.remote_communicator:

                self._logger.debug("Accept remoting client with connection ID %s", id)

                pair.remote_communicator = RemoteCommunicator(
                    pair.comm_reader,
                    pair.comm_writer,
                    pair.data_reader,
                    pair.data_writer,
                    get_data_source_type=lambda type_name: self._extension_hive.get_extension_type(type_name)
                )

                pair.task = self._create_task(pair.remote_communicator.run())

    def _create_task(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task:

        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

        return task
