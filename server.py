import asyncio
import argparse
import collections
# import cProfile, pstats, io
import heapq
# from pstats import SortKey
import ssl
import signal
import functools
# import time

# Semaphore locks should use this form to prevent deadlocks:
# while semaphore == 0:
#     await asyncio.sleep(0)
client_semaphore = 0 # limit number of buffered requests from client, initialized in main

class APIConnection():
    """APIConnection class uses tokens to control connections. Only with a token, a connection can be established, disconnected, reconnected, or be used for data transfer. This also ensures connections can only be used by one request at a time. For performance reasons, stablished connections are stored in a pool for reuse. When a request needs to be sent, it will wait for an available token from ready queue and try to be sent. If anything goes wrong before receiving a response from API, the request will wait for a new token and retry the process."""
    # disable certificate check
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    chunked = '\r\n0\r\n\r\n' # ending signal for chunked transfer

    def __init__(self, connection_limit=25):
        self.connect_lock = asyncio.Lock() # establish a connection to API one at a time
        self.pool = [None for _ in range(connection_limit)] # connection pool
        self.ready_queue = collections.deque([x for x in range(connection_limit)]) # tokens to access connections
        self.ready_queue_size = connection_limit # number of tokens that are unused
        self.limit = connection_limit # max simultaneous connections to API
        self.read_buffer = 20000 # API read buffer

    async def _connect(self, token, client_writer=None):
        """Create connections to API one at a time"""
        # assert(self.pool[token] == None)
        retry_api = True
        async with self.connect_lock:
            while retry_api:
                if client_writer and client_writer.transport.is_closing():
                    self.ready_queue.append(token) # release token, token not ready
                    self.ready_queue_size = self.ready_queue_size + 1
                    print("Client connection lost. Abort API connection establishment.")
                    break
                retry_api = False
                try:
                    api_reader, api_writer = await asyncio.open_connection('localhost', 443, ssl=self.ctx)
                    self.pool[token] = [api_reader, api_writer]
                    self.ready_queue.appendleft(token) # release token, token ready
                    self.ready_queue_size = self.ready_queue_size + 1
                    print("API connection established!")
                except Exception as e:
                    print(f"Failed to connect {token} to API!")
                    print(e)
                    retry_api = True
                    await asyncio.sleep(3)

    async def _disconnect(self, token):
        """Close connection to API"""
        # print(f"closing {token}")
        api_connection = self.pool[token]
        if api_connection:
            api_writer = api_connection[1]
            self.pool[token] = None
            api_writer.close()
            print(f"API connection {token} closed.")
        else:
            print(f"API connection {token} closed already.")

    async def _reconnect(self, token):
        """Reconect to API"""
        await self._disconnect(token)
        await self._connect(token)

    async def _get_available(self):
        """Returns an available token"""
        while self.ready_queue_size == 0:
            await asyncio.sleep(0.01)
        self.ready_queue_size = self.ready_queue_size - 1
        return self.ready_queue.popleft()

    async def get(self, query: str, client_writer=None):
        """Gets an available token, tests the underlying connection, and tries to send out the request"""
        response = None
        while True:
            token = await self._get_available()
            # print(token)
            if self.pool[token] is None:
                asyncio.create_task(self._connect(token, client_writer)) # non-blocking call
                await asyncio.sleep(0.01)
                continue
            elif self.pool[token][1].transport.is_closing():
                asyncio.create_task(self._reconnect(token)) # non-blocking call
                await asyncio.sleep(0.01)
                continue

            # connection is working normally
            api_reader, api_writer = self.pool[token]
            # prevent the server from outputing too much error caused by connection failure, thus not serving new requests
            try:
                # print(f'start sending {token}')
                api_writer.write(query.encode('utf-8'))
                # print(f'finish sending {token}')
                await api_writer.drain()
                # print(f'start reading {token}')
                # get response form API, timeout is set to 3 seconds
                response = await asyncio.wait_for((api_reader.read(self.read_buffer)), timeout=3.0)
                # print(f'finish reading {token}')
                response = response.decode('utf-8')
                # print(f'{token}: {response}')
                # print(f'{token}: responded')
                if response == '':
                    # print(f'{token}: Invalid response')
                    raise Exception('Invalid response!')
                if 'Transfer-Encoding: chunked' in response and response[-7:] != self.chunked:
                    # print(f"{token} chuncked...")
                    while True:
                        followup = await api_reader.read(self.read_buffer) # '\r\n0\r\n\r\n'
                        followup = followup.decode()
                        response = response + followup
                        if followup[-7:] == self.chunked:
                            break
                        # print(f"{token} chuncked {_}")
                break
            except:
                print(f'API connection {token} interrupted: {query}')
                asyncio.create_task(self._reconnect(token))
        # print(f'put back {token}')
        self.ready_queue.appendleft(token) # release token, token ready
        self.ready_queue_size = self.ready_queue_size + 1
        # print(f'end response {token}')
        return response

    async def finish(self):
        """Close all connections to API"""
        for x in range(self.limit):
            await self._disconnect(x)


async def respond(writer, message, api_connection, response_queue, receive_cnt, transmit_control):
    """Wait for the results from proton_api and send it to the client, requests are labelled with receive_cnt and pushed into a min heap to ensure FIFO order"""
    global client_semaphore
    # writer.write("200 permit\n".encode())
    # await writer.drain()
    # client_semaphore = client_semaphore + 1
    # return

    # build smtpd request dictionary
    smtpd_request = {}
    lines = message.split('\n')
    for x in lines:
        key_val = x.split('=', 1)
        if len(key_val) == 2:
            smtpd_request[key_val[0]] = key_val[1]
        else:
            smtpd_request[key_val[0]] = ''

    query = (
        f"GET {'/api/mail/incoming/recipient?Email=' + smtpd_request['recipient']} HTTP/1.1\r\n"
        f"Host: localhost\r\n"
        f"Accept: */*\r\n"
        f"\r\n"
    )

    # make sure responses to client are FIFO
    # async with lock:
    api_response = await api_connection.get(query, writer)
    if writer.transport.is_closing():
        assert(False)
        return # client connection lost

    await asyncio.sleep(0)
    status_code = api_response[9:12] # HTTP/1.1 xxx
    # permit
    if status_code == '200' or status_code == '204':
        response = "action=OK\n\n"
    # reject
    elif status_code == '422':
        Error = api_response.split('"Error": "', 1)[1].split('"', 1)[0]
        response = "action=REJECT " + Error + "\n\n"
    elif status_code == '400':
        response = "action=REJECT Bad request\n\n"
    elif status_code == '503':
        print(api_response.split('\r\n', 1)[0])
        response = "action=DEFER Please try again later\n\n"
    else:
        try:
            Error = api_response.split('"Error": "', 1)[1].split('"', 1)[0]
            # temporary rejects
            response = "action=REJECT " + Error + "\n\n"
        except IndexError:
            # temporary rejects
            response = "action=REJECT Bad request\n\n"

    # insert an element: [incoming order, encoded response]
    heapq.heappush(response_queue, [receive_cnt, response.encode()])
    # print(response_queue)
    transmit_control[0] = transmit_control[0] + 1
    # print(f"readyt: {response_queue[0]}, expectedt: {transmit_control[1]}")

    # If response_queue is not empty and the element on top of min-heap has the smallest receive_cnt of all elements not yet sent, send the element. Repeat this step until the condition is not met.
    while transmit_control[0] != 0 and response_queue[0][0] == transmit_control[1]:
        _, leaving = heapq.heappop(response_queue)
        # assert(leaving != b'')
        transmit_control[1] = transmit_control[1] + 1
        transmit_control[0] = transmit_control[0] - 1
        # prevent the server from outputing too much error caused by connection failure, thus not serving new requests
        try:
            # reply to client
            writer.write(leaving)
            # print(f"{_}, {leaving}")
            await writer.drain()
        except:
            pass
    client_semaphore = client_semaphore + 1

async def handle_query(reader, writer, api_connection):
    """This function reads request data from the client after a connection is established. After reading one request, it will initiate the respond() function to process the request and send it back, but instead of waiting respond() to finish its task, this function will continue to read the next request immediately."""
    # enable profiler
    # pr = cProfile.Profile()
    # pr.enable()
    global client_semaphore
    tasks = []
    response_queue = [] # min heap
    transmit_control = [0, 0] # size of response_queue, transmit count
    receive_cnt = 0 # receive cnt
    while True:
        # read data from client
        try:
            data = await reader.readuntil(b'\n\n')
        except asyncio.IncompleteReadError: # Golang cannot write EOF
            break
        message = data.decode()

        # the server can handle only around 50k requests per second, creating too many tasks will slow down the asyncio scheduler
        while client_semaphore == 0:
            # sleep for 1 second have a smaller impact on performance compared to sleep for 0 seconds
            await asyncio.sleep(1)

        # reset connection when EOF reached
        if reader.at_eof():
            break
        # non blocking function call to respond()
        tasks.append(asyncio.create_task(respond(writer, message, api_connection, response_queue, receive_cnt, transmit_control)))
        client_semaphore = client_semaphore - 1
        receive_cnt = receive_cnt + 1

    # wait for the last response to be read by client
    while len(tasks) != 0:
        done, tasks = await asyncio.wait(tasks)

    # close client connection gracefully
    if writer.can_write_eof():
        writer.write_eof()
    writer.close()
    print("Client connection closed.")
    # disable profiler
    # pr.disable()
    # s = io.StringIO()
    # sortby = SortKey.CUMULATIVE
    # ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    # ps.print_stats()
    # print(s.getvalue())

async def sig_handle(api_connection, signum):
    """Clean up and close all connections"""
    all_tasks = asyncio.Task.all_tasks() # Python 3.6
    # all_tasks = asyncio.all_tasks() # Python 3.7
    for x in all_tasks: # cancel all running tasks
        x.cancel()
    await api_connection.finish()
    exit(signum)

async def main(address='127.0.0.1', port=8888, connection_limit=1):
    api_connection = APIConnection(connection_limit=connection_limit)
    server = await asyncio.start_server(lambda r, w: handle_query(r, w, api_connection), address, port)
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, functools.partial(asyncio.ensure_future, sig_handle(api_connection, signal.SIGINT)))
    loop.add_signal_handler(signal.SIGTERM, functools.partial(asyncio.ensure_future, sig_handle(api_connection, signal.SIGTERM)))

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

# get server address and port
parser = argparse.ArgumentParser(description='Specify server address and port')
parser.add_argument('address', type=str, nargs=1, help='server address')
parser.add_argument('port', type=int, nargs=1, help='server port')
parser.add_argument('max_api_con', type=int, default=25, nargs='?', help='max simultaneous connections to api, defaults to 25')
parser.add_argument('max_client_buf', type=int, default=100, nargs='?', help='max buffered client requests, defaults to 100')
# print(args)

if __name__ == '__main__':
    args = parser.parse_args()
    client_semaphore = args.max_client_buf
    asyncio.run(main(address=args.address[0], port=args.port[0], connection_limit=args.max_api_con))
