import asyncio

from uuid import uuid4

from typing import *
import json

from datetime import datetime

import httpx

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from contextlib import asynccontextmanager

class Client:
    def __init__(self, websocket: WebSocket):
        self.id = str(uuid4())
        self.websocket = websocket
        self.stats = {
            'queue_size': 0
        }

class Ticket:
    def __init__(self, ticket_type: str, data: dict[str, Any], postback_url: str = None, meta: dict = {}, priority: int = 0):
        self.id = str(uuid4())
        self.ticket_type = ticket_type
        self.data = data
        self.postback_url = postback_url
        self.meta = meta
        self.priority = priority or 0

        self.start_time = datetime.now()

        self.status = 'pending' # pending, assigned, done, failed

        self.assigned_worker_id = None
        self.result = None

    @property
    def timed_out(self):
        return (datetime.now() - self.start_time).total_seconds() > 60

    async def wait_for_result(self):
        start_timestamp = datetime.now()

        while not self.status in ['done', 'failed']:
            # timeout after 30 seconds
            print("seconds: {}".format((datetime.now() - start_timestamp).total_seconds()))
            if (datetime.now() - start_timestamp).total_seconds() > 30:
                print("Ticket {} timed out after 30 seconds".format(self.id))

                break

            try:
                await asyncio.sleep(0.25)
            except Exception as e:
                print("Error waiting for ticket result: {}".format(e))

    def to_dict(self):
        return {
            'id': self.id,
            'type': self.ticket_type,
            'data': self.data,
            'assigned_worker_id': self.assigned_worker_id,
            'meta': self.meta
        }

class Master:
    def __init__(self):
        self.clients = []
        self.ticket_queue = []
        self.ticket_assignment_thread = None

    def start_ticket_assignment_thread(self):
        if self.ticket_assignment_thread is not None:
            self.stop_ticket_assignment_thread()

        async def ticket_assignment_thread():
            while True:
                print("Tickets in queue: {}, Clients: {}".format(len(self.ticket_queue), len(self.clients)))
                
                await asyncio.sleep(1)

                tickets_to_remove = []

                # Assign tickets to workers
                for ticket in self.ticket_queue[:]:
                    try:
                        if ticket.status == 'done':
                            # Add ticket to list of tickets to remove
                            tickets_to_remove.append(ticket)

                            continue
                        elif ticket.timed_out:
                            # Add ticket to list of tickets to remove
                            print("Ticket {} timed out after 60 seconds".format(ticket.id))

                            tickets_to_remove.append(ticket)

                            continue

                        # if not yet assigned
                        if ticket.assigned_worker_id is None:
                            # Find the worker with the smallest queue size
                            candidate_worker = None

                            for client in self.clients:
                                if type(client.stats.get('queue_size')) == int and (candidate_worker is None or client.stats['queue_size'] < candidate_worker.stats['queue_size']):
                                    candidate_worker = client

                            if candidate_worker is not None:
                                print("Candidate worker = {}".format(candidate_worker.id))

                                await self.assign_ticket(ticket, candidate_worker)
                        else:
                            # ensure the client is still connected
                            client = next(c for c in self.clients if c.id == ticket.assigned_worker_id)

                            # if the client is no longer connected, unassign the ticket
                            if client is None:
                                ticket.assigned_worker_id = None
                                ticket.status = 'pending'

                    except Exception as e:
                        print("Error assigning ticket: {}".format(e))

                for ticket in tickets_to_remove:
                    self.ticket_queue.remove(ticket)

        self.ticket_assignment_thread = asyncio.create_task(ticket_assignment_thread())

    def stop_ticket_assignment_thread(self):
        if self.ticket_assignment_thread is not None:
            self.ticket_assignment_thread.cancel()

            self.ticket_assignment_thread = None

    def add_ticket(self, ticket: Ticket):
        self.ticket_queue.append(ticket)

        # sort tickets by priority so that high numbers are first
        self.ticket_queue.sort(key=lambda t: t.priority, reverse=True)

        print("ticket queue: {}".format([(t.id, t.ticket_type, t.priority) for t in self.ticket_queue]))

        return ticket
    
    def remove_ticket(self, ticket: Ticket | str):
        if type(ticket) is str:
            self.ticket_queue = [t for t in self.ticket_queue if t.id != ticket]
        else:
            self.ticket_queue.remove(ticket)

    async def assign_ticket(self, ticket: Ticket, client: Client | str):
        client_object = client if type(client) is Client else next(c for c in self.clients if c.id == client)

        if client_object is None:
            raise Exception("Client not found")
        
        ticket.assigned_worker_id = client_object.id
        ticket.status = 'assigned'

        message = json.dumps({
            'type': 'AssignTicket',
            'ticket': ticket.to_dict()
        })

        print("sending message: {}".format(message))

        await client_object.websocket.send_text(message)

    def add_client(self, client: Client):
        print("Add client {}".format(client.id))
        
        self.clients.append(client)

    def remove_client(self, client: Client | str):
        print("Remove client {}".format(client if client is str else client.id))

        if type(client) is str:
            self.clients = [c for c in self.clients if c.id != client]
        else:
            self.clients.remove(client)
    
    async def on_message(self, client: Client, message: dict[str, Any]):
        import json

        message_type = message['type']

        if not message_type:
            raise Exception("Message type not specified")
        
        if message_type == 'WorkerStats':
            client.stats = message['stats']

            await client.websocket.send_text(json.dumps({
                'type': 'Pong'
            }))
        elif message_type == 'WorkerResult':
            print("Got worker result: {}".format(message))

            ticket_id = message['ticket_id']
            result = message['result']

            ticket = next(t for t in self.ticket_queue if t.id == ticket_id)

            if not ticket:
                raise Exception("Ticket not found with ID {}".format(ticket_id))

            print("result = {}".format(result))
            
            ticket.result = result
            ticket.status = 'done'

            # Send result to postback URL
            postback_url = ticket.postback_url

            json = {
                'results': result,
                'meta': ticket.meta if ticket.meta else { }
            }

            if postback_url is str and postback_url != '':
                try:
                    # requests.post(postback_url, json=result)
                    async with httpx.AsyncClient() as client:
                        await client.post(postback_url, json=json)
                except Exception as e:
                    print("Error posting to postback URL {}: {}".format(postback_url, e))

                    raise Exception("Error posting to postback URL {}: {}".format(postback_url, e))
        else:
            raise Exception("Unknown message type: {}".format(message_type))


master = Master()

app = FastAPI()

@app.on_event('startup')
async def init_app():
    global master

    master.start_ticket_assignment_thread()

@app.on_event('shutdown')
async def shutdown_app():
    global master

    master.stop_ticket_assignment_thread()

@app.post("/")
async def provide_completions_endpoint(input: dict[str, str], postback_url: str = None, meta: dict = {}, priority: int = 0):
    global master

    ticket_data = {
    }

    ticket = Ticket('ProvideCompletions', ticket_data, postback_url, meta, priority)
    master.add_ticket(ticket)

    # if no postback_url was provided, asynchoronously wait until the ticket has a result.
    # We wait a maximum of 30 seconds.
    if not postback_url:
        await ticket.wait_for_result()
        print("After waiting for result {}".format(ticket.result))

        if ticket.result:
            return {
                'results': ticket.result,
                'meta': ticket.meta if ticket.meta else { }
            }
        else:
            # Send timeout status code
            return {
                'status': 408,
                'message': 'Ticket timed out after 30 seconds',
                'results': []
            }

    return ticket.to_dict()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    global master

    print("Hit endpoint to join")
    
    await websocket.accept()

    client = Client(websocket)
    master.add_client(client)

    while True:
        try:
            data = await websocket.receive_text()

            as_json = json.loads(data)

            await master.on_message(client, as_json)
        except WebSocketDisconnect:
            master.remove_client(client)

            break
        except Exception as e:
            print("Error responding to message \"{}\": {}".format(data, str(e)))