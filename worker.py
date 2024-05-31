from typing import *

import json

from haystack.document_stores import InMemoryDocumentStore
from haystack.nodes import AnswerParser, DensePassageRetriever, TransformersQueryClassifier, QuestionGenerator, FARMReader, EmbeddingRetriever, BM25Retriever, TopPSampler, PreProcessor, PromptNode, PromptTemplate, JoinDocuments, SentenceTransformersRanker, LinkContentFetcher
from haystack.nodes.ranker import LostInTheMiddleRanker
from haystack.nodes.other.docs2answers import Docs2Answers
from haystack.utils import launch_es, print_answers, fetch_archive_from_http, print_documents, print_questions
from haystack.pipelines import Pipeline, GenerativeQAPipeline, ExtractiveQAPipeline
from haystack.nodes.connector import Crawler

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--master', required=True)
args = parser.parse_args()


def provide_completions(input: dict[str, str]):
    print("Run provide completions: {}".format(input))

    result = []

    return result

import asyncio
class WorkerQueue:
    def __init__(self):
        self.tickets = []

    def add(self, ticket: dict[str, Any]):
        self.tickets.append(ticket)

    @property
    def size(self):
        return len(self.tickets)

class Worker:
    def __init__(self, websocket):
        self.websocket = websocket
        self.queue = WorkerQueue()
        self.stats_thread = None
        self.work_thread = None

    def start_stats_thread(self):
        if self.stats_thread is not None:
            self.stop_stats_thread()

        async def stats_thread():
            while True:
                await asyncio.sleep(1)

                await self.send_stats()

        self.stats_thread = asyncio.create_task(stats_thread())

    def stop_stats_thread(self):
        if self.stats_thread is not None:
            self.stats_thread.cancel()

            self.stats_thread = None

    def start_work_thread(self):
        if self.work_thread is not None:
            self.stop_work_thread()

        async def work_thread():
            while True:
                await asyncio.sleep(.5)

                if self.queue.size == 0:
                    continue

                idx = len(self.queue.tickets) - 1

                ticket = self.queue.tickets[idx]
                print("Working on ticket: {}".format(ticket))

                try:
                    result = None

                    if ticket['type'] == 'ProvideCompletions':
                        result = provide_completions(ticket['data'])
                    else:
                        raise Exception("Unknown ticket type: {}".format(ticket['type']))

                    # Send result
                    message = {
                        'type': 'WorkerResult',
                        'ticket_id': ticket['id'],
                        'result': result
                    }

                    await self.websocket.send(json.dumps(message))

                    self.queue.tickets.pop(idx)

                except Exception as e:
                    print("Error: {}".format(e))

        self.work_thread = asyncio.create_task(work_thread())

    def stop_work_thread(self):
        if self.work_thread is not None:
            self.work_thread.cancel()

            self.work_thread = None

    async def assign_ticket(self, ticket: dict[str, Any]):
        self.queue.add(ticket)

    async def on_message(self, message):
        print("Got message from server: {}".format(message));

        # Read message as json
        message_json = json.loads(message)
        message_type = message_json['type']

        if not message_type:
            raise Exception("Message type not specified")
        
        if message_type == 'AssignTicket':
            ticket = message_json['ticket']

            print("Got ticket: {}".format(ticket))

            await self.assign_ticket(ticket)
        elif message_type == 'Pong':
            # Ignore
            pass
        else:
            raise Exception("Unknown message type: {}".format(message_type))
        
    async def send_stats(self):
        message = {
            'type': 'WorkerStats',
            'stats': {
                'queue_size': self.queue.size
            }
        }

        await self.websocket.send(json.dumps(message))

async def main():
    import websockets
    from websockets.client import connect

    async for websocket in connect("ws://{}:8000/ws".format(args.master)):
        print("Got websocket connection: {}".format(websocket))

        worker = Worker(websocket)
        worker.start_stats_thread()
        worker.start_work_thread()

        try:
            while True:
                message = await asyncio.wait_for(websocket.recv(), timeout=10)

                await worker.on_message(message)

        except websockets.ConnectionClosed:
            print("Websocket connection closed, stopping...")

            worker.stop_stats_thread()
            worker.stop_work_thread()

            continue
        except Exception as e:
            print("Error: {}".format(e))

            worker.stop_stats_thread()
            worker.stop_work_thread()

            continue

if __name__ == "__main__":
    # # Init by running a test query
    # results = provide_completions([{ "question": "What is a good way to pre-load the required binaries for our ML models?", "answer": "I don't know, but I'm going to try this hack." }], ["good way to pre-load ml binaries"])

    # assert results[0]["answers"][0]["answer"] == "I don't know, but I'm going to try this hack."

    asyncio.run(main())