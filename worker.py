from typing import *

import json
from datetime import datetime

from haystack.components.builders import ChatPromptBuilder
from haystack.components.generators.chat import OpenAIChatGenerator
from haystack.dataclasses import ChatMessage
from haystack import Pipeline
from haystack.utils import Secret

# for .env
from dotenv import load_dotenv

load_dotenv()

# no parameter init, we don't use any runtime template variables
prompt_builder = ChatPromptBuilder()
llm = OpenAIChatGenerator(api_key=Secret.from_env_var("OPENAI_API_KEY"), model="gpt-3.5-turbo")

# from haystack.document_stores import InMemoryDocumentStore
# from haystack.nodes import AnswerParser, DensePassageRetriever, TransformersQueryClassifier, QuestionGenerator, FARMReader, EmbeddingRetriever, BM25Retriever, TopPSampler, PreProcessor, PromptNode, PromptTemplate, JoinDocuments, SentenceTransformersRanker, LinkContentFetcher
# from haystack.nodes.ranker import LostInTheMiddleRanker
# from haystack.nodes.other.docs2answers import Docs2Answers
# from haystack.utils import launch_es, print_answers, fetch_archive_from_http, print_documents, print_questions
# from haystack.pipelines import Pipeline, GenerativeQAPipeline, ExtractiveQAPipeline
# from haystack.nodes.connector import Crawler

import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-m', '--master', default='localhost')
args = parser.parse_args()

completion_prompt = """
You are a helpful coding assistant that is proficient in many languages. You will guide the user by completing their code with the most suitable suggestion.
You will respond in JSON, allowing the user's coding tool to parse your response.
You have a small window of context into the surrounding code. You also are given a list of names of files the user has actively opened, as it may be helpful for you to provide accurate code suggestions.
If you need more context about a specific piece of code or a specific code file because it is relevant to the current problem you are solving, you will add that to your response as a function call.
Your JSON schema you will use is as follows:
```
{
    completionText: string, // Only the part to fill in for the cursor position (if applicable). Can be multiline. Can be blank if N/A.
    negativeLineIndex: number, // negative index for which to backspace lines from the input, if lines need to be deleted to make way for the completion.
    negativeCharacterIndex: number, // negative index for which to backspace characters from the input, if characters need to be deleted to make way for the completion.
    functionCalls?: string[] = [ 'get_file_contents("thefilename.cpp")' ]
}
```
""".rstrip()

pipe = Pipeline()
pipe.add_component("prompt_builder", prompt_builder)
pipe.add_component("llm", llm)
pipe.connect("prompt_builder.prompt", "llm.messages")

def provide_completions(input: dict[str, Any]) -> list[dict[str, Any]]:
    print("Run provide completions: {}".format(input))

    messages = [ChatMessage.from_system(completion_prompt),
                ChatMessage.from_user(json.dumps(input))]
    
    resp = pipe.run(data={"prompt_builder": {"template_variables":{}, "template": messages}})

    print("Got response: {}".format(resp))

    # return resp["llm"]["replies"][-1].content

    return [{
        "completionText": "Hello, World!",
        "negativeLineIndex": 0,
        "negativeCharacterIndex": 0
    }]

import asyncio
class WorkerQueue:
    COOLDOWN_BY_TYPE = {
        "ProvideCompletions": 1
    }

    def __init__(self):
        self.tickets = []
        self.last_execution_by_type = {}

    def add(self, ticket: dict[str, Any]):
        self.tickets.append(ticket)

    def remove(self, ticket: str):
        self.tickets = [t for t in self.tickets if t['id'] != ticket]

    @property
    def size(self):
        return len(self.tickets)
    
    def next_ticket_index(self) -> int:
        if self.size == 0:
            return -1
        
        now = datetime.now()

        for i in range(0, len(self.tickets)):
            ticket_cooldown = WorkerQueue.COOLDOWN_BY_TYPE.get(self.tickets[i]["type"], 0)
            last_execution = self.last_execution_by_type.get(self.tickets[i]["type"])

            if ticket_cooldown == 0 or last_execution is None or (now - last_execution).total_seconds() >= ticket_cooldown:
                return i
            
            print("Skipping ticket of type {}, last execution = {}, cooldown = {}".format(self.tickets[i]["type"], last_execution, ticket_cooldown))
            
        return -1
    
    def set_last_execution_time(self, ticket_type: str, time: datetime):
        self.last_execution_by_type[ticket_type] = time

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

                idx = self.queue.next_ticket_index()

                if idx == -1:
                    continue

                ticket = self.queue.tickets[idx]
                print("Working on ticket: {}".format(ticket))

                try:
                    result = None

                    self.queue.set_last_execution_time(ticket['type'], datetime.now())

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

                except Exception as e:
                    print("Error: {}".format(e))
                finally:
                    self.queue.tickets.pop(idx)

        self.work_thread = asyncio.create_task(work_thread())

    def stop_work_thread(self):
        if self.work_thread is not None:
            self.work_thread.cancel()

            self.work_thread = None

    async def assign_ticket(self, ticket: dict[str, Any]):
        self.queue.add(ticket)

    async def unassign_ticket(self, ticket: str):
        self.queue.remove(ticket)

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
        elif message_type == 'UnassignTicket':
            ticket = message_json['ticket']

            print("Unassigning ticket: {}".format(ticket))

            await self.unassign_ticket(ticket)
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

    print("Connecting to master: {}".format(args.master))

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

if __name__ == '__main__':
    print("Test")

    asyncio.run(main())