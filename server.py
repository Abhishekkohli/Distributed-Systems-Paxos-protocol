from fastapi import FastAPI
from database import DataBase
from paxos import Paxos
import httpx
from typing import List, Optional
from pydantic import BaseModel
from models import Transaction
import asyncio

class Message(BaseModel):
    type : str
    sender_id: str
    sender_port : int
    ballot_num: int
    value: Optional[str] = None
    log: Optional[List[str|int]] = None

class Server:
    servers : list
    other_servers:list
    servers_instance:dict
    def __init__(self, server_id,user_id):
        self.server_id = server_id
        self.port = self.get_port(server_id)
        self.datastore = DataBase(server_id,user_id)
        self.paxos = Paxos(server_id,self,user_id)
        self.balance = 100
        self.log = []
        self.user_input = user_id


    def get_port(self, id):
        # Mapping server IDs to ports
        port_map = {
            'S1': 8010,
            'S2': 8011,
            'S3': 8015,
            'S4': 8016,
            'S5': 8017
        }
        return port_map.get(id)
    
    def get_server_id_db(self, server_id):
        # Mapping server IDs to ports
        port_map = {
            'S1': 1,
            'S2': 2,
            'S3': 3,
            'S4': 4,
            'S5': 5
        }
        return port_map.get(server_id)

    async def process_transaction(self, S, R, amt):
        Paxos.total_no_servers = len(Server.servers)
        Paxos.majority = Paxos.total_no_servers  - 1
        if self.balance >= amt:
            # Case (1): Local transaction processing
            self.balance -= amt
            print(f"Transaction success: {S} -> {R} with {amt}")
            Transaction.id += 1
            transaction = Transaction(log = [S,R,amt],server_id_db=self.get_server_id_db(self.server_id),transaction_id=Transaction.id)
            self.paxos.log.append(transaction)
        else:
            # Case (2): Initiate modified Paxos if balance is insufficient
            print(f"Initiating Paxos for insufficient balance on {S}")
            self.paxos.retry_queue.append([S,R,amt])
            print(f"failed transactions: {self.paxos.retry_queue}")
            #await self.paxos.run_consensus(S, R, amt)
            await self.paxos.prepare()

    async def send(self, destination_server_id, message: dict):
        """Send message to another server."""
        message['receiver_id']=destination_server_id
        destination_port = self.get_port(destination_server_id)
        url = f"http://127.0.0.1:{destination_port}/receive"
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.post(url, json=message)
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting {exc.request.url!r}.")
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error occurred: {exc.response.status_code} - {exc.response.text}")
        except httpx.TimeoutException:
            print(f"Request to {url} timed out.")

    async def broadcast(self, message: dict):
        """Broadcast message to all servers."""
        for server_id in Server.servers:
            if message["type"] != "COMMIT":
                if server_id != self.server_id:
                    await self.send(server_id, message)
            else:
               await self.send(server_id, message) 

    def print_balance(self):
        print(f"Local balance of client {self.server_id} is {self.balance}")

    def print_log(self):
        print(f"Local Logs of client {self.server_id}:")
        all_logs = []
        for local_log in self.paxos.log:
            all_logs.append(local_log.log)
        print(all_logs)
        
    async def print_db(self):
        print(f"Datastore of client {self.server_id} is:")
        print(self.datastore.get_datastore())
