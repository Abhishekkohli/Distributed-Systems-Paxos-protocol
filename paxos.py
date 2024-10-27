import requests
import asyncio
from fastapi import FastAPI
import time
import psycopg2
from models import Transaction

class Paxos:
    ballot = 0
    total_no_servers : int = 0
    majority:int
    def __init__(self, server_id,server_instance,user_input):
        self.server_id = server_id
        self.accept_val = None
        self.promises = []
        self.leader = None
        self.log = [] # Local logs of a server
        self.promised_ballot = -1  # Ballot number for which the server has made a promise
        self.accepted_count= 0
        self.server_instance = server_instance
        self.user_input = user_input
        self.retry_queue = []

    def get_next_ballot(self):
        connection = psycopg2.connect(
            database="banka",
            user="postgres",
            password="abhishek1999@",
            host="localhost",
            port="5432"
        )
        cursor = connection.cursor()
        
        # Fetch and increment the current ballot number
        cursor.execute("SELECT current_ballot FROM public.ballot_number WHERE id=1 FOR UPDATE")
        current_ballot = cursor.fetchone()[0]
        new_ballot = current_ballot + 1
        cursor.execute(f"UPDATE public.ballot_number SET current_ballot={new_ballot} WHERE id=1")
        
        connection.commit()
        cursor.close()
        connection.close()
        
        return new_ballot

    # Prepare phase (Leader election)
    async def prepare(self):
        Paxos.ballot = self.get_next_ballot()
        self.promises = []
        prepare_msg = {"type": "PREPARE", "ballot_num": Paxos.ballot, "sender_id": self.server_id , "value":None,"log":None}
        print(f"Prepare message sent on {Paxos.ballot}")
        await self.server_instance.broadcast(prepare_msg)

    async def handle_prepare(self, msg):
        ballot_num = msg["ballot_num"]
        print(f"Prepare message received on {ballot_num}")
        if ballot_num > self.server_instance.paxos.promised_ballot:
            self.server_instance.paxos.promised_ballot = ballot_num
            log = []
            for a in self.server_instance.paxos.log:
                local_log = a.log.copy()
                local_log.append(a.server_id_db)
                local_log.append(a.transaction_id)
                log.append(local_log)
                a.sent_in_promise = True
            promise_msg = {
                "type": "PROMISE",
                "ballot_num": ballot_num,
                "value": self.server_instance.paxos.accept_val,
                "log": log,
                "sender_id": self.server_id
            }
            print(f"Promise message sent on {ballot_num}")
            await self.server_instance.send(msg["sender_id"], promise_msg)

    async def handle_promise(self, msg):
        ballot_num = msg["ballot_num"]
        print(f"Promise message received on {ballot_num}")
        self.server_instance.paxos.promises.append(msg)
        major_block = None
        time.sleep(3)
        if len(self.server_instance.paxos.promises) >= Paxos.majority:
            # Received majority of promises, now propose a major transaction block
            if self.server_instance.paxos.promises[0]["value"] is not None:
                major_block = self.server_instance.paxos.promises[0]["value"]
            await self.propose_major_block(major_block,ballot_num)

    async def propose_major_block(self,major_block,ballot_num):
        if major_block is None:
            major_block = self.create_major_transaction_block()
        accept_msg = {
            "type": "ACCEPT",
            "ballot_num": ballot_num,
            "value": major_block,
            "sender_id": self.server_id,
            "log":None
        }
        print(f"Accept message sent on {ballot_num}")
        await self.server_instance.broadcast(accept_msg)

    def create_major_transaction_block(self):
        combined_log = []
        for promise in self.server_instance.paxos.promises:
            combined_log.extend(promise["log"])
        #Appending the local logs of its own (i.e. leader)
        for temp in self.server_instance.paxos.log:
            local_log = temp.log.copy()
            local_log.append(temp.server_id_db)
            local_log.append(temp.transaction_id)
            combined_log.append(local_log)
            temp.sent_in_promise = True
        return combined_log

    # Handle Accept phase
    async def handle_accept(self, msg):
        print(f"Accept message received on {msg["ballot_num"]}")
        if msg["ballot_num"] >= self.server_instance.paxos.promised_ballot:
            self.server_instance.paxos.accept_val = msg["value"]
            self.server_instance.paxos.promised_ballot = msg["ballot_num"]
            accepted_msg = {
                "type": "ACCEPTED",
                "ballot_num": msg["ballot_num"],
                "value": msg["value"],
                "sender_id": self.server_id,
                "log":None
            }
            print(f"Accepted message sent on {msg["ballot_num"]}")
            await self.server_instance.send(msg["sender_id"], accepted_msg)

    async def handle_accepted(self, msg):
        print(f"Accepted message received on {msg["ballot_num"]}")
        self.server_instance.paxos.accepted_count += 1
        if self.server_instance.paxos.accepted_count >= Paxos.majority:
            # Majority of ACCEPTED messages received, we can decide the block
            # Commit the transaction block and persist it
            self.server_instance.paxos.accept_val = msg["value"]
            print(f"Decided on the block: {msg["value"]}")
            commit_msg = {
                "type": "COMMIT",
                "value": None,
                "sender": self.server_id,
                "ballot_num":msg["ballot_num"],
                "log":None
            }
            print(f"Commit message sent on {msg["ballot_num"]}")
            await self.server_instance.broadcast(commit_msg)

    async def handle_commit(self,msg):
        print(f"Commit message received on {msg["ballot_num"]}")
        # Persist the block on this server
        print(f"Datastore object is: {self.server_instance.datastore}")
        self.server_instance.datastore.commit(self.server_instance.paxos.accept_val)
        self.server_instance.paxos.accept_val = None
        # Delrting the local logs of a server that are now committed
        for temp in self.server_instance.paxos.log:
            if temp.sent_in_promise:
                self.server_instance.paxos.log.remove(temp)
        self.update_balance()
        print(f"Received decision and committed block to the datastore: {msg['value']}")
        # Retry any pending transactions from the retry queue
        self.retry_failed_transactions()

    def update_balance(self):
        logs = self.server_instance.datastore.get_datastore("where read= false")
        print(f"Datastore logs are: {logs}")
        print(f"server's id: {self.server_id}")
        print(f"server's user input: {self.server_instance.user_input}")
        for log in logs:
            print(f"Calculating log: {log}")
            if log[1] == self.server_id and log[3] == self.server_instance.user_input:
                print(f"Balance updating: {log[2]}")
                self.server_instance.balance += log[2]
        print(f"Balance updated after commit is:{self.server_instance.balance}")

    def retry_failed_transactions(self):
        print(f"failed transactions: {self.retry_queue}")
        for transaction in self.server_instance.paxos.retry_queue[:]:
            print(f"Retrying transaction: {transaction}")
            #await self.server_instance.process_transaction(transaction[0],transaction[1],transaction[2])
            #await self.process_transaction(S,R,amt)
            if self.server_instance.balance >= transaction[2]:
            # Case (1): Local transaction processing
                self.server_instance.balance -= transaction[2]
                print(f"Transaction success: {transaction[0]} -> {transaction[1]} with {transaction[2]}")
                Transaction.id += 1
                transaction_obj = Transaction(log = [transaction[0],transaction[1],transaction[2]],server_id_db=self.get_server_id_db(self.server_id),transaction_id=Transaction.id)
                self.server_instance.paxos.log.append(transaction_obj)
                self.server_instance.paxos.retry_queue.remove(transaction)
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
