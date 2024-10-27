from fastapi import FastAPI, BackgroundTasks
from server import Server
import pandas as pd
import time
from pydantic import BaseModel
from paxos import Paxos
import os

class UserInput(BaseModel):
    user_input: int

servers = ['S1', 'S2', 'S3', 'S4', 'S5']
#servers_instance = {}

start_time: float
end_time: float
no_of_transactions: int

# Initialize servers for each client
port_map = {
    8010: 'S1',
    8011: 'S2',
    8015: 'S3',
    8016: 'S4',
    8017: 'S5'
}

# Retrieve the port from environment variables or command-line arguments
port = int(os.getenv('PORT', 8010))
server_id = port_map.get(port)
print(f"Server_id:{server_id}")

# Initialize a single server instance per process
servers_instance = {server_id: Server(server_id,-1)}
Server.servers_instance = servers_instance

app = FastAPI()

# Load the transactions from the input file
file_path = 'C:\\Users\\abkohli\\OneDrive - Stony Brook University\\Documents\\Academics\\Fall 24\\Distributed Systems\\project1\\apaxos-Abhishek-Kohli-27\\lab1_Test.csv'
data = pd.read_csv(file_path, header=None)

def retrieve_rows_based_on_input(data, user_input):
    start_retrieval = False
    results = []

    for index, row in data.iterrows():
        if not pd.isna(row[0]) and row[0] == float(user_input):
            start_retrieval = True

        if start_retrieval:
            if not pd.isna(row[0]) and row[0] != float(user_input):
                break

            results.append((row[1], row[2]))

    return results

async def process_transactions(user_input):
    servers_instance[server_id].datastore.user_id = user_input
    servers_instance[server_id].user_input = user_input
    global no_of_transactions, start_time, end_time
    no_of_transactions = 0

    retrieved_data = retrieve_rows_based_on_input(data, user_input)

    start_time = time.time()

    for i, (col2, col3) in enumerate(retrieved_data):
        print(f"Row {i+1} - Column 2: {col2}, Column 3: {col3}")
        col2 = col2.strip('() ').split(',')

        if isinstance(col3, str):
            col3 = col3.strip('[] ').split(', ')
            Server.servers = col3

        # Process the transactions
        S, R, amt = col2[0].strip(), col2[1].strip(), int(col2[2])
        if S == server_id:
            await servers_instance[S].process_transaction(S, R, amt)
            no_of_transactions += 1

    end_time = time.time()
    #print("All transactions processed. Printing the system status now")
    #system_status()

@app.post("/process")
async def handle_process_request(user_input: UserInput, background_tasks: BackgroundTasks):
    if user_input.user_input == "-1":
        return {"message": "Termination signal received"}
    print("abc")
    background_tasks.add_task(process_transactions, user_input.user_input)
    return {"message": "Transaction processing started"}

# FastAPI routes to receive and handle Paxos messages
@app.post("/receive")
async def receive_message(msg: dict):
    server_instance = Server.servers_instance.get(msg["receiver_id"])
    paxos = Paxos(server_id=msg["receiver_id"], server_instance=server_instance,user_input=-1)
    if msg["type"] == "PREPARE":
        await paxos.handle_prepare(msg)
    elif msg["type"] == "PROMISE":
        await paxos.handle_promise(msg)
    elif msg["type"] == "ACCEPT":
        await paxos.handle_accept(msg)
    elif msg["type"] == "ACCEPTED":
        await paxos.handle_accepted(msg)
    elif msg["type"] == "COMMIT":
        print(f"Commit server id {paxos.server_id}")
        await paxos.handle_commit(msg)
    return {"status": "message received"}

@app.post("/status")
async def system_status():
    servers_instance[server_id].print_balance()
    servers_instance[server_id].print_log()
    await servers_instance[server_id].print_db()
    print("Performace of this server")
    try:
        total_time_latency = end_time - start_time
        latency = total_time_latency / no_of_transactions
        print({"throughput": 1/latency, "latency": latency})
    except Exception as e:
        print({"throughput": 0, "latency": 0})