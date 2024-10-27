import psycopg2
class DataBase:
    server_to_client = {'S1':'a','S2':'b','S3':'c','S4':'d','S5':'e'}
    def __init__(self, server_id,user_id):
        self.server_id = server_id
        self.user_id = user_id

    def connect_db(self,server_id):
        client_id = self.server_to_client[server_id]
        return psycopg2.connect(
            database="bank" + client_id,
            user="postgres",
            password="abhishek1999@",
            host="localhost",
            port="5432"
        )

    def commit(self,common_logs):
        try:
            print(f"All logs are: {common_logs}")
            self.connection = self.connect_db(self.server_id)
            cursor = self.connection.cursor()
            for x in common_logs:
                x.append(self.user_id)
            common_logs = str(list(tuple(x) for x in common_logs))
            common_logs = common_logs.replace('[','').replace(']','')       
            cursor.execute(f"INSERT INTO public.commontransactions (sender, receiver, amount,server_id,transaction_id,setid) VALUES {common_logs} ON CONFLICT (server_id,transaction_id) DO NOTHING") # Inserting all the records in one go
            self.connection.commit()
            print(f"Transaction logged: {common_logs}")
        except Exception as ex:
            print("Exception in committing transactions to the datastore",ex.args)
        finally:
            cursor.close()
            self.connection.close()

    def get_datastore(self,where_cond = ""):
        try:
            self.connection = self.connect_db(self.server_id)
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT sender,receiver,amount,setid FROM public.commontransactions {where_cond}")
            output = cursor.fetchall()
            cursor.execute(f"UPDATE public.commontransactions SET read = true")
            self.connection.commit()
            return output
        except Exception as ex:
            print("Exception in committing transactions to the datastore",ex.args)
        finally:
            cursor.close()
            self.connection.close()

