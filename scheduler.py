workers = []
chat_worker_dict = dict() # map of chat id to worker
q = Kafka()
def find_available_worker(user_message):
    if user_message.chat in chat_worker_dict:
        worker = chat_worker_dict[user_message.chat]
        if worker.get_status() is True:
            return worker
    else:
        for worker in workers:
            if worker.get_status() is True:
                return worker
    return None 

def scheduler():
    user_message = q[0]
    
    worker = find_available_worker(user_message)
    if worker is None:
        break                      # all workers full -> leave prompts in queue
    worker.add_prompt(user_message)      # local call now, RPC later
    q.commit()

# this is going to read from the user-messages-stream 