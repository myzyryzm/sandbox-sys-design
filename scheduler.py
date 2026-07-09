workers = [] # all workers assigned to 
chat_worker_dict = dict() # map of chat id to worker to enable prefix caching
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
        break                      
    worker.add_prompt(user_message)
    q.commit()

# this is going to read from the user-messages-stream. above is psuedocode implementation. each instance of this service will have maximum 2 workers assigned to it.