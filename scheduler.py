
""" 
workers = [] # all workers assigned to it
chat_worker_dict = dict() # map of chat id to worker to enable prefix caching

sharedFunction: find_available_worker(user_message) =>
if user_message.chat in chat_worker_dict:
    worker = chat_worker_dict[user_message.chat]
    if worker.get_status() is True return worker
else:
    for worker in workers:
        if worker.get_status() is True return worker
return None 

service reads from stream 
if no message then return 
else find_available_worker(user_message)
if no available worker then return
else call worker.add_prompt and commit
"""