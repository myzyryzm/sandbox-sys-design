workers = []  # llm workers
conversation_worker_dict = dict() # map of conversation id to worker
q = Queue()
def find_available_worker():
    # admission control: is there a worker with KV-cache room?
    for worker in workers:
        if worker.has_space_for_prompt():
            return worker
    return None   # all full -> backpressure

def scheduler():
    while True:
        # drain as much as we can this tick
        while True:
            if q.is_empty():
                break                      # nothing waiting

            worker = find_available_worker()
            if worker is None:
                break                      # all workers full -> leave prompts in queue

            prompt = q.read()              # only read once we KNOW we can place it
            worker.add_prompt(prompt)      # local call now, RPC later

        await asyncio.sleep(0.01)          # yield, then check again