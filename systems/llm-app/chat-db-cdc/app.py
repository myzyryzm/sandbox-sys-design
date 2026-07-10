import os, json, time
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from prometheus_client import Counter, start_http_server
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

CAPTURED = Counter('cdc_events_captured_total', 'changes captured', ['table', 'op'])
PRODUCED = Counter('cdc_events_produced_total', 'events produced', ['topic'])
ERRORS = Counter('cdc_errors_total', 'cdc errors')

rules = json.load(open('/cdc.json'))['rules']
# table -> list of {ops:set, stream, topic}
routes = {}
for r in rules:
    routes.setdefault(r['table'], []).append(
        {'ops': set(r['operations']), 'stream': r['stream'], 'topic': r['topic']})

producers = {}  # stream -> KafkaProducer


def producer_for(stream):
    if stream not in producers:
        try:
            admin = KafkaAdminClient(bootstrap_servers=f'{stream}:9092')
            for r in rules:
                if r['stream'] == stream:
                    try:
                        admin.create_topics([NewTopic(r['topic'], 1, 1)])
                    except Exception:
                        pass
            admin.close()
        except Exception:
            pass
        producers[stream] = KafkaProducer(
            bootstrap_servers=f'{stream}:9092',
            value_serializer=lambda v: json.dumps(v).encode(),
            retries=5)
    return producers[stream]


def connect_replication():
    while True:
        try:
            conn = psycopg2.connect(
                host=os.environ['CDC_DB_HOST'],
                port=int(os.environ.get('CDC_DB_PORT', 5432)),
                dbname=os.environ['CDC_DB_NAME'],
                user=os.environ['CDC_DB_USER'],
                password=os.environ['CDC_DB_PASSWORD'],
                connection_factory=LogicalReplicationConnection)
            return conn
        except Exception as e:
            print(f'[cdc] waiting for postgres: {e}', flush=True)
            time.sleep(2)


def on_msg(msg):
    payload = msg.payload  # e.g. "table public.user_message: INSERT: id[integer]:1 ..."
    try:
        if payload.startswith('table '):
            rest = payload[len('table '):]
            ident, after = rest.split(':', 1)
            table = ident.split('.', 1)[1].strip().strip('"')
            op = after.strip().split(':', 1)[0].strip()  # INSERT | UPDATE | DELETE
            for route in routes.get(table, []):
                if op in route['ops']:
                    CAPTURED.labels(table, op).inc()
                    producer_for(route['stream']).send(
                        route['topic'], {'table': table, 'op': op, 'raw': payload})
                    PRODUCED.labels(route['topic']).inc()
    except Exception as e:
        print(f'[cdc] error handling change: {e}', flush=True)
        ERRORS.inc()
    msg.cursor.send_feedback(flush_lsn=msg.data_start)


def main():
    start_http_server(8000)
    slot = os.environ['CDC_PG_SLOT']
    conn = connect_replication()
    cur = conn.cursor()
    try:
        cur.create_replication_slot(slot, output_plugin='test_decoding')
        print(f'[cdc] created replication slot {slot}', flush=True)
    except psycopg2.errors.DuplicateObject:
        conn.rollback()
        print(f'[cdc] replication slot {slot} already exists', flush=True)

    cur.start_replication(slot_name=slot, decode=True)
    print(f'[cdc] streaming changes; routes: {list(routes)}', flush=True)
    cur.consume_stream(on_msg)


if __name__ == '__main__':
    main()
