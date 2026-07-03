import os
import json
import time
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
from prometheus_client import Counter, start_http_server
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

CAPTURED = Counter('cdc_events_captured_total', 'changes captured', ['table', 'op'])
PRODUCED = Counter('cdc_events_produced_total', 'events produced', ['topic'])
ERRORS = Counter('cdc_errors_total', 'cdc errors')

# Rules come from the mounted /cdc.json — never hardcode the list.
rules = json.load(open('/cdc.json'))['rules']

# table -> list of {ops:set, stream, topic}
routes = {}
for r in rules:
    routes.setdefault(r['table'], []).append(
        {'ops': set(r['operations']), 'stream': r['stream'], 'topic': r['topic']})

producers = {}  # stream -> KafkaProducer


def producer_for(stream):
    """One producer per distinct stream; ensure its topics exist (auto-create is off)."""
    if stream not in producers:
        try:
            admin = KafkaAdminClient(bootstrap_servers=f'{stream}:9092')
            for r in rules:
                if r['stream'] == stream:
                    try:
                        admin.create_topics([NewTopic(r['topic'], 1, 1)])
                    except Exception:
                        pass  # already exists
            admin.close()
        except Exception:
            pass  # broker not ready yet; producer connect below retries
        producers[stream] = KafkaProducer(
            bootstrap_servers=f'{stream}:9092',
            value_serializer=lambda v: json.dumps(v).encode())
    return producers[stream]


start_http_server(8000)

slot = os.environ['CDC_PG_SLOT']


def connect():
    """Connect to postgres for logical replication, retrying until it's ready."""
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
        except Exception:
            ERRORS.inc()
            time.sleep(2)


conn = connect()
cur = conn.cursor()
try:
    cur.create_replication_slot(slot, output_plugin='test_decoding')
except psycopg2.errors.DuplicateObject:
    conn.rollback()


def on_msg(msg):
    # e.g. "table public.refund: INSERT: id[integer]:1 amount[numeric]:5.00 ..."
    payload = msg.payload
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
    except Exception:
        ERRORS.inc()
    # Always advance the slot so WAL is released.
    msg.cursor.send_feedback(flush_lsn=msg.data_start)


cur.start_replication(slot_name=slot, decode=True)
cur.consume_stream(on_msg)
