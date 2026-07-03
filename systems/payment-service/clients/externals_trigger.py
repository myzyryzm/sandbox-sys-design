#!/usr/bin/env python3
"""Client script for "externals-trigger".

This client's functions are plain Python functions defined below. The web app runs one like:

    python3 externals_trigger.py --<function> <arg1> <arg2> ...

The arguments are passed positionally, in the order the function declares them.

Make calls to the system through the load balancer with the `lb` helper (see lbclient.py):

    def checkout(order_id):
        r = lb.post("/orders-service/orders/checkout", {"order_id": order_id})
        if r.get("status") == "valid":
            lb.post("/payments-api/complete-payment", {"token": r["token"]})
        else:
            lb.post("/orders-service/orders/cancel", {"order_id": order_id})

Use real control flow — if/else, loops, and chaining one call's response into the next. `lb`
records every call so the web app can show the results and trace them on the diagram. CLI
arguments arrive as strings; coerce them (int(...), == "true", …) where a function needs a
number or boolean.
"""
import sys

from lbclient import lb


# === functions ===
# Authored functions go here. Each is a top-level `def <name>(<args>): ...` and is registered
# in FUNCTIONS below so `--<name>` can invoke it.
def trigger_processPayouts():
    # Trigger the payout provider to drain its buffered payouts. payout-api.processPayouts
    # loops its processing_payouts and fans out to payout-service-2.processPayout for each.
    return lb.post("/payout-api/process-payouts", {})


def trigger_processPayments():
    # Trigger the payments provider to drain its buffered payments. payments-api.processPayments
    # loops its processing_payments and fans out to service-1.paymentFlowStep2a for each.
    return lb.post("/payments-api/process-payment", {})
# === end functions ===


# Maps a function name to its callable. Register every function defined above.
FUNCTIONS = {
    "trigger_processPayouts": trigger_processPayouts,
    "trigger_processPayments": trigger_processPayments,
}


def main(argv):
    if not argv or not argv[0].startswith("--"):
        sys.exit("usage: python3 externals_trigger.py --<function> [args...]")
    name = argv[0][2:]
    fn = FUNCTIONS.get(name)
    if fn is None:
        sys.exit("unknown function: %s" % name)
    fn(*argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
