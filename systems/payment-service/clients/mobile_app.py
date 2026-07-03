#!/usr/bin/env python3
"""Client script for "mobile-app".

This client's functions are plain Python functions defined below. The web app runs one like:

    python3 mobile_app.py --<function> <arg1> <arg2> ...

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
import random
import sys

from lbclient import lb


# === functions ===
# Authored functions go here. Each is a top-level `def <name>(<args>): ...` and is registered
# in FUNCTIONS below so `--<name>` can invoke it.
def checkout(order_id):
    # Register a payment attempt for this order via orders-service.checkout. When the
    # attempt comes back with a provider token AND actually reached register_success,
    # complete the payment against the PSP using a stand-in random card number.
    r = lb.post("/orders-service/checkout", {"order_id": order_id})
    token = r.get("token")
    if token and r.get("status") == "register_success":
        card_number = "".join(random.choice("0123456789") for _ in range(16))
        lb.post("/payments-api/complete-payment", {"token": token, "card_number": card_number})
    return r


def refund(order_id):
    # Ask the payment flow to refund this order. service-1 looks up the order's
    # payment_success OrderPayment and reverses the payin (or short-circuits if the
    # order is already refunded / fully fulfilled). The request carries only order_id.
    return lb.post("/service-1/payment-flow/refund", {"order_id": order_id})


def shipOrder(order_id):
    # Shipping the order triggers the payin fulfillment leg (step 2b): service-1 looks up
    # the order's payment_success OrderPayment and atomically posts the step2b_fufilled
    # Transaction + escrow/seller/platform LedgerEntries (and the Payout if it's ready).
    # The request carries only order_id.
    return lb.post("/service-1/payment-flow/2b", {"order_id": order_id})
# === end functions ===


# Maps a function name to its callable. Register every function defined above.
FUNCTIONS = {
    "checkout": checkout,
    "refund": refund,
    "shipOrder": shipOrder,
}


def main(argv):
    if not argv or not argv[0].startswith("--"):
        sys.exit("usage: python3 mobile_app.py --<function> [args...]")
    name = argv[0][2:]
    fn = FUNCTIONS.get(name)
    if fn is None:
        sys.exit("unknown function: %s" % name)
    fn(*argv[1:])


if __name__ == "__main__":
    main(sys.argv[1:])
