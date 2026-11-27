"""Send signed webhook deliveries to the local stack, the way each sender signs them (#7453 live checks).

    python3 tests/tools/webhook_sender.py URL MODE SECRET [--event issues|ping] [--delivery ID] [--body FILE]
        [--bad] [--stale] [--size BYTES] [--chunked]

MODE: github | gitlab_token | gitlab_signing | custom. Prints only the status and the answer body, never the secret.
"""
import argparse
import base64
import hashlib
import hmac
import http.client
import json
import time
import urllib.parse
import uuid

SAMPLE = {"action": "opened", "issue": {"number": 1, "title": "Sample issue from the sender script"}}


def headers_for(mode, secret, body, args):
    if args.bad:
        secret = secret + "x"
    if mode == "github":
        signature = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
        return {"X-Hub-Signature-256": signature, "X-GitHub-Event": args.event,
                "X-GitHub-Delivery": args.delivery or str(uuid.uuid4())}
    if mode == "gitlab_token":
        return {"X-Gitlab-Token": secret, "X-Gitlab-Event": "Issue Hook",
                "X-Gitlab-Event-UUID": args.delivery or str(uuid.uuid4())}
    if mode == "gitlab_signing":
        key = base64.b64decode(secret.removeprefix("whsec_"))
        webhook_id = args.delivery or "msg_" + uuid.uuid4().hex
        stamp = str(int(time.time()) - (600 if args.stale else 0))
        digest = hmac.new(key, f"{webhook_id}.{stamp}.".encode() + body, hashlib.sha256).digest()
        return {"webhook-id": webhook_id, "webhook-timestamp": stamp,
                "webhook-signature": "v1," + base64.b64encode(digest).decode()}
    if mode == "custom":
        return {"X-Webhook-Token": secret}
    raise SystemExit(f"unknown mode {mode}")


def send(url, headers, body, chunked):
    parts = urllib.parse.urlsplit(url)
    conn = http.client.HTTPConnection(parts.hostname, parts.port or 80, timeout=15)
    headers = {"Content-Type": "application/json", **headers}
    started = time.monotonic()
    if chunked:
        conn.putrequest("POST", parts.path)
        for name, value in {**headers, "Transfer-Encoding": "chunked"}.items():
            conn.putheader(name, value)
        conn.endheaders()
        for start in range(0, len(body), 65536):
            chunk = body[start:start + 65536]
            conn.send(b"%x\r\n%s\r\n" % (len(chunk), chunk))
        conn.send(b"0\r\n\r\n")
    else:
        conn.request("POST", parts.path, body=body, headers=headers)
    reply = conn.getresponse()
    return reply.status, reply.read(), time.monotonic() - started, reply.getheader("Retry-After")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url")
    parser.add_argument("mode")
    parser.add_argument("secret")
    parser.add_argument("--event", default="issues")
    parser.add_argument("--delivery")
    parser.add_argument("--body")
    parser.add_argument("--bad", action="store_true")
    parser.add_argument("--stale", action="store_true")
    parser.add_argument("--size", type=int)
    parser.add_argument("--chunked", action="store_true")
    args = parser.parse_args()
    if args.body:
        body = open(args.body, "rb").read()
    elif args.size:
        body = json.dumps({"pad": "x" * args.size}).encode()
    else:
        body = json.dumps(SAMPLE).encode()
    status, answer, seconds, retry = send(args.url, headers_for(args.mode, args.secret, body, args), body, args.chunked)
    print(json.dumps({"status": status, "body": answer.decode(errors="replace")[:300], "ms": round(seconds * 1000),
                      "retry_after": retry}))


if __name__ == "__main__":
    main()
