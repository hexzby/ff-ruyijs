"""Run Doubao's live page in ruyijs and verify a complete chat SSE response."""
import argparse
from collections import Counter
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from time import monotonic
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[2]
if (ROOT / "dist/js.exe").is_file() or os.environ.get("RUYIJS_SHELL"):
    sys.path.insert(0, str(ROOT / "python"))
from ruyijs import Context, JSError

URL = "https://www.doubao.com/chat/?from_logout=1"
EDITOR = "document.querySelector('[contenteditable=true]')"
SEND = "document.querySelector('[data-testid=chat_input_send_button]')"
ENVIRONMENT = {
    "navigator": {"language": "en-US", "languages": ["en-US", "en"], "hardwareConcurrency": 24},
    "screen": {"width": 2560, "height": 1440, "availWidth": 2560, "availHeight": 1392},
    "viewport": {"width": 1280, "height": 955, "devicePixelRatio": 1},
    "locale": {"timezone": "Asia/Shanghai"},
}

# Observe a clone; the page receives the original Promise and Response.
OBSERVE = """(() => {
  const original = window.fetch;
  window.__ruyijsDoubaoEvidence = [];
  window.fetch = function(...args) {
    const promise = Reflect.apply(original, this, args);
    if (new URL(String(args[0]?.url || args[0]), location.href).pathname === '/chat/completion') {
      promise.then(response => {
        const record = {url: response.url, status: response.status,
          headers: Array.from(response.headers), body: null};
        window.__ruyijsDoubaoEvidence.push(record);
        response.clone().text().then(text => record.body = text,
          error => record.error = String(error));
      }).catch(error => window.__ruyijsDoubaoEvidence.push({error: String(error)}));
    }
    return promise;
  };
})()"""

def sse_events(text):
    events = []
    for block in text.replace("\r\n", "\n").split("\n\n"):
        event, data = {}, []
        for line in block.splitlines():
            key, sep, value = line.partition(":")
            if not sep or not key:
                continue
            value = value.removeprefix(" ")
            if key == "data":
                data.append(value)
            else:
                event[key] = value
        if "event" in event:
            event["data"] = json.loads("\n".join(data)) if data else {}
            events.append(event)
    return events

def verify(response, request, prompt):
    events = sse_events(response["body"])
    counts = Counter(e["event"] for e in events)
    ends = [e["data"] for e in events if e["event"] == "SSE_REPLY_END"]
    end_types = sorted({e.get("end_type") for e in ends if isinstance(e.get("end_type"), int)})
    answer = next((e.get("msg_finish_attr", {}).get("brief", "") for e in ends
                   if e.get("end_type") == 1), "")
    echoes = [e["data"].get("message", {}) for e in events if e["event"] == "FULL_MSG_NOTIFY"]
    echoed = ["".join(b.get("content", {}).get("text_block", {}).get("text", "")
                     for b in m.get("content_block", [])) for m in echoes if m.get("user_type") == 1]
    headers = {k.lower(): v for k, v in response.get("headers", [])}
    checks = {
        "http_200": response.get("status") == 200,
        "event_stream": "text/event-stream" in headers.get("content-type", ""),
        "native_post_200": request.get("method") == "POST" and request.get("status") == 200,
        "request_response_url_match": request.get("url") == response.get("url"),
        "acknowledged": counts["SSE_ACK"] > 0,
        "prompt_echoed": prompt in echoed,
        "answer_present": bool(answer.strip()),
        "all_reply_end_types": end_types == [1, 2, 3],
        "no_error_events": not any("ERROR" in e["event"].upper() for e in events),
    }
    return {"success": all(checks.values()), "checks": checks, "answer": answer,
            "events": dict(counts), "reply_end_types": end_types,
            "response_bytes": len(response["body"].encode("utf-8")),
            "response_sha256": hashlib.sha256(response["body"].encode("utf-8")).hexdigest()}

def write_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def run(args, report):
    deadline = monotonic() + args.timeout
    with Context(timeout=min(args.timeout, 60), proxy=args.proxy, environment=ENVIRONMENT) as context:
        report["shell"] = str(context.shell)
        report["shell_sha256"] = hashlib.sha256(context.shell.read_bytes()).hexdigest()

        def advance():
            if monotonic() >= deadline:
                raise TimeoutError("Doubao workflow deadline exceeded")
            try:
                context.advance(250, max_tasks=1000)
            except JSError as error:
                if len(report["script_errors"]) < 20:
                    report["script_errors"].append({"message": str(error), "stack": error.stack})

        try:
            context.navigate(URL)
            print("Page loaded; waiting for editor", flush=True)
            while not context.eval(f"!!({EDITOR}?.editor)"):
                advance()
            context.eval(OBSERVE)
            context.set("__ruyijsDoubaoPrompt", args.prompt)
            # Use the site's existing Tiptap command; no Selection/API replacement.
            inserted = context.eval(f"""(() => {{const el = {EDITOR}; el.blur();
              if (el.editor.getText()) throw new Error('Expected an empty editor');
              return el.editor.commands.insertContent({{type:'text',text:__ruyijsDoubaoPrompt}});
            }})()""")
            if not inserted:
                raise RuntimeError("The page editor rejected the text")
            while not context.eval(f"!!({SEND}) && !({SEND}).disabled"):
                advance()
            context.click(SEND)
            print("Message submitted; waiting for complete SSE", flush=True)
            while True:
                advance()
                responses = context.eval("JSON.stringify(window.__ruyijsDoubaoEvidence)")
                responses = json.loads(responses)
                if responses and responses[-1].get("error"):
                    raise RuntimeError(responses[-1]["error"])
                if responses and responses[-1].get("body") is not None:
                    break
            response = responses[-1]
            requests = [r for r in context.net_log() if r.get("url") == response["url"]
                        and r.get("method") == "POST"]
            request = requests[-1] if requests else {}
            report.update(verify(response, request, args.prompt))
            query = parse_qs(urlsplit(response["url"]).query)
            report["request"] = {"method": request.get("method"), "path": "/chat/completion",
                "status": request.get("status"), "cookie_names": request.get("cookieNames", []),
                "query_keys": sorted(query), "body_bytes": len(request.get("requestBody", "").encode("utf-8"))}
            write_json(args.output.with_suffix(".request.json"), request)
            write_json(args.output.with_suffix(".response.json"), response)
            args.output.with_suffix(".sse").write_text(response["body"], encoding="utf-8")
        finally:
            if not context.closed:
                report["console"] = context.console_log()

def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prompt", default="你好，你是谁")
    network = parser.add_mutually_exclusive_group()
    network.add_argument("--proxy", help="HTTP(S) proxy URL")
    network.add_argument("--direct", action="store_true", help="Direct connection (default)")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path, default=Path("doubao_result.json"))
    args = parser.parse_args(argv)
    if not args.prompt.strip() or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("prompt must be nonempty and timeout must be positive and finite")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    report = {"success": False, "prompt": args.prompt, "script_errors": [],
              "input_method": "page Tiptap insertContent + native Context.click",
              "response_observer": "fetch wrapper retaining original Promise; reads Response.clone()"}
    start = monotonic()
    try:
        run(args, report)
    except Exception as error:
        report["error"] = str(error)
    report["elapsed_seconds"] = round(monotonic() - start, 3)
    write_json(args.output, report)
    print(json.dumps({k: report.get(k) for k in ("success", "answer", "error", "elapsed_seconds")}, ensure_ascii=True))
    print(f"Report: {args.output.resolve()}")
    return 0 if report["success"] else 1

if __name__ == "__main__":
    raise SystemExit(main())
