from __future__ import annotations

import argparse
import json
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser(description="Request an answer or approval from the current user")
    parser.add_argument("kind", choices=["question", "approval"])
    parser.add_argument("--prompt", required=True)
    parser.add_argument("--question", action="append", default=[], help="id=question text")
    parser.add_argument("--option", action="append", default=[])
    args = parser.parse_args()
    questions = []
    for value in args.question:
        identity, separator, text = value.partition("=")
        if not separator or not identity or not text:
            parser.error("--question must use id=question text")
        questions.append({"id": identity, "question": text, "options": args.option})
    body = json.dumps({"kind": args.kind, "prompt": args.prompt,
        "questions": questions, "options": args.option}).encode()
    request = urllib.request.Request("http://127.0.0.1:8080/v1/native-interactions",
        data=body, method="POST", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=86400) as response:
        print(response.read().decode())


if __name__ == "__main__":
    main()
