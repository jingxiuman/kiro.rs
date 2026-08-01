#!/usr/bin/env python3
"""缓存分摊验证：新增尾部是大 tool_result 时，creation 必须非零。

判据来源：同 payload 重放测不出这个缺陷（重放时前缀确实全部命中，
比例看着是对的）。必须让本轮新增的尾部是一段从未发送过的大 tool_result，
才能暴露「指纹碰撞 + token 漏计 → 新内容被错记成 cache_read」。

两条判据缺一不可：
  1. turn2 的 creation 必须非零——那段正文进了历史（成为可缓存前缀），
     从未发送过就不可能是 read。
  2. turn1 的尾部必须落在 input 而非 creation——turn1 的最后一条是当前轮
     新输入，按设计不切段、不进 cache。只查判据 1 会放过这类错误。

用法：
    KIRO_DATA_DIR=/path/to/data TAG=v1 python3 tools/verify_cache_split.py

每次运行用不同的 TAG 生成不同正文，避免命中上一次运行留下的缓存。
KIRO_DATA_DIR 默认 ../data（相对仓库根），KIRO_URL 默认 127.0.0.1:19095。
退出码 0=PASS，1=FAIL，便于挂进回归流程。
"""
import json
import os
import subprocess
import sys

DATA_DIR = os.environ.get("KIRO_DATA_DIR", "../data")
URL = os.environ.get("KIRO_URL", "http://127.0.0.1:19095") + "/v1/messages"


def body(tag):
    return "".join("fn %s_%d() { let x = %d; }\n" % (tag, i, i) for i in range(1200))


def load_key():
    path = os.path.join(DATA_DIR, "client_api_keys.json")
    try:
        with open(path) as f:
            entries = json.load(f)
    except OSError as e:
        sys.exit("无法读取 %s: %s（用 KIRO_DATA_DIR 指定 data 目录）" % (path, e))
    for e in entries:
        if not e.get("disabled") and not e.get("isSystem"):
            return e["key"]
    sys.exit("no usable client key in " + path)


def call(key, msgs, tools):
    payload = {"model": "claude-sonnet-4.5", "max_tokens": 48,
               "stream": False, "messages": msgs, "tools": tools}
    with open("/tmp/verify_payload.json", "w") as f:
        json.dump(payload, f)
    out = subprocess.run(
        ["curl", "-s", "-m", "180", URL,
         "-H", "x-api-key: " + key, "-H", "content-type: application/json",
         "-d", "@/tmp/verify_payload.json"],
        capture_output=True, text=True).stdout
    try:
        return json.loads(out).get("usage", {})
    except json.JSONDecodeError:
        sys.exit("upstream returned non-JSON: " + out[:300])


def main():
    tag = os.environ.get("TAG", "v1")
    key = load_key()
    tools = [{"name": "read_file", "description": "Read",
              "input_schema": {"type": "object",
                               "properties": {"path": {"type": "string"}}}}]
    a, b = tag + "a", tag + "b"
    base = [
        {"role": "user", "content": "Read the files."},
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "read_file",
             "input": {"path": "/%s.rs" % a}}]},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": body(a)}]},
    ]
    turn2 = base + [
        {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t2", "name": "read_file",
             "input": {"path": "/%s.rs" % b}}]},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t2", "content": body(b)}]},
    ]

    results = {}
    for label, msgs in [("turn1 冷启", base), ("turn2 新增40K工具结果", turn2)]:
        u = call(key, msgs, tools)
        i = u.get("input_tokens") or 0
        c = u.get("cache_creation_input_tokens") or 0
        r = u.get("cache_read_input_tokens") or 0
        print("%-24s input=%-8s creation=%-8s read=%-8s 合计=%s"
              % (label, i, c, r, i + c + r))
        results[label] = (i, c, r)

    i1, c1, r1 = results["turn1 冷启"]
    i2, c2, r2 = results["turn2 新增40K工具结果"]
    print()

    # turn1 只有 3 条消息，含 40K 正文的那条是当前轮新输入，按设计不切段、
    # 不进 cache——它应当落在 input，而非 creation。旧版把它记成 creation 才是错的。
    if c1 > i1:
        print("FAIL: turn1 的当前轮尾部被记成 cache_creation(%d) 而非 input(%d)，"
              "当前轮新输入不该进缓存段" % (c1, i1))
        return 1

    # turn2 的 40K 正文进了历史（成为可缓存前缀），必须计入 creation。
    # 若仍为 0，说明指纹碰撞/漏计未修复：从未发送过的内容被误判为已缓存。
    if c2 == 0:
        print("FAIL: turn2 有一段从未发送过的 40K 正文进入历史，creation 却为 0 "
              "(read=%d)，指纹碰撞/漏计仍在" % r2)
        return 1

    # 守恒：三者之和应与上游 contextUsage 同量级，跨轮不应出现数量级跳变。
    if abs((i1 + c1 + r1) - (i2 + c2 + r2)) > max(i1 + c1 + r1, i2 + c2 + r2):
        print("FAIL: 两轮合计量级不一致（%d vs %d），分摊可能失真"
              % (i1 + c1 + r1, i2 + c2 + r2))
        return 1

    print("PASS: turn1 当前轮尾部计入 input(%d)；"
          "turn2 进入历史的全新正文计入 creation(%d)" % (i1, c2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
