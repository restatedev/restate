#!/usr/bin/env python3
"""Compact per-state dump of a Quint ITF trace (used to write counterexample summaries).

Every state is printed as one line. Variables whose value changed are marked with '*'.
"""
import json
import sys

NAMES = ["now", "lease", "pointer", "sstPresent", "metaObjs", "loc", "ghost"]


def dec(v):
    """Decode the ITF encoding of Quint values into plain Python values."""
    if isinstance(v, dict):
        if "#bigint" in v:
            return int(v["#bigint"])
        if "#set" in v:
            return [dec(x) for x in v["#set"]]
        if "#map" in v:
            return {dec(k): dec(x) for k, x in v["#map"]}
        return {k: dec(x) for k, x in v.items()}
    if isinstance(v, list):
        return [dec(x) for x in v]
    return v


def short(v, prefix):
    """Strip the module prefix from variable names inside a value."""
    if isinstance(v, str):
        return v.replace(prefix, "")
    if isinstance(v, list):
        return [short(x, prefix) for x in v]
    return v
    return v


def main(path):
    d = json.load(open(path))
    vars_ = d["vars"]
    prefix = ""
    for v in vars_:
        if "::M::" in v:
            prefix = v.split("::M::")[0] + "::M::"
            break
    key = {n: prefix + n for n in NAMES}
    prev = None
    for i, st in enumerate(d["states"]):
        vals = {n: dec(st.get(key[n])) for n in NAMES}
        marks = {n: "" if prev is None or prev[n] == vals[n] else "*" for n in NAMES}
        lease = vals["lease"]
        ptr = vals["pointer"]
        loc = vals["loc"]
        ghost = vals["ghost"]
        pcs = " ".join(f"n{n}:{l['pc']}(chk={int(bool(l['chkValid']))},snap={l['snapId']}"
                       f",chkAt={l['checkedAt']},toDel={sorted(l['toDelete']) or ''}"
                       f",ev={sorted(l['evicted']) or ''},ref={sorted(l['refSet']) or ''})"
                       for n, l in sorted(loc.items(), key=lambda kv: kv[0]))
        print(f"{i:2d} now={vals['now']}{marks['now']} "
              f"lease=(h{lease['holder']},id{lease['id']},exp{lease['exp']}){marks['lease']} "
              f"ptr=(etag{ptr['etag']},lsn{ptr['latestLsn']},ret{sorted(ptr['retained']) or ''}){marks['pointer']} "
              f"ssts={sorted(vals['sstPresent'])}{marks['sstPresent']} "
              f"metas={sorted(vals['metaObjs'])}{marks['metaObjs']}")
        print(f"    {pcs}")
        print(f"    ghost: pub={ghost['publishCount']} evict={ghost['evictDeletes']} "
              f"sweep={ghost['sweepDeletes']} discardSst={ghost['discardSsts']} "
              f"i3={ghost['i3Deletes']} foreignPub={ghost['foreignPublishes']} "
              f"discRet={sorted(ghost['discardedRetained']) or ''} — {marks['ghost']}")
        prev = vals


if __name__ == "__main__":
    main(sys.argv[1])
