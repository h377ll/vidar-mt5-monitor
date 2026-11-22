"""Bounded local spool delivery. No daemon installed; explicit caller only.

Retries use a new authentication nonce but preserve event bytes and identity.
An acknowledgement proves durable inbox storage, NOT trade reconciliation.
"""
import json
import os
from pathlib import Path
import secrets
import time
from urllib.request import Request,urlopen
from urllib.parse import urlsplit
import event_receiver as er
import journal as j

def load_key(path):
    p=Path(path)
    if p.is_symlink() or p.stat().st_mode & 0o077:raise ValueError('KEY_PERMISSIONS')
    data=p.read_bytes()
    if len(data)<32:raise ValueError('KEY_TOO_SHORT')
    return data

def deliver(spool, cursor, endpoint, secret, limit=16):
    url=urlsplit(endpoint)
    if url.scheme!='http' or url.hostname!='127.0.0.1' or url.username or url.password or url.path!='/v1/events':
        raise ValueError('LOOPBACK_ONLY')
    spool=Path(spool);cursor=Path(cursor)
    if spool.is_symlink() or cursor.is_symlink():raise ValueError('SYMLINK')
    st=spool.stat();identity=[st.st_dev,st.st_ino]
    state=json.loads(cursor.read_text()) if cursor.exists() else {'identity':identity,'offset':0}
    if state['identity']!=identity or st.st_size<state['offset']:raise ValueError('SPOOL_REPLACED_OR_TRUNCATED')
    sent=0
    with spool.open('rb') as f:
        f.seek(state['offset'])
        for _ in range(limit):
            line=f.readline(er.MAX_BODY+2)
            if not line:break
            if len(line)>er.MAX_BODY:raise ValueError('LINE_TOO_LARGE')
            if not line.endswith(b'\n'):break  # Writer has not committed a complete line.
            obj=json.loads(line);body=j.dumps(obj).encode()
            headers=er.signed_headers(secret,body,int(time.time()),secrets.token_hex(16))
            with urlopen(Request(endpoint,data=body,headers=headers,method='POST'),timeout=3) as response:
                ack=json.load(response)
            if ack.get('status') not in ('stored','duplicate') or ack.get('ack')!=obj.get('seq'):
                raise ValueError('BAD_ACK')
            state['offset']=f.tell()
            j.atomic_text(cursor,j.dumps(state)+'\n')
            sent+=1
    return {'acknowledged_lines':sent,'offset':state['offset']}
