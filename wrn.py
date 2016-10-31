#!/usr/bin/env python3
#coding=utf-8
# from __future__ import print_function
import sqlite3 as db
from collections import defaultdict
import subprocess
import warnings
import os
import os.path
import sys
import shlex

options = defaultdict(str)
conn = None
def init_db():
    global conn
    conn = db.connect("wrn.db")
    cursor = conn.cursor()
    try:
        DB_NAME = "WRunner"

        TASK_TABLE_NAME = 'TASK_LIST'
        cursor.execute('CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY,task varchar(30), cmd varchar(100), output text, tag varchar(100))' %TASK_TABLE_NAME)

        conn.commit()
    except:
        import traceback;traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()

def insert_into_db(text):
    global options
    global conn
    cursor = conn.cursor()
    last_id = -1
    try:
        cursor.execute("INSERT INTO TASK_LIST (task, cmd, output, tag) VALUES(?,?,?,?)",
            (options['task'], options['cmd'], text, options['tag']))
        cursor.execute("SELECT last_insert_rowid()")
        result = cursor.fetchone()
        last_id = result[0]
        conn.commit()
    except:
        import traceback;traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
    return last_id

def query_from_db(text):
    global conn
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM TASK_LIST WHERE id=? OR tag=? OR task=?",
            (text, text, text))
        result = cursor.fetchall()
        for line in result:
            taskId = "Id: {}\n".format(str(line[0]).replace(text, "\033[94m%s\033[0m" % text))
            task = "Task: {}\n".format(line[1].replace(text, "\033[94m%s\033[0m" % text))
            cmd = "Cmd: {}\n".format(line[2])
            output="Output:\n{}\n".format(line[3])
            tag = ""
            if line[4].strip():
                tag = "tag: {}\n".format(line[4].replace(text, "\033[94m%s\033[0m" % text))
            fullStr = ''.join([taskId, task, cmd, output, tag])
            print(fullStr)
            print()
        conn.commit()
    except:
        import traceback;traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()

def parse_file():
    global options
    import yaml
    options = yaml.load(open(".wrn.yaml"))

def parse_args():
    global options
    cur = 1
    argv = sys.argv
    while cur<len(argv):
        curArg = argv[cur]
        if curArg == "--cmd" or curArg == "-c":
            cur += 1
            if "cmd" not in options:
                options["cmd"]=argv[cur]
        elif curArg == "--task":
            cur += 1
            if "task" not in options:
                options["task"]=argv[cur]
        elif curArg == "--tag" or curArg == "-t":
            cur += 1
            if "tag" not in options:
                options["tag"]=argv[cur]
        elif curArg == "query":
            cur+=1
            options["query"] = argv[cur]
        cur += 1

def main():
    if os.path.isfile(".wrn.yaml"):
        parse_file()
    parse_args()

    init_db()

    if "cmd" not in options or "task" not in options:
        print("cmd, task must be provided.")
        exit()
    if "tag" not in options:
        options["tag"]=""

    if "query" in options:
        query_from_db(options['query'])
        exit()


    output = ""
    cmd = options["cmd"]
    print("Cmd executed: {}".format(cmd))

    proces = None
    try:
        process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1)

        for line in iter(process.stdout.readline, ''):
            if not line:
                break
            line_str = line.decode("utf-8")
            print(line_str, file=sys.stderr ,end='')
            output += line_str

        last_id = insert_into_db(output)
        print(last_id)
    except KeyboardInterrupt:
        if process is not None:
            process.kill()
            print("Quit")

if __name__=="__main__":
    main()
