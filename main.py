import sqlite3
import os, json

try:
    conn = sqlite3.connect(os.getenv('NEWPIPE_HISTORY_DB'))        
except Exception as e:
    print(e) 
    exit()
conn.row_factory = sqlite3.Row   #   add this row
cur = conn.cursor()
cur.execute("SELECT * FROM streams")

rows = cur.fetchall()

print('rows count : '+str(len(rows)))

if(len(rows) <= 0):
    print('No Data available');

newpipe_history = list()
for row in rows:
    try:
        cur.execute(f"SELECT * FROM stream_history where stream_id={row['uid']}")
        history = cur.fetchone()
        cur.execute(f"SELECT * FROM stream_state where stream_id={row['uid']}")
        state = cur.fetchone()
        #print(row['uid'])
        data = {
            "videoId": row['url'].split('?v=')[-1],
            "title": row['title'],
            "author": row['uploader'],
            "authorId": row['uploader_url'].split('channel/')[-1],
            "published": row['upload_date'],
            "description": "",
            "viewCount": row['view_count'],
            "lengthSeconds": row['duration'],
            "watchProgress": state['progress_time']//1000 if state else 0,
            "timeWatched": history['access_date'],
            "isLive": False,
            "paid": False,
            "type": "video"
        }
        #print(dict(row))
        #print(row['uid'])
        newpipe_history.append(data)
    except Exception as e:
        continue
print('Bad rows:', len(rows)-len(newpipe_history))
with open('newpipe_tofreetube.db', 'w') as f:
    for i in newpipe_history:
        f.write(json.dumps(i)+'\n')
