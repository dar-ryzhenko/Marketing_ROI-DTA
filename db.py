import sqlite3

conn = sqlite3.connect('shop_sqlite.db')
cur = conn.cursor()

with open('orders_sqlite.sql', 'r', encoding='utf-8') as f:
    sql_script = f.read()

cur.executescript(sql_script)

cur.execute(''' 
            SELECT *
            FROM orders 
            LIMIT 5;
        ''')
print(cur.fetchall())

conn.commit()
conn.close()