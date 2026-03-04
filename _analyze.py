import sqlite3

conn = sqlite3.connect('crawl_data.db')
cur = conn.cursor()

cur.execute('PRAGMA table_info(crawled_urls)')
cols = [r[1] for r in cur.fetchall()]
print('Columns:', cols)

cur.execute('SELECT COUNT(*) FROM crawled_urls')
total = cur.fetchone()[0]
print(f'Total rows: {total}')

cur.execute('SELECT COUNT(DISTINCT domain) FROM crawled_urls')
print(f'Distinct domains: {cur.fetchone()[0]}')

cur.execute('SELECT status, COUNT(*) n FROM crawled_urls GROUP BY status ORDER BY n DESC LIMIT 12')
print('\nStatus codes:')
for r in cur.fetchall():
    print(f'  {r[0]:>5}  {r[1]:>7}')

cur.execute('SELECT MIN(crawled_at), MAX(crawled_at) FROM crawled_urls')
r = cur.fetchone()
print(f'\nTime range: {r[0]}  ->  {r[1]}')

# Top 30 domains by crawl count
cur.execute('''
    SELECT domain, COUNT(*) n,
           SUM(CASE WHEN status=200 THEN 1 ELSE 0 END) ok
    FROM crawled_urls
    GROUP BY domain
    ORDER BY n DESC
    LIMIT 30
''')
print('\nTop 30 domains by crawl count:')
for r in cur.fetchall():
    rate = 100 * r[2] / r[1]
    print(f'  {r[0]:<55} crawls={r[1]:>5}  ok={r[2]:>5}  succ={rate:.0f}%')

# How many domains have only 1 crawl
cur.execute('SELECT COUNT(*) FROM (SELECT domain FROM crawled_urls GROUP BY domain HAVING COUNT(*)=1)')
print(f'\nDomains with only 1 crawl: {cur.fetchone()[0]}')

# crawls per minute over time
cur.execute('SELECT COUNT(*) FROM crawled_urls WHERE crawled_at IS NOT NULL')
print(f'Rows with crawled_at: {cur.fetchone()[0]}')

conn.close()
