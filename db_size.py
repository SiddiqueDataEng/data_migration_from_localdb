import mysql.connector

c = mysql.connector.connect(
    host='',
    port=3306,
    user='',
    password='',
    database='information_schema'
)

cur = c.cursor()

# Total database size
cur.execute("""
    SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 1)
    FROM tables
    WHERE table_schema = 'sattioe1_freeway_erp'
""")

mb = cur.fetchone()[0]

print('MySQL DB size: {} MB ({:.2f} GB)'.format(
    mb, float(mb or 0) / 1024
))

# CSV estimate
total_rows = 4_211_046
avg_bytes_per_row = 150
csv_est_mb = total_rows * avg_bytes_per_row / 1024 / 1024

print('Estimated CSV size: {:.0f} MB ({:.2f} GB)'.format(
    csv_est_mb, csv_est_mb / 1024
))

print()
print('=== SIZE BREAKDOWN BY TABLE ===')

cur2 = c.cursor()

cur2.execute("""
    SELECT
        table_name,
        table_rows,
        ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_mb
    FROM tables
    WHERE table_schema = 'sattioe1_freeway_erp'
    ORDER BY (data_length + index_length) DESC
    LIMIT 20
""")

rows = cur2.fetchall()

print('{:<40s} {:>12s} {:>10s}'.format(
    'Table', 'Est Rows', 'Size MB'
))
print('-' * 66)

for table_name, table_rows, size_mb in rows:
    print('{:<40s} {:>12,} {:>10.2f}'.format(
        str(table_name),
        int(table_rows or 0),
        float(size_mb or 0)
    ))

cur2.close()
cur.close()
c.close()