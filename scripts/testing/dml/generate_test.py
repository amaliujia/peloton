f =  open('seq_scan_insert.sql', 'w')
for i in range(0, 1000000):
    query = "insert into BB values(%d, %d);" % (i, i+1)
    f.write(query + "\n")
f.close()

#query = "select count(*) from BB;"
#f.write(query + "\n")

f =  open('seq_scan_delete.sql', 'w')
for i in range(0, 1000000):
    query = "delete from BB where id = %d;" % i
    f.write(query + "\n")
f.close()
