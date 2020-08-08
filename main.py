from stock import Stock
import config
from sava_data import SaveStock
from dbUtil import DBUtil
# st = Stock()
# es = "INSERT INTO data.all_stock VALUES (%s,%s,%s) ON CONFLICT (code) DO UPDATE SET tradeStatus=excluded.tradeStatus,code_name=excluded.code_name"
# SaveStock.save_as_db(es, st.all_stock("2020-07-31"))
if __name__ == "__main__":
    conn = DBUtil.getConnect()
    cur = conn.cursor()
    sql = """
        select pk_id, ss.execute_table, cron, t.tt
        from system.stock_schedule ss
        join (
            select execute_table,max(truth_time)tt
            from system.stock_schedule group by execute_table
        ) t
		on ss.execute_table = t.execute_table
        where execute_flag='未执行' and logic_time <= now()
        """
    cur.execute(sql)
    schedules = []
    for row in cur.fetchall():
        schedules.append(tuple(row[0], row[1], row[2], row[3]))
    conn.close()
    st = Stock()
    for schedule in schedules:
        if schedule[3] is "null":
            lasttime = "2005-01-01"
        else:
            lasttime = datetime.datetime.strptime(schedule[3], '%Y-%m-%d')
        


