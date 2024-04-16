import psycopg2



class Pg:
    conn = psycopg2.connect(
        "host=development-pg14-cluster-wmp.cluster-crghn1wrpflt.us-west-2.rds.amazonaws.com dbname=core user=stageuser password=goWest123!")
    conn.set_session(autocommit=True)

    def sql(self, query: str, params=None):
        # Execute a query
        cur = self.conn.cursor()
        cur.execute(query, params)
        # Retrieve query results
        res = cur.fetchone()
        cur.close()
        return res[0]
