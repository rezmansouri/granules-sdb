import os
from dotenv import load_dotenv
from tqdm import tqdm
from sqlalchemy import create_engine, text

query = """
with granules as (
    SELECT tracked_id, time
    FROM uniform_granule
    UNION ALL
    SELECT tracked_id, time
    FROM granule_with_dot
    UNION ALL
    SELECT tracked_id, time
    FROM granule_with_lane
    UNION ALL
    SELECT tracked_id, time
    FROM complex_granule
)
SELECT granules.tracked_id as id, MIN(granules.time) AS birth, MAX(granules.time) AS death FROM granules
GROUP BY id;
"""


def main():
    load_dotenv("./conf.env")
    conn_str = os.getenv("CONN_STR")
    engine = create_engine(conn_str)
    conn = engine.connect()
    result = conn.execute(text(query))
    insert_query = "INSERT INTO tracked_granules VALUES "
    for id, birth, death in tqdm(result.fetchall()):
        insert_query += f"\n({id}, '{str(birth)}', '{str(death)}'),"
    insert_query = insert_query[:-1] + ";"
    with open("sql/tracked_granules.sql", "w", encoding="utf-8") as file:
        file.write(insert_query)
    print("DONE")


if __name__ == "__main__":
    main()
