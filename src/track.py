import os
import numpy as np
import json
from dotenv import load_dotenv
from tqdm import trange, tqdm
from copy import deepcopy
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text


TRACK_QUERY = """
WITH current_object AS (
    SELECT id, shape
    FROM public.g1
    WHERE id = thisid
),
previous_objects AS (
    SELECT 
        id, 
        shape, 
        'g1' AS table_name 
    FROM public.g1
    WHERE time = 'timestamp'
    UNION ALL
    SELECT 
        id, 
        shape, 
        'g2' AS table_name 
    FROM public.g2
    WHERE time = 'timestamp'
    UNION ALL
    SELECT
        id, 
        shape, 
        'g3' AS table_name 
    FROM public.g3
    WHERE time = 'timestamp'
    UNION ALL
    SELECT
        id, 
        shape, 
        'g4' AS table_name 
    FROM public.g4
    WHERE time = 'timestamp'
)
SELECT
    previous_objects.id AS previous_id,
    ST_Area(ST_Intersection(current_object.shape, previous_objects.shape)) /
    ST_Area(ST_Union(current_object.shape, previous_objects.shape)) AS iou,
    previous_objects.table_name
FROM current_object, previous_objects
WHERE ST_Intersects(current_object.shape, previous_objects.shape);
"""

T1_QUERY = """
SELECT id FROM g1
    WHERE
    time = 'timestamp'
    ORDER BY id;
"""

TIME_QUERY = '''
SELECT DISTINCT(time) FROM uniform_granule ORDER BY time DESC;
'''

WIDTH = HEIGHT = 3454
INTERVAL = timedelta(seconds=6)

CHANGE_CLASS_WEIGHTS = {
    "uniform_granule": {
        "complex_granule": 1,
        "uniform_granule": 3,
        "granule_with_dot": 1,
        "granule_with_lane": 1,
    },
    "complex_granule": {
        "complex_granule": 3,
        "uniform_granule": 1,
        "granule_with_dot": 1,
        "granule_with_lane": 1,
    },
    "granule_with_dot": {
        "complex_granule": 1,
        "uniform_granule": 1,
        "granule_with_dot": 3,
        "granule_with_lane": 1,
    },
    "granule_with_lane": {
        "complex_granule": 1,
        "uniform_granule": 1,
        "granule_with_dot": 1,
        "granule_with_lane": 3,
    },
}

TABLES = list(CHANGE_CLASS_WEIGHTS.keys())


def main():
    """
    tracker main function
    """
    tracked_id_seq = 1
    update_dict = {
        "uniform_granule": dict(),
        "complex_granule": dict(),
        "granule_with_dot": dict(),
        "granule_with_lane": dict() 
    }
    load_dotenv("./conf.env")
    conn_str = os.getenv("CONN_STR")
    engine = create_engine(conn_str)
    conn = engine.connect()
    result = conn.execute(text(TIME_QUERY))
    times = [str(r[0]) for r in result]
    for i in trange(len(times[:1]), leave=False):
        time_1 = times[i]
        if i == len(times) - 1:
            time_0 = str(datetime.strptime(time_1, '%Y-%m-%d %H:%M:%S') + INTERVAL)
        else:
            time_0 = times[i+1]
        for table_t1 in tqdm(TABLES, leave=False):
            query = T1_QUERY.replace("g1", table_t1).replace("timestamp", time_1)
            result = conn.execute(text(query))
            t1s = result.fetchall()
            for t1 in t1s:
                id_t1 = t1[0]
                if id_t1 not in update_dict[table_t1]:
                    update_dict[table_t1][id_t1] = tracked_id_seq
                    tracked_id_seq += 1
                tracked_id_t1 = update_dict[table_t1][id_t1]
                query = (
                    TRACK_QUERY.replace("thisid", str(id_t1))
                    .replace("timestamp", time_0)
                    .replace("g1", table_t1)
                )
                new_tables = deepcopy(TABLES)
                new_tables.remove(table_t1)
                for j in range(2, 5):
                    query = query.replace(f"g{j}", new_tables[j - 2])
                del new_tables
                result = conn.execute(text(query))
                rows = result.fetchall()
                if len(rows) == 0:
                    continue
                scores = [r[1] * CHANGE_CLASS_WEIGHTS[r[2]][table_t1] for r in rows]
                id_t0, _, table_t0 = rows[np.argmax(scores)]
                update_dict[table_t0][id_t0] = tracked_id_t1
    conn.close()
    for table in update_dict:
        with open(f"track_out/{table}.json", "w", encoding="utf-8") as file:
            json.dump(update_dict[table], file, indent=4)
    print("DONE")


if __name__ == "__main__":
    main()
