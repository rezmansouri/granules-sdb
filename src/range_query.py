import os
import numpy as np
import pandas as pd
from pprint import pprint
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

query = """
WITH all_granules AS (
    SELECT 
        id,
        shape,
        'granule_with_dot' AS type 
    FROM public.granule_with_dot_idx

    UNION ALL
    SELECT 
        id,
        shape,
        'granule_with_lane' AS type 
    FROM public.granule_with_lane_idx

    UNION ALL
    SELECT
        id,
        shape,
        'complex_granule' AS type 
    FROM public.complex_granule_idx

    UNION ALL
    SELECT
        id,
        shape,
        'uniform_granule' AS type 
    FROM public.uniform_granule_idx

)
SELECT COUNT(1)
FROM all_granules
WHERE ST_DWithin(
        shape,
        ST_SetSRID(ST_MakePoint(1727, 1727), 4326),
        range
    );
"""

query_2 = """
WITH all_granules AS (
    SELECT 
        id,
        shape,
        'granule_with_dot' AS type 
    FROM public.granule_with_dot_idx

    UNION ALL
    SELECT 
        id,
        shape,
        'granule_with_lane' AS type 
    FROM public.granule_with_lane_idx

    UNION ALL
    SELECT
        id,
        shape,
        'complex_granule' AS type 
    FROM public.complex_granule_idx

    UNION ALL
    SELECT
        id,
        shape,
        'uniform_granule' AS type 
    FROM public.uniform_granule_idx

)
SELECT *
FROM all_granules
WHERE ST_Contains(shape, ST_SetSRID(ST_Point(pp, pp), 4326));
"""

def get_query_timing(result):
    try:
        # Parse the output to extract planning and execution times
        planning_time, execution_time = None, None
        for row in result:
            line = row[0]  # Each row contains a single text line
            if "Planning Time" in line:
                planning_time = float(line.split(":")[1].strip().split(" ")[0])
            if "Execution Time" in line:
                execution_time = float(line.split(":")[1].strip().split(" ")[0])

        return planning_time, execution_time

    except Exception as e:
        print(f"Error: {e}")
        return None, None

def main():
    load_dotenv("./conf.env")
    conn_str = os.getenv("CONN_STR")
    engine = create_engine(conn_str)
    conn = engine.connect()
    idxs = ['', '_gist', '_sp_gist', '_brin']
    ranges = range(100, 3000, 100)
    result_1 = {idx: [] for idx in idxs}
    n_rows = []
    for idx in idxs:
        for r in ranges:
            q = 'EXPLAIN (ANALYZE, BUFFERS)' + query.replace('_idx', idx).replace('range', str(r))
            result = conn.execute(text(q))
            _, execution_time = get_query_timing(result)
            result_1[idx].append(execution_time)
            # print(planning_time, execution_time)
            
    for r in ranges:
        q = query.replace('_idx', '_gist').replace('range', str(r))
        result = conn.execute(text(q))
        n_row = result.fetchall()[0][0]
        n_rows.append(n_row)
        
    pprint(result_1)
    pprint(n_rows)
    
    df = pd.DataFrame(result_1)
    df.to_csv('1.csv')
            
    result_2 = {idx: [] for idx in idxs}
    for idx in idxs:
        for _ in range(10):
            p = np.random.randint(0, 3454)
            q = 'EXPLAIN (ANALYZE, BUFFERS)' + query_2.replace('_idx', idx).replace('pp', str(p))
            result = conn.execute(text(q))
            _, execution_time = get_query_timing(result)
            # print(idx, planning_time, execution_time)
            result_2[idx].append(execution_time)
    
    pprint(result_2)
    for idx in idxs:
        result_2[idx] = [np.mean(result_2[idx])]
    
    df = pd.DataFrame(result_2)
    df.to_csv('2.csv')
        

if __name__ == '__main__':
    main()