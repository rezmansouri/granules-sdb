import json


def main():
    with open("track_out/complex_granule.json", "r", encoding="utf-8") as f:
        complex_granule = json.load(f)
    with open("track_out/granule_with_dot.json", "r", encoding="utf-8") as f:
        granule_with_dot = json.load(f)
    with open("track_out/granule_with_lane.json", "r", encoding="utf-8") as f:
        granule_with_lane = json.load(f)
    with open("track_out/uniform_granule.json", "r", encoding="utf-8") as f:
        uniform_granule = json.load(f)

    complex_granule_items = list(complex_granule.items())
    complex_granule_items_length = len(complex_granule_items)
    partition_size = complex_granule_items_length // 10
    for i in range(0, 12):
        
        complex_granule_query = ""
        where_clause = ""

        for id, tracked_id in complex_granule_items[i*partition_size:(i+1)*partition_size]:
            complex_granule_query += f'\n\tWHEN id={id} THEN {tracked_id}'
            where_clause += f'{id}, '
            
        complex_granule_query = 'UPDATE complex_granule SET tracked_id = CASE' + complex_granule_query + ' END WHERE id IN (' + where_clause[:-2] + ');'
        with open(f'sql/track_complex_granule_{i}.sql', 'w', encoding='utf-8') as file:
            file.write(complex_granule_query)

    granule_with_dot_query = ""
    where_clause = ""

    for id, tracked_id in granule_with_dot:
        granule_with_dot_query += f'\n\tWHEN id={id} THEN {tracked_id}'
        where_clause += f'{id}, '
        
    granule_with_dot_query = 'UPDATE granule_with_dot SET tracked_id = CASE' + granule_with_dot_query + ' END WHERE id IN (' + where_clause[:-2] + ');'
    with open('sql/track_granule_with_dot.sql', 'w', encoding='utf-8') as file:
        file.write(granule_with_dot_query)

    granule_with_lane_query = ""
    where_clause = ""

    for id, tracked_id in granule_with_lane:
        granule_with_lane_query += f'\n\tWHEN id={id} THEN {tracked_id}'
        where_clause += f'{id}, '
        
    granule_with_lane_query = 'UPDATE granule_with_lane SET tracked_id = CASE' + granule_with_lane_query + ' END WHERE id IN (' + where_clause[:-2] + ');'
    with open('sql/track_granule_with_lane.sql', 'w', encoding='utf-8') as file:
        file.write(granule_with_lane_query)
        
    uniform_granule_query = ""
    where_clause = ""

    for id, tracked_id in uniform_granule:
        uniform_granule_query += f'\n\tWHEN id={id} THEN {tracked_id}'
        where_clause += f'{id}, '
        
    uniform_granule_query = 'UPDATE uniform_granule SET tracked_id = CASE' + uniform_granule_query + ' END WHERE id IN (' + where_clause[:-2] + ');'
    with open('sql/track_uniform_granule.sql', 'w', encoding='utf-8') as file:
        file.write(uniform_granule_query)
    
    

if __name__ == "__main__":
    main()
