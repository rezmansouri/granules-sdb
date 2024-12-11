import os
import sys
import numpy as np
from tqdm import tqdm
from datetime import datetime
from dotenv import load_dotenv
from geoalchemy2 import WKTElement
from geo_utils import array_to_gdf, table_dict


def main():
    
    load_dotenv('./conf.env')
    
    masks_root = sys.argv[1]
    masks_paths = [p for p in os.listdir(masks_root) if p.endswith('.npy')]
    
    queries = {v:'' for v in table_dict.values()}
    
    for mask_path in tqdm(masks_paths):
        time_str = mask_path[4:27]
        time = datetime.strptime(time_str, '%Y_%m_%dT%H_%M_%S_%f')
        mask = np.load(os.path.join(masks_root, mask_path))
        gdf = array_to_gdf(mask)
        for _, row in gdf.iterrows():
            wkt_element = WKTElement(row['geometry'].ExportToWkt(), srid=4326)
            table = table_dict[row['value']]
            query = f"""INSERT INTO {table} (time, shape) VALUES ('{time}', ST_GeomFromText('{wkt_element}', 4326));"""
            queries[table] += query + '\n'
        
    for k, v in queries.items():
        with open(f'sql/{k}.sql', 'w', encoding='utf-8') as file:
            file.write(v)

if __name__ == '__main__':
    main()
