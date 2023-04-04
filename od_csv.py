from collections import defaultdict

import pandas as pd
import geopandas as gpd
import shapely.geometry as shp


if __name__ == '__main__':
    from sys import argv

    trips_file, zones_file, zone_id_col = argv[1:]

    trips = pd.read_csv(trips_file)
    trips: pd.DataFrame = trips[trips['modes'].str.contains('pt')][['start_x', 'start_y', 'end_x', 'end_y']]
    trips['geometry_start'] = trips.apply(lambda x: shp.Point(x['start_x'], x['start_y']), axis=1)
    trips['geometry_end'] = trips.apply(lambda x: shp.Point(x['end_x'], x['end_y']), axis=1)
    trips.drop(columns=['start_x', 'start_y', 'end_x', 'end_y'], inplace=True)

    print('Assigned geometry')

    zones: gpd.GeoDataFrame = gpd.read_file(zones_file)[["ID", "geometry"]]
    print(zones.head())

    trips = gpd.GeoDataFrame(trips).set_geometry('geometry_start')
    trips = trips.sjoin(zones, how='left', predicate='within')
    print(trips.head())

    trips = trips.set_geometry('geometry_end')
    trips = trips.sjoin(zones, how='left', predicate='within', lsuffix='start', rsuffix='end')
    print(trips.head())

    print('Spatial join')

    zones_with_volumes = trips.groupby([f"{zone_id_col}_start", f"{zone_id_col}_end"]).count().iloc[:, 0].reset_index().rename({f"{zone_id_col}_start" : "FROM", f"{zone_id_col}_end": "TO", trips.columns[0]: "DEMAND"})
    zones_with_volumes.to_csv('od_matrix.csv', index=False)

