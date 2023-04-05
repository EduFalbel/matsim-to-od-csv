import pandas as pd
import geopandas as gpd
import shapely.geometry as shp

import click

@click.command()
@click.argument('trips_filename', type=click.Path(exists=True))
@click.argument('zones_filename', type=click.Path(exists=True))
@click.option('--zone-id-col', default='ID', help='ID column in the zones shapefile')
@click.option('--out_filepath', default='od_matrix.csv', help='Filename for the csv output')
def create_od(trips_filename, zones_filename, zone_id_col):
    trips = pd.read_csv(trips_filename)

    # Filter trips which use public transport
    trips: pd.DataFrame = trips[trips['modes'].str.contains('pt')][['start_x', 'start_y', 'end_x', 'end_y']]

    # Add start and end coordinates
    trips['geometry_start'] = trips.apply(lambda x: shp.Point(x['start_x'], x['start_y']), axis=1)
    trips['geometry_end'] = trips.apply(lambda x: shp.Point(x['end_x'], x['end_y']), axis=1)
    trips.drop(columns=['start_x', 'start_y', 'end_x', 'end_y'], inplace=True)

    zones: gpd.GeoDataFrame = gpd.read_file(zones_filename)[["ID", "geometry"]]

    # Determine starting zones of the trips
    trips = gpd.GeoDataFrame(trips).set_geometry('geometry_start')
    trips = trips.sjoin(zones, how='left', predicate='within')

    # Determine end zones of the trips
    trips = trips.set_geometry('geometry_end')
    trips = trips.sjoin(zones, how='left', predicate='within', lsuffix='start', rsuffix='end')

    # Aggregate trips by start and end zones
    zones_with_volumes = trips.groupby([f"{zone_id_col}_start", f"{zone_id_col}_end"]).count().iloc[:, 0].reset_index().rename({f"{zone_id_col}_start" : "FROM", f"{zone_id_col}_end": "TO", trips.columns[0]: "DEMAND"})

    zones_with_volumes.to_csv('od_matrix.csv', index=False)

if __name__ == '__main__':
    create_od()