from collections import defaultdict

import pandas as pd
import geopandas as gpd
import shapely.geometry as shp

import matsim


def turn_nodes_into_gdf(nodes: pd.DataFrame, crs: str) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(nodes.assign(geometry = shp.Point(nodes['x'], nodes['y']))).set_crs(crs).drop(columns=['x', 'y'])

def associate_nodes_with_zones(zones: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Finds out which nodes are contained within each zones"""
    return nodes.sjoin(zones, how='left', predicate='within')

def associate_links_with_zones(zones: gpd.GeoDataFrame, links: pd.DataFrame, nodes: pd.DataFrame, crs: str = 'LV95') -> pd.DataFrame:
    """
    Finds out the start and end zones for each link based on the start and end nodes
    """
    if isinstance(nodes, gpd.GeoDataFrame):
        nodes = turn_nodes_into_gdf(nodes, crs)
    nodes_with_zones = associate_nodes_with_zones(zones, nodes)
    
    return links\
        .merge(nodes_with_zones, left_on='from_node', right_on='node_id')\
        .merge(nodes_with_zones, left_on='to_node', right_on='node_id', suffixes=['_from', '_to'])\

def get_link_counts(events) -> pd.DataFrame:
    """Get counts for each link"""
    
    link_counts = defaultdict(int)

    for event in events:
        link_counts[event['link']] += 1

    link_counts = pd.DataFrame.from_dict(link_counts, orient='index', columns=['count']).rename_axis('link_id')

def aggregate_zones_with_volumes(links_with_zones: pd.DataFrame, link_counts: pd.DataFrame, zone_id_col: str = 'zone_id') -> pd.DataFrame:
    """Given a dataframe of links with respective vehicle counts and associated start and end zones, returns a dataframe"""
    return links_with_zones.merge(link_counts, on='link_id').groupby(by=[f"{zone_id_col}_from", f"{zone_id_col}_to"])[['count']].sum().reset_index().rename({f"{zone_id_col}_from" : "FROM", f"{zone_id_col}_to": "TO", "count": "DEMAND"})

if __name__ == '__main__':
    from sys import argv

    zone_path, network_path, events_path = argv[1:]

    zones: gpd.GeoDataFrame = gpd.read_file(zone_path)

    network = matsim.read_network(network_path)
    nodes, links = network.nodes, network.links

    events = matsim.event_reader(events_path, types='entered link')

    aggregate_zones_with_volumes(associate_links_with_zones(zones, links, nodes), get_link_counts(events)).to_csv("od_matrix.csv")

