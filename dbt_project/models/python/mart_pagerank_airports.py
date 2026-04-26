"""
dbt Python Model: Airport Network Graph Analysis.

Algorithm: Models the airport connection network as a directed weighted graph
and computes centrality metrics using NetworkX:
    - PageRank: importance score based on link structure
    - Betweenness Centrality: how often an airport lies on shortest paths
    - Weighted in/out degree: total connection weight

Input:  int_airport_connections (edge list with weights)
Output: mart_pagerank_airports (airport rankings with graph metrics)

Requires: dbt 1.3+ with Python model support, networkx, pandas
"""

import pandas as pd
import networkx as nx


def model(dbt, session):
    # Configure this model
    dbt.config(
        materialized="table",
        tags=["python", "marts", "graph"],
    )

    # Load edge list from upstream model
    # .df() works with dbt-duckdb >= 1.7; .to_pandas() was used in older versions
    ref = dbt.ref("int_airport_connections")
    edges = ref.df() if hasattr(ref, 'df') else ref.to_pandas()

    if edges.empty:
        return pd.DataFrame(columns=[
            "airport_id", "pagerank_score", "in_degree_weighted",
            "out_degree_weighted", "betweenness_centrality",
            "hub_rank", "community_id",
        ])

    # ── Step 1: Build directed weighted graph ────────────────
    G = nx.DiGraph()

    for _, row in edges.iterrows():
        G.add_edge(
            row["origin_airport_id"],
            row["destination_airport_id"],
            weight=int(row["flight_count"]),
        )

    num_nodes = G.number_of_nodes()
    num_edges = G.number_of_edges()

    # ── Step 2: PageRank ─────────────────────────────────────
    # Google's algorithm adapted for airport networks.
    # Higher score = more "important" airport (receives many connections
    # from other important airports).
    pagerank = nx.pagerank(G, weight="weight", alpha=0.85)

    # ── Step 3: Degree centrality (weighted) ─────────────────
    in_degree = dict(G.in_degree(weight="weight"))
    out_degree = dict(G.out_degree(weight="weight"))

    # ── Step 4: Betweenness centrality ───────────────────────
    # How often an airport lies on the shortest path between others.
    # High betweenness = critical transfer hub.
    # Use approximate algorithm for large graphs (k=min(100, n))
    k = min(100, num_nodes)
    betweenness = nx.betweenness_centrality(
        G, weight="weight", k=k, normalized=True
    )

    # ── Step 5: Clustering coefficient (undirected version) ──
    G_undirected = G.to_undirected()
    clustering = nx.clustering(G_undirected, weight="weight")

    # ── Step 6: Community detection (Louvain-like) ───────────
    # Using connected components as a simple community proxy
    components = nx.weakly_connected_components(G)
    community_map = {}
    for idx, component in enumerate(components):
        for node in component:
            community_map[node] = idx

    # ── Step 7: Assemble results ─────────────────────────────
    nodes = list(pagerank.keys())

    results = pd.DataFrame({
        "airport_id": nodes,
        "pagerank_score": [pagerank[n] for n in nodes],
        "in_degree_weighted": [in_degree.get(n, 0) for n in nodes],
        "out_degree_weighted": [out_degree.get(n, 0) for n in nodes],
        "total_degree_weighted": [
            in_degree.get(n, 0) + out_degree.get(n, 0) for n in nodes
        ],
        "betweenness_centrality": [betweenness.get(n, 0) for n in nodes],
        "clustering_coefficient": [clustering.get(n, 0) for n in nodes],
        "community_id": [community_map.get(n, -1) for n in nodes],
    })

    # Sort by PageRank descending and assign rank
    results = results.sort_values("pagerank_score", ascending=False)
    results["hub_rank"] = range(1, len(results) + 1)

    # Round for readability
    results["pagerank_score"] = results["pagerank_score"].round(6)
    results["betweenness_centrality"] = results["betweenness_centrality"].round(6)
    results["clustering_coefficient"] = results["clustering_coefficient"].round(6)

    # Add graph-level metadata as columns (same value for all rows)
    results["graph_total_nodes"] = num_nodes
    results["graph_total_edges"] = num_edges

    return results
