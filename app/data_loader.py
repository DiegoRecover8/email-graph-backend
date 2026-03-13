from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


class GraphDataStore:
    """
    In-memory loader for the preprocessed graph_data.json.
    Provides convenient indexes for nodes and edges.
    """

    def __init__(self, data_path: Path):
        self.data_path = data_path
        self._raw: Dict[str, Any] = {}
        self.nodes: List[Dict[str, Any]] = []
        self.edges: List[Dict[str, Any]] = []
        self.nodes_by_id: Dict[str, Dict[str, Any]] = {}
        self.edges_by_id: Dict[str, Dict[str, Any]] = {}
        self.neighbors_by_node: Dict[str, List[Dict[str, Any]]] = {}

    def load(self) -> None:
        if not self.data_path.exists():
            raise FileNotFoundError(f"graph_data.json not found at: {self.data_path}")

        with self.data_path.open("r", encoding="utf-8") as f:
            self._raw = json.load(f)

        self.nodes = self._raw.get("nodes", [])
        self.edges = self._raw.get("edges", [])

        self.nodes_by_id = {str(node["id"]): node for node in self.nodes}
        self.edges_by_id = {str(edge["id"]): edge for edge in self.edges}
        self.neighbors_by_node = {str(node["id"]): [] for node in self.nodes}

        for edge in self.edges:
            source = str(edge["source"])
            target = str(edge["target"])
            weight = float(edge.get("weight", 1.0))

            source_label = self.nodes_by_id.get(source, {}).get("label", source)
            target_label = self.nodes_by_id.get(target, {}).get("label", target)

            self.neighbors_by_node.setdefault(source, []).append(
                {"id": target, "label": target_label, "weight": weight}
            )
            self.neighbors_by_node.setdefault(target, []).append(
                {"id": source, "label": source_label, "weight": weight}
            )

        for node_id, neighbors in self.neighbors_by_node.items():
            neighbors.sort(key=lambda n: (-n["weight"], n["label"].lower()))

    def get_graph(self) -> Dict[str, Any]:
        return {"nodes": self.nodes, "edges": self.edges}

    def get_nodes(self) -> List[Dict[str, Any]]:
        return self.nodes

    def get_edges(self) -> List[Dict[str, Any]]:
        return self.edges

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        node = self.nodes_by_id.get(str(node_id))
        if node is None:
            return None

        enriched = dict(node)
        enriched["neighbors"] = self.neighbors_by_node.get(str(node_id), [])
        return enriched

    def get_edge(self, edge_id: str) -> Optional[Dict[str, Any]]:
        return self.edges_by_id.get(str(edge_id))

    def search_nodes(self, query: str) -> List[Dict[str, Any]]:
        q = query.strip().lower()
        if not q:
            return []

        results = [
            node for node in self.nodes
            if q in str(node.get("label", "")).lower()
        ]
        results.sort(key=lambda n: (n["label"].lower(), n["id"]))
        return results