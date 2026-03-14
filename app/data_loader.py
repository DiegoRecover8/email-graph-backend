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
        self.edges_by_node: Dict[str, List[Dict[str, Any]]] = {}

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
        self.edges_by_node = {str(node["id"]): [] for node in self.nodes}

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

            self.edges_by_node.setdefault(source, []).append(edge)
            self.edges_by_node.setdefault(target, []).append(edge)

        for node_id, neighbors in self.neighbors_by_node.items():
            neighbors.sort(key=lambda n: (-n["weight"], n["label"].lower()))

    def get_graph(self) -> Dict[str, Any]:
        return {"nodes": self.nodes, "edges": self.edges}

    def get_nodes(self) -> List[Dict[str, Any]]:
        return self.nodes

    def get_edges(self) -> List[Dict[str, Any]]:
        return self.edges

    def get_node(self, node_id: str, max_messages: int = 20) -> Optional[Dict[str, Any]]:
        node_id = str(node_id)
        node = self.nodes_by_id.get(node_id)
        if node is None:
            return None

        enriched = dict(node)
        enriched["neighbors"] = self.neighbors_by_node.get(node_id, [])

        node_label = str(node.get("label", "")).strip().lower()
        related_messages: List[Dict[str, Any]] = []
        seen_messages = set()

        for edge in self.edges_by_node.get(node_id, []):
            source_id = str(edge["source"])
            target_id = str(edge["target"])

            other_id = target_id if source_id == node_id else source_id
            other_label = self.nodes_by_id.get(other_id, {}).get("label", other_id)

            for msg in edge.get("messages", []):
                sender = str(msg.get("sender", "")).strip()
                recipient = str(msg.get("recipient", "")).strip()
                timestamp = msg.get("timestamp")
                subject = msg.get("subject")
                body = msg.get("body")

                dedupe_key = (
                    edge["id"],
                    str(timestamp or "").strip(),
                    str(subject or "").strip(),
                    sender.lower(),
                    recipient.lower(),
                    str(body or "").strip(),
                )

                if dedupe_key in seen_messages:
                    continue
                seen_messages.add(dedupe_key)

                related_messages.append(
                    {
                        "edge_id": edge["id"],
                        "timestamp": timestamp,
                        "subject": subject,
                        "sender": sender,
                        "recipient": recipient,
                        "body": body,
                        "interlocutor": other_label,
                    }
                )

        related_messages.sort(
            key=lambda m: str(m.get("timestamp") or ""),
            reverse=True,
        )

        enriched["related_messages"] = related_messages[:max_messages]
        return enriched

    def get_edge(self, edge_id: str) -> Optional[Dict[str, Any]]:
        edge = self.edges_by_id.get(str(edge_id))
        if edge is None:
            return None

        deduped_edge = dict(edge)
        seen_messages = set()
        deduped_messages = []

        for msg in edge.get("messages", []):
            key = (
                str(msg.get("timestamp") or "").strip(),
                str(msg.get("subject") or "").strip(),
                str(msg.get("sender") or "").strip().lower(),
                str(msg.get("recipient") or "").strip().lower(),
                str(msg.get("body") or "").strip(),
            )

            if key in seen_messages:
                continue
            seen_messages.add(key)
            deduped_messages.append(msg)

        deduped_edge["messages"] = deduped_messages
        return deduped_edge

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