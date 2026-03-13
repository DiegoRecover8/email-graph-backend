from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, DefaultDict, Dict, Iterable, List, Optional, Tuple

import networkx as nx
import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

GRAPH_PATH = DATA_DIR / "epstein_social_graph_cleaned.gml"
MAPPING_PATH = DATA_DIR / "epstein_name_id_mapping.csv"
EMAILS_PATH = DATA_DIR / "epstein_emails.csv"
OUTPUT_PATH = DATA_DIR / "graph_data.json"


def normalise_name(raw: Any) -> str:
    """
    Reproduce the conservative normalization used in the notebook:
    - strip email metadata / bracket metadata
    - strip parenthetical metadata
    - reorder 'Last, First' -> 'First Last'
    - remove dots and quotes
    - collapse whitespace
    - lowercase
    """
    if raw is None:
        return ""

    name = str(raw).strip()
    if not name:
        return ""

    name = re.sub(r"<[^>]*>", "", name)
    name = re.sub(r"\[[^\]]*\]", "", name)
    name = re.sub(r"\([^)]*\)", "", name)
    name = name.replace("'", "").replace('"', "")

    if "," in name:
        parts = name.split(",", 1)
        last = parts[0].strip()
        first = parts[1].strip()
        if first and "@" not in first:
            name = f"{first} {last}"
        else:
            name = name.replace(",", "")

    name = name.replace(".", "")
    name = re.sub(r"\s+", " ", name).strip()
    return name.lower()


def safe_parse_messages(value: Any) -> List[Dict[str, Any]]:
    """
    Safely parse the CSV messages column.
    Returns an empty list if parsing fails or the structure is invalid.
    """
    if value is None:
        return []

    if isinstance(value, list):
        return [m for m in value if isinstance(m, dict)]

    try:
        parsed = json.loads(value)
    except Exception:
        return []

    if not isinstance(parsed, list):
        return []

    return [m for m in parsed if isinstance(m, dict)]


def to_serializable(value: Any) -> Any:
    """
    Convert values coming from networkx / numpy / pandas into JSON-safe types.
    """
    if value is None:
        return None

    if isinstance(value, (str, int, float, bool)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value

    # numpy scalar-like objects often implement item()
    if hasattr(value, "item"):
        try:
            return to_serializable(value.item())
        except Exception:
            pass

    return str(value)


def sorted_edge_id(source_id: str, target_id: str) -> str:
    a, b = sorted([str(source_id), str(target_id)], key=lambda x: int(x) if x.isdigit() else x)
    return f"{a}__{b}"


def build_name_indexes(mapping_df: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns:
      - name_to_id: canonical normalized name -> node id
      - id_to_name: node id -> human-readable label
    """
    required = {"id", "label_original"}
    missing = required - set(mapping_df.columns)
    if missing:
        raise ValueError(f"Missing columns in mapping CSV: {sorted(missing)}")

    mapping_df = mapping_df.copy()
    mapping_df["id"] = mapping_df["id"].astype(str)
    mapping_df["label_original"] = mapping_df["label_original"].astype(str)
    mapping_df["label_normalized"] = mapping_df["label_original"].apply(normalise_name)

    name_to_id: Dict[str, str] = {}
    id_to_name: Dict[str, str] = {}

    for _, row in mapping_df.iterrows():
        node_id = str(row["id"])
        label_original = row["label_original"]
        label_normalized = row["label_normalized"]

        id_to_name[node_id] = label_original
        if label_normalized and label_normalized not in name_to_id:
            name_to_id[label_normalized] = node_id

    return name_to_id, id_to_name


def extract_edge_messages(
    emails_df: pd.DataFrame,
    name_to_id: Dict[str, str],
    valid_edge_ids: set[str],
) -> DefaultDict[Tuple[str, str], List[Dict[str, Any]]]:
    """
    Reconstruct messages attached to undirected edges.
    Only retains interactions whose endpoints exist in the final mapping and
    whose pair exists in the cleaned graph.
    """
    edge_messages: DefaultDict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)

    for _, row in emails_df.iterrows():
        parsed_messages = safe_parse_messages(row.get("messages"))

        for msg in parsed_messages:
            sender_raw = msg.get("sender")
            recipients_raw = msg.get("recipients", [])

            sender_norm = normalise_name(sender_raw)
            if not sender_norm:
                continue

            if not isinstance(recipients_raw, list):
                recipients = [recipients_raw]
            else:
                recipients = recipients_raw

            for recipient_raw in recipients:
                recipient_norm = normalise_name(recipient_raw)

                if not recipient_norm:
                    continue
                if sender_norm == recipient_norm:
                    continue
                if sender_norm not in name_to_id or recipient_norm not in name_to_id:
                    continue

                source_id = str(name_to_id[sender_norm])
                target_id = str(name_to_id[recipient_norm])

                edge_id = sorted_edge_id(source_id, target_id)
                if edge_id not in valid_edge_ids:
                    continue

                pair = tuple(edge_id.split("__"))

                edge_messages[pair].append(
                    {
                        "timestamp": to_serializable(msg.get("timestamp")),
                        "subject": to_serializable(msg.get("subject")),
                        "sender": sender_norm,
                        "recipient": recipient_norm,
                        "body": to_serializable(msg.get("body")),
                    }
                )

    return edge_messages


def build_nodes(
    graph: nx.Graph,
    id_to_name: Dict[str, str],
) -> List[Dict[str, Any]]:
    degree = dict(graph.degree())
    weighted_degree = dict(graph.degree(weight="weight"))

    nodes: List[Dict[str, Any]] = []
    for node in graph.nodes():
        node_id = str(node)
        label = id_to_name.get(node_id, str(graph.nodes[node].get("label", node_id)))

        nodes.append(
            {
                "id": node_id,
                "label": label,
                "degree": int(to_serializable(degree.get(node, 0)) or 0),
                "weighted_degree": float(to_serializable(weighted_degree.get(node, 0)) or 0.0),
            }
        )

    nodes.sort(key=lambda n: int(n["id"]) if str(n["id"]).isdigit() else str(n["id"]))
    return nodes


def build_edges(
    graph: nx.Graph,
    edge_messages: DefaultDict[Tuple[str, str], List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    edges: List[Dict[str, Any]] = []

    for source, target, data in graph.edges(data=True):
        source_id = str(source)
        target_id = str(target)
        edge_id = sorted_edge_id(source_id, target_id)
        pair = tuple(edge_id.split("__"))

        weight_raw = data.get("weight", 1.0)
        weight = float(to_serializable(weight_raw) or 1.0)

        edges.append(
            {
                "id": edge_id,
                "source": source_id,
                "target": target_id,
                "weight": weight,
                "messages": edge_messages.get(pair, []),
            }
        )

    edges.sort(key=lambda e: tuple(int(x) if x.isdigit() else x for x in e["id"].split("__")))
    return edges


def main() -> None:
    if not GRAPH_PATH.exists():
        raise FileNotFoundError(f"GML file not found: {GRAPH_PATH}")
    if not MAPPING_PATH.exists():
        raise FileNotFoundError(f"Mapping CSV not found: {MAPPING_PATH}")
    if not EMAILS_PATH.exists():
        raise FileNotFoundError(f"Emails CSV not found: {EMAILS_PATH}")

    graph = nx.read_gml(GRAPH_PATH)
    mapping_df = pd.read_csv(MAPPING_PATH)
    emails_df = pd.read_csv(EMAILS_PATH)

    name_to_id, id_to_name = build_name_indexes(mapping_df)

    valid_edge_ids = {
        sorted_edge_id(str(u), str(v))
        for u, v in graph.edges()
    }

    edge_messages = extract_edge_messages(
        emails_df=emails_df,
        name_to_id=name_to_id,
        valid_edge_ids=valid_edge_ids,
    )

    nodes = build_nodes(graph, id_to_name)
    edges = build_edges(graph, edge_messages)

    payload = {
        "nodes": nodes,
        "edges": edges,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"graph_data.json generated at: {OUTPUT_PATH}")
    print(f"Nodes: {len(nodes)}")
    print(f"Edges: {len(edges)}")


if __name__ == "__main__":
    main()