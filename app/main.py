from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.data_loader import GraphDataStore
from app.schemas import (
    EdgeDetailModel,
    EdgeModel,
    GraphModel,
    HealthModel,
    NodeDetailModel,
    NodeModel,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "graph_data.json"

store = GraphDataStore(DATA_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    store.load()
    yield


app = FastAPI(
    title="Email Graph API",
    version="1.0.0",
    description="API REST para visualizar un grafo social de correos electrónicos.",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Reemplaza por tu dominio si luego quieres restringirlo
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthModel)
def health() -> HealthModel:
    return HealthModel(
        status="ok",
        nodes=len(store.nodes),
        edges=len(store.edges),
    )


@app.get("/graph")
def get_graph(min_weight: float = 1.0):
    filtered_edges = [e for e in store.edges if e["weight"] >= min_weight]
    valid_node_ids = set()
    for e in filtered_edges:
        valid_node_ids.add(e["source"])
        valid_node_ids.add(e["target"])
    filtered_nodes = [n for n in store.nodes if n["id"] in valid_node_ids]
    return {"nodes": filtered_nodes, "edges": filtered_edges}


@app.get("/nodes", response_model=List[NodeModel])
def get_nodes() -> List[NodeModel]:
    return [NodeModel(**node) for node in store.get_nodes()]


@app.get("/nodes/{node_id}", response_model=NodeDetailModel)
def get_node(node_id: str) -> NodeDetailModel:
    node = store.get_node(node_id)
    if node is None:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return NodeDetailModel(**node)


@app.get("/edges", response_model=List[EdgeModel])
def get_edges() -> List[EdgeModel]:
    return [
        EdgeModel(
            id=edge["id"],
            source=edge["source"],
            target=edge["target"],
            weight=edge["weight"],
        )
        for edge in store.get_edges()
    ]


@app.get("/edges/{edge_id}", response_model=EdgeDetailModel)
def get_edge(edge_id: str) -> EdgeDetailModel:
    edge = store.get_edge(edge_id)
    if edge is None:
        raise HTTPException(status_code=404, detail=f"Edge '{edge_id}' not found")
    return EdgeDetailModel(**edge)


@app.get("/search/nodes", response_model=List[NodeModel])
def search_nodes(q: str = Query(..., min_length=1, description="Texto a buscar")) -> List[NodeModel]:
    return [NodeModel(**node) for node in store.search_nodes(q)]