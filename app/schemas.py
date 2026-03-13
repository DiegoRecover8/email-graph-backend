from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class MessageModel(BaseModel):
    timestamp: Optional[str] = None
    subject: Optional[str] = None
    sender: str
    recipient: str
    body: Optional[str] = None


class NeighborModel(BaseModel):
    id: str
    label: str
    weight: float = 1.0


class NodeModel(BaseModel):
    id: str
    label: str
    degree: int
    weighted_degree: float


class NodeDetailModel(NodeModel):
    neighbors: List[NeighborModel] = Field(default_factory=list)


class EdgeModel(BaseModel):
    id: str
    source: str
    target: str
    weight: float


class EdgeDetailModel(EdgeModel):
    messages: List[MessageModel] = Field(default_factory=list)


class GraphModel(BaseModel):
    nodes: List[NodeModel]
    edges: List[EdgeDetailModel]


class HealthModel(BaseModel):
    status: str
    nodes: int
    edges: int