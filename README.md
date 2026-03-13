# Email Graph Backend

Backend en FastAPI para visualizar un grafo social de correos electrónicos.

## Estructura

- `preprocess/build_graph_json.py`: genera `data/graph_data.json`
- `app/main.py`: API REST
- `app/schemas.py`: modelos Pydantic
- `app/data_loader.py`: carga e indexa el JSON en memoria

## Datos esperados en `data/`

- `epstein_social_graph_cleaned.gml`
- `epstein_name_id_mapping.csv`
- `epstein_emails.csv`

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt