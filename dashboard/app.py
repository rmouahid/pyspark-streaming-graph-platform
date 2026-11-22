import glob
import json
import os
import time

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from pyvis.network import Network

from config.settings import (
    DASHBOARD_REFRESH_MS,
    DATA_PATH,
    GRAPH_EDGES_PATH,
    GRAPH_VERTICES_PATH,
)

NODE_COLORS = {"User": "#4C9BE8", "Seller": "#5EBD7A", "Product": "#F4A442"}
REFRESH_S   = DASHBOARD_REFRESH_MS / 1000


def load_graph():
    try:
        vertices = pd.read_csv(GRAPH_VERTICES_PATH)
        edges = pd.read_csv(GRAPH_EDGES_PATH)
    except (OSError, UnicodeError, pd.errors.EmptyDataError, pd.errors.ParserError):
        return None, None

    if not {"id", "type", "label"}.issubset(vertices.columns):
        return None, None
    if not {"src", "dst", "relationship"}.issubset(edges.columns):
        return None, None

    node_ids = vertices["id"]
    return vertices, edges[edges["src"].isin(node_ids) & edges["dst"].isin(node_ids)]


def load_recent_events(n: int = 100) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(DATA_PATH, "*.json")), key=os.path.getmtime)[-n:]
    records = []
    for path in files:
        try:
            with open(path, encoding="utf-8") as f:
                records.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    return pd.DataFrame(records) if records else pd.DataFrame()


def build_pyvis(vertices: pd.DataFrame, edges: pd.DataFrame) -> str:
    net = Network(height="480px", width="100%", directed=True,
                  bgcolor="#0E1117", font_color="white")
    net.set_options('{"physics": {"stabilization": {"iterations": 80}}, "edges": {"font": {"size": 9}}}')
    for _, row in vertices.iterrows():
        net.add_node(row["id"], label=row["id"],
                     color=NODE_COLORS.get(row["type"], "#AAA"),
                     title=f'{row["type"]} — {row["label"]}')
    node_ids = set(vertices["id"])
    for _, row in edges.iterrows():
        if row["src"] in node_ids and row["dst"] in node_ids:
            net.add_edge(row["src"], row["dst"], label=row["relationship"], color="#666666")
    return net.generate_html()


def main():
    st.set_page_config(page_title="Streaming Graph Dashboard", layout="wide")
    st.title("Streaming Graph Platform")

    vertices, edges = load_graph()
    events          = load_recent_events()

    col_graph, col_metrics = st.columns([2, 1])

    with col_graph:
        st.subheader("Graphe en temps réel")
        if vertices is not None and not vertices.empty:
            components.html(build_pyvis(vertices, edges), height=500)
            st.caption(f"{len(vertices)} nœuds · {len(edges)} arêtes")
        else:
            st.info("En attente du pipeline… Lancez `./run.sh`")

    with col_metrics:
        st.subheader("Légende")
        for node_type, color in NODE_COLORS.items():
            st.markdown(f"<span style='color:{color}; font-size:16px'>■</span> {node_type}",
                        unsafe_allow_html=True)

        st.divider()

        if not events.empty and "action_type" in events.columns:
            st.subheader("Actions (100 derniers événements)")
            st.bar_chart(events["action_type"].value_counts())

            st.subheader("Achats par catégorie")
            purchases = events[events["action_type"] == "ACHAT"]
            if not purchases.empty:
                st.dataframe(
                    purchases["product_cat"].value_counts().rename("achats"),
                    use_container_width=True,
                )
        else:
            st.info("Aucun événement reçu.")

    st.caption(f"⟳ Rafraîchissement toutes les {int(REFRESH_S)} secondes")
    time.sleep(REFRESH_S)
    st.rerun()


if __name__ == "__main__":
    main()
