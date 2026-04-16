# ============================================================
# DASHBOARD E-COMMERCE - SYSTÈME DÉCISIONNEL
# Projet L3 S3 - Base MySQL ecommerce_source
# ============================================================
# Installer les dépendances :
#   pip install dash dash-bootstrap-components plotly pandas sqlalchemy pymysql
#
# Lancer :
#   python dashboard_ecommerce.py
# ============================================================

import dash
from dash import dcc, html, Input, Output, callback, State
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import numpy as np
import sys

import os # N'oublie pas d'ajouter cet import en haut du fichier


def query(sql, params=None):
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)
    

def clean_dataframe(df, *columns):
    """Nettoie les colonnes en remplaçant NaN/None par 'Non défini'"""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].fillna("Non défini")
            df[col] = df[col].replace([np.nan, None, ""], "Non défini")
            df[col] = df[col].astype(str)
    return df

# ============================================================
# CONFIGURATION DE LA BASE DE DONNÉES
# ============================================================

def get_engine():
    # 1. On vérifie d'abord si Render nous a donné une URL (DATABASE_URL)
    env_url = os.getenv('DATABASE_URL')
    
    if env_url:
        # Si on est sur Render, on utilise l'URL d'Aiven directement
        return create_engine(env_url)
    
    # 2. Sinon, on utilise ta configuration locale (sur ton EliteBook)
    DB_CONFIG = {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "Mano@2005",
        "database": "ecommerce_source",
    }
    
    user_encoded = quote_plus(DB_CONFIG['user'])
    password_encoded = quote_plus(DB_CONFIG['password'])
    
    url = (
        f"mysql+pymysql://{user_encoded}:{password_encoded}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset=utf8mb4"
    )
    return create_engine(url)

# ============================================================
# REQUÊTES DE DONNÉES
# ============================================================

def get_kpi_global():
    df = query("""
        SELECT
            COUNT(DISTINCT c.id_commande)                          AS nb_commandes,
            SUM(lc.sous_total)                                      AS ca_total,
            SUM((lc.prix_unitaire - p.cout_achat) * lc.quantite)   AS marge_brute,
            COUNT(DISTINCT c.id_client)                             AS nb_clients_actifs,
            AVG(c.montant_total)                                    AS panier_moyen
        FROM commandes c
        JOIN lignes_commandes lc ON c.id_commande = lc.id_commande
        JOIN produits p          ON lc.id_produit  = p.id_produit
        WHERE c.statut_commande != 'Annulée'
    """)
    return df.iloc[0]


def get_ca_mensuel():
    df = query("""
        SELECT
            DATE_FORMAT(c.date_commande, '%Y-%m') AS mois,
            SUM(lc.sous_total)                    AS ca,
            SUM((lc.prix_unitaire - p.cout_achat) * lc.quantite) AS marge
        FROM commandes c
        JOIN lignes_commandes lc ON c.id_commande = lc.id_commande
        JOIN produits p          ON lc.id_produit  = p.id_produit
        WHERE c.statut_commande != 'Annulée'
        GROUP BY mois
        ORDER BY mois
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'mois')
    return df


def get_top_produits():
    df = query("""
        SELECT
            p.nom_produit,
            COALESCE(cat.nom_categorie, 'Non défini') as nom_categorie,
            SUM(lc.quantite)   AS qte_vendue,
            SUM(lc.sous_total) AS ca
        FROM lignes_commandes lc
        JOIN produits p    ON lc.id_produit   = p.id_produit
        LEFT JOIN categories cat ON p.id_categorie = cat.id_categorie
        JOIN commandes c   ON lc.id_commande  = c.id_commande
        WHERE c.statut_commande != 'Annulée'
        GROUP BY p.id_produit, p.nom_produit, p.id_categorie, cat.nom_categorie
        ORDER BY ca DESC
        LIMIT 15
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'nom_categorie', 'nom_produit')
    return df


def get_ca_par_categorie():
    df = query("""
        SELECT
            COALESCE(cat.nom_categorie, 'Non défini') as nom_categorie,
            SUM(lc.sous_total)                                      AS ca,
            SUM((lc.prix_unitaire - p.cout_achat) * lc.quantite)   AS marge
        FROM lignes_commandes lc
        JOIN produits p    ON lc.id_produit   = p.id_produit
        LEFT JOIN categories cat ON p.id_categorie = cat.id_categorie
        JOIN commandes c   ON lc.id_commande  = c.id_commande
        WHERE c.statut_commande != 'Annulée'
        GROUP BY p.id_categorie, cat.nom_categorie
        ORDER BY ca DESC
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'nom_categorie')
    return df


def get_clients_par_segment():
    df = query("""
        SELECT COALESCE(segment, 'Non défini') as segment, COUNT(*) AS nb_clients
        FROM clients
        GROUP BY segment
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'segment')
    return df


def get_ca_par_canal():
    df = query("""
        SELECT
            cv.id_canal,
            COALESCE(cv.nom_canal, 'Non défini') as nom_canal,
            COALESCE(cv.type_canal, 'Non défini') as type_canal,
            SUM(lc.sous_total) AS ca,
            COUNT(DISTINCT c.id_commande) AS nb_commandes
        FROM commandes c
        JOIN canaux_vente cv    ON c.id_canal    = cv.id_canal
        JOIN lignes_commandes lc ON c.id_commande = lc.id_commande
        WHERE c.statut_commande != 'Annulée'
        GROUP BY cv.id_canal, cv.nom_canal, cv.type_canal
        ORDER BY ca DESC
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'type_canal', 'nom_canal')
    return df


def get_ca_par_paiement():
    df = query("""
        SELECT
            mp.id_moyen,
            COALESCE(mp.nom_moyen, 'Non défini') as nom_moyen,
            COALESCE(mp.type_moyen, 'Non défini') as type_moyen,
            SUM(lc.sous_total)            AS ca,
            COUNT(DISTINCT c.id_commande) AS nb_commandes
        FROM commandes c
        JOIN moyens_paiement mp   ON c.id_moyen_paiement = mp.id_moyen
        JOIN lignes_commandes lc  ON c.id_commande       = lc.id_commande
        WHERE c.statut_commande != 'Annulée'
        GROUP BY mp.id_moyen, mp.nom_moyen, mp.type_moyen
        ORDER BY ca DESC
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'type_moyen', 'nom_moyen')
    return df


def get_clients_par_region():
    df = query("""
        SELECT
            COALESCE(zg.region, 'Non défini') as region,
            COUNT(DISTINCT c.id_client)  AS nb_clients,
            SUM(lc.sous_total)           AS ca
        FROM commandes c
        JOIN clients cl             ON c.id_client   = cl.id_client
        JOIN zones_geographiques zg ON cl.id_zone    = zg.id_zone
        JOIN lignes_commandes lc    ON c.id_commande = lc.id_commande
        WHERE c.statut_commande != 'Annulée'
        GROUP BY zg.region
        ORDER BY ca DESC
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'region')
    return df


def get_livraisons_transporteur():
    df = query("""
        SELECT
            t.id_transporteur,
            COALESCE(t.nom_transporteur, 'Non défini') as nom_transporteur,
            COALESCE(t.type_service, 'Non défini') as type_service,
            COUNT(*)                                                        AS nb_livraisons,
            AVG(DATEDIFF(l.date_livraison_reelle, l.date_expedition))       AS delai_moyen,
            SUM(CASE WHEN l.date_livraison_reelle > l.date_livraison_prevue THEN 1 ELSE 0 END) AS nb_retards,
            AVG(l.cout_livraison)                                           AS cout_moyen
        FROM livraisons l
        JOIN transporteurs t ON l.id_transporteur = t.id_transporteur
        WHERE l.date_livraison_reelle IS NOT NULL
        GROUP BY t.id_transporteur, t.nom_transporteur, t.type_service
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'type_service', 'nom_transporteur')
    return df


def get_statuts_commandes():
    df = query("""
        SELECT COALESCE(statut_commande, 'Non défini') as statut_commande, COUNT(*) AS nb
        FROM commandes
        GROUP BY statut_commande
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'statut_commande')
    return df


def get_avis_moyens():
    df = query("""
        SELECT
            p.id_produit,
            p.nom_produit,
            COALESCE(cat.nom_categorie, 'Non défini') as nom_categorie,
            ROUND(AVG(a.note), 2) AS note_moyenne,
            COUNT(*)              AS nb_avis
        FROM avis_clients a
        JOIN produits p    ON a.id_produit   = p.id_produit
        LEFT JOIN categories cat ON p.id_categorie = cat.id_categorie
        GROUP BY p.id_produit, p.nom_produit, p.id_categorie, cat.nom_categorie
        HAVING nb_avis >= 2
        ORDER BY note_moyenne DESC
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'nom_categorie', 'nom_produit')
    return df


def get_campagnes():
    df = query("""
        SELECT
            nom_campagne, COALESCE(type_canal, 'Non défini') as type_canal, budget,
            nb_clics, nb_conversions, chiffre_affaires_genere,
            ROUND(chiffre_affaires_genere / NULLIF(budget, 0), 2) AS roi,
            ROUND(nb_conversions / NULLIF(nb_clics, 0) * 100, 2)  AS taux_conversion
        FROM campagnes_marketing
        WHERE statut = 'Terminée'
        ORDER BY chiffre_affaires_genere DESC
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'type_canal', 'nom_campagne')
    return df


def get_acquisition_clients():
    df = query("""
        SELECT COALESCE(canal_acquisition, 'Non défini') as canal_acquisition, COUNT(*) AS nb_clients
        FROM clients
        GROUP BY canal_acquisition
        ORDER BY nb_clients DESC
    """)
    # Nettoyer robustement les données
    df = clean_dataframe(df, 'canal_acquisition')
    return df


# ============================================================
# PALETTE & STYLE
# ============================================================
COULEURS = {
    "primaire":   "#0A2342",
    "accent":     "#E8871E",
    "clair":      "#F5F7FA",
    "texte":      "#1A1A2E",
    "vert":       "#27AE60",
    "rouge":      "#E74C3C",
    "bleu_clair": "#3498DB",
    "gris":       "#7F8C8D",
}

PALETTE_GRAPHE = [
    "#0A2342", "#E8871E", "#27AE60", "#3498DB",
    "#9B59B6", "#E74C3C", "#1ABC9C", "#F39C12",
]

TEMPLATE_PLOTLY = dict(
    layout = dict(
    # La police globale reste ici, elle s'applique à tout le texte qui n'est pas surchargé ailleurs
    font=dict(family="'Inter', '-apple-system', 'Segoe UI', sans-serif", color=COULEURS["texte"], size=12),
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    colorway=PALETTE_GRAPHE,
    
    # Pour les axes, on sépare bien les propriétés de la ligne et de la police des graduations (tickfont)
    xaxis=dict(
        showgrid=True, 
        gridcolor="#E8ECF0", 
        linecolor="#E8ECF0", 
        showline=False,
        tickfont=dict(size=11) # Si vous voulez changer la taille des chiffres sur l'axe X
    ),
    yaxis=dict(
        showgrid=True, 
        gridcolor="#E8ECF0", 
        linecolor="#E8ECF0", 
        showline=False,
        tickfont=dict(size=11) # Si vous voulez changer la taille des chiffres sur l'axe Y
    ),
    
    legend=dict(
        bgcolor="rgba(0,0,0,0)", 
        font=dict(size=11)
    ),
    margin=dict(l=50, r=30, t=50, b=50),
    )
)


def fmt_fcfa(val):
    if val >= 1_000_000:
        return f"{val/1_000_000:.1f}M FCFA"
    if val >= 1_000:
        return f"{val/1_000:.0f}k FCFA"
    return f"{val:,.0f} FCFA"


# ============================================================
# COMPOSANTS RÉUTILISABLES
# ============================================================

def kpi_card(titre, valeur, symbole, couleur_icone="#E8871E", sous_texte=""):
    return dbc.Card(
        dbc.CardBody([
            html.Div([
                html.Div([
                    html.Div(symbole, style={
                        "fontSize": "1.5rem",
                        "fontWeight": "600",
                        "color": couleur_icone,
                        "width": "50px",
                        "height": "50px",
                        "display": "flex",
                        "alignItems": "center",
                        "justifyContent": "center",
                        "background": f"rgba({int(couleur_icone[1:3], 16)},{int(couleur_icone[3:5], 16)},{int(couleur_icone[5:7], 16)},0.1)",
                        "borderRadius": "8px"
                    }),
                    html.Div([
                        html.P(titre, className="mb-1",
                               style={"fontSize": ".75rem", "color": COULEURS["gris"],
                                      "textTransform": "uppercase", "letterSpacing": "0.5px",
                                      "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
                                      "fontWeight": "600"}),
                        html.H4(valeur, className="mb-0",
                                style={"fontWeight": "700", "color": COULEURS["primaire"],
                                       "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
                                       "fontSize": "1.5rem"}),
                        html.Small(sous_texte, style={"color": COULEURS["gris"], "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif"}) if sous_texte else html.Span(),
                    ], style={"marginLeft": "16px", "flex": "1"}),
                ], style={"display": "flex", "alignItems": "center"}),
            ]),
        ]),
        style={
            "borderRadius": "12px",
            "border": "1px solid #E8ECF0",
            "boxShadow": "0 1px 3px rgba(10,35,66,.06)",
            "background": "#fff",
            "padding": "20px"
        },
        className="mb-3",
    )


def create_filter_bar():
    """Crée une barre de filtres réutilisable"""
    return dbc.Card([
        dbc.CardBody([
            dbc.Row([
                dbc.Col([
                    html.Label("Période début", style={"fontWeight": "600", "fontSize": ".85rem", "marginBottom": "8px"}),
                    dcc.DatePickerSingle(
                        id="filter-start-date",
                        date="2023-01-01",
                        display_format="YYYY-MM-DD",
                        style={"width": "100%"}
                    ),
                ], md=3),
                dbc.Col([
                    html.Label("Période fin", style={"fontWeight": "600", "fontSize": ".85rem", "marginBottom": "8px"}),
                    dcc.DatePickerSingle(
                        id="filter-end-date",
                        date="2025-12-31",
                        display_format="YYYY-MM-DD",
                        style={"width": "100%"}
                    ),
                ], md=3),
                dbc.Col([
                    html.Label("Statut commande", style={"fontWeight": "600", "fontSize": ".85rem", "marginBottom": "8px"}),
                    dcc.Dropdown(
                        id="filter-statut",
                        options=[
                            {"label": "Tous", "value": "all"},
                            {"label": "Livrée", "value": "Livrée"},
                            {"label": "En attente", "value": "En attente"},
                            {"label": "Annulée", "value": "Annulée"},
                        ],
                        value="all",
                        clearable=False,
                        style={"width": "100%"}
                    ),
                ], md=3),
                dbc.Col([
                    html.Label("Action", style={"fontWeight": "600", "fontSize": ".85rem", "marginBottom": "8px"}),
                    dbc.Button("Appliquer filtres", id="apply-filters", color="primary", className="w-100"),
                ], md=3),
            ], style={"gap": "16px"}),
        ], style={"padding": "16px", "background": "#f9fafb"})
    ], style={"borderBottom": f"2px solid {COULEURS['accent']}", "marginBottom": "20px"})


def section_title(texte):
    return html.Div([
        html.H5(texte, style={
            "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
            "color": COULEURS["primaire"],
            "fontWeight": "700",
            "fontSize": "1.1rem",
            "borderLeft": f"4px solid {COULEURS['accent']}",
            "paddingLeft": "16px",
            "marginBottom": "20px",
            "marginTop": "16px",
            "letterSpacing": "0.3px",
        })
    ])


def get_cumulative_ca():
    """Retourne l'évolution cumulative du CA"""
    df = query("""
        SELECT DATE_FORMAT(c.date_commande, '%Y-%m') AS mois,
               SUM(lc.sous_total) AS ca
        FROM commandes c
        JOIN lignes_commandes lc ON c.id_commande = lc.id_commande
        WHERE c.statut_commande != 'Annulée'
        GROUP BY mois
        ORDER BY mois
    """)
    if not df.empty:
        df = clean_dataframe(df, 'mois')
        df['ca_cumul'] = df['ca'].cumsum()
    return df


def get_top_bottom_produits():
    """Retourne les top et bottom 5 produits"""
    df = query("""
        SELECT p.nom_produit, SUM(lc.sous_total) AS ca, SUM(lc.quantite) AS qte
        FROM lignes_commandes lc
        JOIN produits p ON lc.id_produit = p.id_produit
        JOIN commandes c ON lc.id_commande = c.id_commande
        WHERE c.statut_commande != 'Annulée'
        GROUP BY p.id_produit, p.nom_produit
        ORDER BY ca DESC
    """)
    if not df.empty:
        df = clean_dataframe(df, 'nom_produit')
    return df
    return html.Div([
        html.H5(texte, style={
            "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
            "color": COULEURS["primaire"],
            "fontWeight": "700",
            "fontSize": "1.1rem",
            "borderLeft": f"4px solid {COULEURS['accent']}",
            "paddingLeft": "16px",
            "marginBottom": "20px",
            "marginTop": "16px",
            "letterSpacing": "0.3px",
        })
    ])


# ============================================================
# LAYOUT PRINCIPAL
# ============================================================

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    title="Dashboard E-Commerce SN",
    suppress_callback_exceptions=True,
)

TABS = [
    {"label": "Vue globale",      "value": "global"},
    {"label": "Produits",         "value": "produits"},
    {"label": "Clients",          "value": "clients"},
    {"label": "Livraisons",       "value": "livraisons"},
    {"label": "Marketing",        "value": "marketing"},
]

app.layout = html.Div([
    # ---- HEADER ----
    html.Div([
        html.Div([
            html.H2("E-Commerce Analytics", style={
                "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
                "color": "#fff",
                "fontWeight": "700",
                "marginBottom": "4px",
                "fontSize": "1.8rem",
                "letterSpacing": "-0.5px",
            }),
            html.P("Système d'Information Décisionnel pour le e-commerce au Sénégal",
                   style={"color": "rgba(255,255,255,.7)", "fontSize": ".9rem", "marginBottom": 0,
                          "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
                          "fontWeight": "400"}),
        ]),
        html.Div([
            html.Span("SÉNÉGAL", style={
                "background": COULEURS["accent"],
                "color": "#fff",
                "padding": "6px 16px",
                "borderRadius": "6px",
                "fontSize": ".75rem",
                "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
                "fontWeight": "600",
                "letterSpacing": "0.5px",
            })
        ]),
    ], style={
        "background": f"linear-gradient(135deg, {COULEURS['primaire']} 0%, #16395e 100%)",
        "padding": "28px 32px",
        "display": "flex",
        "justifyContent": "space-between",
        "alignItems": "center",
        "boxShadow": "0 2px 8px rgba(10,35,66,.12)",
    }),

    # ---- NAVIGATION TABS ----
    html.Div([
        dcc.Tabs(
            id="tabs-nav",
            value="global",
            children=[
                dcc.Tab(label=t["label"], value=t["value"],
                        style={
                            "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
                            "fontWeight": "500",
                            "fontSize": ".9rem",
                            "color": COULEURS["gris"],
                            "borderBottom": "3px solid transparent",
                            "padding": "16px 24px",
                            "background": "transparent",
                            "border": "none",
                        },
                        selected_style={
                            "fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif",
                            "fontWeight": "600",
                            "fontSize": ".9rem",
                            "color": COULEURS["primaire"],
                            "borderBottom": f"3px solid {COULEURS['accent']}",
                            "padding": "16px 24px",
                            "background": "transparent",
                            "border": "none",
                        })
                for t in TABS
            ],
            style={"borderBottom": f"1px solid #E8ECF0"},
        ),
    ], style={"background": "#fff", "padding": "0 24px"}),

    # ---- CONTENU ----
    html.Div(id="tab-content", style={"padding": "32px 32px", "background": COULEURS["clair"], "minHeight": "80vh"}),

], style={"fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif", "background": COULEURS["clair"]})


# ============================================================
# CALLBACKS
# ============================================================

@callback(Output("tab-content", "children"), Input("tabs-nav", "value"))
def render_tab(tab):
    try:
        print(f"[LOG] Rendering tab: {tab}", file=sys.stderr)
        if tab == "global":
            return render_global()
        elif tab == "produits":
            print("[LOG] Entering render_produits", file=sys.stderr)
            return render_produits()
        elif tab == "clients":
            print("[LOG] Entering render_clients", file=sys.stderr)
            return render_clients()
        elif tab == "livraisons":
            print("[LOG] Entering render_livraisons", file=sys.stderr)
            return render_livraisons()
        elif tab == "marketing":
            print("[LOG] Entering render_marketing", file=sys.stderr)
            return render_marketing()
        return html.P("Onglet inconnu")
    except Exception as e:
        print(f"[ERROR] {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return dbc.Alert(f"Erreur : {str(e)}", color="danger")


# ============================================================
# CALLBACK POUR FILTRES
# ============================================================

@callback(
    Output("store-filters", "data"),
    Input("apply-filters", "n_clicks"),
    [State("filter-start-date", "date"),
     State("filter-end-date", "date"),
     State("filter-statut", "value")],
    prevent_initial_call=True
)
def apply_filters(n_clicks, start_date, end_date, statut):
    return {
        "start_date": start_date,
        "end_date": end_date,
        "statut": statut
    }


# ============================================================
# ONGLET 1 : VUE GLOBALE
# ============================================================

def render_global():
    try:
        kpi = get_kpi_global()
        df_mensuel = get_ca_mensuel()
        df_statuts = get_statuts_commandes()
        df_canal   = get_ca_par_canal()
        df_cumul = get_cumulative_ca()
        df_produits = get_top_bottom_produits()

        # Sécuriser les données du dataframe canal
        if "type_canal" not in df_canal.columns:
            df_canal["type_canal"] = "Non défini"
        else:
            df_canal["type_canal"] = df_canal["type_canal"].fillna("Non défini")
        
        # Vérifier que les colonnes essentielles existent
        if df_canal.empty:
            df_canal = pd.DataFrame({"nom_canal": ["Pas de données"], "type_canal": ["N/A"], "ca": [0]})

        # KPI cards
        row_kpi = dbc.Row([
            dbc.Col(kpi_card("Chiffre d'affaires", fmt_fcfa(kpi["ca_total"]),   "$", COULEURS["accent"]), md=3),
            dbc.Col(kpi_card("Marge brute",        fmt_fcfa(kpi["marge_brute"]), "↑", COULEURS["vert"]),  md=3),
            dbc.Col(kpi_card("Commandes",          f'{int(kpi["nb_commandes"]):,}', "#", COULEURS["bleu_clair"]), md=3),
            dbc.Col(kpi_card("Panier moyen",       fmt_fcfa(kpi["panier_moyen"]), "∅", COULEURS["primaire"]), md=3),
        ], className="mb-4")

        # ===== Graphique 1 : CA & Marge en Combined Chart =====
        fig_ca = go.Figure()
        fig_ca.add_trace(go.Bar(
            x=df_mensuel["mois"], y=df_mensuel["ca"],
            name="Chiffre d'affaires", marker_color=COULEURS["primaire"], opacity=0.8,
            yaxis="y1", hovertemplate="<b>%{x}</b><br>CA: %{y:,.0f} FCFA<extra></extra>",
        ))
        fig_ca.add_trace(go.Scatter(
            x=df_mensuel["mois"], y=df_mensuel["marge"],
            name="Marge brute", mode="lines+markers", yaxis="y2",
            line=dict(color=COULEURS["accent"], width=3),
            marker=dict(size=8, symbol="circle"),
            hovertemplate="<b>%{x}</b><br>Marge: %{y:,.0f} FCFA<extra></extra>",
        ))
        layout_ca = TEMPLATE_PLOTLY["layout"].copy()
        layout_ca["yaxis"] = dict(title=dict(text="Chiffre d'affaires (FCFA)", font=dict(color=COULEURS["primaire"])))
        layout_ca["yaxis2"] = dict(title=dict(text="Marge brute (FCFA)", font=dict(color=COULEURS["accent"])),
                                   overlaying="y", side="right")
        fig_ca.update_layout(title_text="Évolution mensuelle du CA et Marge",
                             **layout_ca)

        # ===== Graphique 2 : CA Cumulatif =====
        if not df_cumul.empty:
            fig_cumul = go.Figure()
            fig_cumul.add_trace(go.Scatter(
                x=df_cumul["mois"], y=df_cumul["ca_cumul"],
                fill="tozeroy", name="CA Cumulatif",
                line=dict(color=COULEURS["vert"], width=3),
                marker=dict(size=8),
                hovertemplate="<b>%{x}</b><br>CA Cumulatif: %{y:,.0f} FCFA<extra></extra>",
            ))
            fig_cumul.update_layout(title_text="Évolution cumulative du CA",
                                   yaxis_title="CA Cumulatif (FCFA)",
                                   **TEMPLATE_PLOTLY["layout"])
        else:
            fig_cumul = None

        # ===== Graphique 3 : Statuts en Donut =====
        fig_statuts = px.pie(
            df_statuts, names="statut_commande", values="nb",
            color_discrete_sequence=PALETTE_GRAPHE,
            hole=0.45,
            title="Distribution des statuts de commande",
        )
        fig_statuts.update_layout(**TEMPLATE_PLOTLY["layout"])
        fig_statuts.update_traces(textinfo="percent+label", textposition="auto",
                                 hovertemplate="<b>%{label}</b><br>Commandes: %{value}<extra></extra>")

        # ===== Graphique 4 : Top 5 vs Bottom 5 Produits =====
        top_5 = df_produits.head(5)
        bottom_5 = df_produits.tail(5)
        
        fig_products = go.Figure()
        fig_products.add_trace(go.Bar(
            y=top_5["nom_produit"], x=top_5["ca"], orientation="h",
            name="Top 5", marker_color=COULEURS["vert"],
            hovertemplate="<b>%{y}</b><br>CA: %{x:,.0f} FCFA<extra></extra>",
        ))
        fig_products.add_trace(go.Bar(
            y=bottom_5["nom_produit"], x=bottom_5["ca"], orientation="h",
            name="Bottom 5", marker_color=COULEURS["rouge"],
            hovertemplate="<b>%{y}</b><br>CA: %{x:,.0f} FCFA<extra></extra>",
        ))
        fig_products.update_layout(title_text="Top 5 vs Bottom 5 Produits",
                                  xaxis_title="CA (FCFA)",
                                  barmode="group",
                                  **TEMPLATE_PLOTLY["layout"])

        # ===== Graphique 5 : CA par canal (Horizontal Bar) =====
        fig_canal = px.bar(
            df_canal.sort_values("ca"),
            x="ca", y="nom_canal", orientation="h",
            color_discrete_sequence=[COULEURS["primaire"]],
            title="Chiffre d'affaires par canal",
            labels={"ca": "CA (FCFA)", "nom_canal": ""},
        )
        fig_canal.update_layout(**TEMPLATE_PLOTLY["layout"])
        fig_canal.update_traces(hovertemplate="<b>%{y}</b><br>CA: %{x:,.0f} FCFA<extra></extra>")

        # ===== Graphique 6 : Performance par canal (Scatter) =====
        fig_perf = px.scatter(
            df_canal, x="nb_commandes", y="ca",
            size="nb_commandes", hover_name="nom_canal",
            color_discrete_sequence=[COULEURS["accent"]],
            title="Performance : Volume vs Chiffre d'affaires par canal",
            labels={"nb_commandes": "Nombre de commandes", "ca": "CA (FCFA)"},
        )
        fig_perf.update_layout(**TEMPLATE_PLOTLY["layout"])
        fig_perf.update_traces(marker=dict(size=12, opacity=0.6),
                              hovertemplate="<b>%{hovertext}</b><br>Commandes: %{x}<br>CA: %{y:,.0f} FCFA<extra></extra>")

        # ===== Graphique 7 : Heatmap - CA par mois et statut =====
        try:
            df_pivot = query("""
                SELECT DATE_FORMAT(c.date_commande, '%Y-%m') AS mois,
                       c.statut_commande, SUM(lc.sous_total) AS ca
                FROM commandes c
                JOIN lignes_commandes lc ON c.id_commande = lc.id_commande
                GROUP BY mois, c.statut_commande
                ORDER BY mois, c.statut_commande
            """)
            if not df_pivot.empty:
                pivot = df_pivot.pivot_table(index="statut_commande", columns="mois", values="ca", fill_value=0, aggfunc='sum')
                fig_heatmap = px.imshow(
                    pivot, color_continuous_scale="YlOrRd",
                    title="Heatmap : CA par statut et mois",
                    labels=dict(x="Mois", y="Statut", color="CA (FCFA)"),
                    text_auto=".0f",
                )
                fig_heatmap.update_layout(**TEMPLATE_PLOTLY["layout"], height=400)
            else:
                fig_heatmap = None
        except:
            fig_heatmap = None

        return html.Div([
            # Barre de filtres
            create_filter_bar(),
            
            # KPI cards
            row_kpi,
            
            # Ligne 1 : CA & Marge + Statuts
            dbc.Row([
                dbc.Col([section_title("TENDANCE MENSUELLE"), dcc.Graph(figure=fig_ca, config={"displayModeBar": False})], md=7),
                dbc.Col([section_title("STATUTS"), dcc.Graph(figure=fig_statuts, config={"displayModeBar": False})], md=5),
            ], className="mb-4"),
            
            # Ligne 2 : CA Cumulatif + Top/Bottom Produits
            dbc.Row([
                dbc.Col([section_title("CA CUMULATIF"), 
                        dcc.Graph(figure=fig_cumul, config={"displayModeBar": False}) if fig_cumul else html.P("Données insuffisantes")], md=6),
                dbc.Col([section_title("TOP vs BOTTOM PRODUITS"), 
                        dcc.Graph(figure=fig_products, config={"displayModeBar": False})], md=6),
            ], className="mb-4"),
            
            # Ligne 3 : Canaux
            dbc.Row([
                dbc.Col([section_title("PERFORMANCE PAR CANAL"), dcc.Graph(figure=fig_canal, config={"displayModeBar": False})], md=6),
                dbc.Col([section_title("ANALYSE VOLUME vs CA"), dcc.Graph(figure=fig_perf, config={"displayModeBar": False})], md=6),
            ], className="mb-4"),
            
            # Ligne 4 : Heatmap
            dbc.Row([
                dbc.Col([section_title("HEATMAP TEMPORELLE"), 
                        dcc.Graph(figure=fig_heatmap, config={"displayModeBar": False}) if fig_heatmap else html.P("Données insuffisantes")], 
                        md=12),
            ]),
        ])
    except Exception as e:
        return dbc.Alert(f"Erreur dans l'onglet Vue globale : {str(e)}", color="danger")


# ============================================================
# ONGLET 2 : PRODUITS
# ============================================================

def render_produits():
    try:
        df_top  = get_top_produits()
        df_cat  = get_ca_par_categorie()
        df_avis = get_avis_moyens()

        # Sécuriser les données en remplissant les colonnes manquantes
        if "nom_categorie" not in df_top.columns:
            df_top["nom_categorie"] = "Non défini"
        else:
            df_top["nom_categorie"] = df_top["nom_categorie"].fillna("Non défini")
            
        if "nom_categorie" not in df_cat.columns:
            df_cat["nom_categorie"] = "Non défini"
        else:
            df_cat["nom_categorie"] = df_cat["nom_categorie"].fillna("Non défini")

        # Top produits CA
        fig_top = px.bar(
            df_top.sort_values("ca").tail(10),
            x="ca", y="nom_produit", orientation="h",
            color_discrete_sequence=[COULEURS["primaire"]],
            title="Top 10 produits par CA",
            labels={"ca": "CA (FCFA)", "nom_produit": ""},
        )
        fig_top.update_layout(**TEMPLATE_PLOTLY["layout"])

        # CA vs Marge par catégorie
        fig_cat = go.Figure()
        fig_cat.add_trace(go.Bar(
            x=df_cat["nom_categorie"], y=df_cat["ca"],
            name="CA", marker_color=COULEURS["primaire"],
        ))
        fig_cat.add_trace(go.Bar(
            x=df_cat["nom_categorie"], y=df_cat["marge"],
            name="Marge brute", marker_color=COULEURS["accent"],
        ))
        layout_cat = TEMPLATE_PLOTLY["layout"].copy()
        fig_cat.update_layout(title="CA & Marge par catégorie", barmode="group",
                              xaxis_tickangle=-30, **layout_cat)

        # Scatter quantités vs CA
        fig_scatter = px.scatter(
            df_top, x="qte_vendue", y="ca",
            size="ca",
            hover_name="nom_produit",
            color_discrete_sequence=[COULEURS["primaire"]],
            title="Volume vendu vs CA (taille = CA)",
            labels={"qte_vendue": "Quantité vendue", "ca": "CA (FCFA)"},
        )
        fig_scatter.update_layout(**TEMPLATE_PLOTLY["layout"])

        # Avis clients
        if not df_avis.empty:
            fig_avis = px.bar(
                df_avis.sort_values("note_moyenne"),
                x="note_moyenne", y="nom_produit", orientation="h",
                color="note_moyenne",
                color_continuous_scale=["#E74C3C", "#F39C12", "#27AE60"],
                range_color=[1, 5],
                title="Note moyenne par produit",
                labels={"note_moyenne": "Note /5", "nom_produit": ""},
            )
            fig_avis.update_layout(**TEMPLATE_PLOTLY["layout"])
            avis_block = dcc.Graph(figure=fig_avis, config={"displayModeBar": False})
        else:
            avis_block = html.P("Pas assez d'avis pour afficher.", style={"color": COULEURS["gris"]})

        return html.Div([
            dbc.Row([
                dbc.Col([section_title("Top produits"),       dcc.Graph(figure=fig_top,    config={"displayModeBar": False})], md=6),
                dbc.Col([section_title("Catégories"),         dcc.Graph(figure=fig_cat,    config={"displayModeBar": False})], md=6),
            ], className="mb-3"),
            dbc.Row([
                dbc.Col([section_title("Volume vs CA"),       dcc.Graph(figure=fig_scatter, config={"displayModeBar": False})], md=7),
                dbc.Col([section_title("Satisfaction produit"), avis_block], md=5),
            ]),
        ])
    except Exception as e:
        return dbc.Alert(f"Erreur dans l'onglet Produits : {str(e)}", color="danger")


# ============================================================
# ONGLET 3 : CLIENTS
# ============================================================

def render_clients():
    try:
        df_seg    = get_clients_par_segment()
        df_region = get_clients_par_region()
        df_canal  = get_acquisition_clients()
        df_paie   = get_ca_par_paiement()

        # Remplir les colonnes manquantes en Python aussi
        if "type_moyen" not in df_paie.columns:
            df_paie["type_moyen"] = "Non défini"
        else:
            df_paie["type_moyen"] = df_paie["type_moyen"].fillna("Non défini")
        
        if "nom_moyen" not in df_paie.columns:
            df_paie["nom_moyen"] = "Non défini"
        else:
            df_paie["nom_moyen"] = df_paie["nom_moyen"].fillna("Non défini")

        # Segments
        fig_seg = px.pie(
            df_seg, names="segment", values="nb_clients",
            color_discrete_sequence=PALETTE_GRAPHE,
            hole=0.4,
            title="Répartition par segment client",
        )
        fig_seg.update_layout(**TEMPLATE_PLOTLY["layout"])
        fig_seg.update_traces(textinfo="percent+label")

        # CA par région
        fig_region = px.bar(
            df_region.sort_values("ca", ascending=True),
            x="ca", y="region", orientation="h",
            color="ca",
            color_continuous_scale=["#D6E4F0", COULEURS["primaire"]],
            title="CA par région",
            labels={"ca": "CA (FCFA)", "region": ""},
        )
        fig_region.update_layout(**TEMPLATE_PLOTLY["layout"])
        fig_region.update_coloraxes(showscale=False)

        # Canal d'acquisition
        fig_acq = px.bar(
            df_canal.sort_values("nb_clients"),
            x="nb_clients", y="canal_acquisition", orientation="h",
            color_discrete_sequence=[COULEURS["primaire"]],
            title="Clients par canal d'acquisition",
            labels={"nb_clients": "Nombre de clients", "canal_acquisition": ""},
        )
        layout_acq = TEMPLATE_PLOTLY["layout"].copy()
        fig_acq.update_layout(showlegend=False, **layout_acq)

        # Moyen de paiement
        fig_paie = px.pie(
            df_paie, names="nom_moyen", values="ca",
            color_discrete_sequence=PALETTE_GRAPHE,
            hole=0.4,
            title="CA par moyen de paiement",
        )
        fig_paie.update_layout(**TEMPLATE_PLOTLY["layout"])
        fig_paie.update_traces(textinfo="percent+label")

        return html.Div([
            dbc.Row([
                dbc.Col([section_title("Segments clients"),       dcc.Graph(figure=fig_seg,    config={"displayModeBar": False})], md=5),
                dbc.Col([section_title("CA par région"),          dcc.Graph(figure=fig_region, config={"displayModeBar": False})], md=7),
            ], className="mb-3"),
            dbc.Row([
                dbc.Col([section_title("Acquisition clients"),    dcc.Graph(figure=fig_acq,    config={"displayModeBar": False})], md=6),
                dbc.Col([section_title("Moyens de paiement"),     dcc.Graph(figure=fig_paie,   config={"displayModeBar": False})], md=6),
            ]),
        ])
    except Exception as e:
        return dbc.Alert(f"Erreur dans l'onglet Clients : {str(e)}", color="danger")


# ============================================================
# ONGLET 4 : LIVRAISONS
# ============================================================

def render_livraisons():
    try:
        df_liv = get_livraisons_transporteur()

        # Remplir les colonnes manquantes si elles n'existent pas
        if "type_service" not in df_liv.columns:
            df_liv["type_service"] = "Non défini"
        else:
            df_liv["type_service"] = df_liv["type_service"].fillna("Non défini")
            
        if "nom_transporteur" not in df_liv.columns:
            df_liv["nom_transporteur"] = "Non défini"
        else:
            df_liv["nom_transporteur"] = df_liv["nom_transporteur"].fillna("Non défini")

        # Délai moyen par transporteur
        fig_delai = px.bar(
            df_liv.sort_values("delai_moyen"),
            x="nom_transporteur", y="delai_moyen",
            color_discrete_sequence=[COULEURS["primaire"]],
            title="Délai moyen de livraison (jours)",
            labels={"delai_moyen": "Jours", "nom_transporteur": ""},
        )
        fig_delai.update_layout(**TEMPLATE_PLOTLY["layout"])

        # Taux de retard
        df_liv["taux_retard_pct"] = (df_liv["nb_retards"] / df_liv["nb_livraisons"] * 100).round(1)
        fig_retard = px.bar(
            df_liv.sort_values("taux_retard_pct", ascending=False),
            x="nom_transporteur", y="taux_retard_pct",
            color_discrete_sequence=[COULEURS["accent"]],
            title="Taux de retard par transporteur (%)",
            labels={"taux_retard_pct": "% retards", "nom_transporteur": ""},
        )
        layout_retard = TEMPLATE_PLOTLY["layout"].copy()
        fig_retard.update_layout(**layout_retard)

        # Coût moyen
        fig_cout = px.bar(
            df_liv.sort_values("cout_moyen"),
            x="cout_moyen", y="nom_transporteur", orientation="h",
            color_discrete_sequence=[COULEURS["primaire"]],
            title="Coût moyen de livraison (FCFA)",
            labels={"cout_moyen": "FCFA", "nom_transporteur": ""},
        )
        fig_cout.update_layout(**TEMPLATE_PLOTLY["layout"])

        # Table récap
        tbl = df_liv[["nom_transporteur", "type_service", "nb_livraisons", "delai_moyen", "taux_retard_pct", "cout_moyen"]].copy()
        tbl.columns = ["Transporteur", "Type", "Livraisons", "Délai moy. (j)", "Retards %", "Coût moy. (FCFA)"]
        tbl["Délai moy. (j)"]    = tbl["Délai moy. (j)"].round(1)
        tbl["Coût moy. (FCFA)"]  = tbl["Coût moy. (FCFA)"].astype(int)

        tableau = dbc.Table.from_dataframe(
            tbl, striped=True, bordered=False, hover=True, responsive=True,
            style={"fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif", "fontSize": ".9rem"},
        )

        return html.Div([
            dbc.Row([
                dbc.Col([section_title("Délais de livraison"),  dcc.Graph(figure=fig_delai,  config={"displayModeBar": False})], md=6),
                dbc.Col([section_title("Taux de retard"),       dcc.Graph(figure=fig_retard, config={"displayModeBar": False})], md=6),
            ], className="mb-3"),
            dbc.Row([
                dbc.Col([section_title("Coût de livraison"),    dcc.Graph(figure=fig_cout,   config={"displayModeBar": False})], md=7),
                dbc.Col([section_title("Tableau récapitulatif"), tableau], md=5),
            ]),
        ])
    except Exception as e:
        return dbc.Alert(f"Erreur dans l'onglet Livraisons : {str(e)}", color="danger")


# ============================================================
# ONGLET 5 : MARKETING
# ============================================================

def render_marketing():
    try:
        import plotly.graph_objects as go
        df_camp = get_campagnes()

        # Ajouter la colonne manquante si elle n'existe pas
        if "type_canal" not in df_camp.columns:
            df_camp["type_canal"] = "Non défini"
        else:
            df_camp["type_canal"] = df_camp["type_canal"].fillna("Non défini")
            
        if "nom_campagne" not in df_camp.columns:
            df_camp["nom_campagne"] = "Non défini"
        else:
            df_camp["nom_campagne"] = df_camp["nom_campagne"].fillna("Non défini")

        # ROI par campagne
        fig_roi = px.bar(
            df_camp.sort_values("roi"),
            x="roi", y="nom_campagne", orientation="h",
            color_discrete_sequence=[COULEURS["primaire"]],
            title="ROI par campagne (CA généré / Budget)",
            labels={"roi": "ROI (x)", "nom_campagne": ""},
        )
        fig_roi.update_layout(**TEMPLATE_PLOTLY["layout"])

        # Budget vs CA généré
        fig_bvca = go.Figure()
        fig_bvca.add_trace(go.Bar(
            x=df_camp["nom_campagne"], y=df_camp["budget"],
            name="Budget", marker_color=COULEURS["gris"], opacity=0.7,
        ))
        fig_bvca.add_trace(go.Bar(
            x=df_camp["nom_campagne"], y=df_camp["chiffre_affaires_genere"],
            name="CA généré", marker_color=COULEURS["accent"],
        ))
        layout_bvca = TEMPLATE_PLOTLY["layout"].copy()
        fig_bvca.update_layout(title="Budget vs CA généré par campagne",
                               barmode="group",
                               xaxis_tickangle=-30, **layout_bvca)


        # 1. Extraction manuelle forcée (on sort totalement du monde Pandas/Narwhals)
        x_data = [float(x) for x in df_camp["nb_clics"].tolist()]
        y_data = [float(y) for y in df_camp["nb_conversions"].tolist()]
        names = [str(n) for n in df_camp["nom_campagne"].tolist()]
        
        # Traitement spécifique pour la taille (size)
        raw_sizes = df_camp["chiffre_affaires_genere"].tolist()
        clean_sizes = []
        for v in raw_sizes:
            try:
                val = float(v)
                clean_sizes.append(max(5, val / 1000)) # On réduit l'échelle pour l'affichage
            except:
                clean_sizes.append(5)

        # 2. Construction du graphique avec Graph Objects (plus stable que Express ici)
        fig_conv = go.Figure(data=[go.Scatter(
            x=x_data,
            y=y_data,
            mode='markers',
            text=names,
            marker=dict(
                size=clean_sizes,
                color=COULEURS["primaire"],
                sizemode='area',
                sizeref=2.*max(clean_sizes)/(40.**2), # Ajuste la taille des bulles
                sizemin=4
            )
        )])

        fig_conv.update_layout(
            title="Clics vs Conversions (taille = CA)",
            xaxis_title="Clics",
            yaxis_title="Conversions",
            **TEMPLATE_PLOTLY["layout"]
        )
        
        # Table campagnes
        tbl = df_camp[["nom_campagne", "type_canal", "budget", "nb_conversions", "roi", "taux_conversion"]].copy()
        tbl.columns = ["Campagne", "Canal", "Budget (FCFA)", "Conversions", "ROI", "Taux conv. (%)"]
        tbl["Budget (FCFA)"] = tbl["Budget (FCFA)"].astype(int)

        tableau = dbc.Table.from_dataframe(
            tbl, striped=True, bordered=False, hover=True, responsive=True,
            style={"fontFamily": "'Inter', '-apple-system', 'Segoe UI', sans-serif", "fontSize": ".9rem"},
        )

        return html.Div([
            dbc.Row([
                dbc.Col([section_title("ROI des campagnes"),    dcc.Graph(figure=fig_roi,  config={"displayModeBar": False})], md=6),
                dbc.Col([section_title("Budget vs CA généré"),  dcc.Graph(figure=fig_bvca, config={"displayModeBar": False})], md=6),
            ], className="mb-3"),
            dbc.Row([
                dbc.Col([section_title("Efficacité des clics"), dcc.Graph(figure=fig_conv, config={"displayModeBar": False})], md=7),
                dbc.Col([section_title("Détail campagnes"),     tableau], md=5),
            ]),
        ])
    except Exception as e:
        return dbc.Alert(f"Erreur dans l'onglet Marketing : {str(e)}", color="danger")


# ============================================================
# LANCEMENT
# ============================================================
if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
    
server = app.server  # Indispensable pour que les hébergeurs trouvent l'application Flask
