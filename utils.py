import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Verwende nicht-interaktiven Backend
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import io
import base64
import plotly.express as px
import plotly.graph_objects as go
import re
import warnings

# Unterdrücke spezifische FutureWarning von Pandas in Plotly Express
warnings.filterwarnings('ignore', category=FutureWarning, message='.*When grouping with a length-1 list-like.*')

def normalize(val, min_val, max_val):
    if max_val == min_val:
        return 0.5
    return (val - min_val) / (max_val - min_val)

def calculate_target_group(df, min_age, max_age):
    # Eine echte Kopie des DataFrames erstellen
    df_result = df.copy()
    
    # Debug: Verfügbare Spalten anzeigen
    print(f"Available columns in DataFrame for target group calculation: {df_result.columns.tolist()}")
    
    # Mapping von DB-Labels zu internen, einfachen Namen
    # (Passe dies an die exakten Labels in deiner age_group Tabelle an!)
    age_group_label_mapping = {
        'Unter 3 Jahre': 'age_0_2',
        '3 bis 5 Jahre': 'age_3_5',
        '6 bis 10 Jahre': 'age_6_10',
        '11 bis 17 Jahre': 'age_11_17',
        '18 bis 29 Jahre': 'age_18_29',
        '30 bis 39 Jahre': 'age_30_39',
        '40 bis 49 Jahre': 'age_40_49',
        '50 bis 64 Jahre': 'age_50_64',
        '65 und älter': 'age_65_plus'
        # Füge hier weitere Mappings hinzu, falls nötig
    }

    # Spalten umbenennen für einfachere Handhabung
    df_result = df_result.rename(columns=age_group_label_mapping)

    # Liste der internen Altersspaltennamen
    internal_age_columns = list(age_group_label_mapping.values())

    # Sicherstellen, dass alle benötigten (internen) Altersspalten existieren und numerisch sind
    for col in internal_age_columns:
        if col not in df_result.columns:
            print(f"WARNUNG: Erwartete interne Altersspalte '{col}' nicht gefunden. Wird mit 0 ersetzt.")
            df_result[col] = 0
        else:
            # Konvertiere zu numerischem Typ, fülle NaN mit 0
            df_result[col] = pd.to_numeric(df_result[col], errors='coerce').fillna(0)

    # Zielgruppe initialisieren
    df_result["target_group_size"] = 0.0

    # Berechnung unter 18 Jahre
    # Addiere die entsprechenden internen Spalten
    under_18_cols = ['age_0_2', 'age_3_5', 'age_6_10', 'age_11_17']
    # Nur existierende Spalten berücksichtigen
    existing_under_18_cols = [col for col in under_18_cols if col in df_result.columns]
    df_result["under_18_size"] = df_result[existing_under_18_cols].sum(axis=1)
    df_result["under_18_percent"] = df_result["under_18_size"] / df_result["total"]

    # --- Zielgruppenberechnung basierend auf internen Spalten --- 
    # Diese Logik muss eventuell angepasst werden, je nachdem, wie präzise 
    # die Aufteilung sein soll und wie die Altersgruppen genau definiert sind.
    # Annahme: Lineare Verteilung innerhalb der breiteren Gruppen.

    # Beispielhafte Logik (muss überprüft und ggf. verfeinert werden!):
    # Beispiel: Gruppe 'age_18_29' (12 Jahre: 18-29)
    if 'age_18_29' in df_result.columns:
        start = 18
        end = 29
        count_total = df_result['age_18_29']
        overlap_start = max(min_age, start)
        overlap_end = min(max_age, end)
        if overlap_end >= overlap_start:
            overlap_years = overlap_end - overlap_start + 1
            fraction = overlap_years / (end - start + 1)
            df_result["target_group_size"] += count_total * fraction

    # Beispiel: Gruppe 'age_30_39' (10 Jahre: 30-39)
    if 'age_30_39' in df_result.columns:
        start = 30
        end = 39
        count_total = df_result['age_30_39']
        overlap_start = max(min_age, start)
        overlap_end = min(max_age, end)
        if overlap_end >= overlap_start:
            overlap_years = overlap_end - overlap_start + 1
            fraction = overlap_years / (end - start + 1)
            df_result["target_group_size"] += count_total * fraction
            
    # Beispiel: Gruppe 'age_40_49' (10 Jahre: 40-49)
    if 'age_40_49' in df_result.columns:
        start = 40
        end = 49
        count_total = df_result['age_40_49']
        overlap_start = max(min_age, start)
        overlap_end = min(max_age, end)
        if overlap_end >= overlap_start:
            overlap_years = overlap_end - overlap_start + 1
            fraction = overlap_years / (end - start + 1)
            df_result["target_group_size"] += count_total * fraction

    # Beispiel: Gruppe 'age_50_64' (15 Jahre: 50-64)
    if 'age_50_64' in df_result.columns:
        start = 50
        end = 64
        count_total = df_result['age_50_64']
        overlap_start = max(min_age, start)
        overlap_end = min(max_age, end)
        if overlap_end >= overlap_start:
            overlap_years = overlap_end - overlap_start + 1
            fraction = overlap_years / (end - start + 1)
            df_result["target_group_size"] += count_total * fraction

    # Beispiel: Gruppe 'age_65_plus' (Annahme bis z.B. 85 für Berechnung)
    if 'age_65_plus' in df_result.columns:
        start = 65
        end = 85 # Annahme für die Obergrenze
        count_total = df_result['age_65_plus']
        overlap_start = max(min_age, start)
        overlap_end = min(max_age, end)
        if overlap_end >= overlap_start:
            overlap_years = overlap_end - overlap_start + 1
            # Hier ist die Fraktion schwerer zu schätzen, ggf. anpassen
            fraction = overlap_years / (end - start + 1) 
            df_result["target_group_size"] += count_total * fraction

    # Zielgruppenanteil berechnen (verhindere Division durch Null)
    df_result["target_group_percent"] = (df_result["target_group_size"] / df_result["total"]).fillna(0)
    df_result["target_group_percent"] = df_result["target_group_percent"].clip(0, 1) # Stelle sicher, dass der Anteil zwischen 0 und 1 liegt

    # Berechnung sonstige (über der Zielgruppe)
    # Sicherstellen, dass alle Größen numerisch sind und NaN mit 0 füllen
    df_result["total"] = pd.to_numeric(df_result["total"], errors='coerce').fillna(0)
    df_result["target_group_size"] = pd.to_numeric(df_result["target_group_size"], errors='coerce').fillna(0)
    df_result["under_18_size"] = pd.to_numeric(df_result["under_18_size"], errors='coerce').fillna(0)
    
    df_result["others_size"] = df_result["total"] - df_result["target_group_size"] - df_result["under_18_size"]
    # Negative Werte vermeiden
    df_result["others_size"] = df_result["others_size"].clip(lower=0)
    df_result["others_percent"] = (df_result["others_size"] / df_result["total"]).fillna(0)
    
    # Debug: Zeige Ergebnis der Zielgruppenberechnung
    print(df_result[['location_name', 'total', 'target_group_size', 'target_group_percent']].head())
    
    return df_result

def generate_charts(df):
    # Alle Städte nach Score sortieren
    sorted_cities = df.sort_values("score", ascending=False).copy()
    
    # Finde den minimalen Score-Wert für eine bessere Skalierung
    min_score = sorted_cities['score'].min()
    max_score = sorted_cities['score'].max()
    # Starte die Achse etwas unterhalb des minimalen Wertes (ca. 10% unter dem Minimum)
    x_min = max(0, min_score - (max_score - min_score) * 0.1)
    
    # Top 10 für das erste Diagramm
    fig1 = generate_cities_chart(sorted_cities.head(10), 0, x_min, global_max=max_score)
    
    # Kreisdiagramm für die Top-Stadt
    top_city = sorted_cities.iloc[0]
    fig3 = generate_city_pie_chart(top_city)

    return fig1, sorted_cities, fig3

def generate_cities_chart(cities_df, start_rank=0, x_min=None, global_max=None):
    """
    Erstellt ein Balkendiagramm für eine Gruppe von Städten
    
    Args:
        cities_df: DataFrame mit den Städtedaten
        start_rank: Startwert für die Rangzählung (z.B. 0 für Top 10, 10 für Plätze 11-20)
        x_min: Minimalwert für die X-Achse
        global_max: Maximaler Score-Wert aus allen Städten für konsistente Skalierung
    """
    fig, ax = plt.subplots(figsize=(8, 4))
    
    # Höchstens 10 Städte anzeigen
    if len(cities_df) > 10:
        cities_df = cities_df.head(10)
    
    # Erstelle Beschriftungen mit Platzierungen
    labels = [f"{i+start_rank+1}. {city}" for i, city in enumerate(cities_df["location_name"])]
    
    ax.barh(labels, cities_df["score"], color="steelblue")
    
    # Titel basierend auf den Rängen erstellen
    if start_rank == 0:
        title = "Top 10 Städte nach Score"
    else:
        title = f"Plätze {start_rank+1}-{start_rank+len(cities_df)} nach Score"
    
    ax.set_title(title)
    
    # X-Achse konfigurieren
    # Verwende den globalen Maximalwert, wenn er angegeben ist
    x_max = global_max * 1.02 if global_max is not None else cities_df['score'].max() * 1.02
    
    if x_min is not None:
        ax.set_xlim(x_min, x_max)  # Konsistente Skalierung für alle Diagramme
    
    ax.invert_yaxis()
    
    # Score-Werte als Textlabels am Ende der Balken
    for i, score in enumerate(cities_df["score"]):
        ax.text(score, i, f" {(score * 100):.2f}%", va='center', fontsize=9)
    
    fig = plt.gcf()
    return fig

def generate_city_pie_chart(city_row):
    """Generiert ein Kreisdiagramm für eine bestimmte Stadt mit 'Unter 18 Jahre', 'Zielgruppe' und 'Sonstige'"""
    fig, ax = plt.subplots(figsize=(4, 4))
    
    labels = ['Unter 18 Jahre', 'Zielgruppe', 'Sonstige']
    sizes = [
        city_row["under_18_percent"],
        city_row["target_group_percent"],
        city_row["others_percent"]
    ]
    colors = ["#FF8042", "#00C49F", "#CCC"]
    
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=colors)
    ax.axis("equal")
    ax.set_title(f"Altersverteilung: {city_row['location_name']}")
    
    return fig

def generate_scatter_plot(df):
    """Erstellt einen Scatter-Plot für Einwohnerzahl vs. Einkommen mit Farbe für Zielgruppenanteil"""
    plt.figure(figsize=(10, 6))
    scatter = plt.scatter(
        df['total'], 
        df['Einkommen_2022'], 
        c=df['target_group_percent'],
        alpha=0.6,
        cmap='viridis',
        s=100 * df['norm_target']
    )
    
    plt.colorbar(scatter, label='Zielgruppenanteil')
    plt.xlabel('Einwohnerzahl')
    plt.ylabel('Einkommen')
    plt.title('Einwohnerzahl vs. Einkommen (Farbe: Zielgruppenanteil)')
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    # Beschriftung für Top-Städte
    top_5 = df.sort_values('score', ascending=False).head(5)
    for idx, row in top_5.iterrows():
        plt.annotate(
            row['location_name'], 
            (row['total'], row['Einkommen_2022']),
            xytext=(5, 5),
            textcoords='offset points',
            fontsize=9,
            fontweight='bold'
        )
    
    fig = plt.gcf()
    return fig

def generate_interactive_scatter_plot(df):
    """Erstellt einen interaktiven Scatter-Plot mit Plotly"""
    # Formatiere Einwohnerzahl und Einkommen für Hover-Text
    df['formatted_total'] = df['total'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    df['formatted_income'] = df['Einkommen_2022'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    df['target_percent_display'] = (df['target_group_percent'] * 100).round(1).astype(str) + "%"
    
    # Erstelle einen Scatter Plot mit Plotly Express
    fig = px.scatter(
        df,
        x='total',
        y='Einkommen_2022',
        color='target_group_percent',
        size='norm_target',  # Größe basierend auf normalisiertem Zielgruppenanteil
        size_max=30,
        hover_name='location_name',
        hover_data={
            'total': False,  # Verstecke die Rohwerte
            'Einkommen_2022': False,
            'target_group_percent': False,
            'formatted_total': True,
            'formatted_income': True,
            'target_percent_display': True,
            'Land': True,
            'score': True
        },
        labels={
            'formatted_total': 'Einwohner',
            'formatted_income': 'Einkommen',
            'target_percent_display': 'Zielgruppe',
            'Land': 'Bundesland',
            'score': 'Score'
        },
        color_continuous_scale='viridis',
        template='plotly_white'
    )
    
    # Beschriftungen hinzufügen
    fig.update_layout(
        title='Einwohnerzahl vs. Einkommen (Farbe: Zielgruppenanteil)',
        xaxis_title='Einwohnerzahl',
        yaxis_title='Einkommen',
        coloraxis_colorbar_title='Zielgruppenanteil',
        height=600,
        hovermode='closest'
    )
    
    # Beschriftung für Top-Städte
    top_5 = df.sort_values('score', ascending=False).head(5)
    for idx, row in top_5.iterrows():
        fig.add_annotation(
            x=row['total'],
            y=row['Einkommen_2022'],
            text=row['location_name'],
            showarrow=False,
            bgcolor='rgba(255, 255, 255, 0.7)',
            bordercolor='grey',
            borderwidth=1,
            font=dict(size=10, color='black')
        )
    
    # Als JSON-String zurückgeben
    return fig.to_json()

def perform_clustering(df, n_clusters=5):
    """Führt eine Clustering-Analyse durch und erstellt ein Visualisierungsdiagramm"""
    # Feature-Auswahl
    features = df[['total', 'Einkommen_2022', 'target_group_percent']].copy()
    
    # Skalierung der Daten
    scaler = StandardScaler()
    # NaN-Werte vor dem Skalieren behandeln
    features = features.fillna(0)
    scaled_features = scaler.fit_transform(features)
    
    # K-Means Clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    # Cluster-Zuweisungen basierend auf K-Means
    raw_clusters = kmeans.fit_predict(scaled_features)
    
    # Zentren berechnen
    centers = scaler.inverse_transform(kmeans.cluster_centers_)
    
    # Zuordnung der Cluster zu den Interpretationen basierend auf ihren Charakteristika
    cluster_mapping = {}
    
    # Finde den Cluster mit den höchsten Bevölkerungszahlen -> Großstädte (blau, Cluster 2)
    cluster_mapping[np.argmax([center[0] for center in centers])] = 1  # Blau
    
    # Finde den Cluster mit dem höchsten Zielgruppenanteil -> Universitätsstädte (lila, Cluster 4)
    cluster_mapping[np.argmax([center[2] for center in centers])] = 3  # Lila
    
    # Finde den Cluster mit dem höchsten Einkommen -> Wohlhabende Mittelstädte (orange, Cluster 5)
    cluster_mapping[np.argmax([center[1] for center in centers])] = 4  # Orange
    
    # Finde den Cluster mit dem niedrigsten Einkommen -> Ländliche Regionen (grün, Cluster 3)
    cluster_mapping[np.argmin([center[1] for center in centers])] = 2  # Grün
    
    # Der übrig gebliebene Cluster ist Mittelstädte (rot, Cluster 1)
    for i in range(n_clusters):
        if i not in cluster_mapping:
            cluster_mapping[i] = 0  # Rot
    
    # Remapping der Cluster-Zuweisungen und dem Haupt-DataFrame hinzufügen
    
    mapped_clusters = []
    for c in raw_clusters:
        mapped_value = cluster_mapping.get(c, 0)
        mapped_clusters.append(mapped_value)
    
    # WICHTIG: Füge die gemappte Spalte zum originalen DataFrame hinzu, NICHT zu df_clustered
    df['cluster'] = mapped_clusters 
    
    # Debug-Ausgabe für Cluster-Verteilung im *originalen* df
    cluster_counts = df['cluster'].value_counts().to_dict()
    
    # Cluster-Beschreibungen
    cluster_descriptions = [
        'Mittelstädte',
        'Großstädte',
        'Ländliche Regionen',
        'Universitätsstädte',
        'Wohlhabende Mittelstädte'
    ]
    
    # Definiere die gleichen Farben wie im interaktiven Clustering
    cluster_colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00']
    cmap = matplotlib.colors.ListedColormap(cluster_colors)
    
    # Clustering-Ergebnisse visualisieren
    plt.figure(figsize=(10, 6))
    
    # Verwende die ersten beiden Features für die Visualisierung
    # BENUTZE df statt df_clustered!
    scatter = plt.scatter(
        df['total'], 
        df['Einkommen_2022'], 
        c=df['cluster'], # <-- Verwende die gemappte Spalte aus df
        cmap=cmap,
        alpha=0.6,
        s=80,
        vmin=0,  # Setze Minimum auf 0
        vmax=4   # Setze Maximum auf 4 (entspricht 5 Clustern, 0-4)
    )
    
    # Clusterzentren
    plt.scatter(
        centers[:, 0], 
        centers[:, 1], 
        c='black', 
        s=200, 
        alpha=0.8, 
        marker='X'
    )
    
    plt.xlabel('Einwohnerzahl')
    plt.ylabel('Einkommen')
    plt.title(f'Clustering-Analyse (K={n_clusters})')
    
    # Colorbar anpassen, um nur die 5 Cluster anzuzeigen
    cbar = plt.colorbar(scatter, label='Cluster')
    cbar.set_ticks([0, 1, 2, 3, 4])
    # Verwende die Cluster-Beschreibungen für die Labels
    cbar.set_ticklabels([cluster_descriptions[i] for i in range(n_clusters)]) 
    
    plt.grid(True, alpha=0.3)
    
    # Beschriftung für einige Städte
    # BENUTZE df statt df_clustered!
    for cluster_id in range(n_clusters):
        cluster_data = df[df['cluster'] == cluster_id]
        if len(cluster_data) > 0:
            # Annotiere die Top-Stadt nach Score im Cluster
            top_in_cluster = cluster_data.sort_values('score', ascending=False).head(1)
            for _, row in top_in_cluster.iterrows():
                plt.annotate(
                    row['location_name'], 
                    (row['total'], row['Einkommen_2022']),
                    xytext=(5, 5),
                    textcoords='offset points',
                    fontsize=9,
                    fontweight='bold'
                )
    
    plt.tight_layout()
    
    # Cluster-Zusammenfassung als DataFrame
    cluster_summary = []
    # BENUTZE df statt df_clustered!
    for i in range(n_clusters):
        cluster_df = df[df['cluster'] == i]
        if len(cluster_df) > 0:
            summary = {
                'Cluster': f'Cluster {i+1}',
                'Beschreibung': cluster_descriptions[i],
                'Anzahl Städte': len(cluster_df),
                'Durchschn. Einwohner': f"{int(cluster_df['total'].mean()):,}".replace(",", "."),
                'Durchschn. Einkommen': f"{int(cluster_df['Einkommen_2022'].mean()):,}".replace(",", "."),
                'Durchschn. Zielgruppe': f"{(cluster_df['target_group_percent'].mean() * 100):.1f}%",
                'Durchschn. Score': f"{(cluster_df['score'].mean() * 100):.2f}%",
                'Top Stadt': cluster_df.sort_values('score', ascending=False).iloc[0]['location_name']
            }
        else:
            summary = {
                'Cluster': f'Cluster {i+1}',
                'Beschreibung': cluster_descriptions[i],
                'Anzahl Städte': 0,
                'Durchschn. Einwohner': "0",
                'Durchschn. Einkommen': "0",
                'Durchschn. Zielgruppe': '0,0%',
                'Durchschn. Score': '0,00%',
                'Top Stadt': 'N/A'
            }
        cluster_summary.append(summary)
    
    cluster_summary_df = pd.DataFrame(cluster_summary)
    
    fig = plt.gcf()
    return fig, cluster_summary_df

def generate_interactive_clustering(df, n_clusters=5):
    """Erstellt ein interaktives Clustering mit Plotly"""
    # Feature-Auswahl
    features = df[['total', 'Einkommen_2022', 'target_group_percent']].copy()
    
    # Skalierung der Daten
    scaler = StandardScaler()
    # NaN-Werte vor dem Skalieren behandeln
    features = features.fillna(0)
    scaled_features = scaler.fit_transform(features)
    
    # K-Means Clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    # df_clustered = df.copy() # Kopie nicht hier, da wir 'cluster' zu df hinzufügen
    
    # Cluster-Zuweisungen basierend auf K-Means
    raw_clusters = kmeans.fit_predict(scaled_features)
    
    # Zentren berechnen
    centers = scaler.inverse_transform(kmeans.cluster_centers_)
    
    # Zuordnung der Cluster zu den Interpretationen
    cluster_mapping = {}
    cluster_mapping[np.argmax([center[0] for center in centers])] = 1  # Großstädte (Blau)
    cluster_mapping[np.argmax([center[2] for center in centers])] = 3  # Universitätsstädte (Lila)
    cluster_mapping[np.argmax([center[1] for center in centers])] = 4  # Wohlhabende Mittelstädte (Orange)
    cluster_mapping[np.argmin([center[1] for center in centers])] = 2  # Ländliche Regionen (Grün)
    for i in range(n_clusters):
        if i not in cluster_mapping:
            cluster_mapping[i] = 0  # Mittelstädte (Rot)
    
    # Remapping der Cluster-Zuweisungen zum originalen DataFrame hinzufügen
    df['cluster'] = [cluster_mapping[c] for c in raw_clusters]
    
    # Datenbereinigung und Formatierung für Hover/Size (ähnlich wie bei anderer Plot-Funktion)
    df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)
    df['Einkommen_2022'] = pd.to_numeric(df['Einkommen_2022'], errors='coerce').fillna(0)
    df['target_group_percent'] = pd.to_numeric(df['target_group_percent'], errors='coerce').fillna(0)
    df['norm_target'] = pd.to_numeric(df['norm_target'], errors='coerce').fillna(0)
    if 'Land' not in df.columns:
        df['Land'] = 'N/A'

    df['formatted_total'] = df['total'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    df['formatted_income'] = df['Einkommen_2022'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    df['target_percent_display'] = (df['target_group_percent'] * 100).round(1).astype(str) + "%"
    
    # Statistiken pro Cluster berechnen
    cluster_stats = []
    cluster_descriptions = [
        'Mittelstädte',
        'Großstädte',
        'Ländliche Regionen',
        'Universitätsstädte',
        'Wohlhabende Mittelstädte'
    ]
    for i in range(n_clusters):
        cluster_data = df[df['cluster'] == i]
        if len(cluster_data) > 0:
            top_city = cluster_data.sort_values('score', ascending=False).iloc[0]['location_name']
            stats = {
                'cluster': f'Cluster {i+1}',
                'description': cluster_descriptions[i],
                'count': len(cluster_data),
                'avg_population': f"{int(cluster_data['total'].mean()):,}".replace(",", "."),
                'avg_income': f"{int(cluster_data['Einkommen_2022'].mean()):,}".replace(",", "."),
                'avg_target': (cluster_data['target_group_percent'].mean() * 100).round(1),
                'avg_score': f"{(cluster_data['score'].mean() * 100):.2f}%",
                'top_city': top_city
            }
            cluster_stats.append(stats)
        else:
            stats = {
                'cluster': f'Cluster {i+1}',
                'description': cluster_descriptions[i],
                'count': 0,
                'avg_population': "0",
                'avg_income': "0",
                'avg_target': 0.0,
                'avg_score': '0,00%',
                'top_city': 'N/A'
            }
            cluster_stats.append(stats)
    
    # Definiere Clusterfarben
    cluster_colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00']
    cluster_names = {
        0: 'Cluster 1: Mittelstädte',
        1: 'Cluster 2: Großstädte',
        2: 'Cluster 3: Ländliche Regionen',
        3: 'Cluster 4: Universitätsstädte',
        4: 'Cluster 5: Wohlhabende Mittelstädte'
    }
    df['cluster_name'] = df['cluster'].map(cluster_names)


    # Erstelle Plotly-Figur
    fig = px.scatter(
        df, # Benutze den originalen DataFrame
        x='total',
        y='Einkommen_2022',
        color='cluster_name',  # Verwende die benannten Cluster
        hover_name='location_name',
        hover_data={
            'total': False,
            'Einkommen_2022': False,
            'cluster_name': True,
            'formatted_total': True,
            'formatted_income': True,
            'target_percent_display': True,
            'Land': True,
            'score': True
        },
        labels={
            'total': 'Einwohnerzahl', # Achsenbeschriftung angepasst
            'Einkommen_2022': 'Einkommen', # Achsenbeschriftung angepasst
            'formatted_total': 'Einwohner',
            'formatted_income': 'Einkommen',
            'target_percent_display': 'Zielgruppe',
            'Land': 'Bundesland',
            'score': 'Score',
            'cluster_name': 'Cluster'
        },
        color_discrete_map={
            'Cluster 1: Mittelstädte': cluster_colors[0],
            'Cluster 2: Großstädte': cluster_colors[1],
            'Cluster 3: Ländliche Regionen': cluster_colors[2],
            'Cluster 4: Universitätsstädte': cluster_colors[3],
            'Cluster 5: Wohlhabende Mittelstädte': cluster_colors[4]
        },
        size='norm_target',
        size_max=20,
        template='plotly_white',
        category_orders={'cluster_name': [
            'Cluster 1: Mittelstädte',
            'Cluster 2: Großstädte', 
            'Cluster 3: Ländliche Regionen',
            'Cluster 4: Universitätsstädte', 
            'Cluster 5: Wohlhabende Mittelstädte'
        ]}
    )
    
    # Linien um Marker anpassen
    fig.update_traces(
        marker=dict(
            line=dict(width=1, color='DarkSlateGrey')
        )
    )
    
    # Layout anpassen
    fig.update_layout(
        title='Clustering der Städte nach Einwohnerzahl, Einkommen und Zielgruppenanteil',
        xaxis_title='Einwohnerzahl',
        yaxis_title='Einkommen',
        height=600,
        legend_title='Cluster',
        hovermode='closest'
    )
    
    # Clusterzentren hinzufügen
    centers_df = pd.DataFrame({
        'total': centers[:, 0],
        'Einkommen_2022': centers[:, 1],
        'cluster_name': [f'Zentrum C{i+1}' for i in range(n_clusters)]
    })
    
    # Zentren als Kreuze darstellen
    fig.add_trace(
        px.scatter(
            centers_df, 
            x='total', 
            y='Einkommen_2022', 
            text='cluster_name',
            color_discrete_sequence=['black']
        ).update_traces(
            marker=dict(symbol='x', size=15, line=dict(width=2)),
            mode='markers+text',
            textposition='top center'
        ).data[0]
    )
    
    # Zusammenfassung der Cluster-Statistiken als HTML-Tabelle
    stats_df = pd.DataFrame(cluster_stats)
    stats_df.columns = [
        'Cluster', 'Beschreibung', 'Anzahl Städte', 'Durchschn. Einwohner', 'Durchschn. Einkommen',
        'Durchschn. Zielgruppe (%)', 'Durchschn. Score', 'Top Stadt'
    ]
    
    # Als JSON und HTML zurückgeben
    return {
        'plot': fig.to_json(),
        'stats': stats_df.to_html(
            classes='table table-striped table-hover',
            index=False,
            justify='left',
            border=0
        )
    }

def generate_filtered_clustering(df, n_clusters=5, selected_clusters=None):
    """
    Erstellt ein interaktives Clustering mit Plotly, aber zeigt nur die ausgewählten Cluster an
    
    Args:
        df: DataFrame mit den Daten
        n_clusters: Anzahl der Cluster (Standard: 5)
        selected_clusters: Liste der Cluster-Indizes, die angezeigt werden sollen (0-4)
    
    Returns:
        Dictionary mit Plotly-JSON und HTML-Tabelle für die Statistiken
    """
    if selected_clusters is None:
        selected_clusters = list(range(n_clusters))  # Standardmäßig alle Cluster anzeigen
    
    # Feature-Auswahl
    features = df[['total', 'Einkommen_2022', 'target_group_percent']].copy()
    
    # Skalierung der Daten
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    
    # K-Means Clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df_clustered = df.copy()
    
    # Cluster-Zuweisungen basierend auf K-Means
    raw_clusters = kmeans.fit_predict(scaled_features)
    
    # Zentren berechnen
    centers = scaler.inverse_transform(kmeans.cluster_centers_)
    
    # Zuordnung der Cluster zu den Interpretationen
    cluster_mapping = {}
    
    # Finde den Cluster mit den höchsten Bevölkerungszahlen -> Großstädte (blau, Cluster 2)
    cluster_mapping[np.argmax([center[0] for center in centers])] = 1  # Blau
    
    # Finde den Cluster mit dem höchsten Zielgruppenanteil -> Universitätsstädte (lila, Cluster 4)
    cluster_mapping[np.argmax([center[2] for center in centers])] = 3  # Lila
    
    # Finde den Cluster mit dem höchsten Einkommen -> Wohlhabende Mittelstädte (orange, Cluster 5)
    cluster_mapping[np.argmax([center[1] for center in centers])] = 4  # Orange
    
    # Finde den Cluster mit dem niedrigsten Einkommen -> Ländliche Regionen (grün, Cluster 3)
    cluster_mapping[np.argmin([center[1] for center in centers])] = 2  # Grün
    
    # Der übrig gebliebene Cluster ist Mittelstädte (rot, Cluster 1)
    for i in range(n_clusters):
        if i not in cluster_mapping:
            cluster_mapping[i] = 0  # Rot
    
    # Remapping der Cluster-Zuweisungen
    df_clustered['cluster'] = [cluster_mapping[c] for c in raw_clusters]
    
    # Formatierung für Hover-Text
    df_clustered['formatted_total'] = df_clustered['total'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    df_clustered['formatted_income'] = df_clustered['Einkommen_2022'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    df_clustered['target_percent_display'] = (df_clustered['target_group_percent'] * 100).round(1).astype(str) + "%"
    
    # Statistiken pro Cluster berechnen (nur für ausgewählte Cluster)
    cluster_stats = []
    for i in range(n_clusters):
        if i in selected_clusters:
            cluster_data = df_clustered[df_clustered['cluster'] == i]
            if len(cluster_data) > 0:
                top_city = cluster_data.sort_values('score', ascending=False).iloc[0]['location_name']
                stats = {
                    'cluster': f'Cluster {i+1}',
                    'count': len(cluster_data),
                    'avg_population': f"{int(cluster_data['total'].mean()):,}".replace(",", "."),
                    'avg_income': f"{int(cluster_data['Einkommen_2022'].mean()):,}".replace(",", "."),
                    'avg_target': (cluster_data['target_group_percent'].mean() * 100).round(1),
                    'avg_score': f"{(cluster_data['score'].mean() * 100):.2f}%",
                    'top_city': top_city
                }
                cluster_stats.append(stats)
            else:
                # Füge einen Platzhalter hinzu, wenn der Cluster leer ist
                stats = {
                    'cluster': f'Cluster {i+1}',
                    'count': 0,
                    'avg_population': "0",
                    'avg_income': "0",
                    'avg_target': 0.0,
                    'avg_score': '0,00%',
                    'top_city': 'N/A'
                }
                cluster_stats.append(stats)
    
    # Definiere Clusterfarben entsprechend der Cluster-Interpretation
    cluster_colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00']
    
    # Cluster-Namen für die Legende
    cluster_names = {
        0: 'Cluster 1: Mittelstädte',
        1: 'Cluster 2: Großstädte',
        2: 'Cluster 3: Ländliche Regionen',
        3: 'Cluster 4: Universitätsstädte',
        4: 'Cluster 5: Wohlhabende Mittelstädte'
    }
    
    # DataFrame für Cluster-Mapping vorbereiten
    df_clustered['cluster_name'] = df_clustered['cluster'].map(cluster_names)
    
    # Filter für ausgewählte Cluster anwenden
    df_filtered = df_clustered[df_clustered['cluster'].isin(selected_clusters)].copy()
    
    # Erstelle Plotly-Figur
    fig = px.scatter(
        df_filtered,  # Nur gefilterte Daten anzeigen
        x='total',
        y='Einkommen_2022',
        color='cluster_name',  # Verwende die benannten Cluster
        hover_name='location_name',
        hover_data={
            'total': False,
            'Einkommen_2022': False,
            'cluster_name': True,
            'formatted_total': True,
            'formatted_income': True,
            'target_percent_display': True,
            'Land': True,
            'score': True
        },
        labels={
            'formatted_total': 'Einwohner',
            'formatted_income': 'Einkommen',
            'target_percent_display': 'Zielgruppe',
            'Land': 'Bundesland',
            'score': 'Score',
            'cluster_name': 'Cluster'
        },
        color_discrete_map={
            'Cluster 1: Mittelstädte': cluster_colors[0],
            'Cluster 2: Großstädte': cluster_colors[1],
            'Cluster 3: Ländliche Regionen': cluster_colors[2],
            'Cluster 4: Universitätsstädte': cluster_colors[3],
            'Cluster 5: Wohlhabende Mittelstädte': cluster_colors[4]
        },
        size='norm_target',
        size_max=20,
        template='plotly_white',
        category_orders={'cluster_name': [
            'Cluster 1: Mittelstädte',
            'Cluster 2: Großstädte', 
            'Cluster 3: Ländliche Regionen',
            'Cluster 4: Universitätsstädte', 
            'Cluster 5: Wohlhabende Mittelstädte'
        ]}
    )
    
    # FutureWarning beheben, indem wir Plotly-Traces direkt bearbeiten
    # Die Warnung entsteht im px.scatter bei der internen Gruppenbildung
    # Diese Zeile unterdrückt die Warnung nicht, aber sie kann als Hinweis dienen,
    # Linien um Marker anpassen
    fig.update_traces(
        marker=dict(
            line=dict(width=1, color='DarkSlateGrey')
        )
    )
    
    # Layout anpassen
    fig.update_layout(
        title='Clustering der Städte nach Einwohnerzahl, Einkommen und Zielgruppenanteil',
        xaxis_title='Einwohnerzahl',
        yaxis_title='Einkommen',
        height=600,
        legend_title='Cluster',
        hovermode='closest'
    )
    
    # Clusterzentren hinzufügen
    # Hier nur die Zentren der ausgewählten Cluster anzeigen
    filtered_centers = []
    filtered_labels = []
    for i in range(n_clusters):
        if i in selected_clusters:
            filtered_centers.append(centers[i])
            filtered_labels.append(f'Zentrum C{i+1}')
    
    if filtered_centers:  # Nur wenn Zentren ausgewählt wurden
        centers_df = pd.DataFrame({
            'total': [center[0] for center in filtered_centers],
            'Einkommen_2022': [center[1] for center in filtered_centers],
            'cluster_name': filtered_labels
        })
        
        # Zentren als Kreuze darstellen
        fig.add_trace(
            px.scatter(
                centers_df, 
                x='total', 
                y='Einkommen_2022', 
                text='cluster_name',
                color_discrete_sequence=['black']
            ).update_traces(
                marker=dict(symbol='x', size=15, line=dict(width=2)),
                mode='markers+text',
                textposition='top center'
            ).data[0]
        )
    
    # Zusammenfassung der Cluster-Statistiken als HTML-Tabelle
    stats_df = pd.DataFrame(cluster_stats)
    
    if not stats_df.empty:  # Nur wenn Daten vorhanden sind
        stats_df.columns = [
            'Cluster', 'Anzahl Städte', 'Durchschn. Einwohner', 'Durchschn. Einkommen',
            'Durchschn. Zielgruppe (%)', 'Durchschn. Score', 'Top Stadt'
        ]
        
        html_stats = stats_df.to_html(
            classes='table table-striped table-hover',
            index=False,
            justify='left',
            border=0
        )
    else:
        html_stats = "<p>Keine Daten für die ausgewählten Cluster verfügbar.</p>"
    
    # Als JSON und HTML zurückgeben
    return {
        'plot': fig.to_json(),
        'stats': html_stats
    }

def encode_figure_to_base64(fig):
    """Konvertiert eine Matplotlib-Figur zu einem Base64-kodierten String"""
    img = io.BytesIO()
    fig.savefig(img, format='png', bbox_inches='tight')
    img.seek(0)
    return base64.b64encode(img.getvalue()).decode('utf-8')

def generate_interactive_map(df, kpi_column='score'):
    """
    Erstellt eine interaktive Deutschlandkarte mit Einfärbung basierend auf ausgewähltem KPI
    
    Args:
        df: DataFrame mit den Daten
        kpi_column: Spalte, die für die Einfärbung verwendet werden soll 
                   ('score', 'Einkommen_2022', 'target_group_percent', 'total')
    
    Returns:
        Plotly-Figur als JSON-String
    """
    # Deutschland-Eintrag entfernen, wenn vorhanden
    df_map = df[~df['location_name'].isin(['Deutschland', 'Bundesrepublik Deutschland'])].copy()
    
    # Normalisiere die KPI-Werte für die Farbskala
    if kpi_column == 'target_group_percent':
        # Für Prozentwerte: Multiplikation mit 100 für bessere Darstellung
        df_map['kpi_value'] = df_map[kpi_column] * 100
        color_scale = 'Viridis'
        kpi_title = 'Zielgruppenanteil (%)'
    elif kpi_column == 'total':
        # Für Einwohnerzahl: Logarithmische Skalierung
        df_map['kpi_value'] = np.log10(df_map[kpi_column])
        color_scale = 'Viridis'
        kpi_title = 'Einwohnerzahl (log)'
    elif kpi_column == 'Einkommen_2022':
        # Für Einkommen: Direkter Wert
        df_map['kpi_value'] = df_map[kpi_column]
        color_scale = 'Viridis'
        kpi_title = 'Einkommen'
    else:  # Standardfall: score
        df_map['kpi_value'] = df_map[kpi_column]
        color_scale = 'Viridis'
        kpi_title = 'Score'
    
    # Erstelle grundlegende Deutschlandkarte
    fig = go.Figure()
    
    # Füge Choropleth-Karte hinzu
    fig.add_trace(go.Choropleth(
        locations=df_map['location_name'],
        z=df_map['kpi_value'],
        locationmode='country names',
        colorscale=color_scale,
        colorbar_title=kpi_title,
        name='',
        text=df_map.apply(
            lambda row: f"{row['location_name']}<br>" +
                       f"Einwohner: {int(row['total']):,}<br>" +
                       f"Einkommen: {int(row['Einkommen_2022']):,} €<br>" +
                       f"Zielgruppe: {row['target_group_percent']*100:.1f}%<br>" +
                       f"Score: {row['score']:.4f}",
            axis=1
        ),
        hoverinfo='text',
        marker_line_color='white',
        marker_line_width=0.5,
    ))
    
    # Aktualisiere Layout
    fig.update_layout(
        title='Deutschland - KPI-Karte',
        geo=dict(
            scope='europe',
            center=dict(lon=10.4515, lat=51.1657),  # Deutschland-Zentrum
            projection_scale=5,
            showland=True,
            landcolor='rgb(243, 243, 243)',
            countrycolor='rgb(204, 204, 204)',
            coastlinecolor='rgb(204, 204, 204)',
            showocean=True,
            oceancolor='rgb(230, 230, 250)'
        ),
        height=700,
        width=1000,
        margin=dict(l=0, r=0, t=30, b=0)
    )
    
    # Als JSON-String zurückgeben
    return fig.to_json()

def create_plz_map(df):
    """
    Erstellt eine interaktive Deutschland-Karte basierend auf PLZ-Regionen
    
    Args:
        df: DataFrame mit den Städtedaten inkl. Postleitzahl-Spalte
    
    Returns:
        JSON-Struktur für die Karte
    """
    # DataFrame mit PLZ-Informationen vorbereiten
    df_plz = df[~df['location_name'].isin(['Deutschland', 'Bundesrepublik Deutschland'])].copy()
    
    # Erstelle GeoJSON-Feature-Collection für PLZ-Regionen
    features = []
    
    for _, row in df_plz.iterrows():
        city_name = row['location_name']
        
        # Extrahiere Postleitzahlen (falls vorhanden)
        plz_list = []
        if 'Postleitzahl' in row and pd.notna(row['Postleitzahl']):
            # Trenne die kommaseparierten PLZs
            plz_text = str(row['Postleitzahl'])
            plz_list = [plz.strip() for plz in re.split(r',|\s+', plz_text) if plz.strip()]
        
        # PLZ-Zone als Feature hinzufügen
        feature = {
            "type": "Feature",
            "properties": {
                "city": city_name,
                "plz": plz_list[:5],  # Nur die ersten 5 PLZs anzeigen
                "total_plz": len(plz_list),
                "einwohner": int(row['total']),
                "einkommen": int(row['Einkommen_2022']),
                "zielgruppe": f"{row['target_group_percent']*100:.1f}%",
                "score": float(row['score']),
                "bundesland": row.get('Land', 'N/A'),
                "kpi_value": float(row['score'])  # Standardmäßig Score verwenden
            },
            "geometry": {
                "type": "Point",
                "coordinates": [10.0, 51.0]  # Standard-Koordinaten, werden im Frontend angepasst
            }
        }
        features.append(feature)
    
    # GeoJSON-Struktur erstellen
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return geojson

def perform_clustering_population_target(df, n_clusters=5):
    """Führt eine Clustering-Analyse durch mit Einwohnerzahl und Zielgruppenanteil und erstellt ein Visualisierungsdiagramm"""
    # Feature-Auswahl - nur Einwohnerzahl und Zielgruppenanteil
    features = df[['total', 'target_group_percent']].copy()
    
    # Skalierung der Daten
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    
    # K-Means Clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    df_clustered = df.copy()
    
    # Cluster-Zuweisungen basierend auf K-Means
    raw_clusters = kmeans.fit_predict(scaled_features)
    
    # Zentren berechnen
    centers = scaler.inverse_transform(kmeans.cluster_centers_)
    
    # Zuordnung der Cluster zu den Interpretationen basierend auf ihren Charakteristika
    cluster_mapping = {}
    
    # Finde den Cluster mit den höchsten Bevölkerungszahlen -> Großstädte
    cluster_mapping[np.argmax([center[0] for center in centers])] = 1  # Blau
    
    # Finde den Cluster mit dem höchsten Zielgruppenanteil -> Universitätsstädte
    cluster_mapping[np.argmax([center[1] for center in centers])] = 3  # Lila
    
    # Finde den Cluster mit dem niedrigsten Zielgruppenanteil -> Wenig Zielgruppe
    cluster_mapping[np.argmin([center[1] for center in centers])] = 2  # Grün
    
    # Finde den Cluster mit den wenigsten Einwohnern und mittlerem/hohem Zielgruppenanteil -> Kleine Universitätsstädte
    remaining = [i for i in range(n_clusters) if i not in cluster_mapping.keys()]
    if len(remaining) >= 2:
        # Sortiere nach Einwohnerzahl (aufsteigend)
        pop_sorted = sorted(remaining, key=lambda i: centers[i][0])
        # Sortiere die verbleibenden nach Zielgruppenanteil (absteigend)
        target_sorted = sorted(pop_sorted, key=lambda i: centers[i][1], reverse=True)
        
        # Der mit dem höchsten Zielgruppenanteil wird Cluster 4 (Kleine Universitätsstädte)
        cluster_mapping[target_sorted[0]] = 4  # Orange
        
        # Der andere ist Cluster 0 (Mittelstädte)
        for i in target_sorted[1:]:
            if i not in cluster_mapping:
                cluster_mapping[i] = 0  # Rot
    else:
        # Falls nur ein Cluster übrig bleibt, ordne ihn als Mittelstädte ein
        for i in remaining:
            cluster_mapping[i] = 0  # Rot
    
    # Remapping der Cluster-Zuweisungen
    df_clustered['cluster'] = [cluster_mapping[c] for c in raw_clusters]
    
    # Definiere die gleichen Farben wie im interaktiven Clustering
    cluster_colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00']
    
    # Konfiguriere den Plot
    plt.figure(figsize=(10, 6))
    
    # Plotte jeden Cluster mit einer anderen Farbe
    for i in range(n_clusters):
        cluster_points = df_clustered[df_clustered['cluster'] == i]
        plt.scatter(
            cluster_points['total'], 
            cluster_points['target_group_percent'],
            s=80,
            c=cluster_colors[i],
            alpha=0.7,
            label=f'Cluster {i+1}'
        )
    
    # Clusterzentren plotten
    for i, center in enumerate(centers):
        cluster_num = [k for k, v in cluster_mapping.items() if v == i]
        if cluster_num:
            cluster_num = cluster_num[0]
            plt.scatter(
                center[0], 
                center[1], 
                s=200, 
                c='black', 
                marker='X', 
                label=f'Zentrum C{cluster_num+1}'
            )
    
    # Top-Städte markieren
    top_cities = df_clustered.sort_values('score', ascending=False).head(5)
    for _, city in top_cities.iterrows():
        plt.annotate(
            city['location_name'],
            (city['total'], city['target_group_percent']),
            xytext=(5, 5),
            textcoords='offset points',
            fontsize=9,
            fontweight='bold'
        )
    
    plt.xlabel('Einwohnerzahl')
    plt.ylabel('Zielgruppenanteil')
    plt.title('Clustering-Analyse (Einwohnerzahl vs. Zielgruppe, K=5)')
    plt.grid(True, alpha=0.3)
    plt.legend()
    
    # Cluster-Beschreibung für die Zusammenfassung
    cluster_descriptions = [
        'Mittelstädte',
        'Großstädte',
        'Städte mit geringem Zielgruppenanteil',
        'Große Universitätsstädte',
        'Kleinere Universitätsstädte'
    ]
    
    # Cluster-Zusammenfassung als DataFrame
    cluster_summary = []
    for i in range(n_clusters):
        cluster_df = df_clustered[df_clustered['cluster'] == i]
        if len(cluster_df) > 0:
            summary = {
                'cluster': f'Cluster {i+1}',
                'description': cluster_descriptions[i],
                'count': len(cluster_df),
                'avg_population': f"{int(cluster_df['total'].mean()):,}".replace(",", "."),
                'avg_income': f"{int(cluster_df['Einkommen_2022'].mean()):,}".replace(",", "."),
                'avg_target': (cluster_df['target_group_percent'].mean() * 100).round(1),
                'avg_score': f"{(cluster_df['score'].mean() * 100):.2f}%",
                'top_city': cluster_df.sort_values('score', ascending=False).iloc[0]['location_name']
            }
        else:
            summary = {
                'cluster': f'Cluster {i+1}',
                'description': cluster_descriptions[i],
                'count': 0,
                'avg_population': "0",
                'avg_income': "0",
                'avg_target': 0.0,
                'avg_score': '0,00%',
                'top_city': 'N/A'
            }
        cluster_summary.append(summary)
    
    cluster_summary_df = pd.DataFrame(cluster_summary)
    
    fig = plt.gcf()
    return fig, cluster_summary_df

def generate_interactive_clustering_population_target(df, n_clusters=5):
    """Erstellt ein interaktives Clustering mit Plotly für Einwohnerzahl und Zielgruppenanteil"""
    # Feature-Auswahl - nur Einwohnerzahl und Zielgruppenanteil
    features = df[['total', 'target_group_percent']].copy()
    
    # Skalierung der Daten
    scaler = StandardScaler()
    # Wichtig: NaN-Werte vor dem Skalieren behandeln
    features = features.fillna(0)
    scaled_features = scaler.fit_transform(features)
    
    # K-Means Clustering
    kmeans = KMeans(n_clusters=n_clusters, random_state=42)
    # df_clustered = df.copy() # Kopie nicht hier, da wir 'cluster' zu df hinzufügen
    
    # Cluster-Zuweisungen basierend auf K-Means
    raw_clusters = kmeans.fit_predict(scaled_features)
    
    # Zentren berechnen
    centers = scaler.inverse_transform(kmeans.cluster_centers_)
    
    # Zuordnung der Cluster zu den Interpretationen basierend auf ihren Charakteristika
    cluster_mapping = {}
    cluster_mapping[np.argmax([center[0] for center in centers])] = 1  # Großstädte (Blau)
    cluster_mapping[np.argmax([center[1] for center in centers])] = 3  # Große Universitätsstädte (Lila)
    cluster_mapping[np.argmin([center[1] for center in centers])] = 2  # Geringer Zielgruppenanteil (Grün)
    remaining = [i for i in range(n_clusters) if i not in cluster_mapping.keys()]
    if len(remaining) >= 2:
        pop_sorted = sorted(remaining, key=lambda i: centers[i][0])
        target_sorted = sorted(pop_sorted, key=lambda i: centers[i][1], reverse=True)
        cluster_mapping[target_sorted[0]] = 4  # Kleinere Universitätsstädte (Orange)
        for i in target_sorted[1:]:
            if i not in cluster_mapping:
                cluster_mapping[i] = 0  # Mittelstädte (Rot)
    else:
        for i in remaining:
            cluster_mapping[i] = 0  # Mittelstädte (Rot)
    
    # Remapping der Cluster-Zuweisungen zum originalen DataFrame hinzufügen
    df['cluster'] = [cluster_mapping[c] for c in raw_clusters]
    
    # Wichtig: Sicherstellen, dass Spalten für Hover und Size numerisch sind
    df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0)
    df['target_group_percent'] = pd.to_numeric(df['target_group_percent'], errors='coerce').fillna(0)
    df['norm_target'] = pd.to_numeric(df['norm_target'], errors='coerce').fillna(0)
    # Sicherstellen, dass Einkommen vorhanden und numerisch ist für Hover-Text
    if 'Einkommen_2022' not in df.columns:
        df['Einkommen_2022'] = 0
    df['Einkommen_2022'] = pd.to_numeric(df['Einkommen_2022'], errors='coerce').fillna(0)
    # Sicherstellen, dass Land vorhanden ist für Hover-Text
    if 'Land' not in df.columns:
        df['Land'] = 'N/A'
        
    # Formatierung für Hover-Text (NACH der Konvertierung)
    df['formatted_total'] = df['total'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    df['formatted_income'] = df['Einkommen_2022'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    df['target_percent_display'] = (df['target_group_percent'] * 100).round(1).astype(str) + "%"
    
    # Statistiken pro Cluster berechnen
    cluster_stats = []
    cluster_descriptions = [
        'Mittelstädte',
        'Großstädte',
        'Städte mit geringem Zielgruppenanteil',
        'Große Universitätsstädte',
        'Kleinere Universitätsstädte'
    ]
    for i in range(n_clusters):
        # Verwende df statt df_clustered
        cluster_data = df[df['cluster'] == i]
        if len(cluster_data) > 0:
            top_city = cluster_data.sort_values('score', ascending=False).iloc[0]['location_name']
            stats = {
                'cluster': f'Cluster {i+1}',
                'description': cluster_descriptions[i],
                'count': len(cluster_data),
                'avg_population': f"{int(cluster_data['total'].mean()):,}".replace(",", "."),
                'avg_income': f"{int(cluster_data['Einkommen_2022'].mean()):,}".replace(",", "."),
                'avg_target': (cluster_data['target_group_percent'].mean() * 100).round(1),
                'avg_score': f"{(cluster_data['score'].mean() * 100):.2f}%",
                'top_city': top_city
            }
            cluster_stats.append(stats)
        else:
            stats = {
                'cluster': f'Cluster {i+1}',
                'description': cluster_descriptions[i],
                'count': 0,
                'avg_population': "0",
                'avg_income': "0",
                'avg_target': 0.0,
                'avg_score': '0,00%',
                'top_city': 'N/A'
            }
            cluster_stats.append(stats)
    
    # Definiere Clusterfarben
    cluster_colors = ['#e41a1c', '#377eb8', '#4daf4a', '#984ea3', '#ff7f00']
    cluster_names = {
        0: f'Cluster 1: {cluster_descriptions[0]}',
        1: f'Cluster 2: {cluster_descriptions[1]}',
        2: f'Cluster 3: {cluster_descriptions[2]}',
        3: f'Cluster 4: {cluster_descriptions[3]}',
        4: f'Cluster 5: {cluster_descriptions[4]}'
    }
    # Verwende df statt df_clustered
    df['cluster_name'] = df['cluster'].map(cluster_names)
    
    # Erstelle Plotly-Figur
    fig = px.scatter(
        df,
        x='total',
        y='target_group_percent',
        color='cluster_name',  # Verwende die benannten Cluster
        hover_name='location_name',
        hover_data={
            'total': False,
            'target_group_percent': False,
            'cluster_name': True,
            'formatted_total': True,
            'formatted_income': True,
            'target_percent_display': True,
            'Land': True,
            'score': True
        },
        labels={
            'formatted_total': 'Einwohner',
            'formatted_income': 'Einkommen',
            'target_percent_display': 'Zielgruppe',
            'Land': 'Bundesland',
            'score': 'Score',
            'cluster_name': 'Cluster'
        },
        color_discrete_map={
            f'Cluster 1: {cluster_descriptions[0]}': cluster_colors[0],
            f'Cluster 2: {cluster_descriptions[1]}': cluster_colors[1],
            f'Cluster 3: {cluster_descriptions[2]}': cluster_colors[2],
            f'Cluster 4: {cluster_descriptions[3]}': cluster_colors[3],
            f'Cluster 5: {cluster_descriptions[4]}': cluster_colors[4]
        },
        size='norm_target',
        size_max=20,
        template='plotly_white',
        category_orders={'cluster_name': [
            f'Cluster 1: {cluster_descriptions[0]}',
            f'Cluster 2: {cluster_descriptions[1]}', 
            f'Cluster 3: {cluster_descriptions[2]}',
            f'Cluster 4: {cluster_descriptions[3]}', 
            f'Cluster 5: {cluster_descriptions[4]}'
        ]}
    )
    
    # Linien um Marker anpassen
    fig.update_traces(
        marker=dict(
            line=dict(width=1, color='DarkSlateGrey')
        )
    )
    
    # Layout anpassen
    fig.update_layout(
        title='Clustering der Städte nach Einwohnerzahl und Zielgruppenanteil',
        xaxis_title='Einwohnerzahl',
        yaxis_title='Zielgruppenanteil',
        height=600,
        legend_title='Cluster',
        hovermode='closest'
    )
    
    # Clusterzentren hinzufügen
    centers_df = pd.DataFrame({
        'total': centers[:, 0],
        'target_group_percent': centers[:, 1],
        'cluster_name': [f'Zentrum C{i+1}' for i in range(n_clusters)]
    })
    
    # Zentren als Kreuze darstellen
    fig.add_trace(
        px.scatter(
            centers_df, 
            x='total', 
            y='target_group_percent', 
            text='cluster_name',
            color_discrete_sequence=['black']
        ).update_traces(
            marker=dict(symbol='x', size=15, line=dict(width=2)),
            mode='markers+text',
            textposition='top center'
        ).data[0]
    )
    
    # Zusammenfassung der Cluster-Statistiken als HTML-Tabelle
    stats_df = pd.DataFrame(cluster_stats)
    stats_df.columns = [
        'Cluster', 'Beschreibung', 'Anzahl Städte', 'Durchschn. Einwohner', 'Durchschn. Einkommen',
        'Durchschn. Zielgruppe (%)', 'Durchschn. Score', 'Top Stadt'
    ]
    
    # Als JSON und HTML zurückgeben
    return {
        'plot': fig.to_json(),
        'stats': stats_df.to_html(
            classes='table table-striped table-hover',
            index=False,
            justify='left',
            border=0
        )
    }

def generate_cities_table_html(sorted_df):
    """Generiert HTML für die Städtetabelle"""
    # Debug-Ausgabe der Spalten im sortierten DataFrame
    
    # Nur die benötigten Spalten auswählen
    # Stelle sicher, dass die Grundsätzlichen Spalten existieren
    required_columns = ['location_name', 'Land', 'total', 'Einkommen_2022', 'target_group_percent', 'score']
    available_columns = [col for col in required_columns if col in sorted_df.columns]
    
    if len(available_columns) < len(required_columns):
        print(f"WARNUNG: Nicht alle erforderlichen Spalten sind vorhanden! Vorhanden: {available_columns}")
    
    display_df = sorted_df[available_columns].copy()
    
    # Prüfen, ob die Cluster-Spalte vorhanden ist
    if 'cluster' in sorted_df.columns:
        # Zeige Beispielwerte an
        display_df['cluster'] = sorted_df['cluster'].astype(int)
    else:
        # Dummy-Werte, falls keine Cluster vorhanden sind
        display_df['cluster'] = 0
    
    # Formatiere die Daten für die Anzeige
    display_df['total'] = display_df['total'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    display_df['Einkommen_2022'] = display_df['Einkommen_2022'].apply(lambda x: f"{int(x):,}".replace(",", "."))
    display_df['target_group_percent'] = (display_df['target_group_percent'] * 100).round(1).astype(str) + "%"
    display_df['score'] = (display_df['score'] * 100).round(2).astype(str) + "%"
    
    # Debug-Ausgabe der Cluster-Verteilung nach der Formatierung
    
    # HTML-Tabelle erstellen
    html = '<div class="table-responsive">'
    html += '<table id="data-table" class="table table-striped table-hover">'
    
    # Kopfzeile
    html += '<thead><tr>'
    html += '<th class="select-all-checkbox-header"><input type="checkbox" class="select-all-checkbox"></th>'
    html += '<th>#</th>'
    html += '<th>Stadt</th>'
    html += '<th>Bundesland</th>'
    html += '<th>Einwohner</th>'
    html += '<th>Einkommen</th>'
    html += '<th>Zielgruppe</th>'
    html += '<th>Cluster</th>'
    html += '<th>Score</th>'
    html += '<th>Aktionen</th>'
    html += '</tr></thead>'
    
    # Tabelleninhalt
    html += '<tbody>'
    
    # Debug-Ausgabe: Zeige die ersten 5 Cluster-Werte
    cluster_samples = display_df['cluster'].head(5).tolist()
    
    for idx, (_, row) in enumerate(display_df.iterrows()):
        # Stelle sicher, dass cluster_id ein Integer ist
        cluster_id = int(row['cluster']) if 'cluster' in row and pd.notna(row['cluster']) else 0
        
        # Hier die Zuordnung direkt basierend auf den bekannten Werten
        if cluster_id == 0:
            cluster_color = "success"  # Grün für Mittelstädte
            cluster_label = "Mittelstädte"
        elif cluster_id == 1:
            cluster_color = "primary"  # Blau für Großstädte
            cluster_label = "Großstädte"
        elif cluster_id == 2:
            cluster_color = "danger"   # Rot für Ländliche Regionen
            cluster_label = "Ländliche Regionen"
        elif cluster_id == 3:
            cluster_color = "purple"   # Lila für Universitätsstädte
            cluster_label = "Universitätsstädte"
        elif cluster_id == 4:
            cluster_color = "warning"  # Orange für Wohlhabende Mittelstädte
            cluster_label = "Wohlhabende Mittelstädte"
        else:
            cluster_color = "secondary"
            cluster_label = f"Cluster {cluster_id+1}"
        
        city_id = sorted_df.index[idx]  # Verwende den DataFrame-Index als ID
        
        html += f'<tr class="city-row" data-id="{city_id}">'
        html += f'<td><input type="checkbox" class="select-checkbox"></td>'
        html += f'<td>{idx + 1}</td>'
        html += f'<td>{row["location_name"]}</td>'
        html += f'<td>{row["Land"]}</td>'
        html += f'<td>{row["total"]}</td>'
        html += f'<td>{row["Einkommen_2022"]}</td>'
        html += f'<td>{row["target_group_percent"]}</td>'
        html += f'<td><span class="badge bg-{cluster_color} cluster-badge">{cluster_label}</span></td>'
        html += f'<td>{row["score"]}</td>'
        html += f'<td>'
        html += f'<button class="btn btn-sm btn-outline-primary open-search-popup" ' \
                f'data-city="{row["location_name"]}" ' \
                f'data-city-display="{row["location_name"]}">' \
                f'<i class="bi bi-search"></i> Lokale Suche</button>'
        html += '</td>'
        html += '</tr>'
    
    html += '</tbody></table></div>'
    
    return html
