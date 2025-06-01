# Cities - Städteanalyse-Tool für Deutschland

## Übersicht

Cities ist eine Flask-basierte Webanwendung zur demografischen Analyse deutscher Städte. Das Tool kombiniert Bevölkerungsdaten mit Geschäftsinformationen aus der Google Maps Places API, um fundierte Entscheidungen für Standortanalysen zu ermöglichen.

## Hauptfunktionen

- **Demografische Analyse**: Verarbeitung von Bevölkerungsdaten inkl. Altersgruppen und Einkommensinformationen
- **Zielgruppenberechnung**: Flexible Berechnung von Zielgruppen basierend auf Altersgruppen
- **Google Maps Integration**: Automatisierte Suche nach Geschäften (Bars, Restaurants, Clubs etc.) in allen analysierten Städten
- **Clustering-Analyse**: Gruppierung von Städten nach demografischen Merkmalen
- **Interaktive Visualisierungen**: Scatter-Plots und Clustering-Diagramme mit Plotly
- **Datenexport**: Excel-Export von Suchergebnissen mit detaillierten Informationen
- **API-Monitoring**: Integration mit Google Cloud Monitoring zur Überwachung der API-Nutzung

## Technologie-Stack

- **Backend**: Flask (Python)
- **Datenbank**: SQLite
- **Frontend**: HTML/CSS/JavaScript mit Bootstrap
- **Datenanalyse**: Pandas, NumPy, Scikit-learn
- **Visualisierung**: Matplotlib, Plotly, Seaborn
- **APIs**: Google Maps Places API
- **Cloud Services**: Google Cloud Monitoring

## Installation

### Voraussetzungen

- Python 3.12+
- Google Maps API-Schlüssel
- (Optional) Google Cloud Service Account für API-Monitoring

### Schritt-für-Schritt Installation

1. **Repository klonen**
   ```bash
   git clone <repository-url>
   cd Cities
   ```

2. **Virtuelle Umgebung erstellen und aktivieren**
   ```bash
   python -m venv venv312
   # Windows:
   venv312\Scripts\activate
   # Linux/Mac:
   source venv312/bin/activate
   ```

3. **Abhängigkeiten installieren**
   ```bash
   pip install -r requirements.txt
   ```

4. **Umgebungsvariablen konfigurieren**
   
   Erstelle eine `.env` Datei im Hauptverzeichnis:
   ```
   GOOGLE_MAPS_API_KEY=dein_google_maps_api_schlüssel
   # Optional für API-Monitoring:
   GOOGLE_CLOUD_PROJECT=dein_projekt_id
   GOOGLE_APPLICATION_CREDENTIALS=pfad/zu/credentials.json
   ```

5. **Datenbank initialisieren**
   ```bash
   sqlite3 data.db < setup_database.sql
   ```

6. **Anwendung starten**
   ```bash
   python app.py
   ```

   Die Anwendung ist dann unter `http://localhost:5000` erreichbar.

## Datenbankstruktur

Die Anwendung verwendet eine SQLite-Datenbank mit folgenden Haupttabellen:

- **city**: Städteinformationen (Name, Bundesland, Koordinaten)
- **demographics**: Demografische Daten pro Stadt und Jahr
- **age_group**: Definition der Altersgruppen
- **demo_age_dist**: Altersverteilung pro Stadt
- **search_term**: Vordefinierte Suchbegriffe
- **place**: Gefundene Orte aus Google Maps
- **place_search**: Verknüpfung zwischen Suchen und gefundenen Orten
- **rating_history**: Historische Bewertungsdaten
- **opening_hours**: Öffnungszeiten der Orte
- **review**: Bewertungen von Nutzern

## Verwendung

### 1. Demografische Analyse

- Startseite aufrufen und Parameter eingeben:
  - Altersbereich der Zielgruppe (Min/Max)
  - Gewichtung für Bevölkerung, Alter und Einkommen
- "Analyse starten" klicken für die Berechnung

### 2. Ortssuche mit Google Maps

- Navigiere zu "Keyword-Suche"
- Wähle einen Suchbegriff oder füge einen neuen hinzu
- Starte die Suche für alle Städte
- Die Ergebnisse werden in der Datenbank gespeichert und gecacht

### 3. Ergebnisse anzeigen

- Wähle Städte und Suchbegriff aus
- Optional: Filterkriterien festlegen (Mindestbewertung, Anzahl Bewertungen)
- Ergebnisse als Tabelle anzeigen oder als Excel exportieren

### 4. Visualisierungen

- **Scatter-Plot**: Zeigt Einwohnerzahl vs. Einkommen mit Zielgruppenanteil
- **Clustering**: Gruppiert Städte in 5 Kategorien:
  - Mittelstädte
  - Großstädte
  - Ländliche Regionen
  - Universitätsstädte
  - Wohlhabende Mittelstädte

## API-Metriken

Die Anwendung bietet ein Dashboard zur Überwachung der Google Maps API-Nutzung:
- Anzahl der API-Aufrufe
- Erfolgsrate
- Durchschnittliche Antwortzeit
- Historische Daten

## Datenquellen

Die demografischen Daten müssen separat in die Datenbank importiert werden. Das Tool erwartet:
- Bevölkerungszahlen nach Altersgruppen
- Einkommensdaten
- Geografische Informationen (Koordinaten)

## Wartung und Entwicklung

### Datenbank-Updates

Die Anwendung fügt bei Bedarf automatisch neue Spalten zur Datenbank hinzu. Manuelle Migrationen sind in der Regel nicht erforderlich.

### Cache-Verwaltung

Suchergebnisse werden in der Datenbank gecacht. Der Cache kann über SQL-Befehle verwaltet werden:
```sql
-- Cache für einen bestimmten Suchbegriff löschen
DELETE FROM place_search WHERE term_id = ?;
```

### Fehlerbehandlung

- Alle API-Fehler werden geloggt
- Bei fehlgeschlagenen API-Aufrufen wird automatisch ein Retry mit lokalem Bias durchgeführt
- Robuste Fehlerbehandlung für Datenbankoperationen

## Sicherheit

- API-Schlüssel werden über Umgebungsvariablen verwaltet
- Datei-Uploads sind auf 16 MB begrenzt
- SQL-Injection-Schutz durch parametrisierte Queries

## Lizenz

[Lizenzinformationen hier einfügen]

## Kontakt

[Kontaktinformationen hier einfügen] 