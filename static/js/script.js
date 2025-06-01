$(document).ready(function() {
    console.log("jQuery ready - Initialisierung beginnt.");

    // -- Selektoren für Hauptanalyse (jQuery) --
    const analyzeButton = $('#analyze');
    const minAgeSlider = $('#min_age');
    const maxAgeSlider = $('#max_age');
    const minAgeValue = $('#min_age_value'); // Span für Min-Alter Anzeige
    const maxAgeValue = $('#max_age_value'); // Span für Max-Alter Anzeige
    const wPopSlider = $('#w_pop');
    const wAgeSlider = $('#w_age');
    const wIncomeSlider = $('#w_income');
    const wPopValue = $('#w_pop_value');       // Span für Pop-Gewicht Anzeige
    const wAgeValue = $('#w_age_value');       // Span für Age-Gewicht Anzeige
    const wIncomeValue = $('#w_income_value'); // Span für Income-Gewicht Anzeige
    const loadingIndicator = $('#loading-spinner');
    const resultsContainer = $('#results');
    const resultsDetailsContainer = $('#results-details');
    const errorContainer = $('#error-message');
    const tableContent = $('#table-container');
    const barContent = $('#chart-container-top10');
    const pieChartContainer = $('#pie-chart-container');
    const scatterPlotContainer = $('#scatter-plot-container');
    const clusteringPlotContainer = $('#clustering-plot-container');
    const clusteringTableContainer = $('#clustering-table-container');
    const clustering2PlotContainer = $('#clustering2-plot-container');
    const clustering2TableContainer = $('#clustering2-table-container');
    const exportSelectedBtn = $('#exportSelectedBtn'); // Neuer Button
    const selectedCountElement = $('#selectedCount'); // Neuer Zähler
    
    // NEU: Selektoren für Filter
    const filterPanel = $('#filterPanel');
    const stateFilterContainer = $('#stateFilterContainer');
    const clusterCheckboxes = $('.cluster-filter'); // Alle Cluster-Checkboxes
    const minPopulationInput = $('#minPopulation');
    const maxPopulationInput = $('#maxPopulation');
    const minIncomeInput = $('#minIncome');
    const maxIncomeInput = $('#maxIncome');
    const minTargetInput = $('#minTarget');
    const maxTargetInput = $('#maxTarget');
    const minScoreInput = $('#minScore');
    const maxScoreInput = $('#maxScore');
    const applyFiltersBtn = $('#applyFiltersBtn');
    const resetAllFiltersBtn = $('#resetAllFiltersBtn');
    const stadtSearchInput = $('#stadtSearchInput');
    const bundeslandSearchInput = $('#bundeslandSearchInput');
    const vertriebsnummerSearchInput = $('#vertriebsnummerSearchInput');
    
    // --- Globale Variablen für Auswahl ---
    let selectedCities = []; // Array für ausgewählte city_ids
    let lastCheckedRow = null; // Für Shift+Click
    let currentAnalysisParams = {}; // Speichert die Parameter der letzten Analyse
    
    // --- Initialisierung & Event Listener für Hauptanalyse (jQuery) ---
    console.log("Suche nach Parameter-Elementen:", 
        "minAgeSlider:", minAgeSlider.length, 
        "maxAgeSlider:", maxAgeSlider.length, 
        "wPopSlider:", wPopSlider.length, 
        "wAgeSlider:", wAgeSlider.length, 
        "wIncomeSlider:", wIncomeSlider.length, 
        "analyzeButton:", analyzeButton.length
    );

    // Funktion zum Aktualisieren der Altersslider-Anzeige
    function updateAgeSliders() {
        if (!minAgeSlider.length || !maxAgeSlider.length) return; // Abbruch, wenn Elemente nicht gefunden
        
        let minVal = parseInt(minAgeSlider.val());
        let maxVal = parseInt(maxAgeSlider.val());

        // Sicherstellen, dass min <= max
        if (minVal > maxVal) {
            // Vertausche die Werte oder setze min = max / max = min
             minAgeSlider.val(maxVal); // Setze min auf den Wert von max
             minVal = maxVal; // Aktualisiere den lokalen Wert
             // Oder umgekehrt: maxAgeSlider.val(minVal);
        }
        
        // Aktualisiere die Anzeige in den Spans
        if (minAgeValue.length) minAgeValue.text(minVal);
        if (maxAgeValue.length) maxAgeValue.text(maxVal);
        
        // Optional: Aktualisiere das kombinierte Label (falls es verwendet wird)
        const ageRangeLabel = $('#age_range_label'); // Suche nach dem alten Label zur Sicherheit
        if (ageRangeLabel.length) {
             ageRangeLabel.text(`${minVal} - ${maxVal}`);
        }
        //console.log(`Age range: ${minVal} - ${maxVal}`);
    }

    // Funktion zum Aktualisieren der Gewichtungsanzeige
    function updateWeightIndicators() {
        if (!wPopSlider.length || !wAgeSlider.length || !wIncomeSlider.length) return;
        
        const wPop = parseInt(wPopSlider.val());
        const wAge = parseInt(wAgeSlider.val());
        const wIncome = parseInt(wIncomeSlider.val());
        
        // Werte (0-100) als Prozentsätze anzeigen
        if(wPopValue.length) wPopValue.text(wPop + '%');
        if(wAgeValue.length) wAgeValue.text(wAge + '%');
        if(wIncomeValue.length) wIncomeValue.text(wIncome + '%');
        //console.log(`Weights: Pop=${wPop}%, Age=${wAge}%, Income=${wIncome}%`);
        
        // TODO: Optional Logik hinzufügen, um Summe auf 100% zu normalisieren
    }
    
    // Initialisiere die Anzeigen, wenn Slider vorhanden sind
    if (minAgeSlider.length && maxAgeSlider.length) {
        console.log("Initialisiere Alters-Slider Anzeige.");
        updateAgeSliders();
        // Event Listener hinzufügen
        minAgeSlider.on('input', updateAgeSliders);
        maxAgeSlider.on('input', updateAgeSliders);
    } else {
        console.warn("Alters-Slider (min_age/max_age) nicht gefunden.");
    }

    if (wPopSlider.length && wAgeSlider.length && wIncomeSlider.length) {
         console.log("Initialisiere Gewichtungs-Slider Anzeige.");
         updateWeightIndicators();
         // Event Listener hinzufügen
         wPopSlider.on('input', updateWeightIndicators);
         wAgeSlider.on('input', updateWeightIndicators);
         wIncomeSlider.on('input', updateWeightIndicators);
    } else {
         console.warn("Gewichtungs-Slider (w_pop/w_age/w_income) nicht gefunden.");
    }
    
    // --- Event Listener für Analysieren Button (jQuery) ---
    if (analyzeButton.length) {
        analyzeButton.on('click', function(event) {
            event.preventDefault(); 
            console.log("Analysieren Button geklickt!"); 
            
            if (!minAgeSlider.length || !maxAgeSlider.length || !wPopSlider.length || !wAgeSlider.length || !wIncomeSlider.length) {
                console.error("Fehler: Slider-Elemente nicht gefunden.");
                errorContainer.text("Fehler: Steuerelemente für Parameter nicht gefunden.").show();
                return; 
            }
            
            // AKTUELLE Parameter speichern für späteren Export
            currentAnalysisParams = {
                min_age: minAgeSlider.val(),
                max_age: maxAgeSlider.val(),
                w_pop: parseFloat(wPopSlider.val()) / 100.0,
                w_age: parseFloat(wAgeSlider.val()) / 100.0,
                w_income: parseFloat(wIncomeSlider.val()) / 100.0
            };

            loadingIndicator.show();
            resultsContainer.hide(); 
            resultsDetailsContainer.hide(); 
            errorContainer.hide();
            
            const formData = new FormData(); 
            formData.append('min_age', currentAnalysisParams.min_age);
            formData.append('max_age', currentAnalysisParams.max_age);
            formData.append('w_pop', currentAnalysisParams.w_pop);
            formData.append('w_age', currentAnalysisParams.w_age);
            formData.append('w_income', currentAnalysisParams.w_income);
            
            // Reset der Auswahl bei neuer Analyse
            selectedCities = [];
            lastCheckedRow = null;
            updateSelectedCount();
            updateExportButton();
            // "Select All" Checkbox zurücksetzen, falls vorhanden
            const selectAllCheckbox = tableContent.find('.select-all-checkbox');
            if (selectAllCheckbox.length) {
                selectAllCheckbox.prop('checked', false);
                selectAllCheckbox.prop('indeterminate', false);
            }

            $.ajax({
                url: '/process',
                type: 'POST',
                data: formData,
                processData: false,
                contentType: false,
                success: function(data) {
                    console.log("Daten empfangen:", data);
                    if (data.error) {
                         errorContainer.text("Backend-Fehler: " + data.error).show();
                         resultsContainer.show();
                         resultsDetailsContainer.hide();
                    } else {
                        parseResponse(data); 
                        resultsContainer.show(); 
                        resultsDetailsContainer.show(); 
                    }
                },
                error: function(xhr, status, error) {
                    console.error('Fehler bei der Analyse:', status, error, xhr.responseText);
                    let errorMsg = 'Ein unerwarteter Fehler ist aufgetreten (' + status + ').';
                    if (xhr.responseJSON && xhr.responseJSON.error) {
                        errorMsg = xhr.responseJSON.error;
                    } else if (xhr.responseText) {
                        // Versuche, Fehler aus dem Text zu extrahieren
                        try {
                            const errData = JSON.parse(xhr.responseText);
                            if (errData.error) errorMsg = errData.error;
                        } catch(e) { 
                             // Manchmal ist es nur HTML, z.B. bei 500er Fehlern ohne JSON
                             if (xhr.responseText.toLowerCase().includes("internal server error")) {
                                  errorMsg = "Interner Serverfehler (500). Bitte Server-Logs prüfen.";
                             } else {
                                  // Fallback
                                  errorMsg = `Fehler: ${status} - ${error}`; 
                             }
                        }
                    }
                    errorContainer.text(errorMsg).show();
                    resultsContainer.show(); // Leeren Container zeigen
                    resultsDetailsContainer.hide(); 
                },
                complete: function() {
                    loadingIndicator.hide();
                }
            });
        });
        console.log("Event Listener für #analyze hinzugefügt.");
    } else {
         console.error("Fehler: Button mit ID 'analyze' nicht gefunden.");
    }
    
    // --- Funktion zum Parsen der Antwort und Aktualisieren der UI (vereinfacht) ---
    function parseResponse(data) {
        console.log("Verarbeite Antwort in parseResponse...");
        
        // Detail-Container zuerst anzeigen, damit Elemente gefunden werden
        resultsDetailsContainer.show(); 
        // Haupt-Ergebniscontainer leeren (wo vorher "Bitte warten" stand)
        resultsContainer.empty(); 

        // Leere spezifische Inhaltscontainer VOR dem Befüllen
        tableContent.empty();
        barContent.empty();
        if (pieChartContainer.length) pieChartContainer.empty(); 
        else console.warn("pieChartContainer nicht gefunden zum Leeren.");
        scatterPlotContainer.empty();
        clusteringPlotContainer.empty();
        clusteringTableContainer.empty();
        clustering2PlotContainer.empty();
        clustering2TableContainer.empty();
        errorContainer.hide(); 

        // Tabelle laden
        if (data.table_html) {
            console.log("Lade Tabelle...");
            tableContent.html(data.table_html);
            populateStateFilter(); 
            populateClusterFilter(); 
            setupTableAndFilterListeners(); // Wichtig: Setup *nach* dem Einfügen des HTMLs
            applyTableFilters(); 
        } else {
             console.warn("Keine Tabellendaten (table_html) in Antwort gefunden.");
             tableContent.html('<p class="text-muted">Keine Tabellendaten verfügbar.</p>');
             // Filter leeren, wenn keine Tabelle da ist
             stateFilterContainer.html(''); 
             clusterFilterContainer.html(''); // Auch Cluster-Filter leeren
        }

        // Balkendiagramm (Top 10) (ENTFERNT)
        /*
        if (data.bar_chart_top10) {
             console.log("Lade Balkendiagramm...");
             // barContent.html('<img src="data:image/png;base64,' + data.bar_chart_top10 + '" alt="Top 10 Städte" class="img-fluid">');
        } else {
            console.warn("Kein Balkendiagramm (bar_chart_top10) in Antwort gefunden.");
            // barContent.html('<p class="text-muted">Kein Balkendiagramm verfügbar.</p>');
        }
        */
        
        // Kreisdiagramm 
        if (data.pie_chart && pieChartContainer.length) {
            console.log("Lade Kreisdiagramm...");
            const cityHeading = $('<h4>').attr('id', 'cityChartHeading').addClass('mt-3 mb-2').text('Altersverteilung (Gesamt oder Top-Stadt)');
            const cityChartImg = $('<img>').attr('src', 'data:image/png;base64,' + data.pie_chart).addClass('img-fluid');
             // Füge zuerst die Elemente hinzu, die im JS erstellt wurden
            pieChartContainer.append(cityHeading); 
            // Stelle sicher, dass Ladeanzeige/Fehler weg sind (falls sie vorher im HTML waren und nicht im JS erstellt)
            pieChartContainer.find('.spinner-border, .alert').remove(); 
            pieChartContainer.append(cityChartImg);
        } else {
             if(!data.pie_chart) console.warn("Kein Kreisdiagramm (pie_chart) in Antwort gefunden.");
             if(!pieChartContainer.length) console.warn("pieChartContainer nicht im DOM gefunden für Kreisdiagramm.");
             if(pieChartContainer.length) pieChartContainer.html('<p class="text-muted">Kein Kreisdiagramm verfügbar.</p>');
        }
        
        // Erweiterte Analyse
        if (data.advanced_analysis) {
             console.log("Lade erweiterte Analyse...");
             // Scatter Plot
             if (data.advanced_analysis.scatter_plot) {
                 scatterPlotContainer.html('<img src="data:image/png;base64,' + data.advanced_analysis.scatter_plot + '" alt="Scatter Plot" class="img-fluid">');
                 // Link zum interaktiven Plot (falls benötigt, ID anpassen)
                 const scatterLink = $('<a>').attr('href', '/interactive_scatter').attr('target', '_blank').addClass('btn btn-sm btn-outline-primary mt-2').text('Interaktiven Plot öffnen');
                 scatterPlotContainer.append($('<p class="text-center">').append(scatterLink)); 
             } else {
                 scatterPlotContainer.html('<p class="text-muted">Kein Scatter Plot verfügbar.</p>');
             }
             // Clustering 1
             if (data.advanced_analysis.clustering && data.advanced_analysis.clustering.plot) {
                 clusteringPlotContainer.html('<img src="data:image/png;base64,' + data.advanced_analysis.clustering.plot + '" alt="Clustering 1 Plot" class="img-fluid">');
                 const cluster1Link = $('<a>').attr('href', '/interactive_clustering').attr('target', '_blank').addClass('btn btn-sm btn-outline-primary mt-2').text('Interaktives Clustering öffnen');
                 clusteringPlotContainer.append($('<p class="text-center">').append(cluster1Link)); 
             } else {
                  clusteringPlotContainer.html('<p class="text-muted">Kein Clustering 1 Plot verfügbar.</p>');
             }
             if (data.advanced_analysis.clustering && data.advanced_analysis.clustering.table) {
                 clusteringTableContainer.html(data.advanced_analysis.clustering.table);
             } else {
                  clusteringTableContainer.html('');
             }
             // Clustering 2
             if (data.advanced_analysis.clustering2 && data.advanced_analysis.clustering2.plot) {
                 clustering2PlotContainer.html('<img src="data:image/png;base64,' + data.advanced_analysis.clustering2.plot + '" alt="Clustering 2 Plot" class="img-fluid">');
                 const cluster2Link = $('<a>').attr('href', '/interactive_clustering2').attr('target', '_blank').addClass('btn btn-sm btn-outline-primary mt-2').text('Interaktives Clustering 2 öffnen');
                 clustering2PlotContainer.append($('<p class="text-center">').append(cluster2Link)); 
             } else {
                  clustering2PlotContainer.html('<p class="text-muted">Kein Clustering 2 Plot verfügbar.</p>');
             }
             if (data.advanced_analysis.clustering2 && data.advanced_analysis.clustering2.table) {
                 clustering2TableContainer.html(data.advanced_analysis.clustering2.table);
             } else {
                 clustering2TableContainer.html('');
             }
        } else {
             console.warn("Keine Daten für erweiterte Analyse (advanced_analysis) in Antwort gefunden.");
             scatterPlotContainer.empty();
             clusteringPlotContainer.empty();
             clusteringTableContainer.empty();
             clustering2PlotContainer.empty();
             clustering2TableContainer.empty();
        }
        
        // Stelle sicher, dass der erste Tab aktiv ist (Bootstrap 5)
        var firstTabEl = document.querySelector('#resultsTab li:first-child a')
        if (firstTabEl) {
             var firstTab = new bootstrap.Tab(firstTabEl); // Verwende Bootstrap 5 API
             firstTab.show();
        }
       
        console.log("parseResponse abgeschlossen.");
    }
    
    // --- Funktion zum Befüllen des Bundesländer-Filters ---
    function populateStateFilter() {
        console.log("Befülle Bundesländer-Filter...");
        const table = $('#data-table');
        if (!table.length) {
            stateFilterContainer.html('<p class="text-danger small">Fehler: Tabelle nicht gefunden.</p>');
            return;
        }

        const states = new Set();
        table.find('tbody tr').each(function() {
            const state = $(this).find('td').eq(3).text().trim(); // Spalte 4 (Index 3)
            if (state && state !== 'None') {
                states.add(state);
            }
        });

        stateFilterContainer.empty(); // Vorherigen Inhalt leeren
        if (states.size === 0) {
            stateFilterContainer.html('<p class="text-muted small">Keine Bundesländer gefunden.</p>');
            return;
        }

        // "Alle auswählen" Checkbox hinzufügen
        stateFilterContainer.append(`
            <div class="form-check">
                <input class="form-check-input state-filter-all" type="checkbox" id="state-filter-all" checked>
                <label class="form-check-label fw-bold" for="state-filter-all">Alle auswählen</label>
            </div>
            <hr class="my-1">
        `);

        // Checkboxen für jedes Bundesland
        Array.from(states).sort().forEach((state, index) => {
            const id = `state-filter-${index}`;
            stateFilterContainer.append(`
                <div class="form-check">
                    <input class="form-check-input state-filter" type="checkbox" value="${state}" id="${id}" checked>
                    <label class="form-check-label" for="${id}">${state}</label>
                </div>
            `);
        });
        console.log("Bundesländer-Filter befüllt.");
    }

    // --- NEU: Funktion zum Befüllen des Cluster-Filters ---
    function populateClusterFilter() {
        console.log("Befülle Cluster-Filter...");
        const table = $('#data-table');
        const clusterFilterContainer = $('#clusterFilterContainer'); // Sicherstellen, dass der Selektor hier verfügbar ist
        if (!table.length) {
            clusterFilterContainer.html('<p class="text-danger small">Fehler: Tabelle nicht gefunden.</p>');
            return;
        }

        const clusters = {}; // Objekt zum Speichern von {name: cssClass}
        table.find('tbody tr .cluster-badge').each(function() {
            const badge = $(this);
            const name = badge.text().trim();
            // Extrahiere die Klasse (z.B. cluster-1)
            const cssClassMatch = badge.attr('class').match(/cluster-\d+/);
            if (name && cssClassMatch && !clusters[name]) {
                clusters[name] = cssClassMatch[0]; // Speichere Name und Klasse
            }
        });

        clusterFilterContainer.empty(); // Vorherigen Inhalt leeren
        if (Object.keys(clusters).length === 0) {
            clusterFilterContainer.html('<p class="text-muted small">Keine Cluster gefunden.</p>');
            return;
        }

        // Checkboxen für jeden gefundenen Cluster erstellen (sortiert nach Klasse)
        Object.entries(clusters)
            .sort(([, classA], [, classB]) => classA.localeCompare(classB)) // Sortieren nach cluster-X Klasse
            .forEach(([name, cssClass], index) => {
                const id = `cluster-filter-${index}`; // Eindeutige ID generieren
                // Verwende den extrahierten Klassennamen für das Badge
                clusterFilterContainer.append(`
                    <div class="form-check">
                        <input class="form-check-input cluster-filter" type="checkbox" value="${name}" id="${id}" checked>
                        <label class="form-check-label" for="${id}">
                            <span class="cluster-badge ${cssClass}">${name}</span>
                        </label>
                    </div>
                `);
        });
        console.log("Cluster-Filter befüllt.");
    }

    // --- Funktion zum Anwenden der Filter auf die Tabelle ---
    function applyTableFilters() {
        console.log("Wende Filter an...");
        const table = $('#data-table');
        if (!table.length) return;

        // Aktive Filter lesen
        const stadtSearchText = stadtSearchInput.val()?.toLowerCase().trim() ?? '';
        const bundeslandSearchText = bundeslandSearchInput.val()?.toLowerCase().trim() ?? '';
        const vertriebsnummerSearchText = vertriebsnummerSearchInput.val()?.toLowerCase().trim() ?? '';
        const activeStates = $('.state-filter:checked').map(function() { return $(this).val().toLowerCase(); }).get();
        const activeClusters = $('#clusterFilterContainer .cluster-filter:checked').map(function() {
            return $(this).val().toLowerCase();
        }).get();
        
        const minPop = parseInt(minPopulationInput.val()) || 0;
        const maxPop = parseInt(maxPopulationInput.val()) || Infinity;
        const minInc = parseInt(minIncomeInput.val()) || 0;
        const maxInc = parseInt(maxIncomeInput.val()) || Infinity;
        const minTarg = parseFloat(minTargetInput.val()) || 0;
        const maxTarg = parseFloat(maxTargetInput.val()) || 100;
        const minScr = parseFloat(minScoreInput.val()) || 0;
        const maxScr = parseFloat(maxScoreInput.val()) || 100;
        
        // Zeilen durchgehen und ein-/ausblendet
        let visibleRowCount = 0;
        table.find('tbody tr').each(function() {
            const row = $(this);
            const cells = row.find('td');
            
            // Werte aus Zellen extrahieren (Indizes korrigiert!)
            const platz = cells.eq(1).text().trim();
            const stadt = cells.eq(2).text().toLowerCase();
            const bundesland = cells.eq(3).text().trim().toLowerCase();
            const einwohnerText = cells.eq(4).text().replace(/\./g, '').trim();
            const einkommenText = cells.eq(5).text().replace(/\./g, '').trim();
            const zielgruppeText = cells.eq(6).text().replace('%', '').trim();
            const scoreText = cells.eq(7).text().replace('%', '').trim(); // Korrigiert von Index 8
            const clusterCell = cells.eq(8); // Korrigiert von Index 7
            const vertriebsnummerText = cells.eq(9).text().toLowerCase(); // Korrigierter Spaltenname

            const clusterBadge = clusterCell.find('.cluster-badge');
            const clusterText = clusterBadge.length ? clusterBadge.text().trim().toLowerCase() : clusterCell.text().trim().toLowerCase();

            const einwohner = parseInt(einwohnerText) || 0;
            const einkommen = parseInt(einkommenText) || 0;
            const zielgruppe = parseFloat(zielgruppeText) || 0;
            const score = parseFloat(scoreText) || 0;

            // Filter anwenden
            const showRow =
                // Spezifische Textsuche für jede Spalte
                (stadtSearchText === '' || stadt.includes(stadtSearchText)) &&
                (bundeslandSearchText === '' || bundesland.includes(bundeslandSearchText)) &&
                (vertriebsnummerSearchText === '' || vertriebsnummerText.includes(vertriebsnummerSearchText)) &&
                // Bundesland Filter (Checkboxen)
                (activeStates.length === 0 || activeStates.includes(bundesland) || $('.state-filter-all').is(':checked')) &&
                // Cluster Filter
                (activeClusters.length === 0 || activeClusters.includes(clusterText)) &&
                // Numerische Filter
                (einwohner >= minPop && einwohner <= maxPop) &&
                (einkommen >= minInc && einkommen <= maxInc) &&
                (zielgruppe >= minTarg && zielgruppe <= maxTarg) &&
                (score >= minScr && score <= maxScr);

            if (showRow) {
                row.show();
                visibleRowCount++;
            } else {
                row.hide();
            }
        });
        console.log(`${visibleRowCount} Zeilen entsprechen den Filtern.`);
        // Optional: Feedback geben, wenn keine Zeilen sichtbar sind
    }

    // --- Funktion zum Zurücksetzen aller Filter ---
    function resetAllFilters() {
        console.log("Setze alle Filter zurück...");
        stadtSearchInput.val('');
        bundeslandSearchInput.val('');
        vertriebsnummerSearchInput.val('');
        $('.state-filter, .state-filter-all').prop('checked', true);
        $('.cluster-filter').prop('checked', true);
        $('.num-filter').val(''); // Numerische Felder leeren
        applyTableFilters(); // Filter anwenden, um alle Zeilen anzuzeigen
    }

    // --- Event Listener für Filter und Tabelle ---
    function setupTableAndFilterListeners() {
        console.log("Initialisiere Filter- UND Tabellen-Checkbox-Event-Listener...");
        
        applyFiltersBtn.off('click').on('click', applyTableFilters);
        resetAllFiltersBtn.off('click').on('click', resetAllFilters);
        stateFilterContainer.off('change', '.state-filter-all').on('change', '.state-filter-all', function(){
            const isChecked = $(this).is(':checked');
            stateFilterContainer.find('.state-filter').prop('checked', isChecked);
            // applyTableFilters(); // Optional: Direkt anwenden?
        });
        
        stateFilterContainer.off('change', '.state-filter').on('change', '.state-filter', function(){
             const total = stateFilterContainer.find('.state-filter').length;
             const checked = stateFilterContainer.find('.state-filter:checked').length;
             stateFilterContainer.find('.state-filter-all').prop('checked', total === checked);
             // applyTableFilters(); // Optional: Direkt anwenden?
         });

        stadtSearchInput.off('keyup').on('keyup', function() {
            applyTableFilters();
            updateSelectAllCheckboxState();
        });
        bundeslandSearchInput.off('keyup').on('keyup', function() {
            applyTableFilters();
            updateSelectAllCheckboxState();
        });
        vertriebsnummerSearchInput.off('keyup').on('keyup', function() {
            applyTableFilters();
            updateSelectAllCheckboxState();
        });
        
        // --- Checkbox Listener HIER implementieren ---
        tableContent.off('change', '.select-checkbox, .select-all-checkbox').on('change', '.select-checkbox, .select-all-checkbox', function(event) {
            const checkbox = $(this);
            const isSelectAll = checkbox.hasClass('select-all-checkbox');
            const isChecked = checkbox.is(':checked');

            if (isSelectAll) {
                // Select All Logik (nur für sichtbare Zeilen!)
                tableContent.find('.city-row:visible .select-checkbox').each(function() {
                    const rowCheckbox = $(this);
                    const row = rowCheckbox.closest('.city-row');
                    const cityId = row.data('id'); 
                    
                    rowCheckbox.prop('checked', isChecked);
                    row.toggleClass('table-info', isChecked); 

                    // Update selectedCities array
                    if (isChecked) {
                        if (!selectedCities.includes(cityId)) {
                            selectedCities.push(cityId);
                        }
                    } else {
                        selectedCities = selectedCities.filter(id => id !== cityId);
                    }
                });
                lastCheckedRow = null; 
            } else {
                // Einzelne Checkbox Logik
                const row = checkbox.closest('.city-row');
                const cityId = row.data('id');
                row.toggleClass('table-info', isChecked);

                if (event.shiftKey && lastCheckedRow && lastCheckedRow.closest('#data-table').length > 0) { // Prüfen ob lastCheckedRow noch gültig ist
                    // Shift-Click Logik (nur für sichtbare Zeilen!)
                    const tableRows = tableContent.find('.city-row:visible'); 
                    const lastIndex = tableRows.index(lastCheckedRow);
                    const currentIndex = tableRows.index(row);
                    
                    // Stelle sicher, dass Indizes gültig sind
                    if (lastIndex !== -1 && currentIndex !== -1) {
                        const start = Math.min(lastIndex, currentIndex);
                        const end = Math.max(lastIndex, currentIndex);

                        for (let i = start; i <= end; i++) {
                            const currentRow = tableRows.eq(i);
                            const currentCheckbox = currentRow.find('.select-checkbox');
                            const currentCityId = currentRow.data('id');
                            
                            if (currentCheckbox.prop('checked') !== isChecked) {
                                currentCheckbox.prop('checked', isChecked);
                                currentRow.toggleClass('table-info', isChecked);
                                if (isChecked) {
                                    if (!selectedCities.includes(currentCityId)) {
                                        selectedCities.push(currentCityId);
                                    }
                                } else {
                                    selectedCities = selectedCities.filter(id => id !== currentCityId);
                                }
                            }
                        }
                    } else {
                         // Fallback, wenn Indizes ungültig (z.B. nach Filterung)
                         if (isChecked) {
                            if (!selectedCities.includes(cityId)) selectedCities.push(cityId);
                        } else {
                            selectedCities = selectedCities.filter(id => id !== cityId);
                        }
                    }
                } else {
                     // Normaler Klick auf einzelne Checkbox
                     if (isChecked) {
                        if (!selectedCities.includes(cityId)) {
                            selectedCities.push(cityId);
                        }
                    } else {
                        selectedCities = selectedCities.filter(id => id !== cityId);
                    }
                }
                lastCheckedRow = row; // Speichere als letztes geklickt
            }
             // Update Select All Checkbox Status nach jeder Änderung
            updateSelectAllCheckboxState();
            // Update Zähler und Button
            updateSelectedCount();
            updateExportButton();
            // console.log("Ausgewählte Städte:", selectedCities);
        });
        // --- Ende Checkbox Listener ---

        setupCityRowClickListener(); // Listener für Kreisdiagramm
        console.log("Filter- UND Checkbox-Listener initialisiert.");
    }
    
    // Funktion extrahiert, um Klick-Listener für Zeilen zu setzen
    function setupCityRowClickListener() {
         tableContent.off('click', '.city-row').on('click', '.city-row', function(event) {
             if (!$(event.target).is('input:checkbox') && !$(event.target).closest('button').length) { 
                 const cityId = $(this).data('id');
                 const cityName = $(this).find('td').eq(2).text(); 
                 console.log("Zeile geklickt für Stadt-ID:", cityId, "Name:", cityName);
                 if (cityId) {
                    loadCityPieChart(cityId, cityName); 
                 }
             }
        });
    }

    // --- Funktion zum Laden des Stadt-spezifischen Kreisdiagramms ---
    function loadCityPieChart(cityId, cityName) {
        console.log(`Lade Kreisdiagramm für Stadt ${cityId} (${cityName})...`);
        const pieContainer = $('#pie-chart-container'); 
        const loadingHTML = '<div class="spinner-border text-primary spinner-border-sm" role="status"><span class="visually-hidden">Laden...</span></div>';
        const errorHTML = '<p class="text-danger mt-2">Fehler beim Laden des Diagramms.</p>';
        
        // Alten Inhalt leeren und Ladeanzeige zeigen
        pieContainer.html(`<h5 class="mt-3 mb-2">Altersverteilung: ${cityName} ${loadingHTML}</h5>`);

        // Aktuelle Parameter aus den Slidern holen
        const minAge = minAgeSlider.val() || 18; // Fallback
        const maxAge = maxAgeSlider.val() || 35; // Fallback

        // AJAX-Aufruf an die Backend-Route
        $.ajax({
            url: `/city_chart/${cityId}?min_age=${minAge}&max_age=${maxAge}`, // Route anpassen, falls nötig
            type: 'GET',
            success: function(data) {
                if (data.error) {
                     console.error("Fehler vom Backend:", data.error);
                     pieContainer.html(`<h5 class="mt-3 mb-2">Altersverteilung: ${cityName}</h5>${errorHTML}`);
                } else {
                    console.log("Kreisdiagramm-Daten empfangen.");
                    const imgHTML = `<img src="data:image/png;base64,${data.pie_chart}" alt="Altersverteilung ${cityName}" class="img-fluid" style="max-width: 400px;">`;
                    pieContainer.html(`<h5 class="mt-3 mb-2">Altersverteilung: ${data.city_name || cityName}</h5>${imgHTML}`);
                }
            },
            error: function(xhr, status, error) {
                 console.error('Fehler beim Laden des Stadt-Kreisdiagramms:', status, error, xhr.responseText);
                 pieContainer.html(`<h5 class="mt-3 mb-2">Altersverteilung: ${cityName}</h5>${errorHTML}`);
            }
        });
    }
    
    // --- Funktion zur Aktualisierung des Ausgewählt-Zählers ---
    function updateSelectedCount() {
        $('#selectedCount').text(selectedCities.length);
        
        // Trigger eines benutzerdefinierten Events, um andere Komponenten zu informieren
        const event = new CustomEvent('citiesSelectionChanged', {
            detail: {
                selectedIds: selectedCities,
                count: selectedCities.length
            },
            bubbles: true
        });
        document.dispatchEvent(event);
    }

    // --- NEU: Funktion zum Aktivieren/Deaktivieren des Export-Buttons ---
    function updateExportButton() {
        if (exportSelectedBtn.length) {
            exportSelectedBtn.prop('disabled', selectedCities.length === 0);
        }
    }
    
    // --- NEU: Funktion zum Aktualisieren des "Select All" Checkbox-Status ---
    function updateSelectAllCheckboxState() {
        const totalVisibleCheckboxes = tableContent.find('.city-row:visible .select-checkbox').length;
        const checkedVisibleCheckboxes = tableContent.find('.city-row:visible .select-checkbox:checked').length;
        const selectAllCheckbox = tableContent.find('.select-all-checkbox');
        
        if (selectAllCheckbox.length && totalVisibleCheckboxes > 0) { // Nur aktualisieren, wenn Zeilen sichtbar sind
             if (checkedVisibleCheckboxes === 0) {
                 selectAllCheckbox.prop('checked', false);
                 selectAllCheckbox.prop('indeterminate', false);
             } else if (checkedVisibleCheckboxes === totalVisibleCheckboxes) {
                 selectAllCheckbox.prop('checked', true);
                 selectAllCheckbox.prop('indeterminate', false);
             } else {
                 selectAllCheckbox.prop('checked', false);
                 selectAllCheckbox.prop('indeterminate', true);
             }
        } else if (selectAllCheckbox.length) {
            // Keine sichtbaren Zeilen -> Checkbox deaktivieren/abwählen
            selectAllCheckbox.prop('checked', false);
            selectAllCheckbox.prop('indeterminate', false);
        }
    }

    // --- NEU: Event Listener für Export Button ---
    if (exportSelectedBtn.length) {
         exportSelectedBtn.off('click').on('click', function() { // .off() um Doppelbindung zu vermeiden
             console.log("Exportiere Städte:", selectedCities);
             exportSelectedCities();
         });
         console.log("Event Listener für #exportSelectedBtn hinzugefügt.");
    } else {
         console.warn("Export-Button #exportSelectedBtn nicht gefunden.");
    }

    // --- NEU: Funktion zum Exportieren ---
    function exportSelectedCities() {
        if (selectedCities.length === 0) {
            alert("Bitte wählen Sie zuerst Städte für den Export aus.");
            return;
        }

        exportSelectedBtn.prop('disabled', true).html('<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span> Exportiere...');

        // Parameter der letzten Analyse verwenden
        const exportData = {
            selected_cities: selectedCities,
            min_age: currentAnalysisParams.min_age || 18, 
            max_age: currentAnalysisParams.max_age || 35,
            w_pop: currentAnalysisParams.w_pop || 0.3,
            w_age: currentAnalysisParams.w_age || 0.5,
            w_income: currentAnalysisParams.w_income || 0.2
        };

        console.log("Sende Export-Anfrage mit:", exportData);

        fetch('/export_selected', { 
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(exportData)
        })
        .then(response => {
            if (!response.ok) {
                 return response.json().then(errData => {
                     throw new Error(errData.error || `Serverfehler (${response.status})`);
                 }).catch(() => {
                      throw new Error(`Serverfehler (${response.status})`);
                 });
            }
            const header = response.headers.get('Content-Disposition');
            const filenameMatch = header && header.match(/filename="?(.+?)"?$/);
            const filename = filenameMatch ? filenameMatch[1] : 'exportierte_staedte.xlsx';
            return response.blob().then(blob => ({ blob, filename }));
        })
        .then(({ blob, filename }) => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = filename;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            console.log("Export erfolgreich.");
            exportSelectedBtn.prop('disabled', false).html('<i class="bi bi-file-earmark-excel"></i> Auswahl exportieren');
        })
        .catch(error => {
            console.error('Export-Fehler:', error);
            alert(`Fehler beim Exportieren: ${error.message}`);
            exportSelectedBtn.prop('disabled', false).html('<i class="bi bi-file-earmark-excel"></i> Auswahl exportieren');
        });
    }

    // ... (Keyword Suche Modal Logik bleibt unverändert) ...

});

// --- NEU: Funktion zum Aufklappen der Reviews bei Keyword-Suche ---

// Hilfsfunktion zum Generieren von Sternen (Beispiel - ggf. anpassen)
function generateStars(rating) {
    if (typeof rating !== 'number' || rating < 0 || rating > 5) {
        return '<span class="text-muted small">(N/A)</span>'; // Kleinere Anzeige
    }
    const fullStars = Math.floor(rating);
    const halfStar = rating % 1 >= 0.5 ? 1 : 0;
    const emptyStars = 5 - fullStars - halfStar;
    let starsHtml = '';
    starsHtml += '<i class="bi bi-star-fill text-warning"></i>'.repeat(fullStars);
    if (halfStar) {
        starsHtml += '<i class="bi bi-star-half text-warning"></i>';
    }
    starsHtml += '<i class="bi bi-star text-warning"></i>'.repeat(emptyStars);
    // Zeigt die Zahl daneben, etwas kleiner
    return `<span class="rating-stars">${starsHtml}</span> <span class="text-muted small">(${rating.toFixed(1)})</span>`;
}

// Event Listener für das Klicken auf Bewertungen (Event Delegation am Body)
// Annahme: Die Keyword-Ergebnisse werden dynamisch hinzugefügt.
// Annahme: Das klickbare Bewertungselement hat die Klasse 'place-rating-toggle' und ein data-place-id Attribut.
// Annahme: Der Container für die Reviews hat die Klasse 'place-reviews-container' und ein data-place-id Attribut.
// Annahme: Die place Daten (inkl. Reviews) sind im globalen Scope verfügbar, z.B. in einem Objekt 'keywordPlacesData'
// Wenn 'keywordPlacesData' nicht global ist, muss der Zugriff darauf angepasst werden!
let keywordPlacesData = {}; // Platzhalter - muss befüllt werden, wenn die Keyword-Daten ankommen

// NEU: Listener auf den "Bewertungen anzeigen" Button
$(document.body).on('click', '.show-reviews-btn', function() {
    const button = $(this);
    const placeId = button.data('placeId');
    console.log("'Bewertungen anzeigen' geklickt für Place ID:", placeId);

    // Finde den zugehörigen Review-Container
    // Annahme: Der Container ist ein Geschwister-Element oder im selben Elternelement wie der Button
    // und hat die Klasse 'place-reviews-container' und das passende data-place-id.
    // Passe den Selektor ggf. an deine HTML-Struktur an.
    // ALT: const reviewsContainer = button.closest('.keyword-result-item').find(`.place-reviews-container[data-place-id="${placeId}"]`);
    // Alternativ, wenn global gesucht werden soll:
    const reviewsContainer = $(`.place-reviews-container[data-place-id="${placeId}"]`); // NEU: Globale Suche
    
    const placeInfo = keywordPlacesData[placeId]; // Zugriff auf die globalen Daten

    if (!reviewsContainer.length) {
        console.warn("Review-Container nicht gefunden für Place ID:", placeId);
        return;
    }

    if (!placeInfo || !placeInfo.reviews || placeInfo.reviews.length === 0) {
        console.log("Keine Reviews verfügbar für Place ID:", placeId);
        // Optional: Kurze Info im Container anzeigen
        reviewsContainer.html('<p class="text-muted small m-2">Keine Reviews verfügbar.</p>').slideToggle();
        return;
    }

    // Prüfen, ob Reviews bereits angezeigt werden
    if (reviewsContainer.is(':visible')) {
        reviewsContainer.slideUp(function() { $(this).empty(); }); // Ausblenden und leeren
    } else {
        // Reviews generieren und anzeigen
        let reviewsHtml = '<ul class="list-unstyled mt-2 mb-0 px-2">'; // Padding hinzugefügt
        placeInfo.reviews.forEach(review => {
            reviewsHtml += `
                <li class="review-item mb-2 border-bottom pb-1">
                    <div class="d-flex justify-content-between align-items-center mb-1">
                        <small class="text-muted">${review.relative_publish_time_description || 'Unbekannt'}</small>
                        <span class="rating-stars">${generateStars(review.rating)}</span>
                    </div>
                    <p class="review-text small mt-0 mb-1" style="font-size: 0.85em;">${review.text || 'Kein Text.'}</p>
                    ${review.author_name ? `<small class="text-muted d-block text-end" style="font-size: 0.8em;"><em>- ${review.author_name}</em></small>` : ''}
                </li>
            `;
        });
        reviewsHtml += '</ul>';

        reviewsContainer.html(reviewsHtml).slideDown(); // Inhalt setzen und einblenden
    }
});

// WICHTIG:
// 1. Stelle sicher, dass die Daten von `/get_keyword_results_for_cities` in der Variable `keywordPlacesData` gespeichert werden,
//    sobald sie vom Backend empfangen werden.
// 2. Stelle sicher, dass die HTML-Elemente für die Gesamtbewertung die Klasse `place-rating-toggle` und das Attribut
//    `data-place-id="..."` haben.
// 3. Stelle sicher, dass es für jeden Ort einen (initial leeren und versteckten) Container mit der Klasse
//    `place-reviews-container` und dem Attribut `data-place-id="..."` gibt, der die Reviews aufnehmen kann.
// 4. Binde Font Awesome ein, damit die Stern-Icons korrekt angezeigt werden. 