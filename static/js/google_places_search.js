// Google Places Keyword-Suche
// Funktionalität für die Verwaltung und Ausführung von Suchbegriffen

let searchTerms = []; // Liste der verfügbaren Suchbegriffe
let searchStatusSource = null; // EventSource für SSE

// Zustand für Tab 1: Gespeicherte Ergebnisse
let savedResultsState = {
    selectedKeywordIds: new Set() // Speichert IDs der ausgewählten Keyword-Chips
};

// Zustand für Tab 2: Live-Suche
let liveSearchState = {
    keyword: '' // Ein einzelner String
};

document.addEventListener('DOMContentLoaded', function() {
    const keywordSearchModalElement = document.getElementById('keywordSearchModal');
    if (!keywordSearchModalElement) {
        console.error("KeywordSearchModal nicht gefunden!");
        return;
    }
    const keywordSearchModal = new bootstrap.Modal(keywordSearchModalElement);

    const savedResultsTabButton = document.getElementById('saved-results-tab-button');
    const liveSearchTabButton = document.getElementById('live-search-tab-button');

    const selectedCitiesCountModalSpan = document.getElementById('selected-cities-count-modal');
    const keywordChipsContainer = document.getElementById('keyword-chips-container');
    const showSavedResultsBtn = document.getElementById('show-saved-results-btn');

    // Elemente für das Suchfeld
    const liveSearchKeywordInput = document.getElementById('live-search-keyword-input');
    const clearKeywordInputBtn = document.getElementById('clear-keyword-input');
    
    const startLiveSearchBtn = document.getElementById('start-live-search-btn');
    
    // Suchstatus-Container (aus alter Logik, wird für Live-Suche benötigt)
    const searchStatusContainer = document.getElementById('search-status-container'); 
    const progressContainer = document.getElementById('search-progress');

    keywordSearchModalElement.addEventListener('show.bs.modal', function () {
        console.log("Modal wird geöffnet");
        
        loadSearchTerms();
        updateSelectedCitiesCountInModal();
        if (savedResultsTabButton && savedResultsTabButton.classList.contains('active')) {
            loadSavedResultsState();
        } else if (liveSearchTabButton && liveSearchTabButton.classList.contains('active')) {
            loadLiveSearchState();
        }
        updateShowSavedResultsButtonState();
        updateStartLiveSearchButtonState();
    });

    if (savedResultsTabButton) {
        savedResultsTabButton.addEventListener('show.bs.tab', function () {
            saveLiveSearchState();
            loadSavedResultsState();
            updateShowSavedResultsButtonState();
        });
    }

    if (liveSearchTabButton) {
        liveSearchTabButton.addEventListener('show.bs.tab', function () {
            console.log("Wechsel zu Live-Suche Tab");
            saveSavedResultsState();
            loadLiveSearchState();
            updateStartLiveSearchButtonState();
            
            // API Metriken laden
            loadApiMetrics();
            
            // Fokus auf das Eingabefeld setzen
            setTimeout(() => {
                if (liveSearchKeywordInput && !liveSearchKeywordInput.disabled) {
                    liveSearchKeywordInput.focus();
                }
            }, 100);
        });
    }

    function loadSearchTerms() {
        fetch('/get_search_terms')
            .then(response => response.ok ? response.json() : Promise.reject('Fehler beim Laden der Suchbegriffe'))
            .then(data => {
                searchTerms = data;
                renderKeywordChips();
                updateShowSavedResultsButtonState();
            })
            .catch(error => {
                console.error('Fehler beim Laden der Suchbegriffe:', error);
                if(keywordChipsContainer) keywordChipsContainer.innerHTML = '<span class="text-danger small">Fehler beim Laden der Suchbegriffe.</span>';
            });
    }

    function renderKeywordChips() {
        if (!keywordChipsContainer) return;
        keywordChipsContainer.innerHTML = '';
        if (searchTerms.length === 0) {
            keywordChipsContainer.innerHTML = '<span class="text-muted small fst-italic">Keine Suchbegriffe vorhanden. Bitte fügen Sie zuerst in der "Live-Suche" Suchbegriffe hinzu.</span>';
            return;
        }
        searchTerms.forEach(term => {
            const chipId = `kw-chip-${term.id}`;
            const isChecked = savedResultsState.selectedKeywordIds.has(String(term.id));
            const span = document.createElement('span');
            span.className = 'badge text-bg-light border p-2 user-select-none d-flex align-items-center';
            span.style.cursor = 'pointer';
            
            // Checkbox
            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.className = 'form-check-input me-1 keyword-chip-checkbox';
            checkbox.value = term.id;
            checkbox.id = chipId;
            checkbox.checked = isChecked;
            checkbox.dataset.keywordName = term.name;
            
            // Label
            const label = document.createElement('label');
            label.className = 'form-check-label me-2';
            label.htmlFor = chipId;
            label.textContent = term.name;
            label.style.cursor = 'pointer';
            
            // Refresh-Button
            const refreshBtn = document.createElement('button');
            refreshBtn.type = 'button';
            refreshBtn.className = 'btn btn-link btn-sm p-0 ms-1 text-success';
            refreshBtn.style.fontSize = '0.875rem';
            refreshBtn.title = `"${term.name}" für alle Städte aktualisieren`;
            refreshBtn.innerHTML = '<i class="bi bi-arrow-clockwise"></i>';
            refreshBtn.onclick = function(event) {
                event.stopPropagation(); // Verhindert, dass der Chip-Click ausgelöst wird
                refreshSingleKeyword(term.id, term.name, refreshBtn);
            };
            
            span.appendChild(checkbox);
            span.appendChild(label);
            span.appendChild(refreshBtn);
            keywordChipsContainer.appendChild(span);
            
            // Click-Handler für den gesamten Chip (aber nicht für Refresh-Button)
            span.addEventListener('click', function(event) {
                if (event.target !== checkbox && event.target !== label && !event.target.closest('button')) {
                    checkbox.checked = !checkbox.checked;
                    checkbox.dispatchEvent(new Event('change', { bubbles: true }));
                }
            });
            
            checkbox.addEventListener('change', function() {
                if (this.checked) savedResultsState.selectedKeywordIds.add(this.value);
                else savedResultsState.selectedKeywordIds.delete(this.value);
                updateShowSavedResultsButtonState();
            });
        });
    }

    if (showSavedResultsBtn) {
        showSavedResultsBtn.addEventListener('click', function() {
            const selectedCityIds = getSelectedCityIdsFromMainTable();
            const selectedKeywordIdsArray = Array.from(savedResultsState.selectedKeywordIds);
            if (selectedCityIds.length === 0) return showNotification('Bitte Städte in der Haupttabelle auswählen.', 'warning');
            if (selectedKeywordIdsArray.length === 0) return showNotification('Bitte Suchbegriff(e) auswählen.', 'warning');
            const resultsUrl = `/keyword_search_results?cities=${selectedCityIds.join(',')}&keywords=${selectedKeywordIdsArray.join(',')}`;
            window.open(resultsUrl, '_blank');
            keywordSearchModal.hide();
        });
    }

    function updateShowSavedResultsButtonState() {
        if (!showSavedResultsBtn) return;
        const citiesSelectedCount = getSelectedCityIdsFromMainTable()?.length || 0;
        showSavedResultsBtn.disabled = !(citiesSelectedCount > 0 && savedResultsState.selectedKeywordIds.size > 0);
    }

    function saveSavedResultsState() {
        // selectedKeywordIds wird direkt in savedResultsState.selectedKeywordIds aktualisiert
    }

    function loadSavedResultsState() {
        renderKeywordChips(); // Stellt Chips und deren Auswahl wieder her
    }
    
    // --- Logik für Tab 2: Live Suche ---
    
    // Event Listener für das Keyword-Eingabefeld
    if (liveSearchKeywordInput) {
        liveSearchKeywordInput.addEventListener('input', function() {
            liveSearchState.keyword = this.value.trim();
            updateStartLiveSearchButtonState();
        });
        
        liveSearchKeywordInput.addEventListener('keypress', function(e) {
            if (e.key === 'Enter' && this.value.trim() !== '') {
                e.preventDefault();
                
                // Direkt die Suche starten
                liveSearchState.keyword = this.value.trim();
                startLiveSearchBtn.click();
            }
        });
    }
    
    // Event Listener für den Clear-Button im Eingabefeld
    if (clearKeywordInputBtn) {
        clearKeywordInputBtn.addEventListener('click', function() {
            if (liveSearchKeywordInput) {
                liveSearchKeywordInput.value = '';
                liveSearchState.keyword = '';
                updateStartLiveSearchButtonState();
                liveSearchKeywordInput.focus();
            }
        });
    }
    
    if (startLiveSearchBtn) {
        startLiveSearchBtn.addEventListener('click', function() {
            if (!liveSearchState.keyword) {
                return showNotification("Bitte geben Sie zuerst einen Suchbegriff ein.", 'warning');
            }
            
            const termToSearch = liveSearchState.keyword; // Einzelner Begriff

            // UI während der Suche deaktivieren
            if (liveSearchKeywordInput) liveSearchKeywordInput.disabled = true;
            if (clearKeywordInputBtn) clearKeywordInputBtn.disabled = true;
            startLiveSearchBtn.disabled = true;

            // Logik für Suchstatus-Anzeige
            if (searchStatusContainer) {
                searchStatusContainer.style.display = 'block';
            }
            if (progressContainer) {
                 progressContainer.innerHTML = `Starte Live-Suche für '${termToSearch}'...`;
            } else {
                console.warn("#search-progress Container nicht gefunden für Statusmeldungen.");
            }
            
            startSSEStatusUpdates(
                // Callback für Abschluss, um UI wieder zu aktivieren
                function() {
                    if (liveSearchKeywordInput) liveSearchKeywordInput.disabled = false;
                    if (clearKeywordInputBtn) clearKeywordInputBtn.disabled = false;
                    updateStartLiveSearchButtonState();
                }
            );

            fetch(`/start_search/${encodeURIComponent(termToSearch)}`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({})
            })
            .then(response => response.ok ? response.json() : response.json().then(err => Promise.reject(err.error || 'Fehler beim Start der Live-Suche')))
            .then(data => {
                console.log("Live-Suchanfrage gesendet:", data.message || data.error);
                if (data.error && progressContainer) {
                     progressContainer.innerHTML += `<br><span class="text-danger">Fehler: ${data.error}</span>`;
                }
                // Erfolgsfeedback kommt via SSE
            })
            .catch(error => {
                console.error('Fehler beim Senden der Live-Suchanfrage:', error);
                if (progressContainer) progressContainer.innerHTML += `<br><span class="text-danger">Übertragungsfehler: ${String(error)}</span>`;
                if (searchStatusSource) searchStatusSource.close();
                
                // UI wieder aktivieren bei Fehler
                if (liveSearchKeywordInput) liveSearchKeywordInput.disabled = false;
                if (clearKeywordInputBtn) clearKeywordInputBtn.disabled = false;
                updateStartLiveSearchButtonState();
            });
        });
    }

    function updateStartLiveSearchButtonState() {
        if (!startLiveSearchBtn) return;
        // Button nur aktivieren, wenn ein Begriff eingegeben ist
        startLiveSearchBtn.disabled = !liveSearchState.keyword;
    }

    function saveLiveSearchState() {
        // State wird direkt in liveSearchState aktualisiert
    }

    function loadLiveSearchState() {
        console.log("Lade Live-Suche Zustand:", liveSearchState);
        
        // Stelle sicher, dass das Eingabefeld den gespeicherten Wert hat
        if (liveSearchKeywordInput) {
            liveSearchKeywordInput.value = liveSearchState.keyword || '';
        }
        
        // Button-Status aktualisieren
        updateStartLiveSearchButtonState();
    }

    function getSelectedCityIdsFromMainTable() {
        if (typeof window.getSelectedCityIds === 'function') {
            return window.getSelectedCityIds();
        }
        const selectedCheckboxes = document.querySelectorAll('#data-table tbody .select-checkbox:checked');
        return Array.from(selectedCheckboxes).map(cb => cb.closest('.city-row')?.dataset.id).filter(id => id);
    }

    function updateSelectedCitiesCountInModal() {
        if (!selectedCitiesCountModalSpan) return;
        const count = getSelectedCityIdsFromMainTable()?.length || 0;
        selectedCitiesCountModalSpan.textContent = count;
        updateShowSavedResultsButtonState(); 
    }
    
    document.addEventListener('citiesSelectionChanged', updateSelectedCitiesCountInModal);

    function startSSEStatusUpdates(callback) {
        if (searchStatusSource) searchStatusSource.close();
        
        // Stelle sicher, dass der Progress-Container aus dem *Haupt-Modal* referenziert wird, nicht aus dem alten Such-Popup
        const currentProgressContainer = keywordSearchModalElement.querySelector('#search-progress'); 
        // Fallback, falls nicht im neuen Modal vorhanden (sollte aber platziert werden)
        const finalProgressContainer = currentProgressContainer || progressContainer; 

        if (!finalProgressContainer) {
            console.error("Fortschrittscontainer nicht gefunden!");
            return;
        }
        finalProgressContainer.innerHTML = 'Verbinde für Status-Updates...'; // Initialer Text
        
        searchStatusSource = new EventSource('/search_status');
        searchStatusSource.onopen = () => finalProgressContainer.innerHTML += '<br>SSE-Verbindung geöffnet.';
        searchStatusSource.onmessage = function(event) {
            const message = event.data;
            if (message === "DONE" || message.includes('Suche abgeschlossen.')) {
                finalProgressContainer.innerHTML += `<br><span class="text-success fw-bold">${message}</span>`;
                if (searchStatusSource) searchStatusSource.close();
                loadSearchTerms(); 
                if (callback) callback();
            } else if (message.startsWith("FEHLER:")) {
                finalProgressContainer.innerHTML += `<br><span class="text-danger">${message}</span>`;
                // Bei kritischen Fehlern auch Callback aufrufen
                if (message.includes("KRITISCH") || message.includes("Abgebrochen")) {
                    if (searchStatusSource) searchStatusSource.close();
                    if (callback) callback();
                }
            } else if (message.startsWith("WARNUNG:")) {
                finalProgressContainer.innerHTML += `<br><span class="text-warning">${message}</span>`;
            } else {
                // Kontinuierliches Update: Zeige nur die letzte Nachricht oder füge hinzu
                finalProgressContainer.innerHTML = message; // Ersetzt für saubere Anzeige
            }
            finalProgressContainer.scrollTop = finalProgressContainer.scrollHeight;
        };
        searchStatusSource.onerror = () => {
            finalProgressContainer.innerHTML += '<br><span class="text-danger">SSE-Verbindungsfehler.</span>';
            if (searchStatusSource) searchStatusSource.close();
            if (callback) callback();
        };
    }

    function showNotification(message, type = 'info') {
        const toastContainer = document.querySelector('.toast-container') || createToastContainer();
        const toastEl = document.createElement('div');
        toastEl.className = `toast align-items-center text-bg-${type} border-0`;
        toastEl.setAttribute('role', 'alert');
        toastEl.setAttribute('aria-live', 'assertive');
        toastEl.setAttribute('aria-atomic', 'true');
        const toastBody = document.createElement('div');
        toastBody.className = 'd-flex';
        toastBody.innerHTML = `<div class="toast-body">${message}</div><button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast" aria-label="Close"></button>`;
        toastEl.appendChild(toastBody);
        toastContainer.appendChild(toastEl);
        const toast = new bootstrap.Toast(toastEl, { autohide: true, delay: 5000 });
        toast.show();
        toastEl.addEventListener('hidden.bs.toast', () => toastEl.remove());
    }

    function createToastContainer() {
        let container = document.querySelector('.toast-container');
        if (!container) {
            container = document.createElement('div');
            container.className = 'toast-container position-fixed bottom-0 end-0 p-3';
            container.style.zIndex = "1090";
            document.body.appendChild(container);
        }
        return container;
    }

    // Neue Funktion für das Aktualisieren einzelner Keywords
    function refreshSingleKeyword(termId, termName, refreshBtn) {
        // Verhindere mehrfache gleichzeitige Aktualisierungen
        if (refreshBtn.disabled) {
            return;
        }
        
        // Visuelles Feedback - Spinner anzeigen
        const originalHTML = refreshBtn.innerHTML;
        refreshBtn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status"></span>';
        refreshBtn.disabled = true;
        
        // Zeige Mini-Status für diesen Begriff
        const miniStatusDiv = document.createElement('div');
        miniStatusDiv.className = 'small text-muted mt-2';
        miniStatusDiv.id = `refresh-status-${termId}`;
        miniStatusDiv.innerHTML = `<i class="bi bi-hourglass-split"></i> Aktualisiere "${termName}"...`;
        
        // Füge Status nach dem Container hinzu
        const containerParent = keywordChipsContainer.parentElement;
        const existingStatus = document.getElementById(`refresh-status-${termId}`);
        if (existingStatus) {
            existingStatus.remove();
        }
        containerParent.appendChild(miniStatusDiv);
        
        // Starte SSE für Status-Updates
        const refreshStatusSource = new EventSource('/search_status');
        let lastUpdateTime = Date.now();
        
        refreshStatusSource.onmessage = function(event) {
            const message = event.data;
            const currentTime = Date.now();
            
            // Update Status nur alle 500ms für bessere Performance
            if (currentTime - lastUpdateTime > 500 || message === "DONE" || message.startsWith("FEHLER:")) {
                lastUpdateTime = currentTime;
                
                if (message === "DONE" || message.includes('Suche abgeschlossen.')) {
                    miniStatusDiv.innerHTML = `<i class="bi bi-check-circle text-success"></i> "${termName}" erfolgreich aktualisiert!`;
                    refreshStatusSource.close();
                    
                    // Button wiederherstellen
                    refreshBtn.innerHTML = originalHTML;
                    refreshBtn.disabled = false;
                    
                    // Status nach 3 Sekunden entfernen
                    setTimeout(() => {
                        miniStatusDiv.remove();
                    }, 3000);
                    
                    // Suchbegriffe neu laden (falls neue hinzugefügt wurden)
                    loadSearchTerms();
                } else if (message.startsWith("FEHLER:")) {
                    miniStatusDiv.innerHTML = `<i class="bi bi-x-circle text-danger"></i> Fehler: ${message}`;
                    refreshStatusSource.close();
                    
                    // Button wiederherstellen
                    refreshBtn.innerHTML = originalHTML;
                    refreshBtn.disabled = false;
                } else if (message.startsWith("WARNUNG:")) {
                    miniStatusDiv.innerHTML = `<i class="bi bi-exclamation-triangle text-warning"></i> ${message}`;
                } else {
                    // Zeige Fortschritt
                    miniStatusDiv.innerHTML = `<i class="bi bi-arrow-repeat spin"></i> ${message}`;
                }
            }
        };
        
        refreshStatusSource.onerror = function() {
            miniStatusDiv.innerHTML = `<i class="bi bi-x-circle text-danger"></i> Verbindungsfehler`;
            refreshStatusSource.close();
            
            // Button wiederherstellen
            refreshBtn.innerHTML = originalHTML;
            refreshBtn.disabled = false;
        };
        
        // Starte die Suche
        fetch(`/start_search/${encodeURIComponent(termName)}`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        })
        .then(response => {
            if (!response.ok) {
                return response.json().then(err => Promise.reject(err.error || 'Fehler beim Start der Aktualisierung'));
            }
            return response.json();
        })
        .then(data => {
            console.log(`Aktualisierung für "${termName}" gestartet:`, data.message || data.error);
        })
        .catch(error => {
            console.error('Fehler beim Starten der Aktualisierung:', error);
            miniStatusDiv.innerHTML = `<i class="bi bi-x-circle text-danger"></i> Fehler: ${error}`;
            refreshStatusSource.close();
            
            // Button wiederherstellen
            refreshBtn.innerHTML = originalHTML;
            refreshBtn.disabled = false;
            
            // Fehlermeldung nach 5 Sekunden entfernen
            setTimeout(() => {
                miniStatusDiv.remove();
            }, 5000);
        });
    }

    // Neue Funktion zum Laden der API Metriken
    function loadApiMetrics() {
        const warningContainer = document.getElementById('api-metrics-warning');
        const linesContainer = document.getElementById('api-metrics-lines');
        
        if (!warningContainer || !linesContainer) {
            console.warn('API Metriken Container nicht gefunden');
            return;
        }
        
        // Lade API Metriken vom Server
        fetch('/api_metrics')
            .then(response => response.json())
            .then(data => {
                if (data.warning_message && data.warning_message.length > 0) {
                    // Zeige die Warnmeldungen an
                    linesContainer.innerHTML = data.warning_message.map(msg => 
                        `<div>${msg}</div>`
                    ).join('');
                    
                    // Zeige den Container
                    warningContainer.style.display = 'block';
                } else {
                    // Verstecke den Container, wenn keine Daten vorhanden
                    warningContainer.style.display = 'none';
                }
            })
            .catch(error => {
                console.error('Fehler beim Laden der API Metriken:', error);
                // Bei Fehler zeige Standardwarnung
                linesContainer.innerHTML = `
                    <div>Achtung: Es können nur 1000 Abfragen im Monat durchgeführt werden!</div>
                    <div>Ein Durchgang sind 400 Abfragen.</div>
                    <div>API Metriken konnten nicht geladen werden.</div>
                `;
                warningContainer.style.display = 'block';
            });
    }
}); 