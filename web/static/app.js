// Initialize Tom Select on multi-select dropdowns
document.addEventListener("DOMContentLoaded", function () {
    var config = {
        plugins: ["remove_button"],
        maxOptions: null,
    };

    // Genre groups dropdown (only if element exists)
    var genreGroupsEl = document.getElementById("genre_groups");
    if (genreGroupsEl) {
        new TomSelect("#genre_groups", config);
    }

    new TomSelect("#genres", config);
    new TomSelect("#artists", config);
    new TomSelect("#similar_to", {maxOptions: null});

    initAddTrackSearch();
});

// --- Track management ---

function escapeHtml(text) {
    var div = document.createElement("div");
    div.textContent = text;
    return div.innerHTML;
}

var addTrackSelect = null;

function initAddTrackSearch() {
    addTrackSelect = new TomSelect("#add-track-select", {
        valueField: "plex_id",
        labelField: "title",
        searchField: ["title", "artist"],
        maxOptions: 15,
        load: function (query, callback) {
            if (query.length < 2) return callback();
            fetch("/api/track-search?q=" + encodeURIComponent(query))
                .then(function (res) { return res.json(); })
                .then(function (data) { callback(data); })
                .catch(function () { callback(); });
        },
        render: {
            option: function (item) {
                var bpmText = item.bpm ? " [" + item.bpm + " BPM]" : "";
                return '<div><span class="title">' + escapeHtml(item.title) +
                       '</span> — <small>' + escapeHtml(item.artist) +
                       escapeHtml(bpmText) + '</small></div>';
            },
            item: function (item) {
                return '<div>' + escapeHtml(item.title) + ' — ' + escapeHtml(item.artist) + '</div>';
            },
        },
        onChange: function (value) {
            if (!value) return;
            var item = this.options[value];
            if (item) {
                addTrackToTable(item);
            }
            this.clear(true);
            this.clearOptions();
        },
    });
}

function addTrackToTable(track) {
    var tbody = document.getElementById("playlist-body");
    if (!tbody) return;

    // Check for duplicate
    var existing = tbody.querySelector('tr[data-plex-id="' + track.plex_id + '"]');
    if (existing) {
        existing.classList.remove("highlight-row");
        // Force reflow so animation restarts
        void existing.offsetWidth;
        existing.classList.add("highlight-row");
        return;
    }

    var tr = document.createElement("tr");
    tr.setAttribute("data-plex-id", track.plex_id);
    tr.innerHTML =
        '<td class="drag-handle">&#9776;</td>' +
        "<td>" + escapeHtml(track.title) + "</td>" +
        "<td>" + escapeHtml(track.artist) + "</td>" +
        "<td>" + escapeHtml(track.album) + "</td>" +
        "<td>" + escapeHtml(String(track.bpm || "")) + "</td>" +
        "<td>" + escapeHtml(track.genres || "") + "</td>" +
        '<td><button type="button" class="remove-track-btn" onclick="removeTrack(this)">&times;</button></td>';
    tbody.appendChild(tr);
    updateTrackCount();
}

function removeTrack(btn) {
    var tr = btn.closest("tr");
    if (tr) {
        tr.remove();
        updateTrackCount();
    }
}

function updateTrackCount() {
    var tbody = document.getElementById("playlist-body");
    var display = document.getElementById("track-count-display");
    if (tbody && display) {
        var count = tbody.querySelectorAll("tr").length;
        display.textContent = count;
    }
}

// Initialize SortableJS and show add-track section after preview loads
document.addEventListener("htmx:afterSwap", function (evt) {
    if (evt.detail.target.id === "preview-table") {
        var tbody = document.getElementById("playlist-body");
        if (tbody) {
            new Sortable(tbody, {
                handle: ".drag-handle",
                ghostClass: "sortable-ghost",
                animation: 150,
            });
        }
        var addSection = document.getElementById("add-track-section");
        if (addSection) {
            addSection.style.display = "block";
        }
    }
});

// --- Similar tracks ---

function findSimilarTracks() {
    var tbody = document.getElementById("playlist-body");
    if (!tbody || tbody.querySelectorAll("tr").length === 0) {
        alert("No tracks in preview. Use Preview first.");
        return;
    }

    var rows = tbody.querySelectorAll("tr[data-plex-id]");
    var plex_ids = [];
    rows.forEach(function (row) {
        plex_ids.push(parseInt(row.getAttribute("data-plex-id"), 10));
    });

    var spinner = document.getElementById("similar-spinner");
    if (spinner) spinner.style.display = "inline-block";

    htmx.ajax("POST", "/api/similar-tracks", {
        target: "#similar-tracks",
        values: {
            track_plex_ids: JSON.stringify(plex_ids),
        },
    }).then(function () {
        if (spinner) spinner.style.display = "none";
    });
}

function addFromSimilar(btn) {
    var tr = btn.closest("tr");
    if (!tr) return;

    var track = {
        plex_id: tr.getAttribute("data-plex-id"),
        title: tr.getAttribute("data-title"),
        artist: tr.getAttribute("data-artist"),
        album: tr.getAttribute("data-album"),
        bpm: "",
        genres: "",
    };

    addTrackToTable(track);
    tr.classList.add("added-row");
}

// Submit playlist with explicit plex_ids from DOM order
function submitPlaylist() {
    var tbody = document.getElementById("playlist-body");
    var nameInput = document.getElementById("playlist_name");
    var name = nameInput ? nameInput.value.trim() : "";

    if (!name) {
        alert("Please enter a playlist name.");
        return;
    }

    if (!tbody || tbody.querySelectorAll("tr").length === 0) {
        alert("No tracks to create a playlist from. Use Preview first.");
        return;
    }

    if (!confirm("Create this playlist on Plex?")) {
        return;
    }

    // Collect plex_ids in DOM order
    var rows = tbody.querySelectorAll("tr[data-plex-id]");
    var plex_ids = [];
    rows.forEach(function (row) {
        plex_ids.push(parseInt(row.getAttribute("data-plex-id"), 10));
    });

    // Set hidden input
    var hidden = document.getElementById("track-plex-ids");
    if (hidden) {
        hidden.value = JSON.stringify(plex_ids);
    }

    // Get replace_existing checkbox value
    var replaceCheckbox = document.querySelector('input[name="replace_existing"]');
    var replaceVal = replaceCheckbox && replaceCheckbox.checked ? "on" : "";

    // Show spinner
    var spinner = document.getElementById("create-spinner");
    if (spinner) spinner.style.display = "inline-block";

    // Submit via htmx.ajax
    htmx.ajax("POST", "/api/create-playlist", {
        target: "#create-result",
        values: {
            playlist_name: name,
            replace_existing: replaceVal,
            track_plex_ids: JSON.stringify(plex_ids),
        },
    }).then(function () {
        if (spinner) spinner.style.display = "none";
    });
}
