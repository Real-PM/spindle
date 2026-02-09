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
});
