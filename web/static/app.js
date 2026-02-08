// Initialize Tom Select on multi-select dropdowns
document.addEventListener("DOMContentLoaded", function () {
    var config = {
        plugins: ["remove_button"],
        maxOptions: null,
    };

    new TomSelect("#genres", config);
    new TomSelect("#artists", config);
    new TomSelect("#similar_to", {maxOptions: null});
});
