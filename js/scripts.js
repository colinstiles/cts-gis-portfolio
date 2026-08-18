document.addEventListener('DOMContentLoaded', function() {
    // Add event listener for the "View Gallery" button
    document.querySelector('.btn-primary').addEventListener('click', function(event) {
        event.preventDefault();
        window.location.href = 'map-gallery.html';
    });
});
