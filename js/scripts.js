/*!
    Title: GIS Portfolio
    Version: 1.4
    Last Change: 08/18/2026
    Author: Colin T. Stiles
    Repo: https://github.com/colinstiles/cts-gis-portfolio
*/

(function(window, document, $) {
    if (!$) {
        return;
    }

    // Show current year
    $('#current-year').text(new Date().getFullYear());

    // Remove no-js class
    $('html').removeClass('no-js');

    // Animate to section when nav is clicked
    $('header a[href^="#"]').on('click', function(e) {
        if ($(this).hasClass('no-scroll')) {
            return;
        }

        var heading = $(this).attr('href');
        var $target = $(heading);

        if (!$target.length) {
            return;
        }

        e.preventDefault();

        $('html, body').animate({
            scrollTop: $target.offset().top + 'px'
        }, Math.abs(window.pageYOffset - $target.offset().top));

        if ($('header').hasClass('active')) {
            $('header, body').removeClass('active');
        }
    });

    // Scroll to top
    $('#to-top').on('click', function() {
        $('html, body').animate({
            scrollTop: 0
        }, 500);
    });

    // Scroll to first element
    $('#lead-down span').on('click', function() {
        var $nextSection = $('#lead').next();

        if (!$nextSection.length) {
            return;
        }

        $('html, body').animate({
            scrollTop: $nextSection.offset().top + 'px'
        }, 500);
    });

    // Create timeline
    $('#experience-timeline').each(function() {
        var $timeline = $(this);
        var $userContent = $timeline.children('div');

        $userContent.each(function() {
            $(this)
                .addClass('vtimeline-content')
                .wrap('<div class="vtimeline-point"><div class="vtimeline-block"></div></div>');
        });

        $timeline.find('.vtimeline-point').each(function() {
            $(this).prepend('<div class="vtimeline-icon"><i class="fa fa-map-marker"></i></div>');
        });

        $timeline.find('.vtimeline-content').each(function() {
            var date = $(this).data('date');

            if (date) {
                $(this).parent().prepend('<span class="vtimeline-date">' + date + '</span>');
            }
        });
    });

    // Open mobile menu
    $('#mobile-menu-open').on('click', function() {
        $('header, body').addClass('active');
    });

    // Close mobile menu
    $('#mobile-menu-close').on('click', function() {
        $('header, body').removeClass('active');
    });

    // Load additional projects
    $('#view-more-projects').on('click', function(e) {
        e.preventDefault();
        $(this).fadeOut(300, function() {
            $('#more-projects').fadeIn(300);
        });
    });

    // Stop scrolling animation on user interaction
    $('html, body').on('mousewheel DOMMouseScroll', function() {
        $('html, body').stop();
    });
})(window, document, window.jQuery);
