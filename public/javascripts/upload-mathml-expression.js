(function($) {
    $(function() {
        $('input[type="file"][name="expression"]')
            .change(function() { $('#upload-form').submit(); });
    });
})(jQuery)
