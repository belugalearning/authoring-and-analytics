(function($) {
    $(function() {
        $('input[type="file"][name="plist"]')
            .change(function() { $('#upload-form').submit(); });
    });
})(jQuery)
