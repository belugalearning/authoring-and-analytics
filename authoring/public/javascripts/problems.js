(function($) {
    $(function() {
        $('#wrapper')
            .on('change', '#select-tool', function(e) {
                var toolId = $('#select-tool').val();
                $('table#problems-summary tr.problem').each(function() {
                    $(this).css('display', (toolId == '0' || toolId == $(this).attr('data-tool') ? 'table-row' : 'none'));
                });
            })
            .on('click', '.problem-description', function(e) {
                self.location = '/content/problem/' + $(this).closest('tr').attr('data-id');
            });
        ;
    });
})(jQuery);
