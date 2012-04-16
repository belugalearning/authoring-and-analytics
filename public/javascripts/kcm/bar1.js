(function($) {
    $(function() {
        var data = [4, 8, 15, 16, 23, 42]
          , w = 440
          , h = 140
        ;

        var x = d3.scale.linear()
            .domain([0, d3.max(data)])
            .range([0, w]);

        var y = d3.scale.ordinal()
            .domain(data)
            .rangeBands([0, h]);

        var svg = d3.select('body').append('svg')
                .attr('class', 'chart')
                .attr('width', w + 10)
                .attr('height', h + 15)
        ;

        var chart = svg
            .append('g')
                .attr("transform", "translate(10,15)");

        // mark intervals
        chart.selectAll("line")
                .data(x.ticks(10))
            .enter().append("line")
                .attr("x1", x)
                .attr("x2", x)
                .attr("y1", 0)
                .attr("y2", h)
                .style("stroke", "#ccc");
        
        // label intervals
        chart.selectAll("text.rule")
                .data(x.ticks(10))
            .enter().append("text")
                .attr("class", "rule")
                .attr("x", x)
                .attr("y", 0)
                .attr("dy", -3)
                .attr('editable', 'true')
                .attr("text-anchor", "middle")
                .text(String);

        // bars
        chart.selectAll('rect')
                .data(data)
            .enter().append('rect')
                .attr('width', x)
                .attr('y', y)
                .attr('height', y.rangeBand());

        // label bars
        chart.selectAll('text.bar-label')
                .data(data)
            .enter().append('text')
                .attr('class', 'bar-label') 
                .attr('x', x)
                .attr('y', function(d) { return y(d) + y.rangeBand() / 2; })
                .attr('dx', -3) // padding-right
                .attr('dy', '.35em') // vertical-align: middle
                .attr('text-anchor', 'end') // text-align: right
                .text(String);
                
        // mark x=0
        chart.append("line")
                .attr("y1", 0)
                .attr("y2", h)
                .style("stroke", "#000")
        ;

        svg
                .append("foreignObject")
                    .attr("width", 400)
                    .attr("height", 200)
                    .append("xhtml:body")
                        .style("font", "14px 'Helvetica Neue'")
                        .html("<textarea>An HTML Foreign Object in SVG</textarea>")
    ;
/*
*/

    });
})(jQuery);
