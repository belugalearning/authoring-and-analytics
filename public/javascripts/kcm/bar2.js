(function($) {
    $(function() {
        var t = 1297110663 // start time (seconds since epoch)
          , v = 70 // start value (subscribers)
          , data = d3.range(33).map(next) // starting dataset
          , w = 20, h = 80
          , x, y, chart
        ;

        function next() {
            return {
                time: ++t,
                value: v = ~~Math.max(10, Math.min(90, v + 10 * (Math.random() - .5)))
            };
        }

        function redraw() {
            var slideDuration = 1000
              , rect = chart.selectAll("rect")
                    .data(data, function(d) { return d.time; })
            ;

            rect.enter()
                .insert("rect", "line")
                    .attr("x", function(d, i) { return x(i+1) - .5; })
                    .attr("y", function(d) { return h - y(d.value) - .5; })
                    .attr("width", w)
                    .attr("height", function(d) { return y(d.value); })
                    .transition()
                        .duration(slideDuration)
                        .attr('x', function(d, i) { return x(i) - .5; })
            ;

            rect.transition()
                .duration(slideDuration)
                .attr("x", function(d, i) { return x(i) - .5; });

            rect.exit()
                .transition()
                    .duration(slideDuration)
                    .attr('x', function(d, i) { return x(i-1) - .5; })
                .remove()
            ;
        }

        setInterval(function() {
            data.shift();
            data.push(next());
            redraw();
        }, 1500);

        x = d3.scale
            .linear()
                .domain([0, 1])
                .range([0, w])
        ;

        y = d3.scale
            .linear()
                .domain([0, 100])
                .rangeRound([0, h])
        ;

        chart = d3
            .select("body")
                .append("svg")
                    .attr("class", "chart")
                    .attr("width", w * data.length - 1)
                    .attr("height", h)
        ;

        chart
            .selectAll("rect")
                .data(data)
                .enter()
                .append("rect")
                    .attr("x", function(d, i) { 
                        console.log('here');
                        return x(i) - .5; 
                    })
                    .attr("y", function(d) { return h - y(d.value) - .5; })
                    .attr("width", w)
                    .attr("height", function(d) { return y(d.value); })
        ;

        chart
            .append("line")
                .attr("x1", 0)
                .attr("x2", w * data.length)
                .attr("y1", h - .5)
                .attr("y2", h - .5)
                .style("stroke", "#000")
        ;
    });
})(jQuery);
