(function($) {
    $(function() {
        var margin = {top: 0, right: 0, bottom: 12, left: 24}
          , width = 960 - margin.left - margin.right
          , height = 500 - margin.top - margin.bottom
        ;

        var x = d3.scale.linear()
            .domain([-width / 2, width / 2])
            .range([0, width])
        ;

        var y = d3.scale.linear()
            .domain([-height / 2, height / 2])
            .range([height, 0])
        ;

        var svg = d3.select("body")
            .append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
                .append("g")
                    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
                    .call(d3.behavior.zoom()/*.scaleExtent([1, 8])*/.on("zoom", zoom))
                    .append("g")
        ;

        svg.append("rect")
            .attr("class", "grid")
            .attr("width", width)
            .attr("height", height)
        ;

        svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(d3.svg.axis().scale(x).tickSize(-height).orient("bottom"))
        ;

        svg.append("g")
            .attr("class", "y axis")
            .call(d3.svg.axis().scale(y).ticks(5).tickSize(-width).orient("left"))
        ;

        function zoom() {
            svg.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
        }
    });
})(jQuery);
