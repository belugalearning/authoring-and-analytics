/*
    // now translate graph so that it centres on 960 x 500 window
    var sortedByX = _.sortBy(map.nodes, function(n) { return n.x })
      , sortedByY = _.sortBy(map.nodes, function(n) { return n.y })
      , midX = 0.5 * (parseFloat(sortedByX[0].x) + parseFloat(sortedByX[sortedByX.length-1].x))
      , midY = 0.5 * (parseFloat(sortedByY[0].y) + parseFloat(sortedByY[sortedByY.length-1].y))
      , decX = midX - 480
      , decY = midY - 225
    ;
*/

(function($) {
    $(function() {
        var width = 960
          , height = 500
          , svg, focusBorder, gZoom, gWrapper, gPrereqs, gNodes
          , inFocus = null
          , l = 400
        ;

        function zoom() {
            gZoom.attr("transform", "translate(" + d3.event.translate + ")scale(" + d3.event.scale + ")");
        }

        function nodeWithId(id) {
            var len = kcm.nodes.length, i;
            for (i=0; i<len; i++) {
                if (kcm.nodes[i].id == id) return kcm.nodes[i];
            }
            return null;
        }

        function nodeRectWithId(id) {
            var match = $.grep(d3.selectAll('rect.node')[0], function(o) {
                // TODO: handle empty array error
                var data = d3.select(o).data()[0];
                return data.id == id;
            });
            return match.length ? match[0] : null;
        }

        function gainFocus() {
            var e = d3.event
              , focusTarget = $(e.target).closest('[data-focusable="true"]')[0]
              , type = $(this).attr('data-type')
            ;
            if (focusTarget != this || inFocus == this) return;

            svg.setPanEnabled(['svg','wrapper'].indexOf(type) != -1);

            if (type == 'svg') e.stopPropagation();
            e.preventDefault();

            if (type == 'svg') focusBorder.setEmphasised(true);
            else if ($(inFocus).attr('data-type') == 'svg') focusBorder.setEmphasised(false);

            inFocus = this;
        }

        svg = d3.select("body")
            .append("svg")
                .attr("data-type", "svg")
                .attr("width", width)
                .attr("height", height)
                .attr("data-focusable", "true")
                .on("mousedown", gainFocus)
                .call(d3.behavior.zoom()/*.scaleExtent([1, 8])*/.on("zoom", zoom))
        ;

        focusBorder = svg
            .append("rect")
                .attr("fill", "none")
        ;
        gZoom = svg.append("g");
            
        gWrapper = gZoom
            .append("g")
            .attr("data-type", "wrapper")
            .attr("data-focusable", "true")
            .on("mousedown", gainFocus)
        ;

        gPrereqs = gWrapper.append("g");
        gNodes = gWrapper.append("g");

//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! TEMP
        gPrereqs.selectAll('path.line')
            .data([1])
            .enter()
            .append("svg:line")
                .attr("x1", 0)
                .attr("x2", 300)
                .attr("y1", 0)
                .attr("y2", 0)
                .attr("style", "stroke:#000; stroke-width:4px;")
        ;


        gNodes.selectAll("rect.node")
            .data(kcm.nodes)
            .enter()
            .append("rect")
                .attr("class", "node")
                .attr("style", "fill:#888;")
                .attr("x", function(d) { return d.x; })
                .attr("y", function(d) { return d.y; })
                .attr("width", function(d) { return d.width; })
                .attr("height", function(d) { return d.height; })
                .attr("data-focusable", "true")
                .on("mousedown", gainFocus)
        ;

        d3.select(window)
            .on("keyup", function() {
                console.log("arguments:", arguments, "e:", d3.event);
            })
        ;

        focusBorder.setEmphasised = function(value) {
            value = value == true;
            focusBorder
                .attr("x", value ? 1 : 0)
                .attr("y", value ? 1 : 0)
                .attr("width", width - (value ? 2 : 1))
                .attr("height", height - (value ? 2 : 1))
                .attr("stroke-width", value ? 2 : 1)
                .attr("stroke", value ? "#999" : "#ccc")
            ;
        };
        focusBorder.setEmphasised(false);
        
        /*
        svg.append("svg:clipPath")
            .attr("id", "clip")
            .append("svg:rect")
            .attr("x", 1)
            .attr("y", 1)
            .attr("width", width-2)
            .attr("height", height -2)
            */
console.log(nodeRectWithId(517));
    });
})(jQuery);
