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
        var width
          , height
          , svg, focusBorder, gZoom, gWrapper, gPrereqs, gNodes
          , inFocus = null
          , l = 400
        ;

    $(function() {
        width = $(window).width() - 20;
        height = $(window).height() - 20;
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

    var nodeDragStart = nodeDragEnd = function() {
        var e = d3.event.sourceEvent
          , pos = d3.transform(d3.select(this).attr("transform")).translate
        ;
        pos[2] = e.pageX;
        pos[3] = e.pageY;

        d3.select(this)
            .attr('data-drag-vals', pos.join(","))
    }

    function nodeDrag() {
        var e = d3.event.sourceEvent
          , zoomMultiplier = 1 / d3.transform(gZoom.attr('transform')).scale[0]
          , pos = d3.select(this).attr("data-drag-vals").split(",")
          , dx = zoomMultiplier * (e.pageX - pos[2])
          , dy = zoomMultiplier * (e.pageY - pos[3])
          , x = parseInt(pos[0]) + dx
          , y = parseInt(pos[1]) + dy
          , id = $(this).attr('id')
        ;

        d3.select(this).attr("transform", "translate("+x+","+y+")");

        $('g.link[data-head-node='+id+'], g.link[data-tail-node='+id+']').arrowRedraw();
    }

    $.fn.arrowRedraw = function() {
        $.each(this, function() {
            var gArrow = d3.select(this)
              , gHead = d3.select( $('g#' + $(this).attr('data-head-node'))[0] )
              , gTail = d3.select( $('g#' + $(this).attr('data-tail-node'))[0] )
              , head = gHead.select('rect')
              , tail = gTail.select('rect')
              , hw = parseInt(head.attr("width"))
              , hh = parseInt(head.attr("height"))
              , hx = d3.transform(gHead.attr("transform")).translate[0] + 0.5 * hw
              , hy = d3.transform(gHead.attr("transform")).translate[1] + 0.5 * hh
              , tx = d3.transform(gTail.attr("transform")).translate[0] + 0.5 * parseInt(tail.attr("width"))
              , ty = d3.transform(gTail.attr("transform")).translate[1] + 0.5 * parseInt(tail.attr("height"))
              , len = Math.sqrt(Math.pow(tx-hx,2) + Math.pow(ty-hy,2))
              , grad = (ty-hy) / (tx-hx)
              , theta = hx == tx
                    ? (hy > ty ? 1.5 : 0.5) * Math.PI
                    : Math.atan(grad) + (hx > tx ? Math.PI : 0)
              , ahr // arrow-head radius polar coord
            ;

            if (Math.abs(grad) < Math.abs(hh/hw)) {
                // arrow-head against left or right side of head rect
                var adj = 0.5 * hw * (hx < tx ? 1 : -1);
                ahr = adj / Math.cos(theta);
            } else {
                // arrow-head against top or bottom of head rect
                var opp = 0.5 * hh * (hy < ty ? 1 : -1);
                ahr = opp / Math.sin(theta);
            }

            gArrow
                .attr("transform", "translate("+hx+","+hy+")rotate("+(theta*180/Math.PI)+")")
                .select(".arrow-stem")
                    .attr("transform", "scale("+len+",1)")
            ;
            gArrow
                .select(".arrow-head")
                    .attr("transform", "translate("+ahr+",0)")
            ;
        });
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

        var nodes = gNodes.selectAll('g.node')
            .data(kcm.nodes)
            .enter()
            .append('g')
                .attr('id', function(d) { return d.id; })
                .attr('class', 'node')
                .attr("transform", function(d) { return "translate("+ d.x +","+ d.y +")"; })
                .attr("data-focusable", "true")
                .on("mousedown", gainFocus)
                .call(d3.behavior.drag()
                      .on("dragstart", nodeDragStart)
                      .on("drag", nodeDrag)
                      .on("dragend", nodeDragEnd)
                )
        ;
        nodes
            .append('rect')
                .attr("style", "fill:steelblue;")
                .attr("width", function(d) { return d.width; })
                .attr("height", function(d) { return d.height; })
        ;
        var nodesText = nodes
            .append('text')
                .attr('class', 'node-description')
                .attr('transform', 'translate(5,5)')
        ;
        var nodeTSpans = nodesText.selectAll('tspan')
            .data(function(d) { return d.nodeDescription.split('\n');})
            .enter()
            .append('tspan')
                .attr('x', 0)
                .attr('y', function(d, i) { console.log(d,i); return 14 * (1+i); })
                .text(String)
        ;
        
        gPrereqs.selectAll('g.link')
            .data(kcm.prerequisites)
            .enter()
            .append('g')
                .attr('id', function(d) { return d.id; })
                .attr('class', 'link')
                .attr("data-focusable", "true")
                .attr("data-head-node", function(d) { return d[1]; })
                .attr("data-tail-node", function(d) { return d[0]; })                
        ;
        gPrereqs.selectAll('g.link').selectAll('rect.arrow-stem')
            .data(function(d) { return [d]; })
            .enter()
            .append("rect")
                .attr("class", "arrow-stem")
                .attr("x", 0)
                .attr("y", -0.5)
                .attr("width", 1)
                .attr("height", 1)
                .attr("style", "fill:#000;")
        ;
        gPrereqs.selectAll('g.link').selectAll('path.arrow-head')
            .data(function(d) { return [d]; })
            .enter()
            .append('path') 
                .attr('d', 'M 0 -0.5 l 0 1 l 16 7.5 l 0 -16 z')
                .attr("class", "arrow-head")
                .attr('style', 'fill:#222; stroke-width:0;')
        ;
        $('g.link').arrowRedraw();

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
        
    });
})(jQuery);
