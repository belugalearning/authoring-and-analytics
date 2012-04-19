//(function($) {
    var svg
      , gNodes, gPrereqs, gHead, gTail, gArrow
      , head, tail, arrow
    ;
    
    function positionArrow() {
        var hw = parseInt(head.attr("width"))
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
          , ahr // arrow-head radius (as in polar coords)
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
          , pos = d3.select(this).attr("data-drag-vals").split(",")
          , dx = e.pageX - pos[2]
          , dy = e.pageY - pos[3]
          , x = parseInt(pos[0]) + dx
          , y = parseInt(pos[1]) + dy
        ;
        d3.select(this).attr("transform", "translate("+x+","+y+")");
        positionArrow();
    }

    $(function() {
        svg = d3.select("body")
            .append("svg")
                .attr("data-type", "svg")
                .attr("width", 1000)
                .attr("height", 600)
        ;
        
        gPrereqs = svg.append("g");
        gNodes = svg.append("g");

        gHead = gNodes
            .append("g")
            .attr("transform", "translate(200,200)")
            .call(d3.behavior.drag()
                  .on("dragstart", nodeDragStart)
                  .on("drag", nodeDrag)
                  .on("dragend", nodeDragEnd)
            )
        ;
        head = gHead
            .append("rect")
                .attr("style", "fill:#888;")
                .attr("width", 150)
                .attr("height", 50)
        ;

        gTail = gNodes
            .append("g")
            .attr("transform", "translate(350,135)")
        ;
        tail = gTail
            .append("rect")
                .attr("style", "fill:#888;")
                .attr("width", 30)
                .attr("height", 80)
        ;

        gArrow = gPrereqs.append("g");
        gArrow
            .append("rect")
                .attr("class", "arrow-stem")
                .attr("x", 0)
                .attr("y", 0)
                .attr("width", 1)
                .attr("height", 1)
                .attr("style", "fill:#000;")
        ;
        gArrow
            .append('path') 
                .attr('d', 'M 0 0 l 16 8 l 0 -16 z')
                .attr("class", "arrow-head")
                .attr('style', 'fill:#222; stroke-width:0;')
        ;

        positionArrow();
    });
//})(jQuery);



/*
            var gHead = $('g#' + $(this).attr('data-head-node'))
              , gTail = $('g#' + $(this).attr('data-tail-node'))
              , head = gHead.children('rect')
              , tail = gTail.children('rect')
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

            var test = d3.select(this).selectAll('path');
            console.log(test);

            $(this)
                .attr("transform", "translate("+hx+","+hy+")rotate("+(theta*180/Math.PI)+")")
                .children(".arrow-stem")
                    .attr("transform", "scale("+len+",1)")
            ;
            $(this)
                .children("path")
                    .attr("transform", "translate("+ahr+",0)")
            ;
            */
