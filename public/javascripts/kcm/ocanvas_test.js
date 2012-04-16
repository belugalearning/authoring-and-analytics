(function($) {
    $(function() {
        var canvas = oCanvas.create({ canvas: '#canvas' });
        var dragOptions = { changeZindex: true };

        var button = canvas.display.rectangle({
            x: canvas.width / 2,
            y: canvas.width / 5,
            origin: { x: 'center', y: 'center' },
            width: 300,
            height: 40,
            fill: '#079',
            stroke: '10px #079',
            join: 'round'
        });
        var buttonText = canvas.display.text({
            x: 0,
            y: 0,
            origin: { x: 'center', y: 'center' },
            align: 'center',
            font: 'bold 25px sans-serif',
            text: 'Toggle Rotation',
            fill: '#fff'
        });
        button.addChild(buttonText);
        canvas.addChild(button);

        button.dragAndDrop(dragOptions);
    });
}
