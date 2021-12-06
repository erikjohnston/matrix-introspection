// parse the query string
const params = {};
function decode(s) {
    return decodeURIComponent(s.replace("+", " "));
}
for (const param of window.location.search.substring(1).split("&")) {
    if(param == '') continue;
    const s = param.split('=', 2);
    params[decode(s[0])] = decode(s[1] || "");
}
const room_id = params['room_id'] || '!cURbafjkfsMDVwdRDQ:matrix.org';
let max_stream = params['max_stream'];
if (max_stream === undefined) { max_stream = ""; }
$('#room_id_input').val(room_id);
$('#max_stream_input').val(max_stream);

// assign the given level to the node with the given event id, and walk the
// tree down the edges to ensure that each parent has a higher level.
function assignLevel(level, eventId, nodes, event_map) {
    nodes.update({id: eventId, level: level});

    for (const edge of event_map[eventId].edges) {
        const parentNode = nodes.get(edge);
        if (!parentNode) {
            continue;
        }
        if (parentNode.level > level) {
            // already has a higher level than us
            continue;
        }
        assignLevel(level+1, edge, nodes, event_map);
    }
}

let q = `/room/${room_id}`;
if (max_stream != "") {
    q += `?max_stream=${max_stream}`;
}
$.get( q, function( data ) {
    var event_map = {}

    var nodes = [];
    var edges = [];
    for (var i = 0; i < data.length; i++) {
        var row = data[i];

        event_map[row.event_id] = row;

        nodes.push({
            id: row.event_id,
            label: row.event_id + "\ntype: " + row.etype + "\nstate_key: " + row.state_key,
            group: row.state_group,
        });

        for (var j = 0; j < row.edges.length; j++) {
            edges.push({
                from: row.event_id,
                to: row.edges[j],
                arrows: 'to',
            });
        }
    }

    var nodes = new vis.DataSet(nodes);

    // assign levels to each node, based on their children
    nodes.forEach(function(node) {
        if (node.level === undefined) {
            assignLevel(0, node.id, nodes, event_map);
        }
    });

    // create a network
    var container = document.getElementById('mynetwork');
    var data = {
        nodes: nodes,
        edges: edges
    };
    var options = {
        layout: {
            hierarchical: {
                sortMethod: "directed",
                nodeSpacing: 200,
                direction: "DU",
            },
        },
        interaction: { dragNodes: true },
        physics: {
            enabled: false,
        },
    };

    var network = new vis.Network(container, data, options);

    network.on("click", function (params) {
        if (params.nodes.length == 0) {
            return;
        }

        var event_id = params.nodes[0];
        showEventInfo(event_id);
    });

    $("#event_id_form").on("submit", function(e) {
        e.preventDefault();
        var event_id=$("#event_id_input").val();
        const ev = event_map[event_id];
        if (ev) {
            network.focus(event_id);
            network.selectNodes([event_id]);
        }
        showEventInfo(event_id);
    });

    function showEventInfo(event_id) {
        const ev = event_map[event_id];
        if (ev) {
            $( "#info" ).html(
                "<pre>" + JSON.stringify(ev, null, 4) + "</pre>"
                    + "<p>" + new Date(ev.ts) + "</p>"
            );
        } else {
            $( "#info" ).html("<p>Not found in this batch</p>");
        }

        $( "#state" ).html( "loading..." );

        $.get( "/state/" + event_id, function( data ) {
            $( "#state" ).html( "<pre>" + JSON.stringify(data, null, 4) + "</pre>" );
        }, "json");
    }
}, "json");
