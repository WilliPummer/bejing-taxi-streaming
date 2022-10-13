/* eslint-disable no-undef */

options_red = {
    iconShape: 'circle-dot',
    borderWidth: 5,
    borderColor: 'red'
};

options = {
    iconShape: 'circle-dot',
    borderWidth: 5,
    borderColor: 'green'
};

// config map
let config = {
    minZoom: 7,
    maxZoom: 18,
};
// magnification with which the map will start
const zoom = 11;

// base co-ordinates
const lat = 39.906761504979414;
const lng = 116.4029520497194;


// calling ma
const map = L.map("map", config).setView([lat, lng], zoom);

// Used to load and display tile layers on the map
// Most tile servers require attribution, which you can set under `Layer`
L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
}).addTo(map);

let dict = {};
let source = new EventSource('/stream');

source.onmessage = function (e) {
    if (e.data === "EMPTY_MSG") {
        console.log("EMPTY_MSG")
        return
    }

    let data = JSON.parse(e.data);
    console.log(e.data)
    if (data.id in dict) {
        let [marker, date] = dict[data.id]
        marker.setLatLng(L.latLng(data.lng, data.lat))
        marker.setPopupContent("Taxi: " + data.id + " last update: " + data.date)
        dict[data.id] = [marker, new Date()]
    } else {
        let newMarker = L.marker([data.lng, data.lat], {
            icon: L.BeautifyIcon.icon(options),
            draggable: false
        })
        newMarker.addTo(map).bindPopup("Taxi: " + data.id + " last update: " + data.date);
        dict[data.id] = [newMarker, new Date()]
    }

    source.onerror = function (e) {
        source.close()
    }

    function updateColor() {
        let cur = new Date()
        for (const [key, value] of Object.entries(dict)) {
            let [marker, date] = value
            const diffTime = Math.abs(cur - date);
            if (diffTime > 60000) {
                marker.setIcon(L.BeautifyIcon.icon(options_red))
            }
        }
        setTimeout(updateColor, 15000);

    }

    setTimeout(updateColor, 15000);

    window.onclose = function () {
        source.close()
    }

}
