/* eslint-disable no-undef */

options = {
  iconShape: 'circle-dot',
  borderWidth: 5,
  borderColor: 'red'
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

  source.onmessage = function(e) {
    let data = JSON.parse(e.data);
    console.log(e.data)
    if(data.id in dict){
        let marker = dict[data.id]
        marker.setLatLng(L.latLng(data.lng, data.lat))
    } else {
        let newMarker = L.marker([data.lng, data.lat], {
          icon: L.BeautifyIcon.icon(options),
          draggable: false
        })
        newMarker.addTo(map);
        dict[data.id] = newMarker
    }


  }
