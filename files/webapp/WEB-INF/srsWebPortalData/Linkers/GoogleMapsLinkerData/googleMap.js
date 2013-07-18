window.onunload=GUnload;

var map = null;
var clusterer = null;

var initialized = false;

function init() {
  if (GBrowserIsCompatible()) {
    map = new GMap2(document.getElementById("theMap"));
    map.addControl(new GLargeMapControl());
    map.addControl(new GMapTypeControl());
    clusterer = new Clusterer(map);
    initialized = true;
  }
}
  
function addMarkersToMap(longArray, latArray, nameArray) {
  
  if (map != null) {
    clusterer.ClearCluster(clusterer);
    
 		var bounds = new GLatLngBounds();
 				
    var point;
    var marker;
		
    var htmlString = null;
    var thisName;
    
    for (var loop = 0; loop < latArray.length; loop++) {
      point = new GLatLng(latArray[loop], longArray[loop]);
      bounds.extend(point);
      thisName = nameArray[loop];
			
      if (thisName != "") htmlString = thisName;
      
      clusterer.AddMarker(new GMarker(point, {title:htmlString}), "Zoom in to see these ");
    }
    
    var zoom = map.getBoundsZoomLevel(bounds);
    if (zoom > 5) zoom = 5;
   	
    map.setCenter(bounds.getCenter(), zoom, G_HYBRID_MAP);
  }
}
