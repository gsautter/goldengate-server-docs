var theGoogleMapBasePath = "";
var MapWindow;

function setGoogleMapBasePath(googleMapBasePath) {
  theGoogleMapBasePath = googleMapBasePath;
}

function openMap(relativePath) {
  
  //	create map window if it does not exist or was closed
  if ((MapWindow == null) || MapWindow.closed)
    MapWindow = window.open((theGoogleMapBasePath + "/GMapWindow.html"), "GoogleMap", ("width=550,height=350,top=100,left=100,resizable=yes,scollbars=yes"));
}

function closeMap() {
  
  //	close map window if still open
  if ((MapWindow != null) && !MapWindow.closed)
    MapWindow.close();
}

//	storage for data to display, needs to be here because setTimeout() cannot handle parameters
var theLongArray = null;
var theLatArray = null;
var theNameArray = null;

function displayLocations() {
	
	//	check if data present
	if ((theLongArray == null) || (theLatArray == null) || (theNameArray == null)) return;
	
	//	make sure map window exists
  openMap();
  
 	//	bring map window to front and show data only if initialized completely
	if (MapWindow.initialized) {
  	MapWindow.addMarkersToMap(theLongArray, theLatArray, theNameArray);
		MapWindow.focus();
		
	//	trigger retry in 50ms otherwise
	} else window.setTimeout('displayLocations();', 50);
}

function showLocations(longArray, latArray, nameArray) {
  theLongArray = longArray;
  theLatArray = latArray;
  theNameArray = nameArray;
  displayLocations();
}

function showLocation(long, lat, name) {
	theLongArray = new Array(1);
	theLongArray[0] = long;
	theLatArray = new Array(1);
	theLatArray[0] = lat;
	theNameArray = new Array(1);
	theNameArray[0] = name;
  displayLocations();
}