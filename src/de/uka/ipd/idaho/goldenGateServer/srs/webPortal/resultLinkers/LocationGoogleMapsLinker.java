/*
 * Copyright (c) 2006-, IPD Boehm, Universitaet Karlsruhe (TH) / KIT, by Guido Sautter
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Universität Karlsruhe (TH) nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY UNIVERSITÄT KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal.resultLinkers;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.util.constants.LocationConstants;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalConstants.SearchResultLinker;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * This search result linker produces hyperlinks for plotting locations in
 * search results in GoolgeMaps.
 * 
 * @author sautter
 */
public class LocationGoogleMapsLinker extends SearchResultLinker implements LocationConstants {
	
	/*	TODO build custom version of this, specialized to materials citations, distinguishing:
	 * - coordinates from document vs. geo-referenced
	 * - type status
	 * - specimen count
	*/
	
	private static final String[] unloadCalls = {
		"closeGoogleMap();"
	};
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchResultLinker#getName()
	 */
	public String getName() {
		return "GoogleMaps Linker";
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchResultLinker#getUnloadCalls()
	 */
	public String[] getUnloadCalls() {
		return unloadCalls;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalConstants.SearchResultLinker#writePageHeadExtensions(de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder)
	 */
	public void writePageHeadExtensions(HtmlPageBuilder hpb) throws IOException {
		hpb.writeLine("<script type=\"text/javascript\">");
		hpb.writeLine("var googleMapBasePath = '" + hpb.request.getContextPath() + this.parent.getRelativePath(this.dataPath) + "';");
		hpb.writeLine("var googleMapWindow;");
		
		hpb.writeLine("function openGoogleMap() {");
		//	create map window if it does not exist or was closed
		hpb.writeLine("  if ((googleMapWindow == null) || googleMapWindow.closed)");
		hpb.writeLine("    googleMapWindow = window.open((googleMapBasePath + '/GMapWindow.html'), 'GoogleMap', ('width=550,height=350,top=100,left=100,resizable=yes,scrollbar=yes,scrollbars=yes'));");
		hpb.writeLine("}");
		hpb.writeLine("function closeGoogleMap() {");
		//	close map window if still open
		hpb.writeLine("  if ((googleMapWindow != null) && !googleMapWindow.closed)");
		hpb.writeLine("    googleMapWindow.close();");
		hpb.writeLine("}");

		//	storage for data to display, needs to be here because setTimeout() cannot handle parameters
		hpb.writeLine("var theLongArray = null;");
		hpb.writeLine("var theLatArray = null;");
		hpb.writeLine("var theNameArray = null;");
		hpb.writeLine("function displayLocations() {");
		
		//  check if data present
		hpb.writeLine("  if ((theLongArray == null) || (theLatArray == null) || (theNameArray == null)) return;");
		
		//	make sure map window exists
		hpb.writeLine("  openGoogleMap();");
		
		//	bring map window to front and show data only if initialized completely
		hpb.writeLine("  if (googleMapWindow.initialized) {");
		hpb.writeLine("    googleMapWindow.addMarkersToMap(theLongArray, theLatArray, theNameArray);");
		hpb.writeLine("    googleMapWindow.focus();");
		hpb.writeLine("  }");
		
		//	trigger retry in 50ms otherwise
		hpb.writeLine("  else window.setTimeout('displayLocations();', 50);");
		hpb.writeLine("}");
		
		hpb.writeLine("function showLocations(longArray, latArray, nameArray) {");
		hpb.writeLine("  theLongArray = longArray;");
		hpb.writeLine("  theLatArray = latArray;");
		hpb.writeLine("  theNameArray = nameArray;");
		hpb.writeLine("  displayLocations();");
		hpb.writeLine("}");
		
		hpb.writeLine("function showLocation(long, lat, name) {");
		hpb.writeLine("  theLongArray = new Array(1);");
		hpb.writeLine("  theLongArray[0] = long;");
		hpb.writeLine("  theLatArray = new Array(1);");
		hpb.writeLine("  theLatArray[0] = lat;");
		hpb.writeLine("  theNameArray = new Array(1);");
		hpb.writeLine("  theNameArray[0] = name;");
		hpb.writeLine("  displayLocations();");
		hpb.writeLine("}");
		
		hpb.writeLine("</script>");
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalConstants.SearchResultLinker#getAnnotationSetLinks(de.uka.ipd.idaho.gamta.Annotation[])
	 */
	public SearchResultLink[] getAnnotationSetLinks(Annotation[] annotations) {
		ArrayList linkList = new ArrayList();
		
		//	get data
		LinkData linkData = new LinkData();
		this.addLinks(annotations, linkData);
		
		//	no location in document, don't add link
		if (linkData.locationKeys.isEmpty()) return null;
		
		//	one location in document, use special link
		else if (linkData.locationKeys.size() == 1) {
			linkList.add(new SearchResultLink(VISUALIZATION,
					this.getClass().getName(),
					"GoogleMaps", 
					null, // TODO: insert name of GoogleMaps icon image
					"Plot the location in a Google Map",
					"", 
					"showLocation(" + linkData.longitudes.get(0) + ", " + linkData.latitudes.get(0) + ", '" + linkData.locationNames.get(0) + "'); return false;"
				));
		}
		
		//	locations in document, produce link
		else {
			linkList.add(new SearchResultLink(VISUALIZATION,
					this.getClass().getName(),
					"GoogleMaps", 
					null, // TODO: insert name of GoogleMaps icon image
					"Plot all the locations in a Google Map",
					"", 
					"showLocations(new Array(" + linkData.longitudes.concatStrings(",") + "), new Array(" + linkData.latitudes.concatStrings(",") + "), new Array('" + linkData.locationNames.concatStrings("','") + "')); return false;"
				));
		}
		
		return ((SearchResultLink[]) linkList.toArray(new SearchResultLink[linkList.size()]));
	}

	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalConstants.SearchResultLinker#getDocumentLinks(de.uka.ipd.idaho.gamta.MutableAnnotation)
	 */
	public SearchResultLink[] getDocumentLinks(MutableAnnotation doc) {
		ArrayList linkList = new ArrayList();
		
		//	get annotations
		Annotation[] locations = doc.getAnnotations(LOCATION_TYPE);
		
		//	get data
		LinkData linkData = new LinkData();
		this.addLinks(locations, linkData);
		
		//	no location in document, don't add link
		if (linkData.locationKeys.isEmpty()) return null;
		
		//	one location in document, use special link
		else if (linkData.locationKeys.size() == 1) {
			linkList.add(new SearchResultLink(VISUALIZATION,
					this.getClass().getName(),
					"GoogleMaps", 
					null, // TODO: insert name of GoogleMaps icon image
					"Plot the location mentioned in this document in a Google Map",
					"", 
					"showLocation(" + linkData.longitudes.get(0) + ", " + linkData.latitudes.get(0) + ", '" + linkData.locationNames.get(0) + "'); return false;"
				));
		} 
		
		//	locations in document, produce link
		else {
			linkList.add(new SearchResultLink(VISUALIZATION,
					this.getClass().getName(),
					"GoogleMaps", 
					null, // TODO: insert name of GoogleMaps icon image
					"Plot all the locations mentioned in this document in a Google Map",
					"", 
					"showLocations(new Array(" + linkData.longitudes.concatStrings(",") + "), new Array(" + linkData.latitudes.concatStrings(",") + "), new Array('" + linkData.locationNames.concatStrings("','") + "')); return false;"
				));
		}
		
		return ((SearchResultLink[]) linkList.toArray(new SearchResultLink[linkList.size()]));
	}

	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalConstants.SearchResultLinker#getSearchResultLinks(de.uka.ipd.idaho.gamta.MutableAnnotation[])
	 */
	public SearchResultLink[] getSearchResultLinks(MutableAnnotation[] docs) {
		
		//	get data
		LinkData linkData = new LinkData();
		for (int d = 0; d < docs.length; d++) {
			Annotation[] locations = docs[d].getAnnotations(LOCATION_TYPE);
			this.addLinks(locations, linkData);
		}
		
		//	no location in document, don't add link
		if (linkData.locationKeys.isEmpty()) return null;
		
		//	add link only if locations given
		else {
			ArrayList linkList = new ArrayList();
			
			//	one location in document, use special link
			if (linkData.locationKeys.size() == 1) {
				linkList.add(new SearchResultLink(VISUALIZATION,
						this.getClass().getName(),
						"GoogleMaps", 
						null, // TODO: insert name of GoogleMaps icon image
						"Plot the location mentioned in this search result in a Google Map",
						"", 
						"showLocation(" + linkData.longitudes.get(0) + ", " + linkData.latitudes.get(0) + ", '" + linkData.locationNames.get(0) + "'); return false;"
					));
				
			}
			
			//	locations in document, produce link
			else {
				linkList.add(new SearchResultLink(VISUALIZATION,
						this.getClass().getName(),
						"GoogleMaps", 
						null, // TODO: insert name of GoogleMaps icon image
						"Plot all the locations mentioned in this search result in a Google Map",
						"", 
						"showLocations(new Array(" + linkData.longitudes.concatStrings(",") + "), new Array(" + linkData.latitudes.concatStrings(",") + "), new Array('" + linkData.locationNames.concatStrings("','") + "')); return false;"
					));
			}
			
			return ((SearchResultLink[]) linkList.toArray(new SearchResultLink[linkList.size()]));
		}
	}

	private String clean(String s) {
		return s.replaceAll("\\\"|\\'", "");
	}
	
	private class LinkData {
		private StringVector longitudes = new StringVector();
		private StringVector latitudes = new StringVector();
		private StringVector locationNames = new StringVector();
		private HashSet locationKeys = new HashSet();
	}
	
	private void addLinks(Annotation[] annotations, LinkData linkData) {
		for (int l = 0; l < annotations.length; l++) {
			if (LOCATION_TYPE.equals(annotations[l].getType())) {
				String longitude = annotations[l].getAttribute(LONGITUDE_ATTRIBUTE, "360").toString().trim();
				String latitude = annotations[l].getAttribute(LATITUDE_ATTRIBUTE, "360").toString().trim();
//				if ((longitude.length() != 0) && (latitude.length() != 0)) {
				if (!"360".equals(longitude) && !"360".equals(latitude)) {
					String locationName = this.clean(annotations[l].getValue());
					String locationKey = (longitude + " " + latitude + " " + locationName);
					if (linkData.locationKeys.add(locationKey)) {
						linkData.longitudes.addElement(longitude);
						linkData.latitudes.addElement(latitude);
						linkData.locationNames.addElement(this.clean(annotations[l].getValue()));
					}
				}
			}
		}
	}
}
