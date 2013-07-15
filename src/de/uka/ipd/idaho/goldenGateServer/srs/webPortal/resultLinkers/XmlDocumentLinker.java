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


import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalConstants.SearchResultLinker;

/**
 * @author sautter
 *
 */
public class XmlDocumentLinker extends SearchResultLinker {
	
	private static final String TRANSFORMATION_SETTINGS_PREFIX = "TRANSFORMATION_";
	private static final String TRANSFORMATION_PATH_SETTING = "PATH";
	private static final String TRANSFORMATION_LABEL_SETTING = "LABEL";
	private static final String TRANSFORMATION_TITLE_SETTING = "TITLE";
	private static final String TRANSFORMATION_XSLT_URL_SETTING = "XSLT_URL";
	private static final String TRANSFORMATION_ICON_URL_SETTING = "ICON_URL";
	
	private class XmlLinkData {
		private String path;
		private String label;
		private String title;
		private String xsltUrl;
		private String iconUrl;
		
		private XmlLinkData(String path, String label, String title, String xsltUrl, String iconUrl) {
			this.path = path;
			this.label = label;
			this.title = title;
			this.xsltUrl = xsltUrl;
			this.iconUrl = iconUrl;
		}
	}
	private String linkBaseUrl = null;
	private XmlLinkData[] links = new XmlLinkData[0];
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalConstants.SearchResultLinker#init()
	 */
	protected void init() {
		Settings settings = Settings.loadSettings(new File(this.dataPath, ("config.cnfg")));
		this.linkBaseUrl = settings.getSetting("LinkBaseURL");
			
		String basePath = this.parent.getServletContext().getRealPath(".");
		if (basePath != null) {
			basePath = basePath.replaceAll("\\\\", "/");
			if (basePath.endsWith("."))
				basePath = basePath.substring(0, (basePath.length()-1));
			if (basePath.endsWith("/"))
				basePath = basePath.substring(0, (basePath.length()-1));
			this.linkBaseUrl = ((basePath.indexOf('/') == -1) ? ("/" + basePath) : basePath.substring(basePath.lastIndexOf('/')));
		}
		
		ArrayList linkList = new ArrayList();
		for (int p = 0; p < settings.size(); p++) {
			Settings providerSettings = settings.getSubset(TRANSFORMATION_SETTINGS_PREFIX + p);
			if (providerSettings.size() == 0)
				continue;
			String path = providerSettings.getSetting(TRANSFORMATION_PATH_SETTING);
			if (path == null)
				continue;
			String label = providerSettings.getSetting(TRANSFORMATION_LABEL_SETTING);
			if (label == null)
				continue;
			linkList.add(new XmlLinkData(
				path,
				label,
				providerSettings.getSetting(TRANSFORMATION_TITLE_SETTING, ""),
				providerSettings.getSetting(TRANSFORMATION_XSLT_URL_SETTING),
				providerSettings.getSetting(TRANSFORMATION_ICON_URL_SETTING, "")
			));
		}
		this.links = ((XmlLinkData[]) linkList.toArray(new XmlLinkData[linkList.size()]));
	}
	
	//	TODO use servlet path instead of including XSLT URL in link
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchResultLinker#getName()
	 */
	public String getName() {
		return ("XML Document Linker");
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalConstants.SearchResultLinker#getDocumentLinks(de.uka.ipd.idaho.gamta.MutableAnnotation)
	 */
	public SearchResultLink[] getDocumentLinks(MutableAnnotation doc) {
		ArrayList linkList = new ArrayList();
		
		//	get document ID
		Object docId = doc.getAttribute(DOCUMENT_ID_ATTRIBUTE);
		
		//	return links if ID given
		if (docId != null)
			for (int l = 0; l < this.links.length; l++) try {
				linkList.add(new SearchResultLink(XML_DOCUMENT,
						this.getClass().getName(),
						this.links[l].label, 
						this.links[l].iconUrl, 
						this.links[l].title, 
//						(this.linkBaseUrl + "?" + ID_QUERY_FIELD_NAME + "=" + docId.toString() + ((this.links[l].xsltUrl == null) ? "" : ("&" + XSLT_URL_PARAMETER + "=" + URLEncoder.encode(this.links[l].xsltUrl, "UTF-8")))), 
						(this.linkBaseUrl + "/" + this.links[l].path + "/" + docId.toString() + ((this.links[l].xsltUrl == null) ? "" : ("?" + XSLT_URL_PARAMETER + "=" + URLEncoder.encode(this.links[l].xsltUrl, "UTF-8")))), 
						""));
			} catch (IOException ioe) {}
			
		return ((SearchResultLink[]) linkList.toArray(new SearchResultLink[linkList.size()]));
	}
}
