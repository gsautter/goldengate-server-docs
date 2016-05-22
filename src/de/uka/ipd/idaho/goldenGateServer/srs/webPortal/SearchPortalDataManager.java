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
 *     * Neither the name of the Universität Karlsruhe (TH) / KIT nor the
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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import de.uka.ipd.idaho.easyIO.web.WebAppHost;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;

/**
 * Data management layer shared between the servlets in a GoldenGATE SRS Search
 * Portal web application. This class is instantiated only once for each web
 * application.
 * 
 * @author sautter
 */
public class SearchPortalDataManager extends GoldenGateSrsClient {
	
	private static HashMap instances = new HashMap();
	
	/**
	 * Obtain a data manager instance for a given web application host object.
	 * If one already exists, this method simply returns it. Otherwise, a new
	 * data manager is created based on the argument SRS client object.
	 * @param wah the web application host to obtain the instance for
	 * @param srsc the GoldenGATE SRS client to use for a new instance
	 * @return a data manager object for the argument web application host
	 */
	public static synchronized SearchPortalDataManager getInstance(WebAppHost wah, GoldenGateSrsClient srsc) {
		String wahRootPath = wah.getRootFolder().getAbsolutePath();
		SearchPortalDataManager spdm = ((SearchPortalDataManager) instances.get(wahRootPath));
		if (spdm == null) {
			spdm = new SearchPortalDataManager();
			instances.put(wahRootPath, spdm);
		}
		spdm.setSrsClient(srsc);
		return spdm;
	}
	
	private SearchPortalDataManager() {
		super(null); // we only mimic SRS client, no need for backend connection
	}
	
	private GoldenGateSrsClient srsClient;
	private void setSrsClient(GoldenGateSrsClient srsClient) {
		
		//	first SRS client, need to keep it
		if (this.srsClient == null) {
			this.srsClient = srsClient;
			return;
		}
		
		//	prefer custom sub class over default implementation, customization should be for a reason
		if (!this.srsClient.getClass().equals(srsClient.getClass())) {
			if (this.srsClient.getClass().isAssignableFrom(srsClient.getClass()))
				this.srsClient = srsClient;
			return;
		}
		
		//	prefer caching instance over non-caching one
		if (!this.srsClient.isCachingEnabled() && srsClient.isCachingEnabled()) {
			this.srsClient = srsClient;
			return;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#isCachingEnabled()
	 */
	public boolean isCachingEnabled() {
		return false;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#setCacheFolder(java.io.File)
	 */
	public void setCacheFolder(File cacheFolder) {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getSearchFieldGroups()
	 */
	public SearchFieldGroup[] getSearchFieldGroups() throws IOException {
		return this.srsClient.getSearchFieldGroups();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getSearchFieldGroups(boolean)
	 */
	public SearchFieldGroup[] getSearchFieldGroups(boolean allowCache) throws IOException {
		return this.srsClient.getSearchFieldGroups(allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getXmlDocument(java.lang.String)
	 */
	public MutableAnnotation getXmlDocument(String docId) throws IOException {
		return this.srsClient.getXmlDocument(docId);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getXmlDocument(java.lang.String, boolean)
	 */
	public MutableAnnotation getXmlDocument(String docId, boolean allowCache) throws IOException {
		return this.srsClient.getXmlDocument(docId, allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchDocuments(java.util.Properties, boolean)
	 */
	public DocumentResult searchDocuments(Properties parameters, boolean markSearchables) throws IOException {
		return this.srsClient.searchDocuments(parameters, markSearchables);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchDocuments(java.util.Properties, boolean, boolean)
	 */
	public DocumentResult searchDocuments(Properties parameters, boolean markSearchables, boolean allowCache) throws IOException {
		return this.srsClient.searchDocuments(parameters, markSearchables, allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchDocumentDetails(java.util.Properties, boolean)
	 */
	public DocumentResult searchDocumentDetails(Properties parameters, boolean markSearchables) throws IOException {
		return this.srsClient.searchDocumentDetails(parameters, markSearchables);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchDocumentDetails(java.util.Properties, boolean, boolean)
	 */
	public DocumentResult searchDocumentDetails(Properties parameters, boolean markSearchables, boolean allowCache) throws IOException {
		return this.srsClient.searchDocumentDetails(parameters, markSearchables, allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchDocumentData(java.util.Properties, boolean)
	 */
	public DocumentResult searchDocumentData(Properties parameters, boolean markSearchables) throws IOException {
		return this.srsClient.searchDocumentData(parameters, markSearchables);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchDocumentData(java.util.Properties, boolean, boolean)
	 */
	public DocumentResult searchDocumentData(Properties parameters, boolean markSearchables, boolean allowCache) throws IOException {
		return this.srsClient.searchDocumentData(parameters, markSearchables, allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchDocumentIDs(java.util.Properties)
	 */
	public DocumentResult searchDocumentIDs(Properties parameters) throws IOException {
		return this.srsClient.searchDocumentIDs(parameters);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchDocumentIDs(java.util.Properties, boolean)
	 */
	public DocumentResult searchDocumentIDs(Properties parameters, boolean allowCache) throws IOException {
		return this.srsClient.searchDocumentIDs(parameters, allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getModifiedSince(long)
	 */
	public DocumentResult getModifiedSince(long timestamp) throws IOException {
		return this.srsClient.getModifiedSince(timestamp);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getDocumentList(java.lang.String)
	 */
	public DocumentList getDocumentList(String masterDocID) throws IOException {
		return this.srsClient.getDocumentList(masterDocID);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchIndex(java.util.Properties, boolean)
	 */
	public IndexResult searchIndex(Properties parameters, boolean markSearchables) throws IOException {
		return this.srsClient.searchIndex(parameters, markSearchables);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchIndex(java.util.Properties, boolean, boolean)
	 */
	public IndexResult searchIndex(Properties parameters, boolean markSearchables, boolean allowCache) throws IOException {
		return this.srsClient.searchIndex(parameters, markSearchables, allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchThesaurus(java.util.Properties)
	 */
	public ThesaurusResult searchThesaurus(Properties parameters) throws IOException {
		return this.srsClient.searchThesaurus(parameters);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#searchThesaurus(java.util.Properties, boolean)
	 */
	public ThesaurusResult searchThesaurus(Properties parameters, boolean allowCache) throws IOException {
		return this.srsClient.searchThesaurus(parameters, allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getStatistics()
	 */
	public CollectionStatistics getStatistics() throws IOException {
		return this.srsClient.getStatistics();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getStatistics(java.lang.String)
	 */
	public CollectionStatistics getStatistics(String since) throws IOException {
		return this.srsClient.getStatistics(since);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getStatistics(long)
	 */
	public CollectionStatistics getStatistics(long since) throws IOException {
		return this.srsClient.getStatistics(since);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getStatistics(boolean)
	 */
	public CollectionStatistics getStatistics(boolean allowCache) throws IOException {
		return this.srsClient.getStatistics(allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getStatistics(java.lang.String, boolean)
	 */
	public CollectionStatistics getStatistics(String since, boolean allowCache) throws IOException {
		return this.srsClient.getStatistics(since, allowCache);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient#getStatistics(long, boolean)
	 */
	public CollectionStatistics getStatistics(long since, boolean allowCache) throws IOException {
		return this.srsClient.getStatistics(since, allowCache);
	}
}
