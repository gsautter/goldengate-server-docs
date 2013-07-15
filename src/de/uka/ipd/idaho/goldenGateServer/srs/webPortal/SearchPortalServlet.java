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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.Transformer;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.ComponentInitializer;
import de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet.ReInitializableServlet;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatisticsElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedCollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedDocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedIndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedThesaurusResult;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layouts.DefaultSearchPortalLayout;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils.IsolatorWriter;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Html;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * The Servlet hosting the GoldenGATE SRS search portal. The actual layout of
 * the portal is controlled by a SearchPortalLayout plugin.<br>
 * Beside the ones from the super class, this servlet takes on additional init
 * parameter, namely 'selfLink', passing the default servlet path to the servlet
 * prior to the first invocation. In addition to this default servlet mapping
 * ('/search' by default), this servlet requires four additional mappings:
 * '/thesaurus' for thesaurus search, '/statistics' for the collection
 * statistics, '/html' for displaying individual document, and '/xml' for
 * displaying/delivering individual documents as raw XML data rather than
 * preprocessed for human reading by the plugged-in layout.<br>
 * This servlet can host two types of plugins:
 * <ul>
 * <li>For dynamically adding custom links to search result pages, you can plug
 * <b>SearchResultLinker</b> objects into the search portal. These classes have
 * to implement the SearchPortalConstants.SearchResultLinker interface. The
 * respective implementations have to be deposited in jar files in the
 * 'SearchResultLinkers' sub folder of the web-app's context path. They will be
 * loaded as this servlet is loaded.</li>
 * <li>For customizing the layout of search results prepared for human reading,
 * you can plug a custom layout engin into the search portal. Such a plugin
 * layout has to extend the abstract <b>SearchPortalLayout</b> class. Th e
 * respective implementations have to be deposited in a jar file inside the
 * 'SearchPortalLayouts' sub folder of the web-app's context path. You have to
 * specify the desired class' fully qualified name as the value of the
 * 'layoutClassName' setting in the applet's config file (see below).</li>
 * </ul>
 * For each request, you can specify the <b>result format</b>. The default
 * value is 'html', resulting in the search results being prepared for human
 * reading by the installed layout plugin. Specifying 'xml' will result in raw
 * XML data being returned, intended more for machine than for human
 * readability. For thesaurus search and collection statistics, you can
 * additionally specify 'csv', which will return the thesaurus lookup result or
 * collection statistics in CSV format. Functionally, there is no advantage over
 * XML, but the amount of data is considerably lower.<br>
 * The config file also can provide additional customization parameters:
 * <ul>
 * <li><b>documentLabelSingular</b>: a custom name for the sort of documents
 * hosted in the backing SRS. The default is 'document'. Using this parameter,
 * you can change it to 'article', for instance.</li>
 * <li><b>documentLabelPlural</b>: a custom plural for of the
 * 'documentLabelSingular' parameter</li>
 * <li><b>searchFormTitle</b>: a custom titel for the document search form</li>
 * <li><b>thesaurusFormTitle</b>: a custom titel for the thesaurus search form</li>
 * <li><b>includeSearchFormWithResult</b>: display the search form on result
 * pages?</li>
 * <li><b>displayTitlePattern</b>: a pattern for creating the display title of
 * documents from the documents' attributed, eg titel, author, year of
 * publication, etc.</li>
 * <li><b>resultFormatAlias</b>: with this prefix, you can mark custom result
 * format names that will use XSLT to create the display pages of search
 * results. The settiong 'resultFormatAlias.&lt;alias&gt;' with the value
 * &lt;myXsltUrl&gt; will cause the servlet to transform the search result XML
 * through the styleshet found at &lt;myXsltUrl&gt; if the 'resultFormat'
 * request parameter (see below) is set to &lt;alias&gt;.</li>
 * <li><b>layoutClassName</b>: the class name of the layout plugin to use. Not
 * specifying this parameter will result in the default layout being used. While
 * this does not hamper functionality, the layout produced by the default plugin
 * is configurable only in limited bounds (colors, fonts, ...).</li>
 * </ul>
 * 
 * @author sautter
 */
public class SearchPortalServlet extends AbstractSrsWebPortalServlet implements ReInitializableServlet, SearchPortalConstants {
	
	private static Html html = new Html();
	private static Parser htmlParser = new Parser(html);
	
	private static final String LAYOUT_ENGINE_CLASS_NAME_SETTING = "layoutClassName";
	
	private SearchPortalLayout layout = new DefaultSearchPortalLayout();
	private String layoutDataPath = "";
	
	private String documentLabelSingular = "document";
	private String documentLabelPlural = "documents";
	
	private String[] cssStylesheetsToInlcude;
	
	private SearchResultLinker[] resultLinkers;
	private Properties resultLinkerDataPaths = new Properties();
	
	private LinkedHashSet javaScriptsToInclude = new LinkedHashSet();
	private LinkedHashSet functionsToCallOnLoad = new LinkedHashSet();
	private LinkedHashSet functionsToCallOnUnload = new LinkedHashSet();
	
	private LinkedHashSet searchResultLinks = new LinkedHashSet();
	private LinkedHashSet thesaurusResultLinks = new LinkedHashSet();
	private LinkedHashSet statisticsLineLinks = new LinkedHashSet();
	
	private String searchFormTitle = null;
	private String thesaurusFormTitle = null;
	
	private boolean includeSearchFormWithResult = false;
	
	private HashSet cachedStylesheets = new HashSet();
	
	/** @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPost(final HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		System.out.println("Handling request from " + request.getRemoteAddr());
		StringVector paramCollector = new StringVector();
		Enumeration paramEnum = request.getParameterNames();
		while (paramEnum.hasMoreElements())
			paramCollector.addElement(paramEnum.nextElement().toString());
		
		//	get search mode
		String requestSearchMode = request.getParameter(SEARCH_MODE_PARAMETER);
		paramCollector.removeAll(SEARCH_MODE_PARAMETER);
		
		//	get invokation path (alternative to search mode)
		String invokationPath = request.getServletPath();
		
		//	modify search mode
		if (invokationPath != null) {
			
			//	cut leading slash
			if (invokationPath.startsWith("/"))
				invokationPath = invokationPath.substring(1);
			
			//	request for HTML document
			if ("html".equals(invokationPath))
				requestSearchMode = HTML_DOCUMENT_SEARCH_MODE;
			
			//	request for XML document
			else if ("xml".equals(invokationPath))
				requestSearchMode = XML_DOCUMENT_SEARCH_MODE;
			
			//	thesaurus search
			else if (THESAURUS_SEARCH_MODE.equals(invokationPath))
				requestSearchMode = THESAURUS_SEARCH_MODE;
			
			//	request for statistics
			else if (STATISTICS_MODE.equals(invokationPath))
				requestSearchMode = STATISTICS_MODE;
		}
		
		//	set default search mode
		if (requestSearchMode == null)
			requestSearchMode = DOCUMENT_SEARCH_MODE;
		
		//	get result format
		String requestResultFormat = request.getParameter(RESULT_FORMAT_PARAMETER);
		paramCollector.removeAll(RESULT_FORMAT_PARAMETER);
		
		//	initialize XSLT result transformation
		Transformer xslTransformer;
		
		//	no XSL transformer needed
		if ((requestResultFormat == null) || HTML_RESULT_FORMAT.equals(requestResultFormat) || CSV_RESULT_FORMAT.equals(requestResultFormat))
			xslTransformer = null;
		
		//	resolve alias
		else {
			
			//	get XSLT stylesheet URL
			String xsltUrl = this.resultFormatAliasesToXsltURLs.getProperty(requestResultFormat);
			if (xsltUrl == null)
				xsltUrl = request.getParameter(XSLT_URL_PARAMETER);
			
			//	set general result format
			requestResultFormat = XML_RESULT_FORMAT;
			
			//	get transformer
			xslTransformer = ((xsltUrl == null) ? null : XsltUtils.getTransformer(xsltUrl, !this.cachedStylesheets.add(xsltUrl)));
		}
		paramCollector.removeAll(XSLT_URL_PARAMETER);
		
		//	fix result format
		final String resultFormat = ((requestResultFormat == null) ? HTML_RESULT_FORMAT : requestResultFormat);
		
		//	get index name (null for document search by default, 0 for document index search)
		String requestIndexName = request.getParameter(INDEX_NAME);
		if ("documents".equals(requestIndexName) || "docs".equals(requestIndexName))
			requestIndexName = "0";
		paramCollector.removeAll(INDEX_NAME);
		
		//	get sub index name
		String[] requestSubIndexNames = request.getParameterValues(SUB_INDEX_NAME);
		paramCollector.removeAll(SUB_INDEX_NAME);
		
		//	provide final index ID and sub index IDs
		final String indexName;
		final String subIndexName;
		final String searchMode;
		
		//	request for statistics
		if (STATISTICS_MODE.equals(requestSearchMode)) {
			indexName = null;
			subIndexName = null;
			searchMode = STATISTICS_MODE;
		}
		
		//	search for specific document
		else if (HTML_DOCUMENT_SEARCH_MODE.equals(requestSearchMode)) {
			indexName = null;
			subIndexName = null;
			searchMode = HTML_DOCUMENT_SEARCH_MODE;
		}
		
		//	thesaurus search
		else if (THESAURUS_SEARCH_MODE.equals(requestSearchMode)) {
			indexName = ("0".equals(requestIndexName) ? null : requestIndexName);
			subIndexName = null;
			searchMode = THESAURUS_SEARCH_MODE;
		}
		
		//	thesaurus search
		else if (XML_DOCUMENT_SEARCH_MODE.equals(requestSearchMode)) {
			indexName = null;
			subIndexName = null;
			searchMode = XML_DOCUMENT_SEARCH_MODE;
		}
		
		//	no sub index name at all, do document search if index name is null or 0
		else if ((requestSubIndexNames == null) || (requestSubIndexNames.length == 0)) {
			indexName = requestIndexName;
			subIndexName = null;
			searchMode = (((indexName == null) || ("0".equals(indexName) && !INDEX_SEARCH_MODE.equals(requestSearchMode))) ? DOCUMENT_SEARCH_MODE : INDEX_SEARCH_MODE);
		}
		
		//	request with sub index names
		else {
			StringVector sv = new StringVector();
			sv.addContent(requestSubIndexNames);
			sv.removeAll("0");
			sv.removeAll("documents");
			sv.removeAll("docs");
			if (requestIndexName != null)
				sv.removeAll(requestIndexName);
			
			//	no sub index name distinct from index name
			if (sv.isEmpty()) {
				indexName = requestIndexName;
				subIndexName = null;
				searchMode = (((indexName == null) || ("0".equals(indexName) && !INDEX_SEARCH_MODE.equals(requestSearchMode))) ? DOCUMENT_SEARCH_MODE : INDEX_SEARCH_MODE);
			}
			
			//	sub index specified, do index search
			else {
				indexName = requestIndexName;
				subIndexName = sv.concatStrings("\n");
				searchMode = (((indexName == null) || ("0".equals(indexName) && !INDEX_SEARCH_MODE.equals(requestSearchMode))) ? DOCUMENT_SEARCH_MODE : INDEX_SEARCH_MODE);
			}
		}
		
		//	get minimum size for sub result
		String requestSubResultMinSize = request.getParameter(SUB_RESULT_MIN_SIZE);
		final String subResultMinSize;
		if (subIndexName == null) {
			subResultMinSize = null;
		} else subResultMinSize = requestSubResultMinSize;
		paramCollector.removeAll(SUB_RESULT_MIN_SIZE);
		
		//	check is searchables to mark
		boolean markSearchables = paramCollector.contains(MARK_SEARCHABLES_PARAMETER);
		paramCollector.removeAll(MARK_SEARCHABLES_PARAMETER);
		
		//	set response content type (must be done before obtaining writer)
		if (XML_DOCUMENT_SEARCH_MODE.equals(searchMode) || XML_RESULT_FORMAT.equals(resultFormat))
			response.setContentType("text/xml; charset=" + ENCODING);
		
		else if (THESAURUS_SEARCH_MODE.equals(searchMode) && CSV_RESULT_FORMAT.equals(resultFormat))
			response.setContentType("text/plain; charset=" + ENCODING);
		
		else response.setContentType("text/html; charset=" + ENCODING);
		
		//	read search parameters
		Properties searchParameters = new Properties();
		for (int p = 0; p < paramCollector.size(); p++) {
			String name = paramCollector.get(p);
			String[] values = request.getParameterValues(name);
			if ((values != null) && (values.length != 0)) {
				if (values.length == 1)
					searchParameters.setProperty(name, values[0]);
				else {
					StringVector valueBuilder = new StringVector();
					valueBuilder.addContent(values);
					searchParameters.setProperty(name, valueBuilder.concatStrings("\n"));
				}
			}
		}
		
		//	prepare for pre-setting form fields
		final Properties fieldValues = new Properties();
		for (int p = 0; p < paramCollector.size(); p++) {
			String name = paramCollector.get(p);
			String value = request.getParameter(name);
			if ((value != null) && (value.trim().length() != 0))
				fieldValues.setProperty(name, value);
		}
		
		//	get output writer
		response.setHeader("Cache-Control", "no-cache");
		
		//	request for specific plain XML document
		if (XML_DOCUMENT_SEARCH_MODE.equals(searchMode)) {
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), ENCODING));
			this.doGetDocument(request, xslTransformer, out);
			out.flush();
			out.close();
			return;
		}
		
		//	request for page (document, index, or thesaurus search, or statistics or document detail request)
		if (HTML_RESULT_FORMAT.equals(resultFormat)) {
			this.doHtmlRequest(request, response, searchMode, fieldValues, searchParameters, indexName, subIndexName, subResultMinSize);
			return;
		}
		
		//	get writer for text or XLM result 
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), ENCODING));
		
		//	thesaurus search with non-html result
		if (THESAURUS_SEARCH_MODE.equals(searchMode))
			this.doSearchThesaurus(searchParameters, CSV_RESULT_FORMAT.equals(resultFormat), xslTransformer, out, !FORCE_CACHE.equals(request.getParameter(CACHE_CONTROL_PARAMETER)));
		
		//	thesaurus search with non-html result
		else if (STATISTICS_MODE.equals(searchMode))
			this.doStatistics(CSV_RESULT_FORMAT.equals(resultFormat), xslTransformer, out, !FORCE_CACHE.equals(request.getParameter(CACHE_CONTROL_PARAMETER)));
		
		//	index search with non-html result
		else if (indexName != null) {
			searchParameters.setProperty(INDEX_NAME, indexName);
			
			if (subIndexName != null)
				searchParameters.setProperty(SUB_INDEX_NAME, subIndexName);
			
			if (subResultMinSize != null)
				searchParameters.setProperty(SUB_RESULT_MIN_SIZE, subResultMinSize);
			
			this.doIndexSearch(searchParameters, markSearchables, xslTransformer, out, !FORCE_CACHE.equals(request.getParameter(CACHE_CONTROL_PARAMETER)));
		}
		
		//	document data search with non-html result
		else if (subIndexName != null) {
			searchParameters.setProperty(SUB_INDEX_NAME, subIndexName);
			
			if (subResultMinSize != null)
				searchParameters.setProperty(SUB_RESULT_MIN_SIZE, subResultMinSize);
			
			this.doDocumentIndexSearch(searchParameters, markSearchables, xslTransformer, out, !FORCE_CACHE.equals(request.getParameter(CACHE_CONTROL_PARAMETER)));
		}
		
		//	document search with non-html result
		else this.doDocumentSearch(searchParameters, markSearchables, xslTransformer, out, !FORCE_CACHE.equals(request.getParameter(CACHE_CONTROL_PARAMETER)));
		
		//	finish response
		out.flush();
		out.close();
	}
	
	private void doGetDocument(HttpServletRequest request, Transformer xslTransformer, BufferedWriter out) throws IOException {
		String idQuery = null;
		
		//	read document ID from parameter
		String[] docIds = request.getParameterValues(ID_QUERY_FIELD_NAME);
		if ((docIds != null) && (docIds.length == 1)) idQuery = docIds[0];
		
		//	read invokation path path info (alternative to idQuery parameter in this mode)
		else {
			String pathIdQuery = request.getPathInfo();
			
			//	got document ID in path
			if (pathIdQuery != null) {
				
				//	cut leading slash
				if (pathIdQuery.startsWith("/"))
					pathIdQuery = pathIdQuery.substring(1);
				
				//	set search parameter
				idQuery = pathIdQuery;
			}
			
			//	finally, check query string
			else {
				String requestQueryString = request.getQueryString();
				
				//	got document ID in query position
				if ((requestQueryString != null) && (requestQueryString.length() != 0) && (requestQueryString.indexOf('=') == -1))
					idQuery = requestQueryString;
			}
		}
		
		
		if (idQuery == null)
			out.write("Invalid request format, submit one document ID at a time in this mode.");
		
		else {
			
			//	cut file extension
			if (idQuery.indexOf('.') != -1)
				idQuery = idQuery.substring(0, idQuery.indexOf('.'));
			
			try {
				MutableAnnotation doc = this.srsClient.getXmlDocument(idQuery, !FORCE_CACHE.equals(request.getParameter(CACHE_CONTROL_PARAMETER)));
				
				//	write document (plain or transformed)
				Writer w = XsltUtils.wrap(new IsolatorWriter(out), xslTransformer);
				AnnotationUtils.writeXML(doc, w);
				w.flush();
				w.close();
			}
			catch (IOException e) {
				out.write("Error loading document '" + idQuery + "': " + e.getMessage());
			}
		}
		
		out.newLine();
	}
	
	private void doSearchThesaurus(Properties searchParameters, boolean csv, Transformer xslTransformer, BufferedWriter out, boolean allowCache) throws IOException {
		ThesaurusResult thesaurusResult = this.srsClient.searchThesaurus(searchParameters, allowCache);
		
		//	write thesaurus result in CSV format
		if (csv) {
			while (thesaurusResult.hasNextElement()) {
				ThesaurusResultElement tre = thesaurusResult.getNextThesaurusResultElement();
				out.write(tre.toCsvString('"', thesaurusResult.resultAttributes));
				out.newLine();
			}
		}
		
		//	write thesaurus result as XML
		else {
			
			//	write result (plain or transformed)
			Writer w = XsltUtils.wrap(new IsolatorWriter(out), xslTransformer);
			thesaurusResult.writeXml(w);
			w.flush();
			w.close();
		}
	}
	
	private void doStatistics(boolean csv, Transformer xslTransformer, BufferedWriter out, boolean allowCache) throws IOException {
		CollectionStatistics statistics = this.srsClient.getStatistics(allowCache);
		
		//	write statistics in CSV format
		if (csv) {
			while (statistics.hasNextElement()) {
				CollectionStatisticsElement cse = statistics.getNextCollectionStatisticsElement();
				out.write(cse.toCsvString('"', statistics.resultAttributes));
				out.newLine();
			}
		}
		
		//	write statistics as XML
		else {
			
			//	write result (plain or transformed)
			Writer w = XsltUtils.wrap(new IsolatorWriter(out), xslTransformer);
			statistics.writeXml(w);
			w.flush();
			w.close();
		}
	}
	
	private void doIndexSearch(Properties searchParameters, boolean markSearchables, Transformer xslTransformer, BufferedWriter out, boolean allowCache) throws IOException {
		IndexResult indexResult = this.srsClient.searchIndex(searchParameters, markSearchables, allowCache);
		
		//	write result (plain or transformed)
		Writer w = XsltUtils.wrap(new IsolatorWriter(out), xslTransformer);
		indexResult.writeXml(w);
		w.flush();
		w.close();
	}
	
	private void doDocumentIndexSearch(Properties searchParameters, boolean markSearchables, Transformer xslTransformer, BufferedWriter out, boolean allowCache) throws IOException {
		DocumentResult documentResult = this.srsClient.searchDocumentData(searchParameters, markSearchables, allowCache);
		
		//	write result (plain or transformed)
		Writer w = XsltUtils.wrap(new IsolatorWriter(out), xslTransformer);
		documentResult.writeXml(w);
		w.flush();
		w.close();
	}
	
	private void doDocumentSearch(Properties searchParameters, boolean markSearchables, Transformer xslTransformer, BufferedWriter out, boolean allowCache) throws IOException {
		DocumentResult result = this.srsClient.searchDocuments(searchParameters, markSearchables, allowCache);
		
		//	search returned no results
		if (!result.hasNextElement()) {
			out.write("<" + RESULTS_NODE_NAME + "/>");
			out.newLine();
		}
		
		//	return plain xml documents
		else {
			
			//	open results
			out.write("<" + RESULTS_NODE_NAME + ">");
			out.newLine();
			
			//	write documents
			while (result.hasNextElement()) {
				DocumentResultElement dre = result.getNextDocumentResultElement();
				
				//	result meta information only
				if (dre.document == null) {
					
					//	write result meta information
					out.write("<" + RESULT_NODE_NAME + " " + RELEVANCE_ATTRIBUTE + "=\"" + dre.relevance + "\" " + DOCUMENT_ID_ATTRIBUTE + "=\"" + dre.documentId + "\"/>");
					out.newLine();
				}
				
				//	document given
				else {
					
					//	open result, including relevance of document and document ID
					out.write("<" + RESULT_NODE_NAME + " " + RELEVANCE_ATTRIBUTE + "=\"" + dre.relevance + "\" " + DOCUMENT_ID_ATTRIBUTE + "=\"" + dre.documentId + "\">");
					out.newLine();
					
					//	write document (plain or transformed)
					Writer w = XsltUtils.wrap(new IsolatorWriter(out), xslTransformer);
					AnnotationUtils.writeXML(dre.document, w, null, new HashSet() {
						public boolean contains(Object o) {
							if (BOXED_ATTRIBUTE.equals(o)) return false;
							else if (BOX_TITLE_ATTRIBUTE.equals(o)) return false;
							else if (BOX_PART_LABEL_ATTRIBUTE.equals(o)) return false;
							else if (RESULT_INDEX_FIELDS_ATTRIBUTE.equals(o)) return false;
							else if (RESULT_INDEX_LABEL_ATTRIBUTE.equals(o)) return false;
							else if (RESULT_INDEX_NAME_ATTRIBUTE.equals(o)) return false;
							else return true;
						}
					}, true);
					w.flush();
					w.close();
					
					//	close result
					out.write("</" + RESULT_NODE_NAME + ">");
					out.newLine();
				}
			}
			
			//	close results
			out.write("</" + RESULTS_NODE_NAME + ">");
			out.newLine();
		}
	}
	
	private void doHtmlRequest(HttpServletRequest request, HttpServletResponse response, String searchMode, Properties fieldValues, Properties searchParameters, String indexName, String subIndexName, String subResultMinSize) throws IOException {
		
		//	check cache mode
		boolean allowCache = !FORCE_CACHE.equals(request.getParameter(CACHE_CONTROL_PARAMETER));
		
		//	get search fields
		SearchFieldGroup[] fieldGroups;
		Exception fieldGroupException;
		try {
			fieldGroups = this.srsClient.getSearchFieldGroups(allowCache);
			fieldGroupException = null;
		}
		catch (IOException e) {
			fieldGroups = null;
			fieldGroupException = e;
		}
		
		//	create page builder
		HtmlPageBuilder tr;
		
		//	for statistics, use fix values
		if (STATISTICS_MODE.equals(searchMode)) {
			this.statistics = new BufferedCollectionStatistics(this.srsClient.getStatistics(allowCache));
			tr = new HtmlSearchResultWriter(request, response, (this.includeSearchFormWithResult ? fieldGroups : null), (this.includeSearchFormWithResult ? fieldGroupException : null), fieldValues, this.statistics, "Collection Statistics");
		}
		
		//	get document ID if single document to display
		else if (HTML_DOCUMENT_SEARCH_MODE.equals(searchMode)) {
			
			//	read invokation path path info (alternative to idQuery parameter in this mode)
			if (!searchParameters.containsKey(ID_QUERY_FIELD_NAME)) {
				String pathIdQuery = request.getPathInfo();
				
				//	got document ID in path
				if ((pathIdQuery != null)) {
					
					//	cut leading slash
					if (pathIdQuery.startsWith("/"))
						pathIdQuery = pathIdQuery.substring(1);
					
					//	cut file extension
					if (pathIdQuery.indexOf('.') != -1)
						pathIdQuery = pathIdQuery.substring(0, pathIdQuery.indexOf('.'));
					
					//	set search parameter
					searchParameters.setProperty(ID_QUERY_FIELD_NAME, pathIdQuery);
				}
				
				//	finally, check query string
				else {
					String requestQueryString = request.getQueryString();
					
					//	got document ID in query position
					if ((requestQueryString != null) && (requestQueryString.length() != 0) && (requestQueryString.indexOf('=') == -1)) {
						
						//	cut file extension
						if (requestQueryString.indexOf('.') != -1)
							requestQueryString = requestQueryString.substring(0, requestQueryString.indexOf('.'));
						
						//	set search parameter
						searchParameters.setProperty(ID_QUERY_FIELD_NAME, requestQueryString);
					}
				}
			}
			
			//	get document
			DocumentResult docRes = this.srsClient.searchDocuments(searchParameters, true, allowCache);
			
			//	got document
			if (docRes.hasNextElement()) {
				DocumentResultElement dre = docRes.getNextDocumentResultElement();
				String title = ((String) dre.document.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, ""));
				if (dre.document.hasAttribute(DOCUMENT_TITLE_ATTRIBUTE)) {
					String dreTitle = ((String) dre.document.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, ""));
					if ((dreTitle != null) && !dreTitle.equals(title))
						title = (dreTitle + " - " + title);
				}
				tr = new HtmlSearchResultWriter(request, response, (this.includeSearchFormWithResult ? fieldGroups : null), (this.includeSearchFormWithResult ? fieldGroupException : null), fieldValues, dre, title);
			}
			
			//	no document found
			else tr = new HtmlSearchResultWriter(request, response, fieldGroups, fieldGroupException, fieldValues, new DocumentResultElement(0, searchParameters.getProperty(ID_QUERY_FIELD_NAME), 0.0, null), "Document Not Found");
		}
		
		//	assemble values dynamically for index or document search requests
		else {
			
			//	get search fields now in order to decide which field values to display in title
			SearchFieldGroup[] sfg = ((fieldGroups == null) ? new SearchFieldGroup[0] : fieldGroups);
			
			//	extract prefixes from field groups
			HashSet titleIndexNames = new HashSet();
			for (int g = 0; g < sfg.length; g++) {
				SearchField[] fields = sfg[g].getFields();
				
				//	check if field group is eligible for inclusion in title (at least one text field filled in)
				for (int f = 0; f < fields.length; f++) {
					if (SearchField.TEXT_TYPE.equals(fields[f].type)) {
						String queryFieldValue = fieldValues.getProperty(fields[f].name);
						
						//	check if value in query, and is not the preset one
						if ((queryFieldValue != null) && (queryFieldValue.length() != 0) && !queryFieldValue.equals(fields[f].value))
							titleIndexNames.add(sfg[g].indexName);
					}
				}
			}
			
			//	no query, display forms
			if (titleIndexNames.isEmpty()) {
				if (THESAURUS_SEARCH_MODE.equals(searchMode))
					tr = new HtmlSearchResultWriter(request, response, fieldGroups, fieldGroupException, fieldValues, ((ThesaurusResult) null), "");
				else tr = new HtmlSearchResultWriter(request, response, fieldGroups, fieldGroupException, fieldValues, ((BufferedDocumentResult) null), "");
			}
			
			//	do search
			else {
				
				//	collect actual field values (labels for boolean fields, and option labels for select fields)
				StringBuffer titleBuilder = new StringBuffer();
				for (int g = 0; g < sfg.length; g++) {
					if (titleIndexNames.contains(sfg[g].indexName)) {
						SearchField[] fields = sfg[g].getFields();
						StringVector values = new StringVector();
						
						//	get field values from query
						for (int f = 0; f < fields.length; f++) {
							String qfv = fieldValues.getProperty(fields[f].name);
							if ((qfv != null) && (qfv.length() != 0)) {
								
								//	text field, use actual entry
								if (SearchField.TEXT_TYPE.equals(fields[f].type))
									values.addElement(qfv);
								
								//	boolean field, use label
								else if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
									values.addElement(fields[f].label);
								
								//	select field, use label of selected option
								else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
									SearchFieldOption[] sfo = fields[f].getOptions();
									for (int o = 0; o < sfo.length; o++) {
										if (qfv.equals(sfo[o].value))
											values.addElement(sfo[o].label);
									}
								}
							}
						}
						
						//	add values to title
						if (values.size() != 0) {
							titleBuilder.append((titleBuilder.length() == 0) ? "" : " / ");
							titleBuilder.append(values.concatStrings(", "));
						}
					}
				}
				
				//	store collected values
				String pageTitleExtension = titleBuilder.toString();
				
				//	regular document search
				if (DOCUMENT_SEARCH_MODE.equals(searchMode)) {
					
					DocumentResult docRes;
					String docSearchMode = this.layout.getResultListSearchMode();
					
					if (SEARCH_DOCUMENTS.equals(docSearchMode))
						docRes = this.srsClient.searchDocuments(searchParameters, true, allowCache);
					
					else if (SEARCH_DOCUMENT_DETAILS.equals(docSearchMode))
						docRes = this.srsClient.searchDocumentDetails(searchParameters, true, allowCache);
					
					else if (SEARCH_INDEX.equals(docSearchMode)) {
						String subIndexNames = "";
						for (int f = 0; f < fieldGroups.length; f++)
							subIndexNames += (((f == 0) ? "" : "\n") + fieldGroups[f].indexName);
						if (subIndexNames.length() != 0)
							searchParameters.setProperty(SUB_INDEX_NAME, subIndexNames);
						docRes = this.srsClient.searchDocumentData(searchParameters, true, allowCache);
					}
					
					else docRes = this.srsClient.searchDocumentData(searchParameters, true, allowCache);
					
					//	switch to single document mode if only one document in result
					BufferedDocumentResult bufDocRes = new BufferedDocumentResult(docRes);
					DocumentResult docSearchRes = bufDocRes.getDocumentResult();
					DocumentResultElement soleResDre = null;
					while (docSearchRes.hasNextElement()) {
						DocumentResultElement loopDre = docSearchRes.getNextDocumentResultElement();
						if (soleResDre == null)
							soleResDre = loopDre;
						else {
							soleResDre = null;
							break;
						}
					}
					if (soleResDre != null) {
						response.sendRedirect(request.getContextPath() + "/html/" + soleResDre.documentId);
						return;
					}
					
					boolean includeSearchForms = ((this.includeSearchFormWithResult || bufDocRes.isEmpty()) && (fieldGroups.length != 0));
					tr = new HtmlSearchResultWriter(request, response, (includeSearchForms ? fieldGroups : null), (includeSearchForms ? fieldGroupException : null), fieldValues, bufDocRes, pageTitleExtension);
				}
				
				//	index search
				else if (INDEX_SEARCH_MODE.equals(searchMode)) {
					searchParameters.setProperty(INDEX_NAME, indexName);
					if (subIndexName != null)
						searchParameters.setProperty(SUB_INDEX_NAME, subIndexName);
					if (subResultMinSize != null)
						searchParameters.setProperty(SUB_RESULT_MIN_SIZE, subResultMinSize);
					IndexResult indexRes = this.srsClient.searchIndex(searchParameters, true, allowCache);
					
					boolean includeSearchForms = ((this.includeSearchFormWithResult || !indexRes.hasNextElement()) && (fieldGroups.length != 0));
					tr = new HtmlSearchResultWriter(request, response, (includeSearchForms ? fieldGroups : null), (includeSearchForms ? fieldGroupException : null), fieldValues, indexRes, pageTitleExtension);
				}
				
				//	thesaurus search
				else if (THESAURUS_SEARCH_MODE.equals(searchMode)) {
					if (indexName != null)
						searchParameters.setProperty(INDEX_NAME, indexName);
					ThesaurusResult thesaurusRes = this.srsClient.searchThesaurus(searchParameters, allowCache);
					
					boolean includeSearchForms = ((this.includeSearchFormWithResult || !thesaurusRes.hasNextElement()) && (fieldGroups.length != 0));
					tr = new HtmlSearchResultWriter(request, response, (includeSearchForms ? fieldGroups : null), (includeSearchForms ? fieldGroupException : null), fieldValues, thesaurusRes, pageTitleExtension);
				}
				
				//	display document search form by default
				else tr = new HtmlSearchResultWriter(request, response, fieldGroups, fieldGroupException, fieldValues, ((BufferedDocumentResult) null), pageTitleExtension);
			}
		}
		
		//	stream portal page frame through token receiver
		FileInputStream fis = new FileInputStream(this.findFile("portal.html"));
		try {
			htmlParser.stream(fis, tr);
		}
		catch (IOException ioe) {
			ioe.printStackTrace(System.out);
			throw ioe;
		}
		catch (final Exception e) {
			e.printStackTrace(System.out);
			throw new IOException() {
				public Throwable getCause() {
					return e.getCause();
				}
				public String getMessage() {
					return e.getMessage();
				}
				public String getLocalizedMessage() {
					return e.getLocalizedMessage();
				}
				public StackTraceElement[] getStackTrace() {
					return e.getStackTrace();
				}
				public void printStackTrace() {
					e.printStackTrace();
				}
				public void printStackTrace(PrintStream s) {
					e.printStackTrace(s);
				}
				public void printStackTrace(PrintWriter s) {
					e.printStackTrace(s);
				}
				public String toString() {
					return e.toString();
				}
			};
		}
		finally {
			tr.close();
			fis.close();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#getCssStylesheets()
	 */
	public String[] getCssStylesheets() {
		return this.cssStylesheetsToInlcude;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#getJavaScriptFiles()
	 */
	public String[] getJavaScriptFiles() {
		return ((String[]) this.javaScriptsToInclude.toArray(new String[this.javaScriptsToInclude.size()]));
	}

	private class HtmlSearchResultWriter extends HtmlPageBuilder {
		
		//	request related information and data
		private String searchMode;
		
		//	search form data
		private SearchFieldGroup[] fieldGroups;
		private Exception fieldGroupException;
		private Properties fieldValues;
		
		//	search result data
		private BufferedDocumentResult documentResult;
		private DocumentResultElement document;
		private BufferedIndexResult indexResult;
		private BufferedThesaurusResult thesaurusResult;
		private BufferedCollectionStatistics statistics;
		
		//	result page title
		private String pageTitleExtension;
		
		//	document search result
		private HtmlSearchResultWriter(HttpServletRequest request, HttpServletResponse response, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, BufferedDocumentResult documentResult, String pageTitleExtension) throws IOException {
			super(SearchPortalServlet.this, request, response);
			this.searchMode = DOCUMENT_SEARCH_MODE;
			this.fieldGroups = fieldGroups;
			this.fieldGroupException = fieldGroupException;
			this.fieldValues = fieldValues;
			this.documentResult = documentResult;
			this.document = null;
			this.indexResult = null;
			this.thesaurusResult = null;
			this.statistics = null;
			this.pageTitleExtension = pageTitleExtension;
		}
		
		//	display document
		private HtmlSearchResultWriter(HttpServletRequest request, HttpServletResponse response, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, DocumentResultElement document, String pageTitleExtension) throws IOException {
			super(SearchPortalServlet.this, request, response);
			this.searchMode = HTML_DOCUMENT_SEARCH_MODE;
			this.fieldGroups = fieldGroups;
			this.fieldGroupException = fieldGroupException;
			this.fieldValues = fieldValues;
			this.documentResult = null;
			this.document = document;
			this.indexResult = null;
			this.thesaurusResult = null;
			this.statistics = null;
			this.pageTitleExtension = pageTitleExtension;
		}
		
		//	index result
		private HtmlSearchResultWriter(HttpServletRequest request, HttpServletResponse response, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, IndexResult indexResult, String pageTitleExtension) throws IOException {
			super(SearchPortalServlet.this, request, response);
			this.searchMode = INDEX_SEARCH_MODE;
			this.fieldGroups = fieldGroups;
			this.fieldGroupException = fieldGroupException;
			this.fieldValues = fieldValues;
			this.documentResult = null;
			this.document = null;
			this.indexResult = ((indexResult == null) ? null : new BufferedIndexResult(indexResult));
			this.thesaurusResult = null;
			this.statistics = null;
			this.pageTitleExtension = pageTitleExtension;
		}
		
		//	thesaurus result
		private HtmlSearchResultWriter(HttpServletRequest request, HttpServletResponse response, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, ThesaurusResult thesaurusResult, String pageTitleExtension) throws IOException {
			super(SearchPortalServlet.this, request, response);
			this.searchMode = THESAURUS_SEARCH_MODE;
			this.fieldGroups = fieldGroups;
			this.fieldGroupException = fieldGroupException;
			this.fieldValues = fieldValues;
			this.documentResult = null;
			this.document = null;
			this.indexResult = null;
			this.thesaurusResult = ((thesaurusResult == null) ? null : new BufferedThesaurusResult(thesaurusResult));
			this.statistics = null;
			this.pageTitleExtension = pageTitleExtension;
		}
		
		//	statistics
		private HtmlSearchResultWriter(HttpServletRequest request, HttpServletResponse response, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, BufferedCollectionStatistics statistics, String pageTitleExtension) throws IOException {
			super(SearchPortalServlet.this, request, response);
			this.searchMode = STATISTICS_MODE;
			this.fieldGroups = fieldGroups;
			this.fieldGroupException = fieldGroupException;
			this.fieldValues = fieldValues;
			this.documentResult = null;
			this.document = null;
			this.indexResult = null;
			this.thesaurusResult = null;
			this.statistics = statistics;
			this.pageTitleExtension = pageTitleExtension;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder#getPageTitle(java.lang.String)
		 */
		protected String getPageTitle(String title) {
			return super.getPageTitle(((this.pageTitleExtension.length() == 0) ? "" : (this.pageTitleExtension + " - ")) + title);
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder#include(java.lang.String, java.lang.String)
		 */
		protected void include(String type, String tag) throws IOException {
			if ("includeNavigation".equals(type))
				this.includeNavigation();
			else if ("includeDocStat".equals(type))
				this.includeStatistics();
			else if ("includeSearchForm".equals(type))
				this.includeSearchForms();
			else if ("includeResultIndex".equals(type))
				this.includeResultIndex();
			else if ("includeResults".equals(type))
				this.includeResults();
			else super.include(type, tag);
		}
		
		protected void writePageHeadExtensions() throws IOException {
			for (int l = 0; l < resultLinkers.length; l++)
				resultLinkers[l].writePageHeadExtensions(this);
			
			if ((this.document != null) && (this.document.document != null)) {
				for (int l = 0; l < resultLinkers.length; l++)
					resultLinkers[l].writePageHeadExtensions(this.document.document, this);
			}
			
			//	TODO add multi-document extensions later (not used in XSLT based layout anyway)
		}
		
		protected String[] getOnloadCalls() {
			return ((String[]) functionsToCallOnLoad.toArray(new String[functionsToCallOnLoad.size()]));
		}
		
		protected String[] getOnunloadCalls() {
			return ((String[]) functionsToCallOnUnload.toArray(new String[functionsToCallOnUnload.size()]));
		}
		
		private void includeNavigation() throws IOException {
			if ((this.fieldGroups == null) && (this.fieldGroupException == null)) {
				try {
					if (THESAURUS_SEARCH_MODE.equals(this.searchMode))
						layout.includeNavigationLinks(((NavigationLink[]) thesaurusResultLinks.toArray(new NavigationLink[thesaurusResultLinks.size()])), this);
					else layout.includeNavigationLinks(((NavigationLink[]) searchResultLinks.toArray(new NavigationLink[searchResultLinks.size()])), this);
				}
				catch (Exception e) {
					this.writeExceptionAsXmlComment(("exception including navigation"), e);
				}
			}
		}
		
		private void includeStatistics() throws IOException {
			try {
				layout.includeStatisticsLine(SearchPortalServlet.this.statistics, ((NavigationLink[]) statisticsLineLinks.toArray(new NavigationLink[statisticsLineLinks.size()])), this);
			}
			catch (Exception e) {
				this.writeExceptionAsXmlComment(("exception including statistics line"), e);
			}
		}
		
		private void includeSearchForms() throws IOException {
			
			//	no search field goups available
			if (this.fieldGroups == null) {
				
				//	search field groups were meant to be included, but an exception occured fetching them 
				if (this.fieldGroupException != null)
					this.writeExceptionAsXmlComment("exception fetching forms", this.fieldGroupException);
			}
			
			//	thesaurus search forms
			else if (THESAURUS_SEARCH_MODE.equals(this.searchMode)) {
				try {
					layout.includeThesaurusForms(thesaurusFormTitle, this.fieldGroups, null, this.fieldValues, this);
				}
				catch (Exception e) {
					this.writeExceptionAsXmlComment(("exception including thesaurus forms"), e);
				}
			}
			
			//	document search forms
			else {
				try {
					SearchFieldRow buttonRowFields = getSearchFormExtensions(this.fieldGroups);
					layout.includeSearchForm(searchFormTitle, this.fieldGroups, buttonRowFields, this.fieldValues, this);
				}
				catch (Exception e) {
					this.writeExceptionAsXmlComment(("exception including search form"), e);
				}
			}
		}
		
		private void includeResultIndex() throws IOException {
			try {
				BufferedThesaurusResult[] ri = getResultIndices(this.documentResult, this.document);
				if (ri.length != 0) layout.includeResultIndex(ri, this);
			}
			catch (Exception e) {
				this.writeExceptionAsXmlComment(("exception including result index"), e);
			}
		}
		
		private void includeResults() throws IOException {
			
			//	collection statistics
			if (this.statistics != null) {
				try {
					layout.includeStatistics(this.statistics, this);
				}
				catch (Exception e) {
					this.writeExceptionAsXmlComment(("exception including statistics"), e);
				}
				
			}
			
			//	thesaurus search result
			else if (this.thesaurusResult != null) {
				this.thesaurusResult.sort();
				try {
					layout.includeThesaurusResult(this.thesaurusResult, this);
				}
				catch (Exception e) {
					this.writeExceptionAsXmlComment(("exception including thesaurus search result"), e);
				}
			}
			
			//	index search result
			else if (this.indexResult != null) {
				try {
					layout.includeIndexResult(this.indexResult, this);
				}
				catch (Exception e) {
					this.writeExceptionAsXmlComment(("exception including index result"), e);
				}
			}
			
			//	single document to display
			else if (this.document != null) {
				try {
					if (this.document.document == null) {
						this.writeLine("<!-- exception including display document: no document to display for ID '" + this.document.documentId + "' -->");
					}
					else {
						String[] attributeNames = this.document.getAttributeNames();
						for (int a = 0; a < attributeNames.length; a++)
							if (this.document.document.getDocumentProperty(attributeNames[a]) == null)
								this.document.document.setDocumentProperty(attributeNames[a], ((String) this.document.getAttribute(attributeNames[a])));
						layout.includeResultDocument(this.document, this);
					}
				}
				catch (Exception e) {
					this.writeExceptionAsXmlComment(("exception including display document"), e);
				}
			}
			
			//	document search result
			else if (this.documentResult != null) {
				try {
					
					//	make sure result is fully loaded (will happen in layout engine anyway)
					for (DocumentResult dr = this.documentResult.getDocumentResult(); dr.hasNextElement();)
						dr.getNextDocumentResultElement();
					
					//	check result pivot
					int resultPivot = 20;
					String resultPivotString = this.request.getParameter(RESULT_PIVOT_INDEX_PARAMETER);
					if (resultPivotString != null) try {
						resultPivot = Integer.parseInt(resultPivotString);
					} catch (NumberFormatException nfe) {}
					
					//	generate link if actual number of results equal to or higher than current cutoff 
					String moreResultsLink;
					if (this.documentResult.size() < resultPivot)
						moreResultsLink = null;
					else {
						int nextResultPivot = resultPivot;
						while (nextResultPivot < this.documentResult.size())
							nextResultPivot *= 5;
						moreResultsLink = this.request.getQueryString();
						if (moreResultsLink.indexOf(RESULT_PIVOT_INDEX_PARAMETER) == -1)
							moreResultsLink += ("&" + RESULT_PIVOT_INDEX_PARAMETER + "=" + nextResultPivot);
						else moreResultsLink = moreResultsLink.replaceAll((RESULT_PIVOT_INDEX_PARAMETER + "[^&]++"), (RESULT_PIVOT_INDEX_PARAMETER + "=" + nextResultPivot));
					}
					layout.includeResultList(this.documentResult, this, moreResultsLink);
				}
				catch (Exception e) {
					this.writeExceptionAsXmlComment(("exception including document list"), e);
				}
			}
		}
		
		private BufferedThesaurusResult[] getResultIndices(BufferedDocumentResult result, DocumentResultElement document) throws IOException {
			
			//	check parameters
			if ((result == null) && ((document == null) || ((document.document == null)))) return new BufferedThesaurusResult[0];
			
			//	build data structures
			HashMap indicesByName = new LinkedHashMap();
			
			//	collect data
			if (result != null)
				for (DocumentResult dr = result.getDocumentResult(); dr.hasNextElement();)
					this.fillThesaurusResultDataStructures(dr.getNextDocumentResultElement().document, indicesByName);
			
			else if (document != null)
				this.fillThesaurusResultDataStructures(document.document, indicesByName);
			
			//	eliminate duplicate entries
			ArrayList indexList = new ArrayList();
			for (Iterator riit = indicesByName.values().iterator(); riit.hasNext();) {
				BufferedThesaurusResult ri = ((ResultIndexBuilder) riit.next()).getBufferedThesaurusResult();
				ri.sort(ri.resultAttributes);
				indexList.add(ri);
			}
			
			//	assemble and return result
			return ((BufferedThesaurusResult[]) indexList.toArray(new BufferedThesaurusResult[indexList.size()]));
		}
		
		private void fillThesaurusResultDataStructures(MutableAnnotation doc, HashMap indicesByName) {
			
			//	get index fields from doc
			if (doc.hasAttribute(RESULT_INDEX_NAME_ATTRIBUTE)) {
				String indexName = ((String) doc.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE, ""));
				String indexLabel = ((String) doc.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, ""));
				
				String indexFieldNameString = ((String) doc.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""));
				StringVector indexFieldNames = new StringVector();
				indexFieldNames.parseAndAddElements(indexFieldNameString, " ");
				indexFieldNames.removeAll("");
				
				ResultIndexBuilder rib = ((ResultIndexBuilder) indicesByName.get(indexName));
				if (rib == null) {
					rib = new ResultIndexBuilder(indexFieldNames.toStringArray(), indexName, indexLabel);
					indicesByName.put(indexName, rib);
				}
				rib.addElement(this.produceResultIndexEntry(doc));
			}
			
			//	get index fields from annotations
			Annotation[] annotations = doc.getAnnotations();
			for (int a = 0; a < annotations.length; a++) {
				if (annotations[a].hasAttribute(RESULT_INDEX_NAME_ATTRIBUTE)) {
					String indexName = ((String) doc.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE, ""));
					String indexLabel = ((String) doc.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, ""));
					
					String indexFieldNameString = ((String) doc.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""));
					StringVector indexFieldNames = new StringVector();
					indexFieldNames.parseAndAddElements(indexFieldNameString, " ");
					indexFieldNames.removeAll("");
					
					ResultIndexBuilder rib = ((ResultIndexBuilder) indicesByName.get(indexName));
					if (rib == null) {
						rib = new ResultIndexBuilder(indexFieldNames.toStringArray(), indexName, indexLabel);
						indicesByName.put(indexName, rib);
					}
					rib.addElement(this.produceResultIndexEntry(annotations[a]));
				}
			}
		}
		
		private ThesaurusResultElement produceResultIndexEntry(Annotation data) {
			ThesaurusResultElement tre = new ThesaurusResultElement();
			tre.copyAttributes(data);
			return tre;
		}
		
		private class ResultIndexBuilder {
			
			final String[] resultAttributes;
			final String thesaurusEntryType;
			final String thesaurusName;
			
			private LinkedList elements = new LinkedList();
			private HashSet elementKeys = new HashSet();
			
			ResultIndexBuilder(String[] resultAttributes, String thesaurusEntryType, String thesaurusName) {
				this.resultAttributes = resultAttributes;
				this.thesaurusEntryType = thesaurusEntryType;
				this.thesaurusName = thesaurusName;
			}
			
			void addElement(ThesaurusResultElement tre) {
				if (this.elementKeys.add(this.produceSortKey(tre)))
					this.elements.addLast(tre);
			}
			
			
			private String produceSortKey(ThesaurusResultElement tre) {
				String sortKey = "";
				for (int f = 0; f < this.resultAttributes.length; f++)
					sortKey += ("   " + tre.getAttribute(this.resultAttributes[f]));
				return sortKey.trim().toLowerCase();
			}
			
			BufferedThesaurusResult getBufferedThesaurusResult() {
				return new BufferedThesaurusResult(new ThesaurusResult(this.resultAttributes, this.thesaurusEntryType, this.thesaurusName) {
					public boolean hasNextElement() {
						return (elements.size() != 0);
					}
					public SrsSearchResultElement getNextElement() {
						return (this.hasNextElement() ? ((ThesaurusResultElement) elements.removeFirst()) : null);
					}
				});
			}
		}
	}
//	private class HtmlSearchResultWriter extends TokenReceiver {
//		
//		//	local status information
//		private boolean inHyperLink = false;
//		private boolean lastTagWasStart = false;
//		private String carrySpace = "";
//		
//		//	output writer
//		private BufferedWriter out;
//		
//		//	request related information and data
//		private HttpServletRequest request;
//		private String searchMode;
//		
//		//	search form data
//		private SearchFieldGroup[] fieldGroups;
//		private Exception fieldGroupException;
//		private Properties fieldValues;
//		
//		//	search result data
//		private BufferedDocumentResult documentResult;
//		private DocumentResultElement document;
//		private BufferedIndexResult indexResult;
//		private BufferedThesaurusResult thesaurusResult;
//		private BufferedCollectionStatistics statistics;
//		
//		//	result page title
//		private String pageTitleExtension;
//		
//		//	document search result
//		private HtmlSearchResultWriter(BufferedWriter out, HttpServletRequest request, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, DocumentResult documentResult, String pageTitleExtension) throws IOException {
//			this.out = out;
//			this.request = request;
//			this.searchMode = DOCUMENT_SEARCH_MODE;
//			this.fieldGroups = fieldGroups;
//			this.fieldGroupException = fieldGroupException;
//			this.fieldValues = fieldValues;
//			this.documentResult = ((documentResult == null) ? null : new BufferedDocumentResult(documentResult));
//			this.document = null;
//			this.indexResult = null;
//			this.thesaurusResult = null;
//			this.statistics = null;
//			this.pageTitleExtension = pageTitleExtension;
//			
//			//	switch to single document mode if only one document in result
//			if (this.documentResult != null) {
//				DocumentResultElement resDre = null;
//				DocumentResult docSearchRes = this.documentResult.getDocumentResult();
//				while (docSearchRes.hasNextElement()) {
//					DocumentResultElement loopDre = docSearchRes.getNextDocumentResultElement();
//					if (resDre == null)
//						resDre = loopDre;
//					else {
//						resDre = null;
//						break;
//					}
//				}
//				if (resDre != null) {
//					Properties docSearchParams = new Properties();
//					docSearchParams.setProperty(ID_QUERY_FIELD_NAME, resDre.documentId);
//					DocumentResult docRes = srsClient.searchDocuments(docSearchParams, true);
//					
//					//	got document
//					if (docRes.hasNextElement()) {
//						DocumentResultElement dre = docRes.getNextDocumentResultElement();
//						String title = ((String) dre.document.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, ""));
//						if (dre.document.hasAttribute(DOCUMENT_TITLE_ATTRIBUTE)) {
//							String dreTitle = ((String) dre.document.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, ""));
//							if ((dreTitle != null) && !dreTitle.equals(title))
//								title = (dreTitle + " - " + title);
//						}
//						this.searchMode = HTML_DOCUMENT_SEARCH_MODE;
//						this.pageTitleExtension = title;
//						this.document = dre;
//						this.documentResult = null;
//					}
//				}
//			}
//		}
//		
//		//	display document
//		private HtmlSearchResultWriter(BufferedWriter out, HttpServletRequest request, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, DocumentResultElement document, String pageTitleExtension) throws IOException {
//			this.out = out;
//			this.request = request;
//			this.searchMode = HTML_DOCUMENT_SEARCH_MODE;
//			this.fieldGroups = fieldGroups;
//			this.fieldGroupException = fieldGroupException;
//			this.fieldValues = fieldValues;
//			this.documentResult = null;
//			this.document = document;
//			this.indexResult = null;
//			this.thesaurusResult = null;
//			this.statistics = null;
//			this.pageTitleExtension = pageTitleExtension;
//		}
//		
//		//	index result
//		private HtmlSearchResultWriter(BufferedWriter out, HttpServletRequest request, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, IndexResult indexResult, String pageTitleExtension) throws IOException {
//			this.out = out;
//			this.request = request;
//			this.searchMode = INDEX_SEARCH_MODE;
//			this.fieldGroups = fieldGroups;
//			this.fieldGroupException = fieldGroupException;
//			this.fieldValues = fieldValues;
//			this.documentResult = null;
//			this.document = null;
//			this.indexResult = ((indexResult == null) ? null : new BufferedIndexResult(indexResult));
//			this.thesaurusResult = null;
//			this.statistics = null;
//			this.pageTitleExtension = pageTitleExtension;
//		}
//		
//		//	thesaurus result
//		private HtmlSearchResultWriter(BufferedWriter out, HttpServletRequest request, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, ThesaurusResult thesaurusResult, String pageTitleExtension) throws IOException {
//			this.out = out;
//			this.request = request;
//			this.searchMode = THESAURUS_SEARCH_MODE;
//			this.fieldGroups = fieldGroups;
//			this.fieldGroupException = fieldGroupException;
//			this.fieldValues = fieldValues;
//			this.documentResult = null;
//			this.document = null;
//			this.indexResult = null;
//			this.thesaurusResult = ((thesaurusResult == null) ? null : new BufferedThesaurusResult(thesaurusResult));
//			this.statistics = null;
//			this.pageTitleExtension = pageTitleExtension;
//		}
//		
//		//	statistics
//		private HtmlSearchResultWriter(BufferedWriter out, HttpServletRequest request, SearchFieldGroup[] fieldGroups, Exception fieldGroupException, Properties fieldValues, BufferedCollectionStatistics statistics, String pageTitleExtension) throws IOException {
//			this.out = out;
//			this.request = request;
//			this.searchMode = STATISTICS_MODE;
//			this.fieldGroups = fieldGroups;
//			this.fieldGroupException = fieldGroupException;
//			this.fieldValues = fieldValues;
//			this.documentResult = null;
//			this.document = null;
//			this.indexResult = null;
//			this.thesaurusResult = null;
//			this.statistics = statistics;
//			this.pageTitleExtension = pageTitleExtension;
//		}
//		
//		public void close() throws IOException {
//			//	flushing and closing the writer is done in the surrounding code
//		}
//		
//		public void storeToken(String token, int treeDepth) throws IOException {
//			if (html.isTag(token)) {
//				String type = html.getType(token);
//				
//				//	check include tags
//				if ("includeFile".equals(type))
//					this.includeFile(token, this.out);
//				
//				else if ("includeNavigation".equals(type))
//					this.includeNavigation(token, this.out);
//				
//				else if ("includeDocStat".equals(type))
//					this.includeStatistics(token, this.out);
//				
//				else if ("includeSearchForm".equals(type))
//					this.includeSearchForms(token, this.out);
//				
//				else if ("includeResultIndex".equals(type))
//					this.includeResultIndex(token, this.out);
//				
//				else if ("includeResults".equals(type))
//					this.includeResults(token, this.out);
//				
//				//	page title
//				else if ("title".equalsIgnoreCase(type) && !html.isEndTag(token)) {
//					
//					//	open page title
//					this.out.write(token);
//					this.out.newLine();
//					
//					//	add query specific page title extension
//					if (pageTitleExtension.length() != 0)
//						this.out.write(pageTitleExtension + " - ");
//				}
//				
//				//	page head
//				else if ("head".equalsIgnoreCase(type) && html.isEndTag(token)) {
//					
//					//	write extensions to page head
//					this.writePageHeadExtensions(this.out);
//					
//					//	close page head
//					this.out.write(token);
//					this.out.newLine();
//				}
//				
//				//	start of page body
//				else if ("body".equalsIgnoreCase(type) && !html.isEndTag(token)) {
//					
//					//	include calls to doOnloadCalls() and doOnunloadCalls() functions
//					this.out.write("<body onload=\"doOnloadCalls();\" onunload=\"doOnunloadCalls();\">");
//					this.out.newLine();
//				}
//				
//				//	actual body HTML tag
//				else {
//					
//					//	insert carried space before start tag
//					if ((this.carrySpace.length() != 0) && !html.isEndTag(token)) {
//						this.out.write(this.carrySpace);
//						this.carrySpace = "";
//					}
//					
//					//	image, make link absolute
//					if ("img".equalsIgnoreCase(type)) {
//						
//						//	check for link
//						if (token.indexOf("src=\"") != -1) {
//							
//							//	check if link absolute
//							if (token.indexOf("src=\"http://") == -1)
//								token = token.replaceAll("src\\=\\\"(\\.\\/)?", ("src=\"" + this.request.getContextPath() + "/"));
//						}
//					}
//					
//					//	other token
//					else {
//						
//						//	make href absolute
//						if ("a".equalsIgnoreCase(type)) {
//							if (token.indexOf("href=\"./") != -1)
//								token = token.replaceAll("href\\=\\\"\\.\\/", ("href=\"" + this.request.getContextPath() + "/"));
//							else if (token.indexOf("href=\"//") != -1)
//								token = token.replaceAll("href\\=\\\"\\/\\/", ("href=\"" + "/"));
//							else if (token.indexOf("href=\"/") != -1)
//								token = token.replaceAll("href\\=\\\"\\/", ("href=\"" + this.request.getContextPath() + "/"));
//							else if (token.indexOf("href=\"") != -1) {
//								if ((token.indexOf("href=\"http://") == -1) && (token.indexOf("href=\"" + this.request.getProtocol() + "://") == -1))
//									token = token.replaceAll("href\\=\\\"", ("href=\"" + this.request.getContextPath() + "/"));
//							}
//						}
//						
//						//	remember being in hyperlink (for auto-activation)
//						if ("a".equals(type))
//							this.inHyperLink = !html.isEndTag(token);
//					}
//					
//					this.out.write(token);
//				}
//				
//				this.lastTagWasStart = (!token.startsWith("</") && !token.endsWith("/>") && !html.isSingularTagType(type));
//			}
//			
//			//	textual content
//			else {
//				//	remove spaces from links, and activate them
//				if (token.matches("(http|ftp)\\:\\s*\\/\\/.++")) {
//					String link = token.replaceAll("\\s", "");
//					if (!this.inHyperLink) this.out.write("<a mark=\"autoGenerated\" href=\"" + link + "\">");
//					this.out.write(link);
//					if (!this.inHyperLink) this.out.write("</a>");
//				}
//				
//				//	keep comments and DTDs as they are
//				else if (html.isComment(token) || html.isDTD(token)) {
//					this.out.write(token);
//				}
//				
//				//	normalize text strings
//				else {
//					if (token.trim().length() == 0)
//						this.carrySpace = "";
//					else {
//						MutableTokenSequence mts = Gamta.newTokenSequence(token, null);
//						for (int t = 0; t < (mts.size()-1); t++) {
//							if (!Gamta.insertSpace(mts.valueAt(t), mts.valueAt(t+1)))
//								mts.setWhitespaceAfter("", t);
//						}
//						String leadingWhitespace = (((token.charAt(0) < 33) && !this.lastTagWasStart && Gamta.spaceBefore(mts.firstValue())) ? " " : "");
//						this.carrySpace = (((token.charAt(token.length()-1) < 33) && Gamta.spaceAfter(mts.lastValue())) ? " " : "");
//						token = leadingWhitespace + TokenSequenceUtils.concatTokens(mts, false, true);
//					}
//					this.out.write(token);
//				}
//			}
//		}
//		
//		private void writePageHeadExtensions(BufferedWriter out) throws IOException {
//			
//			//	include CSS
//			for (int c = 0; c < cssStylesheetsToInlcude.length; c++) {
//				String cssStylesheetUrl = cssStylesheetsToInlcude[c];
//				
//				//	make stylesheet URL absolute
//				if (cssStylesheetUrl.indexOf("://") == -1) {
//					
//					//	cut leading local steps
//					if (cssStylesheetUrl.startsWith("./"))
//						cssStylesheetUrl = cssStylesheetUrl.substring(1);
//					if (cssStylesheetUrl.startsWith("/"))
//						cssStylesheetUrl = cssStylesheetUrl.substring(1);
//					
//					//	create absolute URL
//					cssStylesheetUrl = this.request.getContextPath() + "/" + cssStylesheetUrl;
//				}
//				
//				//	write link
//				out.write("<link rel=\"stylesheet\" type=\"text/css\" media=\"all\" href=\"" + cssStylesheetUrl + "\"></link>");
//				out.newLine();
//			}
//			
//			//	include java scripts
//			for (Iterator sit = javaScriptsToInclude.iterator(); sit.hasNext();) {
//				String javaScriptUrl = ((String) sit.next());
//				
//				//	make stylesheet URL absolute
//				if (javaScriptUrl.indexOf("://") == -1) {
//					
//					//	cut leading local steps
//					if (javaScriptUrl.startsWith("./"))
//						javaScriptUrl = javaScriptUrl.substring(1);
//					if (javaScriptUrl.startsWith("/"))
//						javaScriptUrl = javaScriptUrl.substring(1);
//					
//					//	create absolute URL
//					javaScriptUrl = this.request.getContextPath() + "/" + javaScriptUrl;
//				}
//				
//				//	write link
//				out.write("<script type=\"text/javascript\" src=\"" + javaScriptUrl + "\"></script>");
//				out.newLine();
//			}
//			
//			//	create generic doOnloadCalls() and doOnunloadCalls() functions
//			out.write("<script type=\"text/javascript\">");
//			out.newLine();
//			out.write("function doOnloadCalls() {");
//			out.newLine();
//			for (Iterator fit = functionsToCallOnLoad.iterator(); fit.hasNext();) {
//				String call = ((String) fit.next());
//				if (call != null) {
//					this.out.write(call + (call.endsWith(";") ? "" : ";"));
//					this.out.newLine();
//				}
//			}
//			out.write("}");
//			out.newLine();
//			out.write("function doOnunloadCalls() {");
//			out.newLine();
//			for (Iterator fit = functionsToCallOnUnload.iterator(); fit.hasNext();) {
//				String call = ((String) fit.next());
//				if (call != null) {
//					this.out.write(call + (call.endsWith(";") ? "" : ";"));
//					this.out.newLine();
//				}
//			}
//			out.write("}");
//			out.newLine();
//			out.write("</script>");
//			out.newLine();
//		}
//		
//		private void includeFile(String token, BufferedWriter out) throws IOException {
//			TreeNodeAttributeSet as = TreeNodeAttributeSet.getTagAttributes(token, html);
//			String includeFile = as.getAttribute("file");
//			if (includeFile != null) {
//				out.newLine();
//				try {
//					SearchPortalServlet.this.includeFile(includeFile, this);
//				}
//				catch (IOException ioe) {
//					this.writeExceptionAsXmlComment(("exception including file '" + includeFile + "'"), ioe, out);
//				}
//				out.newLine();
//			}
//		}
//		
//		private void includeNavigation(String token, BufferedWriter out) throws IOException {
//			if ((html.isEndTag(token) || html.isSingularTag(token)) && (this.fieldGroups == null) && (this.fieldGroupException == null)) {
//				out.newLine();
//				try {
//					if (THESAURUS_SEARCH_MODE.equals(this.searchMode))
//						layout.includeNavigationLinks(((NavigationLink[]) thesaurusResultLinks.toArray(new NavigationLink[thesaurusResultLinks.size()])), this);
//					else layout.includeNavigationLinks(((NavigationLink[]) searchResultLinks.toArray(new NavigationLink[searchResultLinks.size()])), this);
//				}
//				catch (Exception e) {
//					this.writeExceptionAsXmlComment(("exception including navigation"), e, out);
//				}
//				out.newLine();
//			}
//		}
//		
//		private void includeStatistics(String token, BufferedWriter out) throws IOException {
//			if ((html.isEndTag(token) || html.isSingularTag(token))) {
//				out.newLine();
//				try {
//					layout.includeStatisticsLine(SearchPortalServlet.this.statistics, ((NavigationLink[]) statisticsLineLinks.toArray(new NavigationLink[statisticsLineLinks.size()])), this);
//				}
//				catch (Exception e) {
//					this.writeExceptionAsXmlComment(("exception including statistics line"), e, out);
//				}
//				out.newLine();
//			}
//		}
//		
//		private void includeSearchForms(String token, BufferedWriter out) throws IOException {
//			if ((html.isEndTag(token) || html.isSingularTag(token))) {
//				out.newLine();
//				
//				//	no search field goups available
//				if (this.fieldGroups == null) {
//					
//					//	search field groups were meant to be included, but an exception occured fetching them 
//					if (this.fieldGroupException != null)
//						this.writeExceptionAsXmlComment("exception fetching forms", this.fieldGroupException, out);
//				}
//				
//				//	thesaurus search forms
//				else if (THESAURUS_SEARCH_MODE.equals(this.searchMode)) {
//					try {
//						layout.includeThesaurusForms(thesaurusFormTitle, this.fieldGroups, null, this.fieldValues, this);
//					}
//					catch (Exception e) {
//						this.writeExceptionAsXmlComment(("exception including thesaurus forms"), e, out);
//					}
//				}
//				
//				//	document search forms
//				else {
//					try {
//						SearchFieldRow buttonRowFields = getSearchFormExtensions(this.fieldGroups);
//						layout.includeSearchForm(searchFormTitle, this.fieldGroups, buttonRowFields, this.fieldValues, this);
//					}
//					catch (Exception e) {
//						this.writeExceptionAsXmlComment(("exception including search form"), e, out);
//					}
//				}
//				
//				out.newLine();
//			}
//		}
//		
//		private void includeResultIndex(String token, BufferedWriter out) throws IOException {
//			if ((html.isEndTag(token) || html.isSingularTag(token))) {
//				out.newLine();
//				try {
//					BufferedThesaurusResult[] ri = getResultIndices(this.documentResult, this.document);
//					if (ri.length != 0) layout.includeResultIndex(ri, this);
//				}
//				catch (Exception e) {
//					this.writeExceptionAsXmlComment(("exception including result index"), e, out);
//				}
//				out.newLine();
//			}
//		}
//		
//		private void includeResults(String token, BufferedWriter out) throws IOException {
//			if ((html.isEndTag(token) || html.isSingularTag(token))) {
//				
//				//	collection statistics
//				if (this.statistics != null) {
//					out.newLine();
//					try {
//						layout.includeStatistics(this.statistics, this);
//					}
//					catch (Exception e) {
//						this.writeExceptionAsXmlComment(("exception including statistics"), e, out);
//					}
//					out.newLine();
//					
//				}
//				
//				//	thesaurus search result
//				else if (this.thesaurusResult != null) {
//					this.thesaurusResult.sort();
//					try {
//						layout.includeThesaurusResult(this.thesaurusResult, this);
//					}
//					catch (Exception e) {
//						this.writeExceptionAsXmlComment(("exception including thesaurus search result"), e, out);
//					}
//				}
//				
//				//	index search result
//				else if (this.indexResult != null) {
//					try {
//						layout.includeIndexResult(this.indexResult, this);
//					}
//					catch (Exception e) {
//						this.writeExceptionAsXmlComment(("exception including index result"), e, out);
//					}
//				}
//				
//				//	single document to display
//				else if (this.document != null) {
//					try {
//						if (this.document.document == null) {
//							out.write("<!-- exception including display document: no document to display for ID '" + this.document.documentId + "' -->");
//							out.newLine();
//						}
//						else {
//							String[] attributeNames = this.document.getAttributeNames();
//							for (int a = 0; a < attributeNames.length; a++)
//								if (this.document.document.getDocumentProperty(attributeNames[a]) == null)
//									this.document.document.setDocumentProperty(attributeNames[a], ((String) this.document.getAttribute(attributeNames[a])));
//							layout.includeResultDocument(this.document, this);
//						}
//					}
//					catch (Exception e) {
//						this.writeExceptionAsXmlComment(("exception including display document"), e, out);
//					}
//				}
//				
//				//	document search result
//				else if (this.documentResult != null) {
//					try {
//						
//						//	make sure result is fully loaded (will happen in layout engine anyway)
//						for (DocumentResult dr = this.documentResult.getDocumentResult(); dr.hasNextElement();)
//							dr.getNextDocumentResultElement();
//						
//						//	check result pivot
//						int resultPivot = 20;
//						String resultPivotString = this.request.getParameter(RESULT_PIVOT_INDEX_PARAMETER);
//						if (resultPivotString != null) try {
//							resultPivot = Integer.parseInt(resultPivotString);
//						} catch (NumberFormatException nfe) {}
//						
//						//	generate link if actual number of results equal to or higher than current cutoff 
//						String moreResultsLink;
//						if (this.documentResult.size() < resultPivot)
//							moreResultsLink = null;
//						else {
//							int nextResultPivot = resultPivot;
//							while (nextResultPivot < this.documentResult.size())
//								nextResultPivot *= 5;
//							moreResultsLink = this.request.getQueryString();
//							if (moreResultsLink.indexOf(RESULT_PIVOT_INDEX_PARAMETER) == -1)
//								moreResultsLink += ("&" + RESULT_PIVOT_INDEX_PARAMETER + "=" + nextResultPivot);
//							else moreResultsLink = moreResultsLink.replaceAll((RESULT_PIVOT_INDEX_PARAMETER + "[^&]++"), (RESULT_PIVOT_INDEX_PARAMETER + "=" + nextResultPivot));
//						}
//						layout.includeResultList(this.documentResult, this, moreResultsLink);
//					}
//					catch (Exception e) {
//						this.writeExceptionAsXmlComment(("exception including document list"), e, out);
//					}
//				}
//				
//				out.newLine();
//			}
//		}
//		
//		private void writeExceptionAsXmlComment(String label, Exception e, BufferedWriter out) throws IOException {
//			out.write("<!-- " + label + ": " + e.getMessage());
//			out.newLine();
//			StackTraceElement[] ste = e.getStackTrace();
//			for (int s = 0; s < ste.length; s++) {
//				out.write("  " + ste[s].toString());
//				out.newLine();
//			}
//			out.write("  " + label + " -->");
//			out.newLine();
//		}
//		
//		private BufferedThesaurusResult[] getResultIndices(BufferedDocumentResult result, DocumentResultElement document) throws IOException {
//			
//			//	check parameters
//			if ((result == null) && ((document == null) || ((document.document == null)))) return new BufferedThesaurusResult[0];
//			
//			//	build data structures
//			HashMap indicesByName = new LinkedHashMap();
//			
//			//	collect data
//			if (result != null)
//				for (DocumentResult dr = result.getDocumentResult(); dr.hasNextElement();)
//					this.fillThesaurusResultDataStructures(dr.getNextDocumentResultElement().document, indicesByName);
//			
//			else if (document != null)
//				this.fillThesaurusResultDataStructures(document.document, indicesByName);
//			
//			//	eliminate duplicate entries
//			ArrayList indexList = new ArrayList();
//			for (Iterator riit = indicesByName.values().iterator(); riit.hasNext();) {
//				BufferedThesaurusResult ri = ((ResultIndexBuilder) riit.next()).getBufferedThesaurusResult();
//				ri.sort(ri.resultAttributes);
//				indexList.add(ri);
//			}
//			
//			//	assemble and return result
//			return ((BufferedThesaurusResult[]) indexList.toArray(new BufferedThesaurusResult[indexList.size()]));
//		}
//		
//		private void fillThesaurusResultDataStructures(MutableAnnotation doc, HashMap indicesByName) {
//			
//			//	get index fields from doc
//			if (doc.hasAttribute(RESULT_INDEX_NAME_ATTRIBUTE)) {
//				String indexName = ((String) doc.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE, ""));
//				String indexLabel = ((String) doc.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, ""));
//				
//				String indexFieldNameString = ((String) doc.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""));
//				StringVector indexFieldNames = new StringVector();
//				indexFieldNames.parseAndAddElements(indexFieldNameString, " ");
//				indexFieldNames.removeAll("");
//				
//				ResultIndexBuilder rib = ((ResultIndexBuilder) indicesByName.get(indexName));
//				if (rib == null) {
//					rib = new ResultIndexBuilder(indexFieldNames.toStringArray(), indexName, indexLabel);
//					indicesByName.put(indexName, rib);
//				}
//				rib.addElement(this.produceResultIndexEntry(doc));
//			}
//			
//			//	get index fields from annotations
//			Annotation[] annotations = doc.getAnnotations();
//			for (int a = 0; a < annotations.length; a++) {
//				if (annotations[a].hasAttribute(RESULT_INDEX_NAME_ATTRIBUTE)) {
//					String indexName = ((String) doc.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE, ""));
//					String indexLabel = ((String) doc.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, ""));
//					
//					String indexFieldNameString = ((String) doc.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""));
//					StringVector indexFieldNames = new StringVector();
//					indexFieldNames.parseAndAddElements(indexFieldNameString, " ");
//					indexFieldNames.removeAll("");
//					
//					ResultIndexBuilder rib = ((ResultIndexBuilder) indicesByName.get(indexName));
//					if (rib == null) {
//						rib = new ResultIndexBuilder(indexFieldNames.toStringArray(), indexName, indexLabel);
//						indicesByName.put(indexName, rib);
//					}
//					rib.addElement(this.produceResultIndexEntry(annotations[a]));
//				}
//			}
//		}
//		
//		private ThesaurusResultElement produceResultIndexEntry(Annotation data) {
//			ThesaurusResultElement tre = new ThesaurusResultElement();
//			tre.copyAttributes(data);
//			return tre;
//		}
//		
//		private class ResultIndexBuilder {
//			
//			final String[] resultAttributes;
//			final String thesaurusEntryType;
//			final String thesaurusName;
//			
//			private LinkedList elements = new LinkedList();
//			private HashSet elementKeys = new HashSet();
//			
//			ResultIndexBuilder(String[] resultAttributes, String thesaurusEntryType, String thesaurusName) {
//				this.resultAttributes = resultAttributes;
//				this.thesaurusEntryType = thesaurusEntryType;
//				this.thesaurusName = thesaurusName;
//			}
//			
//			void addElement(ThesaurusResultElement tre) {
//				if (this.elementKeys.add(this.produceSortKey(tre)))
//					this.elements.addLast(tre);
//			}
//			
//			
//			private String produceSortKey(ThesaurusResultElement tre) {
//				String sortKey = "";
//				for (int f = 0; f < this.resultAttributes.length; f++)
//					sortKey += ("   " + tre.getAttribute(this.resultAttributes[f]));
//				return sortKey.trim().toLowerCase();
//			}
//			
//			BufferedThesaurusResult getBufferedThesaurusResult() {
//				return new BufferedThesaurusResult(new ThesaurusResult(this.resultAttributes, this.thesaurusEntryType, this.thesaurusName) {
//					public boolean hasNextElement() {
//						return (elements.size() != 0);
//					}
//					public SrsSearchResultElement getNextElement() {
//						return (this.hasNextElement() ? ((ThesaurusResultElement) elements.removeFirst()) : null);
//					}
//				});
//			}
//		}
//	}
	
	private SearchFieldRow getSearchFormExtensions(SearchFieldGroup[] fieldGroups) {
		SearchFieldRow rts = new SearchFieldRow();
		
		//	field for main result type
		SearchField rt = new SearchField(INDEX_NAME, "Result Type", "0", SearchField.SELECT_TYPE);
		rt.addOption((this.documentLabelPlural.substring(0, 1).toUpperCase() + this.documentLabelPlural.substring(1).toLowerCase()), "0");
		for (int g = 0; g < fieldGroups.length; g++) {
			if (fieldGroups[g].indexEntryLabel != null)
				rt.addOption(fieldGroups[g].getEntryLabelPlural(), fieldGroups[g].indexName);
		}
		rts.addField(rt);
		
		//	field for sub result type
		SearchField srt = new SearchField(SUB_INDEX_NAME, "Sub Type", "0", SearchField.SELECT_TYPE);
		srt.addOption("None", "0");
		for (int g = 0; g < fieldGroups.length; g++) {
			if (fieldGroups[g].indexEntryLabel != null)
				srt.addOption(fieldGroups[g].getEntryLabelPlural(), fieldGroups[g].indexName);
		}
		rts.addField(srt);
		
		//	field for sub result type
		SearchField srms = new SearchField(SUB_RESULT_MIN_SIZE, "Min Size", "0", SearchField.SELECT_TYPE);
		srms.addOption("0");
		srms.addOption("1");
		srms.addOption("2");
		srms.addOption("5");
		srms.addOption("10");
		rts.addField(srms);
		
		return rts;
	}
	
	private Properties resultFormatAliasesToXsltURLs = new Properties();
	
	private BufferedCollectionStatistics statistics;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.AbstractSrsWebPortalServlet#init(de.uka.ipd.idaho.easyIO.settings.Settings)
	 */
	protected void init(Settings config) {
		super.init(config);
		
		//	initially load statistics
		try {
			this.statistics = new BufferedCollectionStatistics(this.srsClient.getStatistics());
		}
		catch (IOException ioe) {
			throw new RuntimeException("Error loading statistics: " + ioe.getMessage(), ioe);
		}
		
		//	initialize layout engine
		this.layout = this.getLayout(new File(this.dataFolder, SearchPortalLayout.LAYOUT_FOLDER_NAME), config.getSetting(LAYOUT_ENGINE_CLASS_NAME_SETTING));
		
		//	do repeatable initialization
		this.reInit(config);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet.ReInitializableServlet#reInit(de.uka.ipd.idaho.easyIO.settings.Settings)
	 */
	public void reInit(Settings config) {
		
		//	get application specific document labels
		this.documentLabelSingular = config.getSetting("documentLabelSingular", this.documentLabelSingular);
		this.documentLabelPlural = config.getSetting("documentLabelPlural", this.documentLabelPlural);
		
		//	get form titles
		this.searchFormTitle = config.getSetting("searchFormTitle");
		this.thesaurusFormTitle = config.getSetting("thesaurusFormTitle");
		
		//	load pattern for document title in result list
		this.displayTitlePattern = AttributePattern.buildPattern(config.getSetting("displayTitlePattern", ""));
		
		//	check when to display search forms
		this.includeSearchFormWithResult = config.containsKey("includeSearchFormWithResult");
		
		
		//	clear registers
		this.resultFormatAliasesToXsltURLs.clear();
		this.javaScriptsToInclude.clear();
		this.functionsToCallOnLoad.clear();
		
		this.searchResultLinks.clear();
		this.thesaurusResultLinks.clear();
		this.statisticsLineLinks.clear();
		
		
		//	initialize result format aliases
		Settings resultFormatAliases = config.getSubset("resultFormatAlias");
		String[] alias = resultFormatAliases.getKeys();
		for (int a = 0; a < alias.length; a++) {
			String aliasXsltUrl = resultFormatAliases.getSetting(alias[a]);
			if (aliasXsltUrl != null)
				this.resultFormatAliasesToXsltURLs.setProperty(alias[a], aliasXsltUrl);
		}
		
		
		//	initialize layout engine
		this.layout.init();
		
		
		//	collet stylesheets to include in result pages
		this.cssStylesheetsToInlcude = this.layout.getStylesheetsToInclude();
		for (int c = 0; c < this.cssStylesheetsToInlcude.length; c++)
			if (this.cssStylesheetsToInlcude[c].indexOf("://") == -1) {
				
				//	cut leading local steps
				if (this.cssStylesheetsToInlcude[c].startsWith("./"))
					this.cssStylesheetsToInlcude[c] = this.cssStylesheetsToInlcude[c].substring(1);
				if (this.cssStylesheetsToInlcude[c].startsWith("/"))
					this.cssStylesheetsToInlcude[c] = this.cssStylesheetsToInlcude[c].substring(1);
				
				//	create URL relative to own data path
				this.cssStylesheetsToInlcude[c] = (this.layoutDataPath + this.cssStylesheetsToInlcude[c]);
			}
		
		
		//	collet JavaScript files to include in result pages
		String[] layoutJavaScriptsToInclude = this.layout.getJavaScripsToInclude();
		if (layoutJavaScriptsToInclude != null) {
			for (int j = 0; j < layoutJavaScriptsToInclude.length; j++) {
				if (layoutJavaScriptsToInclude[j].indexOf("://") == -1) {
					
					//	cut leading local steps
					if (layoutJavaScriptsToInclude[j].startsWith("./"))
						layoutJavaScriptsToInclude[j] = layoutJavaScriptsToInclude[j].substring(1);
					if (layoutJavaScriptsToInclude[j].startsWith("/"))
						layoutJavaScriptsToInclude[j] = layoutJavaScriptsToInclude[j].substring(1);
					
					//	create URL relative to own data path
					layoutJavaScriptsToInclude[j] = (this.layoutDataPath + layoutJavaScriptsToInclude[j]);
				}
			}
			this.javaScriptsToInclude.addAll(Arrays.asList(layoutJavaScriptsToInclude));
		}
		
		
		//	load search result linkers
		this.resultLinkers = this.getResultLinkers(new File(this.dataFolder, SearchResultLinker.RESULT_LINKER_FOLDER_NAME));
		
		
		//	collect java scripts to load, and functions to execute on load and unload
		for (int l = 0; l < this.resultLinkers.length; l++) {
			String resultLinkerDataPath = this.resultLinkerDataPaths.getProperty(this.resultLinkers[l].getName());
			
			String[] javaScriptsToInclude = this.resultLinkers[l].getJavaScriptNames();
			if (javaScriptsToInclude != null) {
				for (int j = 0; j < javaScriptsToInclude.length; j++) {
					if (javaScriptsToInclude[j].indexOf("://") == -1) {
						
						//	cut leading local steps
						if (javaScriptsToInclude[j].startsWith("./"))
							javaScriptsToInclude[j] = javaScriptsToInclude[j].substring(1);
						if (javaScriptsToInclude[j].startsWith("/"))
							javaScriptsToInclude[j] = javaScriptsToInclude[j].substring(1);
						
						//	create URL relative to own data path
						javaScriptsToInclude[j] = (resultLinkerDataPath + javaScriptsToInclude[j]);
					}
				}
				this.javaScriptsToInclude.addAll(Arrays.asList(javaScriptsToInclude));
			}
			
			String[] functionsToCallOnLoad = this.resultLinkers[l].getLoadCalls();
			if (functionsToCallOnLoad != null)
				this.functionsToCallOnLoad.addAll(Arrays.asList(functionsToCallOnLoad));
			String[] functionsToCallOnUnload = this.resultLinkers[l].getUnloadCalls();
			if (functionsToCallOnUnload != null)
				this.functionsToCallOnUnload.addAll(Arrays.asList(functionsToCallOnUnload));
		}
		
		this.javaScriptsToInclude.add("searchPortalScripts.js");
		this.functionsToCallOnLoad.add("initAdjustSubIndexName();");
		
		//	create search result links
		this.searchResultLinks.add(new NavigationLink("Back to Search Form", null, ("./" + DEFAULT_SEARCH_MODE), ((String) null)));
		
		//	create thesaurus result links
		this.thesaurusResultLinks.add(new NavigationLink("Back to Search Form", null, ("./" + THESAURUS_SEARCH_MODE), ((String) null)));
		
		//	create link to manager
		this.statisticsLineLinks.add(new NavigationLink("view statistics", (this.getRelativeDataPath() + "/" + this.layoutDataPath + "statisticsIcon.gif"), ("./" + STATISTICS_MODE), "_blank"));
		
		//	clear XSLT cache
		this.cachedStylesheets.clear();
	}
	
	/**
	 * Retrieve the path of a given File relative to the surrounding web-app's
	 * context path. This allows for plug-ins to retrieve the relative path of
	 * their data folder. The returned path String is either empty, or it starts
	 * with a '/'.
	 * @param path the file path to obtain a relative path for
	 * @return the path of the specified file, relative to the surrounding
	 *         web-app's context path
	 */
	public String getRelativePath(File path) {
		if ((path == null) || !path.exists())
			return null;
		
		path = path.getAbsoluteFile();
		
		String relPath = "";
//		while (!this.rootFolder.equals(path)) {
//			relPath = ("/" + path.getName() + relPath);
//			path = path.getParentFile();
//			if (path == null)
//				return null;
//		}
		while (!this.dataFolder.equals(path)) {
			relPath = ("/" + path.getName() + relPath);
			path = path.getParentFile();
			if (path == null)
				return null;
		}
		
		return (this.getRelativeDataPath() + relPath);
	}
	
	/**
	 * Add a navigation link to the search portal page.
	 * @param link the navigation link to add
	 */
	public void addNavigationLink(NavigationLink link) {
		if (link != null) {
			this.statisticsLineLinks.add(link);
			this.javaScriptsToInclude.addAll(Arrays.asList(link.scripts));
		}
	}
	
	private static final java.io.FileFilter jarFileFilter = new java.io.FileFilter() {
		public boolean accept(File file) {
			return ((file != null) && file.getName().toLowerCase().endsWith(".jar"));
		}
	};
	
	private SearchResultLinker[] getResultLinkers(final File resultLinkerFolder) {
		
		//	get base directory
		if(!resultLinkerFolder.exists()) resultLinkerFolder.mkdir();
		
		//	load resultLinkers
		Object[] resultLinkerObjects = GamtaClassLoader.loadComponents(
				resultLinkerFolder, 
				SearchResultLinker.class, 
				new ComponentInitializer() {
					public void initialize(Object component, String componentJarName) throws Exception {
						String dataPathString = (componentJarName.substring(0, (componentJarName.length() - 4)) + "Data");
						File dataPath = new File(resultLinkerFolder, dataPathString);
						if (!dataPath.exists()) dataPath.mkdir();
						SearchResultLinker resultLinker = ((SearchResultLinker) component);
						resultLinker.setParent(SearchPortalServlet.this);
						resultLinker.setDataPath(dataPath);
						resultLinkerDataPaths.setProperty(resultLinker.getName(), (resultLinkerFolder.getName() + "/" + dataPathString + "/"));
					}
				});
		
		//	store & return resultLinkers
		SearchResultLinker[] resultLinkers = new SearchResultLinker[resultLinkerObjects.length];
		for (int c = 0; c < resultLinkerObjects.length; c++)
			resultLinkers[c] = ((SearchResultLinker) resultLinkerObjects[c]);
		return resultLinkers;
	}
	
	private SearchPortalLayout getLayout(File layoutFolder, String className) {
		
		//	get base directory
		if(!layoutFolder.exists()) layoutFolder.mkdir();
		
		System.out.println("GgSrsWebPortalServlet: trying to load layout engine - " + className);
		if (className != null) {
			
			//	get jars
			File[] jars = layoutFolder.listFiles(jarFileFilter);
			
			//	collect jars
			ArrayList jarUrlList = new ArrayList();
			for (int j = 0; j < jars.length; j++) {
				String jarName = jars[j].getName();
				try {
					
					//	add URL of jar to class loader URLs
					jarUrlList.add(jars[j].toURL());
					
					//	check for binary folder
					File jarBinFolder = new File(layoutFolder, (jarName.substring(0, (jarName.length() - 4)) + "Bin"));
					if (jarBinFolder.exists() && jarBinFolder.isDirectory()) {
						File[] jarBinJars = jarBinFolder.listFiles(jarFileFilter);
						for (int jbj = 0; jbj < jarBinJars.length; jbj++)
							jarUrlList.add(jarBinJars[jbj].toURL());
					}
				} catch (IOException ioe) {}
			}
			
			//	create class loader
			URL[] jarURLs = ((URL[]) jarUrlList.toArray(new URL[jarUrlList.size()]));
			ClassLoader layoutLoader = new URLClassLoader(jarURLs, SearchPortalLayout.class.getClassLoader());
			
			//	try to load class
			try {
				Class layoutClass = layoutLoader.loadClass(className);
				
				//	layout engine class loaded successfully
				if ((layoutClass != null) && !Modifier.isAbstract(layoutClass.getModifiers()) && Modifier.isPublic(layoutClass.getModifiers()) && !Modifier.isInterface(layoutClass.getModifiers()) && SearchPortalLayout.class.isAssignableFrom(layoutClass)) {
					System.out.println("  got layout engine class");
					
					try {
						Object o = layoutClass.newInstance();
						SearchPortalLayout layout = ((SearchPortalLayout) o);
						
						String dataPathString = (className.substring(className.lastIndexOf('.') + 1) + "Data");
						File dataPath = new File(layoutFolder, dataPathString);
						if (!dataPath.exists()) dataPath.mkdir();
						layout.setDataPath(dataPath);
						layout.setParent(this);
						
						this.layoutDataPath = (layoutFolder.getName() + "/" + dataPathString + "/");
						
						return layout;
						
					}
					catch (InstantiationException e) {
						System.out.println("  could not instantiate layout engine class");
					}
					catch (IllegalAccessException e) {
						System.out.println("  illegal acces to layout engine class");
					}
					catch (Throwable t) {
						System.out.println("Error instantiating layout engine class: " + t.getMessage());
						t.printStackTrace(System.out);
					}
				}
			}
			catch (ClassNotFoundException cnfe) {
				System.out.println("  layout engine class not found in these jars:");
				for (int u = 0; u < jarURLs.length; u++)
					System.out.println("     " + jarURLs[u].toString());
			}
			catch (NoClassDefFoundError ncdfe) {
				System.out.println("  definition of layout engine class not found");
			}
		}
		
		//	use fallback default layout
		SearchPortalLayout layout = new DefaultSearchPortalLayout();
		className = layout.getClass().getName();
		
		String dataPathString = (className.substring(className.lastIndexOf('.') + 1) + "Data");
		File dataPath = new File(layoutFolder, dataPathString);
		if (!dataPath.exists()) dataPath.mkdir();
		layout.setDataPath(dataPath);
		layout.setParent(this);
		
		this.layoutDataPath = (layoutFolder.getName() + "/" + dataPathString + "/");
		
		return layout;
	}
//	
//	/**
//	 * Write the content of an HTML file's body element to a token receiver
//	 * @param fileName the name of the file to include
//	 * @param tr the token receiver to write to
//	 * @throws IOException
//	 */
//	private void includeFile(String fileName, final TokenReceiver tr) throws IOException {
//		FileInputStream fis = null;
//		try {
//			TokenReceiver fr = new TokenReceiver() {
//				private boolean inBody = false;
//				public void close() throws IOException {}
//				public void storeToken(String token, int treeDepth) throws IOException {
//					if (html.isTag(token) && "body".equalsIgnoreCase(html.getType(token))) {
//						if (html.isEndTag(token)) this.inBody = false;
//						else this.inBody = true;
//					}
//					else if (this.inBody) tr.storeToken(token, treeDepth);
//				}
//			};
//			fis = new FileInputStream(this.findFile(fileName));
//			htmlParser.stream(fis, fr);
//		}
//		catch (FileNotFoundException fnfe) {
//			//	ignore inclusions that don't exist
//			System.out.println("Include file not found: " + this.layoutDataPath + fileName);
//		}
//		catch (Exception e) {
//			throw new IOException(e.getMessage());
//		}
//		finally {
//			if (fis != null)
//				fis.close();
//		}
//	}
//	
//	private File findFile(String fileName) throws FileNotFoundException {
//		File file;
//		
//		file = new File(this.dataFolder, (this.layoutDataPath + fileName));
//		if (file.exists()) return file;
//		
//		file = new File(this.dataFolder, fileName);
//		if (file.exists()) return file;
//		
//		file = new File(this.rootFolder, fileName);
//		if (file.exists()) return file;
//		
//		throw new FileNotFoundException(fileName);
//	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.WebServlet#findFile(java.lang.String)
	 */
	public File findFile(String fileName) {
		File file = new File(this.dataFolder, (this.layoutDataPath + fileName));
		return (file.exists() ? file : super.findFile(fileName));
	}

	/**	produce a nice looking column name from a field name
	 * @param	fieldName	the field name to parse
	 * @return a nice looking version of the specified field name, in particular, field names in camel case are parsed at internal upper case characters
	 */
	public String produceColumnLabel(String fieldName) {
		if (fieldName.length() < 2) return fieldName;
		StringVector parts = new StringVector();
		fieldName = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
		int c = 1;
		while (c < fieldName.length()) {
			if (Character.isUpperCase(fieldName.charAt(c))) {
				parts.addElement(fieldName.substring(0, c));
				fieldName = fieldName.substring(c);
				c = 1;
			} else c++;
		}
		if (fieldName.length() != 0)
			parts.addElement(fieldName);
		
		for (int p = 0; p < (parts.size() - 1);) {
			String part1 = parts.get(p);
			String part2 = parts.get(p + 1);
			if ((part2.length() == 1) && Character.isUpperCase(part1.charAt(part1.length() - 1))) {
				part1 += part2;
				parts.setElementAt(part1, p);
				parts.remove(p+1);
			}
			else p++;
		}
		
		return parts.concatStrings(" ");
	}
	
	private AttributePattern displayTitlePattern;
	
	/** create a custom title for a single result of a document search
	 * @param	dre		the DocumentResultElement to create a title for
	 * @return a title for the specified DocumentResultElement, assembled from the attribute values of the latter
	 */
	public String createDisplayTitle(final DocumentResultElement dre) {
		return this.displayTitlePattern.createDisplayString(new Properties() {
			public String getProperty(String key, String defaultValue) {
				Object value = dre.getAttribute(key);
				return ((value == null) ? defaultValue : value.toString());
			}
			public String getProperty(String key) {
				Object value = dre.getAttribute(key);
				return ((value == null) ? null : value.toString());
			}
		});
	}
	
	/**
	 * @return the search result linkers
	 */
	public SearchResultLinker[] getResultLinkers() {
		return this.resultLinkers;
	}
	
	/**
	 * @return the label for the documents in the SRS in sigular, e.g. 'article' (default is 'document')
	 */
	public String getDocumentLabelSingular() {
		return this.documentLabelSingular;
	}
	
	/**
	 * @return the label for the documents in the SRS in plural, e.g. 'articles' (default is 'documents')
	 */
	public String getDocumentLabelPlural() {
		return this.documentLabelPlural;
	}
}
