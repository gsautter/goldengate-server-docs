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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedCollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedDocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedIndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedThesaurusResult;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.IoTools;

/**
 * Abstarct super class for specific layout engine implementations for the
 * GoldenGATE SRS search portal servlet. This class holds a reference to the
 * parent servlet and the layout engine's data path.
 * 
 * @author sautter
 */
public abstract class SearchPortalLayout implements SearchPortalConstants {
	
	/**
	 * the name of the child folder to the servlet's root folder where the
	 * search portal layouts and their data are located
	 */
	public static final String LAYOUT_FOLDER_NAME = "Layouts";
	
	/**
	 * a mapping of characters to use in de.htmlXmlUtil.IoTools.prepareForHtml()
	 * to cause characters having HTML entity representations not understood by
	 * browsers to be mapped to themselves
	 */
	public static final Properties HTML_CHAR_MAPPING = new Properties();
	static {
		HTML_CHAR_MAPPING.setProperty("À", "À");
		HTML_CHAR_MAPPING.setProperty("Á", "Á");
		HTML_CHAR_MAPPING.setProperty("Â", "Â");
		HTML_CHAR_MAPPING.setProperty("Ã", "Ã");
		HTML_CHAR_MAPPING.setProperty("Ä", "Ä");
		HTML_CHAR_MAPPING.setProperty("Å", "Å");
		HTML_CHAR_MAPPING.setProperty("Æ", "Æ");
		HTML_CHAR_MAPPING.setProperty("à", "à");
		HTML_CHAR_MAPPING.setProperty("á", "á");
		HTML_CHAR_MAPPING.setProperty("â", "â");
		HTML_CHAR_MAPPING.setProperty("ã", "ã");
		HTML_CHAR_MAPPING.setProperty("ä", "ä");
		HTML_CHAR_MAPPING.setProperty("å", "å");
		HTML_CHAR_MAPPING.setProperty("æ", "æ");
		
		HTML_CHAR_MAPPING.setProperty("Ç", "Ç");
		HTML_CHAR_MAPPING.setProperty("ç", "ç");
		
		HTML_CHAR_MAPPING.setProperty("È", "È");
		HTML_CHAR_MAPPING.setProperty("É", "É");
		HTML_CHAR_MAPPING.setProperty("Ê", "Ê");
		HTML_CHAR_MAPPING.setProperty("Ë", "Ë");
		HTML_CHAR_MAPPING.setProperty("è", "è");
		HTML_CHAR_MAPPING.setProperty("é", "é");
		HTML_CHAR_MAPPING.setProperty("ê", "ê");
		HTML_CHAR_MAPPING.setProperty("ë", "ë");
		
		HTML_CHAR_MAPPING.setProperty("Ì", "Ì");
		HTML_CHAR_MAPPING.setProperty("Í", "Í");
		HTML_CHAR_MAPPING.setProperty("Î", "Î");
		HTML_CHAR_MAPPING.setProperty("Ï", "Ï");
		HTML_CHAR_MAPPING.setProperty("ì", "ì");
		HTML_CHAR_MAPPING.setProperty("í", "í");
		HTML_CHAR_MAPPING.setProperty("î", "î");
		HTML_CHAR_MAPPING.setProperty("ï", "ï");
		
		HTML_CHAR_MAPPING.setProperty("Ñ", "Ñ");
		HTML_CHAR_MAPPING.setProperty("ñ", "ñ");
		
		HTML_CHAR_MAPPING.setProperty("Ò", "Ò");
		HTML_CHAR_MAPPING.setProperty("Ó", "Ó");
		HTML_CHAR_MAPPING.setProperty("Ô", "Ô");
		HTML_CHAR_MAPPING.setProperty("Õ", "Õ");
		HTML_CHAR_MAPPING.setProperty("Ö", "Ö");
		HTML_CHAR_MAPPING.setProperty("Œ", "Œ");
		HTML_CHAR_MAPPING.setProperty("Ø", "Ø");
		HTML_CHAR_MAPPING.setProperty("ò", "ò");
		HTML_CHAR_MAPPING.setProperty("ó", "ó");
		HTML_CHAR_MAPPING.setProperty("ô", "ô");
		HTML_CHAR_MAPPING.setProperty("õ", "õ");
		HTML_CHAR_MAPPING.setProperty("ö", "ö");
		HTML_CHAR_MAPPING.setProperty("œ", "œ");
		HTML_CHAR_MAPPING.setProperty("ø", "ø");
		
		HTML_CHAR_MAPPING.setProperty("Ù", "Ù");
		HTML_CHAR_MAPPING.setProperty("Ú", "Ú");
		HTML_CHAR_MAPPING.setProperty("Û", "Û");
		HTML_CHAR_MAPPING.setProperty("Ü", "Ü");
		HTML_CHAR_MAPPING.setProperty("ù", "ù");
		HTML_CHAR_MAPPING.setProperty("ú", "ú");
		HTML_CHAR_MAPPING.setProperty("û", "û");
		HTML_CHAR_MAPPING.setProperty("ü", "ü");
		
		HTML_CHAR_MAPPING.setProperty("Ý", "Ý");
		HTML_CHAR_MAPPING.setProperty("ý", "ý");
		HTML_CHAR_MAPPING.setProperty("ÿ", "ÿ");
		
		HTML_CHAR_MAPPING.setProperty("ß", "ß");
		
		HTML_CHAR_MAPPING.setProperty("€", "€");
		
		HTML_CHAR_MAPPING.setProperty("–", "-");
		HTML_CHAR_MAPPING.setProperty("—", "-");
		
		HTML_CHAR_MAPPING.setProperty("'", "'");
		HTML_CHAR_MAPPING.setProperty("‘", "'");
		HTML_CHAR_MAPPING.setProperty("’", "'");
		HTML_CHAR_MAPPING.setProperty("‚", "'");
		
		HTML_CHAR_MAPPING.setProperty("“", "&quot;");
		HTML_CHAR_MAPPING.setProperty("”", "&quot;");
		HTML_CHAR_MAPPING.setProperty("„", "&quot;");
		HTML_CHAR_MAPPING.setProperty("‹", "&quot;");
		HTML_CHAR_MAPPING.setProperty("›", "&quot;");
		
		HTML_CHAR_MAPPING.setProperty("†", "†");
		HTML_CHAR_MAPPING.setProperty("‡", "‡");
		
		HTML_CHAR_MAPPING.setProperty("…", "…");
		
		HTML_CHAR_MAPPING.setProperty("‰", "‰");
		HTML_CHAR_MAPPING.setProperty("™", "™");
		HTML_CHAR_MAPPING.setProperty("=", "=");
	}
	
	/**
	 * Prepare a String for being displayed in an HTML page. Details: characters
	 * causing a line feed ('\n', '\r', '\f') are replaced by BR tags special
	 * characters are encoded for HTML display (e.g. '&' is encoded as
	 * &amp;amp;)
	 * @param string the String to be prepared
	 * @return a String looking - in an HTML page - the same as the specified
	 *         one would look in an Editor
	 * 
	 */
	protected static String prepareForHtml(String string) {
		return prepareForHtml(string, HTML_CHAR_MAPPING);
	}
	
	/**
	 * Prepare a String for being displayed in an HTML page. Details: characters
	 * causing a line feed ('\n', '\r', '\f') are replaced by BR tags special
	 * characters are encoded for HTML display (e.g. '&' is encoded as
	 * &amp;amp;)
	 * @param string the String to be prepared
	 * @param mapping a custom mapping of characters (one might e.g. want &Auml;
	 *            normalized to A instead of encoded to &amp;Auml;)
	 * @return a String looking - in an HTML page - the same as the specified
	 *         one would look in an Editor
	 */
	protected static String prepareForHtml(String string, Properties mapping) {
		return IoTools.prepareForHtml(string, mapping);
	}
	
	/** the search portal servlet hosting this layout engine */
	protected SearchPortalServlet parent;

	/** the folder containing this layout engine's data */
	protected File dataPath;

	/**
	 * Make the layout engine know where it's specific data is located
	 * @param dataPath the folder containing this layout engine's data
	 */
	public void setDataPath(File dataPath) {
		this.dataPath = dataPath;
	}

	/**
	 * Make the layout engine know the servlet hosting it
	 * @param parent the search portal servlet hosting this layout engine
	 */
	public void setParent(SearchPortalServlet parent) {
		this.parent = parent;
	}
	
	/**
	 * Initialize the layout engine (called after data path and parent are set)
	 * Note: this default implementation does nothing, sub classes are wellcome
	 * to overwrite it as needed
	 */
	public void init() {}

	/**
	 * Get the stylesheets used by this layout. Paths are expected to be either
	 * absolute (indicate by specifying a protocol, like 'http://'), or relative
	 * to the layout's data path. Note: This method returns an empty array by
	 * default, sub classes may overwrite this method as needed
	 * @return the addresses of the CSS stylesheets this layout engine uses
	 */
	public String[] getStylesheetsToInclude() {
		return new String[0];
	}

	/**
	 * Get the JavaScripts used by this layout. Paths are expected to be either
	 * absolute (indicate by specifying a protocol, like 'http://'), or relative
	 * to the layout's data path. Note: This method returns an empty array by
	 * default, sub classes may overwrite this method as needed
	 * @return the addresses of the JavaScript files this layout engine uses
	 */
	public String[] getJavaScripsToInclude() {
		return new String[0];
	}
	
	/**
	 * Write the search fields for document search
	 * @param links the navigation links to write
	 * @param hpb the HTML page builder to write the navigation links to
	 * @throws IOException
	 */
	public abstract void includeNavigationLinks(NavigationLink[] links, HtmlPageBuilder hpb) throws IOException;

	/**
	 * Write the search form for document search, including the buttons and
	 * possible extra fields
	 * @param formTitle the title for the (group of) search forms (null
	 *            indicates no title)
	 * @param fieldGroups the indexer search fields (represented by the root
	 *            nodes of the individual field sets)
	 * @param buttonRowFields extra search fields to include below the field
	 *            groups, where the buttons are
	 * @param fieldValues the values for pre-setting the fields
	 * @param hpb the HTML page builder to write the search fields to
	 * @throws IOException
	 */
	public abstract void includeSearchForm(String formTitle, SearchFieldGroup[] fieldGroups, SearchFieldRow buttonRowFields, Properties fieldValues, HtmlPageBuilder hpb) throws IOException;
	
	/**
	 * Write an index search result
	 * @param index the index search result to include
	 * @param hpb the HTML page builder to write the index result index
	 * @throws IOException
	 */
	public abstract void includeIndexResult(BufferedIndexResult index, HtmlPageBuilder hpb) throws IOException;
	
	/**
	 * Write an index search result. This default implementation simply loops
	 * through to the two argument version of this method, thus always ignoring
	 * the link. Sub classes are welcome to overwrite it as needed.
	 * @param index the index search result to include
	 * @param hpb the HTML page builder to write the index result index
	 * @param shortLink the shortest possible link for the search result page
	 * @throws IOException
	 */
	public void includeIndexResult(BufferedIndexResult index, HtmlPageBuilder hpb, String shortLink) throws IOException {
		this.includeIndexResult(index, hpb);
	}
	
	/**
	 * Write an index search result. This default implementation simply loops
	 * through to the two argument version of this method, thus always ignoring
	 * the link and label. Sub classes are welcome to overwrite it as needed.
	 * @param index the index search result to include
	 * @param hpb the HTML page builder to write the index result index
	 * @param shortLink the shortest possible link for the search result page
	 * @param searchResultLabel a label for the search result, listing what the
	 *            search was for
	 * @throws IOException
	 */
	public void includeIndexResult(BufferedIndexResult index, HtmlPageBuilder hpb, String shortLink, String searchResultLabel) throws IOException {
		this.includeIndexResult(index, hpb);
	}
	
	/**
	 * Write the indices for a search result.
	 * @param indices the result indices to include
	 * @param hpb the HTML page builder to write the result index to
	 * @throws IOException
	 */
	public abstract void includeResultIndex(BufferedThesaurusResult[] indices, HtmlPageBuilder hpb) throws IOException;
	
	/**
	 * Write the result list of a document search.
	 * @param documents the documents to create a result list from
	 * @param hpb the HTML page builder to write the result list to
	 * @throws IOException
	 */
	public abstract void includeResultList(BufferedDocumentResult documents, HtmlPageBuilder hpb) throws IOException;
	
	/**
	 * Write a result list of a document search. The link for searching with a
	 * higher cutoff may be null, e.g. if there are no further results. This
	 * default implementation simply loops through to the two argument version
	 * of this method, thus always ignoring the link. Sub classes are welcome
	 * to overwrite it as needed.
	 * @param documents the documents to create a result list from
	 * @param hpb the HTML page builder to write the result list to
	 * @param moreResultsLink the link for repeating the search with a higher
	 *            cutoff
	 * @throws IOException
	 */
	public void includeResultList(BufferedDocumentResult documents, HtmlPageBuilder hpb, String moreResultsLink) throws IOException {
		this.includeResultList(documents, hpb);
	}
	
	/**
	 * Write a result list of a document search. The link for searching with a
	 * higher cutoff may be null, e.g. if there are no further results. This
	 * default implementation simply loops through to the two argument version
	 * of this method, thus always ignoring the links. Sub classes are welcome
	 * to overwrite it as needed.
	 * @param documents the documents to create a result list from
	 * @param hpb the HTML page builder to write the result list to
	 * @param moreResultsLink the link for repeating the search with a higher
	 *            cutoff
	 * @param shortLink the shortest possible link for the search result page
	 * @throws IOException
	 */
	public void includeResultList(BufferedDocumentResult documents, HtmlPageBuilder hpb, String moreResultsLink, String shortLink) throws IOException {
		this.includeResultList(documents, hpb);
	}
	
	/**
	 * Write a result list of a document search. The link for searching with a
	 * higher cutoff may be null, e.g. if there are no further results. This
	 * default implementation simply loops through to the two argument version
	 * of this method, thus always ignoring the links and label. Sub classes
	 * are welcome to overwrite it as needed.
	 * @param documents the documents to create a result list from
	 * @param hpb the HTML page builder to write the result list to
	 * @param moreResultsLink the link for repeating the search with a higher
	 *            cutoff
	 * @param shortLink the shortest possible link for the search result page
	 * @param searchResultLabel a label for the search result, listing what the
	 *            search was for
	 * @throws IOException
	 */
	public void includeResultList(BufferedDocumentResult documents, HtmlPageBuilder hpb, String moreResultsLink, String shortLink, String searchResultLabel) throws IOException {
		this.includeResultList(documents, hpb);
	}
	
	/**
	 * Indicate which form of result is required for creating the result list<br>
	 * SEARCH_DOCUMENTS indicates complete document (attention, large amount of
	 * data)<br>
	 * SEARCH_DOCUMENT_DETAILS indicates essential details of the documents<br>
	 * SEARCH_INDEX indicates document metadata, plus the documents' index
	 * entries from all indexers<br>
	 * SEARCH_DOCUMENT_DATA indicates document metatdata<br>
	 * @return the search mode to use for document search, one of SEARCH_DOCS,
	 *         SEARCH_DOC_DETAILS, SEARCH_INDEX, or SEARCH_DOC_DATA
	 */
	public abstract String getResultListSearchMode();
	
	/**
	 * Write a master document summary. This default implementations behaves
	 * just the same as for a document search result. 
	 * @param documents the documents to create the summary
	 * @param hpb the HTML page builder to write the summary to
	 * @throws IOException
	 */
	public void includeDocumentSummary(BufferedDocumentResult documents, HtmlPageBuilder hpb) throws IOException {
		this.includeResultList(documents, hpb);
	}
	
	/**
	 * Indicate which form of result is required for creating the summary. This
	 * default implementation behaves just like a document search.<br>
	 * SEARCH_DOCUMENTS indicates complete documents (attention, large amount of
	 * data)<br>
	 * SEARCH_DOCUMENT_DETAILS indicates essential details of the documents<br>
	 * SEARCH_INDEX indicates document metadata, plus the documents' index
	 * entries from all indexers<br>
	 * SEARCH_DOCUMENT_DATA indicates document metatdata<br>
	 * @return the search mode to use for document summary, one of SEARCH_DOCS,
	 *         SEARCH_DOC_DETAILS, SEARCH_INDEX, or SEARCH_DOC_DATA
	 */
	public String getSummarySearchMode() {
		return this.getResultListSearchMode();
	}
	
	/**
	 * Write a single result document.
	 * @param document the documet to write (null for the document should
	 *            trigger an error message to be displayed)
	 * @param hpb the HTML page builder to write the document to
	 * @throws IOException
	 */
	public abstract void includeResultDocument(DocumentResultElement document, HtmlPageBuilder hpb) throws IOException;
	
	/**
	 * Write the search forms for thesaurus search.
	 * @param formTitle the title for the (group of) search forms (null
	 *            indicates no title)
	 * @param fieldGroups the search fields (represented by the root nodes of
	 *            the individual field sets)
	 * @param buttonRowFields extra search fields to include below each field
	 *            group, where the buttons are
	 * @param fieldValues the values for pre-setting the fields
	 * @param hpb the HTML page builder to write the search fields to
	 * @throws IOException
	 */
	public abstract void includeThesaurusForms(String formTitle, SearchFieldGroup[] fieldGroups, SearchFieldRow buttonRowFields, Properties fieldValues, HtmlPageBuilder hpb) throws IOException;
	
	/**
	 * Write the result of a thesaurus lookup.
	 * @param result the thesaurus lookup result to write
	 * @param hpb the HTML page builder to write the lookup result to
	 * @throws IOException
	 */
	public abstract void includeThesaurusResult(BufferedThesaurusResult result, HtmlPageBuilder hpb) throws IOException;
	
	/**
	 * Write the result of a thesaurus lookup. This default implementation
	 * simply loops through to the two argument version of this method, thus
	 * always ignoring the link. Sub classes are welcome to overwrite it as
	 * needed.
	 * @param result the thesaurus lookup result to write
	 * @param hpb the HTML page builder to write the lookup result to
	 * @param shortLink the shortest possible link for the search result page
	 * @throws IOException
	 */
	public void includeThesaurusResult(BufferedThesaurusResult result, HtmlPageBuilder hpb, String shortLink) throws IOException {
		this.includeThesaurusResult(result, hpb);
	}
	
	/**
	 * Write the result of a thesaurus lookup. This default implementation
	 * simply loops through to the two argument version of this method, thus
	 * always ignoring the link and label. Sub classes are welcome to overwrite
	 * it as needed.
	 * @param result the thesaurus lookup result to write
	 * @param hpb the HTML page builder to write the lookup result to
	 * @param shortLink the shortest possible link for the search result page
	 * @param searchResultLabel a label for the search result, listing what the
	 *            search was for
	 * @throws IOException
	 */
	public void includeThesaurusResult(BufferedThesaurusResult result, HtmlPageBuilder hpb, String shortLink, String searchResultLabel) throws IOException {
		this.includeThesaurusResult(result, hpb);
	}
	
	/**
	 * Write the collection statistics line.
	 * @param statistics the statistics to write
	 * @param links links to include in the statistics line
	 * @param hpb the HTML page builder to write the statistics to
	 * @throws IOException
	 */
	public abstract void includeStatisticsLine(BufferedCollectionStatistics statistics, NavigationLink[] links, HtmlPageBuilder hpb) throws IOException;

	/**
	 * Write the collection statistics.
	 * @param statistics the statistics to write
	 * @param hpb the HTML page builder to write the statistics to
	 * @throws IOException
	 */
	public abstract void includeStatistics(BufferedCollectionStatistics statistics, HtmlPageBuilder hpb) throws IOException;
}
