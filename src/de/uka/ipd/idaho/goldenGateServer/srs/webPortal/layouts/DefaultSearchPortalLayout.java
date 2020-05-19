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
 *     * Neither the name of the Universitaet Karlsruhe (TH) nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY UNIVERSITAET KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layouts;


import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Stack;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatisticsElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedCollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedDocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedIndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedThesaurusResult;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * default layout implementation
 * 
 * @author sautter
 */
public class DefaultSearchPortalLayout extends SearchPortalLayout {
	
	private String resultListMode = LIST_RESULT_LIST_MODE;
	private String resultDisplayMode = PLAIN_RESULT_DISPLAY_MODE;
	
	private String titleTextStyle = "font-family:Verdana; font-size:12pt";
	private String labelTextStyle = "font-family:Verdana; font-size:11pt";
	private String utilityLinkTextStyle = "font-family:Verdana; font-size:8pt";
	private String tableEntryTextStyle = "font-family:Verdana; font-size:10pt";
	private String docContentTextStyle = "font-family:Arial; font-size:10pt";
	private String docMarkupTextStyle = "font-family:Courier; font-size:10pt";
	
	private String tableBorderColor = "#000000";
	private String titleBackgroundColor = "#33CC33";
	private String labelBackgroundColor = "#66FF66";
	private String tableEntryBackgroundColor = "#99FF99";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#init()
	 */
	public void init() {
		Settings settings = Settings.loadSettings(new File(this.dataPath, "config.cnfg"));
		
		//	get result display mode
		this.resultListMode = settings.getSetting(RESULT_LIST_MODE_PARAMETER, this.resultListMode);
		this.resultDisplayMode = settings.getSetting(RESULT_DISPLAY_MODE_PARAMETER, this.resultDisplayMode);
		
		//	read layout data
		this.tableBorderColor = settings.getSetting("tableBorderColor", this.tableBorderColor);
		
		this.titleTextStyle = settings.getSetting("titleTextStyle", this.titleTextStyle);
		this.titleBackgroundColor = settings.getSetting("titleBackgroundColor", this.titleBackgroundColor);
		
		this.labelTextStyle = settings.getSetting("labelTextStyle", this.labelTextStyle);
		this.labelBackgroundColor = settings.getSetting("labelBackgroundColor", this.labelBackgroundColor);
		
		this.utilityLinkTextStyle = settings.getSetting("utilityLinkTextStyle", this.utilityLinkTextStyle);
		
		this.tableEntryTextStyle = settings.getSetting("tableEntryTextStyle", this.tableEntryTextStyle);
		this.tableEntryBackgroundColor = settings.getSetting("tableEntryBackgroundColor", this.tableEntryBackgroundColor);
		
		this.docContentTextStyle = settings.getSetting("docContentTextStyle", this.docContentTextStyle);
		this.docMarkupTextStyle = settings.getSetting("docMarkupTextStyle", this.docMarkupTextStyle);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeNavigationLinks(de.goldenGateSrs.webPortal.SearchPortalServletFlexLayout.NavigationLink[], de.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeNavigationLinks(NavigationLink[] links, HtmlPageBuilder tr) throws IOException {
		
		tr.storeToken("<p style=\"" + this.utilityLinkTextStyle + "\" align=\"left\">", 0);
		
		for (int l = 0; l < links.length; l++) {
			
			if (l != 0) tr.storeToken("&nbsp;", 0);
			
			tr.storeToken("<a href=\"" + links[l].link + "\">", 0);
			tr.storeToken(links[l].label, 0);
			tr.storeToken("</a>", 0);
		}
		
		tr.storeToken("</p>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeSearchForm(java.lang.String, de.goldenGateSrs.GoldenGateSrsConstants.SearchFieldGroup[], de.goldenGateSrs.GoldenGateSrsConstants.SearchFieldRow, java.util.Properties, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeSearchForm(String formTitle, SearchFieldGroup[] fieldGroups, SearchFieldRow buttonRowFields, Properties fieldValues, HtmlPageBuilder tr) throws IOException {
		
		//	open form
		tr.storeToken(("<form method=\"GET\" name=\"" + SRS_SEARCH_FORM_NAME + "\" action=\"" + tr.request.getContextPath() + "/" + DEFAULT_SEARCH_MODE + "\">"), 0);
		
		//	open master table
		tr.storeToken(("<table border=\"2\" fame=\"box\" rules=\"none\" cellpadding=\"3\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\">"), 0);
		
		//	add title row
		if ((formTitle != null) && (formTitle.trim().length() != 0)) {
			tr.storeToken("<tr>", 0);
			tr.storeToken(("<td width=\"100%\" bgcolor=\"" + this.titleBackgroundColor + "\">"), 0);
			tr.storeToken(("<p style=\"" + this.titleTextStyle + "\" align=\"center\">"), 0);
			tr.storeToken("<b>", 0);
			tr.storeToken(prepareForHtml(formTitle, HTML_CHAR_MAPPING), 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	open table body
		tr.storeToken("<tr>", 0);
		tr.storeToken(("<td width=\"100%\" bgcolor=\"" + this.tableEntryBackgroundColor + "\">"), 0);
		
		for (int g = 0; g < fieldGroups.length; g++) {
			
			// open fieldset and write field group legend
			tr.storeToken("<fieldset>", 0);
			tr.storeToken("<legend>", 0);
//			tr.storeToken(prepareForHtml(fieldGroups[g].legend, HTML_CHAR_MAPPING), 0);
			tr.storeToken(prepareForHtml(fieldGroups[g].tooltip, HTML_CHAR_MAPPING), 0);
			tr.storeToken("</legend>", 0);
			
			//	open table for field group
			tr.storeToken("<table frame=\"box\" rules=\"none\">", 0);
			
			//	write field rows
			SearchFieldRow[] fieldRows = fieldGroups[g].getFieldRows();
			for (int r = 0; r < fieldRows.length; r++) {
				
				//	open table row
				tr.storeToken("<tr>", 0);
				
				//	write fields
				SearchField[] fields = fieldRows[r].getFields();
				for (int f = 0; f < fields.length; f++) {
					
					//	open table cell and write label
					tr.storeToken(("<td align=\"center\" colspan=\"" + fields[f].size + "\">"), 0);
					tr.storeToken(("<span style=\"" + this.labelTextStyle + "\">"), 0);
					tr.storeToken(prepareForHtml(fields[f].label, HTML_CHAR_MAPPING), 0);
					tr.storeToken("</span>", 0);
					tr.storeToken(" ", 0);
					
					//	write actual field
					if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
						tr.storeToken(("<input type=\"checkbox\" name=\"" + fields[f].name + "\" value=\"" + true + "\"" + ((fieldValues.containsKey(fields[f].name) || ((fields[f].value != null) && (fields[f].value.length() != 0))) ? " checked=\"checked\"" : "") + ">"), 0);
					
					else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
						tr.storeToken(("<select name=\"" + fields[f].name + "\">"), 0);
						
						SearchFieldOption[] fieldOptions = fields[f].getOptions();
						for (int o = 0; o < fieldOptions.length; o++) {
							tr.storeToken(("<option value=\"" + fieldOptions[o].value + "\"" + (fieldOptions[o].value.equals(fieldValues.getProperty(fields[f].name)) ? " selected=\"selected\"" : "") + ">"), 0);
							tr.storeToken(prepareForHtml(fieldOptions[o].label, HTML_CHAR_MAPPING), 0);
							tr.storeToken("</option>", 0);
						}
						
						tr.storeToken("</select>", 0);
					}
					else
						tr.storeToken(("<input size=\"" + (fields[f].size * 20) + "\" name=\"" + fields[f].name + "\" value=\"" + fieldValues.getProperty(fields[f].name, fields[f].value) + "\">"), 0);
					
					//	close table cell
					tr.storeToken("</td>", 0);
				}
				
				//	close table row
				tr.storeToken("</tr>", 0);
			}
			
			//	close table and fieldset of field group
			tr.storeToken("</table>", 0);
			tr.storeToken("</fieldset>", 0);
		}
		
		//	close table body
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		//	open button row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td align=\"center\">", 0);
		
		//	add button row fields (if any)
		if (buttonRowFields != null) {
			SearchField[] fields = buttonRowFields.getFields();
			for (int f = 0; f < fields.length; f++) {
				
				//	write label
				tr.storeToken(("<span style=\"" + this.labelTextStyle + "\">"), 0);
				tr.storeToken(prepareForHtml(fields[f].label, HTML_CHAR_MAPPING), 0);
				tr.storeToken("</span>", 0);
				tr.storeToken(" ", 0);
				
				//	write actual field
				if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
					tr.storeToken(("<input type=\"checkbox\" name=\"" + fields[f].name + "\" value=\"" + true + "\"" + ((fieldValues.containsKey(fields[f].name) || ((fields[f].value != null) && (fields[f].value.length() != 0))) ? " checked=\"checked\"" : "") + ">"), 0);
				
				else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
					tr.storeToken(("<select name=\"" + fields[f].name + "\">"), 0);
					
					SearchFieldOption[] fieldOptions = fields[f].getOptions();
					for (int o = 0; o < fieldOptions.length; o++) {
						tr.storeToken(("<option value=\"" + fieldOptions[o].value + "\"" + (fieldOptions[o].value.equals(fieldValues.getProperty(fields[f].name)) ? " selected=\"selected\"" : "") + ">"), 0);
						tr.storeToken(prepareForHtml(fieldOptions[o].label, HTML_CHAR_MAPPING), 0);
						tr.storeToken("</option>", 0);
					}
					
					tr.storeToken("</select>", 0);
				}
				else tr.storeToken(("<input size=\"" + (fields[f].size * 20) + "\" name=\"" + fields[f].name + "\" value=\"" + fieldValues.getProperty(fields[f].name, fields[f].value) + "\">"), 0);
				
				//	add spacer
				tr.storeToken("&nbsp;", 0);
			}
		}
		
		//	add buttons
		tr.storeToken("<input type=\"submit\" value=\"Search\">", 0);
		tr.storeToken("&nbsp;", 0);
		tr.storeToken("<input type=\"reset\" value=\"Clear\">", 0);
		
		//	close button row
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		//	close table
		tr.storeToken("</table>", 0);
		
		//	close form
		tr.storeToken("</form>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultIndex(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedThesaurusResult[], de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeResultIndex(BufferedThesaurusResult[] indices, HtmlPageBuilder tr) throws IOException {
		
		//	open master table
		tr.storeToken("<table border=\"2\" frame=\"box\" rules=\"none\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\" cellpadding=\"3\">", 0);
		
		//	add head row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td width=\"100%\" bgcolor=\"" + this.titleBackgroundColor + "\">", 0);
		tr.storeToken("<p style=\"" + this.titleTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken("Search Result Index", 0);
		tr.storeToken("</b>", 0);
		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		//	generate sort keys and remove duplicates, and sort and write indices
		if (indices.length == 0) {
			
			//	create error message
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td align=\"center\">", 0);
			tr.storeToken("<p style=\"" + this.labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("An index view is not available for the results of your search.", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		else {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"left\">", 0);
			
			for (int i = 0; i < indices.length; i++) {
				ThesaurusResult ri = indices[i].getThesaurusResult();
				
				//	add spacer and open index table
				tr.storeToken("<br>", 0);
				tr.storeToken("<table border=\"1\" frame=\"box\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\">", 0);
				
				//	build label row
				tr.storeToken("<tr bgcolor=\"" + this.labelBackgroundColor + "\">", 0);
				tr.storeToken("<td align=\"center\" colspan=\"" + ri.resultAttributes.length + "\">", 0);
				tr.storeToken("<p style=\"" + this.labelTextStyle + "\" align=\"center\">", 0);
				tr.storeToken("<b>", 0);
				tr.storeToken(prepareForHtml(ri.thesaurusName, HTML_CHAR_MAPPING), 0);
				tr.storeToken("</b>", 0);
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
				tr.storeToken("</tr>", 0);
				
				//	build table header row
				tr.storeToken("<tr>", 0);
				for (int f = 0; f < ri.resultAttributes.length; f++) {
					tr.storeToken("<td align=\"center\">", 0);
					tr.storeToken("<p style=\"" + this.tableEntryTextStyle + "\" align=\"center\">", 0);
					tr.storeToken("<b>", 0);
					
					String fieldName = ri.resultAttributes[f];
					tr.storeToken((Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1)), 0);
					
					tr.storeToken("</b>", 0);
					tr.storeToken("</p>", 0);
					tr.storeToken("</td>", 0);
				}
				tr.storeToken("</tr>", 0);
				
				//	write index entries
				while (ri.hasNextElement()) {
					ThesaurusResultElement tre = ri.getNextThesaurusResultElement();
					tr.storeToken("<tr>", 0);
					for (int f = 0; f < ri.resultAttributes.length; f++) {
						tr.storeToken("<td align=\"center\">", 0);
						tr.storeToken("<p style=\"" + this.tableEntryTextStyle + "\" align=\"center\">", 0);
						
						String fieldValue = ((String) tre.getAttribute(ri.resultAttributes[f]));
						if (fieldValue == null) tr.storeToken("&nbsp;", 0);
						else tr.storeToken(prepareForHtml(fieldValue, HTML_CHAR_MAPPING), 0);
						
						tr.storeToken("</p>", 0);
						tr.storeToken("</td>", 0);
					}
					tr.storeToken("</tr>", 0);
				}
				
				//	close table
				tr.storeToken("</table>", 0);
			}
			
			//	close master table
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#getResultListSearchMode()
	 */
	public String getResultListSearchMode() {
		
		if (FULL_RESULT_LIST_MODE.equals(this.resultListMode))
			return SEARCH_DOCUMENTS;
		
		if (DETAIL_RESULT_LIST_MODE.equals(this.resultListMode))
			return SEARCH_DOCUMENT_DETAILS;
		
		if (CONCISE_RESULT_LIST_MODE.equals(this.resultListMode))
			return SEARCH_DOCUMENT_DETAILS;
		
		if (LIST_RESULT_LIST_MODE.equals(this.resultListMode))
			return SEARCH_DOCUMENT_DATA;
		
		return SEARCH_DOCUMENT_DATA;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultList(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedDocumentResult, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeResultList(BufferedDocumentResult documents, HtmlPageBuilder tr) throws IOException {
		
		if (FULL_RESULT_LIST_MODE.equals(this.resultListMode))
			this.includeResultListDocuments(documents, tr);
		
		else if (DETAIL_RESULT_LIST_MODE.equals(this.resultListMode))
			this.includeResultListDetail(documents, tr);
		
		else if (CONCISE_RESULT_LIST_MODE.equals(this.resultListMode))
			this.includeResultListConcise(documents, tr);
		
		else if (LIST_RESULT_LIST_MODE.equals(this.resultListMode))
			this.includeResultListPlain(documents, tr);
		
		else this.includeResultListPlain(documents, tr);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultListDocuments(de.goldenGateSrs.DocumentResult, java.lang.String, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	private void includeResultListDocuments(BufferedDocumentResult documents, HtmlPageBuilder tr) throws IOException {
		ArrayList docList = new ArrayList();
		for (DocumentResult dr = documents.getDocumentResult(); dr.hasNextElement();)
			docList.add(dr.getNextDocumentResultElement().document);
		MutableAnnotation[] docs = ((MutableAnnotation[]) docList.toArray(new MutableAnnotation[docList.size()]));
		
		//	open master table
		tr.storeToken("<table width=\"100%\" border=\"2\" frame=\"box\" rules=\"none\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\" cellpadding=\"3\">", 0);
		
		//	add head row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td width=\"100%\" bgcolor=\"" + this.titleBackgroundColor + "\">", 0);
		tr.storeToken("<p style=\"" + this.titleTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken("Search Results", 0);
		tr.storeToken("</b>", 0);
		
		//	add result level links to external sources
		ArrayList resImageLinks = new ArrayList();
		ArrayList resTextLinks = new ArrayList();
		
		//	get and sort result links
		SearchResultLinker[] linkers = this.parent.getResultLinkers();
		for (int l = 0; l < linkers.length; l++) try {
			SearchResultLink[] links = linkers[l].getSearchResultLinks(docs);
			if (links != null)
				for (int k = 0; k < links.length; k++) {
					if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
						if (links[k].iconUrl != null) resImageLinks.add(links[k]);
						else if (links[k].label != null) resTextLinks.add(links[k]);
					}
				}
		}
		catch (Exception e) {
			System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
			e.printStackTrace(System.out);
		}
		
		//	add result level result links
		if ((resImageLinks.size() + resTextLinks.size()) > 0) {
			
			//	add line of image links
			for (int i = 0; i < resImageLinks.size(); i++) {
				SearchResultLink link = ((SearchResultLink) resImageLinks.get(i));
				
				//	get function of link
				String linkFunction = null;
				if ((link.href != null) && (link.href.trim().length() != 0)) {
					linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
				} else if ((link.onclick != null) && (link.onclick.trim().length() != 0)) {
					linkFunction = "onclick=\"" + link.onclick + "\"";
				}
				
				//	add link icon
				if (linkFunction != null) {
					tr.storeToken((" <span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
					tr.storeToken((" <a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
					tr.storeToken(("<img src=\"" + link.iconUrl + "\"" + ((link.label == null) ? "" : (" alt=\"" + link.label + "\"")) + ">"), 0);
					tr.storeToken("</a>", 0);
					tr.storeToken("</span>", 0);
				}
			}
			
			//	add text links (one per line)
			for (int t = 0; t < resTextLinks.size(); t++) {
				SearchResultLink link = ((SearchResultLink) resTextLinks.get(t));
				
				//	get function of link
				String linkFunction = null;
				if ((link.href != null) && (link.href.trim().length() != 0)) {
					linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
				} else if ((link.onclick != null) && (link.onclick.trim().length() != 0)) {
					linkFunction = "onclick=\"" + link.onclick + "\"";
				}
				
				//	add link text
				tr.storeToken((" <span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
				tr.storeToken("<b>", 0);
				tr.storeToken(" (", 0);
				tr.storeToken(("<a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
				tr.storeToken(link.label, 0);
				tr.storeToken("</a>", 0);
				tr.storeToken(")", 0);
				tr.storeToken("</b>", 0);
				tr.storeToken("</span>", 0);
			}
		}

		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		if (documents.isEmpty()) {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"center\">", 0);
			tr.storeToken("<p style=\"" + this.labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Your search did not return any documents.", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		else {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"left\">", 0);
			
			for (DocumentResult dr = documents.getDocumentResult(); dr.hasNextElement();) {
				
				//	add spacer
				tr.storeToken("<br>", 0);
				
				//	include document
				this.includeResultDocument(dr.getNextDocumentResultElement(), tr);
			}
			
			//	close master table row
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultListDetail(de.goldenGateSrs.DocumentResult, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	private void includeResultListDetail(BufferedDocumentResult documents, HtmlPageBuilder tr) throws IOException {
		ArrayList docList = new ArrayList();
		for (DocumentResult dr = documents.getDocumentResult(); dr.hasNextElement();)
			docList.add(dr.getNextDocumentResultElement().document);
		MutableAnnotation[] docs = ((MutableAnnotation[]) docList.toArray(new MutableAnnotation[docList.size()]));
		
		//	open master table
		tr.storeToken("<table width=\"100%\" border=\"2\" frame=\"box\" rules=\"none\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\" cellpadding=\"3\">", 0);
		
		//	add head row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td width=\"100%\" bgcolor=\"" + this.titleBackgroundColor + "\">", 0);
		tr.storeToken("<p style=\"" + this.titleTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken("Search Results", 0);
		tr.storeToken("</b>", 0);
		
		//	add result level links to external sources
		ArrayList resImageLinks = new ArrayList();
		ArrayList resTextLinks = new ArrayList();
		
		//	get and sort result links
		SearchResultLinker[] linkers = this.parent.getResultLinkers();
		for (int l = 0; l < linkers.length; l++) try {
			SearchResultLink[] links = linkers[l].getSearchResultLinks(docs);
			if (links != null)
				for (int k = 0; k < links.length; k++) {
					if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
						if (links[k].iconUrl != null) resImageLinks.add(links[k]);
						else if (links[k].label != null) resTextLinks.add(links[k]);
					}
				}
		}
		catch (Exception e) {
			System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
			e.printStackTrace(System.out);
		}
		
		//	add result level result links
		if ((resImageLinks.size() + resTextLinks.size()) > 0) {
			
			//	add line of image links
			for (int i = 0; i < resImageLinks.size(); i++) {
				SearchResultLink link = ((SearchResultLink) resImageLinks.get(i));
				
				//	get function of link
				String linkFunction = null;
				if ((link.href != null) && (link.href.trim().length() != 0)) {
					linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
				} else if ((link.onclick != null) && (link.onclick.trim().length() != 0)) {
					linkFunction = "onclick=\"" + link.onclick + "\"";
				}
				
				//	add link icon
				if (linkFunction != null) {
					tr.storeToken((" <span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
					tr.storeToken((" <a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
					tr.storeToken(("<img src=\"" + link.iconUrl + "\"" + ((link.label == null) ? "" : (" alt=\"" + link.label + "\"")) + ">"), 0);
					tr.storeToken("</a>", 0);
					tr.storeToken("</span>", 0);
				}
			}
			
			//	add text links (one per line)
			for (int t = 0; t < resTextLinks.size(); t++) {
				SearchResultLink link = ((SearchResultLink) resTextLinks.get(t));
				
				//	get function of link
				String linkFunction = null;
				if ((link.href != null) && (link.href.trim().length() != 0)) {
					linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
				} else if ((link.onclick != null) && (link.onclick.trim().length() != 0)) {
					linkFunction = "onclick=\"" + link.onclick + "\"";
				}
				
				//	add link text
				tr.storeToken((" <span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
				tr.storeToken("<b>", 0);
				tr.storeToken(" (", 0);
				tr.storeToken(("<a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
				tr.storeToken(link.label, 0);
				tr.storeToken("</a>", 0);
				tr.storeToken(")", 0);
				tr.storeToken("</b>", 0);
				tr.storeToken("</span>", 0);
			}
		}

		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		if (documents.isEmpty()) {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"center\">", 0);
			tr.storeToken("<p style=\"" + this.labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Your search did not return any documents.", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		else {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"left\">", 0);
			
			for (DocumentResult dr = documents.getDocumentResult(); dr.hasNextElement();) try {
				DocumentResultElement dre = dr.getNextDocumentResultElement();
				
				//	add spacer
				tr.storeToken("<br>", 0);
				
				//	open table
				tr.storeToken("<table width=\"100%\" border=\"1\" frame=\"box\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\">", 0);
				
				//	write haed and determine colspan
				int colSpan = this.includeResultDocumentHead(dre, true, tr);
				
				//	open document body cell
				//	add document
				tr.storeToken("<tr>", 0);
				tr.storeToken("<td align=\"left\" colspan=\"" + colSpan + "\">", 0);
				tr.storeToken("<p style=\"" + this.docContentTextStyle + "\">", 0);

				//	include document
				this.includeDocumentBodyBoxed(dre.document, tr, this.docContentTextStyle, true);
				
				//	close document content cell
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
				tr.storeToken("</tr>", 0);
				
				//	close document table
				tr.storeToken("</table>", 0);
			}
			catch (Exception e) {
				System.out.println(" - exception doing documents: " + e.getClass().getName() + " - " + e.getMessage());
				e.printStackTrace(System.out);
			}
			
			//	close master table row
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultListConcise(de.goldenGateSrs.DocumentResult, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	private void includeResultListConcise(BufferedDocumentResult documents, HtmlPageBuilder tr) throws IOException {
		ArrayList docList = new ArrayList();
		for (DocumentResult dr = documents.getDocumentResult(); dr.hasNextElement();)
			docList.add(dr.getNextDocumentResultElement().document);
		MutableAnnotation[] docs = ((MutableAnnotation[]) docList.toArray(new MutableAnnotation[docList.size()]));
		
		//	open master table
		tr.storeToken("<table width=\"100%\" border=\"2\" frame=\"box\" rules=\"none\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\" cellpadding=\"3\">", 0);
		
		//	add head row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td width=\"100%\" bgcolor=\"" + this.titleBackgroundColor + "\">", 0);
		tr.storeToken("<p style=\"" + this.titleTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken("Search Results", 0);
		tr.storeToken("</b>", 0);
		
		//	add result level links to external sources
		ArrayList resImageLinks = new ArrayList();
		ArrayList resTextLinks = new ArrayList();
		
		//	get and sort result links
		SearchResultLinker[] linkers = this.parent.getResultLinkers();
		for (int l = 0; l < linkers.length; l++) try {
			SearchResultLink[] links = linkers[l].getSearchResultLinks(docs);
			if (links != null)
				for (int k = 0; k < links.length; k++) {
					if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
						if (links[k].iconUrl != null) resImageLinks.add(links[k]);
						else if (links[k].label != null) resTextLinks.add(links[k]);
					}
				}
		}
		catch (Exception e) {
			System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
			e.printStackTrace(System.out);
		}
		
		//	add result level result links
		if ((resImageLinks.size() + resTextLinks.size()) > 0) {
			
			//	add line of image links
			for (int i = 0; i < resImageLinks.size(); i++) {
				SearchResultLink link = ((SearchResultLink) resImageLinks.get(i));
				
				//	get function of link
				String linkFunction = null;
				if ((link.href != null) && (link.href.trim().length() != 0)) {
					linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
				} else if ((link.onclick != null) && (link.onclick.trim().length() != 0)) {
					linkFunction = "onclick=\"" + link.onclick + "\"";
				}
				
				//	add link icon
				if (linkFunction != null) {
					tr.storeToken((" <span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
					tr.storeToken((" <a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
					tr.storeToken(("<img src=\"" + link.iconUrl + "\"" + ((link.label == null) ? "" : (" alt=\"" + link.label + "\"")) + ">"), 0);
					tr.storeToken("</a>", 0);
					tr.storeToken("</span>", 0);
				}
			}
			
			//	add text links (one per line)
			for (int t = 0; t < resTextLinks.size(); t++) {
				SearchResultLink link = ((SearchResultLink) resTextLinks.get(t));
				
				//	get function of link
				String linkFunction = null;
				if ((link.href != null) && (link.href.trim().length() != 0)) {
					linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
				} else if ((link.onclick != null) && (link.onclick.trim().length() != 0)) {
					linkFunction = "onclick=\"" + link.onclick + "\"";
				}
				
				//	add link text
				tr.storeToken((" <span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
				tr.storeToken("<b>", 0);
				tr.storeToken(" (", 0);
				tr.storeToken(("<a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
				tr.storeToken(link.label, 0);
				tr.storeToken("</a>", 0);
				tr.storeToken(")", 0);
				tr.storeToken("</b>", 0);
				tr.storeToken("</span>", 0);
			}
		}

		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		if (documents.isEmpty()) {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"center\">", 0);
			tr.storeToken("<p style=\"" + this.labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Your search did not return any documents.", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		else {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"left\">", 0);
			
			for (DocumentResult dr = documents.getDocumentResult(); dr.hasNextElement();) try {
				DocumentResultElement dre = dr.getNextDocumentResultElement();
				
				//	add spacer
				tr.storeToken("<br>", 0);
				
				//	open table
				tr.storeToken("<table width=\"100%\" border=\"1\" frame=\"box\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\">", 0);
				
				//	write haed and determine colspan
				int colSpan = this.includeResultDocumentHead(dre, true, tr);
				
				//	open document body cell
				//	add document
				tr.storeToken("<tr>", 0);
				tr.storeToken("<td align=\"left\" colspan=\"" + colSpan + "\">", 0);
				tr.storeToken("<p style=\"" + this.docContentTextStyle + "\">", 0);

				//	include document
				this.includeDocumentBodyBoxed(dre.document, tr, this.docContentTextStyle, false);
				
				//	close document content cell
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
				tr.storeToken("</tr>", 0);
				
				//	close document table
				tr.storeToken("</table>", 0);
			}
			catch (Exception e) {
				System.out.println(" - exception doing documents: " + e.getClass().getName() + " - " + e.getMessage());
				e.printStackTrace(System.out);
			}
			
			//	close master table row
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultListPlain(de.goldenGateSrs.DocumentResult, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	private void includeResultListPlain(BufferedDocumentResult documents, HtmlPageBuilder tr) throws IOException {
		
		//	open master table
		tr.storeToken("<table width=\"100%\" border=\"2\" frame=\"box\" rules=\"none\" bgcolor=\"" + tableEntryBackgroundColor + "\" bordercolor=\"" + tableBorderColor + "\" cellspacing=\"0\" cellpadding=\"3\">", 0);
		
		//	build label row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td width=\"100%\" bgcolor=\"" + titleBackgroundColor + "\">", 0);
		tr.storeToken("<p style=\"" + titleTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken("Search Result", 0);
		tr.storeToken("</b>", 0);
		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		if (documents.isEmpty()) {
			
			//	add report of no results
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"center\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Your search did not return any results.", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		else {
			
			//	open result cell
			tr.storeToken("<tr bgcolor=\"" + this.tableEntryBackgroundColor + "\">", 0);
			tr.storeToken("<td width=\"100%\" align=\"center\">", 0);
			
			//	add document data
			for (DocumentResult dr = documents.getDocumentResult(); dr.hasNextElement();) {
				
				tr.storeToken("<table width=\"100%\" border=\"1\" frame=\"box\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\">", 0);
				this.includeResultDocumentHead(dr.getNextDocumentResultElement(), true, tr);
				tr.storeToken("</table>", 0);
			}
			
			//	close result cell
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeIndexResult(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedIndexResult, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeIndexResult(BufferedIndexResult index, HtmlPageBuilder tr) throws IOException {
		ArrayList ireList = new ArrayList();
		for (IndexResult ir = index.getIndexResult(); ir.hasNextElement();)
			ireList.add(ir.getNextIndexResultElement());
		IndexResultElement[] indexElements = ((IndexResultElement[]) ireList.toArray(new IndexResultElement[ireList.size()]));
		
		//	open master table
		tr.storeToken("<table border=\"2\" frame=\"box\" rules=\"none\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\" cellpadding=\"3\">", 0);
		
		//	add head row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td bgcolor=\"" + this.titleBackgroundColor + "\">", 0);
		tr.storeToken("<p style=\"" + this.titleTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken((index.indexName + " Search Result"), 0);
		tr.storeToken("</b>", 0);
		
		//	add result level links to external sources
		ArrayList resImageLinks = new ArrayList();
		ArrayList resTextLinks = new ArrayList();
		
		//	get and sort result links
		SearchResultLinker[] linkers = this.parent.getResultLinkers();
		for (int l = 0; l < linkers.length; l++) try {
			SearchResultLink[] links = linkers[l].getAnnotationSetLinks(indexElements);
			if (links != null)
				for (int k = 0; k < links.length; k++) {
					if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
						if (links[k].iconUrl != null) resImageLinks.add(links[k]);
						else if (links[k].label != null) resTextLinks.add(links[k]);
					}
				}
		}
		catch (Exception e) {
			System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
			e.printStackTrace(System.out);
		}
		
		//	add result level result links
		if ((resImageLinks.size() + resTextLinks.size()) > 0) {
			
			//	add line of image links
			for (int i = 0; i < resImageLinks.size(); i++) {
				SearchResultLink link = ((SearchResultLink) resImageLinks.get(i));
				
				//	get function of link
				String linkFunction = null;
				if ((link.href != null) && (link.href.trim().length() != 0))
					linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
				else if ((link.onclick != null) && (link.onclick.trim().length() != 0))
					linkFunction = "onclick=\"" + link.onclick + "\"";
				
				//	add link icon
				if (linkFunction != null) {
					tr.storeToken((" <span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
					tr.storeToken((" <a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
					tr.storeToken(("<img src=\"" + link.iconUrl + "\"" + ((link.label == null) ? "" : (" alt=\"" + link.label + "\"")) + ">"), 0);
					tr.storeToken("</a>", 0);
					tr.storeToken("</span>", 0);
				}
			}
			
			//	add text links (one per line)
			for (int t = 0; t < resTextLinks.size(); t++) {
				SearchResultLink link = ((SearchResultLink) resTextLinks.get(t));
				
				//	get function of link
				String linkFunction = null;
				if ((link.href != null) && (link.href.trim().length() != 0))
					linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
				else if ((link.onclick != null) && (link.onclick.trim().length() != 0))
					linkFunction = "onclick=\"" + link.onclick + "\"";
				
				//	add link text
				tr.storeToken((" <span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
				tr.storeToken("<b>", 0);
				tr.storeToken(" (", 0);
				tr.storeToken(("<a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
				tr.storeToken(link.label, 0);
				tr.storeToken("</a>", 0);
				tr.storeToken(")", 0);
				tr.storeToken("</b>", 0);
				tr.storeToken("</span>", 0);
			}
		}

		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		//	collect links for individual annotations
		HashMap linksByAnnotationId = new HashMap();
		for (int e = 0; e < indexElements.length; e++) {
			ArrayList linkList = new ArrayList();
			for (int l = 0; l < linkers.length; l++) try {
				SearchResultLink[] links = linkers[l].getAnnotationLinks(indexElements[e]);
				if (links != null)
					for (int k = 0; k < links.length; k++) {
						if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
							if (links[k].iconUrl != null) linkList.add(links[k]);
							else if (links[k].label != null) linkList.add(links[k]);
						}
					}
			}
			catch (Exception ex) {
				System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + ex.getClass().getName() + " - " + ex.getMessage());
				ex.printStackTrace(System.out);
			}
			if (!linkList.isEmpty())
				linksByAnnotationId.put(indexElements[e].getAnnotationID(), ((SearchResultLink[]) linkList.toArray(new SearchResultLink[linkList.size()])));
		}
		
		if (indexElements.length == 0) {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td align=\"center\">", 0);
			tr.storeToken("<p style=\"" + this.labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Your search did not return any documents.", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		else {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td align=\"left\">", 0);
			
			//	open index table
			tr.storeToken("<table width=\"100%\" border=\"1\" frame=\"box\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\">", 0);
			
			//	build table header row
			tr.storeToken("<tr>", 0);
			for (int f = 0; f < index.resultAttributes.length; f++) {
				tr.storeToken("<td align=\"center\">", 0);
				tr.storeToken("<p style=\"" + this.tableEntryTextStyle + "\" align=\"center\">", 0);
				tr.storeToken("<b>", 0);
				
				String fieldName = index.resultAttributes[f];
				tr.storeToken((Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1)), 0);
				
				tr.storeToken("</b>", 0);
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
			}
			
			//	add column for external links (if any)
			if (!linksByAnnotationId.isEmpty()) {
				tr.storeToken("<td align=\"center\">", 0);
				tr.storeToken("<p style=\"" + this.tableEntryTextStyle + "\" align=\"center\">", 0);
				tr.storeToken("<b>", 0);
				tr.storeToken("External Links", 0);
				tr.storeToken("</b>", 0);
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
			}
			
			//	close header row
			tr.storeToken("</tr>", 0);
			
			//	write index entries
			for (int e = 0; e < indexElements.length; e++) {
				tr.storeToken("<tr>", 0);
				
				for (int f = 0; f < index.resultAttributes.length; f++) {
					tr.storeToken("<td align=\"center\">", 0);
					tr.storeToken("<p style=\"" + this.tableEntryTextStyle + "\" align=\"center\">", 0);
					
					Object fieldValue = indexElements[e].getAttribute(index.resultAttributes[f]);
					if (fieldValue == null) tr.storeToken("&nbsp;", 0);
					else {
						
						//	check for search link
						Object link = null;
						if (IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE.equals(index.resultAttributes[f]))
							link = indexElements[e].getAttribute(SEARCH_LINK_QUERY_ATTRIBUTE);
						else link = indexElements[e].getAttribute(SEARCH_LINK_QUERY_ATTRIBUTE + index.resultAttributes[f]);
						
						//	no link given
						if (link == null)
							tr.storeToken(prepareForHtml(fieldValue.toString(), HTML_CHAR_MAPPING), 0);
						
						//	link given
						else {
							tr.storeToken(("<a href=\"./" + DEFAULT_SEARCH_MODE + "?" + link + "\" title=\"" + indexElements[e].getAttribute((SEARCH_LINK_TITLE_ATTRIBUTE + index.resultAttributes[f]), link) + "\">"), 0);
							tr.storeToken(prepareForHtml(fieldValue.toString(), HTML_CHAR_MAPPING), 0);
							tr.storeToken("</a>", 0);
						}
					}
					
					tr.storeToken("</p>", 0);
					tr.storeToken("</td>", 0);
				}
				
				//	add column for external links (if any)
				if (!linksByAnnotationId.isEmpty()) {
					tr.storeToken("<td align=\"center\">", 0);
					
					SearchResultLink[] links = ((SearchResultLink[]) linksByAnnotationId.get(indexElements[e].getAnnotationID()));
					if (links == null)
						tr.storeToken("&nbsp;", 0);
						
					else for (int l = 0; l < links.length; l++)
						this.writeInLineResultLink(tr, links[l]);
					
					tr.storeToken("</td>", 0);
				}
				
				tr.storeToken("</tr>", 0);
			}
			
			//	close table
			tr.storeToken("</table>", 0);
			
			//	close master table row
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	private int includeResultDocumentHead(DocumentResultElement document, boolean includeFullDocLink, HtmlPageBuilder tr) throws IOException {
		int headColSpan = 1;
		
		tr.storeToken("<tr bgcolor=\"" + labelBackgroundColor + "\">", 0);
		
		//	get label data
		String docTitle = this.parent.createDisplayTitle(document);
		String originalDocLink = document.getAttribute(DOCUMENT_SOURCE_LINK_ATTRIBUTE, "").toString();
		String relevance = ("" + document.relevance);
		if (relevance.length() > 4)
			relevance = relevance.substring(0, 4);
		
		//	add document title cell
		tr.storeToken("<td width=\"60%\" align=\"center\">", 0);
		tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		
		tr.storeToken(prepareForHtml((docTitle + " (Relevance " + relevance + ")"), HTML_CHAR_MAPPING), 0);
		
		tr.storeToken("</b>", 0);
		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		
		//	add link to full document
		if (includeFullDocLink && (document.documentId.length() != 0)) {
			
			tr.storeToken("<td align=\"center\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			
			String docLink = "./html?" + URLEncoder.encode(document.documentId, "UTF-8");
			
			tr.storeToken(("<a href=\"" + docLink + "\" target=\"_blank\">"), 0);
			tr.storeToken("View Document", 0);
			tr.storeToken("</a>", 0);
			
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			
			headColSpan++;
		}
		
		//	add link to original pdf document
		if (originalDocLink.length() != 0) {
			
			tr.storeToken("<td align=\"center\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			
			tr.storeToken(("<a href=\"" + originalDocLink + "\" target=\"_blank\">"), 0);
			tr.storeToken("Original PDF", 0);
			tr.storeToken("</a>", 0);
			
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			
			headColSpan++;
		}
		
		//	add link to plain XML document
		if (document.documentId.length() != 0) {
			tr.storeToken("<td align=\"center\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			
			String xmlLink = "./xml?" + URLEncoder.encode(document.documentId, "UTF-8");
			
			tr.storeToken(("<a href=\"" + xmlLink + "\" target=\"_blank\" type=\"text/plain\">"), 0);
			tr.storeToken("Plain XML", 0);
			tr.storeToken("</a>", 0);
			
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			
			headColSpan++;
		}
		
		//	add document links if document given
		if (document.document != null) {
			
			//	add document level links to external sources
			ArrayList docImageLinks = new ArrayList();
			ArrayList docTextLinks = new ArrayList();
			
			//	get and sort result links
			SearchResultLinker[] linkers = this.parent.getResultLinkers();
			for (int l = 0; l < linkers.length; l++) try {
				SearchResultLink[] links = linkers[l].getDocumentLinks(document.document);
				if (links != null)
					for (int k = 0; k < links.length; k++) {
						if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
							if (links[k].iconUrl != null) docImageLinks.add(links[k]);
							else if (links[k].label != null) docTextLinks.add(links[k]);
						}
					}
			}
			catch (Exception e) {
				System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
				e.printStackTrace(System.out);
			}
			
			//	add document level result links
			if ((docImageLinks.size() + docTextLinks.size()) > 0) {
				
				//	open link table cell
				tr.storeToken("<td align=\"center\">", 0);
				
				//	add line of image links
				for (int i = 0; i < docImageLinks.size(); i++) {
					SearchResultLink link = ((SearchResultLink) docImageLinks.get(i));
					
					//	get function of link
					String linkFunction = null;
					if ((link.href != null) && (link.href.trim().length() != 0))
						linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
					else if ((link.onclick != null) && (link.onclick.trim().length() != 0))
						linkFunction = "onclick=\"" + link.onclick + "\"";
					
					//	add link icon
					if (linkFunction != null) {
						tr.storeToken(("<span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
						tr.storeToken((" <a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
						tr.storeToken(("<img src=\"" + link.iconUrl + "\"" + ((link.label == null) ? "" : (" alt=\"" + link.label + "\"")) + ">"), 0);
						tr.storeToken("</a>", 0);
						tr.storeToken("</span>", 0);
					}
				}
				
				//	add text links (one per line)
				for (int t = 0; t < docTextLinks.size(); t++) {
					SearchResultLink link = ((SearchResultLink) docTextLinks.get(t));
					
					//	get function of link
					String linkFunction = null;
					if ((link.href != null) && (link.href.trim().length() != 0)) {
						linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
					} else if ((link.onclick != null) && (link.onclick.trim().length() != 0)) {
						linkFunction = "onclick=\"" + link.onclick + "\"";
					}
					
					//	add link text
					if ((t != 0) || !docImageLinks.isEmpty()) tr.storeToken("<br>", 0);
					tr.storeToken(("<span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
					tr.storeToken("<b>", 0);
					tr.storeToken("(", 0);
					tr.storeToken(("<a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
					tr.storeToken(link.label, 0);
					tr.storeToken("</a>", 0);
					tr.storeToken(")", 0);
					tr.storeToken("</b>", 0);
					tr.storeToken("</span>", 0);
				}
				
				//	close link table cell & adjust colspan of layout table
				tr.storeToken("</td>", 0);
				headColSpan++;
			}
		}
		
		//	close table
		tr.storeToken("</tr>", 0);
		
		return headColSpan;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultDocument(de.goldenGateSrs.DocumentResultElement, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeResultDocument(DocumentResultElement document, HtmlPageBuilder tr) throws IOException {
		
		//	open table
		tr.storeToken("<table width=\"100%\" border=\"1\" frame=\"box\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\">", 0);
		
		//	write haed and determine colspan
		int colSpan = this.includeResultDocumentHead(document, false, tr);
		
		//	open document body cell
		//	add document
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td align=\"left\" colspan=\"" + colSpan + "\">", 0);
		tr.storeToken("<p style=\"" + this.docContentTextStyle + "\">", 0);

		if (XML_RESULT_DISPLAY_MODE.equals(this.resultDisplayMode))
			this.includeDocumentBodyXML(document.document, tr);
		else if (BOXED_RESULT_DISPLAY_MODE.equals(this.resultDisplayMode))
			this.includeDocumentBodyBoxed(document.document, tr, this.docContentTextStyle, true);
		else if (PLAIN_RESULT_DISPLAY_MODE.equals(this.resultDisplayMode))
			this.includeDocumentBodyPlain(document.document, tr);
		else this.includeDocumentBodyBoxed(document.document, tr, this.docContentTextStyle, true);
		
		//	close document content cell
		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		//	close document table
		tr.storeToken("</table>", 0);
	}
	
	private static final String LINE_BROKEN = "LINE_BROKEN";
	
	private void includeDocumentBodyXML(MutableAnnotation doc, HtmlPageBuilder tr) throws IOException {
		SearchResultLinker[] linkers = this.parent.getResultLinkers();
		
		Annotation[] nestedAnnotations = doc.getAnnotations();
		int annotationPointer = 0;
		
		//	skip (jump) generic root tags
		while (
				((annotationPointer + 1) < nestedAnnotations.length)
				&&
				(DocumentRoot.DOCUMENT_TYPE.equals(nestedAnnotations[annotationPointer].getType()) 
				&& 
				(nestedAnnotations[annotationPointer].size() == nestedAnnotations[annotationPointer + 1].size()))
			) annotationPointer++;
		
		Stack stack = new Stack();
		
		Token token = null;
		Token lastToken;
		StringBuffer line = null;
		
		boolean lastWasLineBreak = false;
		boolean lastWasTag = true;
		
		for (int t = 0; t < doc.size(); t++) {
			
			//	switch to next Token
			lastToken = token;
			token = doc.tokenAt(t);
			
			//	write end tags for Annotations ending before current Token
			while ((stack.size() > 0) && ((((Annotation) stack.peek()).getStartIndex() + ((Annotation) stack.peek()).size()) <= t)) {
				Annotation annotation = ((Annotation) stack.pop());
				
				if (line != null) {
					tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
					line = null;
					lastWasLineBreak = false;
				}
				
				if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType()))
					tr.storeToken("</a>", 0);
					
				else {
					
					//	line break only if nested Annotations
					if (!lastWasLineBreak && annotation.hasAttribute(LINE_BROKEN))
						tr.storeToken("<br>", 0);
					
					tr.storeToken("<span style=\"" + this.docMarkupTextStyle + "\">", 0);
					tr.storeToken(prepareForHtml(("</" + annotation.getType() + ">"), HTML_CHAR_MAPPING), 0);
					tr.storeToken("<br>", 0);
					tr.storeToken("</span>", 0);
					
					//	add links to external sources
					for (int l = 0; l < linkers.length; l++) try {
						SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
						if (links != null)
							for (int k = 0; k < links.length; k++)
								this.writeInLineResultLink(tr, links[k]);
					}
					catch (Exception e) {
						System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
						e.printStackTrace(System.out);
					}
					
					lastWasLineBreak = true;
					lastWasTag = true;
				}
			}
			
			//	skip space character before unspaced punctuation (e.g. ',') or if explicitly told so
			if (((lastToken == null) || !lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) && Gamta.insertSpace(lastToken, token) && (line != null)) line.append(" ");
			
			//	write start tags for Annotations beginning at current Token
			while ((annotationPointer < nestedAnnotations.length) && (nestedAnnotations[annotationPointer].getStartIndex() == t)) {
				Annotation annotation = nestedAnnotations[annotationPointer];
				annotationPointer++;
				
				if (line != null) {
					tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
					line = null;
					lastWasTag = false;
				}
				
				if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType())) {
					String href = annotation.getAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, "").toString();
					if (href.length() != 0) {
						String title = annotation.getAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, href).toString();
						href = ("./" + DEFAULT_SEARCH_MODE + "?" + href);
						tr.storeToken(("<a href=\"" + href + "\" title=\"" + title + "\">"), 0);
						stack.push(annotation);
					}
				}
				else {
					stack.push(annotation);
					
					if (!lastWasTag) tr.storeToken("<br>", 0);
					tr.storeToken("<span style=\"" + this.docMarkupTextStyle + "\">", 0);
					this.writeStartTag(annotation, tr);
					
					//	line break only if nested Annotations
					int lookahead = 0;
					boolean nested = false;
					while (!nested && ((annotationPointer + lookahead) < nestedAnnotations.length)) {
						if (AnnotationUtils.contains(annotation, nestedAnnotations[annotationPointer + lookahead])) {
							if (SEARCH_LINK_ANNOTATION_TYPE.equals(nestedAnnotations[annotationPointer + lookahead].getType())) lookahead++;
							else nested = true;
						} else lookahead = nestedAnnotations.length;
					}
					if (nested) {
						tr.storeToken("<br>", 0);
						annotation.setAttribute(LINE_BROKEN, LINE_BROKEN);
					}
					tr.storeToken("</span>", 0);
					
					lastWasLineBreak = true;
					lastWasTag = true;
				}
			}
			
			//	add line break at end of paragraph
			if ((lastToken != null) && lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE) && (line != null)) {
				tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
				line = null;
				tr.storeToken("<br>", 0);
			}
			
			//	append current Token
			if (line == null) line = new StringBuffer(token.getValue());
			else line.append(token.getValue());
			lastWasLineBreak = false;
			lastWasTag = false;
		}
		
		//	add last line (if any)
		if (line != null) {
			tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
			line = null;
			lastWasLineBreak = false;
		}
		
		//	write end tags for Annotations not closed so far
		while (stack.size() > 0) {
			Annotation annotation = ((Annotation) stack.pop());
			
			if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType()))
				tr.storeToken("</a>", 0);
				
			else {
				
				//	line break only if nested Annotations
				if (!lastWasLineBreak && annotation.hasAttribute(LINE_BROKEN))
					tr.storeToken("<br>", 0);
				
				tr.storeToken("<span style=\"" + this.docMarkupTextStyle + "\">", 0);
				tr.storeToken(prepareForHtml(("</" + annotation.getType() + ">"), HTML_CHAR_MAPPING), 0);
				tr.storeToken("</span>", 0);
				
				//	add links to external sources
				for (int l = 0; l < linkers.length; l++) try {
					SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
					if (links != null)
						for (int k = 0; k < links.length; k++)
							this.writeInLineResultLink(tr, links[k]);
				}
				catch (Exception e) {
					System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
					e.printStackTrace(System.out);
				}
				
				lastWasLineBreak = false;
			}
		}
	}
	
	private void writeStartTag(Annotation annotation, HtmlPageBuilder tr) throws IOException {
		
		//	add hyperlinks to attribute values
		String attributeNames[] = annotation.getAttributeNames();
		String tagLink = null;
		Properties attributesToLinkQueries = new Properties();
		
		//	for each non-"_" attribute, search for SEARCH_LINK_QUERY_ATTRIBUTE + _attribute and SEARCH_LINK_TITLE_ATTRIBUTE + _attribute
		for (int a = 0; a < attributeNames.length; a++) {
			if (attributeNames[a].startsWith(SEARCH_LINK_QUERY_ATTRIBUTE)) {
				System.out.println("Got search link: " + attributeNames[a]);
				String linkedAttribute = attributeNames[a].substring(SEARCH_LINK_QUERY_ATTRIBUTE.length());
				System.out.println("  linked attribute is: " + linkedAttribute);
				if (annotation.hasAttribute(linkedAttribute)) {
					System.out.println("  linked attribute value: " + annotation.getAttribute(linkedAttribute, "").toString());
					attributesToLinkQueries.setProperty(linkedAttribute, annotation.getAttribute(attributeNames[a], "").toString());
				}
				else if (linkedAttribute.length() == 0) tagLink = annotation.getAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, "").toString();
			}
		}
		
		//	if no link exists, use normal start tag
		if ((tagLink == null) && attributesToLinkQueries.isEmpty())
			tr.storeToken(prepareForHtml(AnnotationUtils.produceStartTag(annotation, new HashSet() {
				public boolean contains(Object o) {
					return ((o != null) && !o.toString().startsWith("_"));
				}
			}, true), HTML_CHAR_MAPPING), 0);
		
		//	if link exists, add hyperlink to attribute value (produce tag here in that case)
		else {
			
			String token = "<";
			if (tagLink == null) token += annotation.getType() + " ";
			else {
				tr.storeToken(prepareForHtml(token, HTML_CHAR_MAPPING), 0);
				tr.storeToken(("<a href=\"./" + DEFAULT_SEARCH_MODE + "?" + tagLink + "\" title=\"" + annotation.getAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, tagLink) + "\">"), 0);
				tr.storeToken(prepareForHtml(annotation.getType(), HTML_CHAR_MAPPING), 0);
				tr.storeToken("</a>", 0);
				token = " ";
			}
			for (int i = 0; i < attributeNames.length; i++) {
				if (!attributeNames[i].startsWith("_")) {
					String attribute = annotation.getAttribute(attributeNames[i], "").toString();
					if (attributesToLinkQueries.containsKey(attributeNames[i])) {
						token += (" " + attributeNames[i] + "=\"");
						tr.storeToken(prepareForHtml(token, HTML_CHAR_MAPPING), 0);
						
						String link = attributesToLinkQueries.getProperty(attributeNames[i]);
						tr.storeToken(("<a href=\"./" + DEFAULT_SEARCH_MODE + "?" + link + "\" title=\"" + annotation.getAttribute((SEARCH_LINK_TITLE_ATTRIBUTE + attributeNames[i]), link) + "\">"), 0);
						tr.storeToken(prepareForHtml(AnnotationUtils.escapeForXml(attribute), HTML_CHAR_MAPPING), 0);
						tr.storeToken("</a>", 0);
						token = "\" ";
					}
					else token += (" " + attributeNames[i] + "=\"" + AnnotationUtils.escapeForXml(attribute, true) + "\"");
				}
			}
			token = (token.startsWith("\"") ? "" : " ") + token.trim() + ">";
			tr.storeToken(prepareForHtml(token, HTML_CHAR_MAPPING), 0);
		}
	}
	
	private static final String ANONYMOUS_BOX_PART_TYPE = "anonymousBoxPart";
	private static final String POSITON_ATTRIBUTE = "_annotationArrayPosition";
	
	private void includeDocumentBodyBoxed(MutableAnnotation doc, HtmlPageBuilder tr, String boxContentTextStyle, boolean writeNonBoxedContent) throws IOException {
		SearchResultLinker[] linkers = this.parent.getResultLinkers();
		
		QueriableAnnotation[] nestedAnnotations = doc.getAnnotations();
		int annotationPointer = 0;
		
		//	skip (jump) generic root tags
		while (
				((annotationPointer + 1) < nestedAnnotations.length)
				&&
				(DocumentRoot.DOCUMENT_TYPE.equals(nestedAnnotations[annotationPointer].getType()) 
				&& 
				(nestedAnnotations[annotationPointer].size() == nestedAnnotations[annotationPointer + 1].size()))
			) annotationPointer++;
		
		Stack stack = new Stack();
		
		Token token = null;
		Token lastToken;
		StringBuffer line = null;
		
		Annotation openBox = null;
		Annotation openBoxLine = null;
		
		int lineStartIndex = 0;
		int lastBoxEnd = 0;
		
		for (int t = 0; t < doc.size(); t++) {
			
			//	switch to next Token
			lastToken = token;
			token = doc.tokenAt(t);
			
			//	write end tags for Annotations ending before current Token
			while ((stack.size() > 0) && (((Annotation) stack.peek()).getEndIndex() <= t)) {
				Annotation annotation = ((Annotation) stack.pop());
				
				if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType())) {
					
					if (line != null) {
						if ((openBox != null) && (openBoxLine == null)) {
							tr.storeToken("<tr>", 0);
							tr.storeToken("<td width=\"20%\">", 0);
							tr.storeToken("<!--Link1-->", 0);
							tr.storeToken("</td>", 0);
							tr.storeToken("<td>", 0);
							tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
							tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
							tr.storeToken("</span>", 0);
							tr.storeToken("</td>", 0);
							tr.storeToken("</tr>", 0);
						}
						else {
							if (writeNonBoxedContent || (openBox != null))
								tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						}
						line = null;
					}
					
					tr.storeToken("</a>", 0);
				}
				
				//	end of a boxed part
				else if (annotation == openBox) {
					
					//	write unlabeled box content 
					if (line != null) {
						if (openBoxLine == null) {
							tr.storeToken("<tr>", 0);
							tr.storeToken("<td width=\"20%\">", 0);
							tr.storeToken("<!--End Of Box-->", 0);
							tr.storeToken("</td>", 0);
							tr.storeToken("<td>", 0);
							tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
							tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
							tr.storeToken("</span>", 0);
							tr.storeToken("</td>", 0);
							tr.storeToken("</tr>", 0);
						}
						else tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						line = null;
					}
					
					//	create links to external sources
					ArrayList linkList = new ArrayList();
					for (int l = 0; l < linkers.length; l++) try {
						SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
						if (links != null)
							for (int k = 0; k < links.length; k++)
								linkList.add(links[k]);
					}
					catch (Exception e) {
						System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
						e.printStackTrace(System.out);
					}
					
					//	add links of box (if any)
					String[] inLineLinkTokens = this.getInLineLinks(annotation, boxContentTextStyle);
					if ((inLineLinkTokens.length != 0) || !linkList.isEmpty()) {
						
						tr.storeToken("<tr>", 0);
						tr.storeToken("<td colspan=\"2\">", 0);
						tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
						
						for (int l = 0; l < inLineLinkTokens.length; l++)
							tr.storeToken(inLineLinkTokens[l], 0);
						
						tr.storeToken("</span>", 0);
						
						for (int l = 0; l < linkers.length; l++)
							this.writeInLineResultLink(tr, ((SearchResultLink) linkList.get(l)));
						
						tr.storeToken("</td>", 0);
						tr.storeToken("</tr>", 0);
					}
					
					//	close box
					tr.storeToken("</table>", 0);
					tr.storeToken("</div>", 0);
					
					openBox = null;
					lastBoxEnd = t;
				}
				
				//	end of a labelled part in a boxed part
				else if (annotation == openBoxLine) {
					
					//	write content of labelled part
					if (line != null) {
						tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						line = null;
					}
					
					//	add search links
					String[] inLineLinkTokens = this.getInLineLinks(annotation, boxContentTextStyle);
					for (int l = 0; l < inLineLinkTokens.length; l++)
						tr.storeToken(inLineLinkTokens[l], 0);
					
					//	add links to external sources
					for (int l = 0; l < linkers.length; l++) try {
						SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
						if (links != null)
							for (int k = 0; k < links.length; k++)
								this.writeInLineResultLink(tr, links[k]);
					}
					catch (Exception e) {
						System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
						e.printStackTrace(System.out);
					}
					
					//	close labelled part of box
					tr.storeToken("</span>", 0);
					tr.storeToken("</td>", 0);
					tr.storeToken("</tr>", 0);
					
					openBoxLine = null;
				}
				else {
					if (writeNonBoxedContent || (openBox != null)) {
						String[] inLineLinkTokens = this.getInLineLinks(annotation, boxContentTextStyle);
						
						//	get links to external sources
						ArrayList linkList = new ArrayList();
						for (int l = 0; l < linkers.length; l++) try {
							SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
							if (links != null)
								for (int k = 0; k < links.length; k++)
									linkList.add(links[k]);
						}
						catch (Exception e) {
							System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
							e.printStackTrace(System.out);
						}
						
						if ((inLineLinkTokens.length + linkList.size()) != 0) {
							
							if (line != null) {
								tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
								line = null;
							}
							
							for (int l = 0; l < inLineLinkTokens.length; l++)
								tr.storeToken(inLineLinkTokens[l], 0);
							
							//	add links to external sources
							for (int l = 0; l < linkList.size(); l++) try {
								SearchResultLink link = ((SearchResultLink) linkList.get(l));
								if (link != null) this.writeInLineResultLink(tr, link);
							}
							catch (Exception e) {
								System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
								e.printStackTrace(System.out);
							}
						}
					}
				}
			}
			
			//	skip space character before unspaced punctuation (e.g. ',') or if explicitly told so
			if (((lastToken == null) || !lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) && Gamta.insertSpace(lastToken, token) && (line != null)) line.append(" ");
			
			//	write start tags for Annotations beginning at current Token
			while ((annotationPointer < nestedAnnotations.length) && (nestedAnnotations[annotationPointer].getStartIndex() == t)) {
				QueriableAnnotation annotation = nestedAnnotations[annotationPointer];
				annotationPointer++;
				
				if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType())) {
					
					String href = annotation.getAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, "").toString();
					if (href.length() != 0) {
						
						if (line != null) {
							if ((openBox != null) && (openBoxLine == null)) {
								tr.storeToken("<tr>", 0);
								tr.storeToken("<td width=\"20%\">", 0);
								tr.storeToken("<!--Link2-->", 0);
								tr.storeToken("</td>", 0);
								tr.storeToken("<td>", 0);
								tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
								tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
								tr.storeToken("</span>", 0);
								tr.storeToken("</td>", 0);
								tr.storeToken("</tr>", 0);
							}
							else {
								if (writeNonBoxedContent)
									tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
							}
							line = null;
						}
						
						if (writeNonBoxedContent || (openBox != null)) {
							String title = annotation.getAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, href).toString();
							href = ("./" + DEFAULT_SEARCH_MODE + "?" + href);
							tr.storeToken(("<a href=\"" + href + "\" title=\"" + title + "\">"), 0);
							stack.push(annotation);
						}
					}
				}
				
				//	inside a box
				else if (openBox != null) {
					
					if (annotation.hasAttribute(BOX_PART_LABEL_ATTRIBUTE)) {
						tr.storeToken(("<!-- (t=" + t + ") start of " + annotation.getType() + ":" + annotation.getAttribute(BOX_PART_LABEL_ATTRIBUTE) + "-" + annotation.getStartIndex() + "-" + annotation.getEndIndex() + " -->"), 0);
					}
					
					//	start of labelled part in box
					if ((openBoxLine == null) && annotation.hasAttribute(BOX_PART_LABEL_ATTRIBUTE)) {
						
						//	write unlabeled box content 
						if (line != null) {
							tr.storeToken("<tr>", 0);
							tr.storeToken("<td width=\"20%\">", 0);
							tr.storeToken("<!--Before Box Line-->", 0);
							tr.storeToken("</td>", 0);
							tr.storeToken("<td>", 0);
							tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
							tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
							tr.storeToken("</span>", 0);
							tr.storeToken("</td>", 0);
							tr.storeToken("</tr>", 0);
							line = null;
						}
						
						if (annotation.hasAttribute(BOX_PART_LABEL_ATTRIBUTE))
							tr.storeToken(("<!-- (t=" + t + ") opening " + annotation.getType() + "-" + annotation.getStartIndex() + "-" + annotation.getEndIndex() + " -->"), 0);
						
						//	add box part label title given
						String boxPartLabel = annotation.getAttribute(BOX_PART_LABEL_ATTRIBUTE, "").toString();
						tr.storeToken("<tr>", 0);
						tr.storeToken("<td width=\"20%\">", 0);
						tr.storeToken("<span style=\"" + this.tableEntryTextStyle + "\">", 0);
						tr.storeToken("<b>", 0);
						tr.storeToken(prepareForHtml(boxPartLabel, HTML_CHAR_MAPPING), 0);
						tr.storeToken("</b>", 0);
						tr.storeToken("</span>", 0);
						tr.storeToken("</td>", 0);
						tr.storeToken("<td>", 0);
						tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
						
						openBoxLine = annotation;
						stack.push(annotation);
					}
					else {
						//	write hyperlink line individually
						if ((line != null) && ((line.indexOf("tp://") != -1) || (line.indexOf("tp: //") != -1))) {
							if (openBoxLine == null) {
								tr.storeToken("<tr>", 0);
								tr.storeToken("<td width=\"20%\">", 0);
								tr.storeToken("<!--URL-->", 0);
								tr.storeToken("</td>", 0);
								tr.storeToken("<td>", 0);
								tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
								tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
								tr.storeToken("</span>", 0);
								tr.storeToken("</td>", 0);
								tr.storeToken("</tr>", 0);
							}
							else {
								if (openBoxLine.getStartIndex() < lineStartIndex)
									tr.storeToken("<br>", 0);
								tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
								if (openBoxLine.getEndIndex() > t)
									tr.storeToken("<br>", 0);
							}
							line = null;
						}
						stack.push(annotation);
					}
				}
				
				//	start of a box
				else if (annotation.hasAttribute(BOXED_ATTRIBUTE)) {
					
					if (line != null) {
						tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						line = null;
					}
					
					//	add spacer
					if (lastBoxEnd == t) tr.storeToken("<br>", 0);
					
					//	make sure box is parquetted with box parts
					Annotation[] boxContent = annotation.getAnnotations(); 
					ArrayList boxParts = new ArrayList();
					for (int a = 0; a < boxContent.length; a++) {
						if (boxContent[a].hasAttribute(BOX_PART_LABEL_ATTRIBUTE))
							boxParts.add(boxContent[a]);
					}
					boolean anonymousBoxParts = false;
					int lastBoxPartEnd = 0;
					for (int p = 0; p < boxParts.size(); p++) {
						Annotation boxPart = ((Annotation) boxParts.get(p));
						if (lastBoxPartEnd < boxPart.getStartIndex()) {
							int anonymousBoxPartEnd = boxPart.getStartIndex();
							while (lastBoxPartEnd < boxPart.getStartIndex()) {
								for (int a = 0; a < boxContent.length; a++) {
									
									//	lastBoxPartEnd inside annotation, make sure anonymousBoxPartEnd is annotation's end at max
									if ((boxContent[a].getStartIndex() < lastBoxPartEnd)
										&& (boxContent[a].getEndIndex() > lastBoxPartEnd)
										&& (boxContent[a].getEndIndex() < anonymousBoxPartEnd)
										) anonymousBoxPartEnd = Math.min(anonymousBoxPartEnd, boxContent[a].getEndIndex());
									
									//	lastBoxPartEnd before annotation, make sure anonymousBoxPartEnd that if anonymousBoxPartEnd does not span annotation's end, it is annotation's start at max
									if ((lastBoxPartEnd < boxContent[a].getStartIndex())
											&& (anonymousBoxPartEnd > boxContent[a].getStartIndex())
											&& (anonymousBoxPartEnd < boxContent[a].getEndIndex())
											) anonymousBoxPartEnd = Math.min(anonymousBoxPartEnd, boxContent[a].getStartIndex());
									
								}
								if (anonymousBoxPartEnd <= lastBoxPartEnd) {
									tr.storeToken("<!-- BoxPart Overlay Malfunction -->", 0);
									lastBoxPartEnd = boxPart.getStartIndex(); // break loop
								}
								else {
									Annotation anonymousBoxPart = doc.addAnnotation(ANONYMOUS_BOX_PART_TYPE, (annotation.getStartIndex() + lastBoxPartEnd), (anonymousBoxPartEnd - lastBoxPartEnd));
									anonymousBoxPart.setAttribute(BOX_PART_LABEL_ATTRIBUTE, "");
									lastBoxPartEnd = anonymousBoxPartEnd;
									anonymousBoxPartEnd = boxPart.getStartIndex();
								}
							}
							anonymousBoxParts = true;
						}
						lastBoxPartEnd = boxPart.getEndIndex();
					}
					if (lastBoxPartEnd < annotation.size()) {
						int anonymousBoxPartEnd = annotation.size();
						while (lastBoxPartEnd < annotation.size()) {
							for (int a = 0; a < boxContent.length; a++) {
								
								//	lastBoxPartEnd inside annotation, make sure anonymousBoxPartEnd is annotation's end at max
								if ((boxContent[a].getStartIndex() < lastBoxPartEnd)
									&& (boxContent[a].getEndIndex() > lastBoxPartEnd)
									&& (boxContent[a].getEndIndex() < anonymousBoxPartEnd)
									) anonymousBoxPartEnd = Math.min(anonymousBoxPartEnd, boxContent[a].getEndIndex());
								
								//	lastBoxPartEnd before annotation, make sure anonymousBoxPartEnd that if anonymousBoxPartEnd does not span annotation's end, it is annotation's start at max
								if ((lastBoxPartEnd < boxContent[a].getStartIndex())
										&& (anonymousBoxPartEnd > boxContent[a].getStartIndex())
										&& (anonymousBoxPartEnd < boxContent[a].getEndIndex())
										) anonymousBoxPartEnd = Math.min(anonymousBoxPartEnd, boxContent[a].getStartIndex());
							}
							if (anonymousBoxPartEnd <= lastBoxPartEnd) {
								tr.storeToken("<!-- BoxPart Overlay Malfunction -->", 0);
								lastBoxPartEnd = annotation.size(); // break loop
							}
							else {
								Annotation anonymousBoxPart = doc.addAnnotation(ANONYMOUS_BOX_PART_TYPE, (annotation.getStartIndex() + lastBoxPartEnd), (anonymousBoxPartEnd - lastBoxPartEnd));
								anonymousBoxPart.setAttribute(BOX_PART_LABEL_ATTRIBUTE, "");
								lastBoxPartEnd = anonymousBoxPartEnd;
								anonymousBoxPartEnd = annotation.size();
							}
						}
						anonymousBoxParts = true;
					}
					if (anonymousBoxParts) {
						nestedAnnotations = doc.getAnnotations();
						for (int a = 0; a < nestedAnnotations.length; a++)
							nestedAnnotations[a].setAttribute(POSITON_ATTRIBUTE, ("" + a));
						final int anp = annotationPointer;
						Arrays.sort(nestedAnnotations, new Comparator() {
							public int compare(Object o1, Object o2) {
								Annotation a1 = ((Annotation) o1);
								Annotation a2 = ((Annotation) o2);
								
								//	check document order
								int c = AnnotationUtils.compare(a1, a2);
								if (c != 0) return c;
								
								//	check position in old array
								int p1 = Integer.parseInt(a1.getAttribute(POSITON_ATTRIBUTE).toString());
								int p2 = Integer.parseInt(a2.getAttribute(POSITON_ATTRIBUTE).toString());
								
								//	at least on outside box annotation
								if ((p1 < anp) || (p2 < anp)) return (p1 - p2);
								
								//	move anonymous box parts to front
								else if (ANONYMOUS_BOX_PART_TYPE.equals(a1.getType())) return -1;
								else if (ANONYMOUS_BOX_PART_TYPE.equals(a2.getType())) return 1;
								
								//	preserve original order
								else return (p1 - p2);
							}
						});
						annotation = nestedAnnotations[annotationPointer-1];
						
						boxContent = annotation.getAnnotations();
						StringBuffer annoBoxLog = new StringBuffer();
						for (int a = 0; a < boxContent.length; a++) {
							if (boxContent[a].hasAttribute(BOX_PART_LABEL_ATTRIBUTE))
								annoBoxLog.append(((annoBoxLog.length() == 0) ? "" : ", ") + boxContent[a].getType() + "-(pos " + boxContent[a].getAttribute(POSITON_ATTRIBUTE) + "): " + boxContent[a].getStartIndex() + "-" + boxContent[a].getEndIndex());
						}
						tr.storeToken("<!-- " + annoBoxLog.toString() + " -->", 0);
					}
					
					//	open box
					tr.storeToken("<div align=\"center\">", 0);
					tr.storeToken("<table width=\"80%\" bgcolor=\"" + this.labelBackgroundColor + "\">", 0);
					
					//	add box title line if title given
					String boxTitle = annotation.getAttribute(BOX_TITLE_ATTRIBUTE, "").toString();
					if (boxTitle.length() != 0) {
						tr.storeToken("<tr bgcolor=\"" + this.titleBackgroundColor + "\">", 0);
						tr.storeToken("<td colspan=\"2\">", 0);
						tr.storeToken("<span style=\"" + this.labelTextStyle + "\">", 0);
						tr.storeToken("<b>", 0);
						tr.storeToken(prepareForHtml(boxTitle, HTML_CHAR_MAPPING), 0);
						tr.storeToken("</b>", 0);
						tr.storeToken("</span>", 0);
						tr.storeToken("</td>", 0);
						tr.storeToken("</tr>", 0);
					}
					
					openBox = annotation;
					stack.push(annotation);
				}
				else {
					//	write hyperlink line individually
					if ((line != null) && ((line.indexOf("tp://") != -1) || (line.indexOf("tp: //") != -1))) {
						if (writeNonBoxedContent)
							tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						line = null;
					}
					if (writeNonBoxedContent)
						stack.push(annotation);
				}
			}
			
			//	add line break at end of paragraph
			if ((lastToken != null) && lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) {
				if (line != null) {
					if ((openBox != null) && (openBoxLine == null)) {
						tr.storeToken("<tr>", 0);
						tr.storeToken("<td width=\"20%\">", 0);
						tr.storeToken("<!--ParaEnd-->", 0);
						tr.storeToken("</td>", 0);
						tr.storeToken("<td>", 0);
						tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
						tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						tr.storeToken("</span>", 0);
						tr.storeToken("</td>", 0);
						tr.storeToken("</tr>", 0);
					}
					else {
						if (writeNonBoxedContent || (openBox != null))
							tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
					}
					line = null;
				}
				if ((openBox == null) && writeNonBoxedContent) tr.storeToken("<br>", 0);
			}
			
			//	append current Token
			if (line == null) {
				line = new StringBuffer(token.getValue());
				lineStartIndex = t;
			}
			else line.append(token.getValue());
		}
		
		//	add last line (if any)
		if (line != null) {
			if ((openBox != null) && (openBoxLine == null)) {
				tr.storeToken("<tr>", 0);
				tr.storeToken("<td width=\"20%\">", 0);
				tr.storeToken("<!--Last Line-->", 0);
				tr.storeToken("</td>", 0);
				tr.storeToken("<td>", 0);
				tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
				tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
				tr.storeToken("</span>", 0);
				tr.storeToken("</td>", 0);
				tr.storeToken("</tr>", 0);
			}
			else {
				if (writeNonBoxedContent || (openBox != null))
					tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
			}
			line = null;
		}
		
		//	add spacer if text not displayed
		if (!writeNonBoxedContent && (openBox == null))
			tr.storeToken("<br>", 0);
		
		//	write end tags for Annotations not closed so far
		while (stack.size() > 0) {
			Annotation annotation = ((Annotation) stack.pop());
			
			if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType()))
				tr.storeToken("</a>", 0);
				
			//	end of a boxed part
			else if (annotation == openBox) {
				
				//	add links of box (if any)
				String[] inLineLinkTokens = this.getInLineLinks(annotation, boxContentTextStyle);
				if (inLineLinkTokens.length != 0) {
					
					tr.storeToken("<tr>", 0);
					tr.storeToken("<td colspan=\"2\">", 0);
					tr.storeToken("<span style=\"" + boxContentTextStyle + "\">", 0);
					
					for (int l = 0; l < inLineLinkTokens.length; l++)
						tr.storeToken(inLineLinkTokens[l], 0);
					
					tr.storeToken("</span>", 0);
					tr.storeToken("</td>", 0);
					tr.storeToken("</tr>", 0);
				}
				
				//	close box
				tr.storeToken("</table>", 0);
				tr.storeToken("</div>", 0);
				
				//	add links to external sources
				for (int l = 0; l < linkers.length; l++) try {
					SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
					if (links != null)
						for (int k = 0; k < links.length; k++)
							this.writeInLineResultLink(tr, links[k]);
				}
				catch (Exception e) {
					System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
					e.printStackTrace(System.out);
				}
				
				//	add spacer at document end so box does not cling to bottom line
				tr.storeToken("<br>", 0);
				
				openBox = null;
			}
			
			//	end of a labelled part in a boxed part
			else if (annotation == openBoxLine) {
				
				//	add serach links
				String[] inLineLinkTokens = this.getInLineLinks(annotation, boxContentTextStyle);
				for (int l = 0; l < inLineLinkTokens.length; l++)
					tr.storeToken(inLineLinkTokens[l], 0);
				
				//	add links to external sources
				for (int l = 0; l < linkers.length; l++) try {
					SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
					if (links != null)
						for (int k = 0; k < links.length; k++)
							this.writeInLineResultLink(tr, links[k]);
				}
				catch (Exception e) {
					System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
					e.printStackTrace(System.out);
				}
				
				//	close labelled part of box
				tr.storeToken("</td>", 0);
				tr.storeToken("</tr>", 0);
				
				openBoxLine = null;
			}
			else {
				if (writeNonBoxedContent || (openBox != null)) {
					String[] inLineLinkTokens = this.getInLineLinks(annotation, ((openBox == null) ? docContentTextStyle : boxContentTextStyle));
					for (int l = 0; l < inLineLinkTokens.length; l++)
						tr.storeToken(inLineLinkTokens[l], 0);
					
					//	add links to external sources
					for (int l = 0; l < linkers.length; l++) try {
						SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
						if (links != null)
							for (int k = 0; k < links.length; k++)
								this.writeInLineResultLink(tr, links[k]);
					}
					catch (Exception e) {
						System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
						e.printStackTrace(System.out);
					}
				}
			}
		}
	}
	
	private void includeDocumentBodyPlain(MutableAnnotation doc, HtmlPageBuilder tr) throws IOException {
		SearchResultLinker[] linkers = this.parent.getResultLinkers();
		
		Annotation[] nestedAnnotations = doc.getAnnotations();
		int annotationPointer = 0;
		
		//	skip (jump) generic root tags
		while (
				((annotationPointer + 1) < nestedAnnotations.length)
				&&
				(DocumentRoot.DOCUMENT_TYPE.equals(nestedAnnotations[annotationPointer].getType()) 
				&& 
				(nestedAnnotations[annotationPointer].size() == nestedAnnotations[annotationPointer + 1].size()))
			) annotationPointer++;
		
		Stack stack = new Stack();
		
		Token token = null;
		Token lastToken;
		StringBuffer line = null;
		
		for (int t = 0; t < doc.size(); t++) {
			
			//	switch to next Token
			lastToken = token;
			token = doc.tokenAt(t);
			
			//	write end tags for Annotations ending before current Token
			while ((stack.size() > 0) && ((((Annotation) stack.peek()).getStartIndex() + ((Annotation) stack.peek()).size()) <= t)) {
				Annotation annotation = ((Annotation) stack.pop());
				
				if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType())) {
					
					if (line != null) {
						tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						line = null;
					}
					
					tr.storeToken("</a>", 0);
				}
				else {
					String[] inLineLinkTokens = this.getInLineLinks(annotation, this.docContentTextStyle);
					if (inLineLinkTokens.length != 0) {
						
						if (line != null) {
							tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
							line = null;
						}
						
						for (int l = 0; l < (inLineLinkTokens.length - 1); l++)
							tr.storeToken(inLineLinkTokens[l], 0);
						
						line = new StringBuffer(inLineLinkTokens[inLineLinkTokens.length - 1]);
					}
					
					//	add links to external sources
					for (int l = 0; l < linkers.length; l++) try {
						SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
						if (links != null)
							for (int k = 0; k < links.length; k++)
								this.writeInLineResultLink(tr, links[k]);
					}
					catch (Exception e) {
						System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
						e.printStackTrace(System.out);
					}
				}
			}
			
			//	skip space character before unspaced punctuation (e.g. ',') or if explicitly told so
			if (((lastToken == null) || !lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) && Gamta.insertSpace(lastToken, token) && (line != null)) line.append(" ");
			
			//	write start tags for Annotations beginning at current Token
			while ((annotationPointer < nestedAnnotations.length) && (nestedAnnotations[annotationPointer].getStartIndex() == t)) {
				Annotation annotation = nestedAnnotations[annotationPointer];
				annotationPointer++;
				
				if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType())) {
					
					if (line != null) {
						tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						line = null;
					}
					
					String href = annotation.getAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, "").toString();
					if (href.length() != 0) {
						String title = annotation.getAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, href).toString();
						href = ("./" + DEFAULT_SEARCH_MODE + "?" + href);
						tr.storeToken(("<a href=\"" + href + "\" title=\"" + title + "\">"), 0);
						stack.push(annotation);
					}
				}
				else {
					//	write line at end of annotation
					if (line != null) {
						tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
						line = null;
					}
					stack.push(annotation);
				}
			}
			
			//	add line break at end of paragraph
			if ((lastToken != null) && lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) {
				if (line != null) {
					tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
					line = null;
				}
				tr.storeToken("<br>", 0);
			}
			
			//	append current Token
			if (line == null) line = new StringBuffer(token.getValue());
			else line.append(token.getValue());
		}
		
		//	add last line (if any)
		if (line != null) {
			tr.storeToken(prepareForHtml(line.toString(), HTML_CHAR_MAPPING), 0);
			line = null;
		}
		
		//	write end tags for Annotations not closed so far
		while (stack.size() > 0) {
			Annotation annotation = ((Annotation) stack.pop());
			
			if (SEARCH_LINK_ANNOTATION_TYPE.equals(annotation.getType())) {
				tr.storeToken("</a>", 0);
				
			} else {
				String[] inLineLinkTokens = this.getInLineLinks(annotation, this.docContentTextStyle);
				for (int l = 0; l < inLineLinkTokens.length; l++)
					tr.storeToken(inLineLinkTokens[l], 0);
				
				//	add links to external sources
				for (int l = 0; l < linkers.length; l++) try {
					SearchResultLink[] links = linkers[l].getAnnotationLinks(annotation);
					if (links != null)
						for (int k = 0; k < links.length; k++)
							this.writeInLineResultLink(tr, links[k]);
				}
				catch (Exception e) {
					System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
					e.printStackTrace(System.out);
				}
			}
		}
	}
	
	private String[] getInLineLinks(Annotation annotation, String linkTextStyle) throws IOException {
		
		//	add hyperlinks to attribute values
		String attributeNames[] = annotation.getAttributeNames();
		String tagLink = null;
		Properties attributesToLinkQueries = new Properties();
		
		//	for each non-"_" attribute, search for SEARCH_LINK_QUERY_ATTRIBUTE + _attribute and SEARCH_LINK_TITLE_ATTRIBUTE + _attribute
		for (int a = 0; a < attributeNames.length; a++) {
			if (attributeNames[a].startsWith(SEARCH_LINK_QUERY_ATTRIBUTE)) {
				String linkedAttribute = attributeNames[a].substring(SEARCH_LINK_QUERY_ATTRIBUTE.length());
				if (annotation.hasAttribute(linkedAttribute))
					attributesToLinkQueries.setProperty(linkedAttribute, annotation.getAttribute(attributeNames[a], "").toString());
				else if (linkedAttribute.length() == 0) tagLink = annotation.getAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, "").toString();
			}
		}
		
		//	if link exists, add square-bracketed part displaying attributes with hyperlinks
		StringVector collector = new StringVector();
		if ((tagLink != null) || !attributesToLinkQueries.isEmpty()) {
			
			//	open in line link part
			collector.addElement(" [[");
			collector.addElement("<span style=\"" + linkTextStyle + "\">");
			
			int writtenLinkCount = 0;
			
			//	write tag link if given
			if (tagLink != null) {
				collector.addElement("<a href=\"./" + DEFAULT_SEARCH_MODE + "?" + tagLink + "\" title=\"" + annotation.getAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, tagLink) + "\">");
				collector.addElement(prepareForHtml(annotation.getType(), HTML_CHAR_MAPPING));
				collector.addElement("</a>");
				writtenLinkCount ++;
			}
			for (int i = 0; i < attributeNames.length; i++) {
				if (!attributeNames[i].startsWith("_")) {
					String attribute = annotation.getAttribute(attributeNames[i], "").toString();
					if (attributesToLinkQueries.containsKey(attributeNames[i])) {
						String link = attributesToLinkQueries.getProperty(attributeNames[i]);
						
						if (writtenLinkCount != 0) collector.addElement(", ");
						
						collector.addElement("<a href=\"./" + DEFAULT_SEARCH_MODE + "?" + link + "\" title=\"" + annotation.getAttribute((SEARCH_LINK_TITLE_ATTRIBUTE + attributeNames[i]), link) + "\">");
						collector.addElement(prepareForHtml((attributeNames[i] + "=\"" + AnnotationUtils.escapeForXml(attribute, true) + "\""), HTML_CHAR_MAPPING));
						collector.addElement("</a>");
						
						writtenLinkCount ++;
					}
				}
			}
			
			//	close in line link part
			collector.addElement("</span>");
			collector.addElement("]]");
		}
		return collector.toStringArray();
	}
	
	private void writeInLineResultLink(HtmlPageBuilder tr, SearchResultLink link) throws IOException  {
		
		//	get optical representation of link
		String linkContent = null;
		if (link.iconUrl != null)
			linkContent = ("<img src=\"" + link.iconUrl + "\"" + ((link.label == null) ? "" : (" alt=\"" + link.label + "\"")) + ">");
		else if (link.label != null)
			linkContent = link.label;
		
		//	get function of link
		String linkFunction = null;
		if (link.href != null)
			linkFunction = "href=\"" + link.href + "\" target=\"_blank\"";
		else if (link.onclick != null)
			linkFunction = "onclick=\"" + link.onclick + "\"";
		
		//	add link if both given
		if ((linkContent != null) && (linkFunction != null)) {
			tr.storeToken(("<span style=\"" + this.utilityLinkTextStyle + "\">"), 0);
			tr.storeToken("<sup>", 0);
			tr.storeToken(("<a " + linkFunction + ((link.title == null) ? "" : (" title=\"" + link.title + "\"")) + ">"), 0);
			tr.storeToken(linkContent, 0);
			tr.storeToken("</a>", 0);
			tr.storeToken("</sup>", 0);
			tr.storeToken("</span>", 0);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeThesaurusForms(java.lang.String, de.goldenGateSrs.GoldenGateSrsConstants.SearchFieldGroup[], de.goldenGateSrs.GoldenGateSrsConstants.SearchFieldRow, java.util.Properties, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeThesaurusForms(String formTitle, SearchFieldGroup[] fieldGroups, SearchFieldRow buttonRowFields, Properties fieldValues, HtmlPageBuilder tr) throws IOException {
		
		//	open master table
		tr.storeToken(("<table border=\"2\" fame=\"box\" rules=\"none\" cellpadding=\"3\" bgcolor=\"" + this.tableEntryBackgroundColor + "\" bordercolor=\"" + this.tableBorderColor + "\" cellspacing=\"0\">"), 0);
		
		//	add title row
		if ((formTitle != null) && (formTitle.trim().length() != 0)) {
			tr.storeToken("<tr>", 0);
			tr.storeToken(("<td width=\"100%\" bgcolor=\"" + this.titleBackgroundColor + "\">"), 0);
			tr.storeToken(("<p style=\"" + this.titleTextStyle + "\" align=\"center\">"), 0);
			tr.storeToken("<b>", 0);
			tr.storeToken(prepareForHtml(formTitle, HTML_CHAR_MAPPING), 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		for (int g = 0; g < fieldGroups.length; g++) {
			
			//	open form
			tr.storeToken("<form method=\"GET\" action=\"" + tr.request.getContextPath() + "/" + THESAURUS_SEARCH_MODE + "\">", 0);
			
			//	open table row & table cell for field table
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td align=\"center\">", 0);
			
			//	open fieldset and write field group legend
			tr.storeToken("<fieldset>", 0);
			tr.storeToken("<legend>", 0);
//			tr.storeToken(prepareForHtml(fieldGroups[g].legend, HTML_CHAR_MAPPING), 0);
			tr.storeToken(prepareForHtml(fieldGroups[g].tooltip, HTML_CHAR_MAPPING), 0);
			tr.storeToken("</legend>", 0);
			
			//	open table for field group
			tr.storeToken("<table frame=\"box\" rules=\"none\">", 0);
			
			//	write field rows
			SearchFieldRow[] fieldRows = fieldGroups[g].getFieldRows();
			for (int r = 0; r < fieldRows.length; r++) {
				
				//	open table row
				tr.storeToken("<tr>", 0);
				
				//	write fields
				SearchField[] fields = fieldRows[r].getFields();
				for (int f = 0; f < fields.length; f++) {
					
					//	open table cell and write label
					tr.storeToken(("<td align=\"center\" colspan=\"" + fields[f].size + "\">"), 0);
					tr.storeToken(("<span style=\"" + this.labelTextStyle + "\">"), 0);
					tr.storeToken(prepareForHtml(fields[f].label, HTML_CHAR_MAPPING), 0);
					tr.storeToken("</span>", 0);
					tr.storeToken(" ", 0);
					
					//	write actual field
					if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
						tr.storeToken(("<input type=\"checkbox\" name=\"" + fields[f].name + "\" value=\"" + true + "\"" + (fieldValues.containsKey(fields[f].name) ? " checked=\"true\"" : "") + ">"), 0);
					
					else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
						tr.storeToken(("<select name=\"" + fields[f].name + "\">"), 0);
						
						SearchFieldOption[] fieldOptions = fields[f].getOptions();
						for (int o = 0; o < fieldOptions.length; o++) {
							tr.storeToken(("<option value=\"" + fields[f].value + "\"" + (fields[f].value.equals(fieldValues.getProperty(fields[f].name)) ? " selected=\"true\"" : "") + ">"), 0);
							tr.storeToken(prepareForHtml(fieldOptions[o].label, HTML_CHAR_MAPPING), 0);
							tr.storeToken("</option>", 0);
						}
						
						tr.storeToken("</select>", 0);
					}
					else
						tr.storeToken(("<input size=\"" + (fields[f].size * 20) + "\" name=\"" + fields[f].name + "\" value=\"" + fieldValues.getProperty(fields[f].name, fields[f].value) + "\">"), 0);
					
					//	close table cell
					tr.storeToken("</td>", 0);
				}
				
				//	close table row
				tr.storeToken("</tr>", 0);
			}
			
			//	close table and fieldset of field group
			tr.storeToken("</table>", 0);
			tr.storeToken("</fieldset>", 0);
			
			//	fieldset row
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
			
			//	open button row
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td align=\"center\">", 0);
			
			//	add button row fields (if any)
			if (buttonRowFields != null) {
				SearchField[] fields = buttonRowFields.getFields();
				for (int f = 0; f < fields.length; f++) {
					
					//	write label
					tr.storeToken(("<span style=\"" + this.labelTextStyle + "\">"), 0);
					tr.storeToken(prepareForHtml(fields[f].label, HTML_CHAR_MAPPING), 0);
					tr.storeToken("</span>", 0);
					tr.storeToken(" ", 0);
					
					//	write actual field
					if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
						tr.storeToken(("<input type=\"checkbox\" name=\"" + fields[f].name + "\" value=\"" + true + "\"" + ((fieldValues.containsKey(fields[f].name) || ((fields[f].value != null) && (fields[f].value.length() != 0))) ? " checked=\"true\"" : "") + ">"), 0);
					
					else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
						tr.storeToken(("<select name=\"" + fields[f].name + "\">"), 0);
						
						SearchFieldOption[] fieldOptions = fields[f].getOptions();
						for (int o = 0; o < fieldOptions.length; o++) {
							tr.storeToken(("<option value=\"" + fieldOptions[o].value + "\"" + (fieldOptions[o].value.equals(fieldValues.getProperty(fields[f].name)) ? " selected=\"true\"" : "") + ">"), 0);
							tr.storeToken(prepareForHtml(fieldOptions[o].label, HTML_CHAR_MAPPING), 0);
							tr.storeToken("</option>", 0);
						}
						
						tr.storeToken("</select>", 0);
					}
					else tr.storeToken(("<input size=\"" + (fields[f].size * 20) + "\" name=\"" + fields[f].name + "\" value=\"" + fieldValues.getProperty(fields[f].name, fields[f].value) + "\">"), 0);
					
					//	add spacer
					tr.storeToken("&nbsp;", 0);
				}
			}
			
			//	add buttons
			tr.storeToken("<input type=\"submit\" value=\"Search\">", 0);
			tr.storeToken("&nbsp;", 0);
			tr.storeToken("<input type=\"reset\" value=\"Clear\">", 0);
			
			//	close button row
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
			
			//	close form
			tr.storeToken("</form>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeThesaurusResult(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedThesaurusResult, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeThesaurusResult(BufferedThesaurusResult result, HtmlPageBuilder tr) throws IOException {
		
		//	open master table
		tr.storeToken("<table border=\"2\" frame=\"box\" rules=\"none\" bgcolor=\"" + tableEntryBackgroundColor + "\" bordercolor=\"" + tableBorderColor + "\" cellspacing=\"0\" cellpadding=\"3\">", 0);
		
		//	build label row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td width=\"100%\" bgcolor=\"" + titleBackgroundColor + "\">", 0);
		tr.storeToken("<p style=\"" + titleTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken(prepareForHtml((((result.thesaurusName == null) || (result.thesaurusName.length() == 0)) ? "Thesaurus Lookup Result" : result.thesaurusName), HTML_CHAR_MAPPING), 0);
		tr.storeToken("</b>", 0);
		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		if (result.isEmpty() || (result.resultAttributes.length == 0)) {
			
			//	add report of no results
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"center\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Your search did not return any results.", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		else {
			
			//	open data table
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"left\">", 0);
			tr.storeToken("<table width=\"98%\" border=\"1\" frame=\"box\" bgcolor=\"" + tableEntryBackgroundColor + "\" bordercolor=\"" + tableBorderColor + "\" cellspacing=\"0\">", 0);
			
			//	build table header row
			tr.storeToken("<tr bgcolor=\"" + labelBackgroundColor + "\">", 0);
			for (int f = 0; f < result.resultAttributes.length; f++) {
				tr.storeToken("<td align=\"center\">", 0);
				tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
				tr.storeToken("<b>", 0);
				
				String fieldName = result.resultAttributes[f];
				tr.storeToken((Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1)), 0);
				
				tr.storeToken("</b>", 0);
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
			}
			tr.storeToken("</tr>", 0);
			
			//	add data
			for (ThesaurusResult thr = result.getThesaurusResult(); thr.hasNextElement();) {
				ThesaurusResultElement tre = thr.getNextThesaurusResultElement();
				
				//	open data row
				tr.storeToken("<tr bgcolor=\"" + tableEntryBackgroundColor + "\">", 0);
				
				//	write cells
				for (int f = 0; f < result.resultAttributes.length; f++) {
					tr.storeToken("<td align=\"center\">", 0);
					tr.storeToken("<p style=\"" + tableEntryTextStyle + "\" align=\"center\">", 0);
					
					String fieldValue = ((String) tre.getAttribute(result.resultAttributes[f]));
					if ((fieldValue == null) || (fieldValue.length() == 0)) tr.storeToken("&nbsp;", 0);
					else tr.storeToken(prepareForHtml(fieldValue, HTML_CHAR_MAPPING), 0);
					
					tr.storeToken("</p>", 0);
					tr.storeToken("</td>", 0);
				}
				
				//	close data row
				tr.storeToken("</tr>", 0);
			}
			
			//	close data table
			tr.storeToken("</table>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeStatisticsLine(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedCollectionStatistics, de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalConstants.NavigationLink[], de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeStatisticsLine(BufferedCollectionStatistics statistics, NavigationLink[] links, HtmlPageBuilder tr) throws IOException {
		
		tr.storeToken("<span style=\"" + this.tableEntryTextStyle + "\">", 0);
		tr.storeToken("Search through&nbsp;", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken(((statistics == null) ? "0" : ("" + statistics.docCount)), 0);
		tr.storeToken("</b>", 0);
		tr.storeToken("&nbsp;" + this.parent.getDocumentLabelPlural().toLowerCase() + " (", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken(((statistics == null) ? "0" : ("" + statistics.wordCount)), 0);
		tr.storeToken("</b>", 0);
		tr.storeToken("&nbsp;words) in the archive.", 0);
		
		if (links != null) {
			for (int l = 0; l < links.length; l++) {
				tr.storeToken("&nbsp;", 0);
				
				String href = "";
				if (links[l].link != null)
					href = (" href=\"" + links[l].link + "\"" + ((links[l].target == null) ? "" : (" target=\"" + links[l].target + "\"")));
				
				String onclick = "";
				if (links[l].onclick != null)
					onclick = (" onclick=\"" + links[l].onclick + "\"");
				
				tr.storeToken(("<a" + href + onclick + ">"), 0);
				if (links[l].icon != null)
					tr.storeToken(("<img src=\"" + links[l].icon + "\" alt=\" \" border=\"0\">"), 0);
				tr.storeToken(links[l].label, 0);
				tr.storeToken("</a>", 0);
			}
		}
		
		tr.storeToken("</span>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeStatistics(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedCollectionStatistics, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeStatistics(BufferedCollectionStatistics statistics, HtmlPageBuilder tr) throws IOException {
		
		//	open master table
		tr.storeToken("<table border=\"2\" frame=\"box\" rules=\"none\" bgcolor=\"" + tableEntryBackgroundColor + "\" bordercolor=\"" + tableBorderColor + "\" cellspacing=\"0\" cellpadding=\"3\">", 0);
		
		//	build label row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td width=\"100%\" bgcolor=\"" + titleBackgroundColor + "\">", 0);
		tr.storeToken("<p style=\"" + titleTextStyle + "\" align=\"center\">", 0);
		tr.storeToken("<b>", 0);
		tr.storeToken("GoldenGATE SRS Statistics", 0);
		tr.storeToken("</b>", 0);
		tr.storeToken("<br>", 0);
		tr.storeToken("These are the top 10 contributors to this retrieval archive", 0);
		tr.storeToken("</p>", 0);
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		//	add report of no results
		if ((statistics == null) || (statistics.docCount == 0) || statistics.isEmpty()) {
			
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"center\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("There are no documents in the archive so far.", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		else {
			
			//	open user statistics table
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" align=\"left\">", 0);
			tr.storeToken("<table width=\"98%\" border=\"1\" frame=\"box\" bgcolor=\"" + tableEntryBackgroundColor + "\" bordercolor=\"" + tableBorderColor + "\" cellspacing=\"0\">", 0);
			
			//	build label row
			tr.storeToken("<tr>", 0);
			
			tr.storeToken("<td align=\"center\" width=\"10%\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Rank", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			
			tr.storeToken("<td align=\"center\" width=\"30%\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("User Name", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			
			tr.storeToken("<td align=\"center\" width=\"30%\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Documents Contributed", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			
			tr.storeToken("<td align=\"center\" width=\"30%\">", 0);
			tr.storeToken("<p style=\"" + labelTextStyle + "\" align=\"center\">", 0);
			tr.storeToken("<b>", 0);
			tr.storeToken("Total Document Size", 0);
			tr.storeToken("</b>", 0);
			tr.storeToken("</p>", 0);
			tr.storeToken("</td>", 0);
			
			tr.storeToken("</tr>", 0);
			
			//	display top 10 users
			int rank = 0;
			for (CollectionStatistics cs = statistics.getCollectionStatistics(); (rank < 10) && cs.hasNextElement();) {
				CollectionStatisticsElement cse = cs.getNextCollectionStatisticsElement();
				rank ++; // increment rank
				
				tr.storeToken("<tr>", 0);
				
				tr.storeToken("<td align=\"center\">", 0);
				tr.storeToken("<p style=\"" + tableEntryTextStyle + "\" align=\"center\">", 0);
				tr.storeToken(("" + rank), 0);
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
				
				tr.storeToken("<td align=\"center\">", 0);
				tr.storeToken("<p style=\"" + tableEntryTextStyle + "\" align=\"center\">", 0);
				tr.storeToken(prepareForHtml(cse.getAttribute(CHECKIN_USER_ATTRIBUTE, "Unknown User").toString(), HTML_CHAR_MAPPING), 0);
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
				
				tr.storeToken("<td align=\"center\">", 0);
				tr.storeToken("<p style=\"" + tableEntryTextStyle + "\" align=\"center\">", 0);
				tr.storeToken(cse.getAttribute(DOCUMENT_COUNT_ATTRIBUTE, "0").toString(), 0);
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
				
				tr.storeToken("<td align=\"center\">", 0);
				tr.storeToken("<p style=\"" + tableEntryTextStyle + "\" align=\"center\">", 0);
				tr.storeToken(cse.getAttribute(WORD_COUNT_ATTRIBUTE, "0").toString(), 0);
				tr.storeToken("</p>", 0);
				tr.storeToken("</td>", 0);
				
				tr.storeToken("</tr>", 0);
			}
			
			tr.storeToken("</table>", 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
}
