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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layouts;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.OutputStreamWriter;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
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
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.IoTools;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Html;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * XSLT based layout implementation. This layout requires six XSLT stylesheets
 * in its data path, one CSS stylesheet, and one JavaScript file:
 * <ul>
 * <li><b>documentResult.xslt</b>: the XSLT stylesheet for rendering the
 * result list of a document search</li>
 * <li><b>resultIndex.xslt</b>: the XSLT stylesheet for rendering the result
 * index part of the result list of a document serach</li>
 * <li><b>indexResult.xslt</b>: the XSLT stylesheet for rendering the result
 * of an index search</li>
 * <li><b>thesaurusResult.xslt</b>: the XSLT stylesheet for rendering the
 * result of a thesaurus lookup</li>
 * <li><b>document.xslt</b>: the XSLT stylesheet for rendering a single
 * document</li>
 * <li><b>statistics.xslt</b>: the XSLT stylesheet for rendering the
 * collection ststistics</li>
 * <li><b>searchPortalLayout.css</b>: the CSS stylesheet providing the layout
 * classes used in the XSLT stylesheets (must not be missing or empty, even if
 * no CSS classes are used in the HTML output of the XSLT, since some hard coded
 * HTML generation (the search form) uses CSS classes)</li>
 * <li><b>searchPortalScripts.js</b>: the file containing the JavaScript code
 * used in the search portal (must not be missing or empty, even if JavaScript
 * is not used in the custom parts of the HTML generation, since some hard coded
 * HTML generation (the search form) uses JavaScript)</li>
 * <li><b>documentResultSortOrder.txt</b>: the list of fields to use for
 * sorting the result of a document search (if empty or missing, the result will
 * be sorted plainly by relevance)</li>
 * </ul>
 * The input to the stylesheets is the plain XML representation of the several
 * search results, most easily obtained by adding 'resultFormat=xml' to a given
 * search URL. For documentation for the CSS classes and JavaScript functions,
 * please refer to the respective files.
 * 
 * @author sautter
 */
public class XsltSearchPortalLayout extends SearchPortalLayout {
	
	private static final Html html = new Html();
	private static final Parser parser = new Parser(html);
	
	private static final String[] defaultDocumentResultSortOrder = {"-" + RELEVANCE_ATTRIBUTE};
	
	private String[] documentResultSortOrder = defaultDocumentResultSortOrder;
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalLayout#getStylesheetsToInclude()
	 */
	public String[] getStylesheetsToInclude() {
		String[] ssl = {"searchPortalLayout.css"};
		return ssl;
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalLayout#getJavaScripsToInclude()
	 */
	public String[] getJavaScripsToInclude() {
		String[] ssl = {"searchPortalScripts.js"};
		return ssl;
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalLayout#init()
	 */
	public void init() {
		
		//	create transformer for document search result list
		try {
			this.documentResultTransformer = this.getTransformer("documentResult.xslt");
			this.documentResultTransformerError = null;
		}
		catch (Exception e) {
			this.documentResultTransformer = null;
			this.documentResultTransformerError = this.wrapException(e);
		}
		
		//	create transformer for document search result index
		try {
			this.resultIndexTransformer = this.getTransformer("resultIndex.xslt");
			this.resultIndexTransformerError = null;
		}
		catch (Exception e) {
			this.resultIndexTransformer = null;
			this.resultIndexTransformerError = this.wrapException(e);
		}
		
		//	create transformer for index search result
		try {
			this.indexResultTransformer = this.getTransformer("indexResult.xslt");
			this.indexResultTransformerError = null;
		}
		catch (Exception e) {
			this.indexResultTransformer = null;
			this.indexResultTransformerError = this.wrapException(e);
		}
		
		//	create transformer for thesaurus search result
		try {
			this.thesaurusResultTransformer = this.getTransformer("thesaurusResult.xslt");
			this.thesaurusResultTransformerError = null;
		}
		catch (Exception e) {
			this.thesaurusResultTransformer = null;
			this.thesaurusResultTransformerError = this.wrapException(e);
		}
		
		//	create transformer for document body
		try {
			this.documentTransformer = this.getTransformer("document.xslt");
			this.documentTransformerError = null;
			
			//	extract annotation types that are referenced in stylesheet templates
			this.documentResultElements = getUsedElementNames(new FileInputStream(new File(this.dataPath, "document.xslt")), true);
			if (this.documentResultElements.contains("*"))
				this.documentResultElements = new HashSet() {
					public boolean contains(Object obj) {
						return true;
					}
				};
		}
		catch (Exception e) {
			this.documentTransformer = null;
			this.documentTransformerError = this.wrapException(e);
		}
		
		//	create transformer for collection statistics
		try {
			this.statisticsTransformer = this.getTransformer("statistics.xslt");
			this.statisticsTransformerError = null;
		}
		catch (Exception e) {
			this.statisticsTransformer = null;
			this.statisticsTransformerError = this.wrapException(e);
		}
		
		//	load document result sort order
		try {
			StringVector drso = StringVector.loadList(new FileInputStream(new File(this.dataPath, "documentResultSortOrder.txt")));
			for (int s = 0; s < drso.size();)
				if (drso.get(s).startsWith("//") || (drso.get(s).length() == 0))
					drso.remove(s);
				else s++;
			this.documentResultSortOrder = (drso.isEmpty() ? defaultDocumentResultSortOrder : drso.toStringArray());
		}
		catch (Exception e) {
			this.documentResultSortOrder = defaultDocumentResultSortOrder;
			e.printStackTrace();
		}
	}
	
	private Transformer getTransformer(String xsltName) throws IOException {
		Transformer transformer = XsltUtils.getTransformer(new File(this.dataPath, xsltName), false);
		transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
		return transformer;
	}
	
	/*
xsl:apply-templates select="expression"
xsl:for-each select="expression"
xsl:if test="expression"
xsl:number count="expression"
xsl:param select="expression"
xsl:sort select="expression"
xsl:template match="pattern"
xsl:value-of select="expression"
xsl:variable select="expression"
xsl:when test="boolean-expression"
xsl:with-param select="expression"
	 */
	private static final Properties expressionAttributeNames = new Properties();
	static {
		expressionAttributeNames.setProperty("xsl:apply-templates", "select");
		expressionAttributeNames.setProperty("xsl:for-each", "select");
		expressionAttributeNames.setProperty("xsl:if", "test");
		expressionAttributeNames.setProperty("xsl:number", "count");
		expressionAttributeNames.setProperty("xsl:param", "select");
		expressionAttributeNames.setProperty("xsl:sort", "select");
		expressionAttributeNames.setProperty("xsl:template", "match");
		expressionAttributeNames.setProperty("xsl:value-of", "select");
		expressionAttributeNames.setProperty("xsl:variable", "select");
		expressionAttributeNames.setProperty("xsl:when", "test");
		expressionAttributeNames.setProperty("xsl:with-param", "select");
	}
	private static final Grammar xmlGrammar = new StandardGrammar();
	private static final Parser xmlParser = new Parser(xmlGrammar);
	private static final Pattern qNamePattern = Pattern.compile("(((([a-zA-Z][a-zA-Z0-9\\_\\-]+)|\\*)\\:)?[a-zA-Z\\_\\-][a-zA-Z0-9\\_\\-]+\\*?)", Pattern.CASE_INSENSITIVE);
	private static final Set getUsedElementNames(InputStream in, boolean close) throws IOException {
		final Set usedElementNames = new HashSet();
		xmlParser.stream(in, new TokenReceiver() {
			public void storeToken(String token, int treeDepth) throws IOException {
				if (xmlGrammar.isTag(token)) {
					if (xmlGrammar.isEndTag(token))
						return;
					String type = xmlGrammar.getType(token).toLowerCase();
					if (type.startsWith("xsl:")) {
						String expressionAttributeName = expressionAttributeNames.getProperty(type);
						if (expressionAttributeName == null)
							return;
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, xmlGrammar);
						String expressionAttributeValue = tnas.getAttribute(expressionAttributeName);
						if (expressionAttributeValue == null)
							return;
						Matcher qNameMatcher = qNamePattern.matcher(expressionAttributeValue);
						while (qNameMatcher.find()) {
							String qName = qNameMatcher.group(0);
							if (((qNameMatcher.start(0) != 0) && ("@'\"".indexOf(expressionAttributeValue.charAt(qNameMatcher.start(0)-1)) != -1)) || ((qNameMatcher.end(0) < expressionAttributeValue.length()) && (("('\"".indexOf(expressionAttributeValue.charAt(qNameMatcher.end(0))) != -1) || expressionAttributeValue.startsWith("::", qNameMatcher.end(0)))))
								return;
							usedElementNames.add(qName.toLowerCase());
							if (qName.indexOf('*') != -1)
								usedElementNames.add("*");
						}
					}
				}
			}
			public void close() throws IOException {}
		});
		if (close)
			in.close();
		return usedElementNames;
	}
	
	private Set documentResultElements = null;
	private Transformer documentResultTransformer = null;
	private IOException documentResultTransformerError = null;
	
	private Transformer resultIndexTransformer = null;
	private IOException resultIndexTransformerError = null;
	
	private Transformer indexResultTransformer = null;
	private IOException indexResultTransformerError = null;
	
	private Transformer thesaurusResultTransformer = null;
	private IOException thesaurusResultTransformerError = null;
	
	private Transformer documentTransformer = null;
	private IOException documentTransformerError = null;
	
	private Transformer statisticsTransformer = null;
	private IOException statisticsTransformerError = null;
	
	//	wrap some Exception in an IOException
	private IOException wrapException(final Exception e) {
		return new IOException() {
			public Throwable getCause() {
				return e;
			}
			public String getMessage() {
				return (e.getClass().getName() + " (" + e.getMessage() + ")");
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
	
	private interface OutputWriter {
		abstract void writeOutput() throws IOException;
		abstract String getOutputName();
	}
	private void doTransformation(final OutputWriter writer, PipedInputStream fromWriter, Transformer transformer, IOException transformerError, final TokenReceiver tr) throws IOException {
		
		//	report instantiation error
		if (transformer == null)
			throw transformerError;
		
		final ArrayList exceptions = new ArrayList();
		
		//	wrap token receiver to decode XML entities in attribute values
		final TokenReceiver unescapingTrWrapper = new TokenReceiver() {
			public void close() throws IOException {
				tr.close();
			}
			public void storeToken(String token, int treeDepth) throws IOException {
				if (html.isEndTag(token))
					tr.storeToken(token, treeDepth);
				
				else if (html.isSingularTag(token))
					tr.storeToken(token, treeDepth);
				
				else if (html.isTag(token))
					tr.storeToken(html.unescape(token), treeDepth);
				
				else tr.storeToken(token, treeDepth);
			}
		};
		
		//	build sender infrastructure
		Thread transformerInputWriterThread = new Thread() {
			public void run() {
				try {
					writer.writeOutput();
				}
				catch (InterruptedIOException iioe) {
					if (DEBUG_XSLT)
						System.out.println("Transformer Input Writer killed due to congested pipe while writing " + writer.getOutputName() + ".");
				}
				catch (IOException ioe) {
					Throwable cause = ioe;
					while ((cause = cause.getCause()) != null) {
						if (cause instanceof InterruptedIOException) {
							if (DEBUG_XSLT)
								System.out.println("Transformer Input Writer killed due to congested pipe while writing " + writer.getOutputName() + ".");
							ioe = null;
							break;
						}
					}
					if (ioe != null)
						exceptions.add(ioe);
				}
				if (DEBUG_XSLT)
					System.out.println("Transformer Input Writer finished.");
			}
		};
		transformerInputWriterThread.start();
		
		//	build receiver infrastructure
		final PipedOutputStream fromTransformer = new PipedOutputStream();
		final BufferedReader br = new BufferedReader(new InputStreamReader(new PipedInputStream(fromTransformer), "utf-8"));
		Thread transformerOutputUnescaperThread = new Thread() {
			public void run() {
				try {
					parser.stream(br, unescapingTrWrapper);
				}
				catch (IOException pe) {
					exceptions.add(pe);
				}
				if (DEBUG_XSLT)
					System.out.println("Transformer Output Unescaper finished.");
			}
		};
		transformerOutputUnescaperThread.start();
		
		//	do transformation
		try {
			transformer.transform(new StreamSource(fromWriter), new StreamResult(fromTransformer));
		}
		catch (TransformerException te) {
			exceptions.add(te);
		}
		
		//	close receiver bridge
		fromTransformer.flush();
		fromTransformer.close();
		
		//	wait for receiver thread
		try {
			transformerOutputUnescaperThread.join();
		} catch (InterruptedException ie) {}
		
		//	kill sender if not finished yet (has to finish before receiver theoretically, but may block due to XSL transformer misbehavior)
		int attempts = 0;
		while (transformerInputWriterThread.isAlive()) try {
			if (DEBUG_XSLT)
				System.out.println("Shutting down Transformer Input Writer (failed to finish  while writing " + writer.getOutputName() + ").");
			transformerInputWriterThread.interrupt();
			attempts++;
			if (attempts > 10) {
				if (DEBUG_XSLT)
					System.out.println("Could not shut down Transformer Input Writer.");
				break;
			}
			try {
				Thread.sleep(1000);
			} catch (InterruptedException ie) {}
		}
		catch (Exception e) {
			if (DEBUG_XSLT)
				System.out.println("Exception shutting down Transformer Input Writer: " + e.getMessage());
			break;
		}
		
		//	throw exception from sender or receiver (if any)
		if (!exceptions.isEmpty()) {
			Exception e = (Exception) exceptions.get(0);
			throw ((e instanceof IOException) ? ((IOException) e) : this.wrapException(e));
		}
	}
	private static final boolean DEBUG_XSLT = true;
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalLayout#includeNavigationLinks(de.goldenGateSrs.webPortal.SearchPortalServletFlexLayout.NavigationLink[], de.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeNavigationLinks(NavigationLink[] links, HtmlPageBuilder tr) throws IOException {
		
		if ((links != null) && (links.length != 0)) {
			tr.storeToken("<p class=\"navigation\">", 0);
			
			for (int l = 0; l < links.length; l++) {
				
				if (l != 0) tr.storeToken("&nbsp;", 0);
				
				tr.storeToken("<a href=\"" + links[l].link + "\">", 0);
				if (links[l].icon != null)
					tr.storeToken(("<img src=\"" + links[l].icon + "\" alt=\" \" border=\"0\">"), 0);
				tr.storeToken(links[l].label, 0);
				tr.storeToken("</a>", 0);
			}
			
			tr.storeToken("</p>", 0);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalLayout#includeSearchForm(java.lang.String, de.goldenGateSrs.GoldenGateSrsConstants.SearchFieldGroup[], de.goldenGateSrs.GoldenGateSrsConstants.SearchFieldRow, java.util.Properties, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeSearchForm(String formTitle, SearchFieldGroup[] fieldGroups, SearchFieldRow buttonRowFields, Properties fieldValues, HtmlPageBuilder tr) throws IOException {
		
		//	open form
		tr.storeToken(("<form method=\"GET\" name=\"" + SRS_SEARCH_FORM_NAME + "\" action=\"./" + DEFAULT_SEARCH_MODE + "\">"), 0);
		
		//	compute overall width
		int formWidth = 0;
		for (int g = 0; g < fieldGroups.length; g++)
			formWidth = Math.max(formWidth, fieldGroups[g].getWidth());
		
		//	display error message for empty forms
		if (formWidth == 0) {
			tr.storeToken(("<table class=\"formTable\">"), 0);
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" class=\"searchErrorMessage\">", 0);
			tr.storeToken(("There are no search fields avaliable for this GoldenGATE SRS, sorry."), 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
			tr.storeToken(("</table>"), 0);
			return;
		}
		
		//	open master table
		tr.storeToken(("<table class=\"formTable\">"), 0);
		
		//	add title row
		if ((formTitle != null) && (formTitle.trim().length() != 0)) {
			tr.storeToken("<tr>", 0);
			tr.storeToken(("<td width=\"100%\" class=\"formTableHeader\">"), 0);
			tr.storeToken(IoTools.prepareForHtml(formTitle, HTML_CHAR_MAPPING), 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		//	open table body
		tr.storeToken("<tr>", 0);
		tr.storeToken(("<td width=\"100%\" class=\"formTableBody\">"), 0);
		
		for (int g = 0; g < fieldGroups.length; g++) {
			
			//	empty field group, just write comment
			if (fieldGroups[g].getWidth() == 0)
				tr.storeToken("<!-- omitting empty field group for '" + fieldGroups[g].label + "' -->", 0);
			
			//	fields to display
			else {
				
				// open fieldset and write field group legend
				tr.storeToken("<fieldset>", 0);
				tr.storeToken("<legend>", 0);
				tr.storeToken(IoTools.prepareForHtml(fieldGroups[g].legend, HTML_CHAR_MAPPING), 0);
				tr.storeToken("</legend>", 0);
				
				//	open table for field group
				tr.storeToken("<table class=\"fieldSetTable\">", 0);
				
				//	write field rows
				SearchFieldRow[] fieldRows = fieldGroups[g].getFieldRows();
				for (int r = 0; r < fieldRows.length; r++) {
					
					//	open table row
					tr.storeToken("<tr>", 0);
					
					//	write fields
					SearchField[] fields = fieldRows[r].getFields();
					for (int f = 0; f < fields.length; f++) {
						
						//	open table cell and write label
						tr.storeToken(("<td class=\"fieldSetTableCell" + fields[f].size + "\" colspan=\"" + fields[f].size + "\">"), 0);
						
						//	compute layout classes
						String labelClass = "label";
						String inputClass = "input";
						if (SearchField.BOOLEAN_TYPE.equals(fields[f].type)) {
							labelClass += "Boolean";
							inputClass += "Boolean";
						}
						else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
							labelClass += ("Select" + fields[f].size);
							inputClass += ("Select" + fields[f].size);
						}
						else {
							labelClass += ("Text" + fields[f].size);
							inputClass += ("Text" + fields[f].size);
						}
						
						//	create id attributes
						String labelId = ("label" + fields[f].name.replaceAll("\\.", ""));
						String fieldId = ("input" + fields[f].name.replaceAll("\\.", ""));
						
						//	add label
						tr.storeToken(("<label id=\"" + labelId + "\" class=\"" + labelClass + "\">"), 0);
						tr.storeToken(IoTools.prepareForHtml(fields[f].label, HTML_CHAR_MAPPING), 0);
						tr.storeToken("</label>", 0);
						
						//	add spacer
						tr.storeToken(" ", 0);
						
						//	write actual field
						if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
							tr.storeToken(("<input id=\"" + fieldId + "\" class=\"" + inputClass + "\" type=\"checkbox\" name=\"" + fields[f].name + "\" value=\"" + true + "\"" + ((fieldValues.containsKey(fields[f].name) || ((fields[f].value != null) && (fields[f].value.length() != 0))) ? " checked=\"true\"" : "") + ">"), 0);
						
						else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
							tr.storeToken(("<select id=\"" + fieldId + "\" class=\"" + inputClass + "\" name=\"" + fields[f].name + "\">"), 0);
							
							String preSelected = fieldValues.getProperty(fields[f].name);
							if (preSelected == null) preSelected = fields[f].value;
							
							SearchFieldOption[] fieldOptions = fields[f].getOptions();
							for (int o = 0; o < fieldOptions.length; o++) {
								tr.storeToken(("<option value=\"" + fieldOptions[o].value + "\"" + (fieldOptions[o].value.equals(preSelected) ? " selected=\"true\"" : "") + ">"), 0);
								tr.storeToken(IoTools.prepareForHtml(fieldOptions[o].label, HTML_CHAR_MAPPING), 0);
								tr.storeToken("</option>", 0);
							}
							
							tr.storeToken("</select>", 0);
						}
						else tr.storeToken(("<input id=\"" + fieldId + "\" class=\"" + inputClass + "\" name=\"" + fields[f].name + "\" value=\"" + fieldValues.getProperty(fields[f].name, fields[f].value) + "\">"), 0);
						
						//	close table cell
						tr.storeToken("</td>", 0);
					}
					
					//	fill in empty cells
					for (int e = fieldRows[r].getWidth(); e < formWidth; e++) {
						tr.storeToken(("<td class=\"fieldSetTableCell1\">"), 0);
						tr.storeToken("&nbsp;", 0);
						tr.storeToken("</td>", 0);
					}
					
					//	close table row
					tr.storeToken("</tr>", 0);
				}
				
				//	close table and fieldset of field group
				tr.storeToken("</table>", 0);
				tr.storeToken("</fieldset>", 0);
			}
		}
		
		//	close table body
		tr.storeToken("</td>", 0);
		tr.storeToken("</tr>", 0);
		
		//	open button row
		tr.storeToken("<tr>", 0);
		tr.storeToken("<td class=\"buttonRow\">", 0);
		
		//	add button row fields (if any)
		if (buttonRowFields != null) {
			SearchField[] fields = buttonRowFields.getFields();
			for (int f = 0; f < fields.length; f++) {
				
				//	compute layout classes
				String labelClass = "brLabel";
				String inputClass = "brInput";
				if (SearchField.BOOLEAN_TYPE.equals(fields[f].type)) {
					labelClass += "Boolean";
					inputClass += "Boolean";
				}
				else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
					labelClass += ("Select" + fields[f].size);
					inputClass += ("Select" + fields[f].size);
				}
				else {
					labelClass += ("Text" + fields[f].size);
					inputClass += ("Text" + fields[f].size);
				}
				
				//	create id attributes
				String labelId = ("brLabel" + fields[f].name.replaceAll("\\.", ""));
				String fieldId = ("brInput" + fields[f].name.replaceAll("\\.", ""));
				
				//	write label
				tr.storeToken(("<label id=\"" + labelId + "\" class=\"" + labelClass + "\">"), 0);
				tr.storeToken(IoTools.prepareForHtml(fields[f].label, HTML_CHAR_MAPPING), 0);
				tr.storeToken("</label>", 0);
				
				//	add spacer
				tr.storeToken(" ", 0);
				
				//	write actual field
				if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
					tr.storeToken(("<input id=\"" + fieldId + "\" class=\"" + inputClass + "\" type=\"checkbox\" name=\"" + fields[f].name + "\" value=\"" + true + "\"" + ((fieldValues.containsKey(fields[f].name) || ((fields[f].value != null) && (fields[f].value.length() != 0))) ? " checked=\"true\"" : "") + ">"), 0);
				
				else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
					tr.storeToken(("<select id=\"" + fieldId + "\" class=\"" + inputClass + "\" name=\"" + fields[f].name + "\">"), 0);
					
					String preSelected = fieldValues.getProperty(fields[f].name);
					if (preSelected == null) preSelected = fields[f].value;
					
					SearchFieldOption[] fieldOptions = fields[f].getOptions();
					for (int o = 0; o < fieldOptions.length; o++) {
						tr.storeToken(("<option value=\"" + fieldOptions[o].value + "\"" + (fieldOptions[o].value.equals(preSelected) ? " selected=\"true\"" : "") + ">"), 0);
						tr.storeToken(IoTools.prepareForHtml(fieldOptions[o].label, HTML_CHAR_MAPPING), 0);
						tr.storeToken("</option>", 0);
					}
					
					tr.storeToken("</select>", 0);
				}
				
				else tr.storeToken(("<input id=\"" + fieldId + "\" class=\"" + inputClass + "\" name=\"" + fields[f].name + "\" value=\"" + fieldValues.getProperty(fields[f].name, fields[f].value) + "\">"), 0);
				
				//	add spacer
				tr.storeToken("&nbsp;", 0);
			}
		}
		
		//	add buttons
		tr.storeToken("<input type=\"submit\" value=\"Search\" class=\"submitButton\">", 0);
		tr.storeToken("&nbsp;", 0);
		tr.storeToken("<input type=\"reset\" value=\"Reset\" class=\"resetButton\">", 0);
		tr.storeToken("&nbsp;", 0);
		tr.storeToken("<input type=\"reset\" value=\"Clear\" onclick=\"return resetFields();\" class=\"clearButton\">", 0);
		
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
	public void includeResultIndex(final BufferedThesaurusResult[] indices, HtmlPageBuilder tr) throws IOException {
		
		PipedInputStream pis = new PipedInputStream();
		final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new PipedOutputStream(pis), "utf-8"));
		OutputWriter writer = new OutputWriter() {
			public void writeOutput() throws IOException {
				int totalIndexSize = 0;
				for (int i = 0; i < indices.length; i++)
					if (!indices[i].isEmpty())
						totalIndexSize++;
				
				if (totalIndexSize == 0) {
					bw.write("<" + RESULTS_NODE_NAME + "/>"); 
					bw.newLine();
				}
				else {
					bw.write("<" + RESULTS_NODE_NAME + ">"); 
					bw.newLine();
					
					for (int i = 0; i < indices.length; i++)
						writeThesaurusResult(indices[i], bw, SUB_RESULTS_NODE_NAME);
					
					bw.write("</" + RESULTS_NODE_NAME + ">"); 
					bw.newLine();
				}
				
				bw.flush();
				bw.close();
			}
			public String getOutputName() {
				return "Result Index";
			}
		};
		
		//	do transformation
		this.doTransformation(writer, pis, this.resultIndexTransformer, this.resultIndexTransformerError, tr);
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalLayout#getResultListSearchMode()
	 */
	public String getResultListSearchMode() {
		return SEARCH_INDEX;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultList(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedDocumentResult, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeResultList(BufferedDocumentResult documents, HtmlPageBuilder tr) throws IOException {
		this.includeResultList(documents, tr, null);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeResultList(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedDocumentResult, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder, java.lang.String)
	 */
	public void includeResultList(final BufferedDocumentResult documents, HtmlPageBuilder tr, final String moreResultsLink) throws IOException {
		
		PipedInputStream pis = new PipedInputStream();
		final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new PipedOutputStream(pis), "utf-8"));
		OutputWriter writer = new OutputWriter() {
			public void writeOutput() throws IOException {
				writeDocumentList(documents, bw, moreResultsLink);
				bw.flush();
				bw.close();
			}
			public String getOutputName() {
				return "Result List";
			}
		};
		
		//	do transformation
		this.doTransformation(writer, pis, this.documentResultTransformer, this.documentResultTransformerError, tr);
	}
	
	private void writeDocumentList(BufferedDocumentResult documents, BufferedWriter out, String moreResultsLink) throws IOException {
		ArrayList dreList = new ArrayList();
		for (DocumentResult dr = documents.getDocumentResult(); dr.hasNextElement();)
			dreList.add(dr.getNextDocumentResultElement());
		DocumentResultElement[] dres = ((DocumentResultElement[]) dreList.toArray(new DocumentResultElement[dreList.size()]));
		HashMap dreSubResultByDre = new HashMap();
		
		//	add result level links to external sources
		ArrayList resultLinks = new ArrayList();
		
		//	get and sort result links
		SearchResultLinker[] linkers = this.parent.getResultLinkers();
		for (int l = 0; l < linkers.length; l++) try {
			SearchResultLink[] links = linkers[l].getAnnotationSetLinks(dres);
			if (links != null)
				for (int k = 0; k < links.length; k++) {
					if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null)))
						resultLinks.add(links[k]);
				}
			}
			catch (Exception e) {
				System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
				e.printStackTrace(System.out);
			}
		
		//	get sub results
		for (int d = 0; d < dres.length; d++) {
			IndexResult[] subResults = dres[d].getSubResults();
			BufferedIndexResult[] dreSubResults = new BufferedIndexResult[subResults.length];
			dreSubResultByDre.put(dres[d], dreSubResults);
			for (int s = 0; s < subResults.length; s++) {
				dreSubResults[s] = new BufferedIndexResult(subResults[s]);
				dreSubResults[s].sort();
			}
		}
		
		//	collect links for individual annotations and collect sub results by type
		HashMap resultElementLinksByAnnotationId = new HashMap();
		HashMap subIresByType = new LinkedHashMap();
		for (int d = 0; d < dres.length; d++) {
			ArrayList dreLinks = new ArrayList();
			for (int l = 0; l < linkers.length; l++) try {
				
				//	link for element
				SearchResultLink[] links = linkers[l].getAnnotationLinks(dres[d]);
				if (links != null)
					for (int k = 0; k < links.length; k++) {
						if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
							if (links[k].iconUrl != null) dreLinks.add(links[k]);
							else if (links[k].label != null) dreLinks.add(links[k]);
						}
					}
				}
				catch (Exception ex) {
					System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + ex.getClass().getName() + " - " + ex.getMessage());
					ex.printStackTrace(System.out);
				}
			
			//	link for sub results
			BufferedIndexResult[] dreSubResults = ((BufferedIndexResult[]) dreSubResultByDre.get(dres[d]));
			for (int s = 0; (dreSubResults != null) && (s < dreSubResults.length); s++) {
				IndexResult subIr = dreSubResults[s].getIndexResult();
				
				//	collect sub result elements by type for result level links
				if (subIr.hasNextElement()) {
					IndexResultElement subIre = subIr.getNextIndexResultElement();
					
					//	collect sub resuls per document
					ArrayList subIreList = new ArrayList();
					
					//	collect sub result elements by type for result level links
					ArrayList subIreTypeList = ((ArrayList) subIresByType.get(subIre.getType()));
					if (subIreTypeList == null) {
						subIreTypeList = new ArrayList();
						subIresByType.put(subIre.getType(), subIreTypeList);
					}
					
					//	process elements
					do {
						subIreList.add(subIre);
						subIreTypeList.add(subIre);
						
						//	collect links for individual sub results
						ArrayList subIreLinkList = new ArrayList();
						for (int l = 0; l < linkers.length; l++) try {
							
							//	link for element
							SearchResultLink[] subLinks = linkers[l].getAnnotationLinks(subIre);
							if (subLinks != null)
								for (int k = 0; k < subLinks.length; k++) {
									if ((subLinks[k] != null) && ((subLinks[k].href != null) || (subLinks[k].onclick != null))) {
										if (subLinks[k].iconUrl != null) subIreLinkList.add(subLinks[k]);
										else if (subLinks[k].label != null) subIreLinkList.add(subLinks[k]);
									}
								}
							}
							catch (Exception ex) {
								System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + ex.getClass().getName() + " - " + ex.getMessage());
								ex.printStackTrace(System.out);
							}
						
						if (!subIreLinkList.isEmpty())
							resultElementLinksByAnnotationId.put(subIre.getAnnotationID(), ((SearchResultLink[]) subIreLinkList.toArray(new SearchResultLink[subIreLinkList.size()])));
					}
					while ((subIre = subIr.getNextIndexResultElement()) != null);
					
					//	collect sub result links for result elements
					IndexResultElement[] subIres = ((IndexResultElement[]) subIreList.toArray(new IndexResultElement[subIreList.size()]));
					for (int l = 0; l < linkers.length; l++) try {
						
						//	link for sub result
						SearchResultLink[] links = linkers[l].getAnnotationSetLinks(subIres);
						if (links != null)
							for (int k = 0; k < links.length; k++) {
								if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
									if (links[k].iconUrl != null) dreLinks.add(links[k]);
									else if (links[k].label != null) dreLinks.add(links[k]);
								}
							}
						}
						catch (Exception ex) {
							System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + ex.getClass().getName() + " - " + ex.getMessage());
							ex.printStackTrace(System.out);
						}
				}
			}
			
			if (!dreLinks.isEmpty())
				resultElementLinksByAnnotationId.put(dres[d].getAnnotationID(), ((SearchResultLink[]) dreLinks.toArray(new SearchResultLink[dreLinks.size()])));
		}
		
		//	get result level links for sub results
		for (Iterator srit = subIresByType.values().iterator(); srit.hasNext();) {
			ArrayList subIreList = ((ArrayList) srit.next());
			Annotation[] subIres = ((Annotation[]) subIreList.toArray(new Annotation[subIreList.size()]));
			for (int l = 0; l < linkers.length; l++) try {
				SearchResultLink[] links = linkers[l].getAnnotationSetLinks(subIres);
				if (links != null)
					for (int k = 0; k < links.length; k++) {
						if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null)))
							resultLinks.add(links[k]);
					}
				}
				catch (Exception e) {
					System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
					e.printStackTrace(System.out);
				}
		}
		
		//	empty result
		if (dres.length == 0) {
			
			//	send empty result
			out.write("<" + RESULTS_NODE_NAME + "/>");
			out.newLine();
		}
		
		//	data to write
		else {
			
			//	sort list TODOne: think of a faster algorithm
			Arrays.sort(dres, new Comparator() {
				private final String[] sortOrderIndexNames;
				private final String[] sortOrderFieldNames;
				private final boolean[] sortOrderNegated;
				{
					this.sortOrderFieldNames = new String[documentResultSortOrder.length];
					this.sortOrderIndexNames = new String[documentResultSortOrder.length];
					this.sortOrderNegated = new boolean[documentResultSortOrder.length];
					for (int f = 0; f < documentResultSortOrder.length; f++) {
						String sortOrderField = documentResultSortOrder[f];
						
						if (sortOrderField.startsWith("-")) {
							sortOrderNegated[f] = true;
							sortOrderField = sortOrderField.substring(1).trim();
						}
						else sortOrderNegated[f] = false;
						
						int split = sortOrderField.indexOf('.');
						if (split == -1) {
							this.sortOrderFieldNames[f] = sortOrderField;
							this.sortOrderIndexNames[f] = null;
						}
						else {
							this.sortOrderFieldNames[f] = sortOrderField.substring(split + 1);
							this.sortOrderIndexNames[f] = sortOrderField.substring(0, split);
						}
					}
				}
				public int compare(Object o1, Object o2) {
					DocumentResultElement dre1 = ((DocumentResultElement) o1);
					DocumentResultElement dre2 = ((DocumentResultElement) o2);
					int c = 0;
					int sfi = 0;
					while ((c == 0) && (sfi < documentResultSortOrder.length)) {
						if (this.sortOrderIndexNames[sfi] == null)
							c = String.CASE_INSENSITIVE_ORDER.compare(
									dre1.getAttribute(documentResultSortOrder[sfi], "").toString(),
									dre2.getAttribute(documentResultSortOrder[sfi], "").toString()
								);
						else {
							IndexResult[] subResults1 = dre1.getSubResults();
							IndexResultElement ire1 = null;
							for (int s = 0; s < subResults1.length; s++) {
								if (this.sortOrderIndexNames[sfi].equals(subResults1[s].indexName) && subResults1[s].hasNextElement()) {
									ire1 = subResults1[s].getNextIndexResultElement();
									s = subResults1.length;
								}
							}
							IndexResult[] subResults2 = dre2.getSubResults();
							IndexResultElement ire2 = null;
							for (int s = 0; s < subResults2.length; s++) {
								if (this.sortOrderIndexNames[sfi].equals(subResults2[s].indexName) && subResults2[s].hasNextElement()) {
									ire2 = subResults2[s].getNextIndexResultElement();
									s = subResults2.length;
								}
							}
							c = String.CASE_INSENSITIVE_ORDER.compare(
									((ire1 == null) ? "" : ire1.getAttribute(this.sortOrderFieldNames[sfi], "").toString()),
									((ire2 == null) ? "" : ire2.getAttribute(this.sortOrderFieldNames[sfi], "").toString())
								);
						}
						if (sortOrderNegated[sfi])
							c = (0 - c);
						sfi++;
					}
					return c;
				}
			});
			
			//	open result
			StringVector resultFields = new StringVector();
			resultFields.addContent(DOCUMENT_ATTRIBUTES);
			out.write("<" + RESULTS_NODE_NAME +
					((moreResultsLink == null) ? "" : (" " + MORE_RESULTS_LINK_ATTRIBUTE + "=\"" + AnnotationUtils.escapeForXml(moreResultsLink, true) + "\"")) +
					" " + RESULT_SIZE_ATTRIBUTE + "=\"" + dres.length + "\"" +
					" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + resultFields.concatStrings(" ") +	"\"" +
					">");
			out.newLine();
			
			//	add result level links
			for (int l = 0; l < resultLinks.size(); l++) {
				SearchResultLink link = ((SearchResultLink) resultLinks.get(l));
				out.write(link.toXml());
				out.newLine();
			}
			
			//	write index entries
			for (int d = 0; d < dres.length; d++) {
				
				//	write result
				DocumentResultElement dre = dres[d];
				BufferedIndexResult[] dreSubResults = ((BufferedIndexResult[]) dreSubResultByDre.get(dres[d]));
				
				//	plain entry
				if ((dreSubResults == null) || (dreSubResults.length == 0)) {
					String value = dre.getValue();
					SearchResultLink[] links = ((SearchResultLink[]) resultElementLinksByAnnotationId.get(dre.getAnnotationID()));
					
					//	no content
					if ((value.length() == 0) && ((links == null) || (links.length == 0))) {
						out.write("<" + RESULT_NODE_NAME);
						String[] attributeNames = dre.getAttributeNames();
						for (int a = 0; a < attributeNames.length; a++) {
							Object aValue = dre.getAttribute(attributeNames[a]);
							if (aValue != null)
								out.write(" " + attributeNames[a] + "=\"" + AnnotationUtils.escapeForXml(aValue.toString(), true) + "\"");
						}
						out.write("/>");
						out.newLine();
					}
					
					else {
						out.write("<" + RESULT_NODE_NAME);
						String[] attributeNames = dre.getAttributeNames();
						for (int a = 0; a < attributeNames.length; a++) {
							Object aValue = dre.getAttribute(attributeNames[a]);
							if (aValue != null)
								out.write(" " + attributeNames[a] + "=\"" + AnnotationUtils.escapeForXml(aValue.toString(), true) + "\"");
						}
						out.write(">");
						out.newLine();
						
						if (links != null)
							for (int l = 0; l < links.length; l++) {
								out.write(links[l].toXml());
								out.newLine();
							}
						
						out.write(AnnotationUtils.escapeForXml(value));
						out.newLine();
						
						out.write("</" + RESULT_NODE_NAME + ">");
						out.newLine();
					}
				}
				
				//	entry with sub results
				else {
					
					//	open main result element
					out.write("<" + RESULT_NODE_NAME);
					String[] attributeNames = dre.getAttributeNames();
					for (int a = 0; a < attributeNames.length; a++) {
						Object aValue = dre.getAttribute(attributeNames[a]);
						if (aValue != null)
							out.write(" " + attributeNames[a] + "=\"" + AnnotationUtils.escapeForXml(aValue.toString(), true) + "\"");
					}
					out.write(">");
					out.newLine();
					
					//	write element links
					SearchResultLink[] links = ((SearchResultLink[]) resultElementLinksByAnnotationId.get(dre.getAnnotationID()));
					if (links != null)
						for (int l = 0; l < links.length; l++) {
							out.write(links[l].toXml());
							out.newLine();
						}
					
					//	write element value
					String value = dre.getValue();
					if (value.length() == 0)
						out.write("<" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + "/>");
					else {
						out.write("<" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + ">");
						out.write(AnnotationUtils.escapeForXml(dre.getValue()));
						out.write("</" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + ">");
					}
					out.newLine();
					
					//	add sub results
					for (int s = 0; s < dreSubResults.length; s++) {
						
						//	get fields of sub result
						StringVector subIndexFields = new StringVector();
						subIndexFields.addContent(dreSubResults[s].resultAttributes);
						out.write("<" + SUB_RESULTS_NODE_NAME + 
								" " + RESULT_INDEX_NAME_ATTRIBUTE + "=\"" + dreSubResults[s].indexName + "\"" +
								" " + RESULT_INDEX_LABEL_ATTRIBUTE + "=\"" + dreSubResults[s].indexLabel + "\"" +
								" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + subIndexFields.concatStrings(" ") + "\"" +
						">");
						out.newLine();
						
						//	write sub result entries
						IndexResult subIr = dreSubResults[s].getIndexResult();
						while (subIr.hasNextElement()) {
							IndexResultElement subIre = subIr.getNextIndexResultElement();
							
							String subValue = subIre.getValue();
							SearchResultLink[] subLinks = ((SearchResultLink[]) resultElementLinksByAnnotationId.get(subIre.getAnnotationID()));
							
							//	no content
							if ((subValue.length() == 0) && ((subLinks == null) || (subLinks.length == 0))) {
								String tag = AnnotationUtils.produceStartTag(subIre);
								out.write(tag.substring(0, (tag.length()-1)) + "/>");
								out.newLine();
							}
							
							else {
								out.write(AnnotationUtils.produceStartTag(subIre));
								out.newLine();
								
								if (subLinks != null)
									for (int l = 0; l < subLinks.length; l++) {
										out.write(subLinks[l].toXml());
										out.newLine();
									}
								
								out.write(AnnotationUtils.escapeForXml(subValue));
								out.newLine();
								
								out.write(AnnotationUtils.produceEndTag(subIre));
								out.newLine();
							}
						}
						
						//	close sub result
						out.write("</" + SUB_RESULTS_NODE_NAME + ">");
						out.newLine();
					}
					
					//	close main result element
					out.write("</" + RESULT_NODE_NAME + ">");
					out.newLine();
				}
			}
			
			//	close main result
			out.write("</" + RESULTS_NODE_NAME + ">");
			out.newLine();
		}
		
		//	enforce writing data
		out.flush();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeIndexResult(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedIndexResult, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeIndexResult(final BufferedIndexResult index, HtmlPageBuilder tr) throws IOException {
		
		PipedInputStream pis = new PipedInputStream();
		final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new PipedOutputStream(pis), "utf-8"));
		OutputWriter writer = new OutputWriter() {
			public void writeOutput() throws IOException {
				writeIndexResult(index, bw);
				bw.flush();
				bw.close();
			}
			public String getOutputName() {
				return "Index Result";
			}
		};
		
		//	do transformation
		this.doTransformation(writer, pis, this.indexResultTransformer, this.indexResultTransformerError, tr);
	}
	
	private void writeIndexResult(BufferedIndexResult index, BufferedWriter out) throws IOException {
		
		//	empty result
		if (index.isEmpty()) {
			
			//	send empty result
			out.write("<" + RESULTS_NODE_NAME + "/>");
			out.newLine();
		}
		
		//	data to write
		else {
			
			//	collect data
			ArrayList ireList = new ArrayList();
			for (IndexResult ir = index.getIndexResult(); ir.hasNextElement();)
				ireList.add(ir.getNextIndexResultElement());
			IndexResultElement[] ires = ((IndexResultElement[]) ireList.toArray(new IndexResultElement[ireList.size()]));
			BufferedIndexResult[][] ireSubResults = new BufferedIndexResult[ires.length][];
			
			//	open result
			StringVector indexFields = new StringVector();
			indexFields.addContent(index.resultAttributes);
			out.write("<" + RESULTS_NODE_NAME + 
					" " + RESULT_SIZE_ATTRIBUTE + "=\"" + ires.length + "\"" +
					" " + RESULT_INDEX_NAME_ATTRIBUTE + "=\"" + index.indexName + "\"" +
					" " + RESULT_INDEX_LABEL_ATTRIBUTE + "=\"" + index.indexLabel + "\"" +
					" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + indexFields.concatStrings(" ") + "\"" +
			">");
			out.newLine();
			
			//	add result level links to external sources
			ArrayList resultLinks = new ArrayList();
			
			//	get and sort result links
			SearchResultLinker[] linkers = this.parent.getResultLinkers();
			for (int l = 0; l < linkers.length; l++) try {
				SearchResultLink[] links = linkers[l].getAnnotationSetLinks(ires);
				if (links != null)
					for (int k = 0; k < links.length; k++) {
						if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null)))
							resultLinks.add(links[k]);
					}
			}
			catch (Exception e) {
				System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
				e.printStackTrace(System.out);
			}
			
			//	get sub results
			for (int i = 0; i < ires.length; i++) {
				IndexResult[] subResults = ires[i].getSubResults();
				ireSubResults[i] = new BufferedIndexResult[subResults.length];
				for (int s = 0; s < subResults.length; s++) {
					ireSubResults[i][s] = new BufferedIndexResult(subResults[s]);
					ireSubResults[i][s].sort();
				}
			}
			
			//	collect links for individual annotations and collect sub results by type
			HashMap resultElementLinksByAnnotationId = new HashMap();
			HashMap subIresByType = new HashMap();
			for (int i = 0; i < ires.length; i++) {
				ArrayList ireLinks = new ArrayList();
				for (int l = 0; l < linkers.length; l++) try {
					
					//	link for element
					SearchResultLink[] links = linkers[l].getAnnotationLinks(ires[i]);
					if (links != null)
						for (int k = 0; k < links.length; k++) {
							if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
								if (links[k].iconUrl != null) ireLinks.add(links[k]);
								else if (links[k].label != null) ireLinks.add(links[k]);
							}
						}
				}
				catch (Exception ex) {
					System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + ex.getClass().getName() + " - " + ex.getMessage());
					ex.printStackTrace(System.out);
				}
				
				//	link for sub results
				for (int s = 0; s < ireSubResults[i].length; s++) {
					IndexResult subIr = ireSubResults[i][s].getIndexResult();
					
					//	collect sub result elements by type for result level links
					if (subIr.hasNextElement()) {
						IndexResultElement subIre = subIr.getNextIndexResultElement();
						
						//	collect sub resuls per document
						ArrayList subIreList = new ArrayList();
						
						//	collect sub result elements by type for result level links
						ArrayList subIreTypeList = ((ArrayList) subIresByType.get(subIre.getType()));
						if (subIreTypeList == null) {
							subIreTypeList = new ArrayList();
							subIresByType.put(subIre.getType(), subIreTypeList);
						}
						
						//	process elements
						do {
							subIreList.add(subIre);
							subIreTypeList.add(subIre);
							
							//	collect links for individual sub results
							ArrayList subIreLinkList = new ArrayList();
							for (int l = 0; l < linkers.length; l++) try {
								
								//	link for element
								SearchResultLink[] subLinks = linkers[l].getAnnotationLinks(subIre);
								if (subLinks != null)
									for (int k = 0; k < subLinks.length; k++) {
										if ((subLinks[k] != null) && ((subLinks[k].href != null) || (subLinks[k].onclick != null))) {
											if (subLinks[k].iconUrl != null) subIreLinkList.add(subLinks[k]);
											else if (subLinks[k].label != null) subIreLinkList.add(subLinks[k]);
										}
									}
								}
								catch (Exception ex) {
									System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + ex.getClass().getName() + " - " + ex.getMessage());
									ex.printStackTrace(System.out);
								}
							
							if (!subIreLinkList.isEmpty())
								resultElementLinksByAnnotationId.put(subIre.getAnnotationID(), ((SearchResultLink[]) subIreLinkList.toArray(new SearchResultLink[subIreLinkList.size()])));
						}
						while ((subIre = subIr.getNextIndexResultElement()) != null);
						
						//	collect sub result links for result elements
						IndexResultElement[] subIres = ((IndexResultElement[]) subIreList.toArray(new IndexResultElement[subIreList.size()]));
						for (int l = 0; l < linkers.length; l++) try {
							
							//	link for sub result
							SearchResultLink[] links = linkers[l].getAnnotationSetLinks(subIres);
							if (links != null)
								for (int k = 0; k < links.length; k++) {
									if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null))) {
										if (links[k].iconUrl != null) ireLinks.add(links[k]);
										else if (links[k].label != null) ireLinks.add(links[k]);
									}
								}
							}
							catch (Exception ex) {
								System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + ex.getClass().getName() + " - " + ex.getMessage());
								ex.printStackTrace(System.out);
							}
					}
				}
				
				if (!ireLinks.isEmpty())
					resultElementLinksByAnnotationId.put(ires[i].getAnnotationID(), ((SearchResultLink[]) ireLinks.toArray(new SearchResultLink[ireLinks.size()])));
			}
			
			//	get result level links for sub results
			for (Iterator srit = subIresByType.values().iterator(); srit.hasNext();) {
				ArrayList subIreList = ((ArrayList) srit.next());
				Annotation[] subIres = ((Annotation[]) subIreList.toArray(new Annotation[subIreList.size()]));
				for (int l = 0; l < linkers.length; l++) try {
					SearchResultLink[] links = linkers[l].getAnnotationSetLinks(subIres);
					if (links != null)
						for (int k = 0; k < links.length; k++) {
							if ((links[k] != null) && ((links[k].href != null) || (links[k].onclick != null)))
								resultLinks.add(links[k]);
						}
					}
					catch (Exception e) {
						System.out.println("Exception adding external link (" + linkers[l].getName() + "): " + e.getClass().getName() + " - " + e.getMessage());
						e.printStackTrace(System.out);
					}
			}
			
			//	add result level links
			for (int i = 0; i < resultLinks.size(); i++) {
				SearchResultLink link = ((SearchResultLink) resultLinks.get(i));
				out.write(link.toXml());
				out.newLine();
			}
			
			//	write index entries
			for (int i = 0; i < ires.length; i++) {
				
				//	write result
				IndexResultElement ire = ires[i];
				
				//	plain entry
				if (ireSubResults[i].length == 0) {
					String value = ire.getValue();
					SearchResultLink[] links = ((SearchResultLink[]) resultElementLinksByAnnotationId.get(ire.getAnnotationID()));
					
					//	no content
					if ((value.length() == 0) && ((links == null) || (links.length == 0))) {
						String tag = AnnotationUtils.produceStartTag(ire);
						out.write(tag.substring(0, (tag.length()-1)) + "/>");
						out.newLine();
					}
					
					else {
						out.write(AnnotationUtils.produceStartTag(ire));
						out.newLine();
						
						if (links != null)
							for (int l = 0; l < links.length; l++) {
								out.write(links[l].toXml());
								out.newLine();
							}
						
						out.write(AnnotationUtils.escapeForXml(value));
						out.newLine();
						
						out.write(AnnotationUtils.produceEndTag(ire));
						out.newLine();
					}
				}
				
				//	entry with sub results
				else {
					
					//	open main result element
					out.write(AnnotationUtils.produceStartTag(ire));
					out.newLine();
					
					//	write element links
					SearchResultLink[] links = ((SearchResultLink[]) resultElementLinksByAnnotationId.get(ire.getAnnotationID()));
					if (links != null)
						for (int l = 0; l < links.length; l++) {
							out.write(links[l].toXml());
							out.newLine();
						}
					
					//	write element value
					String value = ire.getValue();
					if (value.length() == 0)
						out.write("<" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + "/>");
					else {
						out.write("<" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + ">");
						out.write(AnnotationUtils.escapeForXml(value));
						out.write("</" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + ">");
					}
					out.newLine();
					
					//	add sub results
					for (int s = 0; s < ireSubResults[i].length; s++) {
						
						//	get fields of sub result
						StringVector subIndexFields = new StringVector();
						subIndexFields.addContent(ireSubResults[i][s].resultAttributes);
						out.write("<" + SUB_RESULTS_NODE_NAME + 
								" " + RESULT_INDEX_NAME_ATTRIBUTE + "=\"" + ireSubResults[i][s].indexName + "\"" +
								" " + RESULT_INDEX_LABEL_ATTRIBUTE + "=\"" + ireSubResults[i][s].indexLabel + "\"" +
								" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + subIndexFields.concatStrings(" ") + "\"" +
						">");
						out.newLine();
						
						//	write sub result entries
						IndexResult subIr = ireSubResults[i][s].getIndexResult();
						while (subIr.hasNextElement()) {
							IndexResultElement subIre = subIr.getNextIndexResultElement();
							
							String subValue = subIre.getValue();
							SearchResultLink[] subLinks = ((SearchResultLink[]) resultElementLinksByAnnotationId.get(subIre.getAnnotationID()));
							
							//	no content
							if ((subValue.length() == 0) && ((subLinks == null) || (subLinks.length == 0))) {
								String tag = AnnotationUtils.produceStartTag(subIre);
								out.write(tag.substring(0, (tag.length()-1)) + "/>");
								out.newLine();
							}
							
							else {
								out.write(AnnotationUtils.produceStartTag(subIre));
								out.newLine();
								
								if (subLinks != null)
									for (int l = 0; l < subLinks.length; l++) {
										out.write(subLinks[l].toXml());
										out.newLine();
									}
								
								out.write(AnnotationUtils.escapeForXml(subValue));
								out.newLine();
								
								out.write(AnnotationUtils.produceEndTag(subIre));
								out.newLine();
							}
						}
						
						//	close sub result
						out.write("</" + SUB_RESULTS_NODE_NAME + ">");
						out.newLine();
					}
					
					//	close main result element
					out.write(AnnotationUtils.produceEndTag(ire));
					out.newLine();
				}
			}
			
			//	close result
			out.write("</" + RESULTS_NODE_NAME + ">");
			out.newLine();
		}
		
		//	enforce writing data
		out.flush();
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalLayout#includeResultDocument(de.goldenGateSrs.DocumentResultElement, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeResultDocument(final DocumentResultElement document, final HtmlPageBuilder tr) throws IOException {
		
		PipedInputStream pis = new PipedInputStream();
		final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new PipedOutputStream(pis), "utf-8"));
		OutputWriter writer = new OutputWriter() {
			public void writeOutput() throws IOException {
				writeResultDocument(document.document, bw);
				bw.flush();
				bw.close();
			}
			public String getOutputName() {
				return ("Document " + document.documentId);
			}
		};
		
		//	do transformation
		this.doTransformation(writer, pis, this.documentTransformer, this.documentTransformerError, tr);
	}
	
	private void writeResultDocument(MutableAnnotation doc, BufferedWriter out) throws IOException {
		
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		Annotation[] annotations = doc.getAnnotations();
		
		//	include generic document tag
		if (annotations.length == 0) {
			annotations = new Annotation[1];
			annotations[0] = doc;
		}
		else if (!DocumentRoot.DOCUMENT_TYPE.equals(annotations[0].getType())) {
			Annotation[] newNestedAnnotations = new Annotation[annotations.length + 1];
			newNestedAnnotations[0] = doc;
			System.arraycopy(annotations, 0, newNestedAnnotations, 1, annotations.length);
			annotations = newNestedAnnotations;
		}
		
		//	get external linkers (null check added for standalone test runs)
		SearchResultLinker[] linkers = ((this.parent == null) ? new SearchResultLinker[0] : this.parent.getResultLinkers());
		
		Stack stack = new Stack();
		int annotationPointer = 0;
		
		Token token = null;
		Token lastToken;
		
		for (int t = 0; t < doc.size(); t++) {
			
			//	switch to next Token
			lastToken = token;
			token = doc.tokenAt(t);
			
			//	add line break at end of paragraph
			boolean breakBeforeEndTag = true;
			if ((lastToken != null) && lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) {
				buf.write("<br/>");
				buf.newLine();
				breakBeforeEndTag = false;
			}
			
			//	write end tags for Annotations ending before current Token
			while ((stack.size() > 0) && ((((Annotation) stack.peek()).getStartIndex() + ((Annotation) stack.peek()).size()) <= t)) {
				Annotation annotation = ((Annotation) stack.pop());
				if (breakBeforeEndTag) {
					buf.newLine();
					breakBeforeEndTag = false;
				}
				buf.write(AnnotationUtils.produceEndTag(annotation));
				buf.newLine();
			}
			
			//	skip space character before unspaced punctuation (e.g. ',') or if explicitly told so
			if (((lastToken == null) || !lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) && (t != 0) && (doc.getWhitespaceAfter(t-1).length() != 0)) buf.write(" ");
			
			//	write start tags for Annotations beginning at current Token
			while ((annotationPointer < annotations.length) && (annotations[annotationPointer].getStartIndex() == t)) {
				Annotation annotation = annotations[annotationPointer++];
				if (DocumentRoot.DOCUMENT_TYPE.equals(annotation.getType()) || this.documentResultElements.contains(annotation.getType().toLowerCase())) {
					buf.write(AnnotationUtils.produceStartTag(annotation));
					buf.newLine();
					
					//	write external links
					for (int l = 0; l < linkers.length; l++) {
						SearchResultLink[] links = null;
						
						if (DocumentRoot.DOCUMENT_TYPE.equals(annotation.getType()))
							links = linkers[l].getDocumentLinks(doc);
						
						else links = linkers[l].getAnnotationLinks(annotation);
						
						if (links != null) {
							for (int k = 0; k < links.length; k++) {
								buf.write(links[k].toXml());
								buf.newLine();
							}
						}
					}
					
					stack.push(annotation);
				}
			}
			
			//	write current Token
			buf.write(AnnotationUtils.escapeForXml(token.getValue()));
		}
		
		//	write end tags for Annotations not closed so far
		while (stack.size() > 0) {
			Annotation annotation = ((Annotation) stack.pop());
			buf.newLine();
			buf.write(AnnotationUtils.produceEndTag(annotation));
		}
		buf.flush();
		out.newLine();
		out.flush();
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateSrs.webPortal.SearchPortalLayout#includeThesaurusForms(java.lang.String, de.goldenGateSrs.GoldenGateSrsConstants.SearchFieldGroup[], de.goldenGateSrs.GoldenGateSrsConstants.SearchFieldRow, java.util.Properties, de.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeThesaurusForms(String formTitle, SearchFieldGroup[] fieldGroups, SearchFieldRow buttonRowFields, Properties fieldValues, HtmlPageBuilder tr) throws IOException {
		
		//	compute overall width
		int formWidth = 0;
		for (int g = 0; g < fieldGroups.length; g++)
			formWidth = Math.max(formWidth, fieldGroups[g].getWidth());
		
		//	display error message for empty forms
		if (formWidth == 0) {
			tr.storeToken(("<table class=\"formTable\">"), 0);
			tr.storeToken("<tr>", 0);
			tr.storeToken("<td width=\"100%\" class=\"searchErrorMessage\">", 0);
			tr.storeToken(("There are no search fields avaliable for this GoldenGATE SRS, sorry."), 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
			tr.storeToken(("</table>"), 0);
			return;
		}
		
		//	open master table
		tr.storeToken(("<table class=\"formTable\">"), 0);
		
		//	add title row
		if ((formTitle != null) && (formTitle.trim().length() != 0)) {
			tr.storeToken("<tr>", 0);
			tr.storeToken(("<td width=\"100%\" class=\"formTableHeader\">"), 0);
			tr.storeToken(IoTools.prepareForHtml(formTitle, HTML_CHAR_MAPPING), 0);
			tr.storeToken("</td>", 0);
			tr.storeToken("</tr>", 0);
		}
		
		for (int g = 0; g < fieldGroups.length; g++) {
			
			//	empty field group, just write comment
			if (fieldGroups[g].getWidth() == 0)
				tr.storeToken("<!-- omitting empty field group for '" + fieldGroups[g].label + "' -->", 0);
			
			//	fields to display
			else {
				
				//	open form
				tr.storeToken("<form method=\"GET\" action=\"./" + THESAURUS_SEARCH_MODE + "\" class=\"thesaurusForm\">", 0);
				
				//	open table row & table cell for field table
				tr.storeToken("<tr>", 0);
				tr.storeToken("<td class=\"formTableBody\">", 0);
				
				//	open fieldset and write field group legend
				tr.storeToken("<fieldset>", 0);
				tr.storeToken("<legend>", 0);
				tr.storeToken(IoTools.prepareForHtml(fieldGroups[g].legend, HTML_CHAR_MAPPING), 0);
				tr.storeToken("</legend>", 0);
				
				//	open table for field group
				tr.storeToken("<table class=\"fieldSetTable\">", 0);
				
				//	write field rows
				SearchFieldRow[] fieldRows = fieldGroups[g].getFieldRows();
				for (int r = 0; r < fieldRows.length; r++) {
					
					//	open table row
					tr.storeToken("<tr>", 0);
					
					//	write fields
					SearchField[] fields = fieldRows[r].getFields();
					for (int f = 0; f < fields.length; f++) {
						
						//	open table cell and write label
						tr.storeToken(("<td class=\"fieldSetTableCell" + fields[f].size + "\" colspan=\"" + fields[f].size + "\">"), 0);
						
						//	compute layout classes
						String labelClass = "label";
						String inputClass = "input";
						if (SearchField.BOOLEAN_TYPE.equals(fields[f].type)) {
							labelClass += "Boolean";
							inputClass += "Boolean";
						}
						else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
							labelClass += ("Select" + fields[f].size);
							inputClass += ("Select" + fields[f].size);
						}
						else {
							labelClass += ("Text" + fields[f].size);
							inputClass += ("Text" + fields[f].size);
						}
						
						//	create id attributes
						String labelId = ("label" + fields[f].name.replaceAll("\\.", ""));
						String fieldId = ("input" + fields[f].name.replaceAll("\\.", ""));
						
						//	add label
						tr.storeToken(("<label id=\"" + labelId + "\" class=\"" + labelClass + "\">"), 0);
						tr.storeToken(IoTools.prepareForHtml(fields[f].label, HTML_CHAR_MAPPING), 0);
						tr.storeToken("</label>", 0);
						
						//	add spacer
						tr.storeToken(" ", 0);
						
						//	write actual field
						if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
							tr.storeToken(("<input id=\"" + fieldId + "\" class=\"" + inputClass + "\" type=\"checkbox\" name=\"" + fields[f].name + "\" value=\"" + true + "\"" + ((fieldValues.containsKey(fields[f].name) || ((fields[f].value != null) && (fields[f].value.length() != 0))) ? " checked=\"true\"" : "") + ">"), 0);
						
						else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
							tr.storeToken(("<select id=\"" + fieldId + "\" class=\"" + inputClass + "\" name=\"" + fields[f].name + "\">"), 0);
							
							String preSelected = fieldValues.getProperty(fields[f].name);
							if (preSelected == null) preSelected = fields[f].value;
							
							SearchFieldOption[] fieldOptions = fields[f].getOptions();
							for (int o = 0; o < fieldOptions.length; o++) {
								tr.storeToken(("<option value=\"" + fieldOptions[o].value + "\"" + (fieldOptions[o].value.equals(preSelected) ? " selected=\"true\"" : "") + ">"), 0);
								tr.storeToken(IoTools.prepareForHtml(fieldOptions[o].label, HTML_CHAR_MAPPING), 0);
								tr.storeToken("</option>", 0);
							}
							
							tr.storeToken("</select>", 0);
						}
						else
							tr.storeToken(("<input id=\"" + fieldId + "\" class=\"" + inputClass + "\" name=\"" + fields[f].name + "\" value=\"" + fieldValues.getProperty(fields[f].name, fields[f].value) + "\">"), 0);
						
						//	close table cell
						tr.storeToken("</td>", 0);
					}
					
					//	fill in empty cells
					for (int e = fieldRows[r].getWidth(); e < formWidth; e++) {
						tr.storeToken(("<td class=\"fieldSetTableCell1\">"), 0);
						tr.storeToken("&nbsp;", 0);
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
				tr.storeToken("<td class=\"buttonRow\">", 0);
				
				//	add button row fields (if any)
				if (buttonRowFields != null) {
					SearchField[] fields = buttonRowFields.getFields();
					for (int f = 0; f < fields.length; f++) {
						
						//	compute layout classes
						String labelClass = "brLabel";
						String inputClass = "brInput";
						if (SearchField.BOOLEAN_TYPE.equals(fields[f].type)) {
							labelClass += "Boolean";
							inputClass += "Boolean";
						}
						else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
							labelClass += ("Select" + fields[f].size);
							inputClass += ("Select" + fields[f].size);
						}
						else {
							labelClass += ("Text" + fields[f].size);
							inputClass += ("Text" + fields[f].size);
						}
						
						//	create id attributes
						String labelId = ("brLabel" + fields[f].name.replaceAll("\\.", ""));
						String fieldId = ("brInput" + fields[f].name.replaceAll("\\.", ""));
						
						//	write label
						tr.storeToken(("<label id=\"" + labelId + "\" class=\"" + labelClass + "\">"), 0);
						tr.storeToken(IoTools.prepareForHtml(fields[f].label, HTML_CHAR_MAPPING), 0);
						tr.storeToken("</label>", 0);
						
						//	add spacer
						tr.storeToken(" ", 0);
						
						//	write actual field
						if (SearchField.BOOLEAN_TYPE.equals(fields[f].type))
							tr.storeToken(("<input id=\"" + fieldId + "\" class=\"" + inputClass + "\" type=\"checkbox\" name=\"" + fields[f].name + "\" value=\"" + true + "\"" + ((fieldValues.containsKey(fields[f].name) || ((fields[f].value != null) && (fields[f].value.length() != 0))) ? " checked=\"true\"" : "") + ">"), 0);
						
						else if (SearchField.SELECT_TYPE.equals(fields[f].type)) {
							tr.storeToken(("<select id=\"" + fieldId + "\" class=\"" + inputClass + "\" name=\"" + fields[f].name + "\">"), 0);
							
							String preSelected = fieldValues.getProperty(fields[f].name);
							if (preSelected == null) preSelected = fields[f].value;
							
							SearchFieldOption[] fieldOptions = fields[f].getOptions();
							for (int o = 0; o < fieldOptions.length; o++) {
								tr.storeToken(("<option value=\"" + fieldOptions[o].value + "\"" + (fieldOptions[o].value.equals(preSelected) ? " selected=\"true\"" : "") + ">"), 0);
								tr.storeToken(IoTools.prepareForHtml(fieldOptions[o].label, HTML_CHAR_MAPPING), 0);
								tr.storeToken("</option>", 0);
							}
							
							tr.storeToken("</select>", 0);
						}
						else
							tr.storeToken(("<input id=\"" + fieldId + "\" class=\"" + inputClass + "\" name=\"" + fields[f].name + "\" value=\"" + fieldValues.getProperty(fields[f].name, fields[f].value) + "\">"), 0);
						
						//	add spacer
						tr.storeToken("&nbsp;", 0);
					}
				}
				
				//	add buttons
				tr.storeToken("<input type=\"submit\" value=\"Search\" class=\"submitButton\">", 0);
				tr.storeToken("&nbsp;", 0);
				tr.storeToken("<input type=\"reset\" value=\"Reset\" class=\"resetButton\">", 0);
				
				//	close button row
				tr.storeToken("</td>", 0);
				tr.storeToken("</tr>", 0);
				
				//	close form
				tr.storeToken("</form>", 0);
			}
		}
		
		//	close master table
		tr.storeToken("</table>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeThesaurusResult(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedThesaurusResult, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeThesaurusResult(final BufferedThesaurusResult result, HtmlPageBuilder tr) throws IOException {
		
		PipedInputStream pis = new PipedInputStream();
		final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new PipedOutputStream(pis), "utf-8"));
		OutputWriter writer = new OutputWriter() {
			public void writeOutput() throws IOException {
				writeThesaurusResult(result, bw, RESULTS_NODE_NAME);
				bw.flush();
				bw.close();
			}
			public String getOutputName() {
				return "Thesaurus Result";
			}
		};
		
		//	do transformation
		this.doTransformation(writer, pis, this.thesaurusResultTransformer, this.thesaurusResultTransformerError, tr);
	}
	
	private void writeThesaurusResult(BufferedThesaurusResult result, BufferedWriter out, String rootNodeType) throws IOException {
		
		//	write empty data
		if (result.isEmpty()) {
			
			//	write results
			StringVector thesaurusFields = new StringVector();
			thesaurusFields.addContent(result.resultAttributes);
			out.write("<" + rootNodeType + 
					" " + RESULT_SIZE_ATTRIBUTE + "=\"" + 0 + "\"" +
					" " + RESULT_INDEX_NAME_ATTRIBUTE + "=\"" + result.thesaurusEntryType + "\"" +
					" " + RESULT_INDEX_LABEL_ATTRIBUTE + "=\"" + result.thesaurusName + "\"" +
					" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + thesaurusFields.concatStrings(" ") + "\"" +
			"/>");
			out.newLine();
		}
		
		//	write data
		else {
			
			//	sort list
			result.sort(result.resultAttributes);
			
			//	get result field names
			StringVector thesaurusFields = new StringVector();
			thesaurusFields.addContent(result.resultAttributes);
			out.write("<" + rootNodeType + 
					" " + RESULT_SIZE_ATTRIBUTE + "=\"" + result.size() + "\"" +
					" " + RESULT_INDEX_NAME_ATTRIBUTE + "=\"" + result.thesaurusEntryType + "\"" +
					" " + RESULT_INDEX_LABEL_ATTRIBUTE + "=\"" + result.thesaurusName + "\"" +
					" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + thesaurusFields.concatStrings(" ") + "\"" +
			">");
			
			for (ThesaurusResult tr = result.getThesaurusResult(); tr.hasNextElement();) {
				ThesaurusResultElement tre = tr.getNextThesaurusResultElement();
				out.write("  <" + RESULT_NODE_NAME);
				for (int f = 0; f < thesaurusFields.size(); f++) {
					String thesaurusField = thesaurusFields.get(f);
					String thesaurusFieldValue = ((String) tre.getAttribute(thesaurusField));
					if ((thesaurusFieldValue != null) && (thesaurusFieldValue.length() != 0))
						out.write(" " + thesaurusField + "=\"" + AnnotationUtils.escapeForXml(thesaurusFieldValue, true) + "\"");
				}
				out.write("/>");
				out.newLine();
			}
			
			out.write("</" + rootNodeType + ">");
			out.newLine();
		}
		
		out.flush();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeStatisticsLine(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedCollectionStatistics, de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalConstants.NavigationLink[], de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeStatisticsLine(BufferedCollectionStatistics statistics, NavigationLink[] links, HtmlPageBuilder tr) throws IOException {
		tr.storeToken("<span class=\"statisticsLine\">", 0);
		tr.storeToken("Search through&nbsp;", 0);
		tr.storeToken(((statistics == null) ? "0" : ("" + statistics.docCount)), 0);
		tr.storeToken(("&nbsp;" + this.parent.getDocumentLabelPlural().toLowerCase() + " ("), 0);
		tr.storeToken((((statistics == null) ? "0" : ("" + statistics.masterDocCount)) + "&nbsp;documents"), 0);
		tr.storeToken(") in the archive.", 0);
		
		//	include links (if any)
		if (links != null) {
			for (int l = 0; l < links.length; l++) {
				tr.storeToken("<span class=\"statisticsLineLink\">", 0);
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
				tr.storeToken("</span>", 0);
			}
		}
		
		tr.storeToken("</span>", 0);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.SearchPortalLayout#includeStatistics(de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedCollectionStatistics, de.uka.ipd.idaho.htmlXmlUtil.HtmlPageBuilder)
	 */
	public void includeStatistics(final BufferedCollectionStatistics statistics, HtmlPageBuilder tr) throws IOException {
		
		PipedInputStream pis = new PipedInputStream();
		final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new PipedOutputStream(pis), "utf-8"));
		OutputWriter writer = new OutputWriter() {
			public void writeOutput() throws IOException {
				writeStatistics(statistics, bw);
				bw.flush();
				bw.close();
			}
			public String getOutputName() {
				return "Statistics";
			}
		};
		
		//	do transformation
		this.doTransformation(writer, pis, this.statisticsTransformer, this.statisticsTransformerError, tr);
	}
	
	private void writeStatistics(BufferedCollectionStatistics statistics, BufferedWriter out) throws IOException {
		
		StringVector userTableFields = new StringVector();
		userTableFields.addContent(statistics.resultAttributes);
		out.write("<" + RESULTS_NODE_NAME + 
				" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + userTableFields.concatStrings(" ") + "\"" +
				" " + MASTER_DOCUMENT_COUNT_ATTRIBUTE + "=\"" + statistics.masterDocCount + "\"" +
				" " + DOCUMENT_COUNT_ATTRIBUTE + "=\"" + statistics.docCount + "\"" +
				" " + WORD_COUNT_ATTRIBUTE + "=\"" + statistics.wordCount + "\"" +
		">");
		out.newLine();
		
		for (CollectionStatistics cs = statistics.getCollectionStatistics(); cs.hasNextElement();) {
			CollectionStatisticsElement cse = cs.getNextCollectionStatisticsElement();
			out.write("  <" + RESULT_NODE_NAME);
			for (int f = 0; f < userTableFields.size(); f++) {
				String userTableField = userTableFields.get(f);
				String userFieldValue = ((String) cse.getAttribute(userTableField));
				if (userFieldValue != null)
					out.write(" " + userTableField + "=\"" + AnnotationUtils.escapeForXml(userFieldValue, true) + "\"");
			}
			out.write("/>");
			out.newLine();
		}
		
		out.write("</" + RESULTS_NODE_NAME + ">");
		out.newLine();
		
		out.flush();
	}
}
