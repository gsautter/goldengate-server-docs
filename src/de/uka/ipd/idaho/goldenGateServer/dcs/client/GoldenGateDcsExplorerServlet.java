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
package de.uka.ipd.idaho.goldenGateServer.dcs.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import de.uka.ipd.idaho.gamta.util.CountingSet;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.DcStatistics;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatField;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Client servlet for exploring statistics data from GoldenGATE DCS instances.
 * 
 * @author sautter
 */
public abstract class GoldenGateDcsExplorerServlet extends GoldenGateDcsClientServlet {
	private static final Grammar xmlGrammar = new StandardGrammar();
	private static final Parser xmlParser = new Parser(xmlGrammar);
	
	private ArrayList expFields = new ArrayList();
	private TreeMap expFieldsByName = new TreeMap();
	private TreeMap expFieldTreesById = new TreeMap();
	private TreeMap expFieldTreesByExpFieldNames = new TreeMap();
	
	private Transformer expPageBuilder;
	private IOException expPageBuilderException;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.dcs.client.GoldenGateDcsClientServlet#reInit()
	 */
	protected void reInit() throws ServletException {
		super.reInit();
		
		//	clear index data structures
		this.expFields.clear();
		this.expFieldsByName.clear();
		this.expFieldTreesById.clear();
		this.expFieldTreesByExpFieldNames.clear();
		
		//	read field definitions
		try {
			BufferedReader expFieldReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(this.dataFolder, "StatExplorerFields.xml")), "UTF-8"));
			xmlParser.stream(expFieldReader, new TokenReceiver() {
				private ExpFieldTree fieldTree = null;
				private ExpField lastTreeField = null;
				public void storeToken(String token, int treeDepth) throws IOException {
					if (!xmlGrammar.isTag(token))
						return;
					String type = xmlGrammar.getType(token);
					if ("fieldTree".equals(type)) {
						if (xmlGrammar.isEndTag(token)) {
							if ((this.fieldTree != null) && (this.fieldTree.fields.size() != 0)) {
								if (this.fieldTree.fields.size() == 1)
									expFields.add(this.fieldTree.fields.get(0));
								else {
									expFields.add(this.fieldTree);
									expFieldTreesById.put(this.fieldTree.id, this.fieldTree);
									for (int f = 0; f < this.fieldTree.fields.size(); f++)
										expFieldTreesByExpFieldNames.put(((ExpField) this.fieldTree.fields.get(f)).name, this.fieldTree);
								}
							}
							this.fieldTree = null;
							this.lastTreeField = null;
						}
						else {
							TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, xmlGrammar);
							String id = tnas.getAttribute("id");
							String label = tnas.getAttribute("label");
							if ((id != null) && (label != null))
								this.fieldTree = new ExpFieldTree(id, label);
						}
					}
					else if ("field".equals(type)) {
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, xmlGrammar);
						String name = tnas.getAttribute("name");
						String label = tnas.getAttribute("label");
						String level = tnas.getAttribute("level");
						if ((name != null) && (label != null)) {
							if ((this.fieldTree == null) || (level == null)) {
								ExpField ef = new ExpField(name, label, null, null);
								expFields.add(ef);
								expFieldsByName.put(ef.name, ef);
							}
							else {
								this.lastTreeField = new ExpField(name, label, this.lastTreeField, level);
								this.fieldTree.fields.add(this.lastTreeField);
								expFieldsByName.put(this.lastTreeField.name, this.lastTreeField);
							}
						}
					}
				}
				public void close() throws IOException {}
			});
			expFieldReader.close();
		}
		catch (IOException ioe) {
			ioe.printStackTrace(System.out);
		}
		
		//	(re-) load XSLT
		try {
			this.expPageBuilder = XsltUtils.getTransformer(new File(this.dataPath, "StatExplorer.xslt"), false);
			this.expPageBuilderException = null;
		}
		catch (IOException ioe) {
			this.expPageBuilderException = ioe;
		}
	}
	
	private static class ExpField {
		final String name;
		final String label;
		ExpField parent;
		ExpField child;
		String level;
		ExpField(String name, String label, ExpField parent, String level) {
			this.name = name;
			this.label = label;
			this.parent = parent;
			if (this.parent != null)
				this.parent.child = this;
			this.level = level;
		}
	}
	
	private static class ExpFieldTree {
		final String id;
		final String label;
		ArrayList fields = new ArrayList();
		ExpFieldTree(String id, String label) {
			this.id = id;
			this.label = label;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		GoldenGateDcsClient dcsc = this.getDcsClient();
		
		//	send field tree expansion JavaScript
		if ("/expandTreeNode".equals(request.getPathInfo())) {
			
			//	get and check specific parameters
			String eftId = request.getParameter("treeId");
			String eftLevel = request.getParameter("level");
			String eftValue = request.getParameter("value");
			ExpField etef = ((ExpField) this.expFieldsByName.get(eftId + "." + eftLevel));
			StatField esf = this.getField(eftId + "." + eftLevel);
			if ((etef == null) || (etef.child == null) || (eftValue == null) || (esf == null)) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid tree ID or level");
				return;
			}
			
			//	add filters
			StringVector outputFields = new StringVector();
			StringVector groupingFields = new StringVector();
			Properties fieldPredicates = new Properties();
			Properties fieldAggregates = new Properties();
			for (int f = 0; f < this.expFields.size(); f++) {
				Object fObj = this.expFields.get(f);
				if (fObj instanceof ExpField) {
					ExpField ef = ((ExpField) fObj);
					String efPredicate = request.getParameter(ef.name);
					if (efPredicate != null)
						fieldPredicates.setProperty(ef.name, efPredicate);
				}
				else if (fObj instanceof ExpFieldTree) {
					ExpFieldTree eft = ((ExpFieldTree) fObj);
					int lastPredicateTfIndex = -1;
					for (int tf = 0; tf < eft.fields.size(); tf++) {
						ExpField tef = ((ExpField) eft.fields.get(tf));
						if (request.getParameter(tef.name) != null)
							lastPredicateTfIndex = tf;
					}
					for (int tf = 0; tf < eft.fields.size(); tf++) {
						ExpField tef = ((ExpField) eft.fields.get(tf));
						String tefPreficate = request.getParameter(tef.name);
						if (tefPreficate != null)
							fieldPredicates.setProperty(tef.name, tefPreficate);
						else if (lastPredicateTfIndex < tf)
							break;
					}
				}
			}
			
			//	add output fields for to-expand tree level
			outputFields.addElementIgnoreDuplicates(etef.child.name);
			groupingFields.addElementIgnoreDuplicates(etef.child.name);
			
			//	get stats from backend
			DcStatistics stats = dcsc.getStatistics(outputFields.toStringArray(), groupingFields.toStringArray(), new String[0], fieldPredicates, fieldAggregates, new Properties());
			
			//	send Javascript
			response.setContentType("text/javascript");
			response.setCharacterEncoding("UTF-8");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
			for (int s = 0; s < stats.size(); s++) {
				StringTupel st = stats.get(s);
				bw.write("appendTreeNode('" + eftLevel + "', '" + eftValue + "', '" + eftId + "', '" + etef.level + "', '" + st.getValue(esf.statColName, "") + "', " + st.getValue("DocCount") + ", " + (etef.child != null) + ", '" + etef.name + "', '" + etef.label + "');");
				bw.newLine();
			}
			bw.flush();
			return;
		}
		
		//	TODO send treatment lister JavaScript
		if ("/getDocumentData".equals(request.getPathInfo())) {
			
			//	get and check specific parameters
			String eftId = request.getParameter("treeId");
			String eftLevel = request.getParameter("level");
			String eftValue = request.getParameter("value");
			ExpField etef = ((ExpField) this.expFieldsByName.get(eftId + "." + eftLevel));
			StatField esf = this.getField(eftId + "." + eftLevel);
			if ((etef == null) || (etef.child == null) || (eftValue == null) || (esf == null)) {
				response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid tree ID or level");
				return;
			}
			
			//	add filters
			StringVector outputFields = new StringVector();
			StringVector groupingFields = new StringVector();
			Properties fieldPredicates = new Properties();
			Properties fieldAggregates = new Properties();
			for (int f = 0; f < this.expFields.size(); f++) {
				Object fObj = this.expFields.get(f);
				if (fObj instanceof ExpField) {
					ExpField ef = ((ExpField) fObj);
					String efPredicate = request.getParameter(ef.name);
					if (efPredicate != null)
						fieldPredicates.setProperty(ef.name, efPredicate);
				}
				else if (fObj instanceof ExpFieldTree) {
					ExpFieldTree eft = ((ExpFieldTree) fObj);
					int lastPredicateTfIndex = -1;
					for (int tf = 0; tf < eft.fields.size(); tf++) {
						ExpField tef = ((ExpField) eft.fields.get(tf));
						if (request.getParameter(tef.name) != null)
							lastPredicateTfIndex = tf;
					}
					for (int tf = 0; tf < eft.fields.size(); tf++) {
						ExpField tef = ((ExpField) eft.fields.get(tf));
						String tefPreficate = request.getParameter(tef.name);
						if (tefPreficate != null)
							fieldPredicates.setProperty(tef.name, tefPreficate);
						else if (lastPredicateTfIndex < tf)
							break;
					}
				}
			}
			
			//	add output fields for to-expand tree level
			outputFields.addElementIgnoreDuplicates(etef.child.name);
			groupingFields.addElementIgnoreDuplicates(etef.child.name);
			
			//	get stats from backend
			DcStatistics stats = dcsc.getStatistics(outputFields.toStringArray(), groupingFields.toStringArray(), new String[0], fieldPredicates, fieldAggregates, new Properties());
			
			//	send Javascript
			response.setContentType("application/json");
			response.setCharacterEncoding("UTF-8");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
			for (int s = 0; s < stats.size(); s++) {
				StringTupel st = stats.get(s);
				bw.write("appendTreeNode('" + eftLevel + "', '" + eftValue + "', '" + eftId + "', '" + etef.level + "', '" + st.getValue(esf.statColName, "") + "', " + st.getValue("DocCount") + ", " + (etef.child != null) + ", '" + etef.name + "', '" + etef.label + "');");
				bw.newLine();
			}
			bw.flush();
			return;
		}
		
		//	handle other requests the standard way
		super.doGet(request, response);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#getPageBuilder(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected HtmlPageBuilder getPageBuilder(HttpServletRequest request, HttpServletResponse response) throws IOException {
		GoldenGateDcsClient dcsc = this.getDcsClient();
		
		//	read parameters
		StringVector outputFields = new StringVector();
		StringVector groupingFields = new StringVector();
		Properties fieldPredicates = new Properties();
		Properties fieldAggregates = new Properties();
		for (int f = 0; f < this.expFields.size(); f++) {
			Object fObj = this.expFields.get(f);
			if (fObj instanceof ExpField) {
				ExpField ef = ((ExpField) fObj);
				outputFields.addElementIgnoreDuplicates(ef.name);
				groupingFields.addElementIgnoreDuplicates(ef.name);
				String efPredicate = request.getParameter(ef.name);
				if (efPredicate != null)
					fieldPredicates.setProperty(ef.name, efPredicate);
			}
			else if (fObj instanceof ExpFieldTree) {
				ExpFieldTree eft = ((ExpFieldTree) fObj);
				int lastPredicateTfIndex = -1;
				for (int tf = 0; tf < eft.fields.size(); tf++) {
					ExpField tef = ((ExpField) eft.fields.get(tf));
					if (request.getParameter(tef.name) != null)
						lastPredicateTfIndex = tf;
				}
				for (int tf = 0; tf < eft.fields.size(); tf++) {
					ExpField tef = ((ExpField) eft.fields.get(tf));
					outputFields.addElementIgnoreDuplicates(tef.name);
					groupingFields.addElementIgnoreDuplicates(tef.name);
					String tefPreficate = request.getParameter(tef.name);
					if (tefPreficate != null)
						fieldPredicates.setProperty(tef.name, tefPreficate);
					else if (lastPredicateTfIndex < tf)
						break;
				}
			}
		}
		
		//	TODO add additional output fields and aggregates if required (e.g. UUIDs for linking to treatments or article summaries)
		
		//	get stats from backend
		DcStatistics stats = dcsc.getStatistics(outputFields.toStringArray(), groupingFields.toStringArray(), new String[0], fieldPredicates, fieldAggregates, new Properties());
		
		//	run aggregation
		TreeMap valueCountsByFieldName = new TreeMap();
		for (int s = 0; s < stats.size(); s++) {
			StringTupel st = stats.get(s);
			int stDocCount = Integer.parseInt(st.getValue("DocCount"));
			for (int f = 0; f < groupingFields.size(); f++) {
				StatField sf = this.getField(groupingFields.get(f));
				if (sf == null)
					continue;
				CountingSet sfValueCounter = ((CountingSet) valueCountsByFieldName.get(sf.fullName));
				if (sfValueCounter == null) {
					sfValueCounter = new CountingSet(new TreeMap(String.CASE_INSENSITIVE_ORDER));
					valueCountsByFieldName.put(sf.fullName, sfValueCounter);
				}
				sfValueCounter.add(st.getValue(sf.statColName, ""), stDocCount);
			}
		}
		
		//	create XML
		StringWriter xmlSw = new StringWriter();
		xmlSw.write("<exploration>\r\n");
		for (int f = 0; f < groupingFields.size(); f++) {
			StatField sf = this.getField(groupingFields.get(f));
			if (sf == null)
				continue;
			CountingSet efValueCounts = ((CountingSet) valueCountsByFieldName.get(groupingFields.get(f)));
			if (efValueCounts == null)
				continue;
			ExpField ef = ((ExpField) this.expFieldsByName.get(sf.fullName));
			if (ef == null)
				continue;
			if (fieldPredicates.containsKey(sf.fullName))
				xmlSw.write("<filter field=\"" + ef.name + "\" label=\"" + xmlGrammar.escape(ef.label) + "\" value=\"" + xmlGrammar.escape(fieldPredicates.getProperty(ef.name)) + "\"/>\r\n");
			else if ((efValueCounts.elementCount() == 1) && this.expFieldTreesByExpFieldNames.containsKey(ef.name))
				xmlSw.write("<filter field=\"" + ef.name + "\" label=\"" + xmlGrammar.escape(ef.label) + "\" value=\"" + xmlGrammar.escape(efValueCounts.first().toString()) + "\"/>\r\n");
		}
		for (int f = 0; f < this.expFields.size(); f++) {
			Object fObj = this.expFields.get(f);
			if (fObj instanceof ExpField) {
				ExpField ef = ((ExpField) fObj);
				if (fieldPredicates.containsKey(ef.name))
					continue;
				CountingSet efValueCounts = ((CountingSet) valueCountsByFieldName.get(ef.name));
				if (efValueCounts == null)
					continue;
				int[] sortedEfValueCounts = new int[efValueCounts.elementCount()];
				int efValueCountIndex = 0;
				for (Iterator fvit = efValueCounts.iterator(); fvit.hasNext();)
					sortedEfValueCounts[efValueCountIndex++] = -efValueCounts.getCount(fvit.next());
				Arrays.sort(sortedEfValueCounts);
				for (int i = 0; i < sortedEfValueCounts.length; i++)
					sortedEfValueCounts[i] = -sortedEfValueCounts[i];
				HashMap countTiers = new HashMap();
				int tier = 0;
				int tierEnd = 10;
				for (int i = 0; i < sortedEfValueCounts.length;) {
					while ((i < sortedEfValueCounts.length) && ((i < tierEnd) || (sortedEfValueCounts[i] == sortedEfValueCounts[i-1])))
						countTiers.put(new Integer(sortedEfValueCounts[i++]), new Integer(tier));
					tier++;
					tierEnd *= 3;
				}
				xmlSw.write("<field name=\"" + ef.name + "\" label=\"" + xmlGrammar.escape(ef.label) + "\" valueCount=\"" + efValueCounts.elementCount() + "\">\r\n");
				for (Iterator fvit = efValueCounts.iterator(); fvit.hasNext();) {
					String efValue = ((String) fvit.next());
					int efValueCount = efValueCounts.getCount(efValue);
					int efTier = ((Integer) countTiers.get(new Integer(efValueCount))).intValue();
					xmlSw.write("<value value=\"" + xmlGrammar.escape(efValue) + "\" count=\"" + efValueCount + "\" tier=\"" + efTier + "\"/>\r\n");
				}
				xmlSw.write("</field>\r\n");
			}
			else if (fObj instanceof ExpFieldTree) {
				ExpFieldTree eft = ((ExpFieldTree) fObj);
				int lastPredicateTfIndex = -1;
				for (int tf = 0; tf < eft.fields.size(); tf++) {
					ExpField tef = ((ExpField) eft.fields.get(tf));
					if (fieldPredicates.containsKey(tef.name))
						lastPredicateTfIndex = tf;
				}
				xmlSw.write("<fieldTree id=\"" + eft.id + "\" label=\"" + eft.label + "\">\r\n");
				for (int tf = 0; tf < eft.fields.size(); tf++) {
					ExpField tef = ((ExpField) eft.fields.get(tf));
					CountingSet tefValueCounts = ((CountingSet) valueCountsByFieldName.get(tef.name));
					if (tefValueCounts == null)
						continue;
					if (fieldPredicates.containsKey(tef.name) || (tf < lastPredicateTfIndex))
						xmlSw.write("<field name=\"" + tef.name + "\" label=\"" + xmlGrammar.escape(tef.label) + "\" level=\"" + tef.level + "\"/>\r\n");
					else {
						xmlSw.write("<field name=\"" + tef.name + "\" label=\"" + xmlGrammar.escape(tef.label) + "\" level=\"" + tef.level + "\">\r\n");
						for (Iterator fvit = tefValueCounts.iterator(); fvit.hasNext();) {
							String tefValue = ((String) fvit.next());
							xmlSw.write("<value value=\"" + xmlGrammar.escape(tefValue) + "\" count=\"" + tefValueCounts.getCount(tefValue) + "\"/>\r\n");
						}
						xmlSw.write("</field>\r\n");
					}
				}
				xmlSw.write("</fieldTree>\r\n");
			}
		}
		xmlSw.write("</explotation>");
		
		//	return page builder creating HTML page from XML via XSLT
		final StringReader xmlSr = new StringReader(xmlSw.toString());
		return new HtmlPageBuilder(this, request, response) {
			protected boolean includeJavaScriptDomHelpers() {
				return true;
			}
			protected void include(String type, String tag) throws IOException {
				if ("includeBody".equals(type)) {
					if (expPageBuilder == null)
						this.writeExceptionAsXmlComment("Cannot generate Stats Explorer Page due to faulty XSLT", expPageBuilderException);
					else try {
						expPageBuilder.transform(new StreamSource(xmlSr), new StreamResult(this.asWriter()));
					}
					catch (TransformerException te) {
						this.writeExceptionAsXmlComment("Could not generate Stats Explorer Page", te);
					}
				}
				else super.include(type, tag);
			}
			protected void writePageHeadExtensions() throws IOException {
				
				//	general filter handling functions
				this.writeLine("<script type=\"text/javascript\">");
				this.writeLine("var filters = new Object();");
				this.writeLine("function removeFilter(name) {");
				this.writeLine("  var filterRow = getById('filterRow_' + name);");
				this.writeLine("  if (filterRow != null)");
				this.writeLine("    removeElement(filterRow);");
				this.writeLine("  filters[name] = null;");
				this.writeLine("  updateFilters();");
				this.writeLine("}");
				this.writeLine("function updateFilters() {");
				this.writeLine("  var filterForm = getById('filterForm');");
				this.writeLine("  if (filterForm == null)");
				this.writeLine("    return;");
				this.writeLine("  var filtersStr = 'Updating page for the following filters:';");
				this.writeLine("  for (var name in filters) {");
				this.writeLine("    var filterValue = filters[name];");
				this.writeLine("    if (filterValue == null)");
				this.writeLine("      continue;");
				this.writeLine("    filtersStr += ('\n' + name + ' = ' + filterValue);");
				this.writeLine("    var filterField = newElement('input', null, null, null);");
				this.writeLine("    setAttribute(filterField, 'type', 'hidden');");
				this.writeLine("    setAttribute(filterField, 'name', name);");
				this.writeLine("    setAttribute(filterField, 'value', filterValue);");
				this.writeLine("    filterForm.appendChild(filterField);");
				this.writeLine("  }");
				this.writeLine("  alert(filtersStr);");
				this.writeLine("  filterForm.submit();");
				this.writeLine("}");
				this.writeLine("</script>");
				
				//	handling functions for flat category filters
				this.writeLine("<script type=\"text/javascript\">");
				this.writeLine("var visibleFilterTiers = new Object();");
				this.writeLine("function moreFilterValues(id) {");
				this.writeLine("  var visibleTier = visibleFilterTiers[id];");
				this.writeLine("  var showingAllTiers = true;");
				this.writeLine("  if (visibleTier == null)");
				this.writeLine("    visibleTier = 0;");
				this.writeLine("  visibleTier++;");
				this.writeLine("  visibleFilterTiers[id] = visibleTier;");
				this.writeLine("  var tierStyle = getById(id + '_tier' + visibleTier);");
				this.writeLine("  if (tierStyle == null)");
				this.writeLine("    return;");
				this.writeLine("  tierStyle.innerHTML = ('.tier' + visibleTier + ' {}');");
				this.writeLine("  var moreRow = getById('moreRow_' + id);");
				this.writeLine("  if (moreRow == null)");
				this.writeLine("    return;");
				this.writeLine("  if (getById(id + '_tier' + (visibleTier + 1)) == null)");
				this.writeLine("    moreRow.style.display = 'none';");
				this.writeLine("}");
				this.writeLine("function addFilter(id) {");
				this.writeLine("  var filterValue = '';");
				this.writeLine("  for (var s = 0;; s++) {");
				this.writeLine("    var value = getById(id + '_value' + s);");
				this.writeLine("    if (value == null)");
				this.writeLine("      break;");
				this.writeLine("    if (value.checked == false)");
				this.writeLine("      continue;");
				this.writeLine("    var valueRow = getById('valueRow_' + id + s);");
				this.writeLine("    if (valueRow == null)");
				this.writeLine("      break;");
				this.writeLine("    if (valueRow.style.display == 'none')");
				this.writeLine("      continue;");
				this.writeLine("    if (filterValue.length != 0)");
				this.writeLine("      filterValue += ' ';");
				this.writeLine("    filterValue += ('\"' + value.value + '\"');");
				this.writeLine("  }");
				this.writeLine("  if (filterValue.length == 0)");
				this.writeLine("    return;");
				this.writeLine("  filters[id] = filterValue;");
				this.writeLine("  alert('Adding filter for ' + id + ' with value ' + filterValue);");
				this.writeLine("  updateFilters();");
				this.writeLine("}");
				this.writeLine("var filterValueSearches = new Object();");
				this.writeLine("function searchFilterValues(name) {");
				this.writeLine("  var searchValue = getById(name + '_searchValue');");
				this.writeLine("  if (searchValue == null)");
				this.writeLine("    return;");
				this.writeLine("  var searchStr = searchValue.value.toLowerCase();");
				this.writeLine("  if (filterValueSearches[name] == searchStr)");
				this.writeLine("    return;");
				this.writeLine("  for (var s = 0;; s++) {");
				this.writeLine("    var value = getById(name + '_value' + s);");
				this.writeLine("    if (value == null)");
				this.writeLine("      break;");
				this.writeLine("    var valueRow = getById('valueRow_' + name + s);");
				this.writeLine("    if (valueRow == null)");
				this.writeLine("      break;");
				this.writeLine("    if (searchStr.length == 0)");
				this.writeLine("      valueRow.style.display = null;");
				this.writeLine("    else if (value.value.toLowerCase().indexOf(searchStr) == -1)");
				this.writeLine("      valueRow.style.display = 'none';");
				this.writeLine("    else valueRow.style.display = 'table-row';");
				this.writeLine("  }");
				this.writeLine("  filterValueSearches[name] = searchStr;");
				this.writeLine("}");
				this.writeLine("</script>");
				
				//	handling functions for field tree filters
				this.writeLine("<script type=\"text/javascript\">");
				this.writeLine("function expandCollapseTreeNode(level, value, treeId, expand) {");
				this.writeLine("  var expandNode = getById('valueRow_' + treeId + '_' + level + '_' + value);");
				this.writeLine("  if ((expandNode.nextElementSibling == null) || (levelRanks[expandNode.className] >= levelRanks[expandNode.nextElementSibling.className])) {");
				this.writeLine("    var expandButton = getById('expandCollapse_' + treeId + '_' + level + '_' + value);");
				this.writeLine("    expandButton.innerHTML = '-';");
				this.writeLine("    expandButton.onclick = function() {");
				this.writeLine("      expandCollapseTreeNode(level, value, treeId, false);");
				this.writeLine("    };");
				this.writeLine("    var filtersStr = '';");
				this.writeLine("    for (var name in filters) {");
				this.writeLine("      var filterValue = filters[name];");
				this.writeLine("      if (filterValue != null)");
				this.writeLine("        filtersStr += ('&' + name + '=' + encodeURIComponent(filterValue));");
				this.writeLine("    }");
				this.writeLine("    var filterLevel = levelRanks[expandNode.className] + 1;");
				this.writeLine("    for (var expandParent = expandNode; expandParent != null; expandParent = expandParent.previousElementSibling) {");
				this.writeLine("      if (levelRanks[expandParent.className] >= filterLevel)");
				this.writeLine("        continue;");
				this.writeLine("      filterLevel = levelRanks[expandParent.className];");
				this.writeLine("      if (expandParent.firstElementChild.name && expandParent.firstElementChild.value)");
				this.writeLine("        filtersStr += ('&' + expandParent.firstElementChild.name + '=' + expandParent.firstElementChild.value);");
				this.writeLine("      if (filterLevel == 0)");
				this.writeLine("        break;");
				this.writeLine("    }");
				//	TODO activate for online use
				this.writeLine("    var expandScript = newElement('script', 'expandTreeNodeScript', null, null);");
				this.writeLine("    expandScript.type = 'text/javascript';");
				this.writeLine("    expandScript.src = ('/" + this.request.getContextPath() + "/" + this.request.getServletPath() + "/expandTreeNode?treeId=' + treeId + '&level=' + level + '&value=' + value + filtersStr + '&time=' + (new Date).getTime());");
				this.writeLine("    var es = getById('expandTreeNodeScript');");
				this.writeLine("    var esp = es.parentNode;");
				this.writeLine("    removeElement(es);");
				this.writeLine("    esp.appendChild(expandScript);");
				//	TODO de-activate for online use
//				this.writeLine("    alert('Adding expansion script ' + '/GgServer/explorer/expandTreeNode?tree=' + treeId + '&amp;level=' + level + '&amp;value=' + value + filtersStr + '&amp;time=' + (new Date).getTime());");
//				this.writeLine("    for (var c = 0; c < 4; c++) {");
//				this.writeLine("      var childLevel = null;");
//				this.writeLine("      var childValue = null;");
//				this.writeLine("      var childExpandable = false;");
//				this.writeLine("      var childField = null;");
//				this.writeLine("      var childFieldLabel = null;");
//				this.writeLine("      if (level == 'kingdom') {");
//				this.writeLine("        childLevel = 'phylum';");
//				this.writeLine("        childValue = ('Phylum' + Math.round(Math.random() * 65536));");
//				this.writeLine("        childExpandable = true;");
//				this.writeLine("        childField = 'tax.phylum';");
//				this.writeLine("        childFieldLabel = 'Taxonomic Phylum';");
//				this.writeLine("      }");
//				this.writeLine("      else if (level == 'phylum') {");
//				this.writeLine("        childLevel = 'class';");
//				this.writeLine("        childValue = ('Class' + Math.round(Math.random() * 65536));");
//				this.writeLine("        childExpandable = true;");
//				this.writeLine("        childField = 'tax.class';");
//				this.writeLine("        childFieldLabel = 'Taxonomic Class';");
//				this.writeLine("      }");
//				this.writeLine("      else if (level == 'class') {");
//				this.writeLine("        childLevel = 'order';");
//				this.writeLine("        childValue = ('Order' + Math.round(Math.random() * 65536));");
//				this.writeLine("        childExpandable = true;");
//				this.writeLine("        childField = 'tax.order';");
//				this.writeLine("        childFieldLabel = 'Taxonomic Order';");
//				this.writeLine("      }");
//				this.writeLine("      else if (level == 'order') {");
//				this.writeLine("        childLevel = 'family';");
//				this.writeLine("        childValue = ('Family' + Math.round(Math.random() * 65536));");
//				this.writeLine("        childExpandable = true;");
//				this.writeLine("        childField = 'tax.family';");
//				this.writeLine("        childFieldLabel = 'Taxonomic Family';");
//				this.writeLine("      }");
//				this.writeLine("      else if (level == 'family') {");
//				this.writeLine("        childLevel = 'genus';");
//				this.writeLine("        childValue = ('Genus' + Math.round(Math.random() * 65536));");
//				this.writeLine("        childExpandable = true;");
//				this.writeLine("        childField = 'tax.genus';");
//				this.writeLine("        childFieldLabel = 'Taxonomic Genus';");
//				this.writeLine("      }");
//				this.writeLine("      else if (level == 'genus') {");
//				this.writeLine("        childLevel = 'species';");
//				this.writeLine("        childValue = ('species' + Math.round(Math.random() * 65536));");
//				this.writeLine("        childField = 'tax.species';");
//				this.writeLine("        childFieldLabel = 'Taxonomic Species';");
//				this.writeLine("      }");
//				this.writeLine("      appendTreeNode(level, value, treeId, childLevel, childValue, 10, childExpandable, childField, childFieldLabel);");
//				this.writeLine("    }");
				this.writeLine("  }");
				this.writeLine("  else {");
				this.writeLine("    var expandButton = getById('expandCollapse_' + treeId + '_' + level + '_' + value);");
				this.writeLine("    expandButton.innerHTML = (expand ? '-' : '+');");
				this.writeLine("    expandButton.onclick = function() {");
				this.writeLine("      expandCollapseTreeNode(level, value, treeId, !expand);");
				this.writeLine("    };");
				this.writeLine("    for (var expandChild = expandNode.nextElementSibling; expandChild != null; expandChild = expandChild.nextElementSibling) {");
				this.writeLine("      if (levelRanks[expandChild.className] <= levelRanks[expandNode.className])");
				this.writeLine("        break;");
				this.writeLine("      expandChild.style.display = (expand ? 'table-row' : 'none');");
				this.writeLine("    }");
				this.writeLine("  }");
				this.writeLine("}");
				this.writeLine("function appendTreeNode(parentLevel, parentValue, treeId, level, value, count, expandable, field, fieldLabel) {");
				this.writeLine("  var treeParentNode = getById('valueRow_' + treeId + '_' + parentLevel + '_' + parentValue);");
				this.writeLine("  var valueRow = newElement('tr', ('valueRow_' + treeId + '_' + level + '_' + value), ('valueRow_' + treeId + '_' + level), null);");
				this.writeLine("  var valueField = newElement('input', null, null, null);");
				this.writeLine("  valueField.type = 'hidden';");
				this.writeLine("  valueField.name = field;");
				this.writeLine("  valueField.value = value;");
				this.writeLine("  valueRow.insertBefore(valueField, valueRow.firstChild);");
				this.writeLine("  var mainCell = newElement('td', null, null, null);");
				this.writeLine("  valueRow.appendChild(mainCell);");
				this.writeLine("  var mainSpan = newElement('span', null, (treeId + '_' + level), (' ' + value + ' '));");
				this.writeLine("  mainCell.appendChild(mainSpan);");
				this.writeLine("  var expandButton;");
				this.writeLine("  if (expandable) {");
				this.writeLine("    expandButton = newElement('button', ('expandCollapse_' + treeId + '_' + level + '_' + value), 'form-control expand-collapse-tree', '+');");
				this.writeLine("    expandButton.onclick = function() {");
				this.writeLine("      expandCollapseTreeNode(level, value, treeId, true);");
				this.writeLine("    };");
				this.writeLine("  }");
				this.writeLine("  else expandButton = newElement('button', ('expandCollapse_' + treeId + '_' + level + '_' + value), 'form-control expand-collapse-tree', '-');");
				this.writeLine("  mainSpan.insertBefore(expandButton, mainSpan.firstChild);");
				this.writeLine("  var filterButton = newElement('button', null, 'form-control add-filter', 'Add Filter');");
				this.writeLine("  filterButton.title = ('Add Filter for ' + fieldLabel + ' \'' + value + '\'');");
				this.writeLine("  filterButton.onclick = function() {");
				this.writeLine("    addTreeFilter(level, value, treeId);");
				this.writeLine("  };");
				this.writeLine("  mainSpan.appendChild(filterButton);");
				this.writeLine("  var countCell = newElement('td', null, null, count);");
				this.writeLine("  countCell.style.width = '10%';");
				this.writeLine("  countCell.align = 'right';");
				this.writeLine("  valueRow.appendChild(countCell);");
				this.writeLine("  treeParentNode.parentNode.insertBefore(valueRow, treeParentNode.nextSibling);");
				this.writeLine("}");
				this.writeLine("function addTreeFilter(level, value, treeId) {");
				this.writeLine("  var filterNode = getById('valueRow_' + treeId + '_' + level + '_' + value);");
				this.writeLine("  filters[treeId + '.' + level] = ('\"' + value + '\"');");
				this.writeLine("  var filterLevel = levelRanks[filterNode.className];");
				this.writeLine("  for (var filterParent = filterNode.previousElementSibling; filterParent != null; filterParent = filterParent.previousElementSibling) {");
				this.writeLine("    if (levelRanks[filterParent.className] >= filterLevel)");
				this.writeLine("      continue;");
				this.writeLine("    filterLevel = levelRanks[filterParent.className];");
				this.writeLine("    if (filterParent.firstElementChild.name && filterParent.firstElementChild.value)");
				this.writeLine("      filters[filterParent.firstElementChild.name] = ('\"' + filterParent.firstElementChild.value + '\"');");
				this.writeLine("    if (filterLevel == 0)");
				this.writeLine("      break;");
				this.writeLine("  }");
				this.writeLine("  updateFilters();");
				this.writeLine("}");
				this.writeLine("</script>");
			}
		};
	}
	
	public static void main(String[] args) throws Exception {
		File path = new File("E:/Projektdaten/PlaziWebPortal2015");
		Transformer tr = XsltUtils.getTransformer(new File(path, "TreatmentExplorer.xslt"));
		tr.transform(new StreamSource(new BufferedInputStream(new FileInputStream(new File(path, "TreatmentExplorer.xml")))), new StreamResult(System.out));
	}
}
