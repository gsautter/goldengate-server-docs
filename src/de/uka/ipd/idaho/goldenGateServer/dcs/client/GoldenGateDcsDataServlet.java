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
 *     * Neither the name of the Universitaet Karlsruhe (TH) / KIT nor the
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
package de.uka.ipd.idaho.goldenGateServer.dcs.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.gamta.util.gPath.GPathVariableResolver;
import de.uka.ipd.idaho.gamta.util.gPath.types.GPathString;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.DcStatistics;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatField;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldGroup;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldLink;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.IoTools;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Client servlet for retrieving tabular statistics data from GoldenGATE DCS
 * instances.
 * 
 * @author sautter
 */
public abstract class GoldenGateDcsDataServlet extends GoldenGateDcsClientServlet {
	
	private void sendFields(StatFieldSet fieldSet, BufferedWriter bw, String format) throws IOException {
		if ("XML".equals(format)) {
			bw.write("<fieldSet" +
					" label=\"" + StatFieldSet.grammar.escape(fieldSet.label) + "\"" +
					">"); bw.newLine();
		}
		else if ("JSON".equals(format)) {
			bw.write("{\"label\":\"" + fieldSet.label + "\","); bw.newLine();
			bw.write("\"fieldGroups\":["); bw.newLine();
		}
		StatFieldGroup[] fieldGroups = fieldSet.getFieldGroups();
		for (int g = 0; g < fieldGroups.length; g++) {
			if ("XML".equals(format)) {
				bw.write("<fieldGroup" +
						" name=\"" + StatFieldSet.grammar.escape(fieldGroups[g].name) + "\"" +
						" label=\"" + StatFieldSet.grammar.escape(fieldGroups[g].label) + "\"" +
						">"); bw.newLine();
			}
			else if ("JSON".equals(format)) {
				bw.write("{\"name\":\"" + fieldGroups[g].name + "\","); bw.newLine();
				bw.write("\"label\":\"" + fieldGroups[g].label + "\","); bw.newLine();
				bw.write("\"fields\":["); bw.newLine();
			}
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++) {
				if ("XML".equals(format)) {
					bw.write("<field" +
							" name=\"" + StatFieldSet.grammar.escape(fields[f].name) + "\"" +
							" fullName=\"" + StatFieldSet.grammar.escape(fields[f].fullName) + "\"" +
							" label=\"" + StatFieldSet.grammar.escape(fields[f].label) + "\"" +
							" type=\"" + StatFieldSet.grammar.escape(fields[f].dataType) + "\"" +
							((fields[f].dataLength < 1) ? "" : (" length=\"" + fields[f].dataLength + "\"")) +
							" defAggregate=\"" + StatFieldSet.grammar.escape(fields[f].defAggregate) + "\"" +
							" statColName=\"" + StatFieldSet.grammar.escape(fields[f].statColName) + "\"" +
							"/>"); bw.newLine();
				}
				else if ("JSON".equals(format)) {
					bw.write("{\"name\":\"" + fields[f].name + "\","); bw.newLine();
					bw.write("\"fullName\":\"" + fields[f].fullName + "\","); bw.newLine();
					bw.write("\"label\":\"" + fields[f].label + "\","); bw.newLine();
					bw.write("\"type\":\"" + fields[f].dataType + "\","); bw.newLine();
					if (fields[f].dataLength > 0) {
						bw.write("\"length\":\"" + fields[f].dataLength + "\","); bw.newLine();
					}
					bw.write("\"defAggregate\":\"" + fields[f].defAggregate + "\","); bw.newLine();
					bw.write("\"statColName\":\"" + fields[f].statColName + "\""); bw.newLine();
					bw.write("}" + (((f+1) == fields.length) ? "" : ",")); bw.newLine();
				}
				else if ("TXT".equals(format)) {
					bw.write(fields[f].name + " (" + 
							fields[f].fullName + ", " + 
							fields[f].dataType + ((fields[f].dataLength < 1) ? "" : (" " + fields[f].dataLength)) + 
							"): " + fields[f].label + 
							" [" + fields[f].defAggregate + "]" +
							" ==> " + fields[f].statColName); bw.newLine();
				}
			}
			if ("XML".equals(format)) {
				bw.write("</fieldGroup>"); bw.newLine();
			}
			else if ("JSON".equals(format)) {
				bw.write("]"); bw.newLine();
				bw.write("}" + (((g+1) == fieldGroups.length) ? "" : ",")); bw.newLine();
			}
		}
		if ("XML".equals(format)) {
			bw.write("</fieldSet>"); bw.newLine();
		}
		else if ("JSON".equals(format)) {
			bw.write("]"); bw.newLine();
			bw.write("}"); bw.newLine();
		}
	}
	
	private void sendStats(HttpServletRequest request, BufferedWriter bw, DcStatistics stats, String format) throws IOException {
		
		//	CSV output
		if ("CSV".equals(format)) {
			stats.writeAsCSV(bw, true, request.getParameter("separator"));
			return;
		}
		
		//	get fields
		String[] statFields = stats.getFields();
		
		//	XML output
		if ("XML".equals(format)) {
			Properties statFieldsToLabels = new Properties();
			for (int f = 0; f < statFields.length; f++)
				statFieldsToLabels.setProperty(statFields[f], this.getFieldLabel(statFields[f]));
			stats.writeAsXML(bw, "statistics", "statData", null, statFieldsToLabels);
			return;
		}
		
		//	JSON output
		if ("JSON".equals(format)) {
			Properties statFieldsToLabels = new Properties();
			for (int f = 0; f < statFields.length; f++)
				statFieldsToLabels.setProperty(statFields[f], this.getFieldLabel(statFields[f]));
			stats.writeAsJSON(bw, request.getParameter("assignToVariable"), true, statFieldsToLabels, true);
			return;
		}
		
		//	JavaScript request from website
		if ("JS".equalsIgnoreCase(format)) {
			bw.write("clearStatTable();"); bw.newLine();
			bw.write("statTable.style.display = 'none';"); bw.newLine();
			this.writeStatRendererScript(bw, stats, statFields, this.getStatFieldLinks(stats, request));
			bw.write("statTable.style.display = '';"); bw.newLine();
			return;
		}
	}
	
	private void writeStatRendererScript(BufferedWriter bw, DcStatistics stats, String[] statFields, StatFieldLink[] statFieldLinks) throws IOException {
		bw.write("var sr;"); bw.newLine();
		bw.write("sr = addStatRow();"); bw.newLine();
		for (int f = 0; f < statFields.length; f++) {
			bw.write("addStatCell(sr, 'statTableHeader', '" + this.escapeForJavaScript(this.getFieldLabel(statFields[f])) + "');"); bw.newLine();
		}
		for (int t = 0; t < stats.size(); t++) {
			bw.write("sr = addStatRow();"); bw.newLine();
			StringTupel st = stats.get(t);
			for (int f = 0; f < statFields.length; f++) {
//				bw.write("addStatCell(sr, 'statTableCell', '" + this.escapeForJavaScript(st.getValue(statFields[f], "")) + "');"); bw.newLine();
				String value = st.getValue(statFields[f], "");
				String link = ((statFieldLinks[f] == null) ? null : statFieldLinks[f].getFieldLink(value, st));
				bw.write("addStatCell(sr, 'statTableCell', '" + this.escapeForJavaScript(st.getValue(statFields[f], "")) + "'" + ((link == null) ? "" : (", '" + this.escapeForJavaScript(link) + "'")) + ");"); bw.newLine();
			}
		}
	}
	
	private String escapeForJavaScript(String str) {
		StringBuffer escaped = new StringBuffer();
		char ch;
		for (int c = 0; c < str.length(); c++) {
			ch = str.charAt(c);
			if ((ch == '\\') || (ch == '\''))
				escaped.append('\\');
			if (ch < 32)
				escaped.append(' ');
			else escaped.append(ch);
		}
		return escaped.toString();
	}
	
	private StatFieldLink[] getStatFieldLinks(DcStatistics stats, HttpServletRequest request) {
		String[] statFields = stats.getFields();
		StatFieldLink[] statFieldLinks = new StatFieldLink[statFields.length];
		GPathVariableResolver query = new GPathVariableResolver();
		String groupingFieldStr = request.getParameter("groupingFields");
		if (groupingFieldStr == null)
			groupingFieldStr = request.getParameter("groupFields");
		HashSet groupingFields = new HashSet(Arrays.asList(((groupingFieldStr == null) ? "" : groupingFieldStr).split("\\s+")));
		StringBuffer fields = new StringBuffer("DocCount");
		StringBuffer valueFields = new StringBuffer();
		for (int f = 0; f < statFields.length; f++) {
			StatField sf = this.getField(statFields[f]);
			if (sf == null)
				continue;
			fields.append(" ");
			fields.append(statFields[f]);
			if (groupingFields.contains(sf.fullName)) {
				if (valueFields.length() != 0)
					valueFields.append(" ");
				valueFields.append(statFields[f]);
			}
		}
		query.setVariable("$fields", new GPathString(fields.toString()));
		query.setVariable("$valueFields", new GPathString(valueFields.toString()));
		//	TODO_not what else might we need ???
		//	TODO ==> expand this as the need arises (there _will_ be feature requests once this goes public ...)
		GPathVariableResolver sfQuery = new GPathVariableResolver(query);
		for (int f = 0; f < statFields.length; f++) {
			StatField sf = this.getField(statFields[f]);
			if (sf == null)
				continue;
			sfQuery.clear();
			String fPredicate = request.getParameter("FP-" + sf.fullName);
			sfQuery.setVariable("$fieldPredicate", new GPathString((fPredicate == null) ? "" : fPredicate));
			String aggregate = request.getParameter("FA-" + sf.fullName);
			if ((aggregate == null) && !groupingFields.contains(sf.fullName))
				aggregate = sf.defAggregate;
			sfQuery.setVariable("$aggregate", new GPathString((aggregate == null) ? "" : aggregate));
			String aPredicate = request.getParameter("AP-" + sf.fullName);
			sfQuery.setVariable("$aggregatePredicate", new GPathString((aPredicate == null) ? "" : aPredicate));
			statFieldLinks[f] = sf.getFieldLink(sfQuery);
		}
		return statFieldLinks;
	}
	
	private DcStatistics getStats(HttpServletRequest request) throws IOException {
		int limit = -1;
		StringVector outputFields = new StringVector();
		StringVector groupingFields = new StringVector();
		StringVector orderingFields = new StringVector();
		Properties fieldPredicates = new Properties();
		Properties fieldAggregates = new Properties();
		Properties aggregatePredicates = new Properties();
		StringVector customFilters = new StringVector();
		
		for (Enumeration pe = request.getParameterNames(); pe.hasMoreElements();) {
			String paramName = ((String) pe.nextElement());
			String[] paramValues = request.getParameterValues(paramName);
			if ("outputFields".equals(paramName)) {
				for (int v = 0; v < paramValues.length; v++)
					outputFields.parseAndAddElements(paramValues[v], " ");
			}
			else if ("groupFields".equals(paramName) || "groupingFields".equals(paramName)) {
				for (int v = 0; v < paramValues.length; v++)
					groupingFields.parseAndAddElements(paramValues[v], " ");
			}
			else if ("orderFields".equals(paramName) || "orderingFields".equals(paramName)) {
				for (int v = 0; v < paramValues.length; v++)
					orderingFields.parseAndAddElements(paramValues[v], " ");
			}
			else if (paramName.startsWith("FP-")) {
				String fieldName = paramName.substring("FP-".length());
				for (int v = 0; v < paramValues.length; v++)
					fieldPredicates.setProperty(fieldName, paramValues[v]);
			}
			else if (paramName.startsWith("FA-")) {
				String fieldName = paramName.substring("FA-".length());
				for (int v = 0; v < paramValues.length; v++)
					fieldAggregates.setProperty(fieldName, paramValues[v]);
			}
			else if (paramName.startsWith("AP-")) {
				String fieldName = paramName.substring("AP-".length());
				for (int v = 0; v < paramValues.length; v++)
					aggregatePredicates.setProperty(fieldName, paramValues[v]);
			}
			else if (paramName.startsWith("CF-")) {
				for (int v = 0; v < paramValues.length; v++)
					customFilters.addElementIgnoreDuplicates(paramValues[v]);
			}
			if ("limit".equals(paramName)) try {
				limit = Integer.parseInt(paramValues[0]);
			} catch (NumberFormatException nfe) {}
		}
		outputFields.removeDuplicateElements();
		groupingFields.removeDuplicateElements();
		orderingFields.removeDuplicateElements();
		
		//	check data
		if (outputFields.isEmpty())
			return null;
		
		//	get data
		return this.dcsClient.getStatistics(outputFields.toStringArray(), groupingFields.toStringArray(), orderingFields.toStringArray(), fieldPredicates, fieldAggregates, aggregatePredicates, customFilters.toStringArray(), limit, !"force".equals(request.getParameter("cacheControl")));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String pathInfo = request.getPathInfo();
		while ((pathInfo != null) && pathInfo.startsWith("/"))
			pathInfo = pathInfo.substring(1);
		
		//	check if statistics request
		if ((request.getParameter("outputFields") != null) && (request.getParameter("outputFields").trim().length() != 0)) {
			final DcStatistics stats = this.getStats(request);
			
			//	send statistics if there is any
			if (stats != null) {
				String format = request.getParameter("format");
				if ((format == null) && (pathInfo != null) && pathInfo.startsWith("stats/"))
					format = pathInfo.substring("stats/".length());
				
				//	prepare response, and sanitize format
				response.setCharacterEncoding(ENCODING);
				if ("JSON".equalsIgnoreCase(format)) {
					format = "JSON";
					response.setContentType("text/json");
				}
				else if ("XML".equalsIgnoreCase(format)) {
					format = "XML";
					response.setContentType("text/xml");
				}
				else if ("JS".equalsIgnoreCase(format)) {
					format = "JS";
					response.setContentType("text/javascript");
				}
				else if ("HTML".equalsIgnoreCase(format)) {
					final StatFieldSet fieldSet = getFieldSet();
					response.setContentType("text/html");
					this.sendHtmlPage(new HtmlPageBuilder(this, request, response) {
						protected void include(String type, String tag) throws IOException {
							if ("includeBody".equals(type))
								this.includeBody();
							else super.include(type, tag);
						}
						private void includeBody() throws IOException {
							//	TODO maybe add fields and filter value, if without modification options ...
							//	TODO ... or add parameter specifying whether or not to include fields
							this.writeLine("<table id=\"statTable\"></table>");
						}
						protected String getPageTitle(String title) {
							return ((fieldSet.label == null) ? "Document Collection Statistics" : html.escape(fieldSet.label));
						}
						protected String[] getOnloadCalls() {
							String[] olcs = {"displayStats()"};
							return olcs;
						}
						protected boolean includeJavaScriptDomHelpers() {
							return true;
						}
						protected void writePageHeadExtensions() throws IOException {
							this.writeLine("<script type=\"text/javascript\">");
							
							this.writeLine("function displayStats() {");
							writeStatRendererScript(new HtmlPageBuilderWriter(this), stats, stats.getFields(), getStatFieldLinks(stats, this.request));
							this.writeLine("}");
							this.writeLine("function addStatRow() {");
							this.writeLine("  var sr = newElement('tr', null, null, null);");
							this.writeLine("  getById('statTable').appendChild(sr);");
							this.writeLine("  return sr;");
							this.writeLine("}");
//							this.writeLine("function addStatCell(sr, cssClass, value) {"); // TODOne add link as optional fourth parameter
//							this.writeLine("  sr.appendChild(newElement('td', null, cssClass, value));");
//							this.writeLine("}");
							this.writeLine("function addStatCell(sr, cssClass, value, link) {");
							this.writeLine("  if (link) {");
							this.writeLine("    var sc = newElement('td', null, cssClass, null);");
							this.writeLine("    sr.appendChild(sc);");
							this.writeLine("    var sl = newElement('a', null, null, value);");
//							this.writeLine("    sl.href = link;");
							this.writeLine("    if (link.startsWith('return ')) {");
							this.writeLine("      sl.href = '#';");
							this.writeLine("      var linkCall = link.substring(7);");
							this.writeLine("      sl.onclick = function() { return eval(linkCall); }");
							this.writeLine("    }");
							this.writeLine("    else {");
							this.writeLine("      sl.href = link;");
							this.writeLine("      sl.target = '_blank';");
							this.writeLine("    }");
							this.writeLine("    sc.appendChild(sl);");
							this.writeLine("  }");
							this.writeLine("  else sr.appendChild(newElement('td', null, cssClass, value));");
							this.writeLine("}");
							
							this.writeLine("</script>");
						}
					});
					return;
				}
				else {
					format = "CSV";
					response.setContentType("text/csv");
				}
				
				//	deliver statistics
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), ENCODING));
				this.sendStats(request, bw, stats, format);
				bw.flush();
				
				//	we're done here
				return;
			}
		}
		
		//	request for field descriptors
		if ((pathInfo != null) && ("fields".equals(pathInfo) || pathInfo.startsWith("fields/"))) {
			String format = request.getParameter("format");
			if ((format == null) && pathInfo.startsWith("fields/"))
				format = pathInfo.substring("fields/".length());
			
			//	get fields
			StatFieldSet fieldSet = this.getFieldSet();
			
			//	prepare response, and sanitize format
			response.setCharacterEncoding(ENCODING);
			if ("JSON".equalsIgnoreCase(format)) {
				format = "JSON";
				response.setContentType("text/json");
			}
			else if ("TXT".equalsIgnoreCase(format)) {
				format = "TXT";
				response.setContentType("text/plain");
			}
			else {
				format = "XML";
				response.setContentType("text/xml");
			}
			
			//	deliver fields
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), ENCODING));
			this.sendFields(fieldSet, bw, format);
			bw.flush();
			
			//	we're done here
			return;
		}
		
		//	let super class handle that one (provide statistics HTML page)
		super.doPost(request, response);
	}
	
	private static class HtmlPageBuilderWriter extends BufferedWriter {
		private HtmlPageBuilder hpb;
		HtmlPageBuilderWriter(HtmlPageBuilder hpb) {
			super(new Writer() {
				public void write(char[] cbuf, int off, int len) throws IOException {}
				public void flush() throws IOException {}
				public void close() throws IOException {}
			});
			this.hpb = hpb;
		}
		public void write(int c) throws IOException {
			this.hpb.write("" + ((char) c));
		}
		public void write(char[] cbuf, int off, int len) throws IOException {
			this.hpb.write(new String(cbuf, off, len));
		}
		public void write(String s) throws IOException {
			this.hpb.write(s);
		}
		public void write(String s, int off, int len) throws IOException {
			this.hpb.write(s.substring(off, (off + len)));
		}
		public void newLine() throws IOException {
			this.hpb.newLine();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#getPageBuilder(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected HtmlPageBuilder getPageBuilder(HttpServletRequest request, HttpServletResponse response) throws IOException {
		final StatFieldSet fieldSet = getFieldSet();
		final StatFieldGroup[] fieldGroups = fieldSet.getFieldGroups();
		response.setCharacterEncoding(ENCODING);
		response.setContentType("text/html");
		return new HtmlPageBuilder(this, request, response) {
			protected void include(String type, String tag) throws IOException {
				if ("includeBody".equals(type))
					this.includeBody();
				else super.include(type, tag);
			}
			private void includeBody() throws IOException {
				this.writeLine("<table class=\"statFieldTable\" id=\"fieldSelectorTable\">");
				
				this.writeLine("<tr>");
				this.writeLine("<td class=\"statFieldTableHeader\">" + IoTools.prepareForHtml(fieldSet.label) + "</td>");
				this.writeLine("</tr>");
				
				for (int g = 0; g < fieldGroups.length; g++) {
					StatField[] fields = fieldGroups[g].getFields();
					
					this.writeLine("<tr>");
					this.writeLine("<td class=\"statFieldTableCell\">");
					
					this.writeLine("<table class=\"statFieldGroupTable\" id=\"fieldSelectorTable_" + fieldGroups[g].name + "\">");
					this.writeLine("<tr>");
					this.writeLine("<td class=\"statFieldGroupTableHeader\">" + IoTools.prepareForHtml(fieldGroups[g].label));
					if (fieldGroups[g].isSupplementary)
						this.writeLine("<button class=\"statFieldGroupToggle\" id=\"fieldGroupToggle_" + fieldGroups[g].name + "\" style=\"float: right;\" title=\"Expand\" onclick=\"return toggleFieldGroup('" + fieldGroups[g].name + "');\">+</button>");
					else this.writeLine("<button class=\"statFieldGroupToggle\" id=\"fieldGroupToggle_" + fieldGroups[g].name + "\" style=\"float: right;\" title=\"Collapse\" onclick=\"return toggleFieldGroup('" + fieldGroups[g].name + "');\">-</button>");
					this.writeLine("</td>");
					this.writeLine("</tr>");
					
					this.writeLine("<tr id=\"fieldGroupButtons_" + fieldGroups[g].name + "\"" + (fieldGroups[g].isSupplementary ? (" style=\"display: none;\"") : "") + ">");
					this.writeLine("<td class=\"statFieldGroupTableCell\">");
					for (int f = 0; f < fields.length; f++)
						this.writeLine("<button class=\"statFieldSelector\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_selector\" onclick=\"return toggleField('" + fieldGroups[g].name + "_" + fields[f].name + "');\">" + IoTools.prepareForHtml(fields[f].label) + "</button>");
					this.writeLine("</td>");
					this.writeLine("</tr>");
					
					this.writeLine("</table>");
					
					this.writeLine("</td>");
					this.writeLine("</tr>");
				}
				
				this.writeLine("</table>");
				
				this.writeLine("<table class=\"statFieldTable\" id=\"activeFieldTable\" style=\"display: none;\">");
				
				this.writeLine("<tr>");
				this.writeLine("<td class=\"statFieldTableHeader\" colspan=\"6\">Fields to Use in Statistics</td>");
				this.writeLine("</tr>");
				this.writeLine("<tr>");
				this.writeLine("<td class=\"statFieldOptionTableHeader\">Output?</td>");
				this.writeLine("<td class=\"statFieldOptionTableHeader\">Order? (Desc?)</td>");
				this.writeLine("<td class=\"statFieldOptionTableHeader\">Field Name</td>");
				this.writeLine("<td class=\"statFieldOptionTableHeader\">Filter on Values</td>");
				this.writeLine("<td class=\"statFieldOptionTableHeader\">Operation</td>");
				this.writeLine("<td class=\"statFieldOptionTableHeader\">Filter on Operation Result</td>");
				this.writeLine("</tr>");
				
				for (int g = 0; g < fieldGroups.length; g++) {
					StatField[] fields = fieldGroups[g].getFields();
					for (int f = 0; f < fields.length; f++) {
						this.writeLine("<tr id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_options\" class=\"selectedStatFieldOptions\">");
						
						this.write("<td class=\"statFieldOptionTableCell\">");
						this.write("<input type=\"checkbox\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_output\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_output\" value=\"output\" checked=\"true\" onchange=\"outputSelectionChanged('" + fieldGroups[g].name + "_" + fields[f].name + "');\" />");
						this.writeLine("</td>");
						
						this.write("<td class=\"statFieldOptionTableCell\">");
						this.write("<input type=\"checkbox\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_order\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_order\" value=\"order\" />");
						this.write("(<input type=\"checkbox\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_orderDesc\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_orderDesc\" value=\"true\" />)");
						this.writeLine("</td>");
						
						this.writeLine("<td class=\"statFieldOptionTableCell\">" + IoTools.prepareForHtml(fields[f].label) + "</td>");
						
						this.write("<td class=\"statFieldOptionTableCell\">");
						this.write("<input type=\"text\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fp\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fp\" />");
						this.writeLine("</td>");
						
						this.write("<td class=\"statFieldOptionTableCell\">");
						this.writeLine("<select class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa\" onchange=\"aggregateChanged('" + fieldGroups[g].name + "_" + fields[f].name + "');\">");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_group\" value=\"group\">" + "Show Individual Values" + "</option>");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_count-distinct\" value=\"count-distinct\"" + (StatField.STRING_TYPE.equals(fields[f].dataType) ? " selected=\"true\"" : "") + ">" + "Count Distinct Values" + "</option>");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_count\" value=\"count\">" + "Count All Values" + "</option>");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_min\" value=\"min\">" + "Minimum Value" + "</option>");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_max\" value=\"max\">" + "Maximum Value" + "</option>");
						if (StatField.INTEGER_TYPE.equals(fields[f].dataType) || StatField.LONG_TYPE.equals(fields[f].dataType) || StatField.REAL_TYPE.equals(fields[f].dataType)) {
							this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_sum\" value=\"sum\" selected=\"true\">" + "Sum of Values" + "</option>");
							this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_avg\" value=\"avg\">" + "Average Value" + "</option>");
						}
						this.write("</select>");
						this.writeLine("</td>");
						
						this.write("<td class=\"statFieldOptionTableCell\">");
						this.write("<input type=\"text\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_ap\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_ap\" />");
						this.writeLine("</td>");
						
						this.writeLine("</tr>");
					}
				}
				
				this.writeLine("<tr id=\"customFilterRow\" style=\"display: none;\">");
				this.writeLine("<td class=\"statFieldTableCell\" colspan=\"6\"><table id=\"customFilterTable\">");
				this.writeLine("<tr>");
				this.writeLine("<td class=\"customFilterTableHeader\">&nbsp;</td>");
				this.writeLine("<td class=\"customFilterTableHeader\">Left Field</td>");
				this.writeLine("<td class=\"customFilterTableHeader\">Comparison</td>");
				this.writeLine("<td class=\"customFilterTableHeader\">Right Field</td>");
				this.writeLine("<td class=\"customFilterTableHeader\">Apply To</td>");
				this.writeLine("</tr>");
				this.writeLine("</table></td>");
				this.writeLine("</tr>");
				
				this.writeLine("<tr id=\"statButtonRow\">");
				this.write("<td class=\"statFieldTableCell\" colspan=\"6\">");
				this.write("<button value=\"Get Statistics\" class=\"statButton\" id=\"getStatButton\" onclick=\"return updateStats();\">Get Statistics</button>");
				this.write("Maximum Rows: <input type=\"text\" class=\"selectedFieldOption\" id=\"limit\" name=\"limit\" />");
				this.write("<button value=\"Add Custom Filter\" class=\"statButton\" id=\"addCustomFilterButton\" onclick=\"return addCustomFilter();\">Add Custom Filter</button>");
				this.writeLine("</td>");
				this.writeLine("</tr>");
				
				this.writeLine("<tr id=\"statLinkRow\" style=\"display: none;\">");
				this.write("<td class=\"statFieldTableCell\" colspan=\"6\">");
				this.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_HTML\" target=\"_blank\">Get this Statistics in HTML</a>");
				this.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_CSV\" target=\"_blank\">Get this Statistics in CSV</a>");
				this.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_Excel\" target=\"_blank\">Get this Statistics for MS Excel</a>");
				this.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_JSON\" target=\"_blank\">Get this Statistics in JSON</a>");
				this.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_XML\" target=\"_blank\">Get this Statistics in XML</a>");
				this.writeLine("</td>");
				this.writeLine("</tr>");
				
				this.writeLine("</table>");
				
				this.writeLine("<table id=\"statTable\" style=\"display: none;\"></table>");
				this.writeLine("<script id=\"statJs\" type=\"text/javascript\" src=\"toCome\"></script>");
			}
			protected String getPageTitle(String title) {
				return ((fieldSet.label == null) ? "Document Collection Statistics Explorer" : html.escape(fieldSet.label + " Explorer"));
			}
			protected String[] getOnloadCalls() {
				String[] olcs = {"storeFieldOptions()"};
				return olcs;
			}
			protected boolean includeJavaScriptDomHelpers() {
				return true;
			}
			protected void writePageHeadExtensions() throws IOException {
				this.writeLine("<script type=\"text/javascript\">");
				
				/* TODO improve this JavaScript stuff:
				 * - send fields as JSON objects ...
				 * - ... with all the required properties as attributes
				 * - generate field table rows on demand (we know how to do that now) ...
				 * - ... and point from objects to DOM nodes
				 * - rewrite functions to modify objects directly ...
				 * - ... and use their pointers to DOM nodes
				 */
				
				//	list field names
				this.write("var fieldNames = new Array(");
				for (int g = 0; g < fieldGroups.length; g++) {
					StatField[] fields = fieldGroups[g].getFields();
					for (int f = 0; f < fields.length; f++) {
						this.write("'" + fieldGroups[g].name + "_" + fields[f].name + "'");
						if (((g+1) < fieldGroups.length) || ((f+1) < fields.length))
							this.write(", ");
					}
				}
				this.writeLine(");");
				
				//	store field options
				this.writeLine("var fieldOptions = new Object();");
				this.writeLine("function storeFieldOptions() {");
				this.writeLine("  for (var f = 0; f < fieldNames.length; f++) {");
				this.writeLine("    var fo = getById(fieldNames[f] + '_options');");
				this.writeLine("    fieldOptions[fieldNames[f]] = fo;");
				this.writeLine("    removeElement(fo);");
				this.writeLine("  }");
				this.writeLine("}");
				
				//	write data field names
				this.writeLine("var dataFieldNames = new Object();");
				for (int g = 0; g < fieldGroups.length; g++) {
					StatField[] fields = fieldGroups[g].getFields();
					for (int f = 0; f < fields.length; f++)
						this.writeLine("dataFieldNames['" + fieldGroups[g].name + "_" + fields[f].name + "'] = '" + fields[f].fullName + "';");
				}
				
				//	write data field labels
				this.writeLine("var dataFieldLabels = new Object();");
				for (int g = 0; g < fieldGroups.length; g++) {
					StatField[] fields = fieldGroups[g].getFields();
					for (int f = 0; f < fields.length; f++)
						this.writeLine("dataFieldLabels['" + fieldGroups[g].name + "_" + fields[f].name + "'] = '" + fields[f].label + "';");
				}
				
				//	write index of selected fields
				this.writeLine("var fieldActive = new Object();");
				for (int g = 0; g < fieldGroups.length; g++) {
					StatField[] fields = fieldGroups[g].getFields();
					for (int f = 0; f < fields.length; f++)
						this.writeLine("fieldActive['" + fieldGroups[g].name + "_" + fields[f].name + "'] = 'F';");
				}
				
				//	write default aggregates
				this.writeLine("var defaultAggregates = new Object();");
				for (int g = 0; g < fieldGroups.length; g++) {
					StatField[] fields = fieldGroups[g].getFields();
					for (int f = 0; f < fields.length; f++)
						this.writeLine("defaultAggregates['" + fieldGroups[g].name + "_" + fields[f].name + "'] = '" + ((StatField.INTEGER_TYPE.equals(fields[f].dataType) || StatField.LONG_TYPE.equals(fields[f].dataType) || StatField.REAL_TYPE.equals(fields[f].dataType)) ? "sum" : "count-distinct") + "';");
				}
				
				//	select and de-select field
				this.writeLine("var activeFieldNames = new Array();");
				this.writeLine("function toggleField(fieldName) {");
				this.writeLine("  var isActive = (fieldActive[fieldName] == 'T');");
				this.writeLine("  var fieldSelector = getById(fieldName + '_selector');");
				this.writeLine("  if (isActive) {");
				this.writeLine("    fieldActive[fieldName] = 'F';");
				this.writeLine("    fieldSelector.className = 'statFieldSelector';");
				this.writeLine("    removeElement(getById(fieldName + '_options'));");
				this.writeLine("    for (var f = 0; f < activeFieldNames.length; f++)");
				this.writeLine("      if (activeFieldNames[f] == fieldName) {");
				this.writeLine("        activeFieldNames.splice(f, 1);");
				this.writeLine("        break;");
				this.writeLine("      }");
				this.writeLine("    for (var i = 0; i < customFilterIDs.length; i++) {");
				this.writeLine("      var cfLeftField = getById(customFilterIDs[i] + '_left');");
				this.writeLine("      var cfRightField = getById(customFilterIDs[i] + '_right');");
				this.writeLine("      if ((cfLeftField.value == fieldName) || (cfRightField.value == fieldName)) {");
				this.writeLine("        removeCustomFilter(customFilterIDs[i]);");
				this.writeLine("        i--; // counter loop increment, we're removing one element here");
				this.writeLine("      }");
				this.writeLine("      else {");
				this.writeLine("        removeSelectOption(getById(customFilterIDs[i] + '_left'), fieldName);");
				this.writeLine("        removeSelectOption(getById(customFilterIDs[i] + '_right'), fieldName);");
				this.writeLine("      }");
				this.writeLine("    }");
				this.writeLine("  }");
				this.writeLine("  else {");
				this.writeLine("    fieldActive[fieldName] = 'T';");
				this.writeLine("    fieldSelector.className = 'statFieldSelector selectedStatFieldSelector';");
				this.writeLine("    var fieldOptionTable = getById('activeFieldTable');");
				this.writeLine("    fieldOptionTable.style.display = '';");
				this.writeLine("    fieldOptionTable.appendChild(fieldOptions[fieldName]);");
				this.writeLine("    activeFieldNames[activeFieldNames.length] = fieldName;");
				this.writeLine("    for (var i = 0; i < customFilterIDs.length; i++) {");
				this.writeLine("      addSelectOption(getById(customFilterIDs[i] + '_left'), fieldName, dataFieldLabels[fieldName]);");
				this.writeLine("      addSelectOption(getById(customFilterIDs[i] + '_right'), fieldName, dataFieldLabels[fieldName]);");
				this.writeLine("    }");
				this.writeLine("    var cfr = getById('customFilterRow');");
				this.writeLine("    if (cfr != null) {");
				this.writeLine("      removeElement(cfr);");
				this.writeLine("      fieldOptionTable.appendChild(cfr);");
				this.writeLine("    }");
				this.writeLine("    var sbr = getById('statButtonRow');");
				this.writeLine("    if (sbr != null) {");
				this.writeLine("      removeElement(sbr);");
				this.writeLine("      fieldOptionTable.appendChild(sbr);");
				this.writeLine("    }");
				this.writeLine("    var slr = getById('statLinkRow');");
				this.writeLine("    if (slr != null) {");
				this.writeLine("      removeElement(slr);");
				this.writeLine("      fieldOptionTable.appendChild(slr);");
				this.writeLine("    }");
				this.writeLine("  }");
				this.writeLine("}");
				
				//	add or remove field to or from output
				this.writeLine("function outputSelectionChanged(fieldName) {");
				this.writeLine("  var fieldOutputSelector = getById(fieldName + '_output');");
				this.writeLine("  var fieldOrderSelector = getById(fieldName + '_order');");
				this.writeLine("  if (fieldOutputSelector.checked)");
				this.writeLine("    fieldOrderSelector.disabled = false;");
				this.writeLine("  else {");
				this.writeLine("    fieldOrderSelector.checked = false;");
				this.writeLine("    fieldOrderSelector.disabled = true;");
				this.writeLine("    if (getById(fieldName + '_fa_group').selected)");
				this.writeLine("      getById(fieldName + '_fa_' + defaultAggregates[fieldName]).selected = true;");
				this.writeLine("  }");
				this.writeLine("}");
				
				//	change aggregate of field
				this.writeLine("function aggregateChanged(fieldName) {");
				this.writeLine("  var fieldApInput = getById(fieldName + '_ap');");
				this.writeLine("  if (getById(fieldName + '_fa_group').selected) {");
				this.writeLine("    fieldApInput.value = '';");
				this.writeLine("    fieldApInput.disabled = true;");
				this.writeLine("  }");
				this.writeLine("  else fieldApInput.disabled = false;");
				this.writeLine("  checkCustomFilters();");
				this.writeLine("}");
				
				//	add custom filter functions
				this.writeLine("var customFilterCount = 0;");
				this.writeLine("var customFilterIDs = new Array();");
				this.writeLine("function addCustomFilter() {");
				this.writeLine("  var cfId = ('customFilter' + customFilterCount++);");
				this.writeLine("  customFilterIDs[customFilterIDs.length] = cfId;");
				this.writeLine("  var cfTr = newElement('tr', (cfId + '_row'), 'customFilterRow', null);");
				this.writeLine("  var cfTd;");
				this.writeLine("  cfTd = newElement('td', null, 'customFilterRemove', null);");
				this.writeLine("  var cfRb = newElement('button', null, 'customFilterRemoveButton', 'X');");
				this.writeLine("  cfRb.title = 'Remove Custom Filter';");
				this.writeLine("  cfRb.value = 'X';");
				this.writeLine("  cfRb.style.transform = 'scale(0.67, 0.67)';");
				this.writeLine("  cfRb.style.margin = '0px';");
				this.writeLine("  cfRb.onclick = function() {");
				this.writeLine("    return removeCustomFilter(cfId);");
				this.writeLine("  };");
				this.writeLine("  cfTd.appendChild(cfRb);");
				this.writeLine("  cfTr.appendChild(cfTd);");
				this.writeLine("  cfTd = newElement('td', null, 'customFilterField', null);");
				this.writeLine("  var cfLeftField = newElement('select', (cfId + '_left'), 'customFilterFieldSelect', null);");
				this.writeLine("  fillCustomFilterFieldSelect(cfLeftField);");
				this.writeLine("  cfLeftField.onchange = function() {");
				this.writeLine("    checkCustomFilter(cfId);");
				this.writeLine("  };");
				this.writeLine("  cfTd.appendChild(cfLeftField);");
				this.writeLine("  cfTr.appendChild(cfTd);");
				this.writeLine("  cfTd = newElement('td', null, 'customFilterOperator', null);");
				this.writeLine("  var cfOperator = newElement('select', (cfId + '_operator'), 'customFilterOperationSelect', null);");
				this.writeLine("  addSelectOption(cfOperator, 'lt', 'less than');");
				this.writeLine("  addSelectOption(cfOperator, 'le', 'less than or equal to');");
				this.writeLine("  addSelectOption(cfOperator, 'eq', 'equal to');");
				this.writeLine("  addSelectOption(cfOperator, 'ne', 'not equal to');");
				this.writeLine("  addSelectOption(cfOperator, 'ge', 'greater than or equal to');");
				this.writeLine("  addSelectOption(cfOperator, 'gt', 'greater than');");
				this.writeLine("  cfTd.appendChild(cfOperator);");
				this.writeLine("  cfTr.appendChild(cfTd);");
				this.writeLine("  cfTd = newElement('td', null, 'customFilterField', null);");
				this.writeLine("  var cfRightField = newElement('select', (cfId + '_right'), 'customFilterFieldSelect', null);");
				this.writeLine("  fillCustomFilterFieldSelect(cfRightField);");
				this.writeLine("  cfRightField.onchange = function() {");
				this.writeLine("    checkCustomFilter(cfId);");
				this.writeLine("  };");
				this.writeLine("  cfTd.appendChild(cfRightField);");
				this.writeLine("  cfTr.appendChild(cfTd);");
				this.writeLine("  cfTd = newElement('td', null, 'customFilterTarget', null);");
				this.writeLine("  var cfTarget = newElement('select', (cfId + '_target'), 'customFilterTargetSelect', null);");
				this.writeLine("  addSelectOption(cfTarget, 'raw', 'Data Values');");
				this.writeLine("  addSelectOption(cfTarget, 'res', 'Operation Result');");
				this.writeLine("  cfTarget.onchange = function() {");
				this.writeLine("    checkCustomFilter(cfId);");
				this.writeLine("  };");
				this.writeLine("  cfTd.appendChild(cfTarget);");
				this.writeLine("  cfTr.appendChild(cfTd);");
				this.writeLine("  var cfTable = getById('customFilterTable');");
				this.writeLine("  cfTable.appendChild(cfTr);");
				this.writeLine("  var cfRow = getById('customFilterRow');");
				this.writeLine("  cfRow.style.display = '';");
				this.writeLine("}");
				this.writeLine("function fillCustomFilterFieldSelect(select) {");
				this.writeLine("  for (var f = 0; f < activeFieldNames.length; f++)");
				this.writeLine("    addSelectOption(select, activeFieldNames[f], dataFieldLabels[activeFieldNames[f]]);");
				this.writeLine("}");
				this.writeLine("function addSelectOption(select, value, label) {");
				this.writeLine("  var option = newElement('option', null, null, (label ? label : value));");
				this.writeLine("  option.value = value;");
				this.writeLine("  select.options[select.options.length] = option;");
				this.writeLine("}");
				this.writeLine("function removeSelectOption(select, value) {");
				this.writeLine("  for (var o = 0; o < select.options.length; o++)");
				this.writeLine("    if (select.options[o].value == value) {");
				this.writeLine("      select.options.remove(o);");
				this.writeLine("      return;");
				this.writeLine("    }");
				this.writeLine("}");
				this.writeLine("function checkCustomFilters() {");
				this.writeLine("  for (var i = 0; i < customFilterIDs.length; i++)");
				this.writeLine("    checkCustomFilter(customFilterIDs[i]);");
				this.writeLine("}");
				this.writeLine("function checkCustomFilter(cfId) {");
				this.writeLine("  var cfTr = getById(cfId + '_row');");
				this.writeLine("  if (customFilterTypeConsistent(cfId))");
				this.writeLine("    cfTr.className = 'customFilterRow';");
				this.writeLine("  else cfTr.className = 'customFilterRow customFilterError';");
				this.writeLine("}");
				this.writeLine("function customFilterTypeConsistent(cfId) {");
				this.writeLine("  var cfTarget = getById(cfId + '_target').value;");
				this.writeLine("  var cfLeftFieldName = getById(cfId + '_left').value;");
				this.writeLine("  var cfRightFieldName = getById(cfId + '_right').value;");
				this.writeLine("  var cfLeftFieldType;");
				this.writeLine("  var cfRightFieldType;");
				this.writeLine("  if (cfTarget == 'res') {");
				this.writeLine("    var cfLeftFieldAggregate = getById(cfLeftFieldName + '_fa').value;");
				this.writeLine("    if ((cfLeftFieldAggregate == 'group') || (cfLeftFieldAggregate == 'min') || (cfLeftFieldAggregate == 'max'))");
				this.writeLine("      cfLeftFieldType = ((defaultAggregates[cfLeftFieldName] == 'sum') ? 'number' : 'string');");
				this.writeLine("    else cfLeftFieldType = 'number';");
				this.writeLine("    var cfRightFieldAggregate = getById(cfRightFieldName + '_fa').value;");
				this.writeLine("    if ((cfRightFieldAggregate == 'group') || (cfRightFieldAggregate == 'min') || (cfRightFieldAggregate == 'max'))");
				this.writeLine("      cfRightFieldType = ((defaultAggregates[cfRightFieldName] == 'sum') ? 'number' : 'string');");
				this.writeLine("    else cfRightFieldType = 'number';");
				this.writeLine("  }");
				this.writeLine("  else {");
				this.writeLine("    cfLeftFieldType = ((defaultAggregates[cfLeftFieldName] == 'sum') ? 'number' : 'string');");
				this.writeLine("    cfRightFieldType = ((defaultAggregates[cfRightFieldName] == 'sum') ? 'number' : 'string');");
				this.writeLine("  }");
				this.writeLine("  return (cfLeftFieldType == cfRightFieldType);");
				this.writeLine("}");
				this.writeLine("function removeCustomFilter(cfId) {");
				this.writeLine("  var cfTr = getById(cfId + '_row');");
				this.writeLine("  if (cfTr != null)");
				this.writeLine("    removeElement(cfTr);");
				this.writeLine("  for (var i = 0; i < customFilterIDs.length; i++)");
				this.writeLine("    if (customFilterIDs[i] == cfId) {");
				this.writeLine("      customFilterIDs.splice(i, 1);");
				this.writeLine("      break;");
				this.writeLine("    }");
				this.writeLine("  if (customFilterIDs.length == 0) {");
				this.writeLine("    var cfRow = getById('customFilterRow');");
				this.writeLine("    cfRow.style.display = 'none';");
				this.writeLine("  }");
				this.writeLine("  return false;");
				this.writeLine("}");
				
				//	build statistics URL
				this.writeLine("function buildStatUrl() {");
				this.writeLine("  var limitStr = getById('limit').value;");
				this.writeLine("  var limit = ((parseInt(limitStr) > 0) ? parseInt(limitStr) : -1);");
				this.writeLine("  var outputFields = '';");
				this.writeLine("  var groupingFields = '';");
				this.writeLine("  var orderingFields = '';");
				this.writeLine("  var fieldPredicateString = '';");
				this.writeLine("  var fieldAggregateString = '';");
				this.writeLine("  var aggregatePredicateString = '';");
				this.writeLine("  for (var f = 0; f < fieldNames.length; f++) {");
				this.writeLine("    if (fieldActive[fieldNames[f]] != 'T')");
				this.writeLine("      continue;");
				this.writeLine("    if (getById(fieldNames[f] + '_output').checked)");
				this.writeLine("      outputFields = (outputFields + ((outputFields.length == 0) ? '' : '+') + dataFieldNames[fieldNames[f]]);");
				this.writeLine("    if (getById(fieldNames[f] + '_order').checked)");
				this.writeLine("      orderingFields = (orderingFields + ((orderingFields.length == 0) ? '' : '+') + (getById(fieldNames[f] + '_orderDesc').checked ? '-' : '') + dataFieldNames[fieldNames[f]]);");
				this.writeLine("    var aggregate = getById(fieldNames[f] + '_fa').value;");
				this.writeLine("    if (aggregate == 'group')");
				this.writeLine("      groupingFields = (groupingFields + ((groupingFields.length == 0) ? '' : '+') + dataFieldNames[fieldNames[f]]);");
				this.writeLine("    else if (aggregate != defaultAggregates[fieldNames[f]])");
				this.writeLine("      fieldAggregateString = (fieldAggregateString + '&FA-' + dataFieldNames[fieldNames[f]] + '=' + aggregate);");
				this.writeLine("    var fPredicate = getById(fieldNames[f] + '_fp').value;");
				this.writeLine("    if ((fPredicate != null) && (fPredicate.length != 0))");
				this.writeLine("      fieldPredicateString = (fieldPredicateString + '&FP-' + dataFieldNames[fieldNames[f]] + '=' + encodeURIComponent(fPredicate));");
				this.writeLine("    var aPredicate = getById(fieldNames[f] + '_ap').value;");
				this.writeLine("    if ((aggregate != 'group') && (aPredicate != null) && (aPredicate.length != 0))");
				this.writeLine("      aggregatePredicateString = (aggregatePredicateString + '&AP-' + dataFieldNames[fieldNames[f]] + '=' + encodeURIComponent(aPredicate));");
				this.writeLine("  }");
				this.writeLine("  if (outputFields.length == 0)");
				this.writeLine("    return null;");
				this.writeLine("  var statUrl = '" + this.request.getContextPath() + this.request.getServletPath() + "/stats?outputFields=' + outputFields;");
				this.writeLine("  if (groupingFields.length != 0)");
				this.writeLine("    statUrl = (statUrl + '&groupingFields=' + groupingFields);");
				this.writeLine("  if (orderingFields.length != 0)");
				this.writeLine("    statUrl = (statUrl + '&orderingFields=' + orderingFields);");
				this.writeLine("  if (limit != -1)");
				this.writeLine("    statUrl = (statUrl + '&limit=' + limit);");
				this.writeLine("  var customFilterString = '';");
				this.writeLine("  if (customFilterIDs.length != 0) {");
				this.writeLine("    var cfNumber = 0;");
				this.writeLine("    for (var i = 0; i < customFilterIDs.length; i++) {");
				this.writeLine("      if (!customFilterTypeConsistent(customFilterIDs[i]))");
				this.writeLine("        continue;");
				this.writeLine("      var cfTarget = getById(customFilterIDs[i] + '_target').value;");
				this.writeLine("      var cfLeftFieldName = getById(customFilterIDs[i] + '_left').value;");
				this.writeLine("      var cfOperator = getById(customFilterIDs[i] + '_operator').value;");
				this.writeLine("      var cfRightFieldName = getById(customFilterIDs[i] + '_right').value;");
				this.writeLine("      customFilterString = (customFilterString + '&CF-' + cfNumber + '=' + cfTarget + '+' + dataFieldNames[cfLeftFieldName] + '+' + cfOperator + '+' + dataFieldNames[cfRightFieldName]);");
				this.writeLine("      cfNumber++;");
				this.writeLine("    }");
				this.writeLine("  }");
				this.writeLine("  return (statUrl + fieldPredicateString + fieldAggregateString + aggregatePredicateString + customFilterString);");
				this.writeLine("}");
				
				//	update links to statistics
				this.writeLine("function updateStats() {");
				this.writeLine("  var statUrl = buildStatUrl();");
				this.writeLine("  if (statUrl == null)");
				this.writeLine("    return;");
				this.writeLine("  getById('statLink_HTML').href = (statUrl + '&format=HTML');");
				this.writeLine("  getById('statLink_CSV').href = (statUrl + '&format=CSV&separator=' + encodeURIComponent(','));");
				this.writeLine("  getById('statLink_Excel').href = (statUrl + '&format=CSV&separator=' + encodeURIComponent(';'));");
				this.writeLine("  getById('statLink_JSON').href = (statUrl + '&format=JSON');");
				this.writeLine("  getById('statLink_XML').href = (statUrl + '&format=XML');");
				this.writeLine("  getById('statLinkRow').style.display = '';");
				this.writeLine("  var statJs = getById('statJs');");
				this.writeLine("  var statJsParent = statJs.parentNode;");
				this.writeLine("  removeElement(statJs);");
				this.writeLine("  statJs = newElement('script', 'statJs', null, null);");
				this.writeLine("  setAttribute(statJs, 'type', 'text/javascript');");
				this.writeLine("  setAttribute(statJs, 'src', (statUrl + '&format=js&time=' + (new Date).getTime()));");
				this.writeLine("  statJsParent.appendChild(statJs);");
				this.writeLine("  return false;");
				this.writeLine("}");
				
				//	TODO add function for moving active fields up and down
				
				this.writeLine("var statTable = null;");
				this.writeLine("function clearStatTable() {");
				this.writeLine("  if (statTable == null)");
				this.writeLine("    statTable = getById('statTable');");
				this.writeLine("  while (statTable.firstChild != null)");
				this.writeLine("    statTable.removeChild(statTable.firstChild);");
				this.writeLine("}");
				this.writeLine("function addStatRow() {");
				this.writeLine("  var sr = newElement('tr', null, null, null);");
				this.writeLine("  getById('statTable').appendChild(sr);");
				this.writeLine("  return sr;");
				this.writeLine("}");
//				this.writeLine("function addStatCell(sr, cssClass, value) {"); // TODOne add link as optional fourth parameter
//				this.writeLine("  sr.appendChild(newElement('td', null, cssClass, value));");
//				this.writeLine("}");
				this.writeLine("function addStatCell(sr, cssClass, value, link) {");
				this.writeLine("  if (link) {");
				this.writeLine("    var sc = newElement('td', null, cssClass, null);");
				this.writeLine("    sr.appendChild(sc);");
				this.writeLine("    var sl = newElement('a', null, null, value);");
//				this.writeLine("    sl.href = link;");
				this.writeLine("    if (link.startsWith('return ')) {");
				this.writeLine("      sl.href = '#';");
				this.writeLine("      var linkCall = link.substring(7);");
				this.writeLine("      sl.onclick = function() { return eval(linkCall); }");
				this.writeLine("    }");
				this.writeLine("    else {");
				this.writeLine("      sl.href = link;");
				this.writeLine("      sl.target = '_blank';");
				this.writeLine("    }");
				this.writeLine("    sc.appendChild(sl);");
				this.writeLine("  }");
				this.writeLine("  else sr.appendChild(newElement('td', null, cssClass, value));");
				this.writeLine("}");
				
				//	write visibility state for field groups
				this.writeLine("var fieldGroupVisible = new Object();");
				for (int g = 0; g < fieldGroups.length; g++)
					this.writeLine("fieldGroupVisible['" + fieldGroups[g].name + "'] = '" + (fieldGroups[g].isSupplementary ? "F" : "T") + "';");
				
				//	write function toggling field groups
				this.writeLine("function toggleFieldGroup(fieldGroupName) {");
				this.writeLine("  var isVisible = (fieldGroupVisible[fieldGroupName] == 'T');");
				this.writeLine("  var fieldGroupButtons = getById('fieldGroupButtons_' + fieldGroupName);");
				this.writeLine("  var fieldGroupToggle = getById('fieldGroupToggle_' + fieldGroupName);");
				this.writeLine("  if (isVisible) {");
				this.writeLine("    fieldGroupVisible[fieldGroupName] = 'F';");
				this.writeLine("    fieldGroupButtons.style.display = 'none';");
				this.writeLine("    fieldGroupToggle.innerHTML = '+';");
				this.writeLine("    fieldGroupToggle.title = 'Expand';");
				this.writeLine("  }");
				this.writeLine("  else {");
				this.writeLine("    fieldGroupVisible[fieldGroupName] = 'T';");
				this.writeLine("    fieldGroupButtons.style.display = '';");
				this.writeLine("    fieldGroupToggle.innerHTML = '-';");
				this.writeLine("    fieldGroupToggle.title = 'Collapse';");
				this.writeLine("  }");
				this.writeLine("}");
				
				this.writeLine("</script>");
			}
		};
	}
	
	void writeStatFormHtml(HtmlPageBuilder hpb, boolean forDataDisplay) throws IOException {
		StatFieldSet fieldSet = this.getFieldSet();
		StatFieldGroup[] fieldGroups = fieldSet.getFieldGroups();
		hpb.writeLine("<table class=\"statFieldTable\" id=\"fieldSelectorTable\"" + (forDataDisplay ? " style=\"display: none;\"" : "") + ">");
		
		hpb.writeLine("<tr>");
		hpb.writeLine("<td class=\"statFieldTableHeader\">" + IoTools.prepareForHtml(fieldSet.label) + "</td>");
		hpb.writeLine("</tr>");
		
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			
			hpb.writeLine("<tr>");
			hpb.writeLine("<td class=\"statFieldTableCell\">");
			
			hpb.writeLine("<table class=\"statFieldGroupTable\" id=\"fieldSelectorTable_" + fieldGroups[g].name + "\">");
			hpb.writeLine("<tr>");
			hpb.writeLine("<td class=\"statFieldGroupTableHeader\">" + IoTools.prepareForHtml(fieldGroups[g].label));
			if (fieldGroups[g].isSupplementary)
				hpb.writeLine("<button class=\"statFieldGroupToggle\" id=\"fieldGroupToggle_" + fieldGroups[g].name + "\" style=\"float: right;\" title=\"Expand\" onclick=\"return toggleFieldGroup('" + fieldGroups[g].name + "');\">+</button>");
			else hpb.writeLine("<button class=\"statFieldGroupToggle\" id=\"fieldGroupToggle_" + fieldGroups[g].name + "\" style=\"float: right;\" title=\"Collapse\" onclick=\"return toggleFieldGroup('" + fieldGroups[g].name + "');\">-</button>");
			hpb.writeLine("</td>");
			hpb.writeLine("</tr>");
			
			hpb.writeLine("<tr id=\"fieldGroupButtons_" + fieldGroups[g].name + "\"" + (fieldGroups[g].isSupplementary ? (" style=\"display: none;\"") : "") + ">");
			hpb.writeLine("<td class=\"statFieldGroupTableCell\">");
			for (int f = 0; f < fields.length; f++)
				hpb.writeLine("<button class=\"statFieldSelector\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_selector\" onclick=\"return toggleField('" + fieldGroups[g].name + "_" + fields[f].name + "');\">" + IoTools.prepareForHtml(fields[f].label) + "</button>");
			hpb.writeLine("</td>");
			hpb.writeLine("</tr>");
			
			hpb.writeLine("</table>");
			
			hpb.writeLine("</td>");
			hpb.writeLine("</tr>");
		}
		
		hpb.writeLine("</table>");
		
		hpb.writeLine("<table class=\"statFieldTable\" id=\"activeFieldTable\" style=\"display: none;\">");
		
		hpb.writeLine("<tr>");
		hpb.writeLine("<td class=\"statFieldTableHeader\" colspan=\"6\">Fields to Use in Statistics</td>");
		hpb.writeLine("</tr>");
		hpb.writeLine("<tr>");
		hpb.writeLine("<td class=\"statFieldOptionTableHeader\">Output?</td>");
		hpb.writeLine("<td class=\"statFieldOptionTableHeader\">Order? (Desc?)</td>");
		hpb.writeLine("<td class=\"statFieldOptionTableHeader\">Field Name</td>");
		hpb.writeLine("<td class=\"statFieldOptionTableHeader\">Filter on Values</td>");
		hpb.writeLine("<td class=\"statFieldOptionTableHeader\">Operation</td>");
		hpb.writeLine("<td class=\"statFieldOptionTableHeader\">Filter on Operation Result</td>");
		hpb.writeLine("</tr>");
		
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++) {
				hpb.writeLine("<tr id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_options\" class=\"selectedStatFieldOptions\">");
				
				hpb.write("<td class=\"statFieldOptionTableCell\">");
				hpb.write("<input type=\"checkbox\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_output\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_output\" value=\"output\" checked=\"true\" onchange=\"outputSelectionChanged('" + fieldGroups[g].name + "_" + fields[f].name + "');\" />");
				hpb.writeLine("</td>");
				
				hpb.write("<td class=\"statFieldOptionTableCell\">");
				hpb.write("<input type=\"checkbox\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_order\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_order\" value=\"order\" />");
				hpb.write("(<input type=\"checkbox\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_orderDesc\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_orderDesc\" value=\"true\" />)");
				hpb.writeLine("</td>");
				
				hpb.writeLine("<td class=\"statFieldOptionTableCell\">" + IoTools.prepareForHtml(fields[f].label) + "</td>");
				
				hpb.write("<td class=\"statFieldOptionTableCell\">");
				hpb.write("<input type=\"text\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fp\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fp\" />");
				hpb.writeLine("</td>");
				
				hpb.write("<td class=\"statFieldOptionTableCell\">");
				hpb.writeLine("<select class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa\" onchange=\"aggregateChanged('" + fieldGroups[g].name + "_" + fields[f].name + "');\">");
				hpb.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_group\" value=\"group\">" + "Show Individual Values" + "</option>");
				hpb.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_count-distinct\" value=\"count-distinct\"" + (StatField.STRING_TYPE.equals(fields[f].dataType) ? " selected=\"true\"" : "") + ">" + "Count Distinct Values" + "</option>");
				hpb.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_count\" value=\"count\">" + "Count All Values" + "</option>");
				hpb.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_min\" value=\"min\">" + "Minimum Value" + "</option>");
				hpb.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_max\" value=\"max\">" + "Maximum Value" + "</option>");
				if (StatField.INTEGER_TYPE.equals(fields[f].dataType) || StatField.LONG_TYPE.equals(fields[f].dataType) || StatField.REAL_TYPE.equals(fields[f].dataType)) {
					hpb.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_sum\" value=\"sum\" selected=\"true\">" + "Sum of Values" + "</option>");
					hpb.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_avg\" value=\"avg\">" + "Average Value" + "</option>");
				}
				hpb.write("</select>");
				hpb.writeLine("</td>");
				
				hpb.write("<td class=\"statFieldOptionTableCell\">");
				hpb.write("<input type=\"text\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_ap\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_ap\" />");
				hpb.writeLine("</td>");
				
				hpb.writeLine("</tr>");
			}
		}
		
		hpb.writeLine("<tr id=\"customFilterRow\" style=\"display: none;\">");
		hpb.writeLine("<td class=\"statFieldTableCell\" colspan=\"6\"><table id=\"customFilterTable\">");
		hpb.writeLine("<tr>");
		hpb.writeLine("<td class=\"customFilterTableHeader\">&nbsp;</td>");
		hpb.writeLine("<td class=\"customFilterTableHeader\">Left Field</td>");
		hpb.writeLine("<td class=\"customFilterTableHeader\">Comparison</td>");
		hpb.writeLine("<td class=\"customFilterTableHeader\">Right Field</td>");
		hpb.writeLine("<td class=\"customFilterTableHeader\">Apply To</td>");
		hpb.writeLine("</tr>");
		hpb.writeLine("</table></td>");
		hpb.writeLine("</tr>");
		
		hpb.writeLine("<tr id=\"statButtonRow\">");
		hpb.write("<td class=\"statFieldTableCell\" colspan=\"6\">");
		hpb.write("<button value=\"Get Statistics\" class=\"statButton\" id=\"getStatButton\" onclick=\"return updateStats();\">Get Statistics</button>");
		hpb.write("Maximum Rows: <input type=\"text\" class=\"selectedFieldOption\" id=\"limit\" name=\"limit\" />");
		hpb.write("<button value=\"Add Custom Filter\" class=\"statButton\" id=\"addCustomFilterButton\" onclick=\"return addCustomFilter();\">Add Custom Filter</button>");
		hpb.writeLine("</td>");
		hpb.writeLine("</tr>");
		
		hpb.writeLine("<tr id=\"statLinkRow\" style=\"display: none;\">");
		hpb.write("<td class=\"statFieldTableCell\" colspan=\"6\">");
		hpb.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_HTML\" target=\"_blank\">Get this Statistics in HTML</a>");
		hpb.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_CSV\" target=\"_blank\">Get this Statistics in CSV</a>");
		hpb.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_Excel\" target=\"_blank\">Get this Statistics for MS Excel</a>");
		hpb.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_JSON\" target=\"_blank\">Get this Statistics in JSON</a>");
		hpb.write("<a href=\"toCome\" class=\"statLink\" id=\"statLink_XML\" target=\"_blank\">Get this Statistics in XML</a>");
		hpb.writeLine("</td>");
		hpb.writeLine("</tr>");
		
		hpb.writeLine("</table>");
		
		hpb.writeLine("<table id=\"statTable\"" + (forDataDisplay ? "" : "style=\"display: none;\"") + "></table>");
		hpb.writeLine("<script id=\"statJs\" type=\"text/javascript\" src=\"toCome\"></script>");
	}
	
	void writeStatFormJavaScript(HtmlPageBuilder hpb) throws IOException {
		StatFieldSet fieldSet = this.getFieldSet();
		StatFieldGroup[] fieldGroups = fieldSet.getFieldGroups();
		hpb.writeLine("<script type=\"text/javascript\">");
		
		/* TODO improve this JavaScript stuff:
		 * - send fields as JSON objects ...
		 * - ... with all the required properties as attributes
		 * - generate field table rows on demand (we know how to do that now) ...
		 * - ... and point from objects to DOM nodes
		 * - rewrite functions to modify objects directly ...
		 * - ... and use their pointers to DOM nodes
		 */
		
		//	list field names
		hpb.write("var fieldNames = new Array(");
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++) {
				hpb.write("'" + fieldGroups[g].name + "_" + fields[f].name + "'");
				if (((g+1) < fieldGroups.length) || ((f+1) < fields.length))
					hpb.write(", ");
			}
		}
		hpb.writeLine(");");
		
		//	store field options
		hpb.writeLine("var fieldOptions = new Object();");
		hpb.writeLine("function storeFieldOptions() {");
		hpb.writeLine("  for (var f = 0; f < fieldNames.length; f++) {");
		hpb.writeLine("    var fo = getById(fieldNames[f] + '_options');");
		hpb.writeLine("    fieldOptions[fieldNames[f]] = fo;");
		hpb.writeLine("    removeElement(fo);");
		hpb.writeLine("  }");
		hpb.writeLine("}");
		
		//	write data field names
		hpb.writeLine("var dataFieldNames = new Object();");
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++)
				hpb.writeLine("dataFieldNames['" + fieldGroups[g].name + "_" + fields[f].name + "'] = '" + fields[f].fullName + "';");
		}
		
		//	write data field labels
		hpb.writeLine("var dataFieldLabels = new Object();");
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++)
				hpb.writeLine("dataFieldLabels['" + fieldGroups[g].name + "_" + fields[f].name + "'] = '" + fields[f].label + "';");
		}
		
		//	write index of selected fields
		hpb.writeLine("var fieldActive = new Object();");
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++)
				hpb.writeLine("fieldActive['" + fieldGroups[g].name + "_" + fields[f].name + "'] = 'F';");
		}
		
		//	write default aggregates
		hpb.writeLine("var defaultAggregates = new Object();");
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++)
				hpb.writeLine("defaultAggregates['" + fieldGroups[g].name + "_" + fields[f].name + "'] = '" + ((StatField.INTEGER_TYPE.equals(fields[f].dataType) || StatField.LONG_TYPE.equals(fields[f].dataType) || StatField.REAL_TYPE.equals(fields[f].dataType)) ? "sum" : "count-distinct") + "';");
		}
		
		//	select and de-select field
		hpb.writeLine("var activeFieldNames = new Array();");
		hpb.writeLine("function toggleField(fieldName) {");
		hpb.writeLine("  var isActive = (fieldActive[fieldName] == 'T');");
		hpb.writeLine("  var fieldSelector = getById(fieldName + '_selector');");
		hpb.writeLine("  if (isActive) {");
		hpb.writeLine("    fieldActive[fieldName] = 'F';");
		hpb.writeLine("    fieldSelector.className = 'statFieldSelector';");
		hpb.writeLine("    removeElement(getById(fieldName + '_options'));");
		hpb.writeLine("    for (var f = 0; f < activeFieldNames.length; f++)");
		hpb.writeLine("      if (activeFieldNames[f] == fieldName) {");
		hpb.writeLine("        activeFieldNames.splice(f, 1);");
		hpb.writeLine("        break;");
		hpb.writeLine("      }");
		hpb.writeLine("    for (var i = 0; i < customFilterIDs.length; i++) {");
		hpb.writeLine("      var cfLeftField = getById(customFilterIDs[i] + '_left');");
		hpb.writeLine("      var cfRightField = getById(customFilterIDs[i] + '_right');");
		hpb.writeLine("      if ((cfLeftField.value == fieldName) || (cfRightField.value == fieldName)) {");
		hpb.writeLine("        removeCustomFilter(customFilterIDs[i]);");
		hpb.writeLine("        i--; // counter loop increment, we're removing one element here");
		hpb.writeLine("      }");
		hpb.writeLine("      else {");
		hpb.writeLine("        removeSelectOption(getById(customFilterIDs[i] + '_left'), fieldName);");
		hpb.writeLine("        removeSelectOption(getById(customFilterIDs[i] + '_right'), fieldName);");
		hpb.writeLine("      }");
		hpb.writeLine("    }");
		hpb.writeLine("  }");
		hpb.writeLine("  else {");
		hpb.writeLine("    fieldActive[fieldName] = 'T';");
		hpb.writeLine("    fieldSelector.className = 'statFieldSelector selectedStatFieldSelector';");
		hpb.writeLine("    var fieldOptionTable = getById('activeFieldTable');");
		hpb.writeLine("    fieldOptionTable.style.display = '';");
		hpb.writeLine("    fieldOptionTable.appendChild(fieldOptions[fieldName]);");
		hpb.writeLine("    activeFieldNames[activeFieldNames.length] = fieldName;");
		hpb.writeLine("    for (var i = 0; i < customFilterIDs.length; i++) {");
		hpb.writeLine("      addSelectOption(getById(customFilterIDs[i] + '_left'), fieldName, dataFieldLabels[fieldName]);");
		hpb.writeLine("      addSelectOption(getById(customFilterIDs[i] + '_right'), fieldName, dataFieldLabels[fieldName]);");
		hpb.writeLine("    }");
		hpb.writeLine("    var cfr = getById('customFilterRow');");
		hpb.writeLine("    if (cfr != null) {");
		hpb.writeLine("      removeElement(cfr);");
		hpb.writeLine("      fieldOptionTable.appendChild(cfr);");
		hpb.writeLine("    }");
		hpb.writeLine("    var sbr = getById('statButtonRow');");
		hpb.writeLine("    if (sbr != null) {");
		hpb.writeLine("      removeElement(sbr);");
		hpb.writeLine("      fieldOptionTable.appendChild(sbr);");
		hpb.writeLine("    }");
		hpb.writeLine("    var slr = getById('statLinkRow');");
		hpb.writeLine("    if (slr != null) {");
		hpb.writeLine("      removeElement(slr);");
		hpb.writeLine("      fieldOptionTable.appendChild(slr);");
		hpb.writeLine("    }");
		hpb.writeLine("  }");
		hpb.writeLine("}");
		
		//	add or remove field to or from output
		hpb.writeLine("function outputSelectionChanged(fieldName) {");
		hpb.writeLine("  var fieldOutputSelector = getById(fieldName + '_output');");
		hpb.writeLine("  var fieldOrderSelector = getById(fieldName + '_order');");
		hpb.writeLine("  if (fieldOutputSelector.checked)");
		hpb.writeLine("    fieldOrderSelector.disabled = false;");
		hpb.writeLine("  else {");
		hpb.writeLine("    fieldOrderSelector.checked = false;");
		hpb.writeLine("    fieldOrderSelector.disabled = true;");
		hpb.writeLine("    if (getById(fieldName + '_fa_group').selected)");
		hpb.writeLine("      getById(fieldName + '_fa_' + defaultAggregates[fieldName]).selected = true;");
		hpb.writeLine("  }");
		hpb.writeLine("}");
		
		//	change aggregate of field
		hpb.writeLine("function aggregateChanged(fieldName) {");
		hpb.writeLine("  var fieldApInput = getById(fieldName + '_ap');");
		hpb.writeLine("  if (getById(fieldName + '_fa_group').selected) {");
		hpb.writeLine("    fieldApInput.value = '';");
		hpb.writeLine("    fieldApInput.disabled = true;");
		hpb.writeLine("  }");
		hpb.writeLine("  else fieldApInput.disabled = false;");
		hpb.writeLine("  checkCustomFilters();");
		hpb.writeLine("}");
		
		//	add custom filter functions
		hpb.writeLine("var customFilterCount = 0;");
		hpb.writeLine("var customFilterIDs = new Array();");
		hpb.writeLine("function addCustomFilter() {");
		hpb.writeLine("  var cfId = ('customFilter' + customFilterCount++);");
		hpb.writeLine("  customFilterIDs[customFilterIDs.length] = cfId;");
		hpb.writeLine("  var cfTr = newElement('tr', (cfId + '_row'), 'customFilterRow', null);");
		hpb.writeLine("  var cfTd;");
		hpb.writeLine("  cfTd = newElement('td', null, 'customFilterRemove', null);");
		hpb.writeLine("  var cfRb = newElement('button', null, 'customFilterRemoveButton', 'X');");
		hpb.writeLine("  cfRb.title = 'Remove Custom Filter';");
		hpb.writeLine("  cfRb.value = 'X';");
		hpb.writeLine("  cfRb.style.transform = 'scale(0.67, 0.67)';");
		hpb.writeLine("  cfRb.style.margin = '0px';");
		hpb.writeLine("  cfRb.onclick = function() {");
		hpb.writeLine("    return removeCustomFilter(cfId);");
		hpb.writeLine("  };");
		hpb.writeLine("  cfTd.appendChild(cfRb);");
		hpb.writeLine("  cfTr.appendChild(cfTd);");
		hpb.writeLine("  cfTd = newElement('td', null, 'customFilterField', null);");
		hpb.writeLine("  var cfLeftField = newElement('select', (cfId + '_left'), 'customFilterFieldSelect', null);");
		hpb.writeLine("  fillCustomFilterFieldSelect(cfLeftField);");
		hpb.writeLine("  cfLeftField.onchange = function() {");
		hpb.writeLine("    checkCustomFilter(cfId);");
		hpb.writeLine("  };");
		hpb.writeLine("  cfTd.appendChild(cfLeftField);");
		hpb.writeLine("  cfTr.appendChild(cfTd);");
		hpb.writeLine("  cfTd = newElement('td', null, 'customFilterOperator', null);");
		hpb.writeLine("  var cfOperator = newElement('select', (cfId + '_operator'), 'customFilterOperationSelect', null);");
		hpb.writeLine("  addSelectOption(cfOperator, 'lt', 'less than');");
		hpb.writeLine("  addSelectOption(cfOperator, 'le', 'less than or equal to');");
		hpb.writeLine("  addSelectOption(cfOperator, 'eq', 'equal to');");
		hpb.writeLine("  addSelectOption(cfOperator, 'ne', 'not equal to');");
		hpb.writeLine("  addSelectOption(cfOperator, 'ge', 'greater than or equal to');");
		hpb.writeLine("  addSelectOption(cfOperator, 'gt', 'greater than');");
		hpb.writeLine("  cfTd.appendChild(cfOperator);");
		hpb.writeLine("  cfTr.appendChild(cfTd);");
		hpb.writeLine("  cfTd = newElement('td', null, 'customFilterField', null);");
		hpb.writeLine("  var cfRightField = newElement('select', (cfId + '_right'), 'customFilterFieldSelect', null);");
		hpb.writeLine("  fillCustomFilterFieldSelect(cfRightField);");
		hpb.writeLine("  cfRightField.onchange = function() {");
		hpb.writeLine("    checkCustomFilter(cfId);");
		hpb.writeLine("  };");
		hpb.writeLine("  cfTd.appendChild(cfRightField);");
		hpb.writeLine("  cfTr.appendChild(cfTd);");
		hpb.writeLine("  cfTd = newElement('td', null, 'customFilterTarget', null);");
		hpb.writeLine("  var cfTarget = newElement('select', (cfId + '_target'), 'customFilterTargetSelect', null);");
		hpb.writeLine("  addSelectOption(cfTarget, 'raw', 'Data Values');");
		hpb.writeLine("  addSelectOption(cfTarget, 'res', 'Operation Result');");
		hpb.writeLine("  cfTarget.onchange = function() {");
		hpb.writeLine("    checkCustomFilter(cfId);");
		hpb.writeLine("  };");
		hpb.writeLine("  cfTd.appendChild(cfTarget);");
		hpb.writeLine("  cfTr.appendChild(cfTd);");
		hpb.writeLine("  var cfTable = getById('customFilterTable');");
		hpb.writeLine("  cfTable.appendChild(cfTr);");
		hpb.writeLine("  var cfRow = getById('customFilterRow');");
		hpb.writeLine("  cfRow.style.display = '';");
		hpb.writeLine("}");
		hpb.writeLine("function fillCustomFilterFieldSelect(select) {");
		hpb.writeLine("  for (var f = 0; f < activeFieldNames.length; f++)");
		hpb.writeLine("    addSelectOption(select, activeFieldNames[f], dataFieldLabels[activeFieldNames[f]]);");
		hpb.writeLine("}");
		hpb.writeLine("function addSelectOption(select, value, label) {");
		hpb.writeLine("  var option = newElement('option', null, null, (label ? label : value));");
		hpb.writeLine("  option.value = value;");
		hpb.writeLine("  select.options[select.options.length] = option;");
		hpb.writeLine("}");
		hpb.writeLine("function removeSelectOption(select, value) {");
		hpb.writeLine("  for (var o = 0; o < select.options.length; o++)");
		hpb.writeLine("    if (select.options[o].value == value) {");
		hpb.writeLine("      select.options.remove(o);");
		hpb.writeLine("      return;");
		hpb.writeLine("    }");
		hpb.writeLine("}");
		hpb.writeLine("function checkCustomFilters() {");
		hpb.writeLine("  for (var i = 0; i < customFilterIDs.length; i++)");
		hpb.writeLine("    checkCustomFilter(customFilterIDs[i]);");
		hpb.writeLine("}");
		hpb.writeLine("function checkCustomFilter(cfId) {");
		hpb.writeLine("  var cfTr = getById(cfId + '_row');");
		hpb.writeLine("  if (customFilterTypeConsistent(cfId))");
		hpb.writeLine("    cfTr.className = 'customFilterRow';");
		hpb.writeLine("  else cfTr.className = 'customFilterRow customFilterError';");
		hpb.writeLine("}");
		hpb.writeLine("function customFilterTypeConsistent(cfId) {");
		hpb.writeLine("  var cfTarget = getById(cfId + '_target').value;");
		hpb.writeLine("  var cfLeftFieldName = getById(cfId + '_left').value;");
		hpb.writeLine("  var cfRightFieldName = getById(cfId + '_right').value;");
		hpb.writeLine("  var cfLeftFieldType;");
		hpb.writeLine("  var cfRightFieldType;");
		hpb.writeLine("  if (cfTarget == 'res') {");
		hpb.writeLine("    var cfLeftFieldAggregate = getById(cfLeftFieldName + '_fa').value;");
		hpb.writeLine("    if ((cfLeftFieldAggregate == 'group') || (cfLeftFieldAggregate == 'min') || (cfLeftFieldAggregate == 'max'))");
		hpb.writeLine("      cfLeftFieldType = ((defaultAggregates[cfLeftFieldName] == 'sum') ? 'number' : 'string');");
		hpb.writeLine("    else cfLeftFieldType = 'number';");
		hpb.writeLine("    var cfRightFieldAggregate = getById(cfRightFieldName + '_fa').value;");
		hpb.writeLine("    if ((cfRightFieldAggregate == 'group') || (cfRightFieldAggregate == 'min') || (cfRightFieldAggregate == 'max'))");
		hpb.writeLine("      cfRightFieldType = ((defaultAggregates[cfRightFieldName] == 'sum') ? 'number' : 'string');");
		hpb.writeLine("    else cfRightFieldType = 'number';");
		hpb.writeLine("  }");
		hpb.writeLine("  else {");
		hpb.writeLine("    cfLeftFieldType = ((defaultAggregates[cfLeftFieldName] == 'sum') ? 'number' : 'string');");
		hpb.writeLine("    cfRightFieldType = ((defaultAggregates[cfRightFieldName] == 'sum') ? 'number' : 'string');");
		hpb.writeLine("  }");
		hpb.writeLine("  return (cfLeftFieldType == cfRightFieldType);");
		hpb.writeLine("}");
		hpb.writeLine("function removeCustomFilter(cfId) {");
		hpb.writeLine("  var cfTr = getById(cfId + '_row');");
		hpb.writeLine("  if (cfTr != null)");
		hpb.writeLine("    removeElement(cfTr);");
		hpb.writeLine("  for (var i = 0; i < customFilterIDs.length; i++)");
		hpb.writeLine("    if (customFilterIDs[i] == cfId) {");
		hpb.writeLine("      customFilterIDs.splice(i, 1);");
		hpb.writeLine("      break;");
		hpb.writeLine("    }");
		hpb.writeLine("  if (customFilterIDs.length == 0) {");
		hpb.writeLine("    var cfRow = getById('customFilterRow');");
		hpb.writeLine("    cfRow.style.display = 'none';");
		hpb.writeLine("  }");
		hpb.writeLine("  return false;");
		hpb.writeLine("}");
		
		//	build statistics URL
		hpb.writeLine("function buildStatUrl() {");
		hpb.writeLine("  var limitStr = getById('limit').value;");
		hpb.writeLine("  var limit = ((parseInt(limitStr) > 0) ? parseInt(limitStr) : -1);");
		hpb.writeLine("  var outputFields = '';");
		hpb.writeLine("  var groupingFields = '';");
		hpb.writeLine("  var orderingFields = '';");
		hpb.writeLine("  var fieldPredicateString = '';");
		hpb.writeLine("  var fieldAggregateString = '';");
		hpb.writeLine("  var aggregatePredicateString = '';");
		hpb.writeLine("  for (var f = 0; f < fieldNames.length; f++) {");
		hpb.writeLine("    if (fieldActive[fieldNames[f]] != 'T')");
		hpb.writeLine("      continue;");
		hpb.writeLine("    if (getById(fieldNames[f] + '_output').checked)");
		hpb.writeLine("      outputFields = (outputFields + ((outputFields.length == 0) ? '' : '+') + dataFieldNames[fieldNames[f]]);");
		hpb.writeLine("    if (getById(fieldNames[f] + '_order').checked)");
		hpb.writeLine("      orderingFields = (orderingFields + ((orderingFields.length == 0) ? '' : '+') + (getById(fieldNames[f] + '_orderDesc').checked ? '-' : '') + dataFieldNames[fieldNames[f]]);");
		hpb.writeLine("    var aggregate = getById(fieldNames[f] + '_fa').value;");
		hpb.writeLine("    if (aggregate == 'group')");
		hpb.writeLine("      groupingFields = (groupingFields + ((groupingFields.length == 0) ? '' : '+') + dataFieldNames[fieldNames[f]]);");
		hpb.writeLine("    else if (aggregate != defaultAggregates[fieldNames[f]])");
		hpb.writeLine("      fieldAggregateString = (fieldAggregateString + '&FA-' + dataFieldNames[fieldNames[f]] + '=' + aggregate);");
		hpb.writeLine("    var fPredicate = getById(fieldNames[f] + '_fp').value;");
		hpb.writeLine("    if ((fPredicate != null) && (fPredicate.length != 0))");
		hpb.writeLine("      fieldPredicateString = (fieldPredicateString + '&FP-' + dataFieldNames[fieldNames[f]] + '=' + encodeURIComponent(fPredicate));");
		hpb.writeLine("    var aPredicate = getById(fieldNames[f] + '_ap').value;");
		hpb.writeLine("    if ((aggregate != 'group') && (aPredicate != null) && (aPredicate.length != 0))");
		hpb.writeLine("      aggregatePredicateString = (aggregatePredicateString + '&AP-' + dataFieldNames[fieldNames[f]] + '=' + encodeURIComponent(aPredicate));");
		hpb.writeLine("  }");
		hpb.writeLine("  if (outputFields.length == 0)");
		hpb.writeLine("    return null;");
		hpb.writeLine("  var statUrl = '" + hpb.request.getContextPath() + hpb.request.getServletPath() + "/stats?outputFields=' + outputFields;");
		hpb.writeLine("  if (groupingFields.length != 0)");
		hpb.writeLine("    statUrl = (statUrl + '&groupingFields=' + groupingFields);");
		hpb.writeLine("  if (orderingFields.length != 0)");
		hpb.writeLine("    statUrl = (statUrl + '&orderingFields=' + orderingFields);");
		hpb.writeLine("  if (limit != -1)");
		hpb.writeLine("    statUrl = (statUrl + '&limit=' + limit);");
		hpb.writeLine("  var customFilterString = '';");
		hpb.writeLine("  if (customFilterIDs.length != 0) {");
		hpb.writeLine("    var cfNumber = 0;");
		hpb.writeLine("    for (var i = 0; i < customFilterIDs.length; i++) {");
		hpb.writeLine("      if (!customFilterTypeConsistent(customFilterIDs[i]))");
		hpb.writeLine("        continue;");
		hpb.writeLine("      var cfTarget = getById(customFilterIDs[i] + '_target').value;");
		hpb.writeLine("      var cfLeftFieldName = getById(customFilterIDs[i] + '_left').value;");
		hpb.writeLine("      var cfOperator = getById(customFilterIDs[i] + '_operator').value;");
		hpb.writeLine("      var cfRightFieldName = getById(customFilterIDs[i] + '_right').value;");
		hpb.writeLine("      customFilterString = (customFilterString + '&CF-' + cfNumber + '=' + cfTarget + '+' + dataFieldNames[cfLeftFieldName] + '+' + cfOperator + '+' + dataFieldNames[cfRightFieldName]);");
		hpb.writeLine("      cfNumber++;");
		hpb.writeLine("    }");
		hpb.writeLine("  }");
		hpb.writeLine("  return (statUrl + fieldPredicateString + fieldAggregateString + aggregatePredicateString + customFilterString);");
		hpb.writeLine("}");
		
		//	update links to statistics
		hpb.writeLine("function updateStats() {");
		hpb.writeLine("  var statUrl = buildStatUrl();");
		hpb.writeLine("  if (statUrl == null)");
		hpb.writeLine("    return;");
		hpb.writeLine("  getById('statLink_HTML').href = (statUrl + '&format=HTML');");
		hpb.writeLine("  getById('statLink_CSV').href = (statUrl + '&format=CSV&separator=' + encodeURIComponent(','));");
		hpb.writeLine("  getById('statLink_Excel').href = (statUrl + '&format=CSV&separator=' + encodeURIComponent(';'));");
		hpb.writeLine("  getById('statLink_JSON').href = (statUrl + '&format=JSON');");
		hpb.writeLine("  getById('statLink_XML').href = (statUrl + '&format=XML');");
		hpb.writeLine("  getById('statLinkRow').style.display = '';");
		hpb.writeLine("  var statJs = getById('statJs');");
		hpb.writeLine("  var statJsParent = statJs.parentNode;");
		hpb.writeLine("  removeElement(statJs);");
		hpb.writeLine("  statJs = newElement('script', 'statJs', null, null);");
		hpb.writeLine("  setAttribute(statJs, 'type', 'text/javascript');");
		hpb.writeLine("  setAttribute(statJs, 'src', (statUrl + '&format=js&time=' + (new Date).getTime()));");
		hpb.writeLine("  statJsParent.appendChild(statJs);");
		hpb.writeLine("  return false;");
		hpb.writeLine("}");
		
		//	TODO add function for moving active fields up and down
		
		hpb.writeLine("var statTable = null;");
		hpb.writeLine("function clearStatTable() {");
		hpb.writeLine("  if (statTable == null)");
		hpb.writeLine("    statTable = getById('statTable');");
		hpb.writeLine("  while (statTable.firstChild != null)");
		hpb.writeLine("    statTable.removeChild(statTable.firstChild);");
		hpb.writeLine("}");
		hpb.writeLine("function addStatRow() {");
		hpb.writeLine("  var sr = newElement('tr', null, null, null);");
		hpb.writeLine("  getById('statTable').appendChild(sr);");
		hpb.writeLine("  return sr;");
		hpb.writeLine("}");
//		hpb.writeLine("function addStatCell(sr, cssClass, value) {"); // TODOne add link as optional fourth parameter
//		hpb.writeLine("  sr.appendChild(newElement('td', null, cssClass, value));");
//		hpb.writeLine("}");
		hpb.writeLine("function addStatCell(sr, cssClass, value, link) {");
		hpb.writeLine("  if (link) {");
		hpb.writeLine("    var sc = newElement('td', null, cssClass, null);");
		hpb.writeLine("    sr.appendChild(sc);");
		hpb.writeLine("    var sl = newElement('a', null, null, value);");
//		hpb.writeLine("    sl.href = link;");
		hpb.writeLine("    if (link.startsWith('return ')) {");
		hpb.writeLine("      sl.href = '#';");
		hpb.writeLine("      var linkCall = link.substring(7);");
		hpb.writeLine("      sl.onclick = function() { return eval(linkCall); }");
		hpb.writeLine("    }");
		hpb.writeLine("    else {");
		hpb.writeLine("      sl.href = link;");
		hpb.writeLine("      sl.target = '_blank';");
		hpb.writeLine("    }");
		hpb.writeLine("    sc.appendChild(sl);");
		hpb.writeLine("  }");
		hpb.writeLine("  else sr.appendChild(newElement('td', null, cssClass, value));");
		hpb.writeLine("}");
		
		//	write visibility state for field groups
		hpb.writeLine("var fieldGroupVisible = new Object();");
		for (int g = 0; g < fieldGroups.length; g++)
			hpb.writeLine("fieldGroupVisible['" + fieldGroups[g].name + "'] = '" + (fieldGroups[g].isSupplementary ? "F" : "T") + "';");
		
		//	write function toggling field groups
		hpb.writeLine("function toggleFieldGroup(fieldGroupName) {");
		hpb.writeLine("  var isVisible = (fieldGroupVisible[fieldGroupName] == 'T');");
		hpb.writeLine("  var fieldGroupButtons = getById('fieldGroupButtons_' + fieldGroupName);");
		hpb.writeLine("  var fieldGroupToggle = getById('fieldGroupToggle_' + fieldGroupName);");
		hpb.writeLine("  if (isVisible) {");
		hpb.writeLine("    fieldGroupVisible[fieldGroupName] = 'F';");
		hpb.writeLine("    fieldGroupButtons.style.display = 'none';");
		hpb.writeLine("    fieldGroupToggle.innerHTML = '+';");
		hpb.writeLine("    fieldGroupToggle.title = 'Expand';");
		hpb.writeLine("  }");
		hpb.writeLine("  else {");
		hpb.writeLine("    fieldGroupVisible[fieldGroupName] = 'T';");
		hpb.writeLine("    fieldGroupButtons.style.display = '';");
		hpb.writeLine("    fieldGroupToggle.innerHTML = '-';");
		hpb.writeLine("    fieldGroupToggle.title = 'Collapse';");
		hpb.writeLine("  }");
		hpb.writeLine("}");
		
		hpb.writeLine("</script>");
	}
	
	public static void main(String[] args) throws Exception {
		final GoldenGateDcsClient dcsc = new GoldenGateDcsClient(ServerConnection.getServerConnection("http://plazi.cs.umb.edu/GgServer/proxy"), "TCS") {};
		GoldenGateDcsDataServlet dcss = new GoldenGateDcsDataServlet() {
			protected GoldenGateDcsClient getDcsClient() {return dcsc;}
		};
		
		//	run search test
		String[] outputFields = {"tax.familyEpithet", "tax.genusEpithet", "tax.name"};
		String[] groupingFields = {"tax.familyEpithet"};
		String[] orderingFields = {"tax.name"};
		Properties fieldPredicates = new Properties();
		fieldPredicates.setProperty("bib.year", "2010-2011");
		Properties fieldAggregates = new Properties();
//		fieldAggregates.setProperty("tax.name", "count");
		Properties aggregatePredicates = new Properties();
		aggregatePredicates.setProperty("tax.genusEpithet", "5-");
//		aggregatePredicates.setProperty("matCit.specimenCount", "1-");
		String[] customFilters = {"raw+tax.genusEpithet+le+tax.familyEpithet"};
		DcStatistics stats = dcsc.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, customFilters);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out, ENCODING));
		dcss.sendStats(null, bw, stats, "JSON");
		bw.flush();
//		StatFieldSet fieldSet = StatFieldSet.readFieldSet(new BufferedReader(new InputStreamReader(new FileInputStream(new File("E:/Projektdaten/GgDcsTestFields.xml")))));
	}
}