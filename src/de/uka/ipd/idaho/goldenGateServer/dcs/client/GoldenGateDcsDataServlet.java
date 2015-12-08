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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.DcStatistics;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatField;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldGroup;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.IoTools;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
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
			String separator = request.getParameter("separator");
			if ((separator == null) || (separator.length() != 1))
				separator = ",";
			StringRelation.writeCsvData(bw, stats, separator.charAt(0), '"');
			return;
		}
		
		//	get fields
		String[] statFields = stats.getFields();
		
		//	XML output
		if ("XML".equals(format)) {
			bw.write("<statistics");
			for (int f = 0; f < statFields.length; f++)
				bw.write(" " + statFields[f] + "=\"" + StatFieldSet.grammar.escape(this.getFieldLabel(statFields[f])) + "\"");
			bw.write(">"); bw.newLine();
			for (int t = 0; t < stats.size(); t++) {
				StringTupel st = stats.get(t);
				bw.write("<statData");
				for (int f = 0; f < statFields.length; f++)
					bw.write(" " + statFields[f] + "=\"" + StatFieldSet.grammar.escape(st.getValue(statFields[f], "")) + "\"");
				bw.write("/>"); bw.newLine();
			}
			bw.write("</statistics>"); bw.newLine();
			return;
		}
		
		//	JSON output
		if ("JSON".equals(format)) {
			bw.write("{\"fields\": [");
			for (int f = 0; f < statFields.length; f++)
				bw.write(((f == 0) ? "" : ", ") + "\"" + statFields[f] + "\"");
			bw.write("],"); bw.newLine();
			bw.write("\"labels\": {"); bw.newLine();
			for (int f = 0; f < statFields.length; f++) {
				bw.write("\"" + statFields[f] + "\": \"" + this.getFieldLabel(statFields[f]) + "\"" + (((f+1) < statFields.length) ? "," : "")); bw.newLine();
			}
			bw.write("},"); bw.newLine();
			bw.write("\"data\": ["); bw.newLine();
			for (int t = 0; t < stats.size(); t++) {
				StringTupel st = stats.get(t);
				bw.write("{"); bw.newLine();
				for (int f = 0; f < statFields.length; f++) {
					bw.write("\"" + statFields[f] + "\": \"" + st.getValue(statFields[f], "") + "\"" + (((f+1) < statFields.length) ? "," : "")); bw.newLine();
				}
				bw.write("}" + (((t+1) < stats.size()) ? "," : "")); bw.newLine();
			}
			bw.write("]"); bw.newLine();
			bw.write("}"); bw.newLine();
			return;
		}
		
		//	JavaScript request from website
		if ("JS".equalsIgnoreCase(format)) {
			bw.write("clearStatTable();"); bw.newLine();
			bw.write("statTable.style.display = 'none';"); bw.newLine();
			bw.write("var sr;"); bw.newLine();
			bw.write("sr = addStatRow();"); bw.newLine();
			for (int f = 0; f < statFields.length; f++) {
				bw.write("addStatCell(sr, 'statTableHeader', '" + this.escapeForJavaScript(this.getFieldLabel(statFields[f])) + "');"); bw.newLine();
			}
			for (int t = 0; t < stats.size(); t++) {
				bw.write("sr = addStatRow();"); bw.newLine();
				StringTupel st = stats.get(t);
				for (int f = 0; f < statFields.length; f++) {
					bw.write("addStatCell(sr, 'statTableCell', '" + this.escapeForJavaScript(st.getValue(statFields[f], "")) + "');"); bw.newLine();
				}
			}
			bw.write("statTable.style.display = '';"); bw.newLine();
			return;
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
	
	private DcStatistics getStats(HttpServletRequest request) throws IOException {
		int limit = -1;
		StringVector outputFields = new StringVector();
		StringVector groupingFields = new StringVector();
		StringVector orderingFields = new StringVector();
		Properties fieldPredicates = new Properties();
		Properties fieldAggregates = new Properties();
		Properties aggregatePredicates = new Properties();
		
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
		return this.dcsClient.getStatistics(outputFields.toStringArray(), groupingFields.toStringArray(), orderingFields.toStringArray(), fieldPredicates, fieldAggregates, aggregatePredicates, limit, !"force".equals(request.getParameter("cacheControl")));
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
			DcStatistics stats = this.getStats(request);
			
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
					
					this.writeLine("<table class=\"statFieldTable\" id=\"fieldSelectorTable_" + fieldGroups[g].name + "\">");
					this.writeLine("<tr>");
					this.writeLine("<td class=\"statFieldTableHeader\">" + IoTools.prepareForHtml(fieldGroups[g].label) + "</td>");
					this.writeLine("</tr>");
					
					this.writeLine("<tr>");
					this.writeLine("<td class=\"statFieldTableCell\">");
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
				this.writeLine("<td class=\"statFieldTableHeader\">Output?</td>");
				this.writeLine("<td class=\"statFieldTableHeader\">Order? (Desc?)</td>");
				this.writeLine("<td class=\"statFieldTableHeader\">Field Name</td>");
				this.writeLine("<td class=\"statFieldTableHeader\">Filter on Values</td>");
				this.writeLine("<td class=\"statFieldTableHeader\">Operation</td>");
				this.writeLine("<td class=\"statFieldTableHeader\">Filter on Operation Result</td>");
				this.writeLine("</tr>");
				
				for (int g = 0; g < fieldGroups.length; g++) {
					StatField[] fields = fieldGroups[g].getFields();
					for (int f = 0; f < fields.length; f++) {
						this.writeLine("<tr id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_options\" class=\"selectedStatFieldOptions\">");
						
						this.write("<td class=\"statFieldOptionTableCell\">");
						this.write("<input type=\"checkbox\" class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_output\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_output\" value=\"output\" checked=\"true\" onchange=\"outputSelectionChanged('" + fieldGroups[g].name + "_" + fields[f].name + "')\" />");
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
						this.writeLine("<select class=\"selectedFieldOption\" id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa\" name=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa\" onchange=\"aggregateChanged('" + fieldGroups[g].name + "_" + fields[f].name + "')\">");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_group\" value=\"group\">" + "Show Individual Values" + "</option>");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_count-distinct\" value=\"count-distinct\"" + (StatField.STRING_TYPE.equals(fields[f].dataType) ? " selected=\"true\"" : "") + ">" + "Count Distinct Values" + "</option>");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_count\" value=\"count\">" + "Count All Values" + "</option>");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_min\" value=\"min\">" + "Minimum Value" + "</option>");
						this.writeLine("<option id=\"" + fieldGroups[g].name + "_" + fields[f].name + "_fa_max\" value=\"max\">" + "Maximum Value" + "</option>");
						if (StatField.INTEGER_TYPE.equals(fields[f].dataType) || StatField.REAL_TYPE.equals(fields[f].dataType)) {
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
				
				this.writeLine("<tr id=\"statButtonRow\">");
				this.write("<td class=\"statFieldTableCell\" colspan=\"6\">");
				this.write("<button value=\"Get Statistics\" class=\"statButton\" id=\"getStatButton\" onclick=\"return updateStats();\">Get Statistics</button>");
				this.write("Maximum Rows: <input type=\"text\" class=\"selectedFieldOption\" id=\"limit\" name=\"limit\" />");
				this.writeLine("</td>");
				this.writeLine("</tr>");
				
				this.writeLine("<tr id=\"statLinkRow\" style=\"display: none;\">");
				this.write("<td class=\"statFieldTableCell\" colspan=\"6\">");
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
						this.writeLine("defaultAggregates['" + fieldGroups[g].name + "_" + fields[f].name + "'] = '" + ((StatField.INTEGER_TYPE.equals(fields[f].dataType) || StatField.REAL_TYPE.equals(fields[f].dataType)) ? "sum" : "count-distinct") + "';");
				}
				
				//	select and de-select field
				this.writeLine("function toggleField(fieldName) {");
				this.writeLine("  var isActive = (fieldActive[fieldName] == 'T');");
				this.writeLine("  var fieldSelector = getById(fieldName + '_selector');");
				this.writeLine("  if (isActive) {");
				this.writeLine("    fieldActive[fieldName] = 'F';");
				this.writeLine("    fieldSelector.className = 'statFieldSelector';");
				this.writeLine("    removeElement(getById(fieldName + '_options'));");
				this.writeLine("  }");
				this.writeLine("  else {");
				this.writeLine("    fieldActive[fieldName] = 'T';");
				this.writeLine("    fieldSelector.className = 'statFieldSelector selectedStatFieldSelector';");
				this.writeLine("    var fieldOptionTable = getById('activeFieldTable');");
				this.writeLine("    fieldOptionTable.style.display = '';");
				this.writeLine("    fieldOptionTable.appendChild(fieldOptions[fieldName]);");
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
				this.writeLine("  return (statUrl + fieldPredicateString + fieldAggregateString + aggregatePredicateString);");
				this.writeLine("}");
				
				//	update links to statistics
				this.writeLine("function updateStats() {");
				this.writeLine("  var statUrl = buildStatUrl();");
				this.writeLine("  if (statUrl == null)");
				this.writeLine("    return;");
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
				this.writeLine("function addStatCell(sr, cssClass, value) {");
				this.writeLine("  sr.appendChild(newElement('td', null, cssClass, value));");
				this.writeLine("}");
				
				this.writeLine("</script>");
			}
		};
	}
	
	public static void main(String[] args) throws Exception {
		final GoldenGateDcsClient dcsc = new GoldenGateDcsClient(ServerConnection.getServerConnection("http://plazi.cs.umb.edu/GgServer/proxy"), "TCS");
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
		DcStatistics stats = dcsc.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out, ENCODING));
		dcss.sendStats(null, bw, stats, "JSON");
		bw.flush();
//		StatFieldSet fieldSet = StatFieldSet.readFieldSet(new BufferedReader(new InputStreamReader(new FileInputStream(new File("E:/Projektdaten/GgDcsTestFields.xml")))));
	}
}