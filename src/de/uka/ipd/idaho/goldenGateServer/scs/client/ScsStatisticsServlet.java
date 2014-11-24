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
package de.uka.ipd.idaho.goldenGateServer.scs.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet;
import de.uka.ipd.idaho.goldenGateServer.scs.GoldenGateScsConstants.Aggregate;
import de.uka.ipd.idaho.goldenGateServer.scs.GoldenGateScsConstants.Field;
import de.uka.ipd.idaho.goldenGateServer.scs.GoldenGateScsConstants.FieldSet;
import de.uka.ipd.idaho.goldenGateServer.scs.GoldenGateScsConstants.QueryField;
import de.uka.ipd.idaho.goldenGateServer.scs.GoldenGateScsConstants.Statistics;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.IoTools;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Servlet for generating statistics from a backing GoldenGATE SRS Collection
 * Statistics server.
 * 
 * @author sautter
 */
public class ScsStatisticsServlet extends GgServerHtmlServlet {
	
	private GoldenGateScsClient scsClient;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet#doInit()
	 */
	protected void doInit() throws ServletException {
		super.doInit();
		
		//	establish backend connection
		this.scsClient = new GoldenGateScsClient(this.serverConnection);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		//	get statistics and return it as CSV data
		if (("get_statistics".equals(request.getParameter("get_statistics")) || (request.getParameter("statId") != null)) && "csv".equals(request.getParameter("resultFormat"))) {
			
			//	get statistics
			StatisticsTray statTray = this.getStatistics(request);
			
			//	prepare response
			response.setHeader("Cache-Control", "no-cache");
			response.setContentType("text/plain; charset=UTF-8");
			
			//	send statistics
			StringVector keys = new StringVector();
			keys.addContent(statTray.stat.getDataKeys());
			Writer out = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
			Statistics.writeCsvData(out, statTray.stat, '"', keys);
			out.flush();
		}
		
		//	display query form
		else super.doPost(request, response);
	}
	
	private StatisticsTray getStatistics(HttpServletRequest request) throws IOException {
		
		//	check if request for previously retrieved statistics (alternative result format)
		String statId = request.getParameter("statId");
		if ((statId != null) && this.statisticsCache.containsKey(statId))
			return ((StatisticsTray) this.statisticsCache.get(statId));
		
		//	get fields
		FieldSet[] fieldSets = this.scsClient.getFields();
		
		//	build query
		TreeMap queryFields = new TreeMap();
		for (int fs = 0; fs < fieldSets.length; fs++) {
			Field[] fields = fieldSets[fs].getFields();
			for (int f = 0; f < fields.length; f++) {
				boolean use = "U".equals(request.getParameter("use_" + fields[f].fullName));
				
				String operation = (use ? request.getParameter("operation_" + fields[f].fullName) : Aggregate.IGNORE_TYPE);
				if (operation == null)
					continue;
				
				String filter = request.getParameter("filter_" + fields[f].fullName);
				if (filter != null) {
					filter = filter.trim();
					if (filter.length() == 0)
						filter = null;
				}
				
				int sortPriority = -1;
				try {sortPriority = Integer.parseInt(request.getParameter("order_sort_" + fields[f].fullName));}
				catch (NumberFormatException nfe) {}
				catch (NullPointerException npe) {}
				
				int fieldOrderPosition = queryFields.size();
				try {fieldOrderPosition = Integer.parseInt(request.getParameter("order_field_" + fields[f].fullName));}
				catch (NumberFormatException nfe) {}
				catch (NullPointerException npe) {}
				
				if (use || (filter != null))
					queryFields.put(new Integer(fieldOrderPosition), new QueryField(fields[f], operation, filter, sortPriority));
			}
		}
		
		//	add doc count field if selected
		if ("U".equals(request.getParameter("use_" + Field.COUNT_FIELD.fullName))) {
			int sortPriority = -1;
			try {sortPriority = Integer.parseInt(request.getParameter("order_sort_" + Field.COUNT_FIELD.fullName));}
			catch (NumberFormatException nfe) {}
			catch (NullPointerException npe) {}
			
			int fieldOrderPosition = queryFields.size();
			try {fieldOrderPosition = Integer.parseInt(request.getParameter("order_field_" + Field.COUNT_FIELD.fullName));}
			catch (NumberFormatException nfe) {}
			catch (NullPointerException npe) {}
			
			queryFields.put(new Integer(fieldOrderPosition), new QueryField(Field.COUNT_FIELD, Aggregate.COUNT_TYPE, null, sortPriority));
		}
		
		//	get statistics
		Statistics stat = this.scsClient.getStatistics((QueryField[]) queryFields.values().toArray(new QueryField[queryFields.size()]));
		
		//	cache statistics for later retrieval in other format
		StatisticsTray statTray = new StatisticsTray(stat);
		this.statisticsCache.put(statTray.id, statTray);
		
		//	return newly produced tray
		return statTray;
	}
	
	private HashMap statisticsCache = new LinkedHashMap(16, 0.75f, true) {
		protected boolean removeEldestEntry(Entry arg0) {
			return (this.size() > 32);
		}
	};
	private static class StatisticsTray {
		final String id;
		final Statistics stat;
		StatisticsTray(Statistics stat) {
			this.id = Gamta.getAnnotationID();
			this.stat = stat;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#getPageBuilder(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected HtmlPageBuilder getPageBuilder(HttpServletRequest request, HttpServletResponse response) throws IOException {
		final FieldSet[] fieldSets = this.scsClient.getFields();
		return new HtmlPageBuilder(this, request, response) {
			protected void include(String type, String tag) throws IOException {
				if ("includeBody".equals(type)) {
					
					//	open form and main table
					this.writeLine("<form method=\"POST\" action=\"" + this.request.getContextPath() + this.request.getServletPath() + "\" onsubmit=\"storeFieldOrder();\" onreset=\"restoreTables();\">");
					this.writeLine("<input type=\"hidden\" name=\"get_statistics\" value=\"get_statistics\">");
					this.writeLine("<table class=\"statisticsQueryMainTable\">");
					
					//	restore field settings from request if query given
					TreeMap fieldCollector = new TreeMap();
					TreeMap sortCollector = new TreeMap();
					Properties fieldLabels = new Properties();
					
					//	handle doc count field
					int fieldOrderPosition = fieldCollector.size();
					try {fieldOrderPosition = Integer.parseInt(this.request.getParameter("order_field_" + Field.COUNT_FIELD.fullName));}
					catch (NumberFormatException nfe) {}
					catch (NullPointerException npe) {}
					fieldCollector.put(new Integer(fieldOrderPosition), Field.COUNT_FIELD);
					fieldLabels.put(Field.COUNT_FIELD.fullName, "Document Count");
					
					int sortPriority = -1;
					try {sortPriority = Integer.parseInt(this.request.getParameter("order_sort_" + Field.COUNT_FIELD.fullName));}
					catch (NumberFormatException nfe) {}
					catch (NullPointerException npe) {}
					
					//	handle custom fields
					for (int fs = 0; fs < fieldSets.length; fs++) {
						Field[] fields = fieldSets[fs].getFields();
						for (int f = 0; f < fields.length; f++) {
							
							fieldOrderPosition = fieldCollector.size();
							try {fieldOrderPosition = Integer.parseInt(this.request.getParameter("order_field_" + fields[f].fullName));}
							catch (NumberFormatException nfe) {}
							catch (NullPointerException npe) {}
							fieldCollector.put(new Integer(fieldOrderPosition), fields[f]);
							fieldLabels.put(fields[f].fullName, (IoTools.prepareForHtml(fieldSets[fs].label) + ": " + IoTools.prepareForHtml(fields[f].label)));
							
							sortPriority = -1;
							try {sortPriority = Integer.parseInt(this.request.getParameter("order_sort_" + fields[f].fullName));}
							catch (NumberFormatException nfe) {}
							catch (NullPointerException npe) {}
							if (sortPriority != -1)
								sortCollector.put(new Integer(sortPriority), fields[f]);
						}
					}
					
					//	open field table
					this.writeLine("<tr>");
					this.writeLine("<td class=\"statisticsQueryMainTableBody\">");
					this.writeLine("<table id=\"fieldTable\" class=\"statisticsQueryTable fieldTable\">");
					
					//	write headers
					this.writeLine("<tr>");
					this.writeLine("<td class=\"statisticsQueryTableHeader\">Include in Statistics?</td>");
					this.writeLine("<td class=\"statisticsQueryTableHeader\">Field Name</td>");
					this.writeLine("<td class=\"statisticsQueryTableHeader\">Operation</td>");
					this.writeLine("<td class=\"statisticsQueryTableHeader\">Filter Value</td>");
					this.writeLine("</tr>");
					
					
					//	write fields
					for (Iterator fit = fieldCollector.values().iterator(); fit.hasNext();) {
						Field field = ((Field) fit.next());
						
						//	open table row
						this.writeLine("<tr id=\"field_" + field.fullName + "\">");
						
						//	add field selector
						this.write("<td class=\"statisticsQueryTableBody\">");
						this.write("<input type=\"checkbox\" name=\"use_" + field.fullName + "\" value=\"U\"" + ("U".equals(this.request.getParameter("use_" + field.fullName)) ? " checked" : "") + " onclick=\"selectField('" + field.fullName + "', '" + IoTools.prepareForHtml(fieldLabels.getProperty(field.fullName)) + "', this.checked);\">");
						this.write(" <a href=\"#\" onclick=\"return moveUpField('field_" + field.fullName + "');\">Up</a>");
						this.write(" <a href=\"#\" onclick=\"return moveDownField('field_" + field.fullName + "');\">Down</a>");
						this.writeLine("</td>");
						
						//	add label
						this.writeLine("<td class=\"statisticsQueryTableBody\">");
						this.writeLine(IoTools.prepareForHtml(fieldLabels.getProperty(field.fullName)).replaceAll("\\s", "&nbsp;"));
						this.writeLine("</td>");
						
						//	count field (skip aggregate selector and filter input field)
						if (Field.COUNT_FIELD.fullName.equals(field.fullName)) {
							this.writeLine("<td class=\"statisticsQueryTableBody\">");
							this.writeLine("&nbsp;");
							this.writeLine("</td>");
							
							this.writeLine("<td class=\"statisticsQueryTableBody\">");
							this.writeLine("&nbsp;");
							this.writeLine("</td>");
						}
						
						//	other field
						else {
							
							//	add aggregate selector
							this.write("<td class=\"statisticsQueryTableBody\">");
							this.writeLine("<select name=\"operation_" + field.fullName + "\">");
							Aggregate[] aggregates = field.getAggregates();
							for (int a = 0; a < aggregates.length; a++)
								this.writeLine("<option value=\"" + aggregates[a].type + "\"" + (aggregates[a].type.equals(this.request.getParameter("operation_" + field.fullName)) ? " selected" : "") + ">" + this.getAggregateLabel(aggregates[a].type) + "</option>");
							this.writeLine("<option value=\"" + QueryField.GROUP_OPERATION + "\"" + (QueryField.GROUP_OPERATION.equals(this.request.getParameter("operation_" + field.fullName)) ? " selected" : "") + ">" + this.getAggregateLabel(QueryField.GROUP_OPERATION) + "</option>");
							this.write("</select>");
							this.writeLine("</td>");
							
							//	add filter input field
							this.write("<td class=\"statisticsQueryTableBody\">");
							this.write("<input type=\"text\" name=\"filter_" + field.fullName + "\"" + ((this.request.getParameter("filter_" + field.fullName) == null) ? "" : (" value=\"" + this.request.getParameter("filter_" + field.fullName) + "\"")) + ">");
							this.writeLine("</td>");
						}
						
						this.writeLine("</tr>");
					}
					
					//	close field table
					this.writeLine("</table>");
					this.writeLine("</td>");
					
					
					//	open sort table
					this.writeLine("<td class=\"statisticsQueryMainTableBody\">");
					this.writeLine("<table id=\"sortTable\" class=\"statisticsQueryTable sortTable\">");
					
					//	write header (body will come from JavaScripts)
					this.writeLine("<tr>");
					this.writeLine("<td class=\"statisticsQueryTableHeader\">Sort Priority</td>");
					this.writeLine("</tr>");
					
					//	add sort fields
					for (Iterator sit = sortCollector.values().iterator(); sit.hasNext();) {
						Field field = ((Field) sit.next());
						this.writeLine("<tr id=\"sort_" + field.fullName + "\">");
						this.write("<td class=\"statisticsQueryTableBody\">");
						this.write(IoTools.prepareForHtml(fieldLabels.getProperty(field.fullName)));
						this.write(" <a href=\"#\" onclick=\"return moveUpSort('sort_" + field.fullName + "');\">Up</a>");
						this.write(" <a href=\"#\" onclick=\"return moveDownSort('sort_" + field.fullName + "');\">Down</a>");
						this.writeLine("</td>");
						this.writeLine("</tr>");
					}
					
					//	close sort table
					this.writeLine("</table>");
					this.writeLine("</td>");
					this.writeLine("</tr>");
					
					
					//	write buttons
					this.writeLine("<tr>");
					this.writeLine("<td colspan=\"4\" class=\"statisticsQueryMainButtons\">");
					this.writeLine("<input type=\"submit\" value=\"Generate Statistics\" class=\"submitButton\">");
					this.writeLine("&nbsp;");
					this.writeLine("<input type=\"reset\" value=\"Reset Form\" class=\"resetButton\">");
					this.writeLine("</td>");
					this.writeLine("</tr>");
					
					//	close main table
					this.writeLine("</table>");
					
					//	add ordering fields
					this.writeLine("<input type=\"hidden\" name=\"order_field_" + Field.COUNT_FIELD.fullName + "\" id=\"order_field_" + Field.COUNT_FIELD.fullName + "\" value=\"-1\">");
					this.writeLine("<input type=\"hidden\" name=\"order_sort_" + Field.COUNT_FIELD.fullName + "\" id=\"order_sort_" + Field.COUNT_FIELD.fullName + "\" value=\"-1\">");
					for (int fs = 0; fs < fieldSets.length; fs++) {
						Field[] fields = fieldSets[fs].getFields();
						for (int f = 0; f < fields.length; f++) {
							this.writeLine("<input type=\"hidden\" name=\"order_field_" + fields[f].fullName + "\" id=\"order_field_" + fields[f].fullName + "\" value=\"-1\">");
							this.writeLine("<input type=\"hidden\" name=\"order_sort_" + fields[f].fullName + "\" id=\"order_sort_" + fields[f].fullName + "\" value=\"-1\">");
						}
					}
					
					//	close form
					this.writeLine("</form>");
					
					
					//	get statistics if requested
					if ("get_statistics".equals(this.request.getParameter("get_statistics"))) {
						
						//	get statistics
						StatisticsTray statTray = getStatistics(this.request);
						
						//	add CSV link
						this.writeLine("<a href=\"" + this.request.getContextPath() + this.request.getServletPath() + "?get_statistics=get_statistics&resultFormat=csv&statId=" + statTray.id + "\">As CSV Data</a>");
						
						//	get keys
						String[] keys = statTray.stat.getDataKeys();
						
						//	open statistics table
						this.writeLine("<table class=\"statisticsDataTable\">");
						
						//	write headers
						this.writeLine("<tr>");
						for (int k = 0; k < keys.length; k++)
							this.writeLine("<td class=\"statisticsDataTableHeader\">" + keys[k] + "</td>");
						this.writeLine("</tr>");
						
						//	write data
						for (int s = 0; s < statTray.stat.size(); s++) {
							StringTupel st = statTray.stat.get(s);
							this.writeLine("<tr>");
							for (int k = 0; k < keys.length; k++) {
								String value = st.getValue(keys[k]);
								this.writeLine("<td class=\"statisticsDataTableBody\">" + ((value == null) ? "&nbsp;" : IoTools.prepareForHtml(value)) + "</td>");
							}
							this.writeLine("</tr>");
						}
						
						//	close statistics table
						this.writeLine("</table>");
					}
				}
				else super.include(type, tag);
			}
			
			private String getAggregateLabel(String aggregate) {
				if (Aggregate.COUNT_TYPE.equals(aggregate))
					return "Count values in group";
				else if (Aggregate.MIN_TYPE.equals(aggregate))
					return "Use minimum value in group";
				else if (Aggregate.MAX_TYPE.equals(aggregate))
					return "Use maximum value in group";
				else if (Aggregate.SUM_TYPE.equals(aggregate))
					return "Use sum of values in group";
				else if (Aggregate.AVERAGE_TYPE.equals(aggregate))
					return "Use average of values in group";
				else if (Aggregate.IGNORE_TYPE.equals(aggregate))
					return "Exclude this field";
				else if (QueryField.GROUP_OPERATION.equals(aggregate))
					return "Group by this field";
				else return aggregate;
			}
		};
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#getOnloadCalls()
	 */
	public String[] getOnloadCalls() {
		String[] jsCalls = {"init()"};
		return jsCalls;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#writePageHeadExtensions(de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet.HtmlPageBuilder)
	 */
	public void writePageHeadExtensions(HtmlPageBuilder out) throws IOException {
		out.writeLine("<script type=\"text/javascript\">");
		
		out.writeLine("var _fieldTable;");
		out.writeLine("var _sortTable;");
		out.writeLine("var _fieldStore;");
		out.writeLine("var _sortStore;");
		
		out.writeLine("function init() {");
		out.writeLine("  _fieldTable = document.getElementById('fieldTable');");
		out.writeLine("  _sortTable = document.getElementById('sortTable');");
		out.writeLine("  _fieldStore = new Array(_fieldTable.rows.length - 1);");
		out.writeLine("  for (var r = 1; r < _fieldTable.rows.length; r++)");
		out.writeLine("    _fieldStore[r-1] = _fieldTable.rows[r];");
		out.writeLine("  _sortStore = new Array(_sortTable.rows.length - 1);");
		out.writeLine("  for (var r = 1; r < _sortTable.rows.length; r++)");
		out.writeLine("    _sortStore[r-1] = _sortTable.rows[r];");
		out.writeLine("}");
		
		out.writeLine("function storeFieldOrder() {");
		out.writeLine("  for (var r = 1; r < _fieldTable.rows.length; r++) {"); // start from 1 to jump headers
		out.writeLine("    var orderField = document.getElementById('order_' + _fieldTable.rows[r].id);");
		out.writeLine("    if (orderField)");
		out.writeLine("      orderField.value = r;");
		out.writeLine("  }");
		out.writeLine("  for (var r = 1; r < _sortTable.rows.length; r++) {"); // start from 1 to jump headers
		out.writeLine("    var orderField = document.getElementById('order_' + _sortTable.rows[r].id);");
		out.writeLine("    if (orderField)");
		out.writeLine("      orderField.value = r;");
		out.writeLine("  }");
		out.writeLine("}");
		
		out.writeLine("function restoreTables() {");
		out.writeLine("  while (_fieldTable.rows.length > 1) {");
		out.writeLine("    var theRow = _fieldTable.rows[1];");
		out.writeLine("    theRow.parentNode.removeChild(theRow);");
		out.writeLine("  }");
		out.writeLine("  for (var r = 0; r < _fieldStore.length; r++)");
		out.writeLine("    _fieldTable.rows[0].parentNode.appendChild(_fieldStore[r]);");
		out.writeLine("  while (_sortTable.rows.length > 1) {");
		out.writeLine("    var theRow = _sortTable.rows[1];");
		out.writeLine("    theRow.parentNode.removeChild(theRow);");
		out.writeLine("  }");
		out.writeLine("  for (var r = 0; r < _sortStore.length; r++)");
		out.writeLine("    _sortTable.rows[0].parentNode.appendChild(_sortStore[r]);");
		out.writeLine("}");
		
		out.writeLine("function moveUpField(rowId) {");
		out.writeLine("  var theRow = document.getElementById(rowId);");
		out.writeLine("  if (theRow) {");
		out.writeLine("    for (var r = 1; r < _fieldTable.rows.length; r++) {"); // start from 1 to leave headers in place
		out.writeLine("      if ((r != 1) && (_fieldTable.rows[r] == theRow)) {");
		out.writeLine("        var beforeRow = _fieldTable.rows[r-1];");
		out.writeLine("        theRow.parentNode.removeChild(theRow);");
		out.writeLine("        beforeRow.parentNode.insertBefore(theRow, beforeRow);");
		out.writeLine("        return false;");
		out.writeLine("      }");
		out.writeLine("    }");
		out.writeLine("  }");
		out.writeLine("}");
		
		out.writeLine("function moveDownField(rowId) {");
		out.writeLine("  var theRow = document.getElementById(rowId);");
		out.writeLine("  if (theRow) {");
		out.writeLine("    for (var r = 1; r < _fieldTable.rows.length; r++) {"); // start from 1 to leave headers in place
		out.writeLine("      if (((r+1) < _fieldTable.rows.length) && (_fieldTable.rows[r] == theRow)) {");
		out.writeLine("        var afterRow = _fieldTable.rows[r+1];");
		out.writeLine("        afterRow.parentNode.removeChild(afterRow);");
		out.writeLine("        theRow.parentNode.insertBefore(afterRow, theRow);");
		out.writeLine("        return false;");
		out.writeLine("      }");
		out.writeLine("    }");
		out.writeLine("  }");
		out.writeLine("}");
		
		out.writeLine("function selectField(fieldId, fieldLabel, isSelected) {");
		out.writeLine("  if (isSelected) {");
		out.writeLine("    var newRow = _sortTable.insertRow(_sortTable.rows.length);");
		out.writeLine("    newRow.id = ('sort_' + fieldId);");
		out.writeLine("    var td = document.createElement('td');");
		out.writeLine("    td.appendChild(document.createTextNode(fieldLabel));");
		out.writeLine("    td.className = 'statisticsQueryTableBody';");
		out.writeLine("    var up = document.createElement('a');");
		out.writeLine("    up.href = '#';");
		out.writeLine("    up.onclick = function() {");
		out.writeLine("      moveUpSort('sort_' + fieldId);");
		out.writeLine("      return false;");
		out.writeLine("    };");
		out.writeLine("    up.appendChild(document.createTextNode('Up'));");
		out.writeLine("    td.appendChild(document.createTextNode(' '));");
		out.writeLine("    td.appendChild(up);");
		out.writeLine("    var down = document.createElement('a');");
		out.writeLine("    down.href = '#';");
		out.writeLine("    down.onclick = function() {");
		out.writeLine("      moveDownSort('sort_' + fieldId);");
		out.writeLine("      return false;");
		out.writeLine("    };");
		out.writeLine("    down.appendChild(document.createTextNode('Down'));");
		out.writeLine("    td.appendChild(document.createTextNode(' '));");
		out.writeLine("    td.appendChild(down);");
		out.writeLine("    newRow.appendChild(td);");
		out.writeLine("  }");
		out.writeLine("  else {");
		out.writeLine("    var theRow = document.getElementById('sort_' + fieldId);");
		out.writeLine("    if (theRow)");
		out.writeLine("      theRow.parentNode.removeChild(theRow);");
		out.writeLine("    return false;");
		out.writeLine("  }");
		out.writeLine("}");
		 
		out.writeLine("function moveUpSort(rowId) {");
		out.writeLine("  var theRow = document.getElementById(rowId);");
		out.writeLine("  if (theRow) {");
		out.writeLine("    for (var r = 1; r < _sortTable.rows.length; r++) {"); // start from 1 to leave headers in place
		out.writeLine("      if ((r != 1) && (_sortTable.rows[r] == theRow)) {");
		out.writeLine("        var beforeRow = _sortTable.rows[r-1];");
		out.writeLine("        theRow.parentNode.removeChild(theRow);");
		out.writeLine("        beforeRow.parentNode.insertBefore(theRow, beforeRow);");
		out.writeLine("        return false;");
		out.writeLine("      }");
		out.writeLine("    }");
		out.writeLine("  }");
		out.writeLine("}");
		
		out.writeLine("function moveDownSort(rowId) {");
		out.writeLine("  var theRow = document.getElementById(rowId);");
		out.writeLine("  if (theRow) {");
		out.writeLine("    for (var r = 1; r < _sortTable.rows.length; r++) {"); // start from 1 to leave headers in place
		out.writeLine("      if (((r+1) < _sortTable.rows.length) && (_sortTable.rows[r] == theRow)) {");
		out.writeLine("        var afterRow = _sortTable.rows[r+1];");
		out.writeLine("        afterRow.parentNode.removeChild(afterRow);");
		out.writeLine("        theRow.parentNode.insertBefore(afterRow, theRow);");
		out.writeLine("        return false;");
		out.writeLine("      }");
		out.writeLine("    }");
		out.writeLine("  }");
		out.writeLine("}");
		
		out.writeLine("</script>");
	}
}