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
import java.io.File;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.util.CountingSet;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.DcStatistics;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatField;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldGroup;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Client servlet for rendering statistics data from GoldenGATE DCS instances
 * in Google Charts.
 * 
 * @author sautter
 */
public abstract class GoldenGateDcsChartServlet extends GoldenGateDcsClientServlet {
	
	private String chartWindowParameters = "width=550,height=350,top=100,left=100,resizable=yes,scrollbar=yes,scrollbars=yes";
	
	private Settings chartTypeProperties;
	
	private HashSet numericFields = new HashSet();
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#reInit()
	 */
	protected void reInit() throws ServletException {
		super.reInit();
		
		//	read further settings
		this.chartWindowParameters = this.getSetting("chartWindowParameters", this.chartWindowParameters);
		
		//	read options for supported chart types
		this.chartTypeProperties = Settings.loadSettings(new File(this.dataFolder, "chartTypeProperties.cnfg"));
		
		//	clear numeric field names
		this.numericFields.clear();
	}
	
	protected StatFieldSet getFieldSet() throws IOException {
		StatFieldSet fieldSet = super.getFieldSet();
		StatFieldGroup[] fieldGroups = fieldSet.getFieldGroups();
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++) {
				if ("integer".equals(fields[f].dataType) || "real".equals(fields[f].dataType))
					numericFields.add(fields[f].fullName);
			}
		}
		return fieldSet;
	}
	
	private static final Properties numbersToMonthNames = new Properties();
	static {
		numbersToMonthNames.setProperty("1", "Jan");
		numbersToMonthNames.setProperty("01", "Jan");
		numbersToMonthNames.setProperty("2", "Feb");
		numbersToMonthNames.setProperty("02", "Feb");
		numbersToMonthNames.setProperty("3", "Mar");
		numbersToMonthNames.setProperty("03", "Mar");
		numbersToMonthNames.setProperty("4", "Apr");
		numbersToMonthNames.setProperty("04", "Apr");
		numbersToMonthNames.setProperty("5", "May");
		numbersToMonthNames.setProperty("05", "May");
		numbersToMonthNames.setProperty("6", "Jun");
		numbersToMonthNames.setProperty("06", "Jun");
		numbersToMonthNames.setProperty("7", "Jul");
		numbersToMonthNames.setProperty("07", "Jul");
		numbersToMonthNames.setProperty("8", "Aug");
		numbersToMonthNames.setProperty("08", "Aug");
		numbersToMonthNames.setProperty("9", "Sep");
		numbersToMonthNames.setProperty("09", "Sep");
		numbersToMonthNames.setProperty("10", "Oct");
		numbersToMonthNames.setProperty("11", "Nov");
		numbersToMonthNames.setProperty("12", "Dec");
	}
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		//	check what's requested
		String pathInfo = request.getPathInfo();
		
		//	request for chart preview page
		if ("/chart.html".equalsIgnoreCase(pathInfo)) {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("text/html");
			this.sendHtmlPage(new ChartPreviewPageBuilder(this, request, response));
			return;
		}
		
		//	this involves getting the actual data
		if ("/chart.js".equalsIgnoreCase(pathInfo)) {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("text/html");
			response.setHeader("Cache-Control", "no-cache");
			this.sendChartBuilderJavaScript(request, response);
			return;
		}
		
		//	this involves getting the actual data
		if ("/chartData.js".equalsIgnoreCase(pathInfo)) {
			response.setCharacterEncoding("UTF-8");
			response.setContentType("text/html");
			response.setHeader("Cache-Control", "no-cache");
			this.sendChartData(request, response);
			return;
		}
		
		//	request for chart configuration page
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/html");
		this.sendHtmlPage(new ChartBuilderPageBuilder(this, request, response));
	}
	
	private DcStatChartData getChartData(HttpServletRequest request) throws IOException {
		
		//	read chart parameters
		String chartType = request.getParameter("type");
		Settings chartProperties = this.chartTypeProperties.getSubset(chartType);
		
		//	collect query parameters
		StringVector outputFields = new StringVector();
		StringVector groupingFields = new StringVector();
		StringVector orderingFields = new StringVector();
		Properties fieldPredicates = new Properties();
		Properties fieldAggregates = new Properties();
		Properties aggregatePredicates = new Properties();
		
		//	get data series renaming (prefilled with field labels)
		Properties valueFieldAliases = new Properties();
		StatFieldGroup[] sfgs = this.getFieldSet().getFieldGroups();
		for (int g = 0; g < sfgs.length; g++) {
			StatField[] sfs = sfgs[g].getFields();
			for (int f = 0; f < sfs.length; f++)
				valueFieldAliases.setProperty(sfs[f].fullName, sfs[f].label);
		}
		
		//	get value fields
		String valueField = request.getParameter("field");
		if ((valueField != null) && (valueField.length() == 0))
			valueField = null;
		String valueSumField = null;
		boolean gotMultiFieldInput = false;
		
		//	get values for multi-field series
		if ((valueField == null) || (valueField.length() == 0)) {
			
			//	get fields and associated data
			for (int f = 0; true; f++) {
				String vField = request.getParameter("field" + f);
				if ((vField == null) || (vField.length() == 0))
					break;
				String vAggregate = request.getParameter("field" + f + "Aggregate");
				if ((vAggregate == null) || (vAggregate.length() == 0))
					break;
				outputFields.addElement(vField);
				fieldAggregates.setProperty(vField, vAggregate);
				
				gotMultiFieldInput = true;
				
				String vLabel = request.getParameter("field" + f + "Label");
				if ((vLabel != null) && (vLabel.length() != 0))
					valueFieldAliases.setProperty(vField, vLabel);
			}
			
			//	get sum field and label
			valueSumField = request.getParameter("fieldSum");
			if ((valueSumField != null) && (valueSumField.length() != 0)) {
				outputFields.addElement(valueSumField);
				fieldAggregates.setProperty(valueSumField, "sum"); // anything else makes no sense
				String valueSumLabel = request.getParameter("fieldSumLabel");
				if ((valueSumLabel != null) && (valueSumLabel.length() != 0))
					valueFieldAliases.setProperty(valueSumField, valueSumLabel);
			}
			else valueSumField = null;
//			
//			DO NOT DO THIS: multi-field offers custom series and group naming
//			//	switch to single field if only a single field given
//			if (outputFields.size() == 1) {
//				valueField = outputFields.get(0);
//				valueSumField = null;
//				gotMultiFieldInput = false;
//			}
		}
		
		//	get parameters for single-value or grouping based series
		else {
			
			//	add value field
			outputFields.addElement(valueField);
			String valueAggregate = request.getParameter("fieldAggregate");
			if ((valueAggregate != null) && (valueAggregate.length() != 0))
				fieldAggregates.setProperty(valueField, valueAggregate);
		}
		
		//	if we don't have any fields by now, we'll never have any ...
		if (outputFields.isEmpty())
			return null;
		
		//	read series data
		String seriesField = request.getParameter("series");
		String seriesOrder = null;
		if ((seriesField != null) && (seriesField.length() == 0))
			seriesField = null;
//		DO NOT DO THIS: multi-field offers custom series and group naming
//		if ("multiField".equals(seriesField) && (outputFields.size() == 1))
//			seriesField = null;
		if ((seriesField != null) && !"multiField".equals(seriesField)) {
			outputFields.addElement(seriesField);
			groupingFields.addElement(seriesField);
			orderingFields.addElement(seriesField);
			seriesOrder = request.getParameter("seriesOrder");
			if ((seriesOrder != null) && (seriesOrder.length() == 0))
				seriesOrder = null;
			String emptySeriesLabelSubstitute = request.getParameter("emptySeriesLabelSubstitute");
			if ((emptySeriesLabelSubstitute != null) && (emptySeriesLabelSubstitute.length() != 0))
				valueFieldAliases.setProperty("EmPtYsErIeSnAmE", emptySeriesLabelSubstitute);
		}
		
		//	get grouping field if applicable
		boolean isChartGroupable = "true".equals(chartProperties.getSetting("valuesGroupable", "true"));
		String groupField = null;
		String groupOrder = null;
		String artificialGroups = null;
		if (isChartGroupable) {
			groupField = request.getParameter("group");
			if ((groupField != null) && (groupField.length() == 0))
				groupField = null;
//			DO NOT DO THIS: multi-field offers custom series and group naming
//			if ("multiField".equals(groupField) && (outputFields.size() == 1))
//				groupField = null;
			if ((groupField != null) && !"multiField".equals(groupField)) {
				outputFields.addElement(groupField);
				groupingFields.addElement(groupField);
				orderingFields.addElement(groupField);
				groupOrder = request.getParameter("groupOrder");
				if ((groupOrder != null) && (groupOrder.length() == 0))
					groupOrder = null;
				String emptyGroupLabelSubstitute = request.getParameter("emptyGroupLabelSubstitute");
				if ((emptyGroupLabelSubstitute != null) && (emptyGroupLabelSubstitute.length() != 0))
					valueFieldAliases.setProperty("EmPtYgRoUpNaMe", emptyGroupLabelSubstitute);
				artificialGroups = request.getParameter("artificialGroups");
				if ((artificialGroups != null) && (artificialGroups.length() == 0))
					artificialGroups = null;
			}
		}
		
		//	setting series or group to 'multiField' if null, but respective other field given
		if (gotMultiFieldInput) {
			if ((seriesField == null) && !"multiField".equals(groupField))
				seriesField = "multiField";
			else if ((groupField == null) && !"multiField".equals(seriesField))
				groupField = "multiField";
		}
		
		//	decode query parameters
		for (Enumeration pne = request.getParameterNames(); pne.hasMoreElements();) {
			String pn = ((String) pne.nextElement());
			if (!pn.startsWith("FILTER_"))
				continue;
			String filterField = pn.substring("FILTER_".length());
			String filterValue = request.getParameter(pn);
			if ((filterValue == null) || (filterValue.length() == 0))
				continue;
			String filterAggregate = request.getParameter("AGGREGATE_" + filterField);
			if ((filterAggregate == null) || (filterAggregate.length() == 0))
				fieldPredicates.setProperty(filterField, filterValue);
			else {
				fieldAggregates.setProperty(filterField, filterAggregate);
				aggregatePredicates.setProperty(filterField, filterValue);
			}
		}
		
		//	fetch data from backing TCS
		boolean allowCache = !"force".equals(request.getParameter("cacheControl"));
		DcStatistics stats = this.getDcsStats(outputFields.toStringArray(), groupingFields.toStringArray(), orderingFields.toStringArray(), fieldPredicates, fieldAggregates, aggregatePredicates, valueSumField, groupField, allowCache);
		
		//	organize statistics data
		DcStatChartData chartData = new DcStatChartData(stats, seriesField, groupField, valueField);
		if (artificialGroups != null) {
			String[] artificialGroupLabels = artificialGroups.split("\\s*\\;\\s*");
			for (int g = 0; g < artificialGroupLabels.length; g++) {
				while (this.numericFields.contains(groupField) && artificialGroupLabels[g].startsWith("0"))
					artificialGroupLabels[g] = artificialGroupLabels[g].substring("0".length());
				chartData.addGroup(artificialGroupLabels[g], artificialGroupLabels[g], (seriesField != null));
			}
		}
		if ("multiField".equals(seriesField)) {
			for (int f = 0; f < outputFields.size(); f++) {
				String ofn = outputFields.get(f);
				if (!ofn.equals(groupField))
					chartData.addSeries(ofn, valueFieldAliases.getProperty(ofn));
			}
			for (int s = 0; s < stats.size(); s++) {
				StringTupel st = stats.get(s);
				String gn = ((groupField == null) ? null : st.getValue(groupField));
				if ("".equals(gn))
					gn = valueFieldAliases.getProperty("EmPtYgRoUpNaMe", gn);
				for (int f = 0; f < outputFields.size(); f++) {
					String ofn = outputFields.get(f);
					if (!ofn.equals(groupField))
						chartData.addData(st, ofn, gn, ofn);
				}
			}
		}
		else if ("multiField".equals(groupField)) {
			for (int f = 0; f < outputFields.size(); f++) {
				String ofn = outputFields.get(f);
				if (ofn.equals(seriesField))
					continue;
				chartData.addGroup(ofn, valueFieldAliases.getProperty(ofn), (seriesField != null));
			}
			for (int s = 0; s < stats.size(); s++) {
				StringTupel st = stats.get(s);
				String sn = ((seriesField == null) ? null : st.getValue(seriesField));
				if ("".equals(sn))
					sn = valueFieldAliases.getProperty("EmPtYsErIeSnAmE", sn);
				for (int f = 0; f < outputFields.size(); f++) {
					String ofn = outputFields.get(f);
					if (!ofn.equals(seriesField))
						chartData.addData(st, sn, ofn, ofn);
				}
			}
		}
		else for (int s = 0; s < stats.size(); s++) {
			StringTupel st = stats.get(s);
			String sn = ((seriesField == null) ? null : st.getValue(seriesField));
			if ("".equals(sn))
				sn = valueFieldAliases.getProperty("EmPtYsErIeSnAmE", sn);
			String gn = ((groupField == null) ? null : st.getValue(groupField));
			if ("".equals(gn))
				gn = valueFieldAliases.getProperty("EmPtYgRoUpNaMe", gn);
			chartData.addData(st, sn, gn, valueField);
		}
		
		//	sort series and groups (have to do this before the cutoffs, as 'Other' has to stay at end)
		if (seriesOrder != null)
			chartData.orderSeries(seriesOrder, this.numericFields.contains(seriesField));
		if (groupOrder != null)
			chartData.orderGroups(groupOrder, this.numericFields.contains(groupField));
		
		//	handle month names (needs to be done before bucketization)
		boolean translateMonthNumbers = "yes".equals(request.getParameter("translateMonthNumbers"));
		if (translateMonthNumbers) {
			if ((seriesField != null) && seriesField.toLowerCase().endsWith("month"))
				for (Iterator sit = chartData.seriesByName.keySet().iterator(); sit.hasNext();) {
					DcStatSeriesHead dssh = chartData.getSeriesHead((String) sit.next());
					dssh.label = numbersToMonthNames.getProperty(dssh.name, dssh.label);
				}
			if ((groupField != null) && groupField.toLowerCase().endsWith("month"))
				for (Iterator git = chartData.groupsByName.keySet().iterator(); git.hasNext();) {
					DcStatGroup dsg = chartData.getGroup(((String) git.next()), (seriesField != null));
					dsg.label = numbersToMonthNames.getProperty(dsg.name, dsg.label);
				}
		}
		
		//	finally ...
		return chartData;
	}
	
	private void sendChartData(HttpServletRequest request, HttpServletResponse response) throws IOException {
		
		//	use request query string (less cache control parameter) as cache key for JavaScript (readily does the trick in generated pages with static query string templates, which are likely the lion's share of requests) 
		String jsCacheKey = ("DATA " + request.getQueryString().replaceAll("\\&cacheControl=[^\\&]+", ""));
		boolean allowCache = !"force".equals(request.getParameter("cacheControl"));
		
		//	use cache if allowed to
		if (allowCache) {
			JsCacheEntry jsCacheEntry = ((JsCacheEntry) this.jsCache.get(jsCacheKey));
			if (jsCacheEntry != null) {
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
				bw.write(jsCacheEntry.js);
				bw.flush();
				return;
			}
		}
		
		//	get raw chart data
		DcStatChartData chartData = this.getChartData(request);
		if (chartData == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No data fields selected.");
			return;
		}
		
		//	read chart parameters
		String chartType = request.getParameter("type");
		Settings chartProperties = this.chartTypeProperties.getSubset(chartType);
		
		//	(re-)get parameters
		boolean translateMonthNumbers = "yes".equals(request.getParameter("translateMonthNumbers"));
		boolean isChartGroupable = "true".equals(chartProperties.getSetting("valuesGroupable", "true"));
		
		//	generate rendering JavaScript
		final StringWriter jsCacheWriter = new StringWriter();
		BufferedWriter bw = new BufferedWriter(new FilterWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8")) {
			public void write(int c) throws IOException {
				super.write(c);
				jsCacheWriter.write(c);
			}
			public void write(char[] cbuf, int off, int len) throws IOException {
				super.write(cbuf, off, len);
				jsCacheWriter.write(cbuf, off, len);
			}
			public void write(String str, int off, int len) throws IOException {
				super.write(str, off, len);
				jsCacheWriter.write(str, off, len);
			}
		});
		
		//	write chart data array
		bw.write("["); bw.newLine();
		chartData.writeJsonArrayContent(bw, ("number".equals(chartProperties.getSetting("groupFieldType", "string")) && this.numericFields.contains(chartData.groupField) && !"B".equals(request.getParameter("groupCutoff")) && (!chartData.groupField.toLowerCase().endsWith("month") || !translateMonthNumbers)), isChartGroupable, this);
		bw.write("]"); bw.newLine();
		
		//	send & cache data
		bw.flush();
		this.jsCache.put(jsCacheKey, new JsCacheEntry(jsCacheWriter.toString(), this.statsLastUpdated));
	}
	
	private void sendChartBuilderJavaScript(HttpServletRequest request, HttpServletResponse response) throws IOException {
		
		//	use request query string (less cache control parameter) as cache key for JavaScript (readily does the trick in generated pages with static query string templates, which are likely the lion's share of requests) 
		String jsCacheKey = ("SCRIPT " + request.getQueryString().replaceAll("\\&cacheControl=[^\\&]+", ""));
		boolean allowCache = !"force".equals(request.getParameter("cacheControl"));
		
		//	use cache if allowed to
		if (allowCache) {
			JsCacheEntry jsCacheEntry = ((JsCacheEntry) this.jsCache.get(jsCacheKey));
			if (jsCacheEntry != null) {
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
				bw.write(jsCacheEntry.js);
				bw.flush();
				return;
			}
		}
		
		//	get raw chart data
		DcStatChartData chartData = this.getChartData(request);
		if (chartData == null) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No data fields selected.");
			return;
		}
		
		//	collect debug output
		StringWriter debugData = new StringWriter();
		
		//	read chart parameters
		String chartId = request.getParameter("chartId");
		String chartType = request.getParameter("type");
		Settings chartProperties = this.chartTypeProperties.getSubset(chartType);
		String[] chartOptions = chartProperties.getSetting("chartOptionNames", "").trim().split("\\s+");
		Settings chartOptionDefaults = chartProperties.getSubset("chartOptionDefaults");
		
		//	(re-)get parameters
		boolean translateMonthNumbers = "yes".equals(request.getParameter("translateMonthNumbers"));
		boolean isChartGroupable = "true".equals(chartProperties.getSetting("valuesGroupable", "true"));
		
		//	enforce cutoffs, or bucketize
		if ((chartData.seriesField != null) && !"multiField".equals(chartData.seriesField)) {
			String seriesCutoff = request.getParameter("seriesCutoff");
			if ((seriesCutoff == null) || (seriesCutoff.length() == 0) || "0".equals(seriesCutoff)) {}
			else if ("B".equals(seriesCutoff)) {
				String buckets = request.getParameter("seriesBuckets");
				boolean truncate = "true".equals(request.getParameter("truncateSeriesBuckets"));
				if ((buckets != null) && (buckets.length() != 0))
					chartData.bucketizeSeries(buckets, truncate, this.numericFields.contains(chartData.seriesField), (translateMonthNumbers && chartData.seriesField.toLowerCase().endsWith("month")));
			}
			else try {
				chartData.pruneSeries(Integer.parseInt(seriesCutoff));
			} catch (NumberFormatException nfe) {}
		}
		if ((chartData.groupField != null) && !"multiField".equals(chartData.groupField)) {
			String groupCutoff = request.getParameter("groupCutoff");
			if ((groupCutoff == null) || (groupCutoff.length() == 0) || "0".equals(groupCutoff)) {}
			else if ("B".equals(groupCutoff)) {
				String buckets = request.getParameter("groupBuckets");
				boolean truncate = "true".equals(request.getParameter("truncateGroupBuckets"));
				if ((buckets != null) && (buckets.length() != 0))
					chartData.bucketizeGroups(buckets, truncate, this.numericFields.contains(chartData.groupField), (translateMonthNumbers && chartData.groupField.toLowerCase().endsWith("month")));
			}
			else try {
				chartData.pruneGroups(Integer.parseInt(groupCutoff));
			} catch (NumberFormatException nfe) {}
		}
		
		//	get customization parameters
		boolean addDataSumToTitle = "true".equals(request.getParameter("addDataSumToTitle"));
//		
//		//	write raw stats data as comment (for debug purposes)
//		debugData.write("/*\r\n");
//		chartData.sourceData.writeData(debugData);
//		debugData.write("*/\r\n");
		
		//	generate rendering JavaScript
		final StringWriter jsCacheWriter = new StringWriter();
		BufferedWriter bw = new BufferedWriter(new FilterWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8")) {
			public void write(int c) throws IOException {
				super.write(c);
				jsCacheWriter.write(c);
			}
			public void write(char[] cbuf, int off, int len) throws IOException {
				super.write(cbuf, off, len);
				jsCacheWriter.write(cbuf, off, len);
			}
			public void write(String str, int off, int len) throws IOException {
				super.write(str, off, len);
				jsCacheWriter.write(str, off, len);
			}
		});
		bw.write(debugData.toString());
		
		//	write chart data array
		bw.write("function drawChart" + chartId + "() {"); bw.newLine();
		bw.write("  var chartData = google.visualization.arrayToDataTable(["); bw.newLine();
		chartData.writeJsonArrayContent(bw, ("number".equals(chartProperties.getSetting("groupFieldType", "string")) && this.numericFields.contains(chartData.groupField) && !"B".equals(request.getParameter("groupCutoff")) && (!chartData.groupField.toLowerCase().endsWith("month") || !translateMonthNumbers)), isChartGroupable, this);
		bw.write("  ]);"); bw.newLine();
		
		//	write chart options
		bw.write("  var chartOptions = {"); bw.newLine();
		this.writeChartOptions(bw, chartOptions, request, chartOptionDefaults, chartData.value, addDataSumToTitle, (chartData.seriesByName.isEmpty() && (chartData.groupsByName.isEmpty() || "multiField".equals(chartData.groupField))));
		bw.write("  };"); bw.newLine();
		bw.write("  var chart = new google.visualization." + chartProperties.getSetting("chartClassName") + "(document.getElementById('chartDiv" + chartId + "'));"); bw.newLine();
		bw.write("  chart.draw(chartData, chartOptions);"); bw.newLine();
		bw.write("}"); bw.newLine();
		
		//	write rendering function calls
		bw.write("google.load('visualization', '1', {'packages': ['" + chartProperties.getSetting("chartPackage") + "']});"); bw.newLine();
		bw.write("google.setOnLoadCallback(drawChart" + chartId + ");"); bw.newLine();
		
		//	send & cache JavaScript
		bw.flush();
		this.jsCache.put(jsCacheKey, new JsCacheEntry(jsCacheWriter.toString(), this.statsLastUpdated));
	}
	
	private static final int initJsCacheSize = 128;
	private static final int maxJsCacheSize = 512;
	private Map jsCache = Collections.synchronizedMap(new LinkedHashMap(initJsCacheSize, 0.9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return (this.size() > maxJsCacheSize);
		}
		public Object get(Object key) {
			JsCacheEntry value = ((JsCacheEntry) super.get(key));
			
			//	invalidate cache entries whose data is outdated
			if ((value != null) && (value.statsLastUpdated < statsLastUpdated)) {
				this.remove(key);
				return null;
			}
			
			return value;
		}
	});
	private class JsCacheEntry {
		final String js;
		final long statsLastUpdated;
		JsCacheEntry(String js, long statsLastUpdated) {
			this.js = js;
			this.statsLastUpdated = statsLastUpdated;
		}
	}
	
	private long statsLastUpdated = 0;
	private DcStatistics getDcsStats(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String valueSumField, String groupingField, boolean allowCache) throws IOException {
		
		//	get raw data from backing DCS
		DcStatistics stats = this.dcsClient.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, allowCache);
		
		//	update stats data timestamp
		this.statsLastUpdated = Math.max(this.statsLastUpdated, stats.lastUpdated);
		
		//	normalize statistics field names (no dots allowed in CSV column names in backend)
		for (int f = 0; f < outputFields.length; f++) {
			String fn = outputFields[f];
			if (fn.indexOf('.') == -1)
				continue;
			StringBuffer sfnBuilder = new StringBuffer();
			boolean toUpperCase = true;
			for (int c = 0; c < fn.length(); c++) {
				char ch = fn.charAt(c);
				if (ch == '.')
					toUpperCase = true;
				else if (toUpperCase) {
					sfnBuilder.append(Character.toUpperCase(ch));
					toUpperCase = false;
				}
				else sfnBuilder.append(ch);
			}
			String sfn = sfnBuilder.toString();
			stats.renameKey(sfn, fn);
		}
		
		//	subtract individual values from sum
		if (valueSumField != null) try {
			String[] fields = stats.getFields();
			for (int t = 0; t < stats.size(); t++) {
				StringTupel st = stats.get(t);
				int sum = Integer.parseInt(st.getValue(valueSumField, "0"));
				if (sum == 0)
					continue;
				for (int f = 0; f < fields.length; f++)
					if (!"DocCount".equals(fields[f]) && !valueSumField.equals(fields[f]) && ((groupingField == null) || !groupingField.equals(fields[f]))) try {
						sum -= Integer.parseInt(st.getValue(fields[f], "0"));
					} catch (NumberFormatException nfe) {}
				st.setValue(valueSumField, ((sum < 1) ? "0" : ("" + sum)));
			}
		} catch (NumberFormatException nfe) {}
		
		//	finally ...
		return stats;
	}
	
	private void writeChartOptions(BufferedWriter bw, String[] chartOptions, HttpServletRequest request, Settings chartOptionDefaults, int dataValueSum, boolean addDataValueSumToTitle, boolean hideLegend) throws IOException {
		
		//	cut data point sum if not desired
		if (!addDataValueSumToTitle)
			dataValueSum = -1;
		
		//	set up collecting sub objects
		TreeMap chartOptionObjects = new TreeMap();
		
		//	go through options
		for (int o = 0; o < chartOptions.length; o++) {
			
			//	check if we have a quoted (string valued) option value
			boolean quoteValue = false;
			if (chartOptions[o].startsWith("'")) {
				chartOptions[o] = chartOptions[o].substring(1);
				quoteValue = true;
			}
			
			//	get option value
			String chartOptionValue = request.getParameter(chartOptions[o]);
			if ((chartOptionValue == null) || (chartOptionValue.length() == 0))
				chartOptionValue = chartOptionDefaults.getSetting(chartOptions[o]);
			if ((chartOptionValue == null) || (chartOptionValue.length() == 0))
				continue;
			
			//	we have a plain option value
			if (chartOptions[o].indexOf('.') == -1) {
				if ("title".equals(chartOptions[o]) && (dataValueSum != -1)) {
					chartOptionValue = (chartOptionValue + " (n=" + dataValueSum + ")");
					dataValueSum = -1;
				}
				bw.write("    " + chartOptions[o] + ": " + (quoteValue ? ("'" + this.escapeForJavaScript(chartOptionValue) + "'") : chartOptionValue) + ","); bw.newLine();
			}
			
			//	we have an attribute of an option object
			else {
				String chartOptionPrefix = chartOptions[o].substring(0, chartOptions[o].indexOf('.'));
				chartOptions[o] = chartOptions[o].substring(chartOptions[o].indexOf('.') + ".".length());
				TreeMap chartOptionObject = ((TreeMap) chartOptionObjects.get(chartOptionPrefix));
				if (chartOptionObject == null) {
					chartOptionObject = new TreeMap();
					chartOptionObjects.put(chartOptionPrefix, chartOptionObject);
				}
				chartOptionObject.put(chartOptions[o], (quoteValue ? ("'" + this.escapeForJavaScript(chartOptionValue) + "'") : chartOptionValue));
			}
		}
		
		//	write option objects
		for (Iterator copit = chartOptionObjects.keySet().iterator(); copit.hasNext();) {
			String chartOptionPrefix = ((String) copit.next());
			bw.write("    " + chartOptionPrefix + ": {"); bw.newLine();
			TreeMap chartOptionObject = ((TreeMap) chartOptionObjects.get(chartOptionPrefix));
			for (Iterator coit = chartOptionObject.keySet().iterator(); coit.hasNext();) {
				String chartOption = ((String) coit.next());
				bw.write("      " + chartOption + ": " + ((String) chartOptionObject.get(chartOption)) + (coit.hasNext() ? "," : "")); bw.newLine();
			}
			bw.write("    },"); bw.newLine();
		}
		
		//	add title if not done yet and data value sum desired
		if (dataValueSum != -1) {
			bw.write("    title: 'n=" + dataValueSum + "',"); bw.newLine();
			dataValueSum = -1;
		}
		
		//	hide title if requested to
		if (hideLegend) {
			bw.write("    legend: 'none',"); bw.newLine();
		}
		
		bw.write("    doWeHaveDanglingCommas: 'No ;-)'"); bw.newLine();
	}
	
	private String escapeForJavaScript(String str) {
		if (str == null)
			return null;
		StringBuffer eStr = new StringBuffer();
		for (int c = 0; c < str.length(); c++) {
			char ch = str.charAt(c);
			if ((ch == '\'') || (ch == '\\'))
				eStr.append('\\');
			eStr.append(ch);
		}
		return eStr.toString();
	}
	
	private class ChartBuilderPageBuilder extends HtmlPageBuilder {
		private int optionFieldCount = 0;
		ChartBuilderPageBuilder(HtmlPageBuilderHost host, HttpServletRequest request, HttpServletResponse response) throws IOException {
			super(host, request, response);
		}
		protected boolean includeJavaScriptDomHelpers() {
			return true;
		}
		public void storeToken(String token, int treeDepth) throws IOException {
			if ((token.startsWith("<input ") || token.startsWith("<select ")) && (token.indexOf(" id=\"option\"") != -1)) {
				String tokenBeforeId = token.substring(0, token.indexOf(" id=\"option\""));
				String tokenAfterId = token.substring(token.indexOf(" id=\"option\"") + " id=\"option\"".length());
				token = (tokenBeforeId + " id=\"option" + this.optionFieldCount + "\"" + tokenAfterId);
				this.optionFieldCount++;
			}
			super.storeToken(token, treeDepth);
		}
		protected void include(String type, String tag) throws IOException {
			if ("includeBody".equals(type))
				this.includeFile("ChartBuilderForms.html");
			else if ("includeDcsFieldsAsSelectorOptions".equals(type))
				this.writeDcsFieldOptions();
			else super.include(type, tag);
		}
		protected String getPageTitle(String title) {
			return "Treatment Collection Statistics Chart Builder";
		}
		private void writeDcsFieldOptions() throws IOException {
			StatFieldGroup[] sfgs = getFieldSet().getFieldGroups();
			for (int g = 0; g < sfgs.length; g++) {
				this.writeLine("<optgroup label=\"" + sfgs[g].label + "\">");
				StatField[] sfs = sfgs[g].getFields();
				for (int f = 0; f < sfs.length; f++)
					this.writeLine("<option value=\"" + sfs[f].fullName + "\">" + sfs[f].label + "</option>");
				this.writeLine("</optgroup>");
			}
		}
		protected void writePageHeadExtensions() throws IOException {
			this.writeLine("<script type=\"text/javascript\">");
			
			this.writeLine("function buildChart(chartId) {");
			this.writeLine("  var chartUrl = ('" + this.request.getContextPath() + this.request.getServletPath() + "/chart.html?type=' + chartId + buildChartParameterString());");
			this.writeLine("  window.open(chartUrl, 'Chart Preview', '" + chartWindowParameters + "');");
			this.writeLine("}");
			
			StatFieldGroup[] sfgs = getFieldSet().getFieldGroups();
			this.writeLine("function appendFieldsAsOptions(selectorField) {");
			this.writeLine("  var optionGroup;");
			this.writeLine("  var option;");
			for (int g = 0; g < sfgs.length; g++) {
				StatField[] sfs = sfgs[g].getFields();
				this.writeLine("  optionGroup = newElement('optgroup', null, null, null);");
				this.writeLine("  optionGroup.label = '" + sfgs[g].label + "';");
				for (int f = 0; f < sfs.length; f++) {
					this.writeLine("  option = newElement('option', null, null, '" + sfs[f].label + "');");
					this.writeLine("  option.value = '" + sfs[f].fullName + "';");
					this.writeLine("  optionGroup.appendChild(option);");
				}
				this.writeLine("  selectorField.appendChild(optionGroup);");
			}
			this.writeLine("}");
			
			this.writeLine("</script>");
		}
	}
	
	private class ChartPreviewPageBuilder extends HtmlPageBuilder {
		ChartPreviewPageBuilder(HtmlPageBuilderHost host, HttpServletRequest request, HttpServletResponse response) throws IOException {
			super(host, request, response);
		}
		protected void include(String type, String tag) throws IOException {
			if ("includeBody".equals(type))
				this.includeBody();
			else super.include(type, tag);
		}
		private void includeBody() throws IOException {
			
			//	make sure we have a somewhat unique chart ID
			StringBuffer chartIdBuilder = new StringBuffer();
			for (int i = 0; i < 16; i++) {
				int r = ((int) (Math.random() * 52));
				if (r < 26)
					chartIdBuilder.append((char) ('a' + r));
				else chartIdBuilder.append((char) ('A' + (r - 26)));
			}
			
			//	generate page content
			String chartId = chartIdBuilder.toString();
			this.writeLine("<div id=\"chartDiv" + chartId + "\" style=\"width: 900px; height: 500px;\">");
			this.writeLine("  <script type=\"text/javascript\" src=\"https://www.google.com/jsapi\"></script>");
			this.writeLine("  <script type=\"text/javascript\" src=\"" + this.request.getContextPath() + this.request.getServletPath() + "/chart.js?" + this.request.getQueryString() + "&chartId=" + chartId + "&cacheControl=force\"></script>");
			this.writeLine("</div>");
			this.writeLine("<p>To embed the above chart in some other web page, simply copy below HTML code where the chart is intended to appear.</p>");
			this.writeLine("<pre>");
			this.writeLine("  &lt;div id=&quot;chartDiv" + chartId + "&quot; style=&quot;width: 900px; height: 500px;&quot;&gt;");
			this.writeLine("    &lt;script type=&quot;text/javascript&quot; src=&quot;https://www.google.com/jsapi&quot;&gt;&lt;/script&gt;");
			this.writeLine("    &lt;script type=&quot;text/javascript&quot; src=&quot;http://" + this.request.getServerName() + this.request.getContextPath() + this.request.getServletPath() + "/chart.js?" + this.getEscapedQueryString() + "&amp;chartId=" + chartId + "&quot;&gt;&lt;/script&gt;");
			this.writeLine("  &lt;/div&gt;");
			this.writeLine("</pre>");
		}
		private String getEscapedQueryString() {
			StringBuffer queryString = new StringBuffer();
			String rQueryString = this.request.getQueryString();
			for (int c = 0; c < rQueryString.length(); c++) {
				char ch = rQueryString.charAt(c);
				if (ch == '&')
					queryString.append("&amp;");
				else queryString.append(ch);
			}
			return queryString.toString();
		}
		protected String getPageTitle(String title) {
			return "Chart Preview";
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		//	TEST DATA TODO TEST HERE
		Settings chartTypeProperties = Settings.loadSettings(new File("E:/GoldenGATEv3.WebApp/WEB-INF/srsStatsData", "chartTypeProperties.cnfg"));
		TestRequest request = new TestRequest();
//		request.setProperty("emptyGroupLabelSubstitute", "UNKNOWN");
//		request.setProperty("group", "matCit.month");
//		request.setProperty("groupCutoff", "B");
//		request.setProperty("groupBuckets", "3;6;9");
//		request.setProperty("series", "matCit.elevation");
//		request.setProperty("seriesCutoff", "B");
//		request.setProperty("seriesBuckets", "0;1000;2000;3000;4000;5000;6000;7000;8000");
//		request.setProperty("truncateSeriesBuckets", "true");
//		request.setProperty("translateMonthNumbers", "yes");
		request.setProperty("group", "multiField");
		request.setProperty("field0", "matCit.specimenCount");
		request.setProperty("field0Aggregate", "sum");
		request.setProperty("field0", "matCit.specimenCountMale");
		request.setProperty("field0Aggregate", "sum");
		request.setProperty("field1", "matCit.specimenCountFemale");
		request.setProperty("field1Aggregate", "sum");
		request.setProperty("fieldSum", "matCit.specimenCount");
//		request.setProperty("group", "tax.familyEpithet");
//		request.setProperty("series", "multiField");
//		request.setProperty("fieldSum", "matCit.specimenCount");
//		request.setProperty("field0", "matCit.specimenCountMale");
//		request.setProperty("field1", "matCit.specimenCountFemale");
//		request.setProperty("field0Aggregate", "sum");
//		request.setProperty("field1Aggregate", "sum");
		request.setProperty("title", "Specimens by Quarter and Elevation");
		request.setProperty("addDataSumToTitle", "true");
		request.setProperty("type", "col");
		request.setProperty("chartId", "0815");
		
		//	testing super class against sub class here, but little other chance to get live data
		ServerConnection sc = ServerConnection.getServerConnection("http://plazi.cs.umb.edu/GgServer/proxy");
		final GoldenGateDcsClient dcsc = new GoldenGateDcsClient(sc, "TCS");
		GoldenGateDcsChartServlet ggdcs = new GoldenGateDcsChartServlet() {
			protected GoldenGateDcsClient getDcsClient() {
				return dcsc;
			}
		};
		
		//	get fields
		StatFieldSet fieldSet = dcsc.getFieldSet();
		Properties fieldLabels = new Properties();
		HashSet numericFields = new HashSet();
		fieldLabels.setProperty("DocCount", fieldSet.docCountLabel);
		StatFieldGroup[] fieldGroups = fieldSet.getFieldGroups();
		for (int g = 0; g < fieldGroups.length; g++) {
			StatField[] fields = fieldGroups[g].getFields();
			for (int f = 0; f < fields.length; f++) {
				fieldLabels.setProperty(fields[f].statColName, fields[f].label);
				if ("integer".equals(fields[f].dataType) || "integer".equals(fields[f].dataType))
					numericFields.add(fields[f].fullName);
			}
		}
		
		//	read chart parameters
		String chartId = request.getParameter("chartId");
		String chartType = request.getParameter("type");
		Settings chartProperties = chartTypeProperties.getSubset(chartType);
		String[] chartOptions = chartProperties.getSetting("chartOptionNames", "").trim().split("\\s+");
		Settings chartOptionDefaults = chartProperties.getSubset("chartOptionDefaults");
		
		//	collect query parameters
		StringVector outputFields = new StringVector();
		StringVector groupingFields = new StringVector();
		StringVector orderingFields = new StringVector();
		Properties fieldPredicates = new Properties();
		Properties fieldAggregates = new Properties();
		Properties aggregatePredicates = new Properties();
		
		//	get data series renaming (prefilled with field labels)
		Properties valueFieldAliases = new Properties();
		StatFieldGroup[] sfgs = fieldSet.getFieldGroups();
		for (int g = 0; g < sfgs.length; g++) {
			StatField[] sfs = sfgs[g].getFields();
			for (int f = 0; f < sfs.length; f++)
				valueFieldAliases.setProperty(sfs[f].fullName, sfs[f].label);
		}
		
		//	get value fields
		String valueField = request.getParameter("field");
		if ((valueField != null) && (valueField.length() == 0))
			valueField = null;
		String valueSumField = null;
		boolean gotMultiFieldInput = false;
		
		//	collect debug output
		StringWriter debugData = new StringWriter();
		
		//	get values for multi-field series
		if (valueField == null) {
			
			//	get fields and associated data
			for (int f = 0; true; f++) {
				String vField = request.getParameter("field" + f);
				if ((vField == null) || (vField.length() == 0))
					break;
				String vAggregate = request.getParameter("field" + f + "Aggregate");
				if ((vAggregate == null) || (vAggregate.length() == 0))
					break;
				outputFields.addElement(vField);
				fieldAggregates.setProperty(vField, vAggregate);
				
				gotMultiFieldInput = true;
				
				String vLabel = request.getParameter("field" + f + "Label");
				if ((vLabel != null) && (vLabel.length() != 0))
					valueFieldAliases.setProperty(vField, vLabel);
			}
			
			//	get sum field and label
			valueSumField = request.getParameter("fieldSum");
			if ((valueSumField != null) && (valueSumField.length() != 0)) {
				outputFields.addElement(valueSumField);
				fieldAggregates.setProperty(valueSumField, "sum"); // anything else makes no sense
				String valueSumLabel = request.getParameter("fieldSumLabel");
				if ((valueSumLabel != null) && (valueSumLabel.length() != 0))
					valueFieldAliases.setProperty(valueSumField, valueSumLabel);
			}
			else valueSumField = null;
//			
//			DO NOT DO THIS: multi-field offers custom series and group naming
//			//	switch to single field if only a single field given
//			if (outputFields.size() == 1) {
//				valueField = outputFields.get(0);
//				valueSumField = null;
//				gotMultiFieldInput = false;
//			}
		}
		
		//	get parameters for single-value or grouping based series
		else {
			
			//	add value field
			outputFields.addElement(valueField);
			String valueAggregate = request.getParameter("fieldAggregate");
			if ((valueAggregate != null) && (valueAggregate.length() != 0))
				fieldAggregates.setProperty(valueField, valueAggregate);
		}
		
		//	if we don't have any fields by now, we'll never have any ...
		if (outputFields.isEmpty()) {
//			response.sendError(HttpServletResponse.SC_BAD_REQUEST, "No data fields selected.");
			System.out.println("No data fields selected.");
			return;
		}
		
		//	read series data
		String seriesField = request.getParameter("series");
		String seriesOrder = null;
		if ((seriesField != null) && (seriesField.length() == 0))
			seriesField = null;
//		DO NOT DO THIS: multi-field offers custom series and group naming
//		if ("multiField".equals(seriesField) && (outputFields.size() == 1))
//			seriesField = null;
		if ((seriesField != null) && !"multiField".equals(seriesField)) {
			outputFields.addElement(seriesField);
			groupingFields.addElement(seriesField);
			orderingFields.addElement(seriesField);
			seriesOrder = request.getParameter("seriesOrder");
			if ((seriesOrder != null) && (seriesOrder.length() == 0))
				seriesOrder = null;
			String emptySeriesLabelSubstitute = request.getParameter("emptySeriesLabelSubstitute");
			if ((emptySeriesLabelSubstitute != null) && (emptySeriesLabelSubstitute.length() != 0))
				valueFieldAliases.setProperty("EmPtYsErIeSnAmE", emptySeriesLabelSubstitute);
		}
		
		//	get grouping field if applicable
		boolean isChartGroupable = "true".equals(chartProperties.getSetting("valuesGroupable", "true"));
		String groupField = null;
		String groupOrder = null;
		String artificialGroups = null;
		if (isChartGroupable) {
			groupField = request.getParameter("group");
			if ((groupField != null) && (groupField.length() == 0))
				groupField = null;
//			DO NOT DO THIS: multi-field offers custom series and group naming
//			if ("multiField".equals(groupField) && (outputFields.size() == 1))
//				groupField = null;
			if ((groupField != null) && !"multiField".equals(groupField)) {
				outputFields.addElement(groupField);
				groupingFields.addElement(groupField);
				orderingFields.addElement(groupField);
				groupOrder = request.getParameter("groupOrder");
				if ((groupOrder != null) && (groupOrder.length() == 0))
					groupOrder = null;
				String emptyGroupLabelSubstitute = request.getParameter("emptyGroupLabelSubstitute");
				if ((emptyGroupLabelSubstitute != null) && (emptyGroupLabelSubstitute.length() != 0))
					valueFieldAliases.setProperty("EmPtYgRoUpNaMe", emptyGroupLabelSubstitute);
				artificialGroups = request.getParameter("artificialGroups");
				if ((artificialGroups != null) && (artificialGroups.length() == 0))
					artificialGroups = null;
			}
		}
		
		//	setting series or group to 'multiField' if null, but respective other field given
		if (gotMultiFieldInput) {
			if ((seriesField == null) && !"multiField".equals(groupField))
				seriesField = "multiField";
			else if ((groupField == null) && !"multiField".equals(seriesField))
				groupField = "multiField";
		}
		
		//	decode query parameters
		for (Enumeration pne = request.getParameterNames(); pne.hasMoreElements();) {
			String pn = ((String) pne.nextElement());
			if (!pn.startsWith("FILTER_"))
				continue;
			String filterField = pn.substring("FILTER_".length());
			String filterValue = request.getParameter(pn);
			if ((filterValue == null) || (filterValue.length() == 0))
				continue;
			String filterAggregate = request.getParameter("AGGREGATE_" + filterField);
			if ((filterAggregate == null) || (filterAggregate.length() == 0))
				fieldPredicates.setProperty(filterField, filterValue);
			else {
				fieldAggregates.setProperty(filterField, filterAggregate);
				aggregatePredicates.setProperty(filterField, filterValue);
			}
		}
		
		//	fetch data from backing DCS
		DcStatistics stats = getDcsStats(dcsc, outputFields.toStringArray(), groupingFields.toStringArray(), orderingFields.toStringArray(), fieldPredicates, fieldAggregates, aggregatePredicates, valueSumField, groupField);
		stats.writeData(System.out);
		
		//	organize statistics data
		DcStatChartData chartData = new DcStatChartData(stats, seriesField, groupField, valueField);
		if (artificialGroups != null) {
			String[] artificialGroupLabels = artificialGroups.split("\\s*\\;\\s*");
			for (int g = 0; g < artificialGroupLabels.length; g++) {
				while (numericFields.contains(groupField) && artificialGroupLabels[g].startsWith("0"))
					artificialGroupLabels[g] = artificialGroupLabels[g].substring("0".length());
				chartData.addGroup(artificialGroupLabels[g], artificialGroupLabels[g], (seriesField != null));
			}
		}
		if ("multiField".equals(seriesField)) {
			for (int f = 0; f < outputFields.size(); f++) {
				String ofn = outputFields.get(f);
				if (!ofn.equals(groupField))
					chartData.addSeries(ofn, valueFieldAliases.getProperty(ofn));
			}
			for (int s = 0; s < stats.size(); s++) {
				StringTupel st = stats.get(s);
				String gn = ((groupField == null) ? null : st.getValue(groupField));
				if ("".equals(gn))
					gn = valueFieldAliases.getProperty("EmPtYgRoUpNaMe", gn);
				for (int f = 0; f < outputFields.size(); f++) {
					String ofn = outputFields.get(f);
					if (!ofn.equals(groupField))
						chartData.addData(st, ofn, gn, ofn);
				}
			}
		}
		else if ("multiField".equals(groupField)) {
			for (int f = 0; f < outputFields.size(); f++) {
				String ofn = outputFields.get(f);
				if (ofn.equals(seriesField))
					continue;
				chartData.addGroup(ofn, valueFieldAliases.getProperty(ofn), (seriesField != null));
			}
			for (int s = 0; s < stats.size(); s++) {
				StringTupel st = stats.get(s);
				String sn = ((seriesField == null) ? null : st.getValue(seriesField));
				if ("".equals(sn))
					sn = valueFieldAliases.getProperty("EmPtYsErIeSnAmE", sn);
				for (int f = 0; f < outputFields.size(); f++) {
					String ofn = outputFields.get(f);
					if (!ofn.equals(seriesField))
						chartData.addData(st, sn, ofn, ofn);
				}
			}
		}
		else for (int s = 0; s < stats.size(); s++) {
			StringTupel st = stats.get(s);
			String sn = ((seriesField == null) ? null : st.getValue(seriesField));
			if ("".equals(sn))
				sn = valueFieldAliases.getProperty("EmPtYsErIeSnAmE", sn);
			String gn = ((groupField == null) ? null : st.getValue(groupField));
			if ("".equals(gn))
				gn = valueFieldAliases.getProperty("EmPtYgRoUpNaMe", gn);
			chartData.addData(st, sn, gn, valueField);
		}
		
		//	sort series and groups (have to do this before the cutoffs, as 'Other' has to stay at end)
		if (seriesOrder != null)
			chartData.orderSeries(seriesOrder, numericFields.contains(seriesField));
		if (groupOrder != null)
			chartData.orderGroups(groupOrder, numericFields.contains(groupField));
		
		//	handle month names (needs to be done before bucketization)
		boolean translateMonthNumbers = "yes".equals(request.getParameter("translateMonthNumbers"));
		if (translateMonthNumbers) {
			if ((seriesField != null) && seriesField.toLowerCase().endsWith("month"))
				for (Iterator sit = chartData.seriesByName.keySet().iterator(); sit.hasNext();) {
					DcStatSeriesHead dssh = chartData.getSeriesHead((String) sit.next());
					dssh.label = numbersToMonthNames.getProperty(dssh.name, dssh.label);
				}
			if ((groupField != null) && groupField.toLowerCase().endsWith("month"))
				for (Iterator git = chartData.groupsByName.keySet().iterator(); git.hasNext();) {
					DcStatGroup dsg = chartData.getGroup(((String) git.next()), (seriesField != null));
					dsg.label = numbersToMonthNames.getProperty(dsg.name, dsg.label);
				}
		}
		
		//	enforce cutoffs, or bucketize
		if ((seriesField != null) && !"multiField".equals(seriesField)) {
			String seriesCutoff = request.getParameter("seriesCutoff");
			if ((seriesCutoff == null) || (seriesCutoff.length() == 0) || "0".equals(seriesCutoff)) {}
			else if ("B".equals(seriesCutoff)) {
				String buckets = request.getParameter("seriesBuckets");
				boolean truncate = "true".equals(request.getParameter("truncateSeriesBuckets"));
				if ((buckets != null) && (buckets.length() != 0))
					chartData.bucketizeSeries(buckets, truncate, numericFields.contains(seriesField), (translateMonthNumbers && seriesField.toLowerCase().endsWith("month")));
			}
			else try {
				chartData.pruneSeries(Integer.parseInt(seriesCutoff));
			} catch (NumberFormatException nfe) {}
		}
		if ((groupField != null) && !"multiField".equals(groupField)) {
			String groupCutoff = request.getParameter("groupCutoff");
			if ((groupCutoff == null) || (groupCutoff.length() == 0) || "0".equals(groupCutoff)) {}
			else if ("B".equals(groupCutoff)) {
				String buckets = request.getParameter("groupBuckets");
				boolean truncate = "true".equals(request.getParameter("truncateGroupBuckets"));
				if ((buckets != null) && (buckets.length() != 0))
					chartData.bucketizeGroups(buckets, truncate, numericFields.contains(groupField), (translateMonthNumbers && groupField.toLowerCase().endsWith("month")));
			}
			else try {
				chartData.pruneGroups(Integer.parseInt(groupCutoff));
			} catch (NumberFormatException nfe) {}
		}
		
		//	get customization parameters
		boolean addDataSumToTitle = "true".equals(request.getParameter("addDataSumToTitle"));
		
		//	write raw stats data as comment (for debug purposes)
		debugData.write("/*\r\n");
		stats.writeData(debugData);
		debugData.write("*/\r\n");
		
		//	generate rendering JavaScript
//		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"));
		bw.write(debugData.toString());
		
		//	write chart data array
		bw.write("function drawChart" + chartId + "() {"); bw.newLine();
		bw.write("  var chartData = google.visualization.arrayToDataTable(["); bw.newLine();
		chartData.writeJsonArrayContent(bw, ("number".equals(chartProperties.getSetting("groupFieldType", "string")) && numericFields.contains(groupField) && !"B".equals(request.getParameter("groupCutoff")) && (!groupField.toLowerCase().endsWith("month") || !translateMonthNumbers)), isChartGroupable, ggdcs);
		bw.write("  ]);"); bw.newLine();
		
		//	write chart options
		bw.write("  var chartOptions = {"); bw.newLine();
		writeChartOptions(ggdcs, bw, chartOptions, request, chartOptionDefaults, chartData.value, addDataSumToTitle);
		bw.write("  };"); bw.newLine();
		bw.write("  var chart = new google.visualization." + chartProperties.getSetting("chartClassName") + "(document.getElementById('chartDiv" + chartId + "'));"); bw.newLine();
		bw.write("  chart.draw(chartData, chartOptions);"); bw.newLine();
		bw.write("}"); bw.newLine();
		
		//	write rendering function calls
		bw.write("google.load('visualization', '1', {'packages': ['" + chartProperties.getSetting("chartPackage") + "']});"); bw.newLine();
		bw.write("google.setOnLoadCallback(drawChart" + chartId + ");"); bw.newLine();
		
		//	send data
		bw.flush();
	}
	
	private static DcStatistics getDcsStats(GoldenGateDcsClient dcsClient, String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String valueSumField, String groupingField) throws IOException {
		
		//	get raw data from backing DCS
		DcStatistics stats = dcsClient.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates);
		
		//	normalize statistics field names (no dots allowed in CSV column names in backend)
		for (int f = 0; f < outputFields.length; f++) {
			String fn = outputFields[f];
			if (fn.indexOf('.') == -1)
				continue;
			StringBuffer sfnBuilder = new StringBuffer();
			boolean toUpperCase = true;
			for (int c = 0; c < fn.length(); c++) {
				char ch = fn.charAt(c);
				if (ch == '.')
					toUpperCase = true;
				else if (toUpperCase) {
					sfnBuilder.append(Character.toUpperCase(ch));
					toUpperCase = false;
				}
				else sfnBuilder.append(ch);
			}
			String sfn = sfnBuilder.toString();
			stats.renameKey(sfn, fn);
		}
		
		//	subtract individual values from sum
		if (valueSumField != null) try {
			String[] fields = stats.getFields();
			for (int t = 0; t < stats.size(); t++) {
				StringTupel st = stats.get(t);
				int sum = Integer.parseInt(st.getValue(valueSumField, "0"));
				if (sum == 0)
					continue;
				for (int f = 0; f < fields.length; f++)
					if (!"DocCount".equals(fields[f]) && !valueSumField.equals(fields[f]) && ((groupingField == null) || !groupingField.equals(fields[f]))) try {
						sum -= Integer.parseInt(st.getValue(fields[f], "0"));
					} catch (NumberFormatException nfe) {}
				st.setValue(valueSumField, ((sum < 1) ? "0" : ("" + sum)));
			}
		} catch (NumberFormatException nfe) {}
		
		//	finally ...
		return stats;
	}
	
	private static void writeChartOptions(GoldenGateDcsChartServlet ggdcs, BufferedWriter bw, String[] chartOptions, TestRequest request, Settings chartOptionDefaults, int dataValueSum, boolean addDataValueSumToTitle) throws IOException {
		
		//	cut data point sum if not desired
		if (!addDataValueSumToTitle)
			dataValueSum = -1;
		
		//	set up collecting sub objects
		TreeMap chartOptionObjects = new TreeMap();
		
		//	go through options
		for (int o = 0; o < chartOptions.length; o++) {
			
			//	check if we have a quoted (string valued) option value
			boolean quoteValue = false;
			if (chartOptions[o].startsWith("'")) {
				chartOptions[o] = chartOptions[o].substring(1);
				quoteValue = true;
			}
			
			//	get option value
			String chartOptionValue = request.getParameter(chartOptions[o]);
			if ((chartOptionValue == null) || (chartOptionValue.length() == 0))
				chartOptionValue = chartOptionDefaults.getSetting(chartOptions[o]);
			if ((chartOptionValue == null) || (chartOptionValue.length() == 0))
				continue;
			
			//	we have a plain option value
			if (chartOptions[o].indexOf('.') == -1) {
				if ("title".equals(chartOptions[o]) && (dataValueSum != -1)) {
					chartOptionValue = (chartOptionValue + " (n=" + dataValueSum + ")");
					dataValueSum = -1;
				}
				bw.write("    " + chartOptions[o] + ": " + (quoteValue ? ("'" + ggdcs.escapeForJavaScript(chartOptionValue) + "'") : chartOptionValue) + ","); bw.newLine();
			}
			
			//	we have an attribute of an option object
			else {
				String chartOptionPrefix = chartOptions[o].substring(0, chartOptions[o].indexOf('.'));
				chartOptions[o] = chartOptions[o].substring(chartOptions[o].indexOf('.') + ".".length());
				TreeMap chartOptionObject = ((TreeMap) chartOptionObjects.get(chartOptionPrefix));
				if (chartOptionObject == null) {
					chartOptionObject = new TreeMap();
					chartOptionObjects.put(chartOptionPrefix, chartOptionObject);
				}
				chartOptionObject.put(chartOptions[o], (quoteValue ? ("'" + ggdcs.escapeForJavaScript(chartOptionValue) + "'") : chartOptionValue));
			}
		}
		
		//	write option objects
		for (Iterator copit = chartOptionObjects.keySet().iterator(); copit.hasNext();) {
			String chartOptionPrefix = ((String) copit.next());
			bw.write("    " + chartOptionPrefix + ": {"); bw.newLine();
			TreeMap chartOptionObject = ((TreeMap) chartOptionObjects.get(chartOptionPrefix));
			for (Iterator coit = chartOptionObject.keySet().iterator(); coit.hasNext();) {
				String chartOption = ((String) coit.next());
				bw.write("      " + chartOption + ": " + ((String) chartOptionObject.get(chartOption)) + (coit.hasNext() ? "," : "")); bw.newLine();
			}
			bw.write("    },"); bw.newLine();
		}
		
		//	add title if not done yet and data value sum desired
		if (dataValueSum != -1) {
			bw.write("    title: 'n=" + dataValueSum + "',"); bw.newLine();
			dataValueSum = -1;
		}
		bw.write("    doWeHaveDanglingCommas: 'No ;-)'"); bw.newLine();
	}
	
	private static class TestRequest extends Properties {
		String getParameter(String name) {
			return this.getProperty(name);
		}
		public Enumeration getParameterNames() {
			return this.keys();
		}
	}
	
	private static class DcStatChartData {
		final DcStatistics sourceData;
		
		String seriesField;
		String groupField;
		
		boolean translateMonthNumbers;
		
		LinkedHashMap seriesByName = new LinkedHashMap();
		LinkedHashMap groupsByName = new LinkedHashMap();
		String valueField;
		int value = 0;
		
		DcStatChartData(DcStatistics stats, String seriesField, String groupField, String valueField) {
			this.sourceData = stats;
			this.seriesField = seriesField;
			this.groupField = groupField;
			this.valueField = valueField;
		}
		void addSeries(String name, String label) {
			DcStatSeriesHead dssh = this.getSeriesHead(name);
			if (label != null)
				dssh.label = label;
		}
		DcStatSeriesHead getSeriesHead(String name) {
			DcStatSeriesHead dssh = ((DcStatSeriesHead) this.seriesByName.get(name));
			if (dssh == null) {
				dssh = new DcStatSeriesHead(name);
				this.seriesByName.put(name, dssh);
			}
			return dssh;
		}
		void addGroup(String name, String label, boolean isMultiSeries) {
			DcStatGroup dsg = this.getGroup(name, isMultiSeries);
			if (label != null)
				dsg.label = label;
		}
		DcStatGroup getGroup(String name, boolean isMultiSeries) {
			DcStatGroup dsg = ((DcStatGroup) this.groupsByName.get(name));
			if (dsg == null) {
				dsg = new DcStatGroup(name, isMultiSeries);
				this.groupsByName.put(name, dsg);
			}
			return dsg;
		}
		void addData(StringTupel st, String seriesName, String groupName, String valueField) {
			try {
				int value = Integer.parseInt(st.getValue(valueField, "0"));
				this.value += value;
				if (seriesName != null)
					this.getSeriesHead(seriesName).add(value);
				if (groupName != null)
					this.getGroup(groupName, (seriesName != null)).add(value, seriesName);
			} catch (NumberFormatException nfe) {}
		}
		void pruneGroups(int cutoff) {
			if (this.groupsByName.size() <= cutoff)
				return;
			TreeSet groups = new TreeSet(new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatGroup dsg1 = ((DcStatGroup) obj1);
					DcStatGroup dsg2 = ((DcStatGroup) obj2);
					return ((dsg1.value == dsg2.value) ? dsg1.label.compareTo(dsg2.label) : (dsg2.value - dsg1.value));
				}
			});
			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();)
				groups.add(this.getGroup(((String) git.next()), (this.seriesByName.size() != 0)));
			DcStatGroup oDsg = new DcStatGroup("other", (this.seriesByName.size() != 0));
			oDsg.label = ("Other (" + (this.groupsByName.size() - cutoff) + ")");
			while (groups.size() > cutoff) {
				DcStatGroup dsg = ((DcStatGroup) groups.last());
				this.groupsByName.remove(dsg.name);
				groups.remove(dsg);
				oDsg.addAll(dsg);
			}
			this.groupsByName.put(oDsg.name, oDsg);
		}
		void pruneSeries(int cutoff) {
			if (this.seriesByName.size() <= cutoff)
				return;
			TreeSet series = new TreeSet(new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
					DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
					return ((dssh1.value == dssh2.value) ? dssh1.label.compareTo(dssh2.label) : (dssh2.value - dssh1.value));
				}
			});
			for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();)
				series.add(this.getSeriesHead((String) sit.next()));
			DcStatSeriesHead oDssh = new DcStatSeriesHead("other");
			oDssh.label = ("Other (" + (this.seriesByName.size() - cutoff) + ")");
			while (series.size() > cutoff) {
				DcStatSeriesHead dssh = ((DcStatSeriesHead) series.last());
				this.seriesByName.remove(dssh.name);
				series.remove(dssh);
				oDssh.value += dssh.value;
				for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
					DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
					dsg.renameSeries(dssh.name, oDssh.name);
				}
			}
			this.seriesByName.put(oDssh.name, oDssh);
		}
		void bucketizeGroups(String bucketBoundaries, boolean truncate, boolean numeric, boolean monthNames) {
			
			//	parse, de-duplicate, and validate bucket boundaries
			String[] bucketBoundaryStrings = bucketBoundaries.trim().split("\\s*\\;\\s*");
			ArrayList bucketBoundaryList = new ArrayList();
			for (int b = 0; b < bucketBoundaryStrings.length; b++) {
				if (bucketBoundaryList.contains(bucketBoundaryStrings[b]))
					continue;
				if (numeric) try {
					Double.parseDouble(bucketBoundaryStrings[b].replaceAll("\\,", "."));
				} catch (NumberFormatException nfe) {continue;}
				if (bucketBoundaryStrings[b].length() != 0)
					bucketBoundaryList.add(bucketBoundaryStrings[b]);
			}
			if (bucketBoundaryList.size() < 2)
				return;
			
			//	get bucket order
			Comparator bucketOrder = this.getLabelOrder(numeric);
			
			//	assess whether to sort ascending or descending
			int upStepCount = 0;
			int downStepCount = 0;
			for (int b = 1; b < bucketBoundaryList.size(); b++) {
				int c = bucketOrder.compare(bucketBoundaryList.get(b-1), bucketBoundaryList.get(b));
				if (c < 0)
					upStepCount++;
				else if (0 < c)
					downStepCount++;
			}
			boolean bucketBoundariesDescending = (downStepCount > upStepCount);
			
			//	order bucket boundaries
			Collections.sort(bucketBoundaryList, bucketOrder);
			if (bucketBoundariesDescending)
				Collections.reverse(bucketBoundaryList);
			
			//	create buckets
			DcStatGroup[] buckets = new DcStatGroup[bucketBoundaryList.size() + 1];
			for (int b = 0; b < bucketBoundaryList.size(); b++)
				buckets[b] = new DcStatGroup(((String) bucketBoundaryList.get(b)), (this.seriesByName.size() != 0));
			if (monthNames)
				buckets[bucketBoundaryList.size()] = new DcStatGroup((bucketBoundariesDescending ? "1" : "12"), (this.seriesByName.size() != 0));
			else if (numeric)
				buckets[bucketBoundaryList.size()] = new DcStatGroup(("" + (bucketBoundariesDescending ? Integer.MIN_VALUE : Integer.MAX_VALUE)), (this.seriesByName.size() != 0));
			else buckets[bucketBoundaryList.size()] = new DcStatGroup((bucketBoundariesDescending ? "" : "\uFFFF"), (this.seriesByName.size() != 0));
			
			//	initialize numeric left boundaries in labels
			if (numeric)
				for (int b = 1; b < buckets.length; b++) {
					buckets[b].label = ("" + (Double.parseDouble(buckets[b-1].name) + (bucketBoundariesDescending ? -1 : 1)));
					buckets[b].label = buckets[b].label.replaceAll("\\.[0]+\\z", "");
				}
			
			//	keep track if lowmost bucket has value less than boundary
			boolean lowmostBucketIsLessEqual = false;
			
			//	distribute data into buckets, collecting left boundary in label
			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
				DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
				for (int b = 0; b < buckets.length; b++) {
					int c = bucketOrder.compare(dsg.name, buckets[b].name);
					if (bucketBoundariesDescending && (0 <= c)) {
						buckets[b].addAll(dsg);
						if (0 < bucketOrder.compare(dsg.name, buckets[b].label)) {
							buckets[b].label = dsg.name;
							if ((b + 1) == buckets.length)
								lowmostBucketIsLessEqual = true;
						}
						break;
					}
					else if (!bucketBoundariesDescending && (c <= 0)) {
						buckets[b].addAll(dsg);
						if (bucketOrder.compare(dsg.name, buckets[b].label) < 0)
							buckets[b].label = dsg.name;
						if ((b == 0) && (c < 0))
							lowmostBucketIsLessEqual = true;
						break;
					}
				}
			}
			
			//	create labels
			if (monthNames) {
				if (Integer.parseInt(buckets[0].label) < 1)
					buckets[0].label = "1";
				else if (Integer.parseInt(buckets[0].label) > 12)
					buckets[0].label = "12";
				for (int b = 0; b < buckets.length; b++) {
					if (buckets[b].label.equals(buckets[b].name))
						buckets[b].label = numbersToMonthNames.getProperty(buckets[b].name, buckets[b].name);
					else buckets[b].label = (numbersToMonthNames.getProperty(buckets[b].label, buckets[b].label) + "-" + numbersToMonthNames.getProperty(buckets[b].name, buckets[b].name));
				}
			}
			else {
				buckets[0].label = ((bucketBoundariesDescending ? "\u2265" : (lowmostBucketIsLessEqual ? "\u2264" : "")) + buckets[0].name);
				for (int b = 1; b < (buckets.length - 1); b++) {
					if (!buckets[b].label.equals(buckets[b].name))
						buckets[b].label = (buckets[b].label + "-" + buckets[b].name);
				}
				buckets[buckets.length-1].label = ((bucketBoundariesDescending ? (lowmostBucketIsLessEqual ? "\u2264" : "") : "\u2265") + buckets[buckets.length-1].label);
			}
			
			//	leading and tailing set empty buckets to null
			if (truncate) {
				for (int b = 0; b < buckets.length; b++) {
					if (buckets[b] == null)
						continue;
					else if (buckets[b].value == 0)
						buckets[b] = null;
					else break;
				}
				for (int b = (buckets.length-1); b >= 0; b--) {
					if (buckets[b] == null)
						continue;
					else if (buckets[b].value == 0)
						buckets[b] = null;
					else break;
				}
			}
			
			//	replace plain groups with bucktized ones
			this.groupsByName.clear();
			for (int b = 0; b < buckets.length; b++) {
				if (buckets[b] != null)
					this.groupsByName.put(buckets[b].name, buckets[b]);
			}
		}
		void bucketizeSeries(String bucketBoundaries, boolean truncate, boolean numeric, boolean monthNames) {
			
			//	parse, de-duplicate, and validate bucket boundaries
			String[] bucketBoundaryStrings = bucketBoundaries.trim().split("\\s*\\;\\s*");
			ArrayList bucketBoundaryList = new ArrayList();
			for (int b = 0; b < bucketBoundaryStrings.length; b++) {
				if (bucketBoundaryList.contains(bucketBoundaryStrings[b]))
					continue;
				if (numeric) try {
					Double.parseDouble(bucketBoundaryStrings[b].replaceAll("\\,", "."));
				} catch (NumberFormatException nfe) {continue;}
				if (bucketBoundaryStrings[b].length() != 0)
					bucketBoundaryList.add(bucketBoundaryStrings[b]);
			}
			if (bucketBoundaryList.size() < 2)
				return;
			
			//	get bucket order
			Comparator bucketOrder = this.getLabelOrder(numeric);
			
			//	assess whether to sort ascending or descending
			int upStepCount = 0;
			int downStepCount = 0;
			for (int b = 1; b < bucketBoundaryList.size(); b++) {
				int c = bucketOrder.compare(bucketBoundaryList.get(b-1), bucketBoundaryList.get(b));
				if (c < 0)
					upStepCount++;
				else if (0 < c)
					downStepCount++;
			}
			boolean bucketBoundariesDescending = (downStepCount > upStepCount);
			
			//	order bucket boundaries
			Collections.sort(bucketBoundaryList, bucketOrder);
			if (bucketBoundariesDescending)
				Collections.reverse(bucketBoundaryList);
			
			//	create buckets
			DcStatSeriesHead[] buckets = new DcStatSeriesHead[bucketBoundaryList.size() + 1];
			for (int b = 0; b < bucketBoundaryList.size(); b++)
				buckets[b] = new DcStatSeriesHead((String) bucketBoundaryList.get(b));
			if (monthNames)
				buckets[bucketBoundaryList.size()] = new DcStatSeriesHead(bucketBoundariesDescending ? "1" : "12");
			else if (numeric)
				buckets[bucketBoundaryList.size()] = new DcStatSeriesHead("" + (bucketBoundariesDescending ? Integer.MIN_VALUE : Integer.MAX_VALUE));
			else buckets[bucketBoundaryList.size()] = new DcStatSeriesHead(bucketBoundariesDescending ? "" : "\uFFFF");
			
			//	initialize numeric left boundaries labels
			if (numeric)
				for (int b = 1; b < buckets.length; b++) {
					buckets[b].label = ("" + (Double.parseDouble(buckets[b-1].name) + (bucketBoundariesDescending ? -1 : 1)));
					buckets[b].label = buckets[b].label.replaceAll("\\.[0]+\\z", "");
				}
			
			//	keep track if lowmost bucket has value less than boundary
			boolean lowmostBucketIsLessEqual = false;
			
			//	distribute data into buckets, collecting left boundary in label
			for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
				DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
				for (int b = 0; b < buckets.length; b++) {
					int c = bucketOrder.compare(dssh.name, buckets[b].name);
					if (bucketBoundariesDescending && (0 <= c)) {
						buckets[b].value += dssh.value;
						for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
							DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
							dsg.renameSeries(dssh.name, buckets[b].name);
							if ((b + 1) == buckets.length)
								lowmostBucketIsLessEqual = true;
						}
						if (0 < bucketOrder.compare(dssh.name, buckets[b].label))
							buckets[b].label = dssh.name;
						break;
					}
					else if (!bucketBoundariesDescending && (c <= 0)) {
						buckets[b].value += dssh.value;
						for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
							DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
							dsg.renameSeries(dssh.name, buckets[b].name);
						}
						if (bucketOrder.compare(dssh.name, buckets[b].label) < 0)
							buckets[b].label = dssh.name;
						if ((b == 0) && (c < 0))
							lowmostBucketIsLessEqual = true;
						break;
					}
				}
			}
			
			//	create labels
			if (monthNames) {
				if (Integer.parseInt(buckets[0].label) < 1)
					buckets[0].label = "1";
				else if (Integer.parseInt(buckets[0].label) > 12)
					buckets[0].label = "12";
				for (int b = 0; b < buckets.length; b++) {
					if (buckets[b].label.equals(buckets[b].name))
						buckets[b].label = numbersToMonthNames.getProperty(buckets[b].name, buckets[b].name);
					else buckets[b].label = (numbersToMonthNames.getProperty(buckets[b].label, buckets[b].label) + "-" + numbersToMonthNames.getProperty(buckets[b].name, buckets[b].name));
				}
			}
			else {
				buckets[0].label = ((bucketBoundariesDescending ? "\u2265" : (lowmostBucketIsLessEqual ? "\u2264" : "")) + buckets[0].name);
				for (int b = 1; b < (buckets.length - 1); b++) {
					if (!buckets[b].label.equals(buckets[b].name))
						buckets[b].label = (buckets[b].label + "-" + buckets[b].name);
				}
				buckets[buckets.length-1].label = ((bucketBoundariesDescending ? (lowmostBucketIsLessEqual ? "\u2264" : "") : "\u2265") + buckets[buckets.length-1].label);
			}
			
			//	leading and tailing set empty buckets to null
			if (truncate) {
				for (int b = 0; b < buckets.length; b++) {
					if (buckets[b] == null)
						continue;
					else if (buckets[b].value == 0)
						buckets[b] = null;
					else break;
				}
				for (int b = (buckets.length-1); b >= 0; b--) {
					if (buckets[b] == null)
						continue;
					else if (buckets[b].value == 0)
						buckets[b] = null;
					else break;
				}
			}
			
			//	replace plain series with bucktized ones
			this.seriesByName.clear();
			for (int b = 0; b < buckets.length; b++) {
				if (buckets[b] != null)
					this.seriesByName.put(buckets[b].name, buckets[b]);
			}
		}
		void orderGroups(String groupOrder, boolean namesNumeric) {
			final Comparator labelOrder = this.getLabelOrder(namesNumeric);
			Comparator order;
			if ("label".equals(groupOrder))
				order = new Comparator() {
					public int compare(Object obj1, Object obj2) {
						DcStatGroup dsg1 = ((DcStatGroup) obj1);
						DcStatGroup dsg2 = ((DcStatGroup) obj2);
						return labelOrder.compare(dsg1.label, dsg2.label);
					}
				};
			else if ("-label".equals(groupOrder))
				order = new Comparator() {
					public int compare(Object obj1, Object obj2) {
						DcStatGroup dsg1 = ((DcStatGroup) obj1);
						DcStatGroup dsg2 = ((DcStatGroup) obj2);
						return labelOrder.compare(dsg2.label, dsg1.label);
					}
				};
			else if ("value".equals(groupOrder))
				order = new Comparator() {
					public int compare(Object obj1, Object obj2) {
						DcStatGroup dsg1 = ((DcStatGroup) obj1);
						DcStatGroup dsg2 = ((DcStatGroup) obj2);
						return ((dsg1.value == dsg2.value) ? labelOrder.compare(dsg1.label, dsg2.label) : (dsg1.value - dsg2.value));
					}
				};
			else if ("-value".equals(groupOrder))
				order = new Comparator() {
					public int compare(Object obj1, Object obj2) {
						DcStatGroup dsg1 = ((DcStatGroup) obj1);
						DcStatGroup dsg2 = ((DcStatGroup) obj2);
						return ((dsg1.value == dsg2.value) ? labelOrder.compare(dsg1.label, dsg2.label) : (dsg2.value - dsg1.value));
					}
				};
			else return;
			TreeSet groups = new TreeSet(order);
			groups.addAll(this.groupsByName.values());
			this.groupsByName.clear();
			for (Iterator git = groups.iterator(); git.hasNext();) {
				DcStatGroup dsg = ((DcStatGroup) git.next());
				this.groupsByName.put(dsg.name, dsg);
			}
		}
		void orderSeries(String seriesOrder, boolean namesNumeric) {
			final Comparator labelOrder = this.getLabelOrder(namesNumeric);
			Comparator order;
			if ("label".equals(seriesOrder))
				order = new Comparator() {
					public int compare(Object obj1, Object obj2) {
						DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
						DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
						return labelOrder.compare(dssh1.label, dssh2.label);
					}
				};
			else if ("-label".equals(seriesOrder))
				order = new Comparator() {
					public int compare(Object obj1, Object obj2) {
						DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
						DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
						return labelOrder.compare(dssh2.label, dssh1.label);
					}
				};
			else if ("value".equals(seriesOrder))
				order = new Comparator() {
					public int compare(Object obj1, Object obj2) {
						DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
						DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
						return ((dssh1.value == dssh2.value) ? labelOrder.compare(dssh1.label, dssh2.label) : (dssh1.value - dssh2.value));
					}
				};
			else if ("-value".equals(seriesOrder))
				order = new Comparator() {
					public int compare(Object obj1, Object obj2) {
						DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
						DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
						return ((dssh1.value == dssh2.value) ? labelOrder.compare(dssh1.label, dssh2.label) : (dssh2.value - dssh1.value));
					}
				};
			else return;
			TreeSet series = new TreeSet(order);
			series.addAll(this.seriesByName.values());
			this.seriesByName.clear();
			for (Iterator sit = series.iterator(); sit.hasNext();) {
				DcStatSeriesHead dssh = ((DcStatSeriesHead) sit.next());
				this.seriesByName.put(dssh.name, dssh);
			}
		}
		private Comparator getLabelOrder(boolean namesNumeric) {
			if (namesNumeric) return new Comparator() {
				public int compare(Object obj1, Object obj2) {
					String l1 = ((String) obj1).replaceAll("\\,", ".");
					String l2 = ((String) obj2).replaceAll("\\,", ".");
					return Double.compare(Double.parseDouble(l1), Double.parseDouble(l2));
				}
			};
			else return new Comparator() {
				public int compare(Object obj1, Object obj2) {
					return ((String) obj1).compareTo((String) obj2);
				}
			};
		}
		void writeJsonArrayContent(BufferedWriter bw, boolean groupLabelsNumeric, boolean isChartGroupable, GoldenGateDcsChartServlet ggdcs) throws IOException {
			if (this.seriesByName.isEmpty() && this.groupsByName.isEmpty()) {
				bw.write("    ['" + ggdcs.getFieldLabel("Data") + "', '" + ggdcs.getFieldLabel(this.valueField) + "'],"); bw.newLine();
//				System.out.println("Series\tValue");
				bw.write("    ['" + ggdcs.getFieldLabel(this.valueField) + "', " + this.value + "]"); bw.newLine();
//				System.out.println(this.valueField + "\t" + this.value);
			}
			else if (this.seriesByName.isEmpty()) {
				bw.write("    ['" + ggdcs.getFieldLabel("Group") + "', '" + ggdcs.getFieldLabel((this.valueField == null) ? "Value" : this.valueField) + "'],"); bw.newLine();
//				System.out.println("Group\tValue");
				for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
					DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
					if (groupLabelsNumeric) try {
						Double.parseDouble(dsg.label);
						bw.write("    [" + dsg.label + "");
					} catch (NumberFormatException nfe) {continue;}
					else bw.write("    ['" + ggdcs.escapeForJavaScript(dsg.label) + "'");
					bw.write(", " + dsg.value + "]" + (git.hasNext() ? "," : "")); bw.newLine();
//					System.out.println(dsg.label + "\t" + dsg.value);
				}
			}
			else if (this.groupsByName.isEmpty()) {
				if (isChartGroupable) {
					bw.write("    ['" + ggdcs.escapeForJavaScript(ggdcs.getFieldLabel("Series")) + "'");
//					System.out.print("Series");
					for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
						DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
						bw.write(", '" + ggdcs.escapeForJavaScript(dssh.label) + "'");
//						System.out.print("\t" + dssh.label);
					}
					bw.write("],");
					bw.newLine();
					bw.write("    [''");
//					System.out.print("");
					for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
						DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
						bw.write(", " + dssh.value + "");
//						System.out.print("\t" + dssh.value);
					}
					bw.write("]");
					bw.newLine();
				}
				else {
					bw.write("    ['" + ggdcs.getFieldLabel("Series") + "', '" + ggdcs.getFieldLabel((this.valueField == null) ? "Value" : this.valueField) + "'],"); bw.newLine();
//					System.out.println("Series\tValue");
					for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
						DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
						bw.write("    ['" + ggdcs.escapeForJavaScript(dssh.label) + "', " + dssh.value + "]" + (sit.hasNext() ? "," : "")); bw.newLine();
//						System.out.println(dssh.label + "\t" + dssh.value);
					}
				}
			}
			else {
				bw.write("    ['" + ggdcs.escapeForJavaScript(ggdcs.getFieldLabel("Data")) + "'");
//				System.out.print("Series");
				for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
					DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
					bw.write(", '" + ggdcs.escapeForJavaScript(dssh.label) + "'");
//					System.out.print("\t" + dssh.label);
				}
				bw.write("],");
				bw.newLine();
//				System.out.println();
				for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
					DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
					if (groupLabelsNumeric) try {
						Double.parseDouble(dsg.label);
						bw.write("    [" + dsg.label + "");
					} catch (NumberFormatException nfe) {continue;}
					else bw.write("    ['" + ggdcs.escapeForJavaScript(dsg.label) + "'");
//					System.out.print(dsg.label);
					for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
						DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
						bw.write(", " + dsg.seriesValues.getCount(dssh.name) + "");
//						System.out.print("\t" + dsg.seriesValues.getCount(dssh.name));
					}
					bw.write("]" + (git.hasNext() ? "," : ""));
					bw.newLine();
//					System.out.println();
				}
			}
		}
//		void printData() {
//			if (this.seriesByName.isEmpty() && this.groupsByName.isEmpty()) {
//				System.out.println("Series\tValue");
//				System.out.println(this.valueField + "\t" + this.value);
//			}
//			else if (this.seriesByName.isEmpty()) {
//				System.out.println("Group\tValue");
//				for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
//					DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
//					System.out.println(dsg.label + "\t" + dsg.value);
//				}
//			}
//			else if (this.groupsByName.isEmpty()) {
//				System.out.println("Series\tValue");
//				for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
//					DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
//					System.out.println(dssh.label + "\t" + dssh.value);
//				}
//			}
//			else {
//				System.out.print("Series");
//				for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
//					DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
//					System.out.print("\t" + dssh.label);
//				}
//				System.out.println();
//				for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
//					DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
//					System.out.print(dsg.label);
//					for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
//						DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
//						System.out.print("\t" + dsg.seriesValues.getCount(dssh.name));
//					}
//					System.out.println();
//				}
//			}
//		}
	}
	
	private static class DcStatSeriesHead {
		final String name;
		String label;
		int value = 0;
		DcStatSeriesHead(String name) {
			this.name = name;
			this.label = name;
		}
		void add(int value) {
			this.value += value;
		}
	}
	
	private static class DcStatGroup {
		final String name;
		String label;
		int value = 0;
		CountingSet seriesValues = null;
		DcStatGroup(String name, boolean isMultiSeries) {
			this.name = name;
			this.label = name;
			if (isMultiSeries)
				this.seriesValues = new CountingSet();
		}
		void add(int value, String seriesName) {
			this.value += value;
			if ((this.seriesValues != null) && (seriesName != null))
				this.seriesValues.add(seriesName, value);
		}
		void addAll(DcStatGroup dsg) {
			this.value += dsg.value;
			if ((this.seriesValues != null) && (dsg.seriesValues != null))
				this.seriesValues.addAll(dsg.seriesValues);
		}
		void renameSeries(String osn, String nsn) {
			if (this.seriesValues == null)
				return;
			int sv = this.seriesValues.removeAll(osn);
			if (sv != 0)
				this.seriesValues.add(nsn, sv);
		}
	}
}