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
package de.uka.ipd.idaho.goldenGateServer.dcs.client.charts;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Properties;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.DcStatistics;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatField;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldGroup;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldSet;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Abstract query interpretation and data formatting facility for charts built
 * on to of GoldenGATE Document Collection Statistics. This class exists to
 * simplify binding the servlets to different chart rendering facilities. 
 * 
 * @author sautter
 */
public abstract class DcStatChartEngine {
	private Settings chartTypeProperties;
	private DcStatChartRenderer chartRenderer;
	
	/** Constructor
	 * @param chartTypeProperties lists of configuration options for the charts
	 *            offered by the connected rendering facilities, indexed by the
	 *            chart types provided by the latter
	 */
	protected DcStatChartEngine(Settings chartTypeProperties, DcStatChartRenderer chartRenderer) {
		this.chartTypeProperties = chartTypeProperties;
		this.chartRenderer = chartRenderer;
		this.chartRenderer.setData(this, this.chartTypeProperties);
	}
	
	/**
	 * Retrieve statistics data using the options specified in a request. The
	 * argument chart properties are specific to the type of chart the data is
	 * intended to be rendered in. If the request is invalid, this method
	 * returns null.
	 * @param request the request holding the query data
	 * @return the statistics data
	 * @throws IOException
	 */
	public DcStatChartData getStatChartData(DcStatChartRequest request) throws IOException {
		String chartType = request.getParameter("type");
		return this.getStatChartData(request, this.chartRenderer.isMultiSeriesChart(chartType));
	}
	
	/**
	 * Retrieve statistics data using the options specified in a request. The
	 * argument chart properties are specific to the type of chart the data is
	 * intended to be rendered in. If the request is invalid, this method
	 * returns null.
	 * @param request the request holding the query data
	 * @param isMultiSeriesChart does the chart to be rendered allow for multiple
	 *            data series to be displayed?
	 * @return the statistics data
	 * @throws IOException
	 */
	public DcStatChartData getStatChartData(DcStatChartRequest request, boolean isMultiSeriesChart) throws IOException {
		
		//	collect query parameters
		StringVector outputFields = new StringVector();
		StringVector groupingFields = new StringVector();
		StringVector orderingFields = new StringVector();
		Properties fieldPredicates = new Properties();
		Properties fieldAggregates = new Properties();
		Properties aggregatePredicates = new Properties();
		
		//	get data series renaming (pre-filled with field labels)
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
			throw new EmptyQueryException("No output fields selected.");
		
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
		String groupField = null;
		String groupOrder = null;
		String artificialGroups = null;
		if (isMultiSeriesChart) {
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
		boolean allowCache = !"force".equals(request.getParameter("cacheControl"));
		DcStatistics stats = this.getStatData(outputFields.toStringArray(), groupingFields.toStringArray(), orderingFields.toStringArray(), fieldPredicates, fieldAggregates, aggregatePredicates, valueSumField, groupField, allowCache);
		if (stats == null)
			return null;
		
		//	organize statistics data
		DcStatChartData chartData = new DcStatChartData(stats, seriesField, groupField, valueField, "yes".equals(request.getParameter("translateMonthNumbers")));
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
		if (chartData.translateMonthNumbers)
			chartData.translateMonthNumbers();
		
		//	finally ...
		return chartData;
	}
	
	private DcStatistics getStatData(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String valueSumField, String groupingField, boolean allowCache) throws IOException {
		
		//	get raw data from backing DCS
		DcStatistics stats = this.getStatData(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, allowCache);
		if (stats == null)
			return null;
		
		//	normalize statistics field names (no dots allowed in CSV column names in backend)
		boolean needModifyStats = (valueSumField != null);
		for (int f = 0; f < outputFields.length; f++)
			if (outputFields[f].indexOf('.') != -1) {
				needModifyStats = true;
				break;
			}
		if (needModifyStats&& stats.isReadOnly()) {
			DcStatistics wStats = new DcStatistics(stats.getFields(), stats.lastUpdated);
			for (int t = 0; t < stats.size(); t++) {
				StringTupel st = stats.get(t);
				StringTupel wSt = new StringTupel();
				String[] keys = st.getKeyArray();
				for (int k = 0; k < keys.length; k++)
					wSt.setValue(keys[k], st.getValue(keys[k]));
				wStats.addElement(wSt);
			}
			stats = wStats;
		}
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
	
	/**
	 * Write the data for a chart to some writer, as a JSON array.
	 * @param request the request containing the query parameters
	 * @param bw the writer to write to
	 * @throws IOException
	 */
	public void writeStatChartData(DcStatChartRequest request, BufferedWriter bw) throws IOException {
		
		//	get raw chart data
		DcStatChartData chartData = this.getStatChartData(request);
		
		//	write chart data array
		this.writeStatChartData(chartData, request, bw);
	}
	
	/**
	 * Write the data for a chart to some writer, as a JSON array.
	 * @param request the request containing the query parameters
	 * @param bw the writer to write to
	 * @throws IOException
	 */
	public void writeStatChartData(DcStatChartData chartData, DcStatChartRequest request, BufferedWriter bw) throws IOException {
		
		//	read chart parameters
		String chartType = request.getParameter("type");
		
		//	write chart data array
		bw.write("["); bw.newLine();
		chartData.writeJsonArrayContent(
				bw,
				this.chartRenderer.isGroupingFieldNumeric(chartType),
				this.chartRenderer.isMultiSeriesChart(chartType),
				request,
				this,
				null // no renderer here, we want the plain array
			);
		bw.write("]"); bw.newLine();
	}
	
	/**
	 * A request to the statistics engine.
	 * 
	 * @author sautter
	 */
	public static interface DcStatChartRequest {
		
		/**
		 * Retrieve the value of a specific parameter.
		 * @param name the name of the parameter
		 * @return the value of the parameter
		 */
		public abstract String getParameter(String name);
		
		/**
		 * Retrieve an enumeration of the available parameter names. There is
		 * no guarantee as to the order in which the parameter names are given.
		 * @return an enumeration of the parameter names
		 */
		public abstract Enumeration getParameterNames();
	}
	
	/**
	 * Retrieve statistics data, e.g. from the backing DCS. Implementations can
	 * also use other sources, however.
	 * @return the field set
	 * @throws IOException
	 */
	protected abstract DcStatistics getStatData(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, boolean allowCache) throws IOException;
	
	/**
	 * Exception thrown if no statistics data can be retrieved due to an empty
	 * query.
	 * 
	 * @author sautter
	 */
	public static class EmptyQueryException extends IOException {
		EmptyQueryException(String message) {
			super(message);
		}
	}
	
	
	private StatFieldSet fieldSet;
	private TreeMap fieldsByName = new TreeMap();
	private Properties fieldLabels = new Properties();
	private HashSet numericFields = new HashSet();
	
	/**
	 * Retrieve the field set from the backing DCS. This method also populates
	 * field name to label mappings.
	 * @return the field set
	 * @throws IOException
	 */
	StatFieldSet getFieldSet() throws IOException {
		if (this.fieldSet == null) {
			this.fieldSet = this.getStatFieldSet();
			this.fieldLabels.setProperty("DocCount", this.fieldSet.docCountLabel);
			StatFieldGroup[] fieldGroups = this.fieldSet.getFieldGroups();
			for (int g = 0; g < fieldGroups.length; g++) {
				StatField[] fields = fieldGroups[g].getFields();
				for (int f = 0; f < fields.length; f++) {
					this.fieldsByName.put(fields[f].fullName, fields[f]);
					this.fieldsByName.put(fields[f].statColName, fields[f]);
					this.fieldLabels.setProperty(fields[f].fullName, fields[f].label);
					this.fieldLabels.setProperty(fields[f].statColName, fields[f].label);
					if ("integer".equals(fields[f].dataType) || "real".equals(fields[f].dataType))
						this.numericFields.add(fields[f].fullName);
				}
			}
		}
		return this.fieldSet;
	}
	
	/**
	 * Retrieve the field with a given name. If there is no field with the
	 * argument name, this method returns null.
	 * @param fieldName the field name
	 * @return the field
	 */
	StatField getField(String fieldName) {
		if (this.fieldsByName.isEmpty()) try {
			this.getFieldSet();
		} catch (IOException ioe) {}
		return ((fieldName == null) ? null : ((StatField) this.fieldsByName.get(fieldName)));
	}
	
	/**
	 * Retrieve the label of a field with a given name. If there is no label
	 * for the argument field name, this method returns the field name proper.
	 * @param fieldName the field name
	 * @return the label
	 */
	String getFieldLabel(String fieldName) {
		if (this.fieldLabels.isEmpty()) try {
			this.getFieldSet();
		} catch (IOException ioe) {}
		return ((fieldName == null) ? null : this.fieldLabels.getProperty(fieldName, fieldName));
	}
	
	/**
	 * Test if a field with a given name is numeric, i.e., if field values are
	 * numbers.
	 * @param fieldName the field name
	 * @return true if the values of the field with the argument name are
	 *            numbers, false otherwise
	 */
	boolean isNumericField(String fieldName) {
		return this.numericFields.contains(fieldName);
	}
	
	/**
	 * Retrieve the statistics field set, e.g. from the backing DCS.
	 * Implementations can also use other sources, however.
	 * @return the field set
	 * @throws IOException
	 */
	protected abstract StatFieldSet getStatFieldSet() throws IOException;
	
	/**
	 * Escape a string for use in JavaScript. Namely, this method inserts an
	 * escaping backslash before high commas and backslashes.
	 * @param str the string to escape
	 * @return the escaped string
	 */
	public static String escapeForJavaScript(String str) {
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
//	
//	public static void main(String[] args) throws Exception {
//		
//		//	TEST DATA TODO TEST HERE
//		Settings chartTypeProperties = Settings.loadSettings(new File("E:/GoldenGATEv3.WebApp/WEB-INF/srsStatsData", "chartTypeProperties.cnfg"));
//		TestRequest request = new TestRequest();
////		request.setProperty("emptyGroupLabelSubstitute", "UNKNOWN");
////		request.setProperty("group", "matCit.month");
////		request.setProperty("groupCutoff", "B");
////		request.setProperty("groupBuckets", "3;6;9");
////		request.setProperty("series", "matCit.elevation");
////		request.setProperty("seriesCutoff", "B");
////		request.setProperty("seriesBuckets", "0;1000;2000;3000;4000;5000;6000;7000;8000");
////		request.setProperty("truncateSeriesBuckets", "true");
////		request.setProperty("translateMonthNumbers", "yes");
//		request.setProperty("group", "multiField");
//		request.setProperty("field0", "matCit.specimenCount");
//		request.setProperty("field0Aggregate", "sum");
//		request.setProperty("field0", "matCit.specimenCountMale");
//		request.setProperty("field0Aggregate", "sum");
//		request.setProperty("field1", "matCit.specimenCountFemale");
//		request.setProperty("field1Aggregate", "sum");
//		request.setProperty("fieldSum", "matCit.specimenCount");
////		request.setProperty("group", "tax.familyEpithet");
////		request.setProperty("series", "multiField");
////		request.setProperty("fieldSum", "matCit.specimenCount");
////		request.setProperty("field0", "matCit.specimenCountMale");
////		request.setProperty("field1", "matCit.specimenCountFemale");
////		request.setProperty("field0Aggregate", "sum");
////		request.setProperty("field1Aggregate", "sum");
//		request.setProperty("title", "Specimens by Quarter and Elevation");
//		request.setProperty("addDataSumToTitle", "true");
//		request.setProperty("type", "col");
//		request.setProperty("chartId", "0815");
//		
//		//	testing super class against sub class here, but little other chance to get live data
//		ServerConnection sc = ServerConnection.getServerConnection("http://plazi.cs.umb.edu/GgServer/proxy");
//		final GoldenGateDcsClient dcsc = new GoldenGateDcsClient(sc, "TCS");
//		final DcStatChartRenderer scr = new GoogleCharts();
//		DcStatChartEngine sce = new DcStatChartEngine(chartTypeProperties, scr) {
//			protected DcStatistics getStatData(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, boolean allowCache) throws IOException {
//				return dcsc.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, allowCache);
//			}
//			protected StatFieldSet getStatFieldSet() throws IOException {
//				return dcsc.getFieldSet();
//			}
//		};
//		
//		//	get chart data
//		DcStatChartData chartData = sce.getStatChartData(request);
//		
//		//	get fields
//		StatFieldSet fieldSet = dcsc.getFieldSet();
//		Properties fieldLabels = new Properties();
//		HashSet numericFields = new HashSet();
//		fieldLabels.setProperty("DocCount", fieldSet.docCountLabel);
//		StatFieldGroup[] fieldGroups = fieldSet.getFieldGroups();
//		for (int g = 0; g < fieldGroups.length; g++) {
//			StatField[] fields = fieldGroups[g].getFields();
//			for (int f = 0; f < fields.length; f++) {
//				fieldLabels.setProperty(fields[f].statColName, fields[f].label);
//				if ("integer".equals(fields[f].dataType) || "integer".equals(fields[f].dataType))
//					numericFields.add(fields[f].fullName);
//			}
//		}
//		
//		//	generate rendering JavaScript
////		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), "UTF-8"));
//		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(System.out, "UTF-8"));
////		bw.write(debugData.toString());
//		
//		//	write chart data array
//		scr.writeChartBuilderJavaScript(chartData, request, bw);
//		
//		//	send data
//		bw.flush();
//	}
//	
//	private static class TestRequest extends Properties implements DcStatChartRequest {
//		public String getParameter(String name) {
//			return this.getProperty(name);
//		}
//		public Enumeration getParameterNames() {
//			return this.keys();
//		}
//	}
}