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
package de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.renderers;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartData;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine.DcStatChartRequest;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer;

/**
 * Data formatting and rendering facility for building charts on top of
 * GoldenGATE Document Collection Statistics using the GoolgeCharts API.
 * 
 * @author sautter
 */
public class GoogleCharts extends DcStatChartRenderer {
	
	/** zero-argument constructor for class loading */
	public GoogleCharts() {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer#init()
	 */
	protected void init() {
		
		//	inject default properties
		String[] chartTypePropertyNames = defaultChartTypePropetries.getKeys();
		for (int p = 0; p < chartTypePropertyNames.length; p++) {
			if (this.chartTypeProperties.getSetting(chartTypePropertyNames[p]) == null)
				this.chartTypeProperties.setSetting(chartTypePropertyNames[p], defaultChartTypePropetries.getSetting(chartTypePropertyNames[p]));
		}
		
		//	TODO group multi-series chart types
		
		//	TODO group multi-series chart types that require a numeric group field
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer#getRequiredJavaScriptURLs()
	 */
	public String[] getRequiredJavaScriptURLs() {
		String[] rjss = {
			"https://www.google.com/jsapi"
		};
		return rjss;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer#isMultiSeriesChart(java.lang.String)
	 */
	public boolean isMultiSeriesChart(String chartType) {
		Settings chartProperties = this.chartTypeProperties.getSubset(chartType);
		return "true".equals(chartProperties.getSetting("valuesGroupable", "true"));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer#isGroupingFieldNumeric(java.lang.String)
	 */
	public boolean isGroupingFieldNumeric(String chartType) {
		Settings chartProperties = this.chartTypeProperties.getSubset(chartType);
		return "number".equals(chartProperties.getSetting("groupFieldType", "string"));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer#writeChartBuilderJavaScript(de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartData, de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine.DcStatChartRequest, java.lang.String, java.io.BufferedWriter)
	 */
	protected void writeChartBuilderJavaScript(DcStatChartData chartData, DcStatChartRequest request, String chartId, BufferedWriter bw) throws IOException {
		
		//	read chart parameters
		String chartType = request.getParameter("type");
		Settings chartProperties = this.chartTypeProperties.getSubset(chartType);
		String[] chartOptions = chartProperties.getSetting("chartOptionNames", "").trim().split("\\s+");
		Settings chartOptionDefaults = chartProperties.getSubset("chartOptionDefaults");
		
		//	get customization parameters
		boolean addDataSumToTitle = "true".equals(request.getParameter("addDataSumToTitle"));
		
		//	write chart data array
		bw.write("function drawChart" + chartId + "() {"); bw.newLine();
		bw.write("  var chartData = google.visualization.arrayToDataTable(["); bw.newLine();
		this.writeJsonArrayContent(chartData, request, bw);
		bw.write("  ]);"); bw.newLine();
		
		//	write chart options
		bw.write("  var chartOptions = {"); bw.newLine();
		this.writeChartOptions(
				bw,
				chartOptions,
				request,
				chartOptionDefaults,
				chartData.getValue(),
				addDataSumToTitle,
				(
						(chartData.getSeriesCount() == 0)
						&&
						((chartData.getGroupCount() == 0) || "multiField".equals(chartData.groupField))
				)
			);
		bw.write("  };"); bw.newLine();
		bw.write("  var chart = new google.visualization." + chartProperties.getSetting("chartClassName") + "(document.getElementById('chartDiv" + chartId + "'));"); bw.newLine();
		bw.write("  chart.draw(chartData, chartOptions);"); bw.newLine();
		bw.write("}"); bw.newLine();
		
		//	write rendering function calls
		bw.write("google.load('visualization', '1', {'packages': ['" + chartProperties.getSetting("chartPackage") + "']});"); bw.newLine();
		bw.write("google.setOnLoadCallback(drawChart" + chartId + ");"); bw.newLine();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer#getTitleRowExtension(de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartData, de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine.DcStatChartRequest)
	 */
	protected String getTitleRowExtension(DcStatChartData chartData, DcStatChartRequest request) {
		//	TODO add styles, e.g. colors
		return super.getTitleRowExtension(chartData, request);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer#getDataRowExtension(de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartData, de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine.DcStatChartRequest, java.lang.String)
	 */
	protected String getDataRowExtension(DcStatChartData chartData, DcStatChartRequest request, String groupOrSeriesName) {
		//	TODO add styles, e.g. colors
		return super.getDataRowExtension(chartData, request, groupOrSeriesName);
	}
	
	private void writeChartOptions(BufferedWriter bw, String[] chartOptions, DcStatChartRequest request, Settings chartOptionDefaults, int dataValueSum, boolean addDataValueSumToTitle, boolean hideLegend) throws IOException {
		
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
				bw.write("    " + chartOptions[o] + ": " + (quoteValue ? ("'" + DcStatChartEngine.escapeForJavaScript(chartOptionValue) + "'") : chartOptionValue) + ","); bw.newLine();
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
				chartOptionObject.put(chartOptions[o], (quoteValue ? ("'" + DcStatChartEngine.escapeForJavaScript(chartOptionValue) + "'") : chartOptionValue));
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
	
	//	default chart properties TODO extend them
	private static final Settings defaultChartTypePropetries = new Settings();
	static {
		defaultChartTypePropetries.setSetting("bar.chartClassName", "BarChart");
		defaultChartTypePropetries.setSetting("bar.chartPackage", "corechart");
		defaultChartTypePropetries.setSetting("bar.chartOptionNames", "isStacked hAxis.logScale 'hAxis.title vAxis.logScale 'vAxis.title 'title fontSize");
		defaultChartTypePropetries.setSetting("bar.chartOptionDefaults.isStacked", "false");
		defaultChartTypePropetries.setSetting("bar.chartOptionDefaults.hAxis.logScale", "false");
		defaultChartTypePropetries.setSetting("bar.chartOptionDefaults.vAxis.logScale", "false");
		defaultChartTypePropetries.setSetting("bar.chartOptionDefaults.fontSize", "'automatic'");
		
		defaultChartTypePropetries.setSetting("col.chartClassName", "ColumnChart");
		defaultChartTypePropetries.setSetting("col.chartPackage", "corechart");
		defaultChartTypePropetries.setSetting("col.chartOptionNames", "isStacked hAxis.logScale 'hAxis.title vAxis.logScale 'vAxis.title 'title fontSize");
		defaultChartTypePropetries.setSetting("col.chartOptionDefaults.isStacked", "false");
		defaultChartTypePropetries.setSetting("col.chartOptionDefaults.hAxis.logScale", "false");
		defaultChartTypePropetries.setSetting("col.chartOptionDefaults.vAxis.logScale", "false");
		defaultChartTypePropetries.setSetting("col.chartOptionDefaults.fontSize", "'automatic'");
		
		defaultChartTypePropetries.setSetting("geo.chartClassName", "GeoChart");
		defaultChartTypePropetries.setSetting("geo.chartPackage", "geochart");
		defaultChartTypePropetries.setSetting("geo.chartOptionNames", "'region fontSize");
		defaultChartTypePropetries.setSetting("geo.chartOptionDefaults.region", "world");
		defaultChartTypePropetries.setSetting("geo.chartOptionDefaults.fontSize", "'automatic'");
		defaultChartTypePropetries.setSetting("geo.valuesGroupable", "false");
		
		defaultChartTypePropetries.setSetting("pie.chartClassName", "PieChart");
		defaultChartTypePropetries.setSetting("pie.chartPackage", "corechart");
		defaultChartTypePropetries.setSetting("pie.chartOptionNames", "pieHole 'title fontSize");
		defaultChartTypePropetries.setSetting("pie.chartOptionDefaults.pieHole", "0");
		defaultChartTypePropetries.setSetting("pie.chartOptionDefaults.fontSize", "'automatic'");
		defaultChartTypePropetries.setSetting("pie.valuesGroupable", "false");
		
		defaultChartTypePropetries.setSetting("scat.chartClassName", "ScatterChart");
		defaultChartTypePropetries.setSetting("scat.chartPackage", "corechart");
		defaultChartTypePropetries.setSetting("scat.chartOptionNames", "hAxis.logScale 'hAxis.title vAxis.logScale 'vAxis.title 'title fontSize");
		defaultChartTypePropetries.setSetting("scat.chartOptionDefaults.hAxis.logScale", "false");
		defaultChartTypePropetries.setSetting("scat.chartOptionDefaults.vAxis.logScale", "false");
		defaultChartTypePropetries.setSetting("scat.groupFieldType", "number");
		defaultChartTypePropetries.setSetting("scat.chartOptionDefaults.fontSize", "'automatic'");	}
}
