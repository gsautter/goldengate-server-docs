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
package de.uka.ipd.idaho.goldenGateServer.dcs.client.charts;

import java.io.BufferedWriter;
import java.io.IOException;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine.DcStatChartRequest;

/**
 * Abstract data formatting and rendering facility for building charts on top
 * of GoldenGATE Document Collection Statistics. This class exists to simplify
 * binding the charts to different chart rendering facilities, namely to adapt
 * to the syntax and parameter name specificities of respective JavaScript
 * libraries.
 * 
 * @author sautter
 */
public abstract class DcStatChartRenderer {
	protected DcStatChartEngine parent;
	protected Settings chartTypeProperties;
	
	protected DcStatChartRenderer() {}
	
	void setData(DcStatChartEngine parent, Settings chartTypeProperties) {
		this.parent = parent;
		this.chartTypeProperties = chartTypeProperties;
		this.init();
	}
	
	/**
	 * Initialize the chart renderer after parent engine and configured chart
	 * type properties have been injected. This default implementation does
	 * nothing, sub classes are welcome to overwrite it as needed.
	 */
	protected void init() {}
	
	/**
	 * Retrieve the (absolute) URLs of the JavaScript libraries to include in
	 * an HTML page in which JavaScripts generated by this class are supposed
	 * to execute.
	 * @return an array holding the JavaScript library URLs
	 */
	public abstract String[] getRequiredJavaScriptURLs();
	
	/**
	 * Test if the values in a chart of a specific type can be grouped, i.e.,
	 * whether or not a chart of the argument type supports multiple data
	 * series to be displayed together.
	 * @param chartType the chart type to test
	 * @return true if the charts can handle multiple data series, false
	 *            otherwise
	 */
	public abstract boolean isMultiSeriesChart(String chartType);
	
	/**
	 * Test if the grouping field in a multi-series chart has to be numeric.
	 * @param chartType the chart type to test
	 * @return true if grouping field values are numeric, false otherwise
	 */
	public abstract boolean isGroupingFieldNumeric(String chartType);
	
	/**
	 * Write the JavaScript code rendering a chart. This method enforces
	 * cutoffs or bucketizes the argument chart data, and then delegates to the
	 * rendering facility specific method to write the actual JavaScript code.
	 * @param chartData the chart data to render
	 * @param request the request holding further parameters
	 * @param bw the writer to write to
	 * @throws IOException
	 */
	public void writeChartBuilderJavaScript(DcStatChartData chartData, DcStatChartRequest request, BufferedWriter bw) throws IOException {
		
		//	enforce cutoffs, or bucketize
		if ((chartData.seriesField != null) && !"multiField".equals(chartData.seriesField)) {
			String seriesCutoff = request.getParameter("seriesCutoff");
			if ((seriesCutoff == null) || (seriesCutoff.length() == 0) || "0".equals(seriesCutoff)) {}
			else if ("B".equals(seriesCutoff)) {
				String buckets = request.getParameter("seriesBuckets");
				boolean truncate = "true".equals(request.getParameter("truncateSeriesBuckets"));
				if ((buckets != null) && (buckets.length() != 0))
					chartData.bucketizeSeries(buckets, truncate, this.parent.isNumericField(chartData.seriesField), (chartData.translateMonthNumbers && chartData.seriesField.toLowerCase().endsWith("month")));
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
					chartData.bucketizeGroups(buckets, truncate, this.parent.isNumericField(chartData.groupField), (chartData.translateMonthNumbers && chartData.groupField.toLowerCase().endsWith("month")));
			}
			else try {
				chartData.pruneGroups(Integer.parseInt(groupCutoff));
			} catch (NumberFormatException nfe) {}
		}
		
		//	write rendering facility specific JavaScript
		this.writeChartBuilderJavaScript(chartData, request, request.getParameter("chartId"), bw);
	}
	
	/**
	 * Write the JavaScript code rendering a chart, specific to the concrete
	 * chart rendering facilities (JavaScript library) this renderer is built
	 * for.
	 * @param chartData the chart data to render
	 * @param request the request holding further parameters
	 * @param chartId the ID of the chart to render, unique within an HTML page
	 * @param bw the writer to write to
	 * @throws IOException
	 */
	protected abstract void writeChartBuilderJavaScript(DcStatChartData chartData, DcStatChartRequest request, String chartId, BufferedWriter bw) throws IOException;
	
	/**
	 * Write the content of a chart data object as the content of a JSON array.
	 * @param chartData the chart data to write
	 * @param request the request, holding further parameters
	 * @param bw the writer to write to
	 * @throws IOException
	 */
	protected void writeJsonArrayContent(DcStatChartData chartData, DcStatChartRequest request, BufferedWriter bw) throws IOException {
		String chartType = request.getParameter("type");
		chartData.writeJsonArrayContent(
				bw,
				this.isGroupingFieldNumeric(chartType),
				this.isMultiSeriesChart(chartType),
				request,
				this.parent,
				this // inject ourselves so we can provide in-line styling options
			);
	}
	
	/**
	 * Retrieve the headers of extra columns in JSON array content. The string
	 * returned by this method must validly append to the contents of a JSON
	 * array, i.e., either be empty, or have a comma before each header.
	 * Further, for any given chart data object, the number of extra column
	 * headers returned by this method must be the same as the number of column
	 * values returned by the data row counterpart of this method. That also
	 * means that sub classes will usually overwrite both methods or none. This
	 * default implementation returns the empty string.
	 * @param chartData the chart data being rendered into JSON
	 * @param request the request providing further parameters
	 * @return the extra column headers
	 */
	protected String getTitleRowExtension(DcStatChartData chartData, DcStatChartRequest request) {
		return "";
	}
	
	/**
	 * Retrieve the values of extra columns in JSON array content. The string
	 * returned by this method must validly append to the contents of a JSON
	 * array, i.e., either be empty, or have a comma before each column value.
	 * Further, for any given chart data object, the number of extra column
	 * values returned by this method must be the same as the number of column
	 * headers returned by the title row counterpart of this method. That also
	 * means that sub classes will usually overwrite both methods or none. This
	 * default implementation returns the empty string.
	 * @param chartData the chart data being rendered into JSON
	 * @param request the request providing further parameters
	 * @param groupOrSeriesName the name of the data group or series the values
	 *            belong to
	 * @return the extra column headers
	 */
	protected String getDataRowExtension(DcStatChartData chartData, DcStatChartRequest request, String groupOrSeriesName) {
		return "";
	}
}