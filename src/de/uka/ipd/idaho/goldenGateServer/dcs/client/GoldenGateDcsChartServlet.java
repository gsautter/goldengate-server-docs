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
import java.io.FileFilter;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.DcStatistics;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatField;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldGroup;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldSet;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartData;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine.DcStatChartRequest;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine.EmptyQueryException;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.renderers.GoogleCharts;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartRenderer;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;

/**
 * Client servlet for creating statistics data from GoldenGATE DCS instances
 * with the help of JavaScript based rendering libraries or APIs, like Google
 * Charts. The binding to the actual API to use works by means of an API
 * specific renderer object.<br>
 * The latter defaults to the built-in one for Google Charts, but can be
 * replaced by via the configuration of the servlet, namely setting the value
 * of the 'chartRendererClassName' config parameter to the name of the renderer
 * class to use. The renderer class is to be deployed in a JAR file located in
 * the 'Renderers' sub folder of servlet's data folder. Native support for
 * chart rendering APIs other than Google Charts might be added in the future,
 * along with respective configuration files.<br>
 * When replacing the chart rendering API, and thus the chart renderer, it is
 * also likely that two further configuration files require adjustment: The
 * 'chartTypeProperties.cnfg' file, containing the parameters for the supported
 * chart types, which likely depend on the API in use, and the HTML template
 * for the chart builder page, 'ChartBuilderForms.html', which contains input
 * fields for the individual supported chart types.
 * 
 * @author sautter
 */
public abstract class GoldenGateDcsChartServlet extends GoldenGateDcsClientServlet {
	
	private String chartWindowParameters = "width=550,height=350,top=100,left=100,resizable=yes,scrollbar=yes,scrollbars=yes";
	
	private DcStatChartRenderer chartRenderer;
	private DcStatChartEngine chartEngine;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#reInit()
	 */
	protected void reInit() throws ServletException {
		super.reInit();
		
		//	read further settings
		this.chartWindowParameters = this.getSetting("chartWindowParameters", this.chartWindowParameters);
		
		//	read options for supported chart types
		Settings chartTypeProperties = Settings.loadSettings(new File(this.dataFolder, "chartTypeProperties.cnfg"));
		
		//	load class name of chart renderer to use
		String chartRendererClassName = this.getSetting("chartRendererClassName", GoogleCharts.class.getName());
		
		//	create chart renderer loader
		GamtaClassLoader gcl = GamtaClassLoader.createClassLoader(this.getClass());
		try {
			File[] jars = (new File(this.dataFolder, "Renderers")).listFiles(new FileFilter() {
				public boolean accept(File file) {
					return (file.isFile() && file.getName().toLowerCase().endsWith(".jar"));
				}
			});
			if (jars != null) {
				for (int j = 0; j < jars.length; j++)
					gcl.addJar(jars[j]);
			}
		} catch (IOException ioe) {}
		
		//	create chart renderer proper
		try {
			Class chartRendererClass = gcl.loadClass(chartRendererClassName);
			this.chartRenderer = ((DcStatChartRenderer) chartRendererClass.newInstance());
		}
		catch (ClassNotFoundException cnfe) {
			throw new ServletException(("Chart renderer class not found: " + chartRendererClassName), cnfe);
		}
		catch (InstantiationException ie) {
			throw new ServletException(("Chart renderer class could not be instantiated: " + chartRendererClassName), ie);
		}
		catch (IllegalAccessException iae) {
			throw new ServletException(("Illegal access to chart renderer class: " + chartRendererClassName), iae);
		}
		catch (Exception e) {
			throw new ServletException(("Exception loading chart renderer class: " + chartRendererClassName), e);
		}
		
		//	create chart engine, using DCS as its data source
		this.chartEngine = new DcStatChartEngine(chartTypeProperties, this.chartRenderer) {
			protected StatFieldSet getStatFieldSet() throws IOException {
				return getFieldSet();
			}
			protected DcStatistics getStatData(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, boolean allowCache) throws IOException {
				return getDcsStats(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, allowCache);
			}
		};
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
	
	private void sendChartData(final HttpServletRequest request, HttpServletResponse response) throws IOException {
		
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
		
		//	wrap request
		DcStatChartRequest statRequest = new DcStatChartRequest() {
			public Enumeration getParameterNames() {
				return request.getParameterNames();
			}
			public String getParameter(String name) {
				return request.getParameter(name);
			}
		};
		
		//	get raw chart data
		DcStatChartData chartData;
		try {
			chartData = this.chartEngine.getStatChartData(statRequest);
		}
		catch (EmptyQueryException eqe) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, eqe.getMessage());
			return;
		}
		
		//	generate JSON array
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
		
		//	write chart data
		this.chartEngine.writeStatChartData(chartData, statRequest, bw);
		
		//	send & cache data
		bw.flush();
		this.jsCache.put(jsCacheKey, new JsCacheEntry(jsCacheWriter.toString(), this.statsLastUpdated));
	}
	
	private void sendChartBuilderJavaScript(final HttpServletRequest request, HttpServletResponse response) throws IOException {
		
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
		
		//	wrap request
		DcStatChartRequest statRequest = new DcStatChartRequest() {
			public Enumeration getParameterNames() {
				return request.getParameterNames();
			}
			public String getParameter(String name) {
				return request.getParameter(name);
			}
		};
		
		//	get raw chart data
		DcStatChartData chartData;
		try {
			chartData = this.chartEngine.getStatChartData(statRequest);
		}
		catch (EmptyQueryException eqe) {
			response.sendError(HttpServletResponse.SC_BAD_REQUEST, eqe.getMessage());
			return;
		}
		
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
		
		//	let renderer do actual generation
		this.chartRenderer.writeChartBuilderJavaScript(chartData, statRequest, bw);
		
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
	private DcStatistics getDcsStats(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, boolean allowCache) throws IOException {
		
		//	get raw data from backing DCS
		DcStatistics stats = this.dcsClient.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, allowCache);
		
		//	update stats data timestamp
		this.statsLastUpdated = Math.max(this.statsLastUpdated, stats.lastUpdated);
		
		//	finally ...
		return stats;
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
			return "Document Collection Statistics Chart Builder";
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
			
			//	get renderer specific JavaScript URLs
			String[] jsLibURLs = chartRenderer.getRequiredJavaScriptURLs();
			
			//	generate page content
			String chartId = chartIdBuilder.toString();
			this.writeLine("<div id=\"chartDiv" + chartId + "\" style=\"width: 900px; height: 500px;\">");
			for (int s = 0; s < jsLibURLs.length; s++)
				this.writeLine("  <script type=\"text/javascript\" src=\"" + jsLibURLs[s] + "\"></script>");
			this.writeLine("  <script type=\"text/javascript\" src=\"" + this.request.getContextPath() + this.request.getServletPath() + "/chart.js?" + this.request.getQueryString() + "&chartId=" + chartId + "&cacheControl=force\"></script>");
			this.writeLine("</div>");
			this.writeLine("<p>To embed the above chart in some other web page, simply copy below HTML code where the chart is intended to appear.</p>");
			this.writeLine("<pre>");
			this.writeLine("  &lt;div id=&quot;chartDiv" + chartId + "&quot; style=&quot;width: 900px; height: 500px;&quot;&gt;");
			for (int s = 0; s < jsLibURLs.length; s++)
				this.writeLine("    &lt;script type=&quot;text/javascript&quot; src=&quot;" + jsLibURLs[s] + "&quot;&gt;&lt;/script&gt;");
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
}