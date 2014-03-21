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

import de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.DcStatistics;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatField;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldGroup;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldSet;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Client servlet for GoldenGATE DCS instances
 * 
 * @author sautter
 */
public abstract class GoldenGateDcsServlet extends GgServerHtmlServlet {
	
	private GoldenGateDcsClient dcsClient;
	
	/**
	 * This implementation fetches the implementation specific DCS client. Sub
	 * classes overwriting this method thus have to make the super invocation.
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet#doInit()
	 */
	protected void doInit() throws ServletException {
		super.doInit();
		
		//	get DCS client
		this.dcsClient = this.getDcsClient();
	}
	
	/**
	 * This implementation clears the field set cache. Sub classes overwriting
	 * this method thus have to make the super invocation.
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#reInit()
	 */
	protected void reInit() throws ServletException {
		super.reInit();
		
		//	clean up field cache to trigger re-fetch
		this.fieldSet = null;
		this.fieldLabels.clear();
	}
	
	private StatFieldSet fieldSet;
	private Properties fieldLabels = new Properties();
	private StatFieldSet getFieldSet() throws IOException {
		if (this.fieldSet == null) {
			this.fieldSet = this.dcsClient.getFieldSet();
			this.fieldLabels.setProperty("DocCount", this.fieldSet.docCountLabel);
			StatFieldGroup[] fieldGroups = this.fieldSet.getFieldGroups();
			for (int g = 0; g < fieldGroups.length; g++) {
				StatField[] fields = fieldGroups[g].getFields();
				for (int f = 0; f < fields.length; f++)
					this.fieldLabels.setProperty(fields[f].statColName, fields[f].label);
			}
		}
		return this.fieldSet;
	}
	
	/**
	 * Obtain the implementation specific DCS client.
	 * @return the DCS client
	 */
	protected abstract GoldenGateDcsClient getDcsClient();
	
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
				bw.write(" " + statFields[f] + "=\"" + StatFieldSet.grammar.escape(this.fieldLabels.getProperty(statFields[f], statFields[f])) + "\"");
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
			bw.write("{\"labels\": {"); bw.newLine();
			for (int f = 0; f < statFields.length; f++) {
				bw.write("\"" + statFields[f] + "\": \"" + this.fieldLabels.getProperty(statFields[f], statFields[f]) + "\"" + (((f+1) < statFields.length) ? "," : "")); bw.newLine();
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
	}
	
	private DcStatistics getStats(HttpServletRequest request) throws IOException {
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
		}
		outputFields.removeDuplicateElements();
		groupingFields.removeDuplicateElements();
		orderingFields.removeDuplicateElements();
		
		//	check data
		if (outputFields.isEmpty())
			return null;
		
		//	get data
		return this.dcsClient.getStatistics(outputFields.toStringArray(), groupingFields.toStringArray(), orderingFields.toStringArray(), fieldPredicates, fieldAggregates, aggregatePredicates);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		//	check out what to do
		String pathInfo = request.getPathInfo();
		if (pathInfo == null) {
			super.doPost(request, response);
			
			//	TODO if query not empty, deliver stats anyway
			
			//	TODO deliver form page via doPost()
			return;
		}
		while (pathInfo.startsWith("/"))
			pathInfo = pathInfo.substring(1);
		
		//	request for field descriptors
		if ("fields".equals(pathInfo) || pathInfo.startsWith("fields/")) {
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
		
		//	request for statistics data
		if ("stats".equals(pathInfo) || pathInfo.startsWith("stats/")) {
			String format = request.getParameter("format");
			if ((format == null) && pathInfo.startsWith("fields/"))
				format = pathInfo.substring("fields/".length());
			
			//	get statistics
			DcStatistics stats = this.getStats(request);
			
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
		
		//	let super class handle that one (provide statistics HTML page)
		super.doPost(request, response);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet#getPageBuilder(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected HtmlPageBuilder getPageBuilder(HttpServletRequest request, HttpServletResponse response) throws IOException {
		//	TODO deliver statistics web page, with form, etc
		return super.getPageBuilder(request, response);
	}
	
	public static void main(String[] args) throws Exception {
		final GoldenGateDcsClient dcsc = new GoldenGateDcsClient(ServerConnection.getServerConnection("http://plazi.cs.umb.edu/GgServer/proxy"), "TCS");
		GoldenGateDcsServlet dcss = new GoldenGateDcsServlet() {
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