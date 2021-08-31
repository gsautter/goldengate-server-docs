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
 *     * Neither the name of the Universitaet Karlsruhe (TH) nor the
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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.AnnotationUtils.XmlOutputOptions;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.util.AnnotationInputStream;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * This servlet provides raw XML documents from the backing GoldenGATE SRS
 * transformed through custom XSLT stylesheets. This allows other parties to
 * retrieve the documents in their favored XML schema. The ID of the document to
 * retrieve and (possible) transform can be specified in two ways: (a) as the
 * value of the 'idQuery' request parameter, or (b) as a suffix to the servlet
 * path invoking this servlet. Likewise, the URL of the XSLT stylesheet to use
 * for transformation can be specified in two wasy: (a) as the value of the
 * 'xsltUrl' query parameter, or (b) by appending its configured name (see
 * below) to the servlet path invoking this servlet. If the document ID is also
 * specified through the path, the order is first the XSLT name, then the
 * document ID.<br>
 * Selected XSLT stylesheets can be installed in this servlet to facilitate
 * calling them by an intuitive name instead of the URL. This is achieved by
 * entering a setting 'XSLT.&lt;xsltName&gt;' in the servlets config file, the
 * value of the setting being the URL of the XSLT stylesheet to be invoked
 * through &lt;xsltName&gt;. If that URL starts with 'http://', it's assumed to
 * be absolute. In contrast, if the URL does not start with 'http://', the URL
 * is interpreted as a local path, relative to the web-apps context path.<br>
 * As a result, there is several ways of getting a document with a given ID (in
 * the following referred to as '&lt;docId&gt;') transformed through an XSLT
 * stylesheet with a given configured name (in the following referred to as
 * '&lt;xsltName&gt;', the configured name 'xml' always refers to the
 * untransformed document) or from a given URL (in the following referred to as
 * '&lt;xsltUrl&gt;'). '&lt;contextUrl&gt;' always corresponds to the URL
 * address of the servlet's web-app context.<br>
 * <code>&lt;contextUrl&gt;/&lt;xsltName&gt;/*</code> has to be mapped to the
 * servlet in the web.xml for the &lt;xsltName&gt; way of invocation to work:
 * <ul>
 * <li><code>&lt;contextUrl&gt;/&lt;xsltName&gt;/&lt;docId&gt;</code></li>
 * <li><code>&lt;contextUrl&gt;/&lt;xsltName&gt;?&lt;docId&gt;</code></li>
 * <li><code>&lt;contextUrl&gt;/&lt;xsltName&gt;?idQuery=&lt;docId&gt;</code></li>
 * </ul>
 * You can also submit the parameters as regular request parameters. This allows
 * for specifying arbitrary XSLT stylesheets at runtime, but on the other hand
 * hampers embedding a direct link in XML data (due to the &amp; in the URL).
 * <code>&lt;contextUrl&gt;/&lt;servletPath&gt;</code> has to be mapped to the
 * servlet in the web.xml for this way of invocation to work:
 * <ul>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;?idQuery=&lt;docId&gt;&amp;xsltUrl=&lt;xsltUrl&gt;</code></li>
 * </ul>
 * A third option is to specify the XSLT name and/or the document ID in the info
 * part of the invocation path.
 * <code>&lt;contextUrl&gt;/&lt;servletPath&gt;/*</code> has to be mapped to
 * the servlet in the web.xml for this way of invocation to work:
 * <ul>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;xsltName&gt;/&lt;docId&gt;</code></li>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;xsltName&gt;?&lt;docId&gt;</code></li>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;xsltName&gt;?idQuery=&lt;docId&gt;</code></li>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;docId&gt;?xsltUrl=&lt;xsltUrl&gt;</code></li>
 * </ul>
 * If the request specifies a document ID, but does not allow for identifying an
 * XSLT stylesheet URL (URL not specified, or xsltName not mapped), the servlet
 * returns the plain document without transformation.
 * 
 * @author sautter
 */
public class XsltServlet extends AbstractSrsWebPortalServlet implements SearchPortalConstants {
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#reInit()
	 */
	protected void reInit() throws ServletException {
		super.reInit();
		
		this.xsltNamesToStylesheetUrls.clear();
		this.cachedStylesheets.clear();
		
		Settings xsltSet = this.config.getSubset("XSLT");
		String[] xsltNames = xsltSet.getKeys();
		for (int x = 0; x < xsltNames.length; x++) {
			String xsltUrl = xsltSet.getSetting(xsltNames[x]);
			if ((xsltUrl != null) && (xsltUrl.trim().length() != 0))
				this.xsltNamesToStylesheetUrls.setProperty(xsltNames[x], xsltUrl.trim());
			Settings xsltOptionSet = this.config.getSubset("XSLT.options." + xsltNames[x]);
			if (xsltOptionSet.isEmpty())
				continue;
			XmlOutputOptions xsltOptions = new XmlOutputOptions();
			NameSetFilter dAnnotationTypes = this.parseNameSetFilter(xsltOptionSet.getSetting("annotationTypes"));
			if (dAnnotationTypes != null)
				xsltOptions.setAnnotationTypes(dAnnotationTypes.filterSet, dAnnotationTypes.invert);
			NameSetFilter dAttributeNames = this.parseNameSetFilter(xsltOptionSet.getSetting("attributeNames"));
			if (dAttributeNames != null)
				xsltOptions.setAttributeNames(dAttributeNames.filterSet, dAttributeNames.invert);
			NameSetFilter dIncludeIdTypes = this.parseNameSetFilter(xsltOptionSet.getSetting("includeIdTypes"));
			if (dIncludeIdTypes != null)
				xsltOptions.setIncludeIdTypes(dIncludeIdTypes.filterSet, dIncludeIdTypes.invert);
			this.xsltNamesToOutputOptions.put(xsltNames[x], xsltOptions);
		}
	}
	
	private static class NameSetFilter {
		final HashSet filterSet;
		final boolean invert;
		NameSetFilter(HashSet filterSet, boolean invert) {
			this.filterSet = filterSet;
			this.invert = invert;
		}
	}
	private NameSetFilter parseNameSetFilter(String nameStr) {
		if (nameStr == null)
			return null;
		nameStr = nameStr.trim();
		if (nameStr.length() == 0)
			return null;
		ArrayList names = new ArrayList(Arrays.asList(nameStr.split("\\s+")));
		boolean invert = false;
		if ("-".equals(names.get(0))) {
			invert = true;
			names.remove(0);
		}
		return (names.isEmpty() ? null : new NameSetFilter(new HashSet(names), invert));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.AbstractSrsWebPortalServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doXsltGet(request, request.getParameterValues(XSLT_URL_PARAMETER), response);
	}
	
	void doXsltGet(HttpServletRequest request, String xsltName, HttpServletResponse response) throws ServletException, IOException {
		String xsltUrl = this.xsltNamesToStylesheetUrls.getProperty(xsltName, xsltName);
		this.doXsltGet(request, xsltUrl.trim().split("\\s++"), response);
	}
	
	private void doXsltGet(HttpServletRequest request, String[] xsltUrls, HttpServletResponse response) throws ServletException, IOException {
//		System.out.println("GoldenGateSRS XSLT: handling request");
//		long start = System.currentTimeMillis();
		
		//	request for specific plain XML document
		String docId = request.getParameter(ID_QUERY_FIELD_NAME);
		
		//	request for specific version of plain XML document
		String docVersionStr = request.getParameter(VERSION_QUERY_FIELD_NAME);
		
		//	get client requested media type (browsers tend to be stubborn about what to do with files ...)
		String requestContentType = request.getParameter("type");
		
		//	parameter missing, check paths
		//	TODO streamline this sucker
		XmlOutputOptions xsltOptions = null;
		if ((docId == null) || (xsltUrls == null)) {
			StringVector fullPath = new StringVector();
			
			String path = request.getServletPath();
			if ((path != null)) {
				
				//	cut leading slash
				if (path.startsWith("/"))
					path = path.substring(1);
				
				//	user last part only
				path = path.substring(path.lastIndexOf('/') + "/".length());
				
				//	add it
				fullPath.addElement(path);
			}
			
			String pathInfo = request.getPathInfo();
			if ((pathInfo != null)) {
				
				//	cut leading slash
				if (pathInfo.startsWith("/"))
					pathInfo = pathInfo.substring("/".length());
				
				//	add it
				fullPath.parseAndAddElements(pathInfo, "/");
			}
			
			String query = request.getQueryString();
			if ((query != null) && (query.indexOf('=') == -1)) {
				
				//	cut leading question mark
				if (query.startsWith("?"))
					query = query.substring("?".length());
				
				//	add it
				fullPath.addElement(query);
			}
			
			//	got no data ...
			if ((docId == null) && fullPath.isEmpty()) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			
			//	only one part in path, has to be document ID
			else if ((docId == null) && (fullPath.size() == 1))
				docId = fullPath.remove(0);
			
			//	from here on, either document ID is not null, or we have at least two elements in fullPath
			else {
				String xsltName = fullPath.remove(0);
				
				//	map invocation path to named XSLT URL and output options
				if (xsltUrls == null) {
					String xsltUrlString = this.xsltNamesToStylesheetUrls.getProperty(xsltName);
					xsltUrls = ((xsltUrlString == null) ? null : xsltUrlString.trim().split("\\s++"));
				}
				xsltOptions = ((XmlOutputOptions) this.xsltNamesToOutputOptions.get(xsltName));
				
				if (docId == null)
					docId = fullPath.remove(0);
				if (fullPath.size() > 0)
					docVersionStr = fullPath.remove(0);
			}
		}
//		System.out.println(" - request parsed after " + (System.currentTimeMillis() - start));
		
		//	parse version number off document ID (need to do that before replacing dashes ...)
		if (docId.indexOf('/') != -1) {
			docVersionStr = docId.substring(docId.indexOf('/') + "/".length());
			docId = docId.substring(0, docId.indexOf('/'));
			while (docVersionStr.startsWith("/"))
				docVersionStr = docVersionStr.substring("/".length());
		}
		
		//	remove dashes from UUIDs
		docId = docId.replaceAll("\\-", "").toUpperCase();
		
		//	parse version number (wherever it came from)
		int docVersion = 0;
		if (docVersionStr != null) try {
			docVersion = Integer.parseInt(docVersionStr);
		} catch (NumberFormatException nfe) {}
		
		//	load document
		MutableAnnotation doc;
		try {
			doc = this.srsClient.getXmlDocument(docId, docVersion, !FORCE_CACHE.equals(request.getParameter(CACHE_CONTROL_PARAMETER)));
		}
		catch (IOException ioe) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, ioe.getMessage());
			return;
		}
//		System.out.println(" - got plain document after " + (System.currentTimeMillis() - start));
		
		//	set response content type (must be done before obtaining writer)
		String contentType = "text/xml";
		if ((xsltUrls != null) && (xsltUrls.length != 0)) {
			Transformer lastTransform = this.getTransformer(xsltUrls[xsltUrls.length-1]);
			String lastTransformMediaType = lastTransform.getOutputProperty(OutputKeys.MEDIA_TYPE);
			if (lastTransformMediaType != null)
				contentType = lastTransformMediaType;
		}
		if (requestContentType == null)
			response.setContentType(contentType);
		else response.setContentType(requestContentType);
		response.setCharacterEncoding(ENCODING);
		response.setHeader("Cache-Control", "no-cache");
		
		//	make sure we have output options set (if to default behavior)
		if (xsltOptions == null)
			xsltOptions = new XmlOutputOptions();
		
		//	get output writer
		BufferedWriter out = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), ENCODING));
		
		//	no XSLT transformer, send plain data
		if ((xsltUrls == null) || (xsltUrls.length == 0)) {
			AnnotationUtils.writeXML(doc, out, xsltOptions);
//			System.out.println(" - plain document sent after " + (System.currentTimeMillis() - start));
		}
		
		//	do transformation
		else {
			
			//	log slow transformations
			XslTransformationTimer xtt = new XslTransformationTimer(docId, Arrays.toString(xsltUrls));
			xtt.start();
			
			//	collect XML namespace declarations actually used in document
			Annotation[] docAnnots = doc.getAnnotations();
			HashSet docXmlns = new HashSet();
			for (int a = 0; a < docAnnots.length; a++) {
				String annotType = docAnnots[a].getType();
				if (annotType.indexOf(':') != -1)
					docXmlns.add(annotType.substring(0, annotType.indexOf(':')));
				String[] annotAns = docAnnots[a].getAttributeNames();
				for (int n = 0; n < annotAns.length; n++) {
					if (annotAns[n].startsWith("xmlns:"))
						continue;
					if (annotAns[n].indexOf(':') != -1)
						docXmlns.add(annotAns[n].substring(0, annotAns[n].indexOf(':')));
				}
			}
			
			//	remove unused XML namespace declarations so XSLT can define its own
			String[] docAns = doc.getAttributeNames();
			for (int n = 0; n < docAns.length; n++) {
				if (!docAns[n].startsWith("xmlns:"))
					continue;
				if (!docXmlns.contains(docAns[n].substring("xmlns:".length())))
					doc.removeAttribute(docAns[n]);
			}
			for (int a = 0; a < docAnnots.length; a++) {
				String[] annotAns = docAnnots[a].getAttributeNames();
				for (int n = 0; n < annotAns.length; n++) {
					if (!annotAns[n].startsWith("xmlns:"))
						continue;
					if (!docXmlns.contains(annotAns[n].substring("xmlns:".length())))
						docAnnots[a].removeAttribute(annotAns[n]);
				}
			}
			
			//	build transformer chain
			InputStream is = new AnnotationInputStream(doc, "  ", "utf-8", xsltOptions); 
			for (int x = 0; x < (xsltUrls.length - 1); x++)
				is = XsltUtils.chain(is, this.getTransformer(xsltUrls[x]));
//			System.out.println(" - got XSL transformers after " + (System.currentTimeMillis() - start));
			
			//	process data through last transformer
			try {
				this.getTransformer(xsltUrls[xsltUrls.length-1]).transform(new StreamSource(is), new StreamResult(out));
//				System.out.println(" - XSL transformation done after " + (System.currentTimeMillis() - start));
			}
			catch (TransformerException te) {
//				System.out.println(" - XSL transformation failed after " + (System.currentTimeMillis() - start));
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, te.getMessageAndLocation());
				return;
			}
			finally {
				xtt.done();
			}
		}
		
		//	finish response
		out.newLine();
		out.flush();
//		System.out.println(" - request done after " + (System.currentTimeMillis() - start));
	}
	
	private static class XslTransformationTimer extends Thread {
		private static final int transformationLogTimeThreshold = (1000 * 20); // 20 seconds for starters, so we find the _real_ oddjobs first
		private final long startTime = System.currentTimeMillis();
		private boolean transformationRunning = true;
		private String docId;
		private String xsltUrl;
		XslTransformationTimer(String docId, String xsltUrl) {
			this.docId = docId;
			this.xsltUrl = xsltUrl;
		}
		public void run() {
			final long timeoutTime = (this.startTime + transformationLogTimeThreshold);
			while (this.transformationRunning) try {
				long time = System.currentTimeMillis();
				if (time < timeoutTime)
					Thread.sleep(Math.min((transformationLogTimeThreshold / 10), (timeoutTime - time)));
				else break;
			} catch (InterruptedException ie) {}
			if (this.transformationRunning)
				System.out.println("SLOW XSLT ON '" + this.docId + "' :'" + this.xsltUrl + "' failed to finish in " + transformationLogTimeThreshold + "ms");
		}
		void done() {
			this.transformationRunning = false;
			long transformationTime = (System.currentTimeMillis() - this.startTime);
			if (transformationTime > transformationLogTimeThreshold)
				System.out.println("SLOW XSLT ON '" + this.docId + "' :'" + this.xsltUrl + "' finished only in " + transformationTime + "ms");
		}
	}
	
	private Properties xsltNamesToStylesheetUrls = new Properties();
	private HashMap xsltNamesToOutputOptions = new HashMap();
	
	private HashSet cachedStylesheets = new HashSet();
	
	private Transformer getTransformer(String xsltUrl) throws IOException {
		Transformer xslt;
		
		if (xsltUrl.indexOf("://") != -1)
			xslt = XsltUtils.getTransformer(xsltUrl, !this.cachedStylesheets.add(xsltUrl));
		else if (xsltUrl.startsWith("/") || (xsltUrl.indexOf(":") != -1))
			xslt = XsltUtils.getTransformer(new File(xsltUrl), !this.cachedStylesheets.add(xsltUrl));
		else xslt = XsltUtils.getTransformer(new File(this.dataFolder, xsltUrl), !this.cachedStylesheets.add(xsltUrl));
		
		if (xslt == null)
			throw new IOException("XSLT transformer chain broken at '" + xsltUrl + "'");
		return xslt;
	}
}
