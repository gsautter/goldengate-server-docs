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
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.easyIO.settings.Settings;

/**
 * The intention of the SearchLinkServlet is to provide a simpler syntax for
 * links to search results, in particular shorter queries. It therefore allows
 * for mapping dedicated virtual search fields to fields in the SRS Web Portal's
 * search form, adding some fixed search parameters along the way. The HTML
 * pages produced by this servlet forward to the respective SRS Web Portal pages
 * immediately. Assuming that &lt;fieldName&gt; represents the name of the
 * virtual search field, for creating a virtual search field, specify two
 * parameters in the servlet's config file:
 * <ul>
 * <li><b>virtualField.&lt;fieldName&gt;.srsFieldName</b>: the name of the
 * real search field in the backing SRS, to nap the query value of
 * &lt;fieldName&gt; to. Most easily, copy them from a URL query submitted
 * through the SRS Search Form.</li>
 * <li><b>virtualField.&lt;fieldName&gt;.srsParameters</b>: a string of
 * additional parameters for the backing SRS, have to be URL readily encoded.
 * Most easily, copy them from a URL query submitted through the SRS Search
 * Form.</li>
 * </ul>
 * You can configure as may virtual search fields as you like, they only have to
 * be named differently. When using multiple virtual fields at a time, make sure
 * the respective parameter strings do not contradict each other. If the latter
 * is the case, search results may turn out other than intended. Parameters that
 * are not mapped will be looped through to SRS unchanged.<br>
 * This servlet expects a SearchPortalServlet mapped to 'search' in the same web
 * application.
 * 
 * @author sautter
 */
public class SearchLinkServlet extends AbstractSrsWebPortalServlet implements SearchPortalConstants {
	
	private Properties fieldNameMappings = new Properties();
	private Properties fieldParameters = new Properties();
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#reInit()
	 */
	protected void reInit() throws ServletException {
		super.reInit();
		
		//	clear registers
		this.fieldNameMappings.clear();
		this.fieldParameters.clear();
		
		//	get subset
		Settings vfSets = this.config.getSubset("virtualField");
		
		//	get virtual field names
		String[] virtualFieldNames = vfSets.getSubsetPrefixes();
		for (int v = 0; v < virtualFieldNames.length; v++) {
			Settings vfSet = vfSets.getSubset(virtualFieldNames[v]);
			String srsFieldName = vfSet.getSetting("srsFieldName");
			
			if (srsFieldName != null) {
				String srsParameters = vfSet.getSetting("srsParameters", "");
				
				while (srsParameters.startsWith("&"))
					srsParameters = srsParameters.substring(1);
				while (srsParameters.endsWith("&"))
					srsParameters = srsParameters.substring(0, (srsParameters.length() - 1));
				
				this.fieldNameMappings.setProperty(virtualFieldNames[v], srsFieldName);
				this.fieldParameters.setProperty(virtualFieldNames[v], srsParameters);
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		Enumeration parameterNames = request.getParameterNames();
		
		StringBuffer srsQuery = new StringBuffer();
		
		while (parameterNames.hasMoreElements()) {
			String parameterName = parameterNames.nextElement().toString();
			System.out.println("Got parameter: " + parameterName);
			
			String[] parameterValues = request.getParameterValues(parameterName);
			System.out.println("Got " + parameterValues.length + " parameter values");
			
			String srsParameters = this.fieldParameters.getProperty(parameterName, "");
			System.out.println("Got SRS parameters: " + srsParameters);
			srsQuery.append(((srsQuery.length() == 0) ? "" : "&") + srsParameters);
			
			String srsFieldName = this.fieldNameMappings.getProperty(parameterName, parameterName);
			System.out.println("Mapped parameter: " + srsFieldName);
			
			for (int v = 0; v < parameterValues.length; v++)
				srsQuery.append(((srsQuery.length() == 0) ? "" : "&") + srsFieldName + "=" + URLEncoder.encode(parameterValues[v], "UTF-8"));
		}
		System.out.println("Got query: " + srsQuery.toString());
		
		response.setContentType("text/html");
		response.setHeader("Cache-Control", "no-cache");
		BufferedWriter bw = new BufferedWriter(response.getWriter());
		
		bw.write("<html>");
		bw.newLine();
		bw.write("<head>");
		bw.newLine();
		bw.write("<meta http-equiv=\"expires\" content=\"0\">");
		bw.newLine();
		bw.write("<meta" +
				" http-equiv=\"refresh\"" +
				" content=\"0; URL=" + request.getContextPath() + "/search?" + srsQuery.toString() + "\"" +
				">");
		bw.newLine();
		bw.write("</head>");
		bw.newLine();
		bw.write("</html>");
		bw.newLine();
		bw.flush();
	}
}