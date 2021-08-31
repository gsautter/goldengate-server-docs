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

import java.io.File;
import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants;
import de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;

/**
 * The purpose of this servlet is to form the basis for the GoldenGATE SRS
 * Search Portal. In addition to the fields of a GoldenGATE Server Client
 * Servlet, this servlet provides a GoldeGATE SRS client connected to the
 * backing SRS and a data parser. In addition, it loops the doGet() method
 * through to the doPost() method.
 * 
 * @author sautter
 */
public abstract class AbstractSrsWebPortalServlet extends GgServerClientServlet implements GoldenGateSrsConstants {
	private static final String ANNOTATION_NESTING_ORDER_SETTING = "ANNOTATION_NESTING_ORDER";
	
	/** an XML conform parser used for passing data objects */
	protected Parser dataParser = new Parser(new StandardGrammar());
	
	/** the SRS client */
	protected GoldenGateSrsClient srsClient;
	
	/**
	 * This implementation establishes the connection to the backing GoldenGATE
	 * SRS. Sub classes overwriting this method have to make the super call.
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet#doInit()
	 */
	protected void doInit() throws ServletException {
		super.doInit();
//		
//		//	get SRS access point
//		this.srsClient = new GoldenGateSrsClient(this.serverConnection);
//		//	TODO_ try and obtain cache folder from configuration rather than always use hard coded location
//		//	==> facilitates using RAM disc cache
//		this.srsClient.setCacheFolder(new File(new File(this.webInfFolder, "caches"), "srsData"));
		
		//	get SRS access point
		GoldenGateSrsClient srsClient = new GoldenGateSrsClient(this.serverConnection);
		//	TODO try and obtain cache folder from configuration rather than always use hard coded location
		//	==> facilitates using RAM disc cache
//		srsClient.setCacheFolder(new File(new File(this.webInfFolder, "caches"), "srsData"));
		srsClient.setCacheFolder(new File(this.cacheRootFolder, "srsData"));
		this.srsClient = SearchPortalDataManager.getInstance(this.webAppHost, srsClient);
		
		//	configure GAMTA
		String ano = this.getSetting(ANNOTATION_NESTING_ORDER_SETTING);
		if (ano != null)
			Gamta.setAnnotationNestingOrder(ano);
	}
	
	/** @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		this.doPost(request, response);
	}
//	
//	//	!!! for test purposes only !!!
//	public static void main(String[] args) throws Exception {
////		String pattern = "@title (@author, @year)";
//		String pattern = "@author@year: @author\\s @year publication '@title'";
//		AttributePattern ap = AttributePattern.buildPattern(pattern);
//		Properties data = new Properties();
//		data.setProperty("title", "Nice Strings from XML");
//		data.setProperty("author", "Sautter");
//		data.setProperty("year", "2008");
//		System.out.println(ap.createDisplayString(data));
//	}
}
