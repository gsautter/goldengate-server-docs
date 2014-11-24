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

import java.io.IOException;
import java.util.Properties;

import javax.servlet.ServletException;

import de.uka.ipd.idaho.goldenGateServer.client.GgServerHtmlServlet;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatField;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldGroup;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.StatFieldSet;

/**
 * Abstract client servlet for GoldenGATE DCS instances, providing the basic
 * infrastructure required to work with DCS data.
 * 
 * @author sautter
 */
public abstract class GoldenGateDcsClientServlet extends GgServerHtmlServlet {
	
	/** the client object to communicate with the backing DCS */
	protected GoldenGateDcsClient dcsClient;
	
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
	
	/**
	 * Retrieve the field set from the backing DCS. This method also populates
	 * field name to label mappings.
	 * @return the field set
	 * @throws IOException
	 */
	protected StatFieldSet getFieldSet() throws IOException {
		if (this.fieldSet == null) {
			this.fieldSet = this.dcsClient.getFieldSet();
			this.fieldLabels.setProperty("DocCount", this.fieldSet.docCountLabel);
			StatFieldGroup[] fieldGroups = this.fieldSet.getFieldGroups();
			for (int g = 0; g < fieldGroups.length; g++) {
				StatField[] fields = fieldGroups[g].getFields();
				for (int f = 0; f < fields.length; f++) {
					this.fieldLabels.setProperty(fields[f].fullName, fields[f].label);
					this.fieldLabels.setProperty(fields[f].statColName, fields[f].label);
				}
			}
		}
		return this.fieldSet;
	}
	
	/**
	 * Retrieve the label of a field with a given name. If there is no label
	 * for the argument field name, this method returns the field name proper.
	 * @param fieldName the field name
	 * @return the label
	 */
	protected String getFieldLabel(String fieldName) {
		return ((fieldName == null) ? null : this.fieldLabels.getProperty(fieldName, fieldName));
	}
	
	/**
	 * Obtain the implementation specific DCS client.
	 * @return the DCS client
	 */
	protected abstract GoldenGateDcsClient getDcsClient();
}