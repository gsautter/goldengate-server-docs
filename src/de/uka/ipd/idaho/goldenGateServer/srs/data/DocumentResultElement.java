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
package de.uka.ipd.idaho.goldenGateServer.srs.data;


import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * A single element in the result of a document search
 * 
 * @author sautter
 */
public class DocumentResultElement extends IndexResultElement implements GoldenGateSrsConstants {
	
	/** the ID of the document*/
	public final String documentId;
	
	/** the actual result document, null in a plain result list*/
	public final DocumentRoot document;
	
	/** the relevance of the document in the search result*/
	public final double relevance;
	
	/**	Constructor
	 * @param	docNr		the number of the document
	 * @param	documentId	the document ID
	 * @param	relevance	the relevance of this document for a query
	 * @param	document	the document itself (may be null if search was only for meta data)
	 */
	public DocumentResultElement(long docNr, String documentId, double relevance, DocumentRoot document) {
		super(docNr, DocumentRoot.DOCUMENT_TYPE, "");
		this.documentId = documentId;
		this.relevance = relevance;
		this.document = document;
		
		if (this.document != null) {
			String[] attributeNames = this.document.getAttributeNames();
			for (int a = 0; a < attributeNames.length; a++) {
				Object value = this.document.getAttribute(attributeNames[a]);
				if ((value != null) && (!this.hasAttribute(attributeNames[a])))
					this.setAttribute(attributeNames[a], value.toString());
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#hasAttribute(java.lang.String)
	 */
	public boolean hasAttribute(String name) {
		return (RELEVANCE_ATTRIBUTE.equals(name) || DOCUMENT_ID_ATTRIBUTE.equals(name) || super.hasAttribute(name));
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#getAttribute(java.lang.String, java.lang.Object)
	 */
	public Object getAttribute(String name, Object def) {
		if (RELEVANCE_ATTRIBUTE.equals(name)) return ("" + this.relevance);
		else if (DOCUMENT_ID_ATTRIBUTE.equals(name)) return ("" + this.documentId);
		else return super.getAttribute(name, def);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#getAttributeNames()
	 */
	public String[] getAttributeNames() {
		StringVector sv = new StringVector();
		sv.addElement(DOCUMENT_ID_ATTRIBUTE);
		sv.addElement(RELEVANCE_ATTRIBUTE);
		sv.addContentIgnoreDuplicates(super.getAttributeNames());
		return sv.toStringArray();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#getAttribute(java.lang.String)
	 */
	public Object getAttribute(String name) {
		if (RELEVANCE_ATTRIBUTE.equals(name)) return ("" + this.relevance);
		else if (DOCUMENT_ID_ATTRIBUTE.equals(name)) return ("" + this.documentId);
		else return super.getAttribute(name);
	}
}
