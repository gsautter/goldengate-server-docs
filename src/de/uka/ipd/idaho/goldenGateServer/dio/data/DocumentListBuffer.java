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
 *     * Neither the name of the Universität Karlsruhe (TH) nor the
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
package de.uka.ipd.idaho.goldenGateServer.dio.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentList.DocumentAttributeSummary;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * List of documents in a GoldenGATE DIO, residing in memory to facilitate
 * sorting and the like in a client side document listing.
 * 
 * @author sautter
 */
public class DocumentListBuffer extends StringRelation implements GoldenGateDioConstants {
	
	/**
	 * the field names for the document list, in the order they should be
	 * displayed
	 */
	public final String[] listFieldNames;
	
	/**
	 * Constructor
	 * @param listFieldNames the field names for the document list, in the order
	 *            they should be displayed
	 */
	public DocumentListBuffer(String[] listFieldNames) {
		this.listFieldNames = listFieldNames;
	}
	
	/**
	 * Constructor building a buffered document list around an iterator-style
	 * one, transfering all document list elements from the latter into the
	 * buffer, thereby consuming them from the argument list's getNextDocument()
	 * method until the hasNextDocument() method returns false.
	 * @param data the source document list
	 */
	public DocumentListBuffer(DocumentList data) {
		this(data, null);
	}
	
	/**
	 * Constructor building a buffered document list around an iterator-style
	 * one, transfering all document list elements from the latter into the
	 * buffer, thereby consuming them from the argument list's getNextDocument()
	 * method until the hasNextDocument() method returns false.
	 * @param data the source document list
	 * @param pm a progress monitor observing the document list being read
	 */
	public DocumentListBuffer(DocumentList data, ProgressMonitor pm) {
		this.listFieldNames = data.listFieldNames;
		
		while (data.hasNextDocument()) {
			DocumentListElement dle = data.getNextDocument();
			StringTupel st = new StringTupel();
			for (int f = 0; f < this.listFieldNames.length; f++) {
				Object value = dle.getAttribute(this.listFieldNames[f]);
				if ((value != null) && (value instanceof String))
					st.setValue(this.listFieldNames[f], AnnotationUtils.unescapeFromXml(value.toString()));
			}
			this.addElement(st);
			
			if (pm == null)
				continue;
			pm.setInfo("" + this.size() + " documents read.");
			int docCount = data.getDocumentCount();
			if (docCount > 0)
				pm.setProgress((this.size() * 100) / docCount);
		}
		
		for (int f = 0; f < this.listFieldNames.length; f++) {
			DocumentAttributeSummary listFieldValues = data.getListFieldValues(this.listFieldNames[f]);
			if (listFieldValues != null)
				this.listFieldValues.put(this.listFieldNames[f], listFieldValues);
		}
		if (this.listFieldValues.isEmpty()) {
			for (int f = 0; f < this.listFieldNames.length; f++) {
				if (summarylessAttributes.contains(this.listFieldNames[f]))
					continue;
				this.listFieldValues.put(this.listFieldNames[f], new DocumentAttributeSummary());
			}
			for (int d = 0; d < this.size(); d++) {
				StringTupel docData = this.get(d);
				for (int f = 0; f < this.listFieldNames.length; f++) {
					if (summarylessAttributes.contains(this.listFieldNames[f]))
						continue;
					String fieldValue = docData.getValue(this.listFieldNames[f]);
					if ((fieldValue == null) || (fieldValue.length() == 0))
						continue;
					this.getListFieldValues(this.listFieldNames[f]).add(fieldValue);
				}
			}
			for (int f = 0; f < this.listFieldNames.length; f++) {
				if (summarylessAttributes.contains(this.listFieldNames[f]))
					continue;
				if (this.getListFieldValues(this.listFieldNames[f]).size() == 0)
					this.listFieldValues.remove(this.listFieldNames[f]);
			}
		}
		
		if (pm != null) {
			pm.setInfo("Document summary data read.");
			pm.setProgress(100);
		}
	}
	
	/**
	 * Retrieve a summary of the values in a list field. The sets returned by
	 * this method are immutable. If there is no summary, this method returns
	 * null, but never an empty set. The set does not contain nulls and is
	 * sorted lexicographically.
	 * @param listFieldName the name of the field
	 * @return a set containing the summary values
	 */
	public DocumentAttributeSummary getListFieldValues(String listFieldName) {
		return ((DocumentAttributeSummary) this.listFieldValues.get(listFieldName));
	}
	
	private Map listFieldValues = new HashMap();
	
	/**
	 * Write this document list to some writer as XML data.
	 * @param out the Writer to write to
	 * @throws IOException
	 */
	public void writeXml(Writer out) throws IOException {
		
		//	produce writer
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	write empty data
		if (this.size() == 0) {
			
			//	write results
			StringVector listFields = new StringVector();
			listFields.addContent(this.listFieldNames);
			buf.write("<" + DocumentList.DOCUMENT_LIST_NODE_NAME + 
					" " + DocumentList.DOCUMENT_LIST_FIELDS_ATTRIBUTE + "=\"" + listFields.concatStrings(" ") + "\"" +
			"/>");
			buf.newLine();
		}
		
		//	write data
		else {
			
			//	get result field names
			StringVector listFields = new StringVector();
			listFields.addContent(this.listFieldNames);
			buf.write("<" + DocumentList.DOCUMENT_LIST_NODE_NAME + 
					" " + DocumentList.DOCUMENT_LIST_FIELDS_ATTRIBUTE + "=\"" + listFields.concatStrings(" ") + "\"" +
					">");
			
			for (int r = 0; r < this.size(); r++) {
				StringTupel dle = this.get(r);
				buf.write("  <" + DocumentList.DOCUMENT_NODE_NAME);
				for (int f = 0; f < listFields.size(); f++) {
					String listField = listFields.get(f);
					String listFieldValue = dle.getValue(listField);
					if ((listFieldValue != null) && (listFieldValue.length() != 0))
						buf.write(" " + listField + "=\"" + AnnotationUtils.escapeForXml(listFieldValue, true) + "\"");
				}
				buf.write("/>");
				buf.newLine();
			}
			
			buf.write("</" + DocumentList.DOCUMENT_LIST_NODE_NAME + ">");
			buf.newLine();
		}
		
		buf.flush();
	}
}
