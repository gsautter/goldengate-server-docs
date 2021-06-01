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


import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;

/**
 * The List of the SRS documents
 * 
 * @author sautter
 */
public abstract class DocumentList extends CsvResult {
	
	/** Constructor
	 * @param dataFieldNames
	 */
	public DocumentList(String[] dataFieldNames) {
		super(dataFieldNames);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getNextElement()
	 */
	public DocumentListElement getNextDocumentListElement() {
		return ((DocumentListElement) this.getNextElement());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getStartTagAttributes()
	 */
	public Properties getStartTagAttributes() {
		if (this.startTagAttributes == null)
			this.startTagAttributes = new Properties();
		return this.startTagAttributes;
	}
	private Properties startTagAttributes = null;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.CsvResult#newResultElement()
	 */
	protected CsvResultElement newResultElement() {
		return new DocumentListElement();
	}
	
	/**
	 * Read a document list from its XML representation, provided by some
	 * Reader. Do not close the Reader when this method returns, since the
	 * returned document list obtains its elements from the reader on the fly,
	 * i.e., as they are retrieved via one of the getNextElement() or
	 * getNextIndexResultElement() methods. The document list will close the
	 * Reader automatically when the backing data is completely read.
	 * @param in the Reader to read from
	 * @return a document list backed by the XML data
	 * @throws IOException
	 */
	public static DocumentList readDocumentList(Reader in) throws IOException {
		ResultBuilder rb = new DocumentListBuilder(in);
		return ((DocumentList) rb.getResult());
	}
	
	private static class DocumentListBuilder extends ResultBuilder {
		DocumentListBuilder(Reader in) throws IOException {
			super(in);
		}
//		protected SrsSearchResult buildResult(String[] resultAttributes, Properties attributes) {
		SrsSearchResult buildResult(String[] resultAttributes, TreeNodeAttributeSet attributes) {
			return new DocumentList(resultAttributes) {
				public boolean hasNextElement() {
					return DocumentListBuilder.this.hasNextElement();
				}
				public SrsSearchResultElement getNextElement() {
					return DocumentListBuilder.this.getNextElement();
				}
			};
		}
	}
}
