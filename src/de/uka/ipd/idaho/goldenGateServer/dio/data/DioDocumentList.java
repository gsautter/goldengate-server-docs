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
package de.uka.ipd.idaho.goldenGateServer.dio.data;


import java.io.IOException;
import java.io.Reader;

import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentList;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;

/**
 * List of documents in a GoldenGATE DIO, implemented iterator-style for
 * efficiency.
 * 
 * @author sautter
 */
public abstract class DioDocumentList extends DocumentList implements GoldenGateDioConstants {
	
	/**
	 * Constructor for general use
	 * @param listFieldNames the field names for the document list, in the order
	 *            they should be displayed
	 */
	public DioDocumentList(String[] listFieldNames) {
		super(listFieldNames);
	}
	
	/**
	 * Constructor for creating wrappers
	 * @param model the document list to wrap
	 */
	public DioDocumentList(DocumentList model) {
		super(model);
	}
	
	/**
	 * Constructor for creating wrappers that add fields
	 * @param model the document list to wrap
	 * @param extensionListFieldNames an array holding additional field names
	 */
	public DioDocumentList(DocumentList model, String[] extensionListFieldNames) {
		super(model, extensionListFieldNames);
	}
	
	public boolean hasNoSummary(String listFieldName) {
		return summarylessAttributes.contains(listFieldName);
	}
	
	public boolean isNumeric(String listFieldName) {
		return numericAttributes.contains(listFieldName);
	}
	
	public boolean isFilterable(String listFieldName) {
		return filterableAttributes.contains(listFieldName);
	}
	
	/**
	 * Wrap a document list around a reader, which provides the list's data in
	 * form of a character stream. Do not close the specified reader after this
	 * method returns. The reader is closed by the returned list after the last
	 * document list element is read.
	 * @param in the Reader to read from
	 * @return a document list that makes the data from the specified reader
	 *         available as document list elements
	 * @throws IOException
	 */
	public static DioDocumentList readDocumentList(Reader in) throws IOException {
		return readDocumentList(in, ProgressMonitor.silent);
	}
	
	/**
	 * Wrap a document list around a reader, which provides the list's data in
	 * form of a character stream. Do not close the specified reader after this
	 * method returns. The reader is closed by the returned list after the last
	 * document list element is read.
	 * @param in the Reader to read from
	 * @param pm a progress monitor observing the reading process
	 * @return a document list that makes the data from the specified reader
	 *         available as document list elements
	 * @throws IOException
	 */
	public static DioDocumentList readDocumentList(Reader in, ProgressMonitor pm) throws IOException {
		
		//	wrap DIO specific behavior around generic list
		final DocumentList dl = DocumentList.readDocumentList(in, pm);
		return new DioDocumentList(dl) {
			public boolean hasNextDocument() {
				return dl.hasNextDocument();
			}
			public DocumentListElement getNextDocument() {
				return dl.getNextDocument();
			}
			public int getDocumentCount() {
				return dl.getDocumentCount();
			}
			public int getRetrievedDocumentCount() {
				return dl.getRetrievedDocumentCount();
			}
			public int getRemainingDocumentCount() {
				return dl.getRemainingDocumentCount();
			}
			public boolean hasNoSummary(String listFieldName) {
				return (super.hasNoSummary(listFieldName) || dl.hasNoSummary(listFieldName));
			}
			public boolean isNumeric(String listFieldName) {
				return (super.isNumeric(listFieldName) || dl.isNumeric(listFieldName));
			}
			public boolean isFilterable(String listFieldName) {
				return (super.isFilterable(listFieldName) || dl.isFilterable(listFieldName));
			}
		};
	}
}