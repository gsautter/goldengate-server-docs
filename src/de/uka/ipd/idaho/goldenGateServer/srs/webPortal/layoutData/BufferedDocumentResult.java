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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData;

import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;

/**
 * @author sautter
 *
 */
public class BufferedDocumentResult extends BufferedResult {
	
	/**
	 * @param data
	 */
	public BufferedDocumentResult(final DocumentResult data) {
		super(new DocumentResult(data.resultAttributes) {
			public boolean hasNextElement() {
				return data.hasNextElement();
			}
			public SrsSearchResultElement getNextElement() {
				DocumentResultElement dre = ((DocumentResultElement) data.getNextElement());
				if (dre == null) return null;
				else if (dre instanceof BufferedDocumentResultElement) return dre;
				else return new BufferedDocumentResultElement(dre);
			}
		});
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedResult#getResult()
	 */
	public DocumentResult getDocumentResult() {
		final SrsSearchResult result = this.getResult();
		return new DocumentResult(result.resultAttributes) {
			public boolean hasNextElement() {
				return result.hasNextElement();
			}
			public SrsSearchResultElement getNextElement() {
				return ((DocumentResultElement) result.getNextElement());
			}
		};
	}
	
	static class BufferedDocumentResultElement extends DocumentResultElement {
		private BufferedIndexResult[] subResults;
		BufferedDocumentResultElement(DocumentResultElement dre) {
			super(dre.docNr, dre.documentId, dre.relevance, dre.document);
			this.copyAttributes(dre);
			IndexResult[] subResults = dre.getSubResults();
			this.subResults = new BufferedIndexResult[subResults.length];
			for (int s = 0; s < subResults.length; s++)
				this.subResults[s] = new BufferedIndexResult(subResults[s]);
		}
		public IndexResult[] getSubResults() {
			IndexResult[] subResults = new IndexResult[this.subResults.length];
			for (int s = 0; s < this.subResults.length; s++)
				subResults[s] = this.subResults[s].getIndexResult(BufferedDocumentResultElement.this);
			return subResults;
		}
	}
}
