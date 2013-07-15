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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData;

import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;

/**
 * @author sautter
 *
 */
public class BufferedIndexResult extends BufferedResult {
	
	/** the name of the index this result comes from */
	public final String indexName;
	
	/** the name of the index this result comes from */
	public final String indexLabel;
	
	private IndexResult data;
	
	/**
	 * @param data
	 */
	public BufferedIndexResult(final IndexResult data) {
		super(new IndexResult(data.resultAttributes, data.indexName, data.indexLabel) {
			public boolean hasNextElement() {
				return data.hasNextElement();
			}
			public SrsSearchResultElement getNextElement() {
				IndexResultElement ire = ((IndexResultElement) data.getNextElement());
				if (ire == null) return null;
				else if (ire instanceof BufferedIndexResultElement) return ire;
				else return new BufferedIndexResultElement(ire);
			}
		});
		this.data = data;
		this.indexName = this.data.indexName;
		this.indexLabel = this.data.indexLabel;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedResult#getResult()
	 */
	public IndexResult getIndexResult() {
		final SrsSearchResult result = this.getResult();
		return new IndexResult(result.resultAttributes, this.data.indexName, this.data.indexLabel) {
			public boolean hasNextElement() {
				return result.hasNextElement();
			}
			public SrsSearchResultElement getNextElement() {
				return ((IndexResultElement) result.getNextElement());
			}
		};
	}
	
	static class BufferedIndexResultElement extends IndexResultElement {
		
		private BufferedIndexResult[] subResults;
		
		BufferedIndexResultElement(IndexResultElement ire) {
			super(ire.docNr, ire.getType(), ire.getValue());
			this.copyAttributes(ire);
			IndexResult[] subResults = ire.getSubResults();
			this.subResults = new BufferedIndexResult[subResults.length];
			for (int s = 0; s < subResults.length; s++)
				this.subResults[s] = new BufferedIndexResult(subResults[s]);
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResultElement#getSubResults()
		 */
		public IndexResult[] getSubResults() {
			IndexResult[] subResults = new IndexResult[this.subResults.length];
			for (int s = 0; s < this.subResults.length; s++)
				subResults[s] = this.subResults[s].getIndexResult();
			return subResults;
		}
	}
}
