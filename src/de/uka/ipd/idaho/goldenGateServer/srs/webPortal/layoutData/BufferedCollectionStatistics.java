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

import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;

/**
 * @author sautter
 *
 */
public class BufferedCollectionStatistics extends BufferedResult {
	
	/**	the number of master documents in the document collection */
	public final int masterDocCount;
	
	/**	the number of individually retrievable text units in the document collection */
	public final int docCount;
	
	/**	the total number of words in the document collection */
	public final int wordCount;
	
	/** the timestamp since the time relative statistics are counted (as a numeric UTC timestamp or relative time string constant) */
	public final String since;
	
	/**	the number of master documents in the document collection since a given time */
	public final int masterDocCountSince;
	
	/**	the number of individually retrievable text units in the document collection since a given time */
	public final int docCountSince;
	
	/**	the total number of words in the document collection since a given time */
	public final int wordCountSince;
	
	private CollectionStatistics data;
	
	/**
	 * @param data
	 */
	public BufferedCollectionStatistics(CollectionStatistics data) {
		super(data);
		this.data = data;
		this.masterDocCount = this.data.masterDocCount;
		this.docCount = this.data.docCount;
		this.wordCount = this.data.wordCount;
		this.since = this.data.since;
		this.masterDocCountSince = this.data.masterDocCountSince;
		this.docCountSince = this.data.docCountSince;
		this.wordCountSince = this.data.wordCountSince;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.layoutData.BufferedResult#getResult()
	 */
	public CollectionStatistics getCollectionStatistics() {
		final SrsSearchResult result = this.getResult();
		return new CollectionStatistics(result.resultAttributes, this.data.masterDocCount, this.data.docCount, this.data.wordCount, this.data.since, this.data.masterDocCountSince, this.data.docCountSince, this.data.wordCountSince) {
			public boolean hasNextElement() {
				return result.hasNextElement();
			}
			public SrsSearchResultElement getNextElement() {
				return result.getNextElement();
			}
		};
	}
}
