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
package de.uka.ipd.idaho.goldenGateServer.srs;


import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

/**
 * The (ranked) result of a query to the SRS 
 * 
 * @author sautter
 */
public class QueryResult {
	
	/**
	 * Comparator ordering query result elements by document number. This is
	 * helpful for joining and merging query results, etc.
	 */
	public static final Comparator docNumberSortOrder = new Comparator() {
		public int compare(Object o1, Object o2) {
			long docNr1 = ((QueryResultElement) o1).docNr;
			long docNr2 = ((QueryResultElement) o2).docNr;
			if (docNr1 == docNr2)
				return 0;
			else if (docNr1 < docNr2)
				return -1;
			else return 1;
		}
	};
	
	/**
	 * Comparator ordering query result elements by descending relevance. This
	 * is helpful for ranking elements in query results, etc.
	 */
	public static final Comparator decreasingRelevanceSortOrder = new Comparator() {
		public int compare(Object o1, Object o2) {
			return Double.compare(((QueryResultElement) o2).relevance, ((QueryResultElement) o1).relevance);
		}
	};
	
	private QueryResultElement[] elements = new QueryResultElement[16];
	private int size = 0;
	private int modCount = 0;
	private int cleanModCount = 0;
	
	private int maxSize = 0;
	private boolean isSortedDescending = true;
	
	private int mergeCount = 1;
	
	/**	Constructor
	 */
	public QueryResult() {
		this(0, true);
	}
	
	/**	Constructor
	 * @param	keepSorted	if set to true, the ResultElements of this QueryResult will be kept sorted by their relevance in descending order
	 */
	public QueryResult(boolean keepSorted) {
		this(0, keepSorted);
	}
	
	/**	Constructor
	 * @param	maxSize		if set to a value greater than zero, this QueryResult's size will be restricted to at most maxSize elements, always pruning the least relevant QueryResultElement if the insertion of a new QueryResultElement causes the size to exceed the limit
	 */
	public QueryResult(int maxSize) {
		this(maxSize, true);
	}
	
	/**	Constructor
	 * @param	maxSize		if set to a value greater than zero, this QueryResult's size will be restricted to at most maxSize elements, always pruning the least relevant QueryResultElement if the insertion of a new QueryResultElement causes the size to exceed the limit
	 * @param	keepSorted	if set to true, the ResultElements of this QueryResult will be kept sorted by their relevance in descending order
	 */
	public QueryResult(int maxSize, boolean keepSorted) {
//		this.keepSorted = keepSorted;
//		this.isSortedByRelevance = keepSorted;
		this.maxSize = ((maxSize > 0) ? maxSize : 0);
	}
	
	/**
	 * Add a QueryResultElement to this QueryResult
	 * 	Attention: if a maximum size was specified and the new QueryResultElement causes this QueryResult's size to exceed this limit, the least relevant QueryResultElement will be pruned
	 */
	public void addResultElement(QueryResultElement qre) {
		
		//	ensure capacity
		if (this.elements.length == this.size) {
			QueryResultElement[] elements = new QueryResultElement[this.size * 2];
			System.arraycopy(this.elements, 0, elements, 0, this.size);
			this.elements = elements;
		}
		
		//	add result element
		this.elements[this.size++] = qre;
		this.modCount++;
		
		//	ensure that size limit is not exceeded by too much
		if ((0 < this.maxSize) && ((this.maxSize * 3) < (this.size * 2)))
			this.ensureClean();
	}
	
	private void ensureClean() {
		if (this.modCount == this.cleanModCount)
			return;
		Arrays.sort(this.elements, 0, this.size, (this.isSortedDescending ? decreasingRelevanceSortOrder : Collections.reverseOrder(decreasingRelevanceSortOrder)));
		if ((0 < this.maxSize) && (this.maxSize < this.size)) {
			if (!this.isSortedDescending)
				System.arraycopy(this.elements, (this.size - this.maxSize), this.elements, 0, this.maxSize);
			Arrays.fill(this.elements, this.maxSize, this.size, null);
			this.size = this.maxSize;
		}
		this.cleanModCount = this.modCount;
	}
	
	/**	@return	the index-th QueryResultElement of this QueryResult, or null if the specified index exceeds the number of contained ResultElements
	 */
	public QueryResultElement getResult(int index) {
		this.ensureClean();
		if ((0 <= index) && (index < this.size))
			return this.elements[index];
		else return null;
	}
	
	/**	@return	the number of QueryResultElement contained in this QueryResult
	 */
	public int size() {
		this.ensureClean();
		return this.size;
	}
	
	/**	@return	the ResultElements contained in this QueryResult, packed in an array
	 */
	public QueryResultElement[] getContentArray() {
		this.ensureClean();
		QueryResultElement[] qres = new QueryResultElement[this.size];
		System.arraycopy(this.elements, 0, qres, 0, this.size);
		return qres;
	}
	
	/**
	 * Prune all result elements whose relevance is less than a given threshold.
	 * @param threshold the relevance below which to remove result elements
	 * @return the number of removed result elements
	 */
	public int pruneByRelevance(double threshold) {
		if ((threshold <= 0) || (1 <= threshold))
			return 0;
		this.ensureClean();
		int removed = 0;
		for (int e = 0; e < this.size; e++) {
			if (this.elements[e].relevance < threshold)
				removed++;
			else if (removed != 0)
				this.elements[e - removed] = this.elements[e];
		}
		Arrays.fill(this.elements, (this.size - removed), this.size, null);
		this.size -= removed;
		return removed;
	}
	
	/**
	 * Keep the <code>sizeThreshold</code> most relevant elements of this query
	 * result, prune the rest.
	 * @param sizeThreshold the size threshold
	 * @return the number of removed result elements
	 */
	public int pruneToSize(int sizeThreshold) {
		return this.pruneToSize(sizeThreshold, false);
	}
	
	/**	
	 * Keep the <code>sizeThreshold</code> most relevant elements of this query
	 * result, prune the rest. If <code>preserveSize</code> is true, the number
	 * of elements contained in this query result will be capped like this in
	 * future updates.
	 * @param sizeThreshold the size threshold
	 * @param preserveSize establish the threshold as a new maximum size?
	 * @return the number of removed result elements
	 */
	public int pruneToSize(int sizeThreshold, boolean preserveSize) {
		if (sizeThreshold < 1)
			return 0;
		this.ensureClean();
		if (this.size <= sizeThreshold)
			return 0;
		if (!this.isSortedDescending)
			System.arraycopy(this.elements, (this.size - sizeThreshold), this.elements, 0, sizeThreshold);
		Arrays.fill(this.elements, sizeThreshold, this.size, null);
		int removed = (this.size - sizeThreshold);
		this.size = sizeThreshold;
		if (preserveSize && (sizeThreshold < this.maxSize))
			this.maxSize = sizeThreshold;
		return removed;
	}
	
	/**	
	 * Sort the elements in this query result by their relevance.
	 * @param descending sort descending (the default)?
	 */
	public void sortByRelevance(boolean descending) {
		if (this.isSortedDescending == descending)
			return;
		this.isSortedDescending = descending;
		this.modCount++;
	}
	
	/**	
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		this.ensureClean();
		StringBuffer ret = new StringBuffer();
		ret.append("<result>");
		for (int i = 0; i < this.size; i++)
			ret.append(this.elements[i].toString());
		ret.append("</result>");
		return ret.toString();
	}
	
	/**
	 * Merge a QueryResult with this one.
	 * @param 	qr						the QueryResult to be merged with this one
	 * @param 	relevanceCombination	the mode to combine relevance values
	 * @param 	maxSize					the maximum size of the result
	 * @return	a new QueryResult that contains the ResultElements contained in this or the argument QueryResult
	 */
	public QueryResult merge(QueryResult qr, int relevanceCombination, int maxSize) {
		return merge(this, qr, relevanceCombination, maxSize);
	}
	
	/**
	 * Join a QueryResult with this one.
	 * @param 	qr						the QueryResult to be joined with this one
	 * @param 	relevanceCombination	the mode to combine relevance values
	 * @param 	maxSize					the maximum size of the result
	 * @return	a new QueryResult that contains the ResultElements contained in both this and the argument QueryResult
	 */
	public QueryResult join(QueryResult qr, int relevanceCombination, int maxSize) {
		return join(this, qr, relevanceCombination, maxSize);
	}
	
	/**	Constant for defining the fashion of combining the relevance values 
	 * 	of two ResultElements representing the same document (matching IDs),
	 * 	thus which relevance value is assigned to the QueryResultElement in the
	 * 	combined QueryResult:<br>
	 * <br>
	 * 	be r1 the relevance of the QueryResultElement from this QueryResult<br>
	 * 	be r2 the relevance of the QueryResultElement from the argument QueryResult<br>
	 * 	be r the relevance of the QueryResultElement stored in the combined QueryResult<br>
	 * <br>
	 * 	USE_MIN				r = min(r1, r2)
	 */
	public static final int USE_MIN = 0;
	
	/**	Constant for defining the fashion of combining the relevance values 
	 * 	of two ResultElements representing the same document (matching IDs),
	 * 	thus which relevance value is assigned to the QueryResultElement in the
	 * 	combined QueryResult:<br>
	 * <br>
	 * 	be r1 the relevance of the QueryResultElement from this QueryResult<br>
	 * 	be r2 the relevance of the QueryResultElement from the argument QueryResult<br>
	 * 	be r the relevance of the QueryResultElement stored in the combined QueryResult<br>
	 * <br>
	 * 	USE_AVERAGE			r = (r1 + r2) / 2<br>
	 */
	public static final int USE_AVERAGE = 1;
	
	/**	Constant for defining the fashion of combining the relevance values 
	 * 	of two ResultElements representing the same document (matching IDs),
	 * 	thus which relevance value is assigned to the QueryResultElement in the
	 * 	combined QueryResult:<br>
	 * <br>
	 * 	be r1 the relevance of the QueryResultElement from this QueryResult<br>
	 * 	be r2 the relevance of the QueryResultElement from the argument QueryResult<br>
	 * 	be r the relevance of the QueryResultElement stored in the combined QueryResult<br>
	 * <br>
	 * 	USE_MAX				r = max(r1, r2)
	 */
	public static final int USE_MAX = 2;
	
	/**	Constant for defining the fashion of combining the relevance values 
	 * 	of two ResultElements representing the same document (matching IDs),
	 * 	thus which relevance value is assigned to the QueryResultElement in the
	 * 	combined QueryResult:<br>
	 * <br>
	 * 	be r1 the relevance of the QueryResultElement from this QueryResult<br>
	 * 	be r2 the relevance of the QueryResultElement from the argument QueryResult<br>
	 * 	be r the relevance of the QueryResultElement stored in the combined QueryResult<br>
	 * <br>
	 * 	MULTIPLY				r = r1 * r2
	 */
	public static final int MULTIPLY = 3;
	
	/**	Constant for defining the fashion of combining the relevance values 
	 * 	of two ResultElements representing the same document (matching IDs),
	 * 	thus which relevance value is assigned to the QueryResultElement in the
	 * 	combined QueryResult:<br>
	 * <br>
	 * 	be r1 the relevance of the QueryResultElement from this QueryResult<br>
	 * 	be r2 the relevance of the QueryResultElement from the argument QueryResult<br>
	 * 	be r the relevance of the QueryResultElement stored in the combined QueryResult<br>
	 * <br>
	 * 	INVERSE_MULTIPLY		r = 1 - ((1 - r1) * (1 - r2))
	 */
	public static final int INVERSE_MULTIPLY = 4;
	
	/**
	 * Merge two query results, keeping only the elements contained in both of them.
	 * @param 	queryResult1			the first QueryResult to be merged
	 * @param 	queryResult2			the second QueryResult to be merged
	 * @param 	relevanceCombination	the mode to combine the relevance values
	 * @param 	maxSize					the maximum size for the result
	 * @return	a new QueryResult that contains all the ResultElements contained in either one of the argument QueryResults
	 */
	public static QueryResult merge(QueryResult queryResult1, QueryResult queryResult2, int relevanceCombination, int maxSize) {
		
		//	check parameters
		if ((queryResult1 == null) && (queryResult2 == null))
			return null;
		if (queryResult1 == null)
			return queryResult2;
		if (queryResult2 == null)
			return queryResult1;
		
		//	produce result
		QueryResult result = new QueryResult(maxSize);
		result.mergeCount = queryResult1.mergeCount + queryResult2.mergeCount;
		
		//	check parameter'c content
		if ((queryResult1.size() == 0) && (queryResult2.size() == 0)) return result;
		
		//	check if merge computation necessary
		if (queryResult1.size() == 0) {
			if (relevanceCombination == USE_MIN)
				return queryResult1;
			else if (relevanceCombination == MULTIPLY)
				return queryResult1;
			else if (relevanceCombination == USE_MAX)
				return queryResult2;
			else if (relevanceCombination == INVERSE_MULTIPLY)
				return queryResult2;
		}
		if (queryResult2.size() == 0) {
			if (relevanceCombination == USE_MIN)
				return queryResult2;
			else if (relevanceCombination == MULTIPLY)
				return queryResult2;
			else if (relevanceCombination == USE_MAX)
				return queryResult1;
			else if (relevanceCombination == INVERSE_MULTIPLY)
				return queryResult1;
		}
		
		//	get content arrays
		QueryResultElement[] qres1 = queryResult1.getContentArray();
		QueryResultElement[] qres2 = queryResult2.getContentArray();
		
		//	perform sort-merge full outer sort join
		Arrays.sort(qres1, docNumberSortOrder);
		Arrays.sort(qres2, docNumberSortOrder);
		for (int index1 = 0, index2 = 0; (index1 < qres1.length) || (index2 < qres2.length);) {
			int c;
			if ((index1 < qres1.length) && (index2 < qres2.length))
				c = docNumberSortOrder.compare(qres1[index1], qres2[index2]);
			else if (index1 < qres1.length)
				c = -1;
			else c = 1;
			QueryResultElement qre;
			
			//	ResultElements with matching IDs, join them
			if (c == 0) {
				qre = new QueryResultElement(qres1[index1].docNr, getCombinedRelevance(qres1[index1].relevance, queryResult1.mergeCount, qres2[index2].relevance, queryResult2.mergeCount, relevanceCombination));
				index1++;
				index2++;
			}
			
			//	ID of r1 is smaller QueryResultElement, increment index1
			else if (c < 0) {
				qre = new QueryResultElement(qres1[index1].docNr, getCombinedRelevance(qres1[index1].relevance, queryResult1.mergeCount, getMergeDummyRelevance(qres1[index1].relevance, relevanceCombination), 1, relevanceCombination));
				index1++;
			}
			
			//	ID of r2 is smaller QueryResultElement, increment index2
			else {
				qre = new QueryResultElement(qres2[index2].docNr, getCombinedRelevance(qres2[index2].relevance, queryResult2.mergeCount, getMergeDummyRelevance(qres2[index2].relevance, relevanceCombination), 1, relevanceCombination));
				index2++;
			}
			
			//	store combined element
			result.addResultElement(qre);
		}
		
		return result;
	}
	
	//	get a dummy value for the relevance of result elements that don't have a merge partner
	private static double getMergeDummyRelevance(double relevance, int relevanceCombination) {
		switch (relevanceCombination) {
			case USE_MIN: return 0;
			case USE_AVERAGE: return 0;
			case USE_MAX: return relevance;
			case MULTIPLY: return 0;
			case INVERSE_MULTIPLY: return 0;
		}
		return 0;
	}
	
	/**
	 * Join two query result, keeping only the elements contained in both of them.
	 * @param 	queryResult1			the first QueryResult to be joined
	 * @param 	queryResult2			the second QueryResult to be joined
	 * @param 	relevanceCombination	the mode to combine the relevance values
	 * @param 	maxSize					the maximum size for the result
	 * @return	a new QueryResult that contains only the ResultElements contained in both of the argument QueryResults
	 */
	public static QueryResult join(QueryResult queryResult1, QueryResult queryResult2, int relevanceCombination, int maxSize) {
		
		//	check parameters
		if ((queryResult1 == null) && (queryResult2 == null))
			return null;
		if (queryResult1 == null)
			return new QueryResult(maxSize);
		if (queryResult2 == null)
			return new QueryResult(maxSize);
		
		//	produce result
		QueryResult result = new QueryResult(maxSize);
		result.mergeCount = queryResult1.mergeCount + queryResult2.mergeCount;
		
		//	get content arrays
		QueryResultElement[] qres1 = queryResult1.getContentArray();
		QueryResultElement[] qres2 = queryResult2.getContentArray();
		
		//	perform sort-merge inner sort join
		Arrays.sort(qres1, docNumberSortOrder);
		Arrays.sort(qres2, docNumberSortOrder);
		for (int index1 = 0, index2 = 0; (index1 < qres1.length) && (index2 < qres2.length);) {
			int c = docNumberSortOrder.compare(qres1[index1], qres2[index2]);
			
			//	ResultElements with matching IDs, join them
			if (c == 0) {
				QueryResultElement qre = new QueryResultElement(qres1[index1].docNr, getCombinedRelevance(qres1[index1].relevance, queryResult1.mergeCount, qres2[index2].relevance, queryResult2.mergeCount, relevanceCombination));
				result.addResultElement(qre);
				index1++;
				index2++;
			}
			
			//	ID of r1 is smaller, increment index1
			else if (c < 0)
				index1++;
			
			//	ID of r2 is smaller, increment index2
			else index2++;
		}
		
		return result;
	}
	
	private static double getCombinedRelevance(double relevance1, int mergeCount1, double relevance2, int mergeCount2, int relevanceCombination) {
		switch (relevanceCombination) {
			case USE_MIN: return ((relevance1 < relevance2) ? relevance1 : relevance2);
			case USE_AVERAGE: return ((((relevance1 + relevance2) > 0) && ((mergeCount1 + mergeCount2) > 0)) ? 
					(((relevance1 * mergeCount1) + (relevance2 * mergeCount2)) / (mergeCount1 + mergeCount2)) : 0);
			case USE_MAX: return ((relevance1 > relevance2) ? relevance1 : relevance2);
			case MULTIPLY: return (relevance1 * relevance2);
			case INVERSE_MULTIPLY: return (1 - ((1 - relevance1) * (1 - relevance2)));
		}
		return 0;
	}
}
