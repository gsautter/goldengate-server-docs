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
package de.uka.ipd.idaho.goldenGateServer.srs;


import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

/**
 * The (ranked) result of a query to the SRS 
 * 
 * @author sautter
 */
public class QueryResult {
	
	private static final Comparator idSortOrder = new Comparator() {
		public int compare(Object o1, Object o2) {
//			return ((QueryResultElement) o1).docNr - ((QueryResultElement) o2).docNr;
			long docNr1 = ((QueryResultElement) o1).docNr;
			long docNr2 = ((QueryResultElement) o2).docNr;
			if (docNr1 == docNr2)
				return 0;
			else if (docNr1 < docNr2)
				return -1;
			else return 1;
		}
	};
	
	private static final Comparator relevanceSortOrder = new Comparator() {
		public int compare(Object o1, Object o2) {
			return Double.compare(((QueryResultElement) o2).relevance, ((QueryResultElement) o1).relevance);
		}
	};
	
	private Vector elements = new Vector();
	
	private boolean keepSorted = true;
	private int maxSize = 0;
	
	private boolean isSortedByRelevance = true;
	private boolean isSortedByID = false;
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
		this.keepSorted = keepSorted;
		this.isSortedByRelevance = keepSorted;
		this.maxSize = ((maxSize > 0) ? maxSize : 0);
	}
	
	/**
	 * Add a QueryResultElement to this QueryResult
	 * 	Attention: if a maximum size was specified and the new QueryResultElement causes this QueryResult's size to exceed this limit, the least relevant QueryResultElement will be pruned
	 */
	public void addResultElement(QueryResultElement qre) {
		
		//	insert new QueryResultElement at appropriate index
		if (this.keepSorted) {
			
			int index = (this.elements.size() + 1) / 2;
			int step = (index + 1) / 2;
			boolean done = (this.elements.size() == 1);
			
			//	find the appropriate index ...
			while (!done && (index > 0) && (index < this.elements.size())) {
				
				//	get relevance values of ResultElements directly neighbored to the current index position
				double before = ((QueryResultElement) this.elements.get(index-1)).relevance;
				double after = ((QueryResultElement) this.elements.get(index)).relevance;
				
				//	move downward
				if (((this.isSortedDescending) ? (qre.relevance > before) : (qre.relevance < before))) {
					index -= step;
					step = (step + 1) / 2;
				}
				
				//	move upward
				else if (((this.isSortedDescending) ? (qre.relevance <= after) : (qre.relevance >= after))) {
					index += step;
					step = (step + 1) / 2;
				}
				
				//	found insertion spot
				else done = true;
			}
			
			//	keep the index within the bounds
			index = ((index >= 0) ? index : 0);
			index = ((index <= this.elements.size()) ? index : this.elements.size());
			
			//	... and insert the new QueryResultElement
			//	be careful if there is only one QueryResultElement so far
			if (this.elements.size() == 1)
				this.elements.insertElementAt(qre, (((((QueryResultElement) this.elements.get(0)).relevance > qre.relevance) == this.isSortedDescending) ? 1 : 0));
			else this.elements.insertElementAt(qre, index);
		}
		
		//	simply add the new QueryResultElement
		else this.elements.addElement(qre);
		
		//	ensure that the size limit is not exceeded
		if ((this.maxSize > 0) && (this.elements.size() > this.maxSize))
			this.removeLeastRelevantElement();
	}
	
	/**	@return	the index-th QueryResultElement of this QueryResult, or null if the specified index exceeds the number of contained ResultElements
	 */
	public QueryResultElement getResult(int index) {
		if ((index > -1) && (index < this.elements.size()))
			return ((QueryResultElement) this.elements.get(index));
		else return null;
	}
	
	/**	@return	the number of QueryResultElement contained in this QueryResult
	 */
	public int size() {
		return this.elements.size();
	}
	
	/**	@return	the ResultElements contained in this QueryResult, packed in an array
	 */
	public QueryResultElement[] getContentArray() {
		QueryResultElement[] res = new QueryResultElement[this.elements.size()];
		for (int e = 0; e < this.elements.size(); e++)
			res[e] = ((QueryResultElement) this.elements.get(e));
		return res;
	}
	
	/**
	 * Prune all ResultElements whose relevance is less than the specified threshold.
	 */
	public void pruneByRelevance(double threshold) {
		if ((threshold <= 0) || (1 <= threshold))
			return;
		for (int e = 0; e < this.elements.size(); e++) {
			if (((QueryResultElement) this.elements.get(e)).relevance < threshold)
				this.elements.removeElementAt(e--);
		}
	}
	
	/**
	 * Keep the sizeThreshold most relevant ResultElements of this QueryResult, prune the rest.
	 * @param	sizeThreshold	the size threshold
	 */
	public void pruneToSize(int sizeThreshold) {
		this.pruneToSize(sizeThreshold, false);
	}
	
	/**	keep the sizeThreshold most relevant ResultElements of this QueryResult, prune the rest
	 * @param	sizeThreshold	the size threshold
	 * @param	preserveSize 	if set to true, the number of ResultElements contained in this QueryResult will be restricted to sizeThreshold in the future
	 */
	public void pruneToSize(int sizeThreshold, boolean preserveSize) {
		if (sizeThreshold < 1)
			return;
		while (this.elements.size() > sizeThreshold)
			this.removeLeastRelevantElement();
		if (preserveSize && (this.maxSize > sizeThreshold))
			this.maxSize = sizeThreshold;
	}
	
	/**	
	 * Sort this QueryResult's ResultElements by their relevance.
	 * @param	descending	if set to true, the ResultElements will be sorted in descending order, ascending order otherwise
	 */
	public void sortByRelevance(boolean descending) {
		if (this.isSortedByRelevance && (this.isSortedDescending == descending))
			return;
		
		Collections.sort(this.elements, (descending ? Collections.reverseOrder(relevanceSortOrder) : relevanceSortOrder));
		
		this.isSortedByRelevance = true;
		this.isSortedByID = false;
		this.isSortedDescending = descending;
//		//	check if there's something to do
//		if (!this.isSortedByRelevance || (this.isSortedDescending != descending)) {
//			Collections.sort(this.elements, (descending ? Collections.reverseOrder(relevanceSortOrder) : relevanceSortOrder));
//			
//			this.isSortedByRelevance = true;
//			this.isSortedByID = false;
//			this.isSortedDescending = descending;
//		}
	}
	
	/**	
	 * Sort this QueryResult's ResultElements by the ID of the documents they represent.
	 * @param	descending	if set to true, the ResultElements will be sorted in descending order, ascending order otherwise
	 * This method is intended to be used before joining or merging two QueryResults
	 */
	public void sortByID(boolean descending) {
		if (this.isSortedByID && (this.isSortedDescending == descending))
			return;
		
		Collections.sort(this.elements, (descending ? Collections.reverseOrder(idSortOrder) : idSortOrder));
		
		this.isSortedByID = true;
		this.isSortedByRelevance = false;
		this.isSortedDescending = descending;
//		//	check if there's something to do
//		if (!this.isSortedByID || (this.isSortedDescending != descending)) {
//			Collections.sort(this.elements, (descending ? Collections.reverseOrder(idSortOrder) : idSortOrder));
//			
//			this.isSortedByID = true;
//			this.isSortedByRelevance = false;
//			this.isSortedDescending = descending;
//		}
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
	
	/**	remove the least relevant QueryResultElement from this QueryResult
	 */
	private void removeLeastRelevantElement() {
		if (this.elements.isEmpty())
			return;
		
		//	determine least relevant element ...
		int minRelevanceIndex = -1;
		
		//	if the ResultElements are sorted, it's an easy job
		if (this.isSortedByRelevance)
			minRelevanceIndex = ((this.isSortedDescending) ? (this.elements.size() - 1) : 0);
			
		//	search otherwise
		else {
			double minRelevance = 1;
			for (int i = 0; i < this.elements.size(); i++) {
				QueryResultElement qre = ((QueryResultElement) this.elements.get(i));
				
				//	if more than one QueryResultElement have the least relevance value, mark the last one
				if (qre.relevance <= minRelevance) {
					minRelevance = qre.relevance;
					minRelevanceIndex = i;
				}
			}
		}
		
		//	... and remove it
		if (minRelevanceIndex != -1)
			this.elements.removeElementAt(minRelevanceIndex);
	}
	
	/**	@see	java.lang.Object#toString()
	 */
	public String toString() {
		StringBuffer ret = new StringBuffer();
		ret.append("<result>");
		for (int i = 0; i < this.elements.size(); i++) {
			ret.append(this.elements.get(i).toString());
		}
		ret.append("</result>");
		return ret.toString();
	}
	
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
		QueryResult result = new QueryResult(maxSize, (queryResult2.keepSorted || queryResult1.keepSorted));
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
		
		//	get contant arrays
		QueryResultElement[] qres1 = queryResult1.getContentArray();
		QueryResultElement[] qres2 = queryResult2.getContentArray();
		
		//	sort content arrays for the full outer sort join
		Arrays.sort(qres1, idSortOrder);
		Arrays.sort(qres2, idSortOrder);
		
		int index1 = 0;
		int index2 = 0;
		
		while ((index1 < qres1.length) || (index2 < qres2.length)) {
			int c;
			if ((index1 < qres1.length) && (index2 < qres2.length))
				c = idSortOrder.compare(qres1[index1], qres2[index2]);
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
		
		if ((queryResult1 == null) && (queryResult2 == null))
			return null;
		if (queryResult1 == null)
			return new QueryResult(maxSize, (queryResult2.keepSorted));
		if (queryResult2 == null)
			return new QueryResult(maxSize, (queryResult1.keepSorted));
		
		QueryResult result = new QueryResult(maxSize, (queryResult2.keepSorted || queryResult1.keepSorted));
		result.mergeCount = queryResult1.mergeCount + queryResult2.mergeCount;
		
		QueryResultElement[] qres1 = queryResult1.getContentArray();
		QueryResultElement[] qres2 = queryResult2.getContentArray();
		
		Arrays.sort(qres1, idSortOrder);
		Arrays.sort(qres2, idSortOrder);
		
		int index1 = 0;
		int index2 = 0;
		
		while ((index1 < qres1.length) && (index2 < qres2.length)) {
			int c = idSortOrder.compare(qres1[index1], qres2[index2]);
			
			//	ResultElements with matching IDs, join them
			if (c == 0) {
				QueryResultElement qre = new QueryResultElement(qres1[index1].docNr, getCombinedRelevance(qres1[index1].relevance, queryResult1.mergeCount, qres2[index2].relevance, queryResult2.mergeCount, relevanceCombination));
				result.addResultElement(qre);
				index1++;
				index2++;
			}
			
			//	ID of r1 is smaller QueryResultElement, increment index1
			else if (c < 0)
				index1++;
			
			//	ID of r2 is smaller QueryResultElement, increment index2
			else index2++;
		}
		
		return result;
	}
	
	/**	@return		the relevance values r1 and r2 combined in the fashion determined by relevanceCombination
	 */
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
