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

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;

import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;

/**
 * Wrapper for an SRS search result that buffers the elements of the backing
 * SrsSearchResult. The getResult() method provides actual SrsSearchResult
 * objects with the iteration position set to the first element. This class and
 * its sub classes that correspond to SrsSearchResult's sub classes are intended
 * for situations where a layout engine needs to iterate over a search result
 * multiple times instead of once.<br>
 * Instances of this class fill their buffer in an on-demand fashion from the
 * backing result. This implies that other code should not retrieve elements
 * from the backing result any more once the latter is wrapped in this class.
 * 
 * @author sautter
 */
public abstract class BufferedResult {
	
	/**
	 * the attribute names for the result, in the order the attribute values
	 * should be displayed (copied from the backing result)
	 */
	public final String[] resultAttributes;

	private SrsSearchResult data;
	private ArrayList elements = new ArrayList();
	
	/**
	 * Constructor
	 * @param data the SRS search result to wrap and whose elements to buffer
	 */
	public BufferedResult(SrsSearchResult data) {
		this.data = data;
		this.resultAttributes = this.data.resultAttributes;
	}
	
	/**
	 * Produce a result object iterating over the elements of the backing
	 * result. Each new result object retrieved from this method has its
	 * iteration position set to the first element of the backing result.
	 * @return a result object iterating over the buffered elements of the
	 *         backing result
	 */
	public SrsSearchResult getResult() {
		return new SrsSearchResult(this.data.resultAttributes) {
			int elementIndex = 0;
			public boolean hasNextElement() {
				return BufferedResult.this.hasNextElement(this.elementIndex);
			}
			public SrsSearchResultElement getNextElement() {
				return BufferedResult.this.getNextElement(this.elementIndex++);
			}
			
			/*
			 * these methods are independent of the actual elements, therefore
			 * we need not handle them based on the buffer and can just loop
			 * them through to the backing result
			 */
			public Comparator getSortOrder() {
				return data.getSortOrder();
			}
			public Properties getStartTagAttributes() {
				return data.getStartTagAttributes();
			}
			public SrsSearchResultElement readElement(String[] tokens) throws IOException {
				return data.readElement(tokens);
			}
			public void writeElement(Writer out, SrsSearchResultElement element) throws IOException {
				data.writeElement(out, element);
			}
		};
	}
	
	synchronized boolean hasNextElement(int index) {
		
		//	extend buffer to requested element if possible
		this.extendBuffer(index);
		
		//	do we have sufficient elements in the buffer now?
		return (index < this.elements.size());
	}
	
	synchronized SrsSearchResultElement getNextElement(int index) {
		
		//	extend buffer to requested element if possible
		this.extendBuffer(index);
		
		//	do we have sufficient elements in the buffer now?
		return ((index < this.elements.size()) ? ((SrsSearchResultElement) this.elements.get(index)) : null);
	}
	
	//	extend buffer up to requested element if possible
	private void extendBuffer(int size) {
		while ((this.elements.size() < (size+1)) && this.data.hasNextElement())
			this.elements.add(this.data.getNextElement());
	}
	
	/**
	 * Check whethere the buffered result is empty. This is the case if (a)
	 * there are no elements in the buffer and (b) the backing result's
	 * hasNextElement() method returns false.
	 * @return true if the buffered result is empty, false otherwise
	 */
	public boolean isEmpty() {
		return (this.elements.isEmpty() && !this.data.hasNextElement());
	}
	
	/**
	 * Obtain the number of elements currently contained in the buffer. If the
	 * backing result has not been read to its end, this number might be smaller
	 * than the actual number of result elements. Only after an invocation of
	 * sort(), or after a result retrieved from the getResult() method has been
	 * iterated completely, this method returns the actual number of result
	 * elements.
	 * @return the number of elements currently contained in the buffer
	 */
	public int size() {
		return this.elements.size();
	}
	
	/**
	 * Sort the buffered result elements by the natural sort order of the
	 * backing result. This method retrieves any remaining elements from the
	 * backing result before doing the actual sorting. This is in order to keep
	 * the sort order stable, which it might not be if further elements are
	 * added later on.
	 */
	public synchronized void sort() {
		this.sort(this.data.getSortOrder());
	}
	
	/**
	 * Sort the buffered result elements by the values they provide for a set of
	 * attributes. This method retrieves any remaining elements from the backing
	 * result before doing the actual sorting. This is in order to keep the sort
	 * order stable, which it might not be if further elements are added later
	 * on.
	 * @param sortAttributes the attributes whose values to use for sorting
	 */
	public synchronized void sort(final String[] sortAttributes) {
		this.sort(new Comparator() {
			public int compare(Object o1, Object o2) {
				SrsSearchResultElement re1 = ((SrsSearchResultElement) o1);
				SrsSearchResultElement re2 = ((SrsSearchResultElement) o2);
				int c = 0;
				for (int a = 0; (c == 0) && (a < sortAttributes.length); a++) {
					String av1 = re1.getAttribute(sortAttributes[a], "").toString();
					String av2 = re2.getAttribute(sortAttributes[a], "").toString();
					c = av1.compareTo(av2);
				}
				return c;
			}
		});
	}
	
	/**
	 * Sort the buffered result elements by a custom order. This method
	 * retrieves any remaining elements from the backing result before doing the
	 * actual sorting. This is in order to keep the sort order stable, which it
	 * might not be if further elements are added later on. The arguments of the
	 * compare() method will be SrsSearchResultElement objects, their actual
	 * class being the element class of the backing result, thus even more
	 * specific. Hence, the argument Comparator may cast them without check.
	 * @param sortOrder the Comparator to use for sorting
	 */
	public synchronized void sort(Comparator sortOrder) {
		while (this.data.hasNextElement())
			this.elements.add(this.data.getNextElement());
		Collections.sort(this.elements, ((sortOrder == null) ? this.data.getSortOrder() : sortOrder));
	}
}
