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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;

/**
 * Read-only list of search result elements. This class is intended to provide a
 * random-accessible buffer for a sub set or all the elements of an SRS search.
 * 
 * @author sautter
 */
public class SrsSearchResultBuffer {
	
	/**
	 * the attribute names for the result, in the order the attribute values
	 * should be displayed
	 */
	public final String[] resultAttributes;
	
	private SrsSearchResult result;
	private ArrayList elements = new ArrayList();
	
	/**
	 * Constructor
	 * @param result the SRS serach result to retrieve the buffer's entries from
	 * @param limit the maximum number of entries for the buffer to retrieve from
	 *            the specified SRS search result (specifying 0 as the limit
	 *            will read all (remaining) elements from the backing result)
	 */
	public SrsSearchResultBuffer(SrsSearchResult result, int limit) {
		this.resultAttributes = result.resultAttributes;
		
		this.result = result;
		while (((limit == 0) || (this.elements.size() < limit)) && this.result.hasNextElement())
			this.elements.add(this.result.getNextElement());
	}
	
	/**
	 * Retrieve the backing result of the buffer. The getNextElement() method
	 * should not be used on a Result object retrieved from this method, since
	 * this would consume a result element past the buffer's control. This
	 * method is merely intended to check the actual type of the backing result.
	 * @return the backing result of the buffer
	 */
	public SrsSearchResult getResult() {
		return this.result;
	}
	
	/**
	 * Retrieve a result object backed by the current content of the buffer. No
	 * method invokation on the returned result modifies the backing result of
	 * the buffer.
	 * @return a result object backed by the current content of the buffer.
	 */
	public SrsSearchResult asResult() {
		return new SrsSearchResult(this.resultAttributes) {
			SrsSearchResultElement[] subElements = ((SrsSearchResultElement[]) elements.toArray(new SrsSearchResultElement[elements.size()]));
			int subElementIndex = 0;
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#hasNextElement()
			 */
			public boolean hasNextElement() {
				return (this.subElementIndex < this.subElements.length);
			}
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getNextElement()
			 */
			public SrsSearchResultElement getNextElement() {
				return this.subElements[this.subElementIndex++];
			}
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getSortOrder()
			 */
			public Comparator getSortOrder() {
				return result.getSortOrder();
			}
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getStartTagAttributes()
			 */
			public Properties getStartTagAttributes() {
				return result.getStartTagAttributes();
			}
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#writeElement(java.io.Writer, de.uka.ipd.idaho.goldenGateServer.srs.data.ResultElement)
			 */
			public void writeElement(Writer out, SrsSearchResultElement element) throws IOException {
				result.writeElement(out, element);
			}

			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#readElement(java.lang.String[])
			 */
			public SrsSearchResultElement readElement(String[] tokens) throws IOException {
				return result.readElement(tokens);
			}
		};
	}
	
	/**
	 * Add further elements from the backing result to the buffer, at most
	 * &lt;limit&gt; ones. Specifying 0 as the limit will read all remaining
	 * elements from the backing result.
	 * @param limit the maximum number of entries for the list to retrieve from
	 *            the specified SRS search result
	 * @return the number of elements actually added, at most &lt;limit&gt;
	 */
	public synchronized int addElements(int limit) {
		int added = 0;
		while (((limit == 0) || (added < limit)) && this.result.hasNextElement()) {
			this.elements.add(this.result.getNextElement());
			added++;
		}
		return added;
	}
	
	//	TODO: implement random access methods
	public synchronized SrsSearchResultElement elementAt(int index) {
		return ((SrsSearchResultElement) this.elements.get(index));
	}
	
	/**
	 * Retrieve size of the list, i.e., the number of elements currently
	 * buffered. Though this method cannot guarantee to reflect the full number
	 * of elements in the underlying result, it provides a lower bound.
	 * @return the current size of the list
	 */
	public int size() {
		return this.elements.size();
	}
	
	/**
	 * Sort the current content of the buffer, using the Comparator of the
	 * backing result.
	 */
	public void sort() {
		this.sort(null);
	}
	
	/**
	 * Sort the current content of the buffer. The arguments of the compare()
	 * method will be SrsSearchResultElement objects, their actual class being
	 * the element class of the backing Result, thus even more specific. Hence,
	 * the argument Comparator may cast them without check. If the specified
	 * Comparator is null, the Comparator of the backing result will be used.
	 * @param sortOrder the Comparator defining the sort order
	 */
	public synchronized void sort(Comparator sortOrder) {
		Collections.sort(this.elements, ((sortOrder == null) ? this.result.getSortOrder() : sortOrder));
	}
	
	/**
	 * Write the XML representation of the currently buffered part of the
	 * underlying result to some Writer. This method does not modify the
	 * underlying result of the fraction of it that is buffered. Due to this
	 * property, it cannot guarantee that all elements of the underlying result
	 * are written out. To ensure the latter, first use the addElements() method
	 * until it returns 0 to make sure the list has buffered all the elements of
	 * the underlying result.
	 * @param out the Writer to write to
	 * @throws IOException
	 */
	public synchronized void writeXml(Writer out) throws IOException {
		
		//	produce writer
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	empty result
		if (this.elements.isEmpty()) {
			buf.write(this.result.produceEmptyTag());
			buf.newLine();
		}
		
		//	result with elements
		else {
			buf.write(this.result.produceStartTag());
			buf.newLine();
			
			for (int e = 0; e < this.elements.size(); e++) {
				this.result.writeElement(buf, this.elementAt(e));
				buf.newLine();
			}
			
			buf.write(this.result.produceEndTag());
			buf.newLine();
		}
		
		//	flush Writer if it was wrapped
		if (buf != out)
			buf.flush();
	}
}
