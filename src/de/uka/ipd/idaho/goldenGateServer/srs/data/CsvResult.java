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
 *     * Neither the name of the Universitaet Karlsruhe (TH) / KIT nor the
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
import java.util.Comparator;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;

/**
 * @author sautter
 */
public abstract class CsvResult extends SrsSearchResult {
	
	/** Constructor
	 * @param resultAttributes
	 */
	protected CsvResult(String[] resultAttributes) {
		super(resultAttributes);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#getSortOrder()
	 */
	public Comparator getSortOrder() {
		if (this.sortOrder == null)
			this.sortOrder = new Comparator() {
				public int compare(Object o1, Object o2) {
					CsvResultElement cre1 = ((CsvResultElement) o1);
					CsvResultElement cre2 = ((CsvResultElement) o2);
					int c = 0;
					for (int a = 0; a < resultAttributes.length; a++) {
						String s1 = ((String) cre1.getAttribute(resultAttributes[a], ""));
						String s2 = ((String) cre2.getAttribute(resultAttributes[a], ""));
						if ((c = s1.compareTo(s2)) != 0)
							return c;
					}
					return c;
				}
			};
		return this.sortOrder;
	}
	private Comparator sortOrder;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#writeElement(java.io.Writer, de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement)
	 */
	public void writeElement(Writer out, SrsSearchResultElement element) throws IOException {
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	write element
		buf.write("  <" + RESULT_NODE_NAME);
		for (int a = 0; a < this.resultAttributes.length; a++) {
			String statisticsFieldValue = ((String) element.getAttribute(this.resultAttributes[a]));
			if ((statisticsFieldValue != null) && (statisticsFieldValue.length() != 0))
				buf.write(" " + this.resultAttributes[a] + "=\"" + AnnotationUtils.escapeForXml(statisticsFieldValue, true) + "\"");
		}
		buf.write("/>");
		buf.newLine();
		
		//	flush Writer if wrapped here
		if (buf != out)
			buf.flush();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#readElement(java.lang.String[])
	 */
	public SrsSearchResultElement readElement(String[] tokens) throws IOException {
		TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(tokens[0], grammar);
		CsvResultElement cre = null;
		for (int a = 0; a < this.resultAttributes.length; a++) {
			String attributeValue = tnas.getAttribute(this.resultAttributes[a]);
			if (attributeValue != null) {
				if (cre == null)
					cre = this.newResultElement();
				cre.setAttribute(this.resultAttributes[a], attributeValue);
			}
		}
		return cre;
	}
	
	/**
	 * Produce an instance of the specific element subclass required for this
	 * result.
	 * @return a new result element
	 */
	protected abstract CsvResultElement newResultElement();
}
