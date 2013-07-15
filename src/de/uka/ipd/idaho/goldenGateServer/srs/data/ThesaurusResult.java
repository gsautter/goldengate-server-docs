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
package de.uka.ipd.idaho.goldenGateServer.srs.data;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Comparator;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;

/**
 * The result of a thesaurus lookup
 * 
 * @author sautter
 */
public abstract class ThesaurusResult extends SrsSearchResult implements GoldenGateSrsConstants {
	
	/** the annotation type this thesaurus result contains */
	public final String thesaurusEntryType;
	
	/** the name of the thesaurus this result comes from */
	public final String thesaurusName;
	
	/** Constructor
	 * @param	resultAttributes	the field names for the lookup result, in the order they should be displayed
	 * @param	entryType			the annotation type this thesaurus result contains
	 * @param	thesaurusName		the name of the thesaurus this result comes from
	 */
	public ThesaurusResult(String[] resultAttributes, String entryType, String thesaurusName) {
		super(resultAttributes);
		this.thesaurusEntryType = entryType;
		this.thesaurusName = thesaurusName;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getNextElement()
	 */
	public ThesaurusResultElement getNextThesaurusResultElement() {
		return ((ThesaurusResultElement) this.getNextElement());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getStartTagAttributes()
	 */
	public Properties getStartTagAttributes() {
		if (this.startTagAttributes == null) {
			this.startTagAttributes = new Properties();
			this.startTagAttributes.setProperty(RESULT_INDEX_NAME_ATTRIBUTE, this.thesaurusEntryType);
			this.startTagAttributes.setProperty(RESULT_INDEX_LABEL_ATTRIBUTE, this.thesaurusName);
		}
		return this.startTagAttributes;
	}
	private Properties startTagAttributes = null;

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getSortOrder()
	 */
	public Comparator getSortOrder() {
		if (this.sortOrder == null)
			this.sortOrder = new Comparator() {
				public int compare(Object o1, Object o2) {
					ThesaurusResultElement tre1 = ((ThesaurusResultElement) o1);
					ThesaurusResultElement tre2 = ((ThesaurusResultElement) o2);
					int c = 0;
					for (int a = 0; a < resultAttributes.length; a++) {
						String s1 = ((String) tre1.getAttribute(resultAttributes[a], ""));
						String s2 = ((String) tre2.getAttribute(resultAttributes[a], ""));
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
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#writeElement(java.io.Writer, de.uka.ipd.idaho.goldenGateServer.srs.data.ResultElement)
	 */
	public void writeElement(Writer out, SrsSearchResultElement element) throws IOException {
		
		//	produce writer
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	write element
		buf.write("  <" + RESULT_NODE_NAME);
		for (int a = 0; a < this.resultAttributes.length; a++) {
			String thesaurusFieldValue = ((String) element.getAttribute(this.resultAttributes[a]));
			if ((thesaurusFieldValue != null) && (thesaurusFieldValue.length() != 0))
				buf.write(" " + this.resultAttributes[a] + "=\"" + AnnotationUtils.escapeForXml(thesaurusFieldValue, true) + "\"");
		}
		buf.write("/>");
		buf.newLine();
		
		//	flush Writer if it was wrapped
		if (buf != out)
			buf.flush();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#readElement(java.lang.String[])
	 */
	public SrsSearchResultElement readElement(String[] tokens) throws IOException {
		TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(tokens[0], grammar);
		String[] attributeNames = tnas.getAttributeNames();
		ThesaurusResultElement tre = null;
		for (int a = 0; a < attributeNames.length; a++) {
			String attributeValue = tnas.getAttribute(attributeNames[a]);
			if (attributeValue != null) {
				if (tre == null)
					tre = new ThesaurusResultElement();
				tre.setAttribute(attributeNames[a], attributeValue);
			}
		}
		return tre;
	}
	
	/**
	 * Read a thesaurus result from its XML representation, provided by some
	 * Reader. Do not close the Reader when this method returns, since the
	 * returned thesaurus result obtains its elements from the reader on the
	 * fly, i.e., as they are retrieved via one of the getNextElement() or
	 * getNextIndexResultElement() methods. The thesaurus result will close the
	 * Reader automatically when the backing data is completely read.
	 * @param in the Reader to read from
	 * @return a thesaurus result backed by the XML data
	 * @throws IOException
	 */
	public static ThesaurusResult readThesaurusResult(Reader in) throws IOException {
		ResultBuilder rb = new ThesaurusResultBuilder(in);
		return ((ThesaurusResult) rb.getResult());
	}
	
	private static class ThesaurusResultBuilder extends ResultBuilder {
		ThesaurusResultBuilder(Reader in) throws IOException {
			super(in);
		}
		protected SrsSearchResult buildResult(String[] resultAttributes, Properties attributes) {
			String thesaurusName = attributes.getProperty(RESULT_INDEX_NAME_ATTRIBUTE);
			String thesaurusLabel = attributes.getProperty(RESULT_INDEX_LABEL_ATTRIBUTE);
			return new ThesaurusResult(resultAttributes, thesaurusName, thesaurusLabel) {
				public boolean hasNextElement() {
					return ThesaurusResultBuilder.this.hasNextElement();
				}
				public SrsSearchResultElement getNextElement() {
					return ThesaurusResultBuilder.this.getNextElement();
				}
			};
		}
	}
}
