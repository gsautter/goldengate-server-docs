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
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;

/**
 * the statistics of the SRS document collection, plus contributing users
 * 
 * @author sautter
 */
public abstract class CollectionStatistics extends SrsSearchResult {
	
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
	
	/** Constructor
	 * @param resultAttributes
	 * @param masterDocCount
	 * @param docCount
	 * @param wordCount
	 * @param since
	 * @param masterDocCountSince
	 * @param docCountSince
	 * @param wordCountSince
	 */
	public CollectionStatistics(String[] statisticsFieldNames, int masterDocCount, int docCount, int wordCount, String since, int masterDocCountSince, int docCountSince, int wordCountSince) {
		super(statisticsFieldNames);
		this.masterDocCount = masterDocCount;
		this.docCount = docCount;
		this.wordCount = wordCount;
		this.since = since;
		this.masterDocCountSince = masterDocCountSince;
		this.docCountSince = docCountSince;
		this.wordCountSince = wordCountSince;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getNextElement()
	 */
	public CollectionStatisticsElement getNextCollectionStatisticsElement() {
		return ((CollectionStatisticsElement) this.getNextElement());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getSortOrder()
	 */
	public Comparator getSortOrder() {
		if (this.sortOrder == null)
			this.sortOrder = new Comparator() {
				public int compare(Object o1, Object o2) {
					CollectionStatisticsElement cse1 = ((CollectionStatisticsElement) o1);
					CollectionStatisticsElement cse2 = ((CollectionStatisticsElement) o2);
					int c = 0;
					for (int a = 0; a < resultAttributes.length; a++) {
						String s1 = ((String) cse1.getAttribute(resultAttributes[a], ""));
						String s2 = ((String) cse2.getAttribute(resultAttributes[a], ""));
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
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getStartTagAttributes()
	 */
	public Properties getStartTagAttributes() {
		if (this.startTagAttributes == null) {
			this.startTagAttributes = new Properties();
			this.startTagAttributes.setProperty(MASTER_DOCUMENT_COUNT_ATTRIBUTE, ("" + this.masterDocCount));
			this.startTagAttributes.setProperty(DOCUMENT_COUNT_ATTRIBUTE, ("" + this.docCount));
			this.startTagAttributes.setProperty(WORD_COUNT_ATTRIBUTE, ("" + this.wordCount));
			this.startTagAttributes.setProperty(GET_STATISTICS_SINCE_PARAMETER, this.since);
			this.startTagAttributes.setProperty(MASTER_DOCUMENT_COUNT_SINCE_ATTRIBUTE, ("" + this.masterDocCountSince));
			this.startTagAttributes.setProperty(DOCUMENT_COUNT_SINCE_ATTRIBUTE, ("" + this.docCountSince));
			this.startTagAttributes.setProperty(WORD_COUNT_SINCE_ATTRIBUTE, ("" + this.wordCountSince));
		}
		return this.startTagAttributes;
	}
	private Properties startTagAttributes = null;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#writeElement(java.io.Writer, de.uka.ipd.idaho.goldenGateServer.srs.data.ResultElement)
	 */
	public void writeElement(Writer out, SrsSearchResultElement element) throws IOException {
		
		//	produce writer
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
		
		//	flush Writer if it was wrapped
		if (buf != out)
			buf.flush();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#readElement(java.lang.String[])
	 */
	public SrsSearchResultElement readElement(String[] tokens) throws IOException {
		TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(tokens[0], grammar);
		CollectionStatisticsElement cse = null;
		for (int a = 0; a < this.resultAttributes.length; a++) {
			String attributeValue = tnas.getAttribute(this.resultAttributes[a]);
			if (attributeValue != null) {
				if (cse == null)
					cse = new CollectionStatisticsElement();
				cse.setAttribute(this.resultAttributes[a], attributeValue);
			}
		}
		return cse;
	}
	
	/**
	 * Read a collection statistics from its XML representation, provided by
	 * some Reader. Do not close the Reader when this method returns, since the
	 * returned index result obtains its elements from the reader on the fly,
	 * i.e., as they are retrieved via one of the getNextElement() or
	 * getNextIndexResultElement() methods. The collection statistics will close
	 * the Reader automatically when the backing data is completely read.
	 * @param in the Reader to read from
	 * @return a collection statistics backed by the XML data
	 * @throws IOException
	 */
	public static CollectionStatistics readCollectionStatistics(Reader in) throws IOException {
		ResultBuilder rb = new CollectionStatisticsBuilder(in);
		return ((CollectionStatistics) rb.getResult());
	}
	
	private static class CollectionStatisticsBuilder extends ResultBuilder {
		CollectionStatisticsBuilder(Reader in) throws IOException {
			super(in);
		}
		protected SrsSearchResult buildResult(String[] resultAttributes, Properties attributes) {
			int masterDocCount = Integer.parseInt(attributes.getProperty(MASTER_DOCUMENT_COUNT_ATTRIBUTE, "0"));
			int docCount = Integer.parseInt(attributes.getProperty(DOCUMENT_COUNT_ATTRIBUTE, "0"));
			int wordCount = Integer.parseInt(attributes.getProperty(WORD_COUNT_ATTRIBUTE, "0"));
			String since = attributes.getProperty(GET_STATISTICS_SINCE_PARAMETER, "-1");
			int masterDocCountSince = Integer.parseInt(attributes.getProperty(MASTER_DOCUMENT_COUNT_SINCE_ATTRIBUTE, "0"));
			int docCountSince = Integer.parseInt(attributes.getProperty(DOCUMENT_COUNT_SINCE_ATTRIBUTE, "0"));
			int wordCountSince = Integer.parseInt(attributes.getProperty(WORD_COUNT_SINCE_ATTRIBUTE, "0"));
			return new CollectionStatistics(resultAttributes, masterDocCount, docCount, wordCount, since, masterDocCountSince, docCountSince, wordCountSince) {
				public boolean hasNextElement() {
					return CollectionStatisticsBuilder.this.hasNextElement();
				}
				public SrsSearchResultElement getNextElement() {
					return CollectionStatisticsBuilder.this.getNextElement();
				}
			};
		}
	}
}
