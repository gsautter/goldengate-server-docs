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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeSet;

import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.gamta.TokenSequence;
import de.uka.ipd.idaho.gamta.TokenSequenceUtils;
import de.uka.ipd.idaho.gamta.Tokenizer;

/**
 * A single element in the result of an index search, i.e. an individual index
 * entry. The value of this annotation may be empty, but never is null.
 * 
 * @author sautter
 */
public class IndexResultElement extends SrsSearchResultElement implements Annotation {
	
	/** the name of the attribute addressing the Annotation value instead of an actual attribute */
	public static final String INDEX_ENTRY_VALUE_ATTRIBUTE = "entryValue";
	
	/** the name of the attribute holding a nice label for the Annotation value*/
	public static final String INDEX_ENTRY_VALUE_LABEL_ATTRIBUTE = "entryValueLabel";
	
	/** the name of the attribute addressing the number of the document this index entry refers to */
	public static final String DOCUMENT_NUMBER_ATTRIBUTE = "docNumber";
	
	private String type;
	private TokenSequence data;
	
	/** the document number this index entry refers to */
	public final long docNr;
	
	private ArrayList subResults = null;
	private IndexResultElement parent = null;
	
	/** Constructor
	 * @param	docNr	the document number this index entry refers to
	 * @param	type	the type of the index entry
	 * @param	data	the value of the index entry
	 */
	public IndexResultElement(long docNr, String type, String data) {
		this.docNr = docNr;
		this.type = type;
		this.data = Gamta.INNER_PUNCTUATION_TOKENIZER.tokenize(data);
	}
	
	private String idString = null;
	private String getIdString() {
		if (this.idString == null)
			this.idString = this.produceIdString();
		return this.idString;
	}
	
	private String produceIdString() {
		StringBuffer idString = new StringBuffer(this.getValue());
		String[] attributeNames = this.getAttributeNames();
		for (int a = 0; a < attributeNames.length; a++) {
			idString.append(" ");
			Object attributeValue = this.getAttribute(attributeNames[a]);
			if (attributeValue != null)
				idString.append(attributeValue.toString());
		}
		return idString.toString();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	public boolean equals(Object obj) {
		return (this.compareTo(obj) == 0);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	public int hashCode() {
		return this.getIdString().hashCode();
	}
	
	/**
	 * Produce a String key for sorting index result elements in lexicographical
	 * order
	 * @param attributeNames the name of the attributes to use for the sort key,
	 *            in the order to use them in
	 * @return a String key for this index result elements to allow for sorting
	 */
	public String getSortString(String[] attributeNames) {
		if (attributeNames == this.sortStringFields)
			return this.sortString;
		
		StringBuffer sortStringAssembler = new StringBuffer();
		for (int a = 0; a < attributeNames.length; a++)
			sortStringAssembler.append(" " + (INDEX_ENTRY_VALUE_ATTRIBUTE.equals(attributeNames[a]) ? this.getValue() : this.getAttribute(attributeNames[a], " ")));
		if (sortStringAssembler.toString().trim().length() == 0)
			return this.getValue();
		this.sortString = sortStringAssembler.toString().trim();
		this.sortStringFields = attributeNames;
		
		return this.sortString;
	}
	private String sortString = null;
	private String[] sortStringFields = null;
	
	/**
	 * Add a subordinate index result of this index result element, i.e. a list
	 * of elements subordinate to this one, or null, if there is no subordinate
	 * result
	 * @param subResult the subordinate result to add
	 */
	public void addSubResult(IndexResult subResult) {
		if (this.subResults == null)
			this.subResults = new ArrayList(4);
		this.subResults.add(subResult);
	}
	
	/**
	 * @return the subordinate index results of this index result element, i.e.
	 *         a list of elements subordinate to this one, or null, if there is
	 *         no subordinate result
	 */
	public IndexResult[] getSubResults() {
		if (this.subResults == null)
			return new IndexResult[0];
		IndexResult[] subIrs = new IndexResult[this.subResults.size()];
		for (int r = 0; r < this.subResults.size(); r++) {
			final IndexResult subIr = ((IndexResult) this.subResults.get(r));
			subIrs[r] = new IndexResult(subIr.resultAttributes, subIr.indexName, subIr.indexLabel) {
				public boolean hasNextElement() {
					return subIr.hasNextElement();
				}
				public SrsSearchResultElement getNextElement() {
					IndexResultElement next = subIr.getNextIndexResultElement();
					next.parent = IndexResultElement.this;
					return next;
				}
			};
		}
		return subIrs;
	}
	
	/**
	 * Retrieve the parent index result element. If this index result element
	 * belongs to a top level result rather than a sub result, this method
	 * returns null. Otherwise, it returns the index result element to whose
	 * sub result this index result element belongs.
	 * @return the parent index result element
	 */
	public IndexResultElement getParent() {
		return this.parent;
	}
	
	/**
	 * Set the parent element for index result elements that belong to a sub
	 * result. This method only exists to facilitate deep copying, not for use
	 * in client code.
	 * @param parent the parent index result element
	 */
	public void setParent(IndexResultElement parent) {
		this.parent = parent;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#clearAttributes()
	 */
	public void clearAttributes() {
		this.idString = null;
		super.clearAttributes();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#copyAttributes(de.uka.ipd.idaho.gamta.Attributed)
	 */
	public void copyAttributes(Attributed source) {
		this.idString = null;
		super.copyAttributes(source);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#removeAttribute(java.lang.String)
	 */
	public Object removeAttribute(String name) {
		this.idString = null;
		return super.removeAttribute(name);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#setAttribute(java.lang.String, java.lang.Object)
	 */
	public Object setAttribute(String name, Object value) {
		this.idString = null;
		return super.setAttribute(name, value);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#hasAttribute(java.lang.String)
	 */
	public boolean hasAttribute(String name) {
		return (INDEX_ENTRY_VALUE_ATTRIBUTE.equals(name) || DOCUMENT_NUMBER_ATTRIBUTE.equals(name) || super.hasAttribute(name));
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#getAttribute(java.lang.String, java.lang.Object)
	 */
	public Object getAttribute(String name, Object def) {
		if (INDEX_ENTRY_VALUE_ATTRIBUTE.equals(name))
			return this.getValue();
		else if (DOCUMENT_NUMBER_ATTRIBUTE.equals(name))
			return ("" + this.docNr);
		else return super.getAttribute(name, def);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#getAttributeNames()
	 */
	public String[] getAttributeNames() {
		TreeSet ans = new TreeSet(Arrays.asList(super.getAttributeNames()));
		ans.add(DOCUMENT_NUMBER_ATTRIBUTE);
		return ((String[]) ans.toArray(new String[ans.size()]));
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed#getAttribute(java.lang.String)
	 */
	public Object getAttribute(String name) {
		if (INDEX_ENTRY_VALUE_ATTRIBUTE.equals(name))
			return this.getValue();
		else if (DOCUMENT_NUMBER_ATTRIBUTE.equals(name))
			return ("" + this.docNr);
		else return super.getAttribute(name);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getStartIndex()
	 */
	public int getStartIndex() {
		return 0;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getEndIndex()
	 */
	public int getEndIndex() {
		return this.data.size();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getType()
	 */
	public String getType() {
		return this.type;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#changeTypeTo(java.lang.String)
	 */
	public String changeTypeTo(String newType) {
		return this.type;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getAnnotationID()
	 */
	public String getAnnotationID() {
		return this.getIdString();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getValue()
	 */
	public String getValue() {
		return TokenSequenceUtils.concatTokens(this.data);
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return this.getValue();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#toXML()
	 */
	public String toXML() {
		String value = this.getValue();
		if (value.length() == 0) {
			String tag = AnnotationUtils.produceStartTag(this);
			return (tag.substring(0, (tag.length() - ">".length())) + "/>");
		}
		else return (AnnotationUtils.produceStartTag(this) + AnnotationUtils.escapeForXml(this.getValue()) + AnnotationUtils.produceEndTag(this));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getDocument()
	 */
	public QueriableAnnotation getDocument() {
		return null;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getDocumentProperty(java.lang.String)
	 */
	public String getDocumentProperty(String propertyName) {
		return null;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getDocumentProperty(java.lang.String, java.lang.String)
	 */
	public String getDocumentProperty(String propertyName, String defaultValue) {
		return defaultValue;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.Annotation#getDocumentPropertyNames()
	 */
	public String[] getDocumentPropertyNames() {
		return new String[0];
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#tokenAt(int)
	 */
	public Token tokenAt(int index) {
		return this.data.tokenAt(index);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#firstToken()
	 */
	public Token firstToken() {
		return this.data.firstToken();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#lastToken()
	 */
	public Token lastToken() {
		return this.data.lastToken();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#valueAt(int)
	 */
	public String valueAt(int index) {
		return this.data.valueAt(index);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#firstValue()
	 */
	public String firstValue() {
		return this.data.firstValue();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#lastValue()
	 */
	public String lastValue() {
		return this.data.lastValue();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#getLeadingWhitespace()
	 */
	public String getLeadingWhitespace() {
		return "";
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#getWhitespaceAfter(int)
	 */
	public String getWhitespaceAfter(int index) {
		return this.data.getWhitespaceAfter(index);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#size()
	 */
	public int size() {
		return this.data.size();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#getTokenizer()
	 */
	public Tokenizer getTokenizer() {
		return this.data.getTokenizer();
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.TokenSequence#getSubsequence(int, int)
	 */
	public TokenSequence getSubsequence(int start, int size) {
		return this.data.getSubsequence(start, size);
	}

	/* (non-Javadoc)
	 * @see java.lang.CharSequence#length()
	 */
	public int length() {
		return this.data.length();
	}

	/* (non-Javadoc)
	 * @see java.lang.CharSequence#charAt(int)
	 */
	public char charAt(int index) {
		return this.data.charAt(index);
	}

	/* (non-Javadoc)
	 * @see java.lang.CharSequence#subSequence(int, int)
	 */
	public CharSequence subSequence(int start, int end) {
		return this.data.subSequence(start, end);
	}

	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	public int compareTo(Object obj) {
		if (obj == this)
			return 0;
		if (obj instanceof IndexResultElement) {
			IndexResultElement ire = ((IndexResultElement) obj);
			int c = this.getValue().compareTo(ire.getValue());
			if (c != 0)
				return c;
//			StringVector attributeNameCollector = new StringVector();
//			attributeNameCollector.addContentIgnoreDuplicates(this.getAttributeNames());
//			attributeNameCollector.addContentIgnoreDuplicates(ire.getAttributeNames());
//			attributeNameCollector.sortLexicographically(false, false);
//			String[] attributeNames = attributeNameCollector.toStringArray();
			TreeSet attributeNameSet = new TreeSet(String.CASE_INSENSITIVE_ORDER);
			attributeNameSet.addAll(Arrays.asList(this.getAttributeNames()));
			attributeNameSet.add(Arrays.asList(ire.getAttributeNames()));
			String[] attributeNames = ((String[]) attributeNameSet.toArray(new String[attributeNameSet.size()]));
			return this.getSortString(attributeNames).compareTo(ire.getSortString(attributeNames));
		}
		else return -1;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.CharSpan#getStartOffset()
	 */
	public int getStartOffset() {
		return 0;
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.gamta.CharSpan#getEndOffset()
	 */
	public int getEndOffset() {
		return this.data.length();
	}
}
