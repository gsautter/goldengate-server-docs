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
package de.uka.ipd.idaho.goldenGateServer.ats;


import de.uka.ipd.idaho.easyIO.util.RandomByteSource;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.gamta.TokenSequence;
import de.uka.ipd.idaho.gamta.Tokenizer;
import de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Constant bearer for the GoldenGATE ATS
 * 
 * @author sautter
 */
public interface GoldenGateAtsConstants extends GoldenGateServerConstants, LiteratureConstants {
	
	/** the command for retrieving the list of hosted annotations types */
	public static final String GET_ANNOTATION_TYPES = "GET_ANNOTATION_TYPES";
	
	/** the command for retrieving the list annotations of a specific type */
	public static final String GET_ANNOTATIONS = "GET_ANNOTATIONS";
	
	/** CSV value delimiter character for data storage and transfer */
	public static final char CSV_DELIMITER = '"';
	
	/** the name of the attribute storing the ID of the document an annotation originates from */
	public static final String SOURCE_DOCUMENT_ID = "sourceDocId";
	
	/**
	 * Annotation wrapper for string tupels, to enable application of GPath predicates
	 * 
	 * @author sautter
	 */
	public static class AtsAnnotation extends AbstractAttributed implements QueriableAnnotation {
		private static QueriableAnnotation[] qaDummy = new QueriableAnnotation[0];
		private static String[] sDummy = new String[0];
		
		private String type;
		private StringTupel data;
		private String id;
		private String valueString;
		private TokenSequence valueTokens;
		
		/**
		 * Constructor
		 * @param type the annotation type
		 * @param data the string tupel to wrap
		 * @param keys the keys to the data
		 */
		public AtsAnnotation(String type, StringTupel data, String[] keys) {
			this.type = type;
			this.data = data;
			this.valueString = this.data.getValue(ANNOTATION_VALUE_ATTRIBUTE);
			for (int k = 0; k < keys.length; k++) {
				if (ANNOTATION_VALUE_ATTRIBUTE.equals(keys[k]) || SOURCE_DOCUMENT_ID.equals(keys[k]))
					continue;
				String value = this.data.getValue(keys[k]);
				if (value != null)
					this.setAttribute(keys[k], value);
			}
			int[] idParts = {
				this.valueString.hashCode(),
				AnnotationUtils.produceStartTag(this).hashCode(),
			};
			this.id = new String(RandomByteSource.getHexCode(idParts));
		}
		
		private void checkValue() {
			if (this.valueTokens != null)
				return;
			this.valueTokens = Gamta.newTokenSequence(this.valueString, Gamta.INNER_PUNCTUATION_TOKENIZER);
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
			this.checkValue();
			return this.valueTokens.length();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#tokenAt(int)
		 */
		public Token tokenAt(int index) {
			this.checkValue();
			return this.valueTokens.tokenAt(index);
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#firstToken()
		 */
		public Token firstToken() {
			this.checkValue();
			return this.valueTokens.firstToken();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#lastToken()
		 */
		public Token lastToken() {
			this.checkValue();
			return this.valueTokens.lastToken();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#valueAt(int)
		 */
		public String valueAt(int index) {
			this.checkValue();
			return this.valueTokens.valueAt(index);
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#firstValue()
		 */
		public String firstValue() {
			this.checkValue();
			return this.valueTokens.firstValue();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#lastValue()
		 */
		public String lastValue() {
			this.checkValue();
			return this.valueTokens.lastValue();
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
			this.checkValue();
			return this.valueTokens.getWhitespaceAfter(index);
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#size()
		 */
		public int size() {
			this.checkValue();
			return this.valueTokens.size();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#getTokenizer()
		 */
		public Tokenizer getTokenizer() {
			this.checkValue();
			return this.valueTokens.getTokenizer();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.TokenSequence#getSubsequence(int, int)
		 */
		public TokenSequence getSubsequence(int start, int size) {
			this.checkValue();
			return this.valueTokens.getSubsequence(start, size);
		}

		/* (non-Javadoc)
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		public int compareTo(Object o) {
			if (o == this)
				return 0;
			if (o instanceof AtsAnnotation) {
				AtsAnnotation aa = ((AtsAnnotation) o);
				return this.valueString.compareTo(aa.valueString);
			}
			return -1;
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
			this.checkValue();
			return this.valueTokens.length();
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
			return newType;
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.Annotation#getAnnotationID()
		 */
		public String getAnnotationID() {
			return this.id;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.Annotation#getValue()
		 */
		public String getValue() {
			return this.valueString;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.Annotation#toXML()
		 */
		public String toXML() {
			return AnnotationUtils.produceStartTag(this) + this.valueString + AnnotationUtils.produceEndTag(this);
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
			return null;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.Annotation#getDocumentPropertyNames()
		 */
		public String[] getDocumentPropertyNames() {
			return sDummy;
		}
		
		/* (non-Javadoc)
		 * @see java.lang.CharSequence#length()
		 */
		public int length() {
			return this.valueString.length();
		}
		
		/* (non-Javadoc)
		 * @see java.lang.CharSequence#charAt(int)
		 */
		public char charAt(int index) {
			return this.valueString.charAt(index);
		}
		
		/* (non-Javadoc)
		 * @see java.lang.CharSequence#subSequence(int, int)
		 */
		public CharSequence subSequence(int start, int end) {
			return this.valueString.subSequence(start, end);
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.QueriableAnnotation#getAbsoluteStartIndex()
		 */
		public int getAbsoluteStartIndex() {
			return 0;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.QueriableAnnotation#getAbsoluteStartOffset()
		 */
		public int getAbsoluteStartOffset() {
			return 0;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.QueriableAnnotation#getAnnotations()
		 */
		public QueriableAnnotation[] getAnnotations() {
			return qaDummy;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.QueriableAnnotation#getAnnotations(java.lang.String)
		 */
		public QueriableAnnotation[] getAnnotations(String type) {
			return qaDummy;
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.QueriableAnnotation#getAnnotationTypes()
		 */
		public String[] getAnnotationTypes() {
			return sDummy;
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.gamta.QueriableAnnotation#getAnnotationNestingOrder()
		 */
		public String getAnnotationNestingOrder() {
			return "";
		}
	}
}