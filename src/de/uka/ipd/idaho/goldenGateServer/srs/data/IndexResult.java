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
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * The result of an index search
 * 
 * @author sautter
 */
public abstract class IndexResult extends SrsSearchResult {
	
	/** the name of the index this result comes from */
	public final String indexName;
	
	/** the name of the index this result comes from */
	public final String indexLabel;
	
	/** Constructor
	 * @param	indexName			the name of the index this result comes from
	 * @param	indexLabel			a nice name for the index
	 * @param	resultAttributes	the attribute names for the lookup result, in the order the attribute values should be displayed
	 */
	public IndexResult(String[] resultAttributes, String indexName, String indexLabel) {
		super(resultAttributes);
		this.indexName = indexName;
		this.indexLabel = indexLabel;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getNextElement()
	 */
	public IndexResultElement getNextIndexResultElement() {
		return ((IndexResultElement) this.getNextElement());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getSortOrder()
	 */
	public Comparator getSortOrder() {
		if (this.sortOrder == null)
			this.sortOrder = new Comparator() {
				public int compare(Object o1, Object o2) {
					IndexResultElement ire1 = ((IndexResultElement) o1);
					IndexResultElement ire2 = ((IndexResultElement) o2);
					int c = ire1.getSortString(resultAttributes).compareTo(ire2.getSortString(resultAttributes));
					if (c != 0)
						return c;
					if (ire1.docNr == ire2.docNr)
						return 0;
					else if (ire1.docNr < ire2.docNr)
						return -1;
					else return 1;
				}
			};
		return this.sortOrder;
	}
	private Comparator sortOrder;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getStartTagAttributes()
	 */
	public Properties getStartTagAttributes() {
		Properties attributes = new Properties();
		attributes.setProperty(RESULT_INDEX_NAME_ATTRIBUTE, this.indexName);
		attributes.setProperty(RESULT_INDEX_LABEL_ATTRIBUTE, this.indexLabel);
		return attributes;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#writeElement(java.io.Writer, de.uka.ipd.idaho.goldenGateServer.srs.data.ResultElement)
	 */
	public void writeElement(Writer out, SrsSearchResultElement element) throws IOException {
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	write result
		IndexResultElement ire = ((IndexResultElement) element);
		IndexResult[] subResults = ire.getSubResults();
		
		//	plain entry
		if (subResults.length == 0) {
			buf.write(ire.toXML());
			buf.newLine();
		}
		
		//	entry with sub results
		else {
			
			//	open main result element
			buf.write(AnnotationUtils.produceStartTag(ire));
			buf.newLine();
			
			//	write element value
			String value = ire.getValue();
			if (value.length() == 0)
				buf.write("<" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + "/>");
			else {
				buf.write("<" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + ">");
				buf.write(AnnotationUtils.escapeForXml(value));
				buf.write("</" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + ">");
			}
			buf.newLine();
			
			//	add sub results
			for (int s = 0; s < subResults.length; s++) {
				
				//	get fields of sub result
				StringVector subIndexFields = new StringVector();
				subIndexFields.addContent(subResults[s].resultAttributes);
				buf.write("<" + SUB_RESULTS_NODE_NAME + 
						" " + RESULT_INDEX_NAME_ATTRIBUTE + "=\"" + subResults[s].indexName + "\"" +
						" " + RESULT_INDEX_LABEL_ATTRIBUTE + "=\"" + subResults[s].indexLabel + "\"" +
						" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + subIndexFields.concatStrings(" ") + "\"" +
				">");
				buf.newLine();
				
				//	write sub result entries
				while (subResults[s].hasNextElement()) {
					buf.write(subResults[s].getNextIndexResultElement().toXML());
					buf.newLine();
				}
				
				//	close sub result
				buf.write("</" + SUB_RESULTS_NODE_NAME + ">");
				buf.newLine();
			}
			
			//	close main result element
			buf.write(AnnotationUtils.produceEndTag(ire));
			buf.newLine();
		}
		
		//	flush Writer if wrapped here
		if (buf != out)
			buf.flush();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#readElement(java.lang.String[])
	 */
	public SrsSearchResultElement readElement(String[] tokens) throws IOException {
		String reType = null;
		String reValue = null;
		TreeNodeAttributeSet reAttributes = null;
		
		IndexResultElement ire = null;
		
		IndexResult reSubResult = null;
		ArrayList reSubResultElementList = null;
		ArrayList reSubResults = new ArrayList();
		
		String sreType = null;
		String sreValue = null;
		TreeNodeAttributeSet sreAttributes = null;
		
		for (int t = 0; t < tokens.length; t++) {
			String token = tokens[t];
			if (grammar.isTag(token)) {
				String tokenType = grammar.getType(token);
				if (SUB_RESULTS_NODE_NAME.equals(tokenType)) {
					if (grammar.isSingularTag(token) || grammar.isEndTag(token)) {
						if ((reSubResult != null) && (reSubResultElementList.size() != 0)) {
							Collections.sort(reSubResultElementList, reSubResult.getSortOrder());
							reSubResults.add(reSubResult);
						}
						reSubResult = null;
						reSubResultElementList = null;
					}
					else /* start of sub result */ {
						TreeNodeAttributeSet srTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						StringVector fieldNames = new StringVector();
						fieldNames.parseAndAddElements(srTnas.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""), " ");
						fieldNames.removeAll("");
						String indexName = srTnas.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE);
						String indexLabel = srTnas.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, "Thesaurus Lookup Result");
						
						final ArrayList subResultElements = new ArrayList();
						reSubResultElementList = subResultElements;
						reSubResult = new IndexResult(fieldNames.toStringArray(), indexName, indexLabel) {
							int sreIndex = 0;
							public boolean hasNextElement() {
								return (this.sreIndex < subResultElements.size());
							}
							public SrsSearchResultElement getNextElement() {
								return ((SrsSearchResultElement) subResultElements.get(this.sreIndex++));
							}
						};
					}
				}
				else if (IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE.equals(tokenType)) /* ignore value tags, only there to mark value */ {
					if (grammar.isSingularTag(token)) {
						if (reSubResult == null)
							reValue = "";
						else sreValue = "";
					}
				}
				else if (reSubResult != null) /* sub result tag */ {
					if (grammar.isSingularTag(token)) {
						TreeNodeAttributeSet sreTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						String docNr = sreTnas.getAttribute(IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE, "0");
						IndexResultElement subIre = new IndexResultElement(Long.parseLong(docNr), tokenType, "");
						String[] attributeNames = sreTnas.getAttributeNames();
						for (int a = 0; a < attributeNames.length; a++) {
							String attributeValue = sreTnas.getAttribute(attributeNames[a]);
							if ((attributeValue != null) && (attributeValue.length() != 0))
								subIre.setAttribute(attributeNames[a], attributeValue);
						}
						reSubResultElementList.add(subIre);
					}
					else if (grammar.isEndTag(token)) {
						if ((reSubResult != null) && (sreType != null) && (sreAttributes != null) && (sreValue != null)) {
							String docNr = sreAttributes.getAttribute(IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE, "0");
							IndexResultElement subIre = new IndexResultElement(Long.parseLong(docNr), sreType, sreValue);
							String[] attributeNames = sreAttributes.getAttributeNames();
							for (int a = 0; a < attributeNames.length; a++) {
								String attributeValue = sreAttributes.getAttribute(attributeNames[a]);
								if ((attributeValue != null) && (attributeValue.length() != 0))
									subIre.setAttribute(attributeNames[a], attributeValue);
							}
							reSubResultElementList.add(subIre);
						}
						sreType = null;
						sreValue = null;
						sreAttributes = null;
					}
					else {
						sreType = tokenType;
						sreAttributes = TreeNodeAttributeSet.getTagAttributes(token, grammar);
					}
				}
				else /* result tag */ {
					if (grammar.isSingularTag(token)) {
						TreeNodeAttributeSet reTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						String docNr = reTnas.getAttribute(IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE, "0");
						ire = new IndexResultElement(Long.parseLong(docNr), tokenType, "");
						String[] attributeNames = reTnas.getAttributeNames();
						for (int a = 0; a < attributeNames.length; a++) {
							String attributeValue = reTnas.getAttribute(attributeNames[a]);
							if ((attributeValue != null) && (attributeValue.length() != 0))
								ire.setAttribute(attributeNames[a], attributeValue);
						}
					}
					else if (grammar.isEndTag(token)) {
						if ((reType != null) && (reAttributes != null) && (reValue != null)) {
							String docNr = reAttributes.getAttribute(IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE, "0");
							ire = new IndexResultElement(Long.parseLong(docNr), reType, reValue);
							String[] attributeNames = reAttributes.getAttributeNames();
							for (int a = 0; a < attributeNames.length; a++) {
								String attributeValue = reAttributes.getAttribute(attributeNames[a]);
								if ((attributeValue != null) && (attributeValue.length() != 0))
									ire.setAttribute(attributeNames[a], attributeValue);
							}
							for (int s = 0; s < reSubResults.size(); s++)
								ire.addSubResult((IndexResult) reSubResults.get(s));
						}
					}
					else {
						reType = tokenType;
						reAttributes = TreeNodeAttributeSet.getTagAttributes(token, grammar);
					}

				}
			}
			else /* text data */ {
				if ((sreType != null) && (sreAttributes != null) && (token.trim().length() != 0)) // sub result element value
					sreValue = grammar.unescape(token.trim());
				else if ((reType != null) && (reAttributes != null) && (token.trim().length() != 0)) // result element value
					reValue = grammar.unescape(token.trim());
			}
		}
		
		//	return completed element
		return ire;
	}
	
	/**
	 * Read an index result from its XML representation, provided by some
	 * Reader. Do not close the Reader when this method returns, since the
	 * returned index result obtains its elements from the reader on the fly,
	 * i.e., as they are retrieved via one of the getNextElement() or
	 * getNextIndexResultElement() methods. The index result will close the
	 * Reader automatically when the backing data is completely read.
	 * @param in the Reader to read from
	 * @return an index result backed by the XML data
	 * @throws IOException
	 */
	public static IndexResult readIndexResult(Reader in) throws IOException {
		ResultBuilder rb = new IndexResultBuilder(in);
		return ((IndexResult) rb.getResult());
	}
	
	private static class IndexResultBuilder extends ResultBuilder {
		IndexResultBuilder(Reader in) throws IOException {
			super(in);
		}
//		protected SrsSearchResult buildResult(String[] resultAttributes, Properties attributes) {
		SrsSearchResult buildResult(String[] resultAttributes, TreeNodeAttributeSet attributes) {
			String indexName = attributes.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE);
			String indexLabel = attributes.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE);
			return new IndexResult(resultAttributes, indexName, indexLabel) {
				public boolean hasNextElement() {
					return IndexResultBuilder.this.hasNextElement();
				}
				public SrsSearchResultElement getNextElement() {
					return IndexResultBuilder.this.getNextElement();
				}
			};
		}
	}
}
