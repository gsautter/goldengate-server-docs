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
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * The result of a document search
 * 
 * @author sautter
 */
public abstract class DocumentResult extends SrsSearchResult {
	
	static final String[] drAttributeNames = {
			RELEVANCE_ATTRIBUTE,
			DOCUMENT_ID_ATTRIBUTE,
			CHECKIN_USER_ATTRIBUTE,
			CHECKIN_TIME_ATTRIBUTE,
			UPDATE_USER_ATTRIBUTE,
			UPDATE_TIME_ATTRIBUTE,
			MASTER_DOCUMENT_TITLE_ATTRIBUTE,
			DOCUMENT_TITLE_ATTRIBUTE,
			DOCUMENT_AUTHOR_ATTRIBUTE,
			DOCUMENT_DATE_ATTRIBUTE,
			DOCUMENT_SOURCE_LINK_ATTRIBUTE,
			PAGE_NUMBER_ATTRIBUTE,
			LAST_PAGE_NUMBER_ATTRIBUTE,
		};
	
	/** Constructor
	 */
	public DocumentResult() {
		this(drAttributeNames);
	}
	
	/**
	 * Constructor
	 * @param resultAttributes the attribute names for the document result, in
	 *            the order the attribute values should be displayed
	 */
	public DocumentResult(String[] resultAttributes) {
		super(resultAttributes);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getNextElement()
	 */
	public DocumentResultElement getNextDocumentResultElement() {
		return ((DocumentResultElement) this.getNextElement());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getSortOrder()
	 */
	public Comparator getSortOrder() {
		if (this.sortOrder == null)
			this.sortOrder = new Comparator() {
				public int compare(Object o1, Object o2) {
					DocumentResultElement dre1 = ((DocumentResultElement) o1);
					DocumentResultElement dre2 = ((DocumentResultElement) o2);
					if (dre1.relevance == dre2.relevance) return 0;
					return ((dre1.relevance < dre2.relevance) ? 1 : -1);
				}
			};
		return this.sortOrder;
	}
	private Comparator sortOrder;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getStartTagAttributes()
	 */
	public Properties getStartTagAttributes() {
		return new Properties();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#writeElement(java.io.Writer, de.uka.ipd.idaho.goldenGateServer.srs.data.ResultElement)
	 */
	public void writeElement(Writer out, SrsSearchResultElement element) throws IOException {
		
		//	produce writer
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	get data
		DocumentResultElement dre = ((DocumentResultElement) element);
		IndexResult[] subResults = dre.getSubResults();
		
		//	complete document, not only meta data
		if (dre.document != null) {
			
			//	open result
			buf.write("<" + RESULT_NODE_NAME + " " + RELEVANCE_ATTRIBUTE + "=\"" + dre.relevance + "\" " + DOCUMENT_ID_ATTRIBUTE + "=\"" + dre.documentId + "\"");
			String[] attributeNames = dre.getAttributeNames();
			for (int a = 0; a < attributeNames.length; a++) {
				if (!RELEVANCE_ATTRIBUTE.equals(attributeNames[a]) && !DOCUMENT_ID_ATTRIBUTE.equals(attributeNames[a])) {
					Object value = dre.getAttribute(attributeNames[a]);
					if ((value != null) && (value instanceof String))
						buf.write(" " + attributeNames[a] + "=\"" + AnnotationUtils.escapeForXml(value.toString(), true) + "\"");
				}
			}
			buf.write(">");
			buf.newLine();
			
			//	write document
			GenericGamtaXML.storeDocument(dre.document, buf);
			buf.newLine();
			
			//	close result
			buf.write("</" + RESULT_NODE_NAME + ">");
			buf.newLine();
		}
		
		//	plain document data
		else if (subResults.length == 0) {
			
			//	open result
			buf.write("<" + RESULT_NODE_NAME + " " + RELEVANCE_ATTRIBUTE + "=\"" + dre.relevance + "\" " + DOCUMENT_ID_ATTRIBUTE + "=\"" + dre.documentId + "\"");
			String[] attributeNames = dre.getAttributeNames();
			for (int a = 0; a < attributeNames.length; a++) {
				if (!RELEVANCE_ATTRIBUTE.equals(attributeNames[a]) && !DOCUMENT_ID_ATTRIBUTE.equals(attributeNames[a])) {
					Object value = dre.getAttribute(attributeNames[a]);
					if ((value != null) && (value instanceof String))
						buf.write(" " + attributeNames[a] + "=\"" + AnnotationUtils.escapeForXml(value.toString()) + "\"");
				}
			}
			buf.write("/>");
			buf.newLine();
		}
		
		//	entry with sub results
		else {
			
			//	open result
			buf.write("<" + RESULT_NODE_NAME + " " + RELEVANCE_ATTRIBUTE + "=\"" + dre.relevance + "\" " + DOCUMENT_ID_ATTRIBUTE + "=\"" + dre.documentId + "\"");
			String[] attributeNames = dre.getAttributeNames();
			for (int a = 0; a < attributeNames.length; a++) {
				if (!RELEVANCE_ATTRIBUTE.equals(attributeNames[a]) && !DOCUMENT_ID_ATTRIBUTE.equals(attributeNames[a])) {
					Object value = dre.getAttribute(attributeNames[a]);
					if ((value != null) && (value instanceof String))
						buf.write(" " + attributeNames[a] + "=\"" + AnnotationUtils.escapeForXml(value.toString()) + "\"");
				}
			}
			buf.write(">");
			buf.newLine();
			
			//	write empty value element
			buf.write("<" + IndexResultElement.INDEX_ENTRY_VALUE_ATTRIBUTE + "/>");
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
			buf.write("</" + RESULT_NODE_NAME + ">");
			buf.newLine();
		}
		
		//	flush Writer if it was wrapped
		if (buf != out)
			buf.flush();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#readElement(java.lang.String[])
	 */
	public SrsSearchResultElement readElement(String[] tokens) throws IOException {
		DocumentResultElement dre = null;
		
		IndexResult reSubResult = null;
		ArrayList reSubResultElementList = null;
		
		String sreType = null;
		String sreValue = null;
		TreeNodeAttributeSet sreAttributes = null;
		
		for (int t = 0; t < tokens.length; t++) {
			String token = tokens[t];
			
			if (grammar.isTag(token) && RESULT_NODE_NAME.equals(grammar.getType(token))) {
				
				//	end of result element, nothing to do
				if (grammar.isEndTag(token)) {
					//	TODO: clean up if block empty
				}
				
				//	start tag (empty or not)
				else {
					
					//	read attributes
					TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
					double relevance = 0.0;
					try {
						relevance = Double.parseDouble(tnas.getAttribute(RELEVANCE_ATTRIBUTE, "0.0"));
					} catch (NumberFormatException e) {}
					long docNr = 0;
					try {
						docNr = Long.parseLong(tnas.getAttribute(IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE, "0"));
					} catch (NumberFormatException e) {}
					String docId = tnas.getAttribute(DOCUMENT_ID_ATTRIBUTE, "");
					
					//	create document result element
					if (!tnas.isEmpty()) {
						dre = new DocumentResultElement(docNr, docId, relevance, null);
						String[] attributeNames = tnas.getAttributeNames();
						for (int a = 0; a < attributeNames.length; a++) {
							String value = tnas.getAttribute(attributeNames[a]);
							if (value != null)
								dre.setAttribute(attributeNames[a], value);
						}
					}
				}
			}
			
			//	content of result element
			else if (dre != null) {
				
				if (grammar.isTag(token)) {
					String tokenType = grammar.getType(token);
					
					if (SUB_RESULTS_NODE_NAME.equals(tokenType)) {
						
						//	end of result or sub result
						if (grammar.isSingularTag(token) || grammar.isEndTag(token)) {
							if ((reSubResult != null) && (reSubResultElementList != null) && (reSubResultElementList.size() != 0)) {
								Collections.sort(reSubResultElementList, reSubResult.getSortOrder());
								dre.addSubResult(reSubResult);
							}
							reSubResult = null;
							reSubResultElementList = null;
						}
						
						//	start of a result or sub result
						else {
							TreeNodeAttributeSet srTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
							StringVector fieldNames = new StringVector();
							fieldNames.parseAndAddElements(srTnas.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""), " ");
							fieldNames.removeAll("");
							String indexName = srTnas.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE);
							String indexLabel = srTnas.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, "Result Index");
							
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
					
					//	sub result tag
					else if (reSubResult != null) {
						
						//	read empty element
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
						
						//	end of sub result element
						else if (grammar.isEndTag(token)) {
							
							//	store element if data complete
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
							
							//	clear data slots
							sreType = null;
							sreValue = null;
							sreAttributes = null;
						}
						
						//	start of sub result element
						else {
							sreType = tokenType;
							sreAttributes = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						}
					}
				}
				
				//	text data
				else {
					
					//	sub result element value
					if ((sreType != null) && (sreAttributes != null) && (token.trim().length() != 0))
						sreValue = grammar.unescape(token.trim());
				}
			}
		}
		
		//	return element
		return dre;
	}
	
	/**
	 * Read a document result from its XML representation, provided by some
	 * Reader. Do not close the Reader when this method returns, since the
	 * returned document result obtains its elements from the reader on the fly,
	 * i.e., as they are retrieved via one of the getNextElement() or
	 * getNextIndexResultElement() methods. The document result will close the
	 * Reader automatically when the backing data is completely read. This
	 * method expects a Reader that provides document meta data only, with
	 * optional sub results. It does not handle complete documents.
	 * @param in the Reader to read from
	 * @return a document result backed by the XML data
	 * @throws IOException
	 */
	public static DocumentResult readDocumentDataResult(Reader in) throws IOException {
		ResultBuilder rb = new DocumentDataResultBuilder(in);
		return ((DocumentResult) rb.getResult());
	}
	
	private static class DocumentDataResultBuilder extends ResultBuilder {
		DocumentDataResultBuilder(Reader in) throws IOException {
			super(in);
		}
		protected SrsSearchResult buildResult(String[] resultAttributes, Properties attributes) {
			return new DocumentResult(resultAttributes) {
				public boolean hasNextElement() {
					return DocumentDataResultBuilder.this.hasNextElement();
				}
				public SrsSearchResultElement getNextElement() {
					return DocumentDataResultBuilder.this.getNextElement();
				}
			};
		}
	}
	
	/**
	 * Read a document result from its XML representation, provided by some
	 * Reader. Do not close the Reader when this method returns, since the
	 * returned document result obtains its elements from the reader on the fly,
	 * i.e., as they are retrieved via one of the getNextElement() or
	 * getNextDocumentResultElement() methods. The document result will close the
	 * Reader automatically when the backing data is completely read. This
	 * method expects a Reader that provides complete documents as the result
	 * elements. It does not handle sub results as document meta data.
	 * @param in the Reader to read from
	 * @return a document result backed by the XML data
	 * @throws IOException
	 */
	public static DocumentResult readDocumentResult(Reader in) throws IOException {
		ResultBuilder rb = new DocumentResultBuilder(in);
		return ((DocumentResult) rb.getResult());
	}
	
	private static class DocumentResultBuilder extends ResultBuilder {
		DocumentResultBuilder(Reader in) throws IOException {
			super(in);
		}
		protected SrsSearchResult buildResult(String[] resultAttributes, Properties attributes) {
			return new DocumentResult(resultAttributes) {
				public boolean hasNextElement() {
					return DocumentResultBuilder.this.hasNextElement();
				}
				public SrsSearchResultElement getNextElement() {
					return DocumentResultBuilder.this.getNextElement();
				}
				public SrsSearchResultElement readElement(final String[] tokens) throws IOException {
					TreeNodeAttributeSet resultDocAttributes = null;
					
					DocumentResultElement dre = null;
					int docDataStart = -1;
					
					for (int t = 0; t < tokens.length; t++) {
						String token = tokens[t];
						
						//	result tag, and no tags of document data yet or remaining open
						if (grammar.isTag(token) && RESULT_NODE_NAME.equals(grammar.getType(token))) {
							if (grammar.isEndTag(token)) {
								if (docDataStart != -1) {
									
									final int dds = docDataStart;
									final int dde = t;
									DocumentRoot doc = GenericGamtaXML.readDocument(new Reader() {
										int cti = dds;
										int cto = 0;
										public void close() throws IOException {}
										public int read(char[] cbuf, int off, int len) throws IOException {
											if (this.cti >= dde)
												return -1;
											int r = 0;
											while (len > 0) {
												if (this.cto < tokens[this.cti].length()) {
													int l = Math.min(len, (tokens[this.cti].length() - this.cto));
													tokens[this.cti].getChars(this.cto, (this.cto + l), cbuf, off);
													this.cto += l;
													r += l;
													len -= l;
													off += l;
												}
												if (this.cto < tokens[this.cti].length())
													return r;
												this.cti++;
												if (this.cti >= dde)
													return r;
												this.cto = 0;
											}
											return r;
										}
									});
									
									double relevance = 0.0;
									try {
										relevance = Double.parseDouble(resultDocAttributes.getAttribute(RELEVANCE_ATTRIBUTE, "0.0"));
									} catch (NumberFormatException e) {}
									long docNr = 0;
									try {
										docNr = Long.parseLong(resultDocAttributes.getAttribute(IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE, "0"));
									} catch (NumberFormatException e) {}
									String docId = resultDocAttributes.getAttribute(DOCUMENT_ID_ATTRIBUTE, "");
									
									doc.setAttribute(RELEVANCE_ATTRIBUTE, ("" + relevance));
									doc.setAttribute(DOCUMENT_ID_ATTRIBUTE, docId);
									String[] attributeNames = resultDocAttributes.getAttributeNames();
									for (int a = 0; a < attributeNames.length; a++) {
										String value = resultDocAttributes.getAttribute(attributeNames[a]);
										if (value != null) doc.setAttribute(attributeNames[a], value);
									}
									
									dre = new DocumentResultElement(docNr, docId, relevance, doc);
									attributeNames = doc.getAttributeNames();
									for (int a = 0; a < attributeNames.length; a++) {
										String value = resultDocAttributes.getAttribute(attributeNames[a]);
										if (value != null) {
											doc.setAttribute(attributeNames[a], value);
											dre.setAttribute(attributeNames[a], value);
										}
									}
									
									docDataStart = -1;
								}
							}
							else {
								resultDocAttributes = TreeNodeAttributeSet.getTagAttributes(token, grammar);
								docDataStart = t+1;
							}
						}
					}
					
					//	return result element
					return dre;
				}
			};
		}
	}
//	public static void main(String[] args) throws Exception {
//		final String[] tokens = {"be", "fore", "in", "si", "de", "af", "ter"};
//		final int dds = 2;
//		final int dde = 5;
//		Reader in = new Reader() {
//			int cti = dds;
//			int cto = 0;
//			public void close() throws IOException {}
//			public int read(char[] cbuf, int off, int len) throws IOException {
//				if (this.cti >= dde)
//					return -1;
//				int r = 0;
//				while (len > 0) {
//					if (this.cto < tokens[this.cti].length()) {
//						int l = Math.min(len, (tokens[this.cti].length() - this.cto));
//						tokens[this.cti].getChars(this.cto, (this.cto + l), cbuf, off);
//						this.cto += l;
//						r += l;
//						len -= l;
//						off += l;
//					}
//					if (this.cto < tokens[this.cti].length())
//						return r;
//					this.cti++;
//					if (this.cti >= dde)
//						return r;
//					this.cto = 0;
//				}
//				return r;
//			}
//		};
//		int r;
////		while ((r = in.read()) != -1) {
////			System.out.print((char) r);
////		}
//		char[] cBuf = new char[3];
//		while ((r = in.read(cBuf, 0, cBuf.length)) != -1) {
//			System.out.print(new String(cBuf, 0, r));
//		}
//	}
}
