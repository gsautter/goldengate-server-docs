///*
// * Copyright (c) 2006-, IPD Boehm, Universitaet Karlsruhe (TH) / KIT, by Guido Sautter
// * All rights reserved.
// *
// * Redistribution and use in source and binary forms, with or without
// * modification, are permitted provided that the following conditions are met:
// *
// *     * Redistributions of source code must retain the above copyright
// *       notice, this list of conditions and the following disclaimer.
// *     * Redistributions in binary form must reproduce the above copyright
// *       notice, this list of conditions and the following disclaimer in the
// *       documentation and/or other materials provided with the distribution.
// *     * Neither the name of the Universitaet Karlsruhe (TH) / KIT nor the
// *       names of its contributors may be used to endorse or promote products
// *       derived from this software without specific prior written permission.
// *
// * THIS SOFTWARE IS PROVIDED BY UNIVERSITAET KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
// * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
// * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// */
//package de.uka.ipd.idaho.goldenGateServer.srs;
//
//import java.io.BufferedWriter;
//import java.io.IOException;
//import java.io.Reader;
//import java.io.Writer;
//import java.util.Collections;
//import java.util.LinkedHashMap;
//import java.util.LinkedList;
//import java.util.Map;
//
//import de.uka.ipd.idaho.gamta.DocumentRoot;
//import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
//import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResultElement;
//import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;
//import de.uka.ipd.idaho.htmlXmlUtil.Parser;
//import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
//import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
//import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
//import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;
//import de.uka.ipd.idaho.stringUtils.StringVector;
//
///**
// * @author sautter
// */
//public class IndexData implements GoldenGateSrsConstants {
//	long docNr;
//	String docId;
//	Map indexResultsByName = Collections.synchronizedMap(new LinkedHashMap());
//	IndexData(long docNr, String docId) {
//		this.docNr = docNr;
//		this.docId = docId;
//	}
//	
//	void addIndexResult(IndexResult indexResult) {
//		this.indexResultsByName.put(indexResult.indexName, indexResult);
//	}
//	
//	IndexResult getIndexResult(String indexName) {
//		return ((IndexResult) this.indexResultsByName.get(indexName));
//	}
//	
//	IndexResult[] getIndexResults() {
//		return ((IndexResult[]) this.indexResultsByName.values().toArray(new IndexResult[this.indexResultsByName.size()]));
//	}
//	
//	void removeIndexResult(String indexName) {
//		this.indexResultsByName.remove(indexName);
//	}
//	
//	void writeIndexData(Writer out) throws IOException {
//		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
//		buf.write("<" + DocumentRoot.DOCUMENT_TYPE + 
//				" " + IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE + "=\"" + this.docNr + "\"" +
//				" " + DOCUMENT_ID_ATTRIBUTE + "=\"" + this.docId + "\"" +
//				">");
//		buf.newLine();
//		
//		//	add sub results
//		IndexResult[] subResults = this.getIndexResults();
//		for (int s = 0; s < subResults.length; s++) {
//			
//			//	get fields of sub result
//			StringVector subIndexFields = new StringVector();
//			subIndexFields.addContent(subResults[s].resultAttributes);
//			buf.write("<" + SUB_RESULTS_NODE_NAME + 
//					" " + RESULT_INDEX_NAME_ATTRIBUTE + "=\"" + subResults[s].indexName + "\"" +
//					" " + RESULT_INDEX_LABEL_ATTRIBUTE + "=\"" + subResults[s].indexLabel + "\"" +
//					" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + subIndexFields.concatStrings(" ") + "\"" +
//			">");
//			buf.newLine();
//			
//			//	write sub result entries
//			while (subResults[s].hasNextElement()) {
//				//	TODO remove empty attributes
//				buf.write(subResults[s].getNextIndexResultElement().toXML());
//				buf.newLine();
//			}
//			
//			//	close sub result
//			buf.write("</" + SUB_RESULTS_NODE_NAME + ">");
//			buf.newLine();
//		}
//		
//		//	flush Writer if wrapped here
//		buf.write("</" + DocumentRoot.DOCUMENT_TYPE + ">");
//		buf.newLine();
//		if (buf != out)
//			buf.flush();
//	}
//	
//	static IndexData readIndexData(Reader in) throws IOException {
//		final IndexData[] indexData = {null};
//		parser.stream(in, new TokenReceiver() {
//			IndexResult reSubResult = null;
//			LinkedList reSubResultElementList = null;
//			
//			String sreType = null;
//			String sreValue = null;
//			TreeNodeAttributeSet sreAttributes = null;
//			
//			public void storeToken(String token, int treeDepth) throws IOException {
//				if (grammar.isTag(token)) {
//					String type = grammar.getType(token);
//					if (DocumentRoot.DOCUMENT_TYPE.equals(type)) {
//						if (grammar.isSingularTag(token) || grammar.isEndTag(token)) {}
//						else /* start of sub result list */ {
//							TreeNodeAttributeSet idTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
//							long docNr = Long.parseLong(idTnas.getAttribute(IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE));
//							String docId = idTnas.getAttribute(DOCUMENT_ID_ATTRIBUTE);
//							indexData[0] = new IndexData(docNr, docId);
//						}
//					}
//					else if (IndexResult.SUB_RESULTS_NODE_NAME.equals(type)) /* start of sub result list */ {
//						if (grammar.isSingularTag(token) || grammar.isEndTag(token)) {
//							if ((this.reSubResult != null) && (this.reSubResultElementList.size() != 0)) {
//								Collections.sort(this.reSubResultElementList, this.reSubResult.getSortOrder());
//								indexData[0].addIndexResult(this.reSubResult);
//							}
//							this.reSubResult = null;
//							this.reSubResultElementList = null;
//						}
//						else /* start of sub result */ {
//							TreeNodeAttributeSet srTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
//							StringVector fieldNames = new StringVector();
//							fieldNames.parseAndAddElements(srTnas.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""), " ");
//							fieldNames.removeAll("");
//							String indexName = srTnas.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE);
//							String indexLabel = srTnas.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, (indexName + " Index Data"));
//							
//							final LinkedList subResultElements = new LinkedList();
//							this.reSubResultElementList = subResultElements;
//							this.reSubResult = new IndexResult(fieldNames.toStringArray(), indexName, indexLabel) {
//								public boolean hasNextElement() {
//									return (subResultElements.size() != 0);
//								}
//								public SrsSearchResultElement getNextElement() {
//									return ((SrsSearchResultElement) subResultElements.removeFirst());
//								}
//							};
//						}
//					}
//					else {
//						if (grammar.isSingularTag(token)) {
//							TreeNodeAttributeSet sreTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
//							IndexResultElement subIre = new IndexResultElement(indexData[0].docNr, type, "");
//							String[] attributeNames = sreTnas.getAttributeNames();
//							for (int a = 0; a < attributeNames.length; a++) {
//								String attributeValue = sreTnas.getAttribute(attributeNames[a]);
//								if ((attributeValue != null) && (attributeValue.length() != 0))
//									subIre.setAttribute(attributeNames[a], attributeValue);
//							}
//							this.reSubResultElementList.add(subIre);
//						}
//						else if (grammar.isEndTag(token)) {
//							if ((this.reSubResult != null) && (this.sreType != null) && (this.sreAttributes != null) && (this.sreValue != null)) {
//								IndexResultElement subIre = new IndexResultElement(indexData[0].docNr, this.sreType, this.sreValue);
//								String[] attributeNames = this.sreAttributes.getAttributeNames();
//								for (int a = 0; a < attributeNames.length; a++) {
//									String attributeValue = this.sreAttributes.getAttribute(attributeNames[a]);
//									if ((attributeValue != null) && (attributeValue.length() != 0))
//										subIre.setAttribute(attributeNames[a], attributeValue);
//								}
//								this.reSubResultElementList.add(subIre);
//							}
//							this.sreType = null;
//							this.sreValue = null;
//							this.sreAttributes = null;
//						}
//						else {
//							this.sreType = type;
//							this.sreAttributes = TreeNodeAttributeSet.getTagAttributes(token, grammar);
//						}
//					}
//				}
//				else if (this.sreType != null)
//					this.sreValue = token;
//			}
//			public void close() throws IOException {}
//		});
//		
//		//	return completed element
//		return indexData[0];
//	}
//	
//	//	parser and grammar for data
//	private static final Grammar grammar = new StandardGrammar();
//	private static final Parser parser = new Parser(grammar);
//}
