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
package de.uka.ipd.idaho.goldenGateServer.srs;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;

import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.ReadOnlyDocument;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;

/**
 * Interface holding constants for communicating with the GoldenGateSRS,
 * formatting search results, providing links, etc. plus utility classes
 * 
 * @author sautter
 */
public interface GoldenGateSrsConstants extends GoldenGateServerConstants, LiteratureConstants {
//	
//	/**
//	 * container for utility methods (which cannot be implemented in the
//	 * surrounding interface directly)
//	 */
//	public static class Utils {
//		
//		private static Grammar documentGrammar = new StandardGrammar() {
//			
//			/** @see de.htmlXmlUtil.grammars.StandardGrammar#getCharCode(char)
//			 */
//			public String getCharCode(char c) {
//				if (c == '<') return "&lt;";
//				else if (c == '>') return "&gt;";
//				else if (c == '&') return "&amp;";
//				else if (c == '"') return "&quot;";
//				else return super.getCharCode(c);
//			}
//			
//			/** @see de.htmlXmlUtil.grammars.StandardGrammar#isCharCode(java.lang.String)
//			 */
//			public boolean isCharCode(String code) {
//				return ("&amp;".equals(code) || "&lt;".equals(code) || "&gt;".equals(code) || "&quot;".equals(code));
//			}
//			
//			/** @see de.htmlXmlUtil.grammars.Grammar#getCharLookahead()
//			 */
//			public int getCharLookahead() {
//				return 6;
//			}
//		};
//		
//		private static Parser documentParser = new Parser(documentGrammar);
//		
//		private static StringVector paragraphTags = new StringVector();
//		private static final String paragraphTagString = "br;hr;table";
//		static {
//			paragraphTags.parseAndAddElements(paragraphTagString, ";");
//		}
//		
//		/**
//		 * @return an SgmlDocumentReader configured to read a document from the
//		 *         SRS
//		 * @throws IOException
//		 */
//		public static SgmlDocumentReader getDocumentReader() throws IOException {
//			return getDocumentReader(null);
//		}
//		
//		/**
//		 * @return an SgmlDocumentReader configured to read a document from the
//		 *         SRS
//		 * @throws IOException
//		 */
//		public static SgmlDocumentReader getDocumentReader(MutableAnnotation doc) throws IOException {
//			return new SgmlDocumentReader(doc, documentGrammar, null, null, paragraphTags);
//		}
//		
//		/**
//		 * @return a Parser configured to process data from the SRS
//		 */
//		public static Parser getParser() {
//			return new Parser(documentGrammar);
//		}
//
//		/**
//		 * read a document from a stream
//		 * @param source the stream to read from
//		 * @return the document read from the specified stream
//		 * @throws IOException
//		 */
//		public static MutableAnnotation readDocument(InputStream source) throws IOException {
//			return readDocument(new InputStreamReader(source, ENCODING));
//		}
//
//		/**
//		 * read a document from a Reader (Use of this method is intended for
//		 * cases where the encoding has to be controlled externally, otherwise,
//		 * InputStreams should be used)
//		 * @param source the Reader to read from
//		 * @return the document read from the specified stream
//		 * @throws IOException
//		 */
//		public static MutableAnnotation readDocument(Reader source) throws IOException {
//			SgmlDocumentReader dc = new SgmlDocumentReader(null, documentGrammar, null, null, paragraphTags);
//			documentParser.stream(source, dc);
//			dc.close();
//			return dc.getDocument();
//		}
//
//		/**
//		 * write a document to an OutputStream
//		 * @param data the document to write
//		 * @param output the OutputStream to write to
//		 * @throws IOException
//		 */
//		public static void writeDocument(QueriableAnnotation data, OutputStream output) throws IOException {
//			writeDocument(data, new OutputStreamWriter(output, ENCODING));
//		}
//
//		/**
//		 * write a document to a Writer (Use of this method is intended for
//		 * cases where the encoding has to be controlled externally, otherwise,
//		 * OutputStreams should be used)
//		 * @param data the document to write
//		 * @param output the Writer to write to
//		 * @throws IOException
//		 */
//		public static void writeDocument(QueriableAnnotation data, Writer output) throws IOException {
//			
//			BufferedWriter buf = ((output instanceof BufferedWriter) ? ((BufferedWriter) output) : new BufferedWriter(output));
//			
//			Annotation[] nestedAnnotations = data.getAnnotations();
//			
//			//	include generic document tag if document has attributes
//			if ((nestedAnnotations.length != 0) && !DocumentRoot.DOCUMENT_TYPE.equals(nestedAnnotations[0].getType())) {
//				String[] docAttributeNames = data.getAttributeNames();
//				if (docAttributeNames.length != 0) {
//					Annotation[] newNestedAnnotations = new Annotation[nestedAnnotations.length + 1];
//					newNestedAnnotations[0] = data;
//					System.arraycopy(nestedAnnotations, 0, newNestedAnnotations, 1, nestedAnnotations.length);
//					nestedAnnotations = newNestedAnnotations;
//				}
//			}
//			Stack stack = new Stack();
//			int annotationPointer = 0;
//			
//			Token token = null;
//			Token lastToken;
//			
//			for (int t = 0; t < data.size(); t++) {
//				
//				//	switch to next Token
//				lastToken = token;
//				token = data.tokenAt(t);
//				
//				//	add line break at end of paragraph
//				boolean breakBeforeEndTag = true;
//				if ((lastToken != null) && lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) {
//					buf.write("<br/>");
//					buf.newLine();
//					breakBeforeEndTag = false;
//				}
//				
//				//	write end tags for Annotations ending before current Token
//				while ((stack.size() > 0) && ((((Annotation) stack.peek()).getStartIndex() + ((Annotation) stack.peek()).size()) <= t)) {
//					Annotation annotation = ((Annotation) stack.pop());
//					if (breakBeforeEndTag) {
//						buf.newLine();
//						breakBeforeEndTag = false;
//					}
//					buf.write(AnnotationUtils.produceEndTag(annotation));
//					buf.newLine();
//				}
//				
//				//	skip space character before unspaced punctuation (e.g. ',') or if explicitly told so
//				if (((lastToken == null) || !lastToken.hasAttribute(Token.PARAGRAPH_END_ATTRIBUTE)) && (t != 0) && (data.getWhitespaceAfter(t-1).length() != 0)) buf.write(" ");
//				
//				//	write start tags for Annotations beginning at current Token
//				while ((annotationPointer < nestedAnnotations.length) && (nestedAnnotations[annotationPointer].getStartIndex() == t)) {
//					buf.write(AnnotationUtils.produceStartTag(nestedAnnotations[annotationPointer]));
//					buf.newLine();
//					stack.push(nestedAnnotations[annotationPointer]);
//					annotationPointer++;
//				}
//				
//				//	write current Token
//				buf.write(AnnotationUtils.escapeForXml(token.getValue()));
//			}
//			
//			//	write end tags for Annotations not closed so far
//			while (stack.size() > 0) {
//				Annotation annotation = ((Annotation) stack.pop());
//				buf.newLine();
//				buf.write(AnnotationUtils.produceEndTag(annotation));
//			}
//			
//			if (buf != output)
//				buf.flush();
//		}
//	}
	
	
	/** the list documents command */
	public static final String LIST_DOCUMENTS = "SRS_LIST_DOCUMENTS";

	/** the command for obtaining search forms */
	public static final String GET_SEARCH_FIELDS = "SRS_GET_SEARCH_FIELDS";

	/** the command for obtaining statistics on the document collection */
	public static final String GET_STATISICS = "SRS_GET_STATISTICS";

	/** the master doc count attribute of the statistics */
	public static final String MASTER_DOCUMENT_COUNT_ATTRIBUTE = "MasterCount";

	/** the doc count attribute of the statistics */
	public static final String DOCUMENT_COUNT_ATTRIBUTE = "DocCount";

	/** the word count attribute of the statistics */
	public static final String WORD_COUNT_ATTRIBUTE = "WordCount";

	/** the command for searching documents */
	public static final String SEARCH_DOCUMENTS = "SRS_SEARCH_DOCUMENTS";

	/**
	 * the command for searching document metadata and relevance, plus essential
	 * details contributed by indexers
	 */
	public static final String SEARCH_DOCUMENT_DETAILS = "SRS_SEARCH_DOCUMENT_DETAILS";

	/**
	 * the command for searching document metadata, the relevance, the ID, the
	 * title, author, page number, and checkin user, but not the document itself
	 */
	public static final String SEARCH_DOCUMENT_DATA = "SRS_SEARCH_DOCUMENT_DATA";

	/**
	 * the command for searching document IDs, the relevance and the ID, but not
	 * the document itself
	 */
	public static final String SEARCH_DOCUMENT_IDS = "SRS_SEARCH_DOCUMENT_IDS";

	/** the command for obtaining one specific document in its plain XML form */
	public static final String GET_XML_DOCUMENT = "SRS_GET_XML_DOCUMENT";

	/** the command for searching entries of index tables */
	public static final String SEARCH_INDEX = "SRS_SEARCH_INDEX";

	/** the command for thesaurus-style searching index tables */
	public static final String SEARCH_THESAURUS = "SRS_SEARCH_THESAURUS";

	/** the query parameter holding specific document IDs */
	public static final String ID_QUERY_FIELD_NAME = "idQuery";

//	/**
//	 * the query parameter holding the ID of the index to obtain results from
//	 * (the index to search for SEARCH_THESAURUS, the index to return results
//	 * from for SEARCH_INDEX)
//	 */
//	public static final String INDEX_ID = "indexId";
//
	/**
	 * the query parameter holding the name of the index to obtain results from
	 * (the index to search for SEARCH_THESAURUS, the index to return results
	 * from for SEARCH_INDEX)
	 */
	public static final String INDEX_NAME = "indexName";
	
//	/**
//	 * the query parameter holding (as a comma separated list) the IDs of the
//	 * indices to obtain result details from (for SEARCH_DOCUMENT_DATA and
//	 * SEARCH_INDEX)
//	 */
//	public static final String SUB_INDEX_ID = "subIndexId";
//
	/**
	 * the query parameter holding (as a comma separated list) the names of the
	 * indices to obtain result details from (for SEARCH_DOCUMENT_DATA and
	 * SEARCH_INDEX)
	 */
	public static final String SUB_INDEX_NAME = "subIndexName";

	/**
	 * the query parameter holding the minimum number of available result
	 * details from an element in the main result (for SEARCH_DOCUMENT_DATA and
	 * SEARCH_INDEX)
	 */
	public static final String SUB_RESULT_MIN_SIZE = "minSubResultSize";

	/**
	 * the query parameter holding the timestamp since which document have to
	 * have been modified to be included in the result of a search
	 */
	public static final String LAST_MODIFIED_SINCE = "lastModifiedSince";
	
	/**
	 * the query parameter holding the timestamp before which document may
	 * have been last modified to be included in the result of a search
	 */
	public static final String LAST_MODIFIED_BEFORE = "lastModifiedBefore";
	
	/**
	 * the query parameter for telling the SRS with which stylesheet to
	 * transform documents before returning them to the requester (only for
	 * SEARCH_DOCUMENTS command). If this parameter is specified, but the URL is
	 * not accessible, or the stylesheet has errors, no transformation will be
	 * done, resulting in the whole documents being returned in their generic
	 * form.
	 */
	public static final String XSLT_URL_PARAMETER = "xsltUrl";

	/**
	 * the query parameter for specifying the cutoff index for the result
	 * documents (only for SEARCH_DOCUMENTS command). In particular, all
	 * documents less relevant than the one with the specified index are not
	 * returned to the requester. Specifying 0 will result in all documents with
	 * a non-zero relevance being returned (ATTENTION: HIGH EFFORT)
	 */
	public static final String RESULT_PIVOT_INDEX_PARAMETER = "resultPivot";

	/**
	 * the query parameter for specifying how to merge the results of individual
	 * sub queries
	 */
	public static final String RESULT_MERGE_MODE_PARAMETER = "mergeMode";

	/**
	 * the query parameter for specifying if indexer components should add
	 * search links to annotations (only for SEARCH_DOCUMENTS command)
	 */
	public static final String MARK_SEARCHABLES_PARAMETER = "markSearchables";

	/**
	 * the query parameter for specifying the minimum relevance of a document
	 * for being returned to the requester (only for SEARCH_DOCUMENTS command).
	 * This parameter has only an effect if the value if out of (0,1].
	 */
	public static final String MINIMUM_RELEVANCE_PARAMETER = "minRelevance";

	/** the attribute or document property holding the name of a document */
	public static final String DOCUMENT_NAME_ATTRIBUTE = "docName";
	
	/** the attribute holding the name of the user who uploaded a document */
	public static final String CHECKIN_USER_ATTRIBUTE = "checkinUser";

	/** the attribute holding the time when a document was uploaded */
	public static final String CHECKIN_TIME_ATTRIBUTE = "checkinTime";

	/** the attribute holding the name of the user who last updated a document */
	public static final String UPDATE_USER_ATTRIBUTE = "updateUser";

	/** the attribute holding the time when a document was last updated */
	public static final String UPDATE_TIME_ATTRIBUTE = "updateTime";

	/** the name of the XML element enclosing a group of search fields */
	public static final String FIELDS_NODE_NAME = "fields";

	/** the attribute holding the label for a group of search fields */
	public static final String FIELDS_LABEL_ATTRIBUTE = "label";

	/** the name of the XML element representing a search field */
	public static final String FIELD_NODE_NAME = "field";

	/** the attribute holding the name of a search field */
	public static final String FIELD_NAME_ATTRIBUTE = "name";

	/** the attribute holding the label of a search field */
	public static final String FIELD_LABEL_ATTRIBUTE = "label";

	/** the name of the XML element enclosing a group of search fields */
	public static final String F_FIELD_GROUP_NODE_NAME = "fieldGroup";

	/** the name of the XML element representing a search field row */
	public static final String F_FIELD_ROW_NODE_NAME = "fieldRow";

	/** the name of the XML element representing an individual search field */
	public static final String F_FIELD_NODE_NAME = "field";

	/**
	 * the name of the XML element representing an option in a select search
	 * field
	 */
	public static final String F_OPTION_NODE_NAME = "option";

//	/** the attribute holding the ID of a search field or search field group */
//	public static final String F_INDEX_ID_ATTRIBUTE = "indexId";
//
	/** the attribute holding the name of a search field or search field group */
	public static final String F_NAME_ATTRIBUTE = "name";

	/**
	 * the attribute holding the label for a search field group or row, or an
	 * individual search field, or an option of a select field
	 */
	public static final String F_LABEL_ATTRIBUTE = "label";

	/** the attribute holding the legend of a search field group */
	public static final String F_LEGEND_ATTRIBUTE = "legend";

	/** the attribute holding the legend of a search field group */
	public static final String F_INDEX_ENTRY_LABEL_ATTRIBUTE = "indexEntryLabel";

	/** the attribute holding the type of a search field */
	public static final String F_TYPE_ATTRIBUTE = "type";

	/** the attribute holding the size (in grid cells) of a search field */
	public static final String F_SIZE_ATTRIBUTE = "size";

	/**
	 * the attribute holding the value of a search field, or an option of a
	 * select field
	 */
	public static final String F_VALUE_ATTRIBUTE = "value";
	
	/**
	 * Container for all of the search fields provided by a single indexer
	 * 
	 * @author sautter
	 */
	public static class SearchFieldGroup {
		
		/** the name of the index this field group belongs to */
		public final String indexName;

		// /** the ID of the index this field group belongs to, namely the hash
		// code of the indexer's class name */
		// public final int indexId;
//		private final int indexId;

		/** the label (nice name) of the index this field group belongs to */
		public final String label;

		/**
		 * the legend of the field group, i.e. a brief textual explanation of
		 * what the to search for with the fields of this group
		 */
		public final String legend;

		/**
		 * the label (nice name) of the entries of the index this field group
		 * belongs to (null indicates that the index entries cannot be the
		 * result of a search on their own)
		 */
		public final String indexEntryLabel;

		/**
		 * Constructor using the index name as the label, no index ID, and no
		 * entry label
		 * @param indexName the name of the index this field group belongs to
		 * @param legend the legend of the field group, i.e. a brief textual
		 *            explanation of what the to search for with the fields of
		 *            this group
		 */
		public SearchFieldGroup(String indexName, String legend) {
//			this(indexName, 0, indexName, legend, null);
			this(indexName, indexName, legend, null);
		}

		/**
		 * Constructor using no index ID, and no entry label
		 * @param indexName the name of the index this field group belongs to
		 * @param label the label (nice name) of the index this field group
		 *            belongs to
		 * @param legend the legend of the field group, i.e. a brief textual
		 *            explanation of what the to search for with the fields of
		 *            this group
		 */
		public SearchFieldGroup(String indexName, String label, String legend) {
//			this(indexName, 0, label, legend, null);
			this(indexName, label, legend, null);
		}

		/**
		 * Constructor using no index ID
		 * @param indexName the name of the index this field group belongs to
		 * @param label the label (nice name) of the index this field group
		 *            belongs to
		 * @param legend the legend of the field group, i.e. a brief textual
		 *            explanation of what the to search for with the fields of
		 *            this group
		 * @param entryLabel the label (nice name) of the entries of the index
		 *            this field group belongs to (null indicates that the index
		 *            entries cannot be the result of a search on their own)
		 */
		public SearchFieldGroup(String indexName, String label, String legend, String entryLabel) {
//			this(indexName, 0, label, legend, entryLabel);
			this.indexName = indexName;
			this.label = label;
			this.legend = legend;
			this.indexEntryLabel = entryLabel;
		}
		
//		/**
//		 * Constructor setting all properties sparately
//		 * @param indexName the name of the index this field group belongs to
//		 * @param indexId the ID of the index this field group belongs to,
//		 *            namely the hash code of the indexer's class name
//		 * @param label the label (nice name) of the index this field group
//		 *            belongs to
//		 * @param legend the legend of the field group, i.e. a brief textual
//		 *            explanation of what the to search for with the fields of
//		 *            this group
//		 * @param entryLabel the label (nice name) of the entries of the index
//		 *            this field group belongs to (null indicates that the index
//		 *            entries cannot be the result of a search on their own)
//		 */
//		public SearchFieldGroup(String indexName, int indexId, String label, String legend, String entryLabel) {
//			this.indexName = indexName;
//			this.indexId = indexId;
//			this.label = label;
//			this.legend = legend;
//			this.indexEntryLabel = entryLabel;
//		}
		
		private ArrayList fieldRows = new ArrayList();
		
		/**
		 * add a row of search fields to this field group
		 * @param fieldRow the SearchFieldRow to add
		 */
		public void addFieldRow(SearchFieldRow fieldRow) {
			if (fieldRow != null) this.fieldRows.add(fieldRow);
		}

		/**
		 * @return the search field rows of this field group
		 */
		public SearchFieldRow[] getFieldRows() {
			return ((SearchFieldRow[]) this.fieldRows.toArray(new SearchFieldRow[this.fieldRows.size()]));
		}

		/**
		 * @return the search fields of this field group, no matter how they are
		 *         arranged in rows
		 */
		public SearchField[] getFields() {
			ArrayList fieldList = new ArrayList();
			SearchFieldRow[] rows = this.getFieldRows();
			for (int r = 0; r < rows.length; r++) {
				SearchField[] fields = rows[r].getFields();
				for (int f = 0; f < fields.length; f++)
					fieldList.add(fields[f]);
			}
			return ((SearchField[]) fieldList.toArray(new SearchField[fieldList.size()]));
		}

		/**
		 * @return the height of this field group in grid rows
		 */
		public int getHeight() {
			return this.fieldRows.size();
		}

		/**
		 * @return the width of this field group in grid cells, thus the maximum
		 *         of the widths of the field rows
		 */
		public int getWidth() {
			int width = 0;
			SearchFieldRow[] rows = this.getFieldRows();
			for (int r = 0; r < rows.length; r++)
				width = Math.max(width, rows[r].getWidth());
			return width;
		}

		/**
		 * @return the singular form of the label (nice name) of the entries of
		 *         the index this field group belongs to (null if
		 *         indexEntryLabel is null)
		 */
		public String getEntryLabelSingular() {
			return this.indexEntryLabel;
		}

		/**
		 * @return the (approximated) plural form of the label (nice name) of
		 *         the entries of the index this field group belongs to (null if
		 *         indexEntryLabel is null)
		 */
		public String getEntryLabelPlural() {
			if (this.indexEntryLabel == null) return null;
			else if (this.indexEntryLabel.endsWith("s") || this.indexEntryLabel.endsWith("x")) return this.indexEntryLabel;
			else if (this.indexEntryLabel.matches("\\.+[aeiou]y")) return (this.indexEntryLabel + "s");
			else if (this.indexEntryLabel.endsWith("y")) return (this.indexEntryLabel.substring(0, (this.indexEntryLabel.length() - 1)) + "ies");
			else return (this.indexEntryLabel + "s");
		}

		/**
		 * write an XML description of this field group to a writer
		 * @param os the OutputStream to write to
		 * @throws IOException
		 */
		public void writeXml(OutputStream os) throws IOException {
			this.writeXml(new OutputStreamWriter(os, ENCODING));
		}

		/**
		 * write an XML description of this field group to a writer
		 * @param w the Writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer w) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			
			if (this.getWidth() == 0) {
				bw.write("<" + F_FIELD_GROUP_NODE_NAME + 
						" " + F_NAME_ATTRIBUTE + "=\"" + this.indexName + "\"" +
//						((this.indexId == 0) ? "" : (" " + F_INDEX_ID_ATTRIBUTE + "=\"" + this.indexId + "\"")) +
						(((this.label == null) || (this.label.length() == 0)) ? "" : (" " + F_LABEL_ATTRIBUTE + "=\"" + this.label + "\"")) +
						(((this.legend == null) || (this.legend.length() == 0)) ? "" : (" " + F_LEGEND_ATTRIBUTE + "=\"" + this.legend + "\"")) +
						(((this.indexEntryLabel == null) || (this.indexEntryLabel.length() == 0)) ? "" : (" " + F_INDEX_ENTRY_LABEL_ATTRIBUTE + "=\"" + this.indexEntryLabel + "\"")) +
						"/>");
				bw.newLine();
				bw.flush();
				
			} else {
				bw.write("<" + F_FIELD_GROUP_NODE_NAME + 
						" " + F_NAME_ATTRIBUTE + "=\"" + this.indexName + "\"" +
//						((this.indexId == 0) ? "" : (" " + F_INDEX_ID_ATTRIBUTE + "=\"" + this.indexId + "\"")) +
						(((this.label == null) || (this.label.length() == 0)) ? "" : (" " + F_LABEL_ATTRIBUTE + "=\"" + this.label + "\"")) +
						(((this.legend == null) || (this.legend.length() == 0)) ? "" : (" " + F_LEGEND_ATTRIBUTE + "=\"" + this.legend + "\"")) +
						(((this.indexEntryLabel == null) || (this.indexEntryLabel.length() == 0)) ? "" : (" " + F_INDEX_ENTRY_LABEL_ATTRIBUTE + "=\"" + this.indexEntryLabel + "\"")) +
						">");
				bw.newLine();
				
				SearchFieldRow[] rows = this.getFieldRows();
				for (int r = 0; r < rows.length; r++)
					rows[r].writeXml(bw);
				
				bw.write("</" + F_FIELD_GROUP_NODE_NAME + ">");
				bw.newLine();
				bw.flush();
			}
		}
		
		/**
		 * create one or more search field groups from the XML data provided by
		 * some InputStream
		 * @param is the InputStream to read from
		 * @return one or more SearchFieldGroup objects created from the XML
		 *         data provided by the specified InputStream
		 * @throws IOException
		 */
		public static SearchFieldGroup[] readFieldGroups(InputStream is) throws IOException {
			return readFieldGroups(new InputStreamReader(is, ENCODING));
		}

		/**
		 * create one or more search field groups from the XML data provided by
		 * some Reader
		 * @param r the Reader to read from
		 * @return one or more SearchFieldGroup objects created from the XML
		 *         data provided by the specified Reader
		 * @throws IOException
		 */
		public static SearchFieldGroup[] readFieldGroups(Reader r) throws IOException {
			final ArrayList fieldGroups = new ArrayList();
			fieldsParser.stream(r, new TokenReceiver() {
				
				private SearchFieldGroup fieldGroup = null;
				private SearchFieldRow fieldRow = null;
				private SearchField field = null;
				
				public void close() throws IOException {
					if ((this.field != null) && (this.fieldRow != null))
						this.fieldRow.addField(this.field);
					this.field = null;
					
					if ((this.fieldRow != null) && (this.fieldGroup != null))
						this.fieldGroup.addFieldRow(this.fieldRow);
					this.fieldRow = null;
					
					if (this.fieldGroup != null)
						fieldGroups.add(this.fieldGroup);
					this.fieldGroup = null;
				}
				public void storeToken(String token, int treeDepth) throws IOException {
					if (fieldsGrammar.isTag(token)) {
						String tokenType = fieldsGrammar.getType(token); 
						
						if (F_FIELD_GROUP_NODE_NAME.equals(tokenType)) {
							
							if (fieldsGrammar.isEndTag(token)) {
								if (this.fieldGroup != null) fieldGroups.add(this.fieldGroup);
								this.fieldGroup = null;
							}
							else if (fieldsGrammar.isSingularTag(token)) {
								TreeNodeAttributeSet tokenAttributes = TreeNodeAttributeSet.getTagAttributes(token, fieldsGrammar);
								String indexName = tokenAttributes.getAttribute(F_NAME_ATTRIBUTE);
								if (indexName != null) {
//										int indexId = 0;
//										try {
//											indexId = Integer.parseInt(tokenAttributes.getAttribute(F_INDEX_ID_ATTRIBUTE, "0"));
//										} catch (NumberFormatException e) {}
									String label = tokenAttributes.getAttribute(F_LABEL_ATTRIBUTE, "");
									String legend = tokenAttributes.getAttribute(F_LEGEND_ATTRIBUTE, "");
									String entryLabel = tokenAttributes.getAttribute(F_INDEX_ENTRY_LABEL_ATTRIBUTE);
//										fieldGroups.add(new SearchFieldGroup(indexName, indexId, label, legend, entryLabel));
									fieldGroups.add(new SearchFieldGroup(indexName, label, legend, entryLabel));
								}
							}
							else {
								TreeNodeAttributeSet tokenAttributes = TreeNodeAttributeSet.getTagAttributes(token, fieldsGrammar);
								String indexName = tokenAttributes.getAttribute(F_NAME_ATTRIBUTE);
								if (indexName != null) {
//										int indexId = 0;
//										try {
//											indexId = Integer.parseInt(tokenAttributes.getAttribute(F_INDEX_ID_ATTRIBUTE, "0"));
//										} catch (NumberFormatException e) {}
									String label = tokenAttributes.getAttribute(F_LABEL_ATTRIBUTE, "");
									String legend = tokenAttributes.getAttribute(F_LEGEND_ATTRIBUTE, "");
									String entryLabel = tokenAttributes.getAttribute(F_INDEX_ENTRY_LABEL_ATTRIBUTE);
//										this.fieldGroup = new SearchFieldGroup(indexName, indexId, label, legend, entryLabel);
									this.fieldGroup = new SearchFieldGroup(indexName, label, legend, entryLabel);
								}
							}
						}
						else if (F_FIELD_ROW_NODE_NAME.equals(tokenType)) {
							
							if (fieldsGrammar.isEndTag(token)) {
								if ((this.fieldGroup != null) && (this.fieldRow != null)) this.fieldGroup.addFieldRow(this.fieldRow);
								this.fieldRow = null;
							}
							else if (!fieldsGrammar.isSingularTag(token)) {
								TreeNodeAttributeSet tokenAttributes = TreeNodeAttributeSet.getTagAttributes(token, fieldsGrammar);
								this.fieldRow = new SearchFieldRow(tokenAttributes.getAttribute(F_LABEL_ATTRIBUTE, ""));
							}
						}
						else if (F_FIELD_NODE_NAME.equals(tokenType)) {
							
							if (fieldsGrammar.isEndTag(token)) {
								if ((this.fieldRow != null) && (this.field != null)) this.fieldRow.addField(this.field);
								this.field = null;
							}
							else if (fieldsGrammar.isSingularTag(token)) {
								if (this.fieldRow != null) {
									TreeNodeAttributeSet tokenAttributes = TreeNodeAttributeSet.getTagAttributes(token, fieldsGrammar);
									String name = tokenAttributes.getAttribute(F_NAME_ATTRIBUTE);
									if (name != null) {
										String label = tokenAttributes.getAttribute(F_LABEL_ATTRIBUTE, "");
										int size = 1;
										try {
											size = Integer.parseInt(tokenAttributes.getAttribute(F_SIZE_ATTRIBUTE, "1"));
										} catch (NumberFormatException e) {}
										String type = tokenAttributes.getAttribute(F_TYPE_ATTRIBUTE, SearchField.TEXT_TYPE);
										String value = tokenAttributes.getAttribute(F_VALUE_ATTRIBUTE, "");
										this.fieldRow.addField(new SearchField(name, label, value, size, type));
									}
								}
							}
							else {
								TreeNodeAttributeSet tokenAttributes = TreeNodeAttributeSet.getTagAttributes(token, fieldsGrammar);
								String name = tokenAttributes.getAttribute(F_NAME_ATTRIBUTE);
								if (name != null) {
									String label = tokenAttributes.getAttribute(F_LABEL_ATTRIBUTE, "");
									int size = 1;
									try {
										size = Integer.parseInt(tokenAttributes.getAttribute(F_SIZE_ATTRIBUTE, "1"));
									} catch (NumberFormatException e) {}
									String type = tokenAttributes.getAttribute(F_TYPE_ATTRIBUTE, SearchField.TEXT_TYPE);
									String value = tokenAttributes.getAttribute(F_VALUE_ATTRIBUTE, "");
									this.field = new SearchField(name, label, value, size, type);
								}
							}
						}
						else if (F_OPTION_NODE_NAME.equals(tokenType)) {
							
							if (!fieldsGrammar.isEndTag(token) && (this.field != null)) {
								TreeNodeAttributeSet tokenAttributes = TreeNodeAttributeSet.getTagAttributes(token, fieldsGrammar);
								String value = tokenAttributes.getAttribute(F_VALUE_ATTRIBUTE);
								if (value != null) {
									String label = tokenAttributes.getAttribute(F_LABEL_ATTRIBUTE, value);
									this.field.addOption(label, value);
								}
							}
						}
					}
				}
			});
			
			return ((SearchFieldGroup[]) fieldGroups.toArray(new SearchFieldGroup[fieldGroups.size()]));
		}
		
		private static final Grammar fieldsGrammar = new StandardGrammar();
		private static final Parser fieldsParser = new Parser(fieldsGrammar);
	}
	
	/**
	 * Container for a grid row of of the search fields. This class is intended
	 * for grouping search fields, thus for giving applications providing a
	 * search interface a hint towards how to arrange search fields. The maximum
	 * number of grid units filled by a field row should not exceed 5 to 6 in
	 * order to give interface design a chance of keeping a search interface in
	 * bounds.
	 * 
	 * @author sautter
	 */
	public static class SearchFieldRow {
		
		/**	the name of the index this field group belongs to */
		public final String label;
		
		/** Constructor for an un-labelled field row (row label will be empty string)
		 */
		public SearchFieldRow() {
			this("");
		}
		
		/** Constructor for a labelled field row
		 * @param label
		 */
		public SearchFieldRow(String label) {
			this.label = ((label == null) ? "" : label);
		}
		
		private ArrayList fields = new ArrayList();
		
		/** add a search field to this field row
		 * @param	field	the SearchField to add
		 */
		public void addField(SearchField field) {
			if (field != null) this.fields.add(field);
		}
		
		/**	@return	the search fields of this field row
		 */
		public SearchField[] getFields() {
			return ((SearchField[]) this.fields.toArray(new SearchField[this.fields.size()]));
		}
		
		/**	@return	the width of this field row in grid cells, thus the sum of the size attributes of the fields
		 */
		public int getWidth() {
			int width = 0;
			SearchField[] fields = this.getFields();
			for (int f = 0; f < fields.length; f++)
				width += fields[f].size;
			return width;
		}
		
		/**	write an XML description of this field row to a writer
		 * @param	w	the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer w) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			
			if (this.getWidth() == 0) {
				bw.write("<" + F_FIELD_ROW_NODE_NAME + 
						(((this.label == null) || (this.label.length() == 0)) ? "" : (" " + F_LABEL_ATTRIBUTE + "=\"" + this.label + "\"")) +
						"/>");
				bw.newLine();
				bw.flush();
				
			} else {
				bw.write("<" + F_FIELD_ROW_NODE_NAME + 
						(((this.label == null) || (this.label.length() == 0)) ? "" : (" " + F_LABEL_ATTRIBUTE + "=\"" + this.label + "\"")) +
						">");
				bw.newLine();
				
				SearchField[] fields = this.getFields();
				for (int f = 0; f < fields.length; f++)
					fields[f].writeXml(bw);
				
				bw.write("</" + F_FIELD_ROW_NODE_NAME + ">");
				bw.newLine();
				bw.flush();
			}
		}
	}
	
	/**
	 * A single search field provided by an indexer
	 * 
	 * @author sautter
	 */
	public static class SearchField {
		
		/** the type constant indicating a text search field */
		public static final String TEXT_TYPE = "TEXT";

		/** the type constant indicating a boolean search field */
		public static final String BOOLEAN_TYPE = "BOOLEAN";

		/** the type constant indicating a selector search field */
		public static final String SELECT_TYPE = "SELECT";

		/**
		 * the name of this search field, in case of a boolean field also the
		 * actual value
		 */
		public final String name;

		/** the label of the search field */
		public final String label;

		/**
		 * the value of the search field, in case of a boolean field any
		 * non-null value will cause the field to be pre-selected, otherwise the
		 * initial value
		 */
		public final String value;

		private ArrayList options = null;

		/**
		 * the size of the search field in measures of cells of a search field
		 * grid, a hint for search interface providers towards how wide this
		 * field should be to allow for appropriate input (default is 1, and the
		 * size makes sense only for text fields and selectors, not for
		 * booleans)
		 */
		public final int size;

		/**
		 * the type of the search field, one of TEXT_TYPE (the default),
		 * BOOLEAN_TYPE, or SELECT_TYPE
		 */
		public final String type;

		/**
		 * Constructor for an initially empty text field with size 1
		 * @param name
		 * @param label
		 */
		public SearchField(String name, String label) {
			this(name, label, "", 1, TEXT_TYPE);
		}

		/**
		 * Constructor for a custom size, initially empty text field
		 * @param name
		 * @param label
		 * @param size
		 */
		public SearchField(String name, String label, int size) {
			this(name, label, "", size, TEXT_TYPE);
		}

		/**
		 * Constructor for a custom type field with size 1, in case of a boolean
		 * field, the name is used as the value
		 * @param name
		 * @param label
		 * @param type
		 */
		public SearchField(String name, String label, String type) {
			this(name, label, (BOOLEAN_TYPE.equals(type) ? ("" + true) : ""), 1, type);
		}

		/**
		 * Constructor for a custom type field with size 1 and a custom
		 * (initial) value
		 * @param name
		 * @param label
		 * @param value
		 * @param type
		 */
		public SearchField(String name, String label, String value, String type) {
			this(name, label, value, 1, type);
		}

		/**
		 * Constructor for a custom field
		 * @param name
		 * @param label
		 * @param value
		 * @param size
		 * @param type
		 */
		public SearchField(String name, String label, String value, int size, String type) {
			this.name = name;
			this.label = label;
			if (BOOLEAN_TYPE.equals(type)) {
				this.size = 1;
				this.type = type;
				this.value = ("" + true);
			}
			else if (SELECT_TYPE.equals(type)) {
				this.size = ((size > 0) ? size : 1);
				this.type = type;
				this.options = new ArrayList();
				this.value = value;
			}
			else {
				this.size = ((size > 0) ? size : 1);
				this.type = TEXT_TYPE;
				this.value = value;
			}
		}
		
		/**
		 * @return the options for a select search field, an empty array
		 *         otherwise
		 */
		public SearchFieldOption[] getOptions() {
			if (this.options == null) return new SearchFieldOption[0];
			else return ((SearchFieldOption[]) this.options.toArray(new SearchFieldOption[this.options.size()]));
		}

		/**
		 * add an option to a select search field, using value as label
		 * @param value
		 */
		public void addOption(String value) {
			if (this.options != null) this.options.add(new SearchFieldOption(value));
		}

		/**
		 * add an option to a select search field, using a custom label
		 * @param label
		 * @param value
		 */
		public void addOption(String label, String value) {
			if (this.options != null) this.options.add(new SearchFieldOption(label, value));
		}

		/**
		 * write an XML description of this search field to a writer
		 * @param w the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer w) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			SearchFieldOption[] options = this.getOptions();
			
			if (options.length == 0) {
				bw.write("<" + F_FIELD_NODE_NAME + 
						" " + F_NAME_ATTRIBUTE + "=\"" + this.name + "\"" +
						(((this.label == null) || (this.label.length() == 0)) ? "" : (" " + F_LABEL_ATTRIBUTE + "=\"" + this.label + "\"")) +
						(((this.value == null) || (this.value.length() == 0)) ? "" : (" " + F_VALUE_ATTRIBUTE + "=\"" + this.value + "\"")) +
						((this.size == 1) ? "" : (" " + F_SIZE_ATTRIBUTE + "=\"" + this.size + "\"")) +
						(BOOLEAN_TYPE.equals(this.type) ? (" " + F_TYPE_ATTRIBUTE + "=\"" + this.type + "\"") : "") + // write select field without options as text field
						"/>");
				bw.newLine();
				bw.flush();
			}
			else {
				bw.write("<" + F_FIELD_NODE_NAME + 
						" " + F_NAME_ATTRIBUTE + "=\"" + this.name + "\"" +
						(((this.label == null) || (this.label.length() == 0)) ? "" : (" " + F_LABEL_ATTRIBUTE + "=\"" + this.label + "\"")) +
						(((this.value == null) || (this.value.length() == 0)) ? "" : (" " + F_VALUE_ATTRIBUTE + "=\"" + this.value + "\"")) +
						((this.size == 1) ? "" : (" " + F_SIZE_ATTRIBUTE + "=\"" + this.size + "\"")) +
						" " + F_TYPE_ATTRIBUTE + "=\"" + SELECT_TYPE + "\"" +
						">");
				bw.newLine();
				
				for (int o = 0; o < options.length; o++)
					options[o].writeXml(bw);
				
				bw.write("</" + F_FIELD_NODE_NAME + ">");
				bw.newLine();
				bw.flush();
			}
		}
	}
	
	/**
	 * An option in a select search field
	 * 
	 * @author sautter
	 */
	public static class SearchFieldOption {
		
		/**	the label of the option */
		public final String label;
		
		/**	the value of the option */
		public final String value;

		/** Constructor using value as label
		 * @param value
		 */
		public SearchFieldOption(String value) {
			this(value, value);
		}
		
		/** Constructor with custom label
		 * @param label
		 * @param value
		 */
		public SearchFieldOption(String label, String value) {
			this.label = label;
			this.value = value;
		}
		
		/**	write an XML description of this search field option to a writer
		 * @param	out		the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
			
			bw.write("<" + F_OPTION_NODE_NAME + 
					" " + F_LABEL_ATTRIBUTE + "=\"" + this.label + "\"" +
					" " + F_VALUE_ATTRIBUTE + "=\"" + this.value + "\"" +
					"/>");
			bw.newLine();
			bw.flush();
		}
	}
	
	/** the name of the XML element enclosing the individual search results */
	public static final String RESULTS_NODE_NAME = "results";
	
	/**
	 * the name of the XML element enclosing the individual sub results of an
	 * element of a search results
	 */
	public static final String SUB_RESULTS_NODE_NAME = "subResults";

	/** the name of the XML element enclosing an individual search result */
	public static final String RESULT_NODE_NAME = "result";

	/** the attribute holding the relevance of a search result */
	public static final String RELEVANCE_ATTRIBUTE = "relevance";

	/**
	 * the attribute holding the ID of the master document a retrievable
	 * document was extracted from (used only for collection management)
	 */
	public static final String MASTER_DOCUMENT_ID_ATTRIBUTE = "masterDocId";

	/**
	 * the attribute holding the page number of the first page of the master
	 * document a retrievable document was extracted from
	 */
	public static final String MASTER_PAGE_NUMBER_ATTRIBUTE = "masterPageNumber";

	/**
	 * the attribute holding the last page number of the first page of the
	 * master document a retrievable document was extracted from
	 */
	public static final String MASTER_LAST_PAGE_NUMBER_ATTRIBUTE = "masterLastPageNumber";

	/**
	 * the attribute holding the number of retrievable documents extracted from
	 * uploaded document (used only for collection management)
	 */
	public static final String SUB_DOCUMENT_COUNT_ATTRIBUTE = "subDocCount";

	/**
	 * the name for the attribute holding the title of the master document a
	 * retrievablw document was split from
	 */
	public static final String MASTER_DOCUMENT_TITLE_ATTRIBUTE = "masterDocTitle";

	/** the attributes of a result element in a document search */
	public static final String[] DOCUMENT_ATTRIBUTES = {
		RELEVANCE_ATTRIBUTE,
		DOCUMENT_ID_ATTRIBUTE,
		CHECKIN_USER_ATTRIBUTE,
		MASTER_DOCUMENT_TITLE_ATTRIBUTE,
		DOCUMENT_TITLE_ATTRIBUTE,
		DOCUMENT_AUTHOR_ATTRIBUTE,
		DOCUMENT_DATE_ATTRIBUTE,
		DOCUMENT_SOURCE_LINK_ATTRIBUTE,
		PAGE_NUMBER_ATTRIBUTE,
		LAST_PAGE_NUMBER_ATTRIBUTE
	};

	/**
	 * the Annotation type for Indexers to use for creating a search link (for
	 * use in the Indexer.markSearchables() method)
	 */
	public static final String SEARCH_LINK_ANNOTATION_TYPE = "searchLink_";

	/** the attribute holding the query for a search link */
	public static final String SEARCH_LINK_QUERY_ATTRIBUTE = "_query_";

	/** the attribute holding the title (tooltip) for a search link */
	public static final String SEARCH_LINK_TITLE_ATTRIBUTE = "_title_";

	/**
	 * the attribute indicating that the content of a specific XML element in a
	 * document should be displayed in a separate box within the rest of the
	 * document (has an effect only if RESULT_DISPLAY_MODE_PARAMETER is set to
	 * BOXED_RESULT_DISPLAY_MODE)
	 */
	public static final String BOXED_ATTRIBUTE = "_boxed";

	/** the attribute holding the title a box */
	public static final String BOX_TITLE_ATTRIBUTE = "_boxTitle";

	/** the attribute holding the label a box */
	public static final String BOX_PART_LABEL_ATTRIBUTE = "_boxPartLabel";

	/**
	 * the attribute specifying in which result index to include an Annotation
	 * value
	 */
	public static final String RESULT_INDEX_NAME_ATTRIBUTE = "_indexName";

	/** the attribute holding the label for this result index */
	public static final String RESULT_INDEX_LABEL_ATTRIBUTE = "_indexLabel";

	/**
	 * the attribute specifying which attributes of an Annotation to show in the
	 * result index, and in which order
	 */
	public static final String RESULT_INDEX_FIELDS_ATTRIBUTE = "_indexFields";

	/** the name for the column to store the document number in */
	public static final String DOC_NUMBER_COLUMN_NAME = "DocNumber";
	
	/**
	 * Event class for notifying listeners of documents being updated or deleted.
	 * Updates cover also storing of new documents, in which case the version will
	 * be 1.
	 * 
	 * @author sautter
	 */
	public static class SrsDocumentEvent extends GoldenGateServerEvent {
		
		public static final int UPDATE_TYPE = 0;
		public static final int DELETE_TYPE = 1;
		
		/**
		 * An DocumentStorageListener listens for documents being stored, updated and
		 * deleted in a GoldenGATE server component involved with document IO.
		 * 
		 * @author sautter
		 */
		public static abstract class SrsDocumentEventListener extends GoldenGateServerEventListener {
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.events.GoldenGateServerEventListener#notify(de.uka.ipd.idaho.goldenGateServer.events.GoldenGateServerEvent)
			 */
			public void notify(GoldenGateServerEvent gse) {
				if (gse instanceof SrsDocumentEvent) {
					SrsDocumentEvent dse = ((SrsDocumentEvent) gse);
					if (dse.type == UPDATE_TYPE)
						this.documentUpdated(dse);
					else if (dse.type == DELETE_TYPE)
						this.documentDeleted(dse);
				}
			}
			
			/**
			 * receive notification that a document was updated (can be both a new
			 * document or an updated version of an existing document)
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            update
			 */
			public abstract void documentUpdated(SrsDocumentEvent dse);
			
			/**
			 * receive notification that a document was deleted
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            deletion
			 */
			public abstract void documentDeleted(SrsDocumentEvent dse);
		}
		
		/** The name of the user who caused the event */
		public final String user;
		
		/** The ID of the document affected by the event */
		public final String documentId;
		
		/**
		 * The document affected by the event, null for deletion events. This
		 * document is strictly read-only, any attempt of modification will
		 * result in a RuntimException being thrown.
		 */
		public final QueriableAnnotation document;
		
		/**
		 * The current version number of the document affected by this event, -1
		 * for deletion events
		 */
		public final int version;
		
		/**
		 * Constructor for update events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was updated
		 * @param document the actual document that was updated
		 * @param version the current version number of the document (after the
		 *            update)
		 * @param sourceClassName the class name of the component issuing the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 */
		public SrsDocumentEvent(String user, String documentId, QueriableAnnotation document, int version, String sourceClassName, long eventTime, EventLogger logger) {
			this(user, documentId, document, version, sourceClassName, eventTime, logger, UPDATE_TYPE);
		}
		
		/**
		 * Constructor for deletion events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was deleted
		 * @param sourceClassName the class name of the component issuing the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 */
		public SrsDocumentEvent(String user, String documentId, String sourceClassName, long eventTime, EventLogger logger) {
			this(user, documentId, null, -1, sourceClassName, eventTime, logger, DELETE_TYPE);
		}
		
		private SrsDocumentEvent(String user, String documentId, QueriableAnnotation document, int version, String sourceClassName, long eventTime, EventLogger logger, int type) {
			super(type, sourceClassName, eventTime, (documentId + "-" + eventTime), logger);
			this.user = user;
			this.documentId = documentId;
			this.document = ((document == null) ? null : new ReadOnlyDocument(document, "The document contained in a DocumentStorageEvent cannot be modified."));
			this.version = version;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent#getParameterString()
		 */
		public String getParameterString() {
			return (super.getParameterString() + " " + this.user + " " + this.documentId + " " + this.version);
		}
		
		/**
		 * Parse a document event from its string representation returned by the
		 * getParameterString() method.
		 * @param data the string to parse
		 * @return a document event created from the specified data
		 */
		public static SrsDocumentEvent parseEvent(String data) {
			String[] dataItems = data.split("\\s");
			return new SrsDocumentEvent(dataItems[4], dataItems[5], null, Integer.parseInt(dataItems[6]), dataItems[1], Long.parseLong(dataItems[2]), null, Integer.parseInt(dataItems[0]));
		}
	}
}
