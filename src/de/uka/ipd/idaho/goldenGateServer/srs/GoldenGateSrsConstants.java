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
package de.uka.ipd.idaho.goldenGateServer.srs;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.ReadOnlyDocument;
import de.uka.ipd.idaho.goldenGateServer.dst.DocumentStoreConstants;
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
public interface GoldenGateSrsConstants extends DocumentStoreConstants {
	
	/** the list documents command */
	public static final String LIST_DOCUMENTS = "SRS_LIST_DOCUMENTS";
	
	/** the command for obtaining search forms */
	public static final String GET_SEARCH_FIELDS = "SRS_GET_SEARCH_FIELDS";
	
	/** the command for obtaining the timestamp of the last data modification */
	public static final String GET_LAST_MODIFIED = "SRS_GET_LAST_MODIFIED";
	
	
	/** the command for obtaining statistics on the document collection */
	public static final String GET_STATISTICS = "SRS_GET_STATISTICS";

	/** the master doc count attribute of the statistics */
	public static final String MASTER_DOCUMENT_COUNT_ATTRIBUTE = "MasterCount";

	/** the doc count attribute of the statistics */
	public static final String DOCUMENT_COUNT_ATTRIBUTE = "DocCount";

	/** the word count attribute of the statistics */
	public static final String WORD_COUNT_ATTRIBUTE = "WordCount";

	/** the master doc count attribute of the statistics since a given time */
	public static final String MASTER_DOCUMENT_COUNT_SINCE_ATTRIBUTE = "MasterCountSince";

	/** the doc count attribute of the statistics since a given time */
	public static final String DOCUMENT_COUNT_SINCE_ATTRIBUTE = "DocCountSince";

	/** the word count attribute of the statistics since a given time */
	public static final String WORD_COUNT_SINCE_ATTRIBUTE = "WordCountSince";
	
	/** the parameter for obtaining statistics on the document collection since a given time */
	public static final String GET_STATISTICS_SINCE_PARAMETER = "since";
	
	/** constant for the GET_STATISICS_SINCE_PARAMETER to obtain the statistics for the last year */
	public static final String GET_STATISTICS_LAST_YEAR = "year";
	
	/** constant for the GET_STATISICS_SINCE_PARAMETER to obtain the statistics for the last half year */
	public static final String GET_STATISTICS_LAST_HALF_YEAR = "halfYear";
	
	/** constant for the GET_STATISICS_SINCE_PARAMETER to obtain the statistics for the last three months */
	public static final String GET_STATISTICS_LAST_THREE_MONTHS = "threeMonths";
	
	/** constant for the GET_STATISICS_SINCE_PARAMETER to obtain the statistics for the last month */
	public static final String GET_STATISTICS_LAST_MONTH = "month";
	

	/** the command for searching documents */
	public static final String SEARCH_DOCUMENTS = "SRS_SEARCH_DOCUMENTS";

	/**
	 * the command for searching document meta data and relevance, plus essential
	 * details contributed by indexers
	 */
	public static final String SEARCH_DOCUMENT_DETAILS = "SRS_SEARCH_DOCUMENT_DETAILS";
	
	/**
	 * the command for searching document meta data, the relevance, the ID, the
	 * title, author, page number, and checkin user, but not the documents proper
	 */
	public static final String SEARCH_DOCUMENT_DATA = "SRS_SEARCH_DOCUMENT_DATA";
	
	/**
	 * the command for searching document IDs, the relevance and the ID, but not
	 * the document itself
	 */
	public static final String SEARCH_DOCUMENT_IDS = "SRS_SEARCH_DOCUMENT_IDS";
	
	/** the command for obtaining one specific document in its plain XML form */
	public static final String GET_XML_DOCUMENT = "SRS_GET_XML_DOCUMENT";
	
	/** the command for obtaining a version one specific document in its plain XML form */
	public static final String GET_XML_DOCUMENT_VERSION = "SRS_GET_XML_DOCUMENT_VERSION";
	
	/** the command for searching entries of index tables */
	public static final String SEARCH_INDEX = "SRS_SEARCH_INDEX";
	
	/** the command for thesaurus-style searching index tables */
	public static final String SEARCH_THESAURUS = "SRS_SEARCH_THESAURUS";
	
	/** the query parameter holding specific document IDs */
	public static final String ID_QUERY_FIELD_NAME = "idQuery";
	
	/** the query parameter holding specific document UUIDs */
	public static final String UUID_QUERY_FIELD_NAME = "uuid";
	
	/** the query parameter holding a specific version number in conjunction with specific document IDs */
	public static final String VERSION_QUERY_FIELD_NAME = "version";

	/**
	 * the query parameter holding the name of the index to obtain results from
	 * (the index to search for SEARCH_THESAURUS, the index to return results
	 * from for SEARCH_INDEX)
	 */
	public static final String INDEX_NAME = "indexName";
	
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
	 * date format for parsing human readable input for document modification
	 * dates, expecting <code>YYYY-MM-DD</code> formatted input
	 */
	public static DateFormat MODIFIED_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	
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
	 * the query parameter for specifying whether or not to include the update
	 * history in the attributes of the result documents (only for
	 * SEARCH_DOCUMENTS and SEARCH_DOCUMENT_DETAILS commands).
	 */
	public static final String INCLUDE_UPDATE_HISTORY_PARAMETER = "includeUpdateHistory";

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
	
	
	/** the name of the XML element enclosing all search field groups belonging to a GoldenGATE SRS */
	public static final String SEARCH_FIELD_GROUPS_NODE_NAME = "searchFieldGroups";

	/** the name of the XML element representing a search field */
	public static final String FIELD_NODE_NAME = "field";

	/** the attribute holding the name of a search field or group thereof */
	public static final String FIELD_NAME_ATTRIBUTE = "name";

	/** the attribute holding the label / nice name of a search field or group thereof */
	public static final String FIELD_LABEL_ATTRIBUTE = "label";

	/** the attribute holding the explanatory tooltip / title of a search field or group thereof */
	public static final String FIELD_TOOLTIP_ATTRIBUTE = "tooltip";

	/** the name of the XML element enclosing a group of search fields */
	public static final String SEARCH_FIELD_GROUP_NODE_NAME = "searchFieldGroup";

	/** the name of the XML element representing a search field row */
	public static final String SEARCH_FIELD_ROW_NODE_NAME = "searchFieldRow";

	/** the name of the XML element representing an individual search field */
	public static final String SEARCH_FIELD_NODE_NAME = "searchField";

	/**
	 * the name of the XML element representing an option in a select search
	 * field
	 */
	public static final String SEARCH_FIELD_OPTION_NODE_NAME = "option";

	/** the attribute holding the legend of a search field group */
	public static final String FIELD_INDEX_ENTRY_LABEL_ATTRIBUTE = "indexEntryLabel";

	/** the attribute holding the type of a search field */
	public static final String FIELD_TYPE_ATTRIBUTE = "type";

	/** the attribute holding the size (in grid cells) of a search field */
	public static final String FIELD_SIZE_ATTRIBUTE = "size";

	/**
	 * the attribute holding the value of a search field, or an option of a
	 * select field
	 */
	public static final String FIELD_VALUE_ATTRIBUTE = "value";
	
	/**
	 * Container for all of the search fields provided by a single indexer
	 * 
	 * @author sautter
	 */
	public static class SearchFieldGroup {
		
		/** the name of the index this field group belongs to */
		public final String indexName;

		/** the label (nice name) of the index this field group belongs to */
		public final String label;
		
		/**
		 * the tooltip of the field group, i.e. a brief textual explanation of
		 * what the to search for with the fields of this group
		 */
		public final String tooltip;

		/**
		 * the label (nice name) of the entries of the index this field group
		 * belongs to (null indicates that the index entries cannot be the
		 * result of a search on their own)
		 */
		public final String indexEntryLabel;

		/**
		 * Constructor using the index name as the label, no tooltip, and no
		 * entry label
		 * @param indexName the name of the index this field group belongs to
		 * @param tooltip the tooltip of the field group, i.e. a brief textual
		 *            explanation of what the to search for with the fields of
		 *            this group
		 */
		public SearchFieldGroup(String indexName, String tooltip) {
			this(indexName, indexName, tooltip, null);
		}

		/**
		 * Constructor using no index ID, and no entry label
		 * @param indexName the name of the index this field group belongs to
		 * @param label the label (nice name) of the index this field group
		 *            belongs to
		 * @param tooltip the tooltip of the field group, i.e. a brief textual
		 *            explanation of what the to search for with the fields of
		 *            this group
		 */
		public SearchFieldGroup(String indexName, String label, String tooltip) {
			this(indexName, label, tooltip, null);
		}

		/**
		 * Constructor using no index ID
		 * @param indexName the name of the index this field group belongs to
		 * @param label the label (nice name) of the index this field group
		 *            belongs to
		 * @param tooltip the tooltip of the field group, i.e. a brief textual
		 *            explanation of what the to search for with the fields of
		 *            this group
		 * @param entryLabel the label (nice name) of the entries of the index
		 *            this field group belongs to (null indicates that the index
		 *            entries cannot be the result of a search on their own)
		 */
		public SearchFieldGroup(String indexName, String label, String tooltip, String entryLabel) {
			if ((indexName == null) || (indexName.trim().length() == 0))
				throw new IllegalArgumentException("Invalid index name.");
			this.indexName = indexName;
			this.label = ((label == null) ? "" : label.trim());
			this.tooltip = ((tooltip == null) ? "" : tooltip.trim());
			this.indexEntryLabel = ((entryLabel == null) ? null : ((entryLabel.trim().length() == 0) ? null : entryLabel));
		}
		
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
			if (this.indexEntryLabel == null)
				return null;
			else if (this.indexEntryLabel.endsWith("s") || this.indexEntryLabel.endsWith("x"))
				return this.indexEntryLabel;
			else if (this.indexEntryLabel.matches("\\.+[aeiou]y"))
				return (this.indexEntryLabel + "s");
			else if (this.indexEntryLabel.endsWith("y"))
				return (this.indexEntryLabel.substring(0, (this.indexEntryLabel.length() - 1)) + "ies");
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
			
			if (this.getWidth() == 0)
				bw.write("<" + SEARCH_FIELD_GROUP_NODE_NAME + 
						" " + FIELD_NAME_ATTRIBUTE + "=\"" + this.indexName + "\"" +
						((this.label.length() == 0) ? "" : (" " + FIELD_LABEL_ATTRIBUTE + "=\"" + grammar.escape(this.label) + "\"")) +
						((this.tooltip.length() == 0) ? "" : (" " + FIELD_TOOLTIP_ATTRIBUTE + "=\"" + grammar.escape(this.tooltip) + "\"")) +
						((this.indexEntryLabel == null) ? "" : (" " + FIELD_INDEX_ENTRY_LABEL_ATTRIBUTE + "=\"" + grammar.escape(this.indexEntryLabel) + "\"")) +
						"/>");
			else {
				bw.write("<" + SEARCH_FIELD_GROUP_NODE_NAME + 
						" " + FIELD_NAME_ATTRIBUTE + "=\"" + this.indexName + "\"" +
						((this.label.length() == 0) ? "" : (" " + FIELD_LABEL_ATTRIBUTE + "=\"" + grammar.escape(this.label) + "\"")) +
						((this.tooltip.length() == 0) ? "" : (" " + FIELD_TOOLTIP_ATTRIBUTE + "=\"" + grammar.escape(this.tooltip) + "\"")) +
						((this.indexEntryLabel == null) ? "" : (" " + FIELD_INDEX_ENTRY_LABEL_ATTRIBUTE + "=\"" + grammar.escape(this.indexEntryLabel) + "\"")) +
						">");
				bw.newLine();
				SearchFieldRow[] rows = this.getFieldRows();
				for (int r = 0; r < rows.length; r++)
					rows[r].writeXml(bw);
				bw.write("</" + SEARCH_FIELD_GROUP_NODE_NAME + ">");
			}
			
			bw.newLine();
			if (w != bw)
				bw.flush();
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
			parser.stream(r, new TokenReceiver() {
				
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
					if (!grammar.isTag(token))
						return;
					
					String tagType = grammar.getType(token); 
					TreeNodeAttributeSet tagAttributes = (grammar.isEndTag(token) ? null : TreeNodeAttributeSet.getTagAttributes(token, grammar));
					
					if (SEARCH_FIELD_NODE_NAME.equals(tagType) && (this.fieldRow != null)) {
						if (tagAttributes == null) {
							if (this.field != null)
								this.fieldRow.addField(this.field);
							this.field = null;
						}
						else {
							String name = tagAttributes.getAttribute(FIELD_NAME_ATTRIBUTE);
							if (name != null) {
								String label = tagAttributes.getAttribute(FIELD_LABEL_ATTRIBUTE, "");
								String tooltip = tagAttributes.getAttribute(FIELD_TOOLTIP_ATTRIBUTE, "");
								int size = 1;
								try {
									size = Integer.parseInt(tagAttributes.getAttribute(FIELD_SIZE_ATTRIBUTE, "1"));
								} catch (NumberFormatException e) {}
								String type = tagAttributes.getAttribute(FIELD_TYPE_ATTRIBUTE, SearchField.TEXT_TYPE);
								String value = tagAttributes.getAttribute(FIELD_VALUE_ATTRIBUTE, "");
								if (grammar.isSingularTag(token))
									this.fieldRow.addField(new SearchField(name, label, tooltip, value, size, type));
								else this.field = new SearchField(name, label, tooltip, value, size, type);
							}
						}
					}
					else if (SEARCH_FIELD_OPTION_NODE_NAME.equals(tagType) && (this.field != null) && (tagAttributes != null)) {
						String value = tagAttributes.getAttribute(FIELD_VALUE_ATTRIBUTE);
						if (value != null) {
							String label = tagAttributes.getAttribute(FIELD_LABEL_ATTRIBUTE, value);
							this.field.addOption(value, label);
						}
					}
					else if (SEARCH_FIELD_ROW_NODE_NAME.equals(tagType) && (this.fieldGroup != null)) {
						if (tagAttributes == null) {
							if (this.fieldRow != null)
								this.fieldGroup.addFieldRow(this.fieldRow);
							this.fieldRow = null;
						}
						else if (!grammar.isSingularTag(token)) {
							String label = tagAttributes.getAttribute(FIELD_LABEL_ATTRIBUTE, "");
							String tooltip = tagAttributes.getAttribute(FIELD_TOOLTIP_ATTRIBUTE, "");
							this.fieldRow = new SearchFieldRow(label, tooltip);
						}
					}
					else if (SEARCH_FIELD_GROUP_NODE_NAME.equals(tagType)) {
						if (tagAttributes == null) {
							if (this.fieldGroup != null)
								fieldGroups.add(this.fieldGroup);
							this.fieldGroup = null;
						}
						else {
							String indexName = tagAttributes.getAttribute(FIELD_NAME_ATTRIBUTE);
							if (indexName != null) {
								String label = tagAttributes.getAttribute(FIELD_LABEL_ATTRIBUTE, "");
								String tooltip = tagAttributes.getAttribute(FIELD_TOOLTIP_ATTRIBUTE, "");
								String entryLabel = tagAttributes.getAttribute(FIELD_INDEX_ENTRY_LABEL_ATTRIBUTE);
								if (grammar.isSingularTag(token))
									fieldGroups.add(new SearchFieldGroup(indexName, label, tooltip, entryLabel));
								else this.fieldGroup = new SearchFieldGroup(indexName, label, tooltip, entryLabel);
							}
						}
					}
				}
			});
			
			return ((SearchFieldGroup[]) fieldGroups.toArray(new SearchFieldGroup[fieldGroups.size()]));
		}
		
		static final Grammar grammar = new StandardGrammar();
		static final Parser parser = new Parser(grammar);
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
		
		/**	the label (nice name) of the field row */
		public final String label;

		/**
		 * the tooltip of the field row, i.e. a brief textual explanation of
		 * what the to search for with the fields of this row
		 */
		public final String tooltip;
		
		/** Constructor for an un-labeled field row (row label will be empty string)
		 */
		public SearchFieldRow() {
			this("", null);
		}
		
		/** Constructor for a labeled field row
		 * @param label the label (nice name) for the field row
		 */
		public SearchFieldRow(String label) {
			this(label, null);
		}
		
		/** Constructor for a labeled field row with a tooltip
		 * @param label the label (nice name) for the field row
		 * @param tooltip the tooltip (explanation text) for the field row
		 */
		public SearchFieldRow(String label, String tooltip) {
			this.label = ((label == null) ? "" : label.trim());
			this.tooltip = ((tooltip == null) ? "" : tooltip.trim());
		}

		private ArrayList fields = new ArrayList();
		
		/** add a search field to this field row
		 * @param	field	the SearchField to add
		 */
		public void addField(SearchField field) {
			if (field != null)
				this.fields.add(field);
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
			
			if (this.getWidth() == 0)
				bw.write("<" + SEARCH_FIELD_ROW_NODE_NAME + 
						((this.label.length() == 0) ? "" : (" " + FIELD_LABEL_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.label) + "\"")) +
						((this.tooltip.length() == 0) ? "" : (" " + FIELD_TOOLTIP_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.tooltip) + "\"")) +
						"/>");
			else {
				bw.write("<" + SEARCH_FIELD_ROW_NODE_NAME + 
						((this.label.length() == 0) ? "" : (" " + FIELD_LABEL_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.label) + "\"")) +
						((this.tooltip.length() == 0) ? "" : (" " + FIELD_TOOLTIP_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.tooltip) + "\"")) +
						">");
				bw.newLine();
				SearchField[] fields = this.getFields();
				for (int f = 0; f < fields.length; f++)
					fields[f].writeXml(bw);
				bw.write("</" + SEARCH_FIELD_ROW_NODE_NAME + ">");
			}
			
			bw.newLine();
			if (w != bw)
				bw.flush();
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
		 * the tooltip of the field, i.e. a brief textual explanation of
		 * what the to search for with the field
		 */
		public final String tooltip;

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
		public SearchField(String name, String label, String tooltip) {
			this(name, label, tooltip, "", 1, TEXT_TYPE);
		}

		/**
		 * Constructor for a custom size, initially empty text field
		 * @param name
		 * @param label
		 * @param size
		 */
		public SearchField(String name, String label, String tooltip, int size) {
			this(name, label, tooltip, "", size, TEXT_TYPE);
		}

		/**
		 * Constructor for a custom type field with size 1, in case of a boolean
		 * field, the name is used as the value
		 * @param name
		 * @param label
		 * @param type
		 */
		public SearchField(String name, String label, String tooltip, String type) {
			this(name, label, tooltip, (BOOLEAN_TYPE.equals(type) ? ("" + true) : ""), 1, type);
		}

		/**
		 * Constructor for a custom type field with size 1 and a custom
		 * (initial) value
		 * @param name
		 * @param label
		 * @param value
		 * @param type
		 */
		public SearchField(String name, String label, String tooltip, String value, String type) {
			this(name, label, tooltip, value, 1, type);
		}

		/**
		 * Constructor for a custom field
		 * @param name
		 * @param label
		 * @param value
		 * @param size
		 * @param type
		 */
		public SearchField(String name, String label, String tooltip, String value, int size, String type) {
			if ((name == null) || (name.trim().length() == 0))
				throw new IllegalArgumentException("Invalid field name.");
			this.name = name;
			this.label = ((label == null) ? "" : label.trim());
			this.tooltip = ((tooltip == null) ? "" : tooltip.trim());
			if (BOOLEAN_TYPE.equals(type)) {
				this.size = 1;
				this.type = type;
				this.value = ("" + true);
			}
			else if (SELECT_TYPE.equals(type)) {
				this.size = ((size > 0) ? size : 1);
				this.type = type;
				this.options = new ArrayList();
				this.value = ((value == null) ? "" : value.trim());
			}
			else {
				this.size = ((size > 0) ? size : 1);
				this.type = TEXT_TYPE;
				this.value = ((value == null) ? "" : value.trim());
			}
		}
		
		/**
		 * @return the options for a select search field, an empty array
		 *         otherwise
		 */
		public SearchFieldOption[] getOptions() {
			if (this.options == null)
				return new SearchFieldOption[0];
			else return ((SearchFieldOption[]) this.options.toArray(new SearchFieldOption[this.options.size()]));
		}

		/**
		 * add an option to a select search field, using value as label
		 * @param value
		 */
		public void addOption(String value) {
			if (this.options != null)
				this.options.add(new SearchFieldOption(value));
		}

		/**
		 * add an option to a select search field, using a custom label
		 * @param label
		 * @param value
		 */
		public void addOption(String value, String label) {
			if (this.options != null)
				this.options.add(new SearchFieldOption(value, label));
		}
		
		/**
		 * write an XML description of this search field to a writer
		 * @param w the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer w) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			SearchFieldOption[] options = this.getOptions();
			
			if ((options.length == 0) || !SELECT_TYPE.equals(this.type))
				bw.write("<" + SEARCH_FIELD_NODE_NAME + 
						" " + FIELD_NAME_ATTRIBUTE + "=\"" + this.name + "\"" +
						((this.label.length() == 0) ? "" : (" " + FIELD_LABEL_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.label) + "\"")) +
						((this.tooltip.length() == 0) ? "" : (" " + FIELD_TOOLTIP_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.tooltip) + "\"")) +
						((this.value.length() == 0) ? "" : (" " + FIELD_VALUE_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.value) + "\"")) +
						((this.size == 1) ? "" : (" " + FIELD_SIZE_ATTRIBUTE + "=\"" + this.size + "\"")) +
						(BOOLEAN_TYPE.equals(this.type) ? (" " + FIELD_TYPE_ATTRIBUTE + "=\"" + this.type + "\"") : "") + // write select field without options as text field
						"/>");
			else {
				bw.write("<" + SEARCH_FIELD_NODE_NAME + 
						" " + FIELD_NAME_ATTRIBUTE + "=\"" + this.name + "\"" +
						((this.label.length() == 0) ? "" : (" " + FIELD_LABEL_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.label) + "\"")) +
						((this.tooltip.length() == 0) ? "" : (" " + FIELD_TOOLTIP_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.tooltip) + "\"")) +
						((this.value.length() == 0) ? "" : (" " + FIELD_VALUE_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.value) + "\"")) +
						((this.size == 1) ? "" : (" " + FIELD_SIZE_ATTRIBUTE + "=\"" + this.size + "\"")) +
						(" " + FIELD_TYPE_ATTRIBUTE + "=\"" + this.type + "\"") + 
						">");
				bw.newLine();
				for (int o = 0; o < options.length; o++)
					options[o].writeXml(bw);
				bw.write("</" + SEARCH_FIELD_NODE_NAME + ">");
			}
			
			bw.newLine();
			if (w != bw)
				bw.flush();
		}
	}
	
	/**
	 * An option in a select search field
	 * 
	 * @author sautter
	 */
	public static class SearchFieldOption {
		
		/**	the value of the option */
		public final String value;
		
		/**	the label of the option */
		public final String label;

		/** Constructor using value as label
		 * @param value
		 */
		SearchFieldOption(String value) {
			this(value, null);
		}
		
		/** Constructor with custom label
		 * @param value
		 * @param label
		 */
		SearchFieldOption(String value, String label) {
			this.value = value;
			this.label = ((label == null) ? value : label.trim());
		}
		
		/**	write an XML description of this search field option to a writer
		 * @param	out		the writer to write to
		 * @throws IOException
		 */
		public void writeXml(Writer w) throws IOException {
			BufferedWriter bw = ((w instanceof BufferedWriter) ? ((BufferedWriter) w) : new BufferedWriter(w));
			bw.write("<" + SEARCH_FIELD_OPTION_NODE_NAME + 
					" " + FIELD_LABEL_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.label) + "\"" +
					" " + FIELD_VALUE_ATTRIBUTE + "=\"" + SearchFieldGroup.grammar.escape(this.value) + "\"" +
					"/>");
			bw.newLine();
			if (w != bw)
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
	 * The attribute holding the UUID of a retrievable document. This attribute
	 * is designed to hold externally issued UUIDs of documents. ID resolution
	 * works against both document ID and document UUID.
	 */
	public static final String DOCUMENT_UUID_ATTRIBUTE = "docUuid";

	/**
	 * The attribute holding the source of a UUID attached to a retrievable
	 * document. This attribute is designed to hold the source of externally
	 * issued UUIDs of documents.
	 */
	public static final String DOCUMENT_UUID_SOURCE_ATTRIBUTE = "docUuidSource";

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
	 * retrievable document was split from
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
	public static class SrsDocumentEvent extends DataObjectEvent {
		
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
		
		/** The ID of the document affected by the event
		 * @deprecated use dataId instead */
		public final String documentId;
		
		/**
		 * The document affected by the event, null for deletion events. This
		 * document is strictly read-only, any attempt of modification will
		 * result in a RuntimException being thrown.
		 */
		public final QueriableAnnotation document;
		//	TODOnot eliminate this field, as it clogs memory when many events queue up
		//	TODOnot introduce overwritable getter method instead, returning null, overwritten inside SRS for updates only
		//	TODOnot back said getter with 256 or so sized cache in SRS
		//	==> NO NEED, as EXP doesn't enqueue objects of this class proper, only update events that only store the document ID
		
		/**
		 * Constructor for update events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was updated
		 * @param document the actual document that was updated
		 * @param version the current version number of the document (after the
		 *            update)
		 * @param sourceClassName the class name of the component issuing the event
		 * @param eventTime the timstamp of the event
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
		 * @param eventTime the timstamp of the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 */
		public SrsDocumentEvent(String user, String documentId, String sourceClassName, long eventTime, EventLogger logger) {
			this(user, documentId, null, -1, sourceClassName, eventTime, logger, DELETE_TYPE);
		}
		
		private SrsDocumentEvent(String user, String documentId, QueriableAnnotation document, int version, String sourceClassName, long eventTime, EventLogger logger, int type) {
			super(user, documentId, version, type, sourceClassName, eventTime, logger);
			this.documentId = documentId;
			this.document = ((document == null) ? null : new ReadOnlyDocument(document, "The document contained in a DocumentStorageEvent cannot be modified."));
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
