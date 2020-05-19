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


import java.util.Properties;

import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResultElement;
import de.uka.ipd.idaho.stringUtils.StringUtils;

/**
 * Convenience implementation of the indexer interface, leaving only the purpose
 * specific methods for non-abstract sub classes
 * 
 * @author sautter
 */
public abstract class AbstractIndexer extends AbstractGoldenGateSrsPlugin implements Indexer {
	
	/**
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#markEssentialDetails(de.uka.ipd.idaho.gamta.MutableAnnotation)
	 */
	public void markEssentialDetails(MutableAnnotation doc) {
		// do nothing by default
	}

	/**
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#markSearchables(de.uka.ipd.idaho.gamta.MutableAnnotation)
	 */
	public void markSearchables(MutableAnnotation doc) {
		//	do nothing by default
	}
	
	/** @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#addSearchAttributes(de.uka.ipd.idaho.gamta.Annotation[])
	 */
	public void addSearchAttributes(Annotation[] annotations) {
		for (int a = 0; a < annotations.length; a++)
			this.addSearchAttributes(annotations[a]);
	}
	
	/**
	 * produce the full name of a search field, i.e. the field name prefixed by
	 * the indexer's ID
	 * @param fieldName the name of the field to qualify
	 * @return the full name of a search field, in particular getIndexName() + '.' +
	 *         fieldName
	 */
	protected String getFullFieldName(String fieldName) {
		String prefix = this.getIndexName() + ".";
		return (fieldName.startsWith(prefix) ? fieldName : (prefix + fieldName));
	}

	/**
	 * produce the nice name of a search field
	 * @param fieldName the name of the field to encode
	 * @return the nice name of a search field, in particular the specified name
	 *         with the first letter converted to upper case
	 */
	protected String getFieldLabel(String fieldName) {
		return (Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1));
	}

	/**
	 * Obtain the search field group for this indexer, using fully qualified
	 * field names, i.e. field names prefixed with the index name
	 * @return a search field group describing the search fields for this index,
	 *         plus their alignment in rows
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#getSearchFieldGroup()
	 */
	public SearchFieldGroup getSearchFieldGroup() {
		String name = this.getIndexName();
		String namePrefix = (name + ".");
		
		SearchFieldGroup fieldGroup = this.getFieldGroup();
//		SearchFieldGroup qualifiedFieldGroup = new SearchFieldGroup(this.getIndexName(), fieldGroup.label, fieldGroup.legend, fieldGroup.indexEntryLabel);
		SearchFieldGroup qualifiedFieldGroup = new SearchFieldGroup(this.getIndexName(), fieldGroup.label, fieldGroup.tooltip, fieldGroup.indexEntryLabel);
		
		SearchFieldRow[] fieldRows = fieldGroup.getFieldRows();
		for (int r = 0; r < fieldRows.length; r++) {
//			SearchFieldRow qualifiedFieldRow = new SearchFieldRow(fieldRows[r].label);
			SearchFieldRow qualifiedFieldRow = new SearchFieldRow(fieldRows[r].label, fieldRows[r].tooltip);
			
			SearchField[] fields = fieldRows[r].getFields();
			for (int f = 0; f < fields.length; f++) {
//				SearchField qualifiedField = new SearchField(
//							(fields[f].name.startsWith(namePrefix) ? fields[f].name : (namePrefix + fields[f].name)),
//							fields[f].label,
//							fields[f].value,
//							fields[f].size,
//							fields[f].type
//						);
				SearchField qualifiedField = new SearchField(
						(fields[f].name.startsWith(namePrefix) ? fields[f].name : (namePrefix + fields[f].name)),
						fields[f].label,
						fields[f].tooltip,
						fields[f].value,
						fields[f].size,
						fields[f].type
					);
				
				if (SearchField.SELECT_TYPE.equals(qualifiedField.type)) {
					SearchFieldOption[] fieldOptions = fields[f].getOptions();
					for (int o = 0; o < fieldOptions.length; o++)
						qualifiedField.addOption(fieldOptions[o].value, fieldOptions[o].label);
				}
				
				qualifiedFieldRow.addField(qualifiedField);
			}
			
			qualifiedFieldGroup.addFieldRow(qualifiedFieldRow);
		}
		return qualifiedFieldGroup;
	}
	
	/**
	 * obtain the non-qualified search field group for this indexer
	 * @return a search field group describing the search fields for this index,
	 *         plus their alignment in rows
	 */
	protected abstract SearchFieldGroup getFieldGroup();
	
	/**
	 * replace a language specific special characters in a string with their
	 * US-ASCII base forms
	 * @param string the string to normalize
	 * @return the normalized string
	 */
	public static String prepareSearchString(String string) {
		StringBuffer sb = new StringBuffer();
		for (int c = 0; c < string.length(); c++)
			sb.append(charNormalizationMappings.getProperty(string.substring(c, (c+1)), StringUtils.getNormalForm(string.charAt(c))));
		return sb.toString();
	}
	//	TODO use StringUtils here
	private static Properties charNormalizationMappings = new Properties();
	static {
		charNormalizationMappings.setProperty("ß","ss");
		charNormalizationMappings.setProperty("–","-");
		charNormalizationMappings.setProperty("—","-");
		charNormalizationMappings.setProperty("\u2010","-");
		charNormalizationMappings.setProperty("\u2011","-");
		charNormalizationMappings.setProperty("\u2012","-");
		charNormalizationMappings.setProperty("\u2013","-");
		charNormalizationMappings.setProperty("\u2014","-");
		charNormalizationMappings.setProperty("\u2015","-");
		charNormalizationMappings.setProperty("\u2212","-");
	}
	
	/**
	 * SQL database backed implementation of an index result, useful for
	 * specific indexers to extend. This class expects that the result
	 * attributes handed to the constructor correspond to the second through
	 * last columns of the SqlQueryResult handed to the constructor. It further
	 * expects that the document number is the first column of the
	 * SqlQueryResult handed to the constructor, and the column to use as the
	 * entries' value is the second column.
	 * 
	 * @author sautter
	 */
	protected static class SqlIndexResult extends IndexResult {
		private SqlResultDataSource data;
		
		/** the type to use for the result entries */
		protected final String entryType;
		
		/**
		 * Constructor
		 * @param resultAttributes the attribute names for the lookup result, in
		 *            the order the attribute values should be displayed
		 * @param indexName the name of the index this result comes from
		 * @param indexLabel a nice name for the index
		 * @param sqr the SqlQueryResult to read result element from
		 */
		protected SqlIndexResult(String[] resultAttributes, String indexName, String indexLabel, SqlQueryResult sqr) {
			this(resultAttributes, indexName, indexLabel, null, sqr);
		}
		
		/**
		 * Constructor
		 * @param resultAttributes the attribute names for the lookup result, in
		 *            the order the attribute values should be displayed
		 * @param indexName the name of the index this result comes from
		 * @param indexLabel a nice name for the index
		 * @param entryType the type to use for the result entries (if null,
		 *            indexName will be used)
		 * @param sqr the SqlQueryResult to read result element from
		 */
		protected SqlIndexResult(String[] resultAttributes, String indexName, String indexLabel, String entryType, SqlQueryResult sqr) {
			super(resultAttributes, indexName, indexLabel);
			this.entryType = ((entryType == null) ? indexName : entryType);
			this.data = new SqlResultDataSource(sqr);
		}
		
		/**
		 * Implemented to check if the backing SqlQueryResult has data for
		 * further elements.
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#hasNextElement()
		 */
		public final boolean hasNextElement() {
			return this.data.hasNextElement();
		}
		
		/**
		 * Implemented to retrieve the data for the next element from the
		 * backing SqlQueryResult, create an IndexResultElement from it (via
		 * the decodeResultElement() method), and return that element.
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#getNextElement()
		 */
		public final SrsSearchResultElement getNextElement() {
			String[] elementData = this.data.getNextElementData();
			return ((elementData == null) ? null : this.decodeResultElement(elementData));
		}
		
		/**
		 * Create an index result element from the data of a tupel in the
		 * backing SqlQueryResult. The size of the array and the content and
		 * order of the contained strings is directly dependent on the
		 * SqlQueryResult handed to the constructor. This implementation expects
		 * that the result attributes handed to the constructor correspond to
		 * the second through last columns of the SqlQueryResult handed to the
		 * constructor. It further expects that the document number is the first
		 * column of the SqlQueryResult handed to the constructor, and the
		 * column to use as the entries' value is the second column. If the data
		 * does not meet these conditions for any reason, sub classes have to
		 * overwrite this method in order to handle the data appropriately.
		 * @param elementData the data to decode
		 * @return an index result element created from the specified data
		 */
		protected IndexResultElement decodeResultElement(String[] elementData) {
			IndexResultElement ire = new IndexResultElement(Long.parseLong(elementData[0]), this.entryType, ((elementData[1] == null) ? "" : elementData[1]));
			if (elementData[1] != null)
				ire.setAttribute(this.resultAttributes[0], elementData[1]);
			
			for (int a = 1; a < this.resultAttributes.length; a++) {
				if (elementData[a + 1] != null)
					ire.setAttribute(this.resultAttributes[a], elementData[a + 1]);
			}
			
			return ire;
		}
	}
	
	/**
	 * SQL database backed implementation of a thesaurus result, useful for
	 * specific indexers to extend. This class expects that the result
	 * attributes handed to the constructor correspond to the columns of the
	 * SqlQueryResult handed to the constructor.
	 * 
	 * @author sautter
	 */
	protected static class SqlThesaurusResult extends ThesaurusResult {
		private SqlResultDataSource data;
		
		/**
		 * Constructor
		 * @param resultFieldNames the field names for the lookup result, in the
		 *            order they should be displayed
		 * @param entryType the annotation type this thesaurus result contains
		 * @param thesaurusName the name of the thesaurus this result comes from
		 * @param sqr the SqlQueryResult to read result element from
		 */
		protected SqlThesaurusResult(String[] resultFieldNames, String entryType, String thesaurusName, SqlQueryResult sqr) {
			super(resultFieldNames, entryType, thesaurusName);
			this.data = new SqlResultDataSource(sqr);
		}
		
		/**
		 * Implemented to check if the backing SqlQueryResult has data for
		 * further elements.
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#hasNextElement()
		 */
		public final boolean hasNextElement() {
			return this.data.hasNextElement();
		}
		
		/**
		 * Implemented to retrieve the data for the next element from the
		 * backing SqlQueryResult, create a ThesaurusResultElement from it (via
		 * the decodeResultElement() method), and return that element.
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#getNextElement()
		 */
		public final SrsSearchResultElement getNextElement() {
			String[] elementData = this.data.getNextElementData();
			return ((elementData == null) ? null : this.decodeResultElement(elementData));
		}
		
		/**
		 * Create a thesaurus result element from the data of a tupel in the
		 * backing SqlQueryResult. The size of the array and the content and
		 * order of the contained strings is directly dependent on the
		 * SqlQueryResult handed to the constructor. This implementation expects
		 * that the result attributes handed to the constructor correspond to
		 * the columns of the SqlQueryResult handed to the constructor. If the
		 * data does not meet this condition for any reason, sub classes have to
		 * overwrite this method in order to handle the data appropriately.
		 * @param elementData the data to decode
		 * @return a thesaurus result element created from the specified data
		 */
		protected ThesaurusResultElement decodeResultElement(String[] elementData) {
			ThesaurusResultElement tre = new ThesaurusResultElement();
			for (int a = 0; a < this.resultAttributes.length; a++)
				if (elementData[a] != null)
					tre.setAttribute(this.resultAttributes[a], elementData[a]);
			return tre;
		}
	}
	
	/**
	 * Utility class for the SQL backed result objects.
	 * 
	 * @author sautter
	 */
	private static class SqlResultDataSource {
		private SqlQueryResult sqr;
		private String[] nextElementData = null;
		
		SqlResultDataSource(SqlQueryResult sqr) {
			this.sqr = sqr;
		}
		
		boolean hasNextElement() {
			this.fillElementBuffer();
			return this.gotElementInBuffer();
		}
		
		String[] getNextElementData() {
			this.fillElementBuffer();
			return this.getFromElementBuffer();
		}
		
		private void fillElementBuffer() {
			if ((this.sqr == null) || this.gotElementInBuffer()) return;
			
			else if (this.sqr.next()) {
				String[] dataCollector = new String[this.sqr.getColumnCount()];
				for (int c = 0; c < dataCollector.length; c++)
					dataCollector[c] = this.sqr.getString(c);
				this.putElementInBuffer(dataCollector);
			}
			
			else {
				this.sqr.close();
				this.sqr = null;
			}
		}
		
		private boolean gotElementInBuffer() {
			return (this.nextElementData != null);
		}
		private void putElementInBuffer(String[] re) {
			this.nextElementData = re;
		}
		private String[] getFromElementBuffer() {
			String[] next = this.nextElementData;
			this.nextElementData = null;
			return next;
		}
		
		protected void finalize() throws Throwable {
			if (this.sqr != null) {
				this.sqr.close();
				this.sqr = null;
			}
		}
	}
}
