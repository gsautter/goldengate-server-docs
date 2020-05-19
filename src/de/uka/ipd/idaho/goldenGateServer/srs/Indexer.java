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


import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;

/**
 * Indexers are plugins to GoldenGATE SRS that build indices for specific parts
 * of the documents (e.g. specific annotations) and allow for searching
 * documents using this specialized index.
 * 
 * In addition, they make the index accessible as a thesaurus, showing its
 * content independent of the documents in a tabular format and without
 * duplicates.
 * 
 * Indexers may also post process result documents before they are returned to
 * the issuer of the query. The usual purpose of this post processing is to
 * create links that allow for submitting specific parts of the document as a
 * query for further searching.
 * 
 * @author sautter
 * 
 */
public interface Indexer extends GoldenGateSrsPlugin {
	
	/**
	 * annotation type for marking a semantically important detail in the
	 * markEssentialDetails() method
	 */
	public static final String DETAIL_ANNOTATION_TYPE = "DETAIL";

	/**
	 * attribute name for specifying the type of a semantically important detail
	 * in the markEssentialDetails() method
	 */
	public static final String DETAIL_TYPE_ATTRIBUTE = "DETAIL_TYPE";
//
//	/** standard definition object for the column to store the document number in */
//	public static final TableColumnDefinition DOC_NUMBER_COLUMN = new TableColumnDefinition(DOC_NUMBER_COLUMN_NAME, TableDefinition.INT_DATATYPE, 0);

	/** standard definition object for the column to store the document number in */
	public static final TableColumnDefinition DOC_NUMBER_COLUMN = new TableColumnDefinition(DOC_NUMBER_COLUMN_NAME, TableDefinition.BIGINT_DATATYPE, 0);
	
	/**
	 * Perform a search in the index for the specified Query, and add a partial
	 * result to it.
	 * @param query the Query to process
	 * @return the result of the specified query for this Indexer, or null if
	 *         the query did not specify any parameters understood by this
	 *         indexer Note: if this method writes class or instance fields, it
	 *         should be synchronized
	 */
	public abstract QueryResult processQuery(Query query);

	/**
	 * Create links for submitting contents of the specified document as search
	 * queries directly
	 * @param doc the document to process Note: if this method writes class or
	 *            instance fields, it should be synchronized
	 */
	public abstract void markSearchables(MutableAnnotation doc);

	/**
	 * Mark essential parts of the document with DETAIL_ANNOTATION_TYPE
	 * annotations
	 * @param doc the document to process Note: if this method writes class or
	 *            instance fields, it should be synchronized
	 */
	public abstract void markEssentialDetails(MutableAnnotation doc);

	/**
	 * Add the search link attributes to an Annotation, allowing for for
	 * submitting search queries directly to this indexer
	 * @param annotation the Annotation to process to process Note: if this
	 *            method writes class or instance fields, it should be
	 *            synchronized
	 */
	public abstract void addSearchAttributes(Annotation annotation);

	/**
	 * Add the search link attributes to an array of Annotations, allowing for
	 * for submitting search queries directly to this indexer
	 * @param annotations the Annotations to process to process Note: if this
	 *            method writes class or instance fields, it should be
	 *            synchronized
	 */
	public abstract void addSearchAttributes(Annotation[] annotations);
//
//	/**
//	 * Obtain the index table entries for a set of document numbers that match
//	 * the specified query. The returned index result should return its elements
//	 * in their natural sort order, i.e., in the order described by the
//	 * Comparator the result returns from its getSortOrder() method. This is to
//	 * facilitate processing index results in a streaming fashion in SRS, and
//	 * because indexers naturally know best how to order their index elements.
//	 * @param query the query to process
//	 * @param docNumbers the document numbers the index entries for which to
//	 *            include in the result
//	 * @param sort sort the returned index entries in their natural ordering
//	 *            (the way they should most likely be displayed)?
//	 * @return the result of the lookup Note: if this method writes class or
//	 *         instance fields, it should be synchronized
//	 */
//	public abstract IndexResult getIndexEntries(Query query, int[] docNumbers, boolean sort);

	/**
	 * Obtain the index table entries for a set of document numbers that match
	 * the specified query. The returned index result should return its elements
	 * in their natural sort order, i.e., in the order described by the
	 * Comparator the result returns from its getSortOrder() method. This is to
	 * facilitate processing index results in a streaming fashion in SRS, and
	 * because indexers naturally know best how to order their index elements.
	 * @param query the query to process
	 * @param docNumbers the document numbers the index entries for which to
	 *            include in the result
	 * @param sort sort the returned index entries in their natural ordering
	 *            (the way they should most likely be displayed)?
	 * @return the result of the lookup Note: if this method writes class or
	 *         instance fields, it should be synchronized
	 */
	public abstract IndexResult getIndexEntries(Query query, long[] docNumbers, boolean sort);

	/**
	 * Do a thesaurus lookup in the index table
	 * @param query the query to process
	 * @return the result of the lookup, in particular a StringRelation
	 *         additionally providing the result fields Note: if this method
	 *         writes class or instance fields, it should be synchronized
	 */
	public abstract ThesaurusResult doThesaurusLookup(Query query);
//
//	/**
//	 * Produce the index entries for a document
//	 * @param doc the document to produce the index entries for
//	 * @param docNr the number of the specified document (to use as foreign key)
//	 *            Note: if this method writes class or instance fields, it
//	 *            should be synchronized
//	 */
//	public abstract void index(QueriableAnnotation doc, int docNr);

	/**
	 * Produce the index entries for a document
	 * @param doc the document to produce the index entries for
	 * @param docNr the number of the specified document (to use as foreign key)
	 *            Note: if this method writes class or instance fields, it
	 *            should be synchronized
	 */
	public abstract void index(QueriableAnnotation doc, long docNr);
//
//	/**
//	 * Delete a document from the index
//	 * @param docNr the number of the document to delete
//	 */
//	public abstract void deleteDocument(int docNr);

	/**
	 * Delete a document from the index
	 * @param docNr the number of the document to delete
	 */
	public abstract void deleteDocument(long docNr);

	/**
	 * Retrieve the name of the index provided by this Indexer. The String
	 * returned by this method must not be null, and it has to be the same as
	 * the one specified as the name of the search field group returned by the
	 * getSearchFieldGroup() method.
	 * @return an arbitrary, but never changing String uniquely identifying this
	 *         Indexer's index, preferably a valid word
	 */
	public abstract String getIndexName();

	/**
	 * @return a search field group describing the search fields for this index,
	 *         plus their alignment in rows
	 */
	public abstract SearchFieldGroup getSearchFieldGroup();
}