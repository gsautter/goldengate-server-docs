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
package de.uka.ipd.idaho.goldenGateServer.srs.indexers;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.goldenGateServer.srs.AbstractIndexer;
import de.uka.ipd.idaho.goldenGateServer.srs.Query;
import de.uka.ipd.idaho.goldenGateServer.srs.QueryResult;
import de.uka.ipd.idaho.goldenGateServer.srs.QueryResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefConstants;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefTypeSystem;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefUtils;
import de.uka.ipd.idaho.plugins.bibRefs.BibRefUtils.RefData;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * Indexer for MODS document meta data.
 * 
 * @author sautter
 */
public class BibliographicIndexer extends AbstractIndexer implements BibRefConstants {
	
	private static final String EXT_ID_ATTRIBUTE = "extId";
	private static final String EXT_ID_TYPE_ATTRIBUTE = "extIdType";
	
	private static final String BIB_INDEX_TABLE_NAME = "BibMetaDataIndex";
	private static final String BIB_REF_INDEX_TABLE_NAME = "BibRefIndex";
	private static final String BIB_ID_INDEX_TABLE_NAME = "BibIdentifierIndex";
	
	private static final GPath authorPath = new GPath("//mods:name[.//mods:roleTerm = 'Author']/mods:namePart");
	
	private static final int EXT_ID_LENGTH = 128;
	private static final int EXT_ID_TYPE_LENGTH = 32;
	private static final int TITLE_LENGTH = 256;
	private static final int AUTHOR_LENGTH = 128;
	private static final int ORIGIN_LENGTH = 128;
	private static final int DATE_LENGTH = 16;
	private static final int BIB_REF_LENGTH = 512;
	
	private static final String BIB_INDEX_LABEL = "Document Meta Data Index";
	private static final String BIB_INDEX_FIELDS = EXT_ID_ATTRIBUTE + " " + DOCUMENT_AUTHOR_ATTRIBUTE + " " + DOCUMENT_TITLE_ATTRIBUTE + " " + PAGE_NUMBER_ATTRIBUTE + " " + DOCUMENT_DATE_ATTRIBUTE;
	
	private BibRefTypeSystem refTypeSystem;
	private String idTypeString = "";
	
	private IoProvider io;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#getIndexName()
	 */
	public String getIndexName() {
		return "BibMetaData";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractGoldenGateSrsPlugin#init()
	 */
	public void init() {
		
		//	get and check database connection
		this.io = this.host.getIoProvider();
		if (!this.io.isJdbcAvailable())
			throw new RuntimeException("MODS Document Indexer cannot work without database access.");
		
		//	assemble index definition
		TableDefinition dtd = new TableDefinition(BIB_INDEX_TABLE_NAME);
		dtd.addColumn(DOC_NUMBER_COLUMN);
		dtd.addColumn(EXT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, EXT_ID_LENGTH);
		dtd.addColumn(EXT_ID_TYPE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, EXT_ID_TYPE_LENGTH);
		dtd.addColumn(DOCUMENT_AUTHOR_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, AUTHOR_LENGTH);
		dtd.addColumn(DOCUMENT_AUTHOR_ATTRIBUTE + "Search", TableDefinition.VARCHAR_DATATYPE, AUTHOR_LENGTH);
		dtd.addColumn(DOCUMENT_DATE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DATE_LENGTH);
		dtd.addColumn(DOCUMENT_DATE_ATTRIBUTE + "Search", TableDefinition.VARCHAR_DATATYPE, DATE_LENGTH);
		dtd.addColumn(DOCUMENT_TITLE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, TITLE_LENGTH);
		dtd.addColumn(DOCUMENT_TITLE_ATTRIBUTE + "Search", TableDefinition.VARCHAR_DATATYPE, TITLE_LENGTH);
		dtd.addColumn(DOCUMENT_ORIGIN_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, ORIGIN_LENGTH);
		dtd.addColumn(DOCUMENT_ORIGIN_ATTRIBUTE + "Search", TableDefinition.VARCHAR_DATATYPE, ORIGIN_LENGTH);
		dtd.addColumn(PAGE_NUMBER_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		dtd.addColumn(LAST_PAGE_NUMBER_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		if (!this.io.ensureTable(dtd, true))
			throw new RuntimeException("Bibliographic Indexer cannot work without database access.");
		
		TableDefinition rtd = new TableDefinition(BIB_REF_INDEX_TABLE_NAME);
		rtd.addColumn(DOC_NUMBER_COLUMN);
		rtd.addColumn(BIBLIOGRAPHIC_REFERENCE_TYPE, TableDefinition.VARCHAR_DATATYPE, BIB_REF_LENGTH);
		if (!this.io.ensureTable(rtd, true))
			throw new RuntimeException("Bibliographic Indexer cannot work without database access.");
		
		TableDefinition idtd = new TableDefinition(BIB_ID_INDEX_TABLE_NAME);
		idtd.addColumn(DOC_NUMBER_COLUMN);
		idtd.addColumn(EXT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, EXT_ID_LENGTH);
		idtd.addColumn(EXT_ID_TYPE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, EXT_ID_TYPE_LENGTH);
		if (!this.io.ensureTable(idtd, true))
			throw new RuntimeException("Bibliographic Indexer cannot work without database access.");
		
		//	create database indexes
		this.io.indexColumn(BIB_INDEX_TABLE_NAME, DOC_NUMBER_COLUMN_NAME);
		this.io.indexColumn(BIB_INDEX_TABLE_NAME, EXT_ID_ATTRIBUTE);
		this.io.indexColumn(BIB_INDEX_TABLE_NAME, (DOCUMENT_DATE_ATTRIBUTE + "Search"));
		this.io.indexColumn(BIB_INDEX_TABLE_NAME, PAGE_NUMBER_ATTRIBUTE);
		this.io.indexColumn(BIB_INDEX_TABLE_NAME, LAST_PAGE_NUMBER_ATTRIBUTE);
		
		this.io.indexColumn(BIB_REF_INDEX_TABLE_NAME, DOC_NUMBER_COLUMN_NAME);
		
		this.io.indexColumn(BIB_ID_INDEX_TABLE_NAME, DOC_NUMBER_COLUMN_NAME);
		this.io.indexColumn(BIB_ID_INDEX_TABLE_NAME, EXT_ID_ATTRIBUTE);
		//	no use indexing author, title, and origin, queried with leading wildcards right now
		//	TODO modify table so indexes can be used
		
		//	load reference type system
		File refTypeSystemPath = new File(this.dataPath, "RefTypeSystem.xml");
		this.refTypeSystem = ((refTypeSystemPath.exists() && refTypeSystemPath.isFile()) ? BibRefTypeSystem.getInstance(refTypeSystemPath, true) : BibRefTypeSystem.getDefaultInstance());
		
		//	load external identifier types
		File idTypePath = new File(this.dataPath, "IdentifierTypes.cnfg");
		if (idTypePath.exists() && idTypePath.isFile()) try {
			StringVector idTypes = StringVector.loadList(idTypePath);
			for (int i = 0; i < idTypes.size(); i++) {
				String idType = idTypes.get(i);
				if ((idType.length() == 0) || idType.startsWith("//") || (idType.indexOf(' ') != -1))
					idTypes.remove(i--);
			}
			this.idTypeString = idTypes.concatStrings(" ").toLowerCase();
		} catch (IOException ioe) {}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractGoldenGateSrsPlugin#exit()
	 */
	public void exit() {
		//	disconnect from database
		this.io.close();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#processQuery(de.uka.ipd.idaho.goldenGateServer.srs.Query)
	 */
	public QueryResult processQuery(Query query) {
		
		//	get data
		String id = query.getValue(EXT_ID_ATTRIBUTE, "").trim();
		StringVector ids = new StringVector();
		if (id.indexOf(',') == -1) {
			if (id.length() > EXT_ID_LENGTH)
				id = id.substring(0, EXT_ID_LENGTH);
		}
		else {
			ids.parseAndAddElements(id, ",");
			for (int i = 0; i < ids.size(); i++) {
				id = ids.get(i).trim();
				if (id.length() > EXT_ID_LENGTH)
					id = id.substring(0, EXT_ID_LENGTH);
				ids.setElementAt(EasyIO.sqlEscape(id), i);
			}
			ids.removeDuplicateElements();
		}
		
		String author = prepareSearchString(query.getValue(DOCUMENT_AUTHOR_ATTRIBUTE, "").trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		String date = query.getValue(DOCUMENT_DATE_ATTRIBUTE, "").trim().toLowerCase().replaceAll("\\p{Punct}", "");
		String title = prepareSearchString(query.getValue(DOCUMENT_TITLE_ATTRIBUTE, "").trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		String origin = prepareSearchString(query.getValue(DOCUMENT_ORIGIN_ATTRIBUTE, "").trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		String partDes = query.getValue(PART_DESIGNATOR_ANNOTATION_TYPE, "").trim().toLowerCase().replaceAll("\\p{Punct}", "");
		if (partDes.length() != 0) {
			if (origin.length() == 0)
				origin = ("" + partDes);
			else origin = (origin + "%" + partDes);
		}
		String page = query.getValue(PAGE_NUMBER_ATTRIBUTE, "").trim();
		
		String ref = prepareSearchString(query.getValue(BIBLIOGRAPHIC_REFERENCE_TYPE, "").trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		
		//	trim data
		if (author.length() > AUTHOR_LENGTH)
			author = author.substring(0, AUTHOR_LENGTH);
		if (date.length() > DATE_LENGTH)
			date = date.substring(0, DATE_LENGTH);
		if (title.length() > TITLE_LENGTH)
			title = title.substring(0, TITLE_LENGTH);
		if (origin.length() > ORIGIN_LENGTH)
			origin = origin.substring(0, ORIGIN_LENGTH);
		if (ref.length() > BIB_REF_LENGTH)
			ref = ref.substring(0, BIB_REF_LENGTH);
		
		//	check if query given
		if ((id + title + page + author + date + origin + ref).trim().length() == 0) return null; 
		
		//	assemble search predicate
		String tableShort = "";
		String tableName = "";
		String joinWhere = "";
		String where = "1=1";
		if (id.length() != 0) {
			tableShort += "i";
			tableName = BIB_ID_INDEX_TABLE_NAME;
			joinWhere += (" AND d." + DOC_NUMBER_COLUMN_NAME + " = i." + DOC_NUMBER_COLUMN_NAME + "");
			if (ids.isEmpty())
				where += (" AND i." + EXT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(id) + "'");
			else where += (" AND i." + EXT_ID_ATTRIBUTE + " IN ('" + ids.concatStrings("', '") + "')");
		}
		if (ref.length() != 0) {
			tableShort += "r";
			tableName = BIB_REF_INDEX_TABLE_NAME;
			joinWhere += (" AND d." + DOC_NUMBER_COLUMN_NAME + " = r." + DOC_NUMBER_COLUMN_NAME + "");
			String[] refParts = ref.split("\\s+");
			for (int p = 0; p < refParts.length; p++)
				where += (" AND r." + BIBLIOGRAPHIC_REFERENCE_TYPE + " LIKE '%" + EasyIO.sqlEscape(refParts[p]) + "%'");
		}
		if ((author.length() + date.length() + title.length() + origin.length() + page.length()) != 0) {
			tableShort += "d";
			tableName = BIB_INDEX_TABLE_NAME;
			if (author.length() != 0)
				where += (" AND d." + DOCUMENT_AUTHOR_ATTRIBUTE + "Search" + " LIKE '%" + EasyIO.prepareForLIKE(author) + "%'");
			if (date.length() != 0)
				where += (" AND d." + DOCUMENT_DATE_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(date) + "'");
			if (title.length() != 0)
				where += (" AND d." + DOCUMENT_TITLE_ATTRIBUTE + "Search" + " LIKE '%" + EasyIO.prepareForLIKE(title) + "%'");
			if (origin.length() != 0)
				where += (" AND d." + DOCUMENT_ORIGIN_ATTRIBUTE + "Search" + " LIKE '%" + EasyIO.prepareForLIKE(origin) + "%'");
			if (page.length() != 0)
				where += (" AND (d." + PAGE_NUMBER_ATTRIBUTE + " <= " + EasyIO.sqlEscape(page) + " AND d." + LAST_PAGE_NUMBER_ATTRIBUTE + " >= " + EasyIO.sqlEscape(page) + ")");
		}
		
		//	assemble query
//		String queryString = ("SELECT DISTINCT d." + DOC_NUMBER_COLUMN_NAME + 
//				" FROM " + BIB_INDEX_TABLE_NAME + " d" + 
//					((ref.length() == 0) ? "" : (", " + BIB_REF_INDEX_TABLE_NAME + " r")) +
//					((id.length() == 0) ? "" : (", " + BIB_ID_INDEX_TABLE_NAME + " i")) +
//				" WHERE " + where + joinWhere +
//				";");
		String queryString;
		if (tableShort.length() == 1) {
			queryString = ("SELECT DISTINCT " + tableShort + "." + DOC_NUMBER_COLUMN_NAME + 
					" FROM " + tableName + " " + tableShort +
					" WHERE " + where +
					";");
		}
		else {
			queryString = ("SELECT DISTINCT d." + DOC_NUMBER_COLUMN_NAME + 
					" FROM " + BIB_INDEX_TABLE_NAME + " d" + 
						((ref.length() == 0) ? "" : (", " + BIB_REF_INDEX_TABLE_NAME + " r")) +
						((id.length() == 0) ? "" : (", " + BIB_ID_INDEX_TABLE_NAME + " i")) +
					" WHERE " + where + joinWhere +
					";");
		}
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(queryString, true);
			
			QueryResult result = new QueryResult();
			while (sqr.next()) {
				String docNr = sqr.getString(0);
				result.addResultElement(new QueryResultElement(Long.parseLong(docNr), 1.0));
			}
			
			return result;
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting document IDs.");
			System.out.println("  Query was " + queryString);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractIndexer#markSearchables(de.uka.ipd.idaho.gamta.MutableAnnotation)
	 */
	public void markSearchables(MutableAnnotation doc) {
		
		//	get, check and extract meta data
		MutableAnnotation[] modsHeaders = doc.getMutableAnnotations("mods:mods");
		if (modsHeaders.length != 0) try {
			modsHeaders[0].setAttribute(BOXED_ATTRIBUTE, BOXED_ATTRIBUTE);
			modsHeaders[0].setAttribute(BOX_TITLE_ATTRIBUTE, "Bibliographic Meta Data");
			modsHeaders[0].lastToken().setAttribute(Token.PARAGRAPH_END_ATTRIBUTE, Token.PARAGRAPH_END_ATTRIBUTE);
			
			Annotation[] modsIdentifiers = doc.getAnnotations("mods:identifier");
			if (modsIdentifiers.length != 0) {
				modsIdentifiers[0].setAttribute(BOX_PART_LABEL_ATTRIBUTE, "External ID");
				Annotation link = doc.addAnnotation(SEARCH_LINK_ANNOTATION_TYPE, modsIdentifiers[0].getStartIndex(), modsIdentifiers[0].size());
				link.setAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, (this.getFullFieldName(EXT_ID_ATTRIBUTE) + "=" + URLEncoder.encode(modsIdentifiers[0].getValue().replaceAll("\\\"", "''"), "UTF-8")));
				link.setAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, ("Search external ID '" + modsIdentifiers[0].getValue().replaceAll("\\\"", "''") + "'"));
			}
			
			Annotation[] modsTitles = doc.getAnnotations("mods:title");
			if (modsTitles.length != 0) {
				modsTitles[0].setAttribute(BOX_PART_LABEL_ATTRIBUTE, "Title");
				Annotation link = doc.addAnnotation(SEARCH_LINK_ANNOTATION_TYPE, modsTitles[0].getStartIndex(), modsTitles[0].size());
				link.setAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, (this.getFullFieldName(DOCUMENT_TITLE_ATTRIBUTE) + "=" + URLEncoder.encode(modsTitles[0].getValue().replaceAll("\\\"", "''"), "UTF-8")));
				link.setAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, ("Search title '" + modsTitles[0].getValue().replaceAll("\\\"", "''") + "'"));
			}
			
			Annotation[] modsAuthors = authorPath.evaluate(modsHeaders[0], null);
			for (int a = 0; a < modsAuthors.length; a++) {
				modsAuthors[a].setAttribute(BOX_PART_LABEL_ATTRIBUTE, "Author");
				Annotation link = doc.addAnnotation(SEARCH_LINK_ANNOTATION_TYPE, modsAuthors[a].getStartIndex(), modsAuthors[a].size());
				link.setAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, (this.getFullFieldName(DOCUMENT_AUTHOR_ATTRIBUTE) + "=" + URLEncoder.encode(modsAuthors[a].getValue().replaceAll("\\\"", "''"), "UTF-8")));
				link.setAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, ("Search author '" + modsAuthors[a].getValue().replaceAll("\\\"", "''") + "'"));
			}
			
			Annotation[] modsDates = doc.getAnnotations("mods:date");
			if (modsDates.length != 0) {
				modsDates[0].setAttribute(BOX_PART_LABEL_ATTRIBUTE, "Publication Date");
				Annotation link = doc.addAnnotation(SEARCH_LINK_ANNOTATION_TYPE, modsDates[0].getStartIndex(), modsDates[0].size());
				link.setAttribute(SEARCH_LINK_QUERY_ATTRIBUTE, (this.getFullFieldName(DOCUMENT_DATE_ATTRIBUTE) + "=" + URLEncoder.encode(modsDates[0].getValue().replaceAll("\\\"", "''"), "UTF-8")));
				link.setAttribute(SEARCH_LINK_TITLE_ATTRIBUTE, ("Search publication date '" + modsDates[0].getValue().replaceAll("\\\"", "''") + "'"));
			}
			
			//	make meta data indexable in result index
			doc.setAttribute(RESULT_INDEX_NAME_ATTRIBUTE, BIB_INDEX_TABLE_NAME);
			doc.setAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, BIB_INDEX_LABEL);
			doc.setAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, BIB_INDEX_FIELDS);
		} catch (Exception e) {}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#addSearchAttributes(de.uka.ipd.idaho.gamta.Annotation)
	 */
	public void addSearchAttributes(Annotation annotation) {
		
		//	add search links to index result elements
		if (CITATION_TYPE.equals(annotation.getType()) || BIBLIOGRAPHIC_REFERENCE_TYPE.equals(annotation.getType())) {
			String extId = annotation.getAttribute(EXT_ID_ATTRIBUTE, "").toString().trim();
			if (extId.length() != 0) try {
				annotation.setAttribute(SEARCH_LINK_QUERY_ATTRIBUTE + EXT_ID_ATTRIBUTE, (this.getFullFieldName(EXT_ID_ATTRIBUTE) + "=" + URLEncoder.encode(extId, "UTF-8")));
				annotation.setAttribute(SEARCH_LINK_TITLE_ATTRIBUTE + EXT_ID_ATTRIBUTE, ("Search External ID '" + extId + "'"));
			} catch (IOException ioe) {}
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#getIndexEntries(de.uka.ipd.idaho.goldenGateServer.srs.Query, long[], boolean)
	 */
	public IndexResult getIndexEntries(Query query, long[] docNumbers, boolean sort) {
		StringVector docNrs = new StringVector();
		for (int n = 0; n < docNumbers.length; n++)
			docNrs.addElementIgnoreDuplicates("" + docNumbers[n]);
		if (docNrs.isEmpty()) return null;
		
		String[] columns = {
//				BIB_ID_ATTRIBUTE, // do not add ID to result attributes, so it's given, but not displayed
				DOCUMENT_AUTHOR_ATTRIBUTE,
				DOCUMENT_DATE_ATTRIBUTE,
				DOCUMENT_TITLE_ATTRIBUTE,
				DOCUMENT_ORIGIN_ATTRIBUTE,
				PAGE_NUMBER_ATTRIBUTE,
				LAST_PAGE_NUMBER_ATTRIBUTE,
			};
		
		StringBuffer sqlQuery = new StringBuffer("SELECT DISTINCT " + DOC_NUMBER_COLUMN_NAME + ", " + EXT_ID_ATTRIBUTE + ", " + EXT_ID_TYPE_ATTRIBUTE);
		for (int c = 0; c < columns.length; c++) {
			sqlQuery.append(", ");
			sqlQuery.append(columns[c]);
		}
		sqlQuery.append(" FROM " + BIB_INDEX_TABLE_NAME); 
		sqlQuery.append(" WHERE " + DOC_NUMBER_COLUMN_NAME + " IN (" + docNrs.concatStrings(", ") + ")");
		sqlQuery.append(" ORDER BY ");
		for (int c = 0; c < columns.length; c++) {
			if (c != 0)
				sqlQuery.append(", ");
			sqlQuery.append(columns[c]);
		}
		sqlQuery.append(";");
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(sqlQuery.toString());
			return new BibMetaDataIndexResult(columns, sqr) {
				
				//	needs special handling because external ID is not in column list
				protected IndexResultElement decodeResultElement(String[] elementData) {
					IndexResultElement ire = new IndexResultElement(Long.parseLong(elementData[0]), this.entryType, ((elementData[1] == null) ? "" : elementData[1]));
					
					if (elementData.length > 1)
						ire.setAttribute(EXT_ID_ATTRIBUTE, elementData[1]);
					if (elementData.length > 2)
						ire.setAttribute(EXT_ID_TYPE_ATTRIBUTE, elementData[2]);
					for (int a = 0; a < this.resultAttributes.length; a++) {
						if ((a+3) >= elementData.length)
							break;
						if (elementData[a + 3] != null)
							ire.setAttribute(this.resultAttributes[a], elementData[a + 3]);
					}
					return ire;
				}
			};
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting index entries.");
			System.out.println("  Query was " + sqlQuery.toString());
			return null;
		}
	}
	
	private class BibMetaDataIndexResult extends SqlIndexResult {
		BibMetaDataIndexResult(String[] resultAttributes, SqlQueryResult sqr) {
			super(resultAttributes, BIBLIOGRAPHIC_REFERENCE_TYPE, "Bibliographic Metadata Index", BIBLIOGRAPHIC_REFERENCE_TYPE, sqr);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#doThesaurusLookup(de.uka.ipd.idaho.goldenGateServer.srs.Query)
	 */
	public ThesaurusResult doThesaurusLookup(Query query) {
		
		//	get data
		String id = query.getValue(EXT_ID_ATTRIBUTE, "").trim();
		StringVector ids = new StringVector();
		if (id.indexOf(',') == -1) {
			if (id.length() > EXT_ID_LENGTH)
				id = id.substring(0, EXT_ID_LENGTH);
		}
		else {
			ids.parseAndAddElements(id, ",");
			for (int i = 0; i < ids.size(); i++) {
				id = ids.get(i).trim();
				if (id.length() > EXT_ID_LENGTH)
					id = id.substring(0, EXT_ID_LENGTH);
				ids.setElementAt(EasyIO.sqlEscape(id), i);
			}
			ids.removeDuplicateElements();
		}
		
		String author = prepareSearchString(query.getValue(DOCUMENT_AUTHOR_ATTRIBUTE, "").trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		String date = query.getValue(DOCUMENT_DATE_ATTRIBUTE, "").trim().toLowerCase().replaceAll("\\p{Punct}", "");
		String title = prepareSearchString(query.getValue(DOCUMENT_TITLE_ATTRIBUTE, "").trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		String origin = prepareSearchString(query.getValue(DOCUMENT_ORIGIN_ATTRIBUTE, "").trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		String partDes = query.getValue(PART_DESIGNATOR_ANNOTATION_TYPE, "").trim().toLowerCase().replaceAll("\\p{Punct}", "");
		if (partDes.length() != 0) {
			if (origin.length() == 0)
				origin = ("" + partDes);
			else origin = (origin + "%" + partDes);
		}
		String page = query.getValue(PAGE_NUMBER_ATTRIBUTE, "").trim();
		
		String ref = prepareSearchString(query.getValue(BIBLIOGRAPHIC_REFERENCE_TYPE, "").trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		
		//	trim data
		if (id.length() > EXT_ID_LENGTH)
			id = id.substring(0, EXT_ID_LENGTH);
		if (author.length() > AUTHOR_LENGTH)
			author = author.substring(0, AUTHOR_LENGTH);
		if (date.length() > DATE_LENGTH)
			date = date.substring(0, DATE_LENGTH);
		if (title.length() > TITLE_LENGTH)
			title = title.substring(0, TITLE_LENGTH);
		if (origin.length() > ORIGIN_LENGTH)
			origin = origin.substring(0, ORIGIN_LENGTH);
		if (ref.length() > BIB_REF_LENGTH)
			ref = ref.substring(0, BIB_REF_LENGTH);
		
		//	check if query given
		if ((id + title + page + author + date + origin + ref).trim().length() == 0) return null; 
		
		//	assemble predicates
		String where = "1=1";
		if (id.length() != 0) {
			where += (" AND d." + DOC_NUMBER_COLUMN_NAME + " = i." + DOC_NUMBER_COLUMN_NAME + "");
			if (ids.isEmpty())
				where += (" AND i." + EXT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(id) + "'");
			else where += (" AND i." + EXT_ID_ATTRIBUTE + " IN ('" + ids.concatStrings("', '") + "')");
		}
		if (ref.length() != 0) {
			where += (" AND d." + DOC_NUMBER_COLUMN_NAME + " = r." + DOC_NUMBER_COLUMN_NAME + "");
			String[] refParts = ref.split("\\s+");
			for (int p = 0; p < refParts.length; p++)
				where += (" AND r." + BIBLIOGRAPHIC_REFERENCE_TYPE + " LIKE '%" + EasyIO.sqlEscape(refParts[p]) + "%'");
		}
		if (author.length() != 0)
			where += (" AND d." + DOCUMENT_AUTHOR_ATTRIBUTE + "Search" + " LIKE '%" + EasyIO.prepareForLIKE(author) + "%'");
		if (date.length() != 0)
			where += (" AND d." + DOCUMENT_DATE_ATTRIBUTE + "Search" + " LIKE '" + EasyIO.sqlEscape(date) + "'");
		if (title.length() != 0)
			where += (" AND d." + DOCUMENT_TITLE_ATTRIBUTE + "Search" + " LIKE '%" + EasyIO.prepareForLIKE(title) + "%'");
		if (origin.length() != 0)
			where += (" AND d." + DOCUMENT_ORIGIN_ATTRIBUTE + "Search" + " LIKE '%" + EasyIO.prepareForLIKE(origin) + "%'");
		if (page.length() != 0)
			where += (" AND (d." + PAGE_NUMBER_ATTRIBUTE + " <= " + EasyIO.sqlEscape(page) + " AND d." + LAST_PAGE_NUMBER_ATTRIBUTE + " >= " + EasyIO.sqlEscape(page) + ")");
		
		//	check if query given
		if (where.length() < 4) return null;
		
		String[] columns = {
				EXT_ID_ATTRIBUTE,
				EXT_ID_TYPE_ATTRIBUTE,
				DOCUMENT_AUTHOR_ATTRIBUTE,
				DOCUMENT_DATE_ATTRIBUTE,
				DOCUMENT_TITLE_ATTRIBUTE,
				DOCUMENT_ORIGIN_ATTRIBUTE,
				PAGE_NUMBER_ATTRIBUTE,
				LAST_PAGE_NUMBER_ATTRIBUTE
			};
		
		StringBuffer sqlQuery = new StringBuffer("SELECT DISTINCT ");
		for (int c = 0; c < columns.length; c++) {
			if (c != 0) sqlQuery.append(", ");
			sqlQuery.append("d." + columns[c]);
		}
		sqlQuery.append(" FROM " + BIB_INDEX_TABLE_NAME + " d" + ((ref.length() == 0) ? "" : (", " + BIB_REF_INDEX_TABLE_NAME + " r")) + ((id.length() == 0) ? "" : (", " + BIB_ID_INDEX_TABLE_NAME + " i"))); 
		sqlQuery.append(" WHERE " + where + ";");
		
		try {
			return new BibMetaDataThesaurusResult(columns, this.io.executeSelectQuery(sqlQuery.toString()));
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while doing thesaurus lookup.");
			System.out.println("  Query was " + sqlQuery.toString());
			return null;
		}
	}
	
	private class BibMetaDataThesaurusResult extends SqlThesaurusResult {
		BibMetaDataThesaurusResult(String[] resultFieldNames, SqlQueryResult sqr) {
			super(resultFieldNames, BIBLIOGRAPHIC_REFERENCE_TYPE, "Bibliographic Metadata Index", sqr);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#index(de.uka.ipd.idaho.gamta.QueriableAnnotation, long)
	 */
	public void index(QueriableAnnotation doc, long docNr) {
		
		//	what do we want to index?
		String extId;
		String extIdType;
		TreeMap extIdsByType = new TreeMap(String.CASE_INSENSITIVE_ORDER);
		String author;
		String date;
		String title;
		String origin;
		String page;
		String lastPage;
		
		//	get data
		QueriableAnnotation[] modsHeader = doc.getAnnotations("mods:mods");
		RefData ref;
		if (modsHeader.length == 0) {
			ref = BibRefUtils.modsAttributesToRefData(doc);
			if (!ref.hasAttribute(AUTHOR_ANNOTATION_TYPE) || !ref.hasAttribute(YEAR_ANNOTATION_TYPE) || !ref.hasAttribute(TITLE_ANNOTATION_TYPE))
				ref = null;
		}
		else {
			ref = BibRefUtils.modsXmlToRefData(modsHeader[0]);
			if (this.refTypeSystem.classify(ref) == null)
				ref = null;
		}
		
		//	try and get details from attributes if MODS header not given
		if (ref == null) {
			extId = doc.getAttribute(EXT_ID_ATTRIBUTE, "").toString().trim();
			extIdType = doc.getAttribute(EXT_ID_TYPE_ATTRIBUTE, "").toString().trim();
			if ((extId.length() != 0) || (extIdType.length() != 0))
				extIdsByType.put(extIdType, extId);
			author = doc.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, "").toString().trim();
			date = doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "").toString().trim();
			title = doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, "").toString().trim();
			origin = doc.getAttribute(DOCUMENT_ORIGIN_ATTRIBUTE, "").toString().trim();
			page = doc.getAttribute(PAGE_NUMBER_ATTRIBUTE, "0").toString().trim();
			lastPage = doc.getAttribute(LAST_PAGE_NUMBER_ATTRIBUTE, "0").toString().trim();
		}
		
		//	get details from MODS header or attributes
		else {
			extId = "";
			extIdType = "";
			String[] idTypes = ref.getIdentifierTypes();
			int idPriority = Integer.MAX_VALUE;
			for (int i = 0; i < idTypes.length; i++) {
				extIdsByType.put(idTypes[i], ref.getIdentifier(idTypes[i]));
				int idp = this.idTypeString.indexOf(idTypes[i].toLowerCase());
				if (idp == -1)
					continue;
				if (idp < idPriority) {
					extId = ref.getIdentifier(idTypes[i]);
					extIdType = idTypes[i];
					idPriority = idp;
				}
			}
			author = ref.getAttributeValueString(AUTHOR_ANNOTATION_TYPE, ", ", " & ");
			if (author == null)
				author = "";
			date = ref.getAttribute(YEAR_ANNOTATION_TYPE);
			if (date == null)
				date = "";
			title = ref.getAttribute(TITLE_ANNOTATION_TYPE);
			if (title == null)
				title = "";
			origin = this.refTypeSystem.getOrigin(ref);
			if (origin == null)
				origin = "";
			String pagination = ref.getAttribute(PAGINATION_ANNOTATION_TYPE);
			if (pagination == null) {
				page = ("" + Integer.MIN_VALUE);
				lastPage = ("" + Integer.MIN_VALUE);
			}
			else {
				String[] pages = pagination.split("\\s*[\\-\\u2010-\\u2015\\u2212]+\\s*");
				page = pages[0];
				lastPage = ((pages.length == 1) ? pages[0] : pages[1]);
			}
		}
		
		//	trim data
		if (extId.length() > EXT_ID_LENGTH)
			extId = extId.substring(0, EXT_ID_LENGTH);
		if (extIdType.length() > EXT_ID_TYPE_LENGTH)
			extIdType = extIdType.substring(0, EXT_ID_TYPE_LENGTH);
		if (author.length() > AUTHOR_LENGTH)
			author = author.substring(0, AUTHOR_LENGTH);
		if (date.length() > DATE_LENGTH)
			date = date.substring(0, DATE_LENGTH);
		if (title.length() > TITLE_LENGTH)
			title = title.substring(0, TITLE_LENGTH);
		if (origin.length() > ORIGIN_LENGTH)
			origin = origin.substring(0, ORIGIN_LENGTH);
		
		//	start column strings
		StringBuffer columns = new StringBuffer(DOC_NUMBER_COLUMN_NAME);
		StringBuffer values = new StringBuffer("" + docNr);
		
		//	add attributes
		columns.append(", " + EXT_ID_ATTRIBUTE);
		values.append(", '" + EasyIO.sqlEscape(extId) + "'");
		columns.append(", " + EXT_ID_TYPE_ATTRIBUTE);
		values.append(", '" + EasyIO.sqlEscape(extIdType) + "'");
		
		columns.append(", " + DOCUMENT_AUTHOR_ATTRIBUTE);
		values.append(", '" + EasyIO.sqlEscape(author) + "'");
		
		columns.append(", " + DOCUMENT_AUTHOR_ATTRIBUTE + "Search");
		String sAuthor = prepareSearchString(author.trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		if (sAuthor.length() > AUTHOR_LENGTH)
			sAuthor = sAuthor.substring(0, AUTHOR_LENGTH);
		values.append(", '" + EasyIO.sqlEscape(sAuthor) + "'");
		
		columns.append(", " + DOCUMENT_DATE_ATTRIBUTE);
		values.append(", '" + EasyIO.sqlEscape(date) + "'");
		
		columns.append(", " + DOCUMENT_DATE_ATTRIBUTE + "Search");
		String sDate = prepareSearchString(date.trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		if (sDate.length() > DATE_LENGTH)
			sDate = sDate.substring(0, DATE_LENGTH);
		values.append(", '" + EasyIO.sqlEscape(sDate) + "'");
		
		columns.append(", " + DOCUMENT_TITLE_ATTRIBUTE);
		values.append(", '" + EasyIO.sqlEscape(title) + "'");
		
		columns.append(", " + DOCUMENT_TITLE_ATTRIBUTE + "Search");
		String sTitle = prepareSearchString(title.trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		if (sTitle.length() > TITLE_LENGTH)
			sTitle = sTitle.substring(0, TITLE_LENGTH);
		values.append(", '" + EasyIO.sqlEscape(sTitle) + "'");
		
		columns.append(", " + DOCUMENT_ORIGIN_ATTRIBUTE);
		values.append(", '" + EasyIO.sqlEscape(origin) + "'");
		
		columns.append(", " + DOCUMENT_ORIGIN_ATTRIBUTE + "Search");
		String sOrigin = prepareSearchString(origin.trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		if (sOrigin.length() > ORIGIN_LENGTH)
			sOrigin = sOrigin.substring(0, ORIGIN_LENGTH);
		values.append(", '" + EasyIO.sqlEscape(sOrigin) + "'");
		
		columns.append(", " + PAGE_NUMBER_ATTRIBUTE);
		values.append(", " + EasyIO.sqlEscape(page) + "");
		
		columns.append(", " + LAST_PAGE_NUMBER_ATTRIBUTE);
		values.append(", " + EasyIO.sqlEscape(lastPage) + "");
		
		//	write index table entry
		String query = ("INSERT INTO " + BIB_INDEX_TABLE_NAME + " (" + columns + ") VALUES (" + values + ");");
		try {
			this.io.executeUpdateQuery(query);
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while indexing document.");
			System.out.println("  Query was " + query);
		}
		
		//	index full reference string
		String sRef;
		if (ref == null)
			sRef = prepareSearchString((author + " (" + date + ") " + title + ". " + origin + ": " + page + "-" + lastPage + " " + extId).trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		else sRef = prepareSearchString(BibRefUtils.toRefString(ref).trim().toLowerCase().replaceAll("\\p{Punct}", ""));
		this.indexRef(sRef, docNr);
		
		//	index external IDs
		for (Iterator idtit = extIdsByType.keySet().iterator(); idtit.hasNext();) {
			extIdType = ((String) idtit.next());
			this.indexExtId(((String) extIdsByType.get(extIdType)), extIdType, docNr);
		}
	}
	
	private void indexRef(String ref, long docNr) {
		if (ref.length() > BIB_REF_LENGTH)
			ref = ref.substring(0, BIB_REF_LENGTH);
		
		//	start column strings
		StringBuffer columns = new StringBuffer(DOC_NUMBER_COLUMN_NAME);
		StringBuffer values = new StringBuffer("" + docNr);
		
		//	add attributes
		columns.append(", " + BIBLIOGRAPHIC_REFERENCE_TYPE);
		values.append(", '" + EasyIO.sqlEscape(ref) + "'");
		
		//	write index table entry
		String query = ("INSERT INTO " + BIB_REF_INDEX_TABLE_NAME + " (" + columns + ") VALUES (" + values + ");");
		try {
			this.io.executeUpdateQuery(query);
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while indexing document.");
			System.out.println("  Query was " + query);
		}
	}
	
	private void indexExtId(String extId, String extIdType, long docNr) {
		if (extId.length() > EXT_ID_LENGTH)
			extId = extId.substring(0, EXT_ID_LENGTH);
		if (extIdType.length() > EXT_ID_TYPE_LENGTH)
			extIdType = extIdType.substring(0, EXT_ID_TYPE_LENGTH);
		
		//	start column strings
		StringBuffer columns = new StringBuffer(DOC_NUMBER_COLUMN_NAME);
		StringBuffer values = new StringBuffer("" + docNr);
		
		//	add attributes
		columns.append(", " + EXT_ID_ATTRIBUTE);
		values.append(", '" + EasyIO.sqlEscape(extId) + "'");
		columns.append(", " + EXT_ID_TYPE_ATTRIBUTE);
		values.append(", '" + EasyIO.sqlEscape(extIdType) + "'");
		
		//	write index table entry
		String query = ("INSERT INTO " + BIB_ID_INDEX_TABLE_NAME + " (" + columns + ") VALUES (" + values + ");");
		try {
			this.io.executeUpdateQuery(query);
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while indexing document.");
			System.out.println("  Query was " + query);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#deleteDocument(long)
	 */
	public void deleteDocument(long docNr) {
		String dataQuery = ("DELETE FROM " + BIB_INDEX_TABLE_NAME + " WHERE " + DOC_NUMBER_COLUMN_NAME + "=" + docNr + ";");
		try {
			this.io.executeUpdateQuery(dataQuery);
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			System.out.println("  Query was " + dataQuery);
		}
		
		String refQuery = ("DELETE FROM " + BIB_REF_INDEX_TABLE_NAME + " WHERE " + DOC_NUMBER_COLUMN_NAME + "=" + docNr + ";");
		try {
			this.io.executeUpdateQuery(refQuery);
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			System.out.println("  Query was " + refQuery);
		}
		
		String idQuery = ("DELETE FROM " + BIB_ID_INDEX_TABLE_NAME + " WHERE " + DOC_NUMBER_COLUMN_NAME + "=" + docNr + ";");
		try {
			this.io.executeUpdateQuery(idQuery);
		}
		catch (SQLException sqle) {
			System.out.println("BibliographicMetaDataIndexer: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			System.out.println("  Query was " + idQuery);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractIndexer#getFieldGroup()
	 */
	protected SearchFieldGroup getFieldGroup() {
		
		SearchFieldRow mainRow = new SearchFieldRow();
		mainRow.addField(new SearchField(BIBLIOGRAPHIC_REFERENCE_TYPE, "Reference", "Full Reference Text", 2));
		mainRow.addField(new SearchField(DOCUMENT_AUTHOR_ATTRIBUTE, "Author", "Document Author"));
		mainRow.addField(new SearchField(DOCUMENT_DATE_ATTRIBUTE, "Year", "Year of Publication"));
		mainRow.addField(new SearchField(DOCUMENT_TITLE_ATTRIBUTE, "Title", "Document Title"));
		
		SearchFieldRow detailRow = new SearchFieldRow();
		detailRow.addField(new SearchField(DOCUMENT_ORIGIN_ATTRIBUTE, "Journal / Publisher", "Name of Journal for Articles, Name of Publisher for Books and Book Chapters", 2));
		detailRow.addField(new SearchField(PART_DESIGNATOR_ANNOTATION_TYPE, "Volume / Issue", "Volume or Issue Number of Journals"));
		detailRow.addField(new SearchField(PAGE_NUMBER_ATTRIBUTE, "Page", "Page Number"));
		detailRow.addField(new SearchField(EXT_ID_ATTRIBUTE, "Identifier", "External Identifier, e.g. DOI, Handle, or ISBN"));
		
		SearchFieldGroup sfg = new SearchFieldGroup(this.getIndexName(), "Bibliographic Metadata Index", "Use these fields to search the bibliographic meta data index.", "Bibliographic Reference");
		sfg.addFieldRow(mainRow);
		sfg.addFieldRow(detailRow);
		
		return sfg;
	}
}