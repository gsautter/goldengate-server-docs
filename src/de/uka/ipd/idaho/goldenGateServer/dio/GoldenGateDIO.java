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
package de.uka.ipd.idaho.goldenGateServer.dio;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest.AttributeFilter;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest.TypeFilter;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.gamta.util.SgmlDocumentReader;
import de.uka.ipd.idaho.gamta.util.gPath.GPath;
import de.uka.ipd.idaho.gamta.util.gPath.exceptions.GPathSyntaxException;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentList.AttributeSummary;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerEventService;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent.DioDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DioDocumentList;
import de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore;
import de.uka.ipd.idaho.goldenGateServer.uaa.UserAccessAuthority;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNode;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.IoTools;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.TreeTools;
import de.uka.ipd.idaho.stringUtils.Dictionary;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.regExUtils.RegExUtils;

/**
 * Server component for storing and retrieving documents by ID over the network,
 * basically the "active part" or "front end" of DocumentStore, named GoldenGATE
 * Document IO Server (DIO). This component also locks documents when checked
 * out by a user for editing, which is the prerequisite for being able to update
 * any existing document.
 * 
 * @author sautter
 */
public class GoldenGateDIO extends AbstractGoldenGateServerComponent implements GoldenGateDioConstants {
	private static final String ANNOTATION_NESTING_ORDER_SETTING = "annotationNestingOrder";
	
	private UserAccessAuthority uaa = null;
	
	private AnnotationChecksumDigest checksumDigest;
	
	private DocumentStore dst;
	
	private IoProvider io;
	
	/*
	 * The name of the DIO's document data table in the backing database. This
	 * constant is public in order to allow other components to perform joins
	 * with the DIO's data. It is NOT intended for external modification.
	 */
	private static final String DOCUMENT_TABLE_NAME = "GgDioDocuments";
	
	private static final String DOCUMENT_ID_HASH_NAME = "docIdHash";
	private static final String EXTERNAL_IDENTIFIER_NAME = "externalIdentifierName";
	private static final int EXTERNAL_IDENTIFIER_NAME_LENGTH = 32;
	private static final String EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME = "extIdConfigHash";
	private static final int EXTERNAL_IDENTIFIER_LENGTH = 255;
	private static final String DOCUMENT_CHECKSUM_COLUMN_NAME = "DocCheckSum";
	
	private static final int DOCUMENT_NAME_COLUMN_LENGTH = 255;
	private static final int DOCUMENT_AUTHOR_COLUMN_LENGTH = 255;
	private static final int DOCUMENT_TITLE_COLUMN_LENGTH = 511;
	private static final int DOCUMENT_KEYWORDS_COLUMN_LENGTH = 1023;
	
	private String extIdAttributeNameList = "";
	private String[] extIdAttributeNames = {};
	
	private int documentListSizeThreshold = 0;
	
	private Set docIdSet = new HashSet();
	private Map docAttributeValueCache = new HashMap();
	
	private void cacheDocumentAttributeValue(String fieldName, String fieldValue) {
		if ((fieldValue == null) || summarylessAttributes.contains(fieldValue))
			return;
		AttributeSummary as = this.getListFieldSummary(fieldName, true);
		as.add(fieldValue);
	}
	private void cacheDocumentAttributeValues(Attributed values) {
		for (int f = 0; f < documentListFields.length; f++)
			this.cacheDocumentAttributeValue(documentListFields[f], ((String) values.getAttribute(documentListFields[f])));
		for (int f = 0; f < documentListFieldsAdmin.length; f++)
			this.cacheDocumentAttributeValue(documentListFieldsAdmin[f], ((String) values.getAttribute(documentListFields[f])));
	}
	private void uncacheDocumentAttributeValue(String fieldName, String fieldValue) {
		if ((fieldValue == null) || summarylessAttributes.contains(fieldValue))
			return;
		AttributeSummary as = this.getListFieldSummary(fieldName, false);
		if (as != null)
			as.remove(fieldValue);
	}
	private void uncacheDocumentAttributeValues(Attributed values) {
		for (int f = 0; f < documentListFields.length; f++)
			this.uncacheDocumentAttributeValue(documentListFields[f], ((String) values.getAttribute(documentListFields[f])));
		for (int f = 0; f < documentListFieldsAdmin.length; f++)
			this.uncacheDocumentAttributeValue(documentListFieldsAdmin[f], ((String) values.getAttribute(documentListFields[f])));
	}
	private AttributeSummary getListFieldSummary(String fieldName, boolean create) {
		AttributeSummary as = ((AttributeSummary) this.docAttributeValueCache.get(fieldName));
		if ((as == null) && create) {
			as = new AttributeSummary();
			this.docAttributeValueCache.put(fieldName, as);
		}
		return as;
	}
	
	/**
	 * Constructor passing 'DIO' as the letter code to super constructor
	 */
	public GoldenGateDIO() {
		super("DIO");
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.AbstractServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	set up checksum digest, filtering update user an time
		this.checksumDigest = new AnnotationChecksumDigest();
		this.checksumDigest.addAttributeFilter(new AttributeFilter() {
			public boolean filterAttribute(String attributeName) {
				return (false
						|| UPDATE_USER_ATTRIBUTE.equals(attributeName)
						|| UPDATE_TIME_ATTRIBUTE.equals(attributeName)
						|| attributeName.startsWith(UPDATE_USER_ATTRIBUTE + "-")
						|| attributeName.startsWith(UPDATE_TIME_ATTRIBUTE + "-")
						|| CHECKOUT_USER_ATTRIBUTE.equals(attributeName)
						|| CHECKOUT_TIME_ATTRIBUTE.equals(attributeName)
						|| DOCUMENT_VERSION_ATTRIBUTE.equals(attributeName)
						);
			}
		});
		
		//	load custom type and attribute filters for checksum
		String checksumIgnoreTypes = this.configuration.getSetting("checksumIgnoreTypes", "");
		if (checksumIgnoreTypes.length() != 0) {
			final ChecksumFilterSet cfs = new ChecksumFilterSet(checksumIgnoreTypes);
			this.checksumDigest.addTypeFilter(new TypeFilter() {
				public boolean filterType(String annotationType) {
					return cfs.contains(annotationType);
				}
			});
		}
		String checksumIgnoreAttributes = this.configuration.getSetting("checksumIgnoreAttributes", "");
		if (checksumIgnoreAttributes.length() != 0) {
			final ChecksumFilterSet cfs = new ChecksumFilterSet(checksumIgnoreAttributes);
			this.checksumDigest.addAttributeFilter(new AttributeFilter() {
				public boolean filterAttribute(String attributeName) {
					return cfs.contains(attributeName);
				}
			});
		}
		
		//	get document storage folder
		String docFolderName = this.configuration.getSetting("documentFolderName", "Documents");
		while (docFolderName.startsWith("./"))
			docFolderName = docFolderName.substring("./".length());
		File docFolder = (((docFolderName.indexOf(":\\") == -1) && (docFolderName.indexOf(":/") == -1) && !docFolderName.startsWith("/")) ? new File(this.dataPath, docFolderName) : new File(docFolderName));
		
		// initialize document store
		this.dst = new DocumentStore("DioDocuments", docFolder, this.configuration.getSetting("documentEncoding"), this);
		
		// get and check database connection
		this.io = this.host.getIoProvider();
		if (!this.io.isJdbcAvailable())
			throw new RuntimeException("GoldenGateDIO: Cannot work without database access.");
		
		//	get status of existing document table
		String tableStateQuery = ("SELECT * FROM " + DOCUMENT_TABLE_NAME + " WHERE 1=0;");
		SqlQueryResult oldTable = null;
		try {
			oldTable = this.io.executeSelectQuery(tableStateQuery, true);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading existing table definition.");
			System.out.println("  query was " + tableStateQuery);
		}
		
		//	create/update document table
		TableDefinition td = new TableDefinition(DOCUMENT_TABLE_NAME);
		
		//	- identification data
		td.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DOCUMENT_ID_HASH_NAME, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(EXTERNAL_IDENTIFIER_NAME, TableDefinition.VARCHAR_DATATYPE, EXTERNAL_IDENTIFIER_NAME_LENGTH);
		td.addColumn(EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(EXTERNAL_IDENTIFIER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, EXTERNAL_IDENTIFIER_LENGTH);
		td.addColumn(DOCUMENT_NAME_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_NAME_COLUMN_LENGTH);
		
		//	- meta data
		td.addColumn(DOCUMENT_AUTHOR_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_AUTHOR_COLUMN_LENGTH);
		td.addColumn(DOCUMENT_DATE_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(DOCUMENT_TITLE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_TITLE_COLUMN_LENGTH);
		td.addColumn(DOCUMENT_KEYWORDS_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_KEYWORDS_COLUMN_LENGTH);
		
		//	- management data
		td.addColumn(CHECKIN_USER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		td.addColumn(CHECKIN_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		td.addColumn(CHECKOUT_USER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		td.addColumn(CHECKOUT_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		td.addColumn(UPDATE_USER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		td.addColumn(UPDATE_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		td.addColumn(DOCUMENT_VERSION_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(DOCUMENT_CHECKSUM_COLUMN_NAME, TableDefinition.VARCHAR_DATATYPE, 32);
		
		//	ensure table
		if (!this.io.ensureTable(td, true))
			throw new RuntimeException("GoldenGateDIO: Cannot work without database access.");
		
		//	index table
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_ID_HASH_NAME);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, EXTERNAL_IDENTIFIER_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, CHECKIN_USER_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, UPDATE_USER_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, CHECKOUT_USER_ATTRIBUTE);
		
		//	get external identifier setting
		this.extIdAttributeNameList = this.configuration.getSetting(EXTERNAL_IDENTIFIER_ATTRIBUTE, "");
		if (this.extIdAttributeNameList != null)
			this.extIdAttributeNames = this.extIdAttributeNameList.split("\\s+");
		
		//	set annotation nesting order for data model
		Gamta.setAnnotationNestingOrder(this.configuration.getSetting(ANNOTATION_NESTING_ORDER_SETTING, Gamta.getAnnotationNestingOrder()));
		System.out.println("  - annotation nesting order set");
		
		//	load document keywording data
		File keywordingFolder = new File(this.dataPath, "Keywording");
		if (keywordingFolder.exists()) try {
			
			//	load stop word list
			this.stopWords = StringVector.loadList(new File(keywordingFolder, "stopWords.txt"));
			
			//	load keywording config file
			TreeNode keyworderRoot = IoTools.readAndParseFile(new File(keywordingFolder, "keywording.xml"));
			TreeNode[] keyworderNodes = TreeTools.getAllNodesOfType(keyworderRoot, "keyworder");
			ArrayList keyworderList = new ArrayList();
			for (int k = 0; k < keyworderNodes.length; k++) try {
				
				//	get basic attributes
				String type = keyworderNodes[k].getAttribute("type");
				String normalizationMode = keyworderNodes[k].getAttribute("normalization");
				int weightBoostFactor = 1;
				try {
					weightBoostFactor = Integer.parseInt(keyworderNodes[k].getAttribute("weightBoost", "1"));
				} catch (NumberFormatException nfe) {}
				
				//	create list based keyworder
				if ("list".equals(type)) {
					String listFileName = keyworderNodes[k].getAttribute("listFile");
					if (listFileName == null)
						continue;
					Dictionary list = StringVector.loadList(new File(keywordingFolder, listFileName));
					keyworderList.add(new ListKeyworder(normalizationMode, weightBoostFactor, list));
				}
				
				//	create pattern based keyworder
				else if ("pattern".equals(type)) {
					String pattern = keyworderNodes[k].getAttribute("pattern");
					if (pattern == null) {
						String patternFileName = keyworderNodes[k].getAttribute("patternFile");
						if (patternFileName == null)
							continue;
						StringVector patternParts = StringVector.loadList(new File(keywordingFolder, patternFileName));
						pattern = RegExUtils.normalizeRegEx(patternParts.concatStrings("\n"));
					}
					try {
						Pattern.compile(pattern);
						keyworderList.add(new PatternKeyworder(normalizationMode, weightBoostFactor, pattern, this.stopWords));
					}
					catch (PatternSyntaxException pse) {
						System.out.println("GoldenGateDIO: " + pse.getClass().getName() + " (" + pse.getMessage() + ") while loading pattern document keyworder '" + keyworderNodes[k] + "'");
					}
				}
				
				//	create GPath based keyworder
				else if ("gPath".equals(type)) {
					String gPathString = keyworderNodes[k].getAttribute("gPath");
					if (gPathString == null) {
						String gPathFileName = keyworderNodes[k].getAttribute("gPathFile");
						if (gPathFileName == null)
							continue;
						StringVector gPathParts = StringVector.loadList(new File(keywordingFolder, gPathFileName));
						gPathString = gPathParts.concatStrings(" ");
					}
					try {
						keyworderList.add(new GPathKeyworder(normalizationMode, weightBoostFactor, new GPath(gPathString)));
					}
					catch (GPathSyntaxException gse) {
						System.out.println("GoldenGateDIO: " + gse.getClass().getName() + " (" + gse.getMessage() + ") while loading GPath document keyworder '" + keyworderNodes[k] + "'");
					}
				}
			}
			
			//	catch exceptions for individual keyworders individually
			catch (IOException ioe) {
				System.out.println("GoldenGateDIO: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document keyworder '" + keyworderNodes[k] + "'");
				ioe.printStackTrace(System.out);
			}
			
			//	store keyworders
			this.documentKeyworders = ((DocumentKeyworder[]) keyworderList.toArray(new DocumentKeyworder[keyworderList.size()]));
		}
		catch (IOException ioe) {
			System.out.println("GoldenGateDIO: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document keywording meta data.");
			ioe.printStackTrace(System.out);
		}
		
		//	get maximum document list size for non-admin users
		try {
			this.documentListSizeThreshold = Integer.parseInt(this.configuration.getSetting("documentListSizeThreshold", "0"));
		} catch (NumberFormatException nfe) {}
		
//		
//		// start folder watchers
//		Settings inputFoldersSettings = this.configuration.getSubset(INPUT_FOLDER_SUBSET_PREFIX);
//		for (int i = 0; i < inputFoldersSettings.size(); i++) {
//			Settings inputFolderSettings = inputFoldersSettings.getSubset("" + i);
//			String inputFolderName = inputFolderSettings.getSetting(INPUT_FOLDER_SETTING);
//			if (inputFolderName != null) this.watchFolder(inputFolderName);
//		}
		
		//	trigger database table checks
		final String fTableStateQuery = tableStateQuery;
		final SqlQueryResult fOldTable = oldTable;
		Thread tableChecker = new Thread("GgDioTableChecker") {
			public void run() {
				checkDatabaseTable(fTableStateQuery, fOldTable);
			}
		};
		tableChecker.start();
	}
	
	private void checkDatabaseTable(String tableStateQuery, SqlQueryResult oldTable) {
		
		//	add check updating ID hash where it's 0 (should run only once, and best at night)
		String getNoIdHashDocsQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME +
				" WHERE " + DOCUMENT_ID_HASH_NAME + " = 0" +
				";";
		LinkedList noIdHashDocIDs = new LinkedList();
		SqlQueryResult idhSqr = null;
		try {
			idhSqr = this.io.executeSelectQuery(getNoIdHashDocsQuery);
			while (idhSqr.next())
				noIdHashDocIDs.addLast(idhSqr.getString(0));
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents with invalid identifiers hash.");
			System.out.println("  query was " + getNoIdHashDocsQuery);
		}
		finally {
			if (idhSqr != null)
				idhSqr.close();
		}
		
		//	update ID hash where necessary
		while (noIdHashDocIDs.size() != 0) {
			String docId = ((String) noIdHashDocIDs.removeFirst());
			String updateIdHashQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
					" SET " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "" +
					" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
					";");
			try {
				this.io.executeUpdateQuery(updateIdHashQuery);
			}
			catch (SQLException sqle) {
				System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating external identifier of document '" + docId + "'.");
				System.out.println("  query was " + updateIdHashQuery);
			}
		}
		
		//	TODO add document checksum where necessary
		
		//	verify external identifier (might have been set after first documents were stored)
		if (this.extIdAttributeNames.length != 0) {
			System.out.println("  - external document identifier list set to " + Arrays.toString(this.extIdAttributeNames) + ", checking database ...");
			
			StringBuffer externalIdentifierNameList = new StringBuffer();
			for (int i = 0; i < this.extIdAttributeNames.length; i++) {
				String externalIdentifierName = this.extIdAttributeNames[i];
				if (externalIdentifierName.length() > EXTERNAL_IDENTIFIER_NAME_LENGTH)
					externalIdentifierName = externalIdentifierName.substring(0, EXTERNAL_IDENTIFIER_NAME_LENGTH);
				if (externalIdentifierNameList.length() != 0)
					externalIdentifierNameList.append(", ");
				externalIdentifierNameList.append("'" + EasyIO.sqlEscape(externalIdentifierName) + "'");
			}
			
			//	update external ID config hash where required
			String updateExtIdHashQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
					" SET " + EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME + " = " + this.extIdAttributeNameList.hashCode() + "" +
					" WHERE " + EXTERNAL_IDENTIFIER_NAME + " IN (" + externalIdentifierNameList + ")" +
						" AND " + EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME + " <> " + this.extIdAttributeNameList.hashCode() + "" +
					";");
			try {
				this.io.executeUpdateQuery(updateExtIdHashQuery);
			}
			catch (SQLException sqle) {
				System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating external identifier config hash.");
				System.out.println("  query was " + updateExtIdHashQuery);
			}
			
			//	get IDs of documents with missing or outdated external identifier
			String getNoExtIdDocsQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE + 
					" FROM " + DOCUMENT_TABLE_NAME +
					" WHERE " + EXTERNAL_IDENTIFIER_NAME + " NOT IN (" + externalIdentifierNameList + ")" +
						" AND " + EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME + " <> " + this.extIdAttributeNameList.hashCode() + "" +
					";";
			LinkedList noExtIdDocIDs = new LinkedList();
			SqlQueryResult eidSqr = null;
			try {
				eidSqr = this.io.executeSelectQuery(getNoExtIdDocsQuery);
				while (eidSqr.next())
					noExtIdDocIDs.addLast(eidSqr.getString(0));
			}
			catch (SQLException sqle) {
				System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents with invalid external identifiers.");
				System.out.println("  query was " + getNoExtIdDocsQuery);
			}
			finally {
				if (eidSqr != null)
					eidSqr.close();
			}
			
			//	update external identifier where necessary
			while (noExtIdDocIDs.size() != 0) {
				String docId = ((String) noExtIdDocIDs.removeFirst());
				Properties docAttributes;
				try {
					docAttributes = this.dst.getDocumentAttributes(docId);
				}
				catch (IOException ioe) {
					System.out.println("GoldenGateDIO: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while getting attributes of document '" + docId + "'.");
					ioe.printStackTrace(System.out);
					continue;
				}
				String externalIdentifier = null;
				String externalIdentifierName = null;
				for (int i = 0; i < this.extIdAttributeNames.length; i++) {
					externalIdentifier = docAttributes.getProperty(this.extIdAttributeNames[i]);
					if (externalIdentifier == null)
						continue;
					externalIdentifier = externalIdentifier.replaceAll("\\s", "");
					externalIdentifierName = this.extIdAttributeNames[i];
					if (externalIdentifier.length() > EXTERNAL_IDENTIFIER_LENGTH)
						externalIdentifier = externalIdentifier.substring(0, EXTERNAL_IDENTIFIER_LENGTH);
					break;
				}
				if ((externalIdentifier == null) || (externalIdentifierName == null)) {
					externalIdentifier = "";
					externalIdentifierName = "";
				}
				
				String updateExtIdQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
						" SET " + EXTERNAL_IDENTIFIER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(externalIdentifier) + "'" +
						", " + EXTERNAL_IDENTIFIER_NAME + " = '" + EasyIO.sqlEscape(externalIdentifierName) + "'" +
						", " + EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME + " = " + this.extIdAttributeNameList.hashCode() + "" +
						" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
							" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "" +
						";");
				try {
					this.io.executeUpdateQuery(updateExtIdQuery);
					if (!"".equals(externalIdentifier))
						System.out.println("    - external identifier of document '" + docId + "' set to '" + externalIdentifierName + "' with value '" + externalIdentifier + "'");
				}
				catch (SQLException sqle) {
					System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating external identifier of document '" + docId + "'.");
					System.out.println("  query was " + updateExtIdQuery);
				}
			}
		}
		
		//	do attribute update if the data table existed before
		if (oldTable != null) try {
			
			//	get status of current document table
			SqlQueryResult newTable = this.io.executeSelectQuery(tableStateQuery);
			
			//	get existing attributes
			HashMap existingColumns = new HashMap();
			for (int c = 0; c < oldTable.getColumnCount(); c++) {
				System.out.println(" - got existing column: " + oldTable.getColumnName(c));
				existingColumns.put(oldTable.getColumnName(c).toLowerCase(), new Integer(oldTable.getColumnLength(c)));
			}
			
			//	diff with current attributes
			Map updateAttributes = new LinkedHashMap();
			for (int c = 0; c < newTable.getColumnCount(); c++) {
				String columnName = newTable.getColumnName(c).toLowerCase();
				if (!documentMetaDataAttributes.containsKey(columnName))
					continue;
				
				if (existingColumns.containsKey(columnName)) {
					if (newTable.getColumnLength(c) > ((Integer) existingColumns.get(columnName)).intValue()) {
						System.out.println(" - got extended column: " + columnName);
						updateAttributes.put(columnName, new Integer(newTable.getColumnLength(c)));
					}
				}
				else {
					System.out.println(" - got added column: " + columnName);
					updateAttributes.put(columnName, new Integer(newTable.getColumnLength(c)));
				}
			}
			
			//	do update
			if (updateAttributes.size() != 0)
				this.updateDataTable(updateAttributes);
			
			//	clean up
			oldTable.close();
			newTable.close();
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading updated table definition.");
			System.out.println("  query was " + tableStateQuery);
		}
	}
	
	private static final Properties documentMetaDataAttributes = new Properties();
	//	TODO keep this in sync with table definition
	//	though it's not elegant, we have to do this case insensitively, as some database servers mess up case in column names, eg PostgreSQL
	static {
		documentMetaDataAttributes.setProperty(DOCUMENT_AUTHOR_ATTRIBUTE.toLowerCase(), DOCUMENT_AUTHOR_ATTRIBUTE);
		documentMetaDataAttributes.setProperty(DOCUMENT_DATE_ATTRIBUTE.toLowerCase(), DOCUMENT_DATE_ATTRIBUTE);
		documentMetaDataAttributes.setProperty(DOCUMENT_TITLE_ATTRIBUTE.toLowerCase(), DOCUMENT_TITLE_ATTRIBUTE);
		documentMetaDataAttributes.setProperty(DOCUMENT_KEYWORDS_ATTRIBUTE.toLowerCase(), DOCUMENT_KEYWORDS_ATTRIBUTE);
	}
	
	private void updateDataTable(Map updateAttributes) {
		
		//	prepare logging
		String updateAttributeString = "";
		for (Iterator ait = updateAttributes.keySet().iterator(); ait.hasNext();) {
			String attributeName = ((String) ait.next());
			if (updateAttributeString.length() != 0)
				updateAttributeString += (ait.hasNext() ? ", " : ((updateAttributes.size() > 2) ? ", and" : " and "));
			updateAttributeString += attributeName;
		}
		System.out.println("  - document table extended, updating attributes " + updateAttributeString + " ...");
		
		// assemble query
		String idQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE + " FROM " + DOCUMENT_TABLE_NAME + ";";
		
		// get document IDs
		LinkedList updateDocIDs = new LinkedList();
		SqlQueryResult uidSqr = null;
		try {
			uidSqr = this.io.executeSelectQuery(idQuery);
			while (uidSqr.next())
				updateDocIDs.addLast(uidSqr.getString(0));
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents with invalid external identifiers.");
			System.out.println("  query was " + idQuery);
		}
		finally {
			if (uidSqr != null)
				uidSqr.close();
		}
		
		//	update document data
		while (!updateDocIDs.isEmpty()) {
			String docId = ((String) updateDocIDs.removeFirst());
			
			//	fetch data from DST
			Properties docAttributes;
			try {
				docAttributes = this.dst.getDocumentAttributes(docId);
			}
			catch (IOException ioe) {
				System.out.println("GoldenGateDIO: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while getting attributes of document '" + docId + "'.");
				ioe.printStackTrace(System.out);
				continue;
			}
			
			//	assemble updates
			StringVector assignments = new StringVector();
			for (Iterator ait = updateAttributes.keySet().iterator(); ait.hasNext();) {
				String columnName = ((String) ait.next());
				String attributeName = documentMetaDataAttributes.getProperty(columnName, columnName);
				String attributeValue = (DOCUMENT_KEYWORDS_ATTRIBUTE.equals(attributeName) ? this.getDocumentKeywordString(docId) : docAttributes.getProperty(attributeName));
				if (attributeValue == null)
					continue;
				
				//	get attribute length (also helps with type)
				int attributeLength = ((Integer) updateAttributes.get(columnName)).intValue();
				
				//	string attribute (fit length)
				if (attributeLength > 0) {
					if (attributeLength < attributeValue.length())
						attributeValue = attributeValue.substring(0, attributeLength);
					assignments.addElement(attributeName + " = '" + EasyIO.sqlEscape(attributeValue) + "'");
				}
				
				//	numeric attribute (parse number in order to catch SQL injections)
				else try {
					long numericAttributeValue = Long.parseLong(attributeValue.trim());
					assignments.addElement(attributeName + " = " + numericAttributeValue);
				}
				catch (NumberFormatException nfe) {}
			}
			
			//	catch empty assignments (might happen for missing attributes in DST)
			if (assignments.isEmpty())
				continue;
			
			//	do update
			String updateQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
					" SET " + assignments.concatStrings(", ") +
					" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
						" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "" +
					";");
			try {
				this.io.executeUpdateQuery(updateQuery);
				System.out.println("    - " + updateAttributeString + " of document '" + docId + "' updated.");
			}
			catch (SQLException sqle) {
				System.out.println("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating external identifier of document '" + docId + "'.");
				System.out.println("  query was " + updateQuery);
			}
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {

		// get access authority
		this.uaa = ((UserAccessAuthority) GoldenGateServerComponentRegistry.getServerComponent(UserAccessAuthority.class.getName()));

		// check success
		if (this.uaa == null) throw new RuntimeException(UserAccessAuthority.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	prefill caches
		DioDocumentList dl = this.getDocumentList(UserAccessAuthority.SUPERUSER_NAME, false, null);
		while (dl.hasNextDocument()) {
			DocumentListElement dle = dl.getNextDocument();
			this.docIdSet.add(dle.getAttribute(DOCUMENT_ID_ATTRIBUTE));
			for (int f = 0; f < dl.listFieldNames.length; f++) {
				if (dl.hasNoSummary(dl.listFieldNames[f]))
					continue;
				this.cacheDocumentAttributeValue(dl.listFieldNames[f], ((String) dle.getAttribute(dl.listFieldNames[f])));
			}
		}
		
		//	register permissions
		this.uaa.registerPermission(UPLOAD_DOCUMENT_PERMISSION);
		this.uaa.registerPermission(UPDATE_DOCUMENT_PERMISSION);
		this.uaa.registerPermission(DELETE_DOCUMENT_PERMISSION);
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.AbstractServerComponent#exitComponent()
	 */
	protected void exitComponent() {
//		
//		//	shut down input folder watchers
//		Settings inputFoldersSettings = this.configuration.getSubset(INPUT_FOLDER_SUBSET_PREFIX);
//		for (int i = 0; i < this.inputFolderWatchers.size(); i++) {
//			InputFolderWatcher ifw = ((InputFolderWatcher) this.inputFolderWatchers.get(i));
//			ifw.shutdown();
//
//			Settings inputFolderSettings = inputFoldersSettings.getSubset("" + i);
//			inputFolderSettings.setSetting(INPUT_FOLDER_SETTING, ifw.folderName);
//		}
//		this.inputFolderWatchers.clear();
		
		//	disconnect from database
		this.io.close();
		
		//	shut down document store
		this.dst.shutdown();
	}
	
	private static final String SHOW_VERSION_HISTORY_COMMAND = "showVersions";
	private static final String REVERT_DOCUMENT_COMMAND = "revertDoc";
	private static final String CHECKOUT_STATE_COMMAND = "checkoutState";
	private static final String RELEASE_DOCUMENT_COMMAND = "releaseDoc";
//	private static final String INPUT_FOLDER_SUBSET_PREFIX = "INPUT_FOLDERS";
//	private static final String INPUT_FOLDER_SETTING = "FOLDER";
//	private static final String LIST_WATCHED_FOLDERS_COMMAND = "watched";
//	private static final String WATCH_FOLDER_COMMAND = "watch";
//	private static final String STOP_WATCH_FOLDER_COMMAND = "stopwatch";
	private static final String IMPORT_FILE_COMMAND = "import";
	private static final String ISSUE_EVENT_COMMAND = "issueEvent";
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
		//	list versions of some document (good for testing that feature, in the first place)
		ca = new ComponentActionConsole() {
			private DateFormat updateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			public String getActionCommand() {
				return SHOW_VERSION_HISTORY_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						SHOW_VERSION_HISTORY_COMMAND + " <documentId>",
						"List the version history of a document:",
						"- <documentId>: The ID of the document to list the versions of"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					DioDocumentVersionHistory dvh = getDocumentVersionHistory(arguments[0]);
					if (dvh == null) {
						this.reportError(" Invalid document ID '" + arguments[0] + "'.");
						return;
					}
					DioDocumentVersion[] dvs = dvh.getVersions();
					this.reportResult("There are currently " + dvs.length + " versions of document '" + arguments[0] + "':");
					for (int v = 0; v < dvs.length; v++)
						this.reportResult("  " + dvs[v].version + ((v == 0) ? " (current)" : "") + " by " + dvs[v].updateUser + ", " + this.updateTimeFormat.format(new Date(dvs[v].updateTime)));
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);
		
		//	list versions of some document (good for testing that feature, in the first place)
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return REVERT_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						REVERT_DOCUMENT_COMMAND + " <documentId> <version>",
						"Revert a document to a previous version:",
						"- <documentId>: The ID of the document to revert",
						("- <version>: The number of versions to revert (positive value: absolute version number as from '" + SHOW_VERSION_HISTORY_COMMAND + "', negative number: relative version number)"),
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 2) {
					DioDocumentVersionHistory dvh = getDocumentVersionHistory(arguments[0]);
					if (dvh == null) {
						this.reportError(" Invalid document ID '" + arguments[0] + "'.");
						return;
					}
					int version;
					try {
						version = Integer.parseInt(arguments[1]);
					}
					catch (NumberFormatException nfe) {
						this.reportError("Invalid version number " + arguments[1]);
						return;
					}
					if (version < 0)
						version = (dvh.getVersion() + version);
					if (version <= 0) {
						this.reportError("Invalid version number " + arguments[1] + " for doc document '" + arguments[0] + "', there are only " + dvh.getVersion() + " versions.");
						this.reportError("Use " + SHOW_VERSION_HISTORY_COMMAND + " to list the existing versions.");
						return;
					}
					DioDocumentVersion docVersion = dvh.getVersion(version);
					if (docVersion == null) {
						this.reportError("Invalid version number " + arguments[1] + " for doc document '" + arguments[0] + "'.");
						this.reportError("Use " + SHOW_VERSION_HISTORY_COMMAND + " to list the existing versions.");
						return;
					}
					try {
						DocumentRoot doc = checkoutDocument(docVersion.updateUser, arguments[0], docVersion.version);
						this.reportResult("Document checked out in version " + docVersion.version);
						updateDocument(docVersion.updateUser, arguments[0], doc, new EventLogger() {
							public void writeLog(String logEntry) {
								reportResult(" ==> " + logEntry);
							}
						});
						this.reportResult("Document updated (reverted) to " + docVersion.version);
						releaseDocument(docVersion.updateUser, arguments[0]);
						this.reportResult("Document released");
					}
					catch (IOException ioe) {
						this.reportError("Could not revert document '" + arguments[0] + "': " + ioe.getMessage());
						this.reportError(ioe);
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID and the version number.");
			}
		};
		cal.add(ca);
		
		//	show the checkout state of some document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return CHECKOUT_STATE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						CHECKOUT_STATE_COMMAND + " <documentId>",
						"Show the checkout status of a document:",
						"- <documentId>: The ID of the document to check"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					String checkoutUser = getCheckoutUser(arguments[0]);
					if (checkoutUser == null)
						this.reportError("Invalid document ID '" + arguments[0] + "'.");
					else if ("".equals(checkoutUser))
						this.reportResult("Document '" + arguments[0] + "' is not checked out by anyone.");
					else this.reportResult("Document '" + arguments[0] + "' is currently checked out by '" + checkoutUser + "'.");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);
		
		//	clear the checkout state some document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELEASE_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						RELEASE_DOCUMENT_COMMAND + " <documentId> <checkoutUser>",
						"Clear the checkout status of a document:",
						"- <documentId>: The ID of the document to release",
						"- <checkoutUser>: The user currently holding the lock on the document with the argument ID",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 2) {
					String checkoutUser = getCheckoutUser(arguments[0]);
					if (checkoutUser == null)
						this.reportError("Invalid document ID '" + arguments[0] + "'.");
					else if ("".equals(checkoutUser))
						this.reportError("Document '" + arguments[0] + "' is not checked out by anyone.");
					else if (!checkoutUser.equals(arguments[1]))
						this.reportError("Document '" + arguments[0] + "' is not checked out by '" + arguments[1] + "'.");
					else {
						releaseDocument(arguments[1], arguments[0]);
						this.reportResult("Document '" + arguments[0] + "' released successfully.");
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID and checkout user as the only argument.");
			}
		};
		cal.add(ca);
		
		// list documents
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT_LIST;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId) && !DOCUMENT_SERVLET_SESSION_ID.equals(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}
				
				//	read filter string
				String filterString = input.readLine();
				Properties filter;
				if (filterString.length() == 0)
					filter = null;
				else {
					String[] filters = filterString.split("\\&");
					filter = new Properties();
					for (int f = 0; f < filters.length; f++) {
						String[] pair = filters[f].split("\\=");
						if (pair.length == 2) {
							String name = pair[0].trim();
							String value = URLDecoder.decode(pair[1].trim(), ENCODING).trim();
							
							String existingValue = filter.getProperty(name);
							if (existingValue != null)
								value = existingValue + "\n" + value;
							
							filter.setProperty(name, value);
						}
					}
				}
				
				DioDocumentList docList = getDocumentList((DOCUMENT_SERVLET_SESSION_ID.equals(sessionId) ? DOCUMENT_SERVLET_SESSION_ID : uaa.getUserNameForSession(sessionId)), false, filter);
				
				output.write(GET_DOCUMENT_LIST);
				output.newLine();
				
				docList.writeData(output);
				output.newLine();
			}
		};
		cal.add(ca);
//
//		// list documents
//		ca = new ComponentActionNetwork() {
//			public String getActionCommand() {
//				return GET_DOCUMENT_LIST_SHARED;
//			}
//			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
//				
//				// check authentication
//				String sessionId = input.readLine();
//				if (!uaa.isValidSession(sessionId) && !DOCUMENT_SERVLET_SESSION_ID.equals(sessionId)) {
//					output.write("Invalid session (" + sessionId + ")");
//					output.newLine();
//					logError("Request for invalid session - " + sessionId);
//					return;
//				}
//				
//				//	read filter string
//				String filterString = input.readLine();
//				Properties filter;
//				if (filterString.length() == 0)
//					filter = null;
//				else {
//					String[] filters = filterString.split("\\&");
//					filter = new Properties();
//					for (int f = 0; f < filters.length; f++) {
//						String[] pair = filters[f].split("\\=");
//						if (pair.length == 2) {
//							String name = pair[0].trim();
//							String value = URLDecoder.decode(pair[1].trim(), ENCODING).trim();
//							
//							String existingValue = filter.getProperty(name);
//							if (existingValue != null)
//								value = existingValue + "\n" + value;
//							
//							filter.setProperty(name, value);
//						}
//					}
//				}
//				
//				DioDocumentList docList = getDocumentList((DOCUMENT_SERVLET_SESSION_ID.equals(sessionId) ? DOCUMENT_SERVLET_SESSION_ID : uaa.getUserNameForSession(sessionId)), false, filter);
//				
//				output.write(GET_DOCUMENT_LIST_SHARED);
//				output.newLine();
//				
//				docList.writeData(output);
//				output.newLine();
//			}
//		};
//		cal.add(ca);
		
		// deliver document update protocol
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_UPDATE_PROTOCOL;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				String docId = input.readLine();
				UpdateProtocol up = ((UpdateProtocol) updateProtocolsByDocId.get(docId));
				if (up == null) {
					output.write("No recent update for document '" + docId + "'");
					output.newLine();
				}
				else {
					output.write(GET_UPDATE_PROTOCOL);
					output.newLine();
					for (int e = 0; e < up.size(); e++) {
						output.write((String) up.get(e));
						output.newLine();
					}
				}
			}
		};
		cal.add(ca);

		// deliver document through network
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId) && !DOCUMENT_SERVLET_SESSION_ID.equals(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}
				
				String docId = input.readLine();
				int version = 0;
				int idVersionSplit = docId.indexOf('.');
				if (idVersionSplit != -1) {
					try {
						version = Integer.parseInt(docId.substring(idVersionSplit + 1));
					} catch (NumberFormatException nfe) {}
					docId = docId.substring(0, idVersionSplit);
				}

				DocumentReader dr = getDocumentAsStream(docId, version);
				try {
					output.write(GET_DOCUMENT);
					output.newLine();
					
					char[] cbuf = new char[1024];
					int read;
					while ((read = dr.read(cbuf, 0, cbuf.length)) != -1)
						output.write(cbuf, 0, read);
					output.newLine();
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
				}
				finally {
					dr.close();
				}
			}
		};
		cal.add(ca);

		// deliver document through network
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT_AS_STREAM;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId) && !DOCUMENT_SERVLET_SESSION_ID.equals(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}
				
				String docId = input.readLine();
				int version = 0;
				int idVersionSplit = docId.indexOf('.');
				if (idVersionSplit != -1) {
					try {
						version = Integer.parseInt(docId.substring(idVersionSplit + 1));
					} catch (NumberFormatException nfe) {}
					docId = docId.substring(0, idVersionSplit);
				}

				DocumentReader dr = getDocumentAsStream(docId, version);
				try {
					output.write(GET_DOCUMENT_AS_STREAM);
					output.newLine();
					
					output.write("" + dr.docLength());
					output.newLine();
					
					char[] cbuf = new char[1024];
					int read;
					while ((read = dr.read(cbuf, 0, cbuf.length)) != -1)
						output.write(cbuf, 0, read);
					output.newLine();
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
				}
				finally {
					dr.close();
				}
			}
		};
		cal.add(ca);

		// update a document, or store a new one
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return UPLOAD_DOCUMENT;
			}
			public void performActionNetwork(final BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, UPLOAD_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for uploading a document");
					output.newLine();
					return;
				}

				try {
					int docSize = Integer.parseInt(input.readLine());
					logInfo(" - size is " + docSize);

					String docName = input.readLine();
					logInfo(" - name is " + docName);

					String externalIdentifierMode = input.readLine();
					logInfo(" - external identifier mode is " + externalIdentifierMode);

					DocumentRoot doc = GenericGamtaXML.readDocument(new Reader() {
						boolean end = false;
						public void close() throws IOException {
							input.close();
						}
						public int read(char[] cbuf, int off, int len) throws IOException {
							if (this.end) return -1;
							int c;
							for (int o = 0; o < (len - off); o++) {
								c = input.read();
								if ((c == '\n') || (c == '\r')) {
									this.end = true;
									return o;
								} else cbuf[off + o] = ((char) c);
							}
							return (len - off);
						}
					});
					logInfo(" - got document, size is " + doc.size());
					
					// check if data transfer complete
					if (doc.size() < docSize) {
						output.write("Document transfer incomplete, received only " + doc.size() + " of " + docSize + " tokens.");
						output.newLine();
						return;
					}
					else if (doc.size() > docSize) logWarning(" - more tokens than predicted, but OK ...");
					
					String user = uaa.getUserNameForSession(sessionId);
					logInfo(" - user is " + user);

					if (!doc.hasAttribute(CHECKIN_USER_ATTRIBUTE))
						doc.setAttribute(CHECKIN_USER_ATTRIBUTE, user);
					doc.setAttribute(UPDATE_USER_ATTRIBUTE, user);

					doc.setAttribute(DOCUMENT_NAME_ATTRIBUTE, docName);

					UpdateProtocol up = new UpdateProtocol(((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE, doc.getAnnotationID())), false);
					int version;
					try {
						version = uploadDocument(user, doc, up, externalIdentifierMode);
						up.setHead(docName, version);
					}
					catch (DuplicateExternalIdentifierException deie) {
						output.write(DUPLICATE_EXTERNAL_IDENTIFIER);
						output.newLine();
						output.write(deie.externalIdentifierAttributeName);
						output.newLine();
						output.write(deie.conflictingExternalIdentifier);
						output.newLine();
						deie.writeConflictingDocuments(output);
						return;
					}
					
					output.write(UPLOAD_DOCUMENT);
					output.newLine();

					output.write("Document '" + docName + "' stored as version " + version);
					output.newLine();
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
				}
			}
		};
		cal.add(ca);

		// check out a document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return CHECKOUT_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, UPDATE_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for checking out a document");
					output.newLine();
					return;
				}

				// get user
				String userName = uaa.getUserNameForSession(sessionId);

				// get document ID
				String docId = input.readLine();
				int version = 0;
				int idVersionSplit = docId.indexOf('.');
				if (idVersionSplit != -1) {
					try {
						version = Integer.parseInt(docId.substring(idVersionSplit + 1));
					} catch (NumberFormatException nfe) {}
					docId = docId.substring(0, idVersionSplit);
				}
				
				// send response
				DocumentReader dr = null;
				try {
					// check out document
					dr = checkoutDocumentAsStream(userName, docId, version);
					
					//	and send it
					output.write(CHECKOUT_DOCUMENT);
					output.newLine();
					
					char[] cbuf = new char[1024];
					int read;
					while ((read = dr.read(cbuf, 0, cbuf.length)) != -1)
						output.write(cbuf, 0, read);
					output.newLine();
				}
				catch (DocumentCheckedOutException dcoe) {
					
					//	catch concurrent checkout exception separately
					output.write(dcoe.getMessage());
					output.newLine();
					return;
				}
				catch (IOException ioe) {

					// release document if sending it fails for any reason
					releaseDocument(userName, docId);

					// propagate exception
					throw ioe;
				}
				finally {
					if (dr != null)
						dr.close();
				}
			}
		};
		cal.add(ca);

		// check out a document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return CHECKOUT_DOCUMENT_AS_STREAM;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, UPDATE_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for checking out a document");
					output.newLine();
					return;
				}

				// get user
				String userName = uaa.getUserNameForSession(sessionId);

				// get document ID
				String docId = input.readLine();
				int version = 0;
				int idVersionSplit = docId.indexOf('.');
				if (idVersionSplit != -1) {
					try {
						version = Integer.parseInt(docId.substring(idVersionSplit + 1));
					} catch (NumberFormatException nfe) {}
					docId = docId.substring(0, idVersionSplit);
				}

				// send response
				DocumentReader dr = null;
				try {
					// check out document
					dr = checkoutDocumentAsStream(userName, docId, version);
					
					//	and send it
					output.write(CHECKOUT_DOCUMENT_AS_STREAM);
					output.newLine();
					
					output.write("" + dr.docLength());
					output.newLine();
					
					char[] cbuf = new char[1024];
					int read;
					while ((read = dr.read(cbuf, 0, cbuf.length)) != -1)
						output.write(cbuf, 0, read);
					output.newLine();
				}
				catch (DocumentCheckedOutException dcoe) {
					
					//	catch concurrent checkout exception separately
					output.write(dcoe.getMessage());
					output.newLine();
					return;
				}
				catch (IOException ioe) {

					// release document if sending it fails for any reason
					releaseDocument(userName, docId);

					// propagate exception
					throw ioe;
				}
				finally {
					if (dr != null)
						dr.close();
				}
			}
		};
		cal.add(ca);

		// update a document, or store a new one
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return UPDATE_DOCUMENT;
			}
			public void performActionNetwork(final BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, UPDATE_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for updating a document");
					output.newLine();
					return;
				}
				
				try {
					int docSize = Integer.parseInt(input.readLine());
					logInfo(" - size is " + docSize);

					String docName = input.readLine();
					logInfo(" - name is " + docName);

					String externalIdentifierMode = input.readLine();
					logInfo(" - external identifier mode is " + externalIdentifierMode);
					
					DocumentRoot doc = GenericGamtaXML.readDocument(new Reader() {
						boolean end = false;
						public void close() throws IOException {
							input.close();
						}
						public int read(char[] cbuf, int off, int len) throws IOException {
							if (this.end) return -1;
							int c;
							for (int o = 0; o < (len - off); o++) {
								c = input.read();
								if ((c == '\n') || (c == '\r')) {
									this.end = true;
									return o;
								} else cbuf[off + o] = ((char) c);
							}
							return (len - off);
						}
					});
					logInfo(" - got document, size is " + doc.size());

					// check if data transfer complete
					if (doc.size() < docSize) {
						output.write("Document transfer incomplete, received only " + doc.size() + " of " + docSize + " tokens.");
						output.newLine();
						return;
					}
					else if (doc.size() > docSize) logWarning(" - more tokens than predicted, but OK ...");

					String user = uaa.getUserNameForSession(sessionId);
					logInfo(" - user is " + user);

					String docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE, doc.getAnnotationID()));

					if (!doc.hasAttribute(CHECKIN_USER_ATTRIBUTE))
						doc.setAttribute(CHECKIN_USER_ATTRIBUTE, user);
					doc.setAttribute(UPDATE_USER_ATTRIBUTE, user);

					doc.setAttribute(DOCUMENT_NAME_ATTRIBUTE, docName);

					UpdateProtocol up = new UpdateProtocol(docId, false);
					int version;
					try {
						version = updateDocument(user, docId, doc, up, externalIdentifierMode);
						up.setHead(docName, version);
					}
					catch (DuplicateExternalIdentifierException deie) {
						output.write(DUPLICATE_EXTERNAL_IDENTIFIER);
						output.newLine();
						output.write(deie.externalIdentifierAttributeName);
						output.newLine();
						output.write(deie.conflictingExternalIdentifier);
						output.newLine();
						deie.writeConflictingDocuments(output);
						return;
					}
					
					output.write(UPDATE_DOCUMENT);
					output.newLine();
					
					output.write("Document '" + docName + "' " + ((version == -1) ? "unchanged" : ("stored as version " + version)));
					output.newLine();
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
				}
			}
		};
		cal.add(ca);

		// delete a document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return DELETE_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}
				
				//	check permission
				if (!uaa.hasSessionPermission(sessionId, DELETE_DOCUMENT_PERMISSION, true)) {
					output.write("Insufficient permissions for deleting a document");
					output.newLine();
					return;
				}
				
				try {
					String docId = input.readLine();
					final UpdateProtocol up = new UpdateProtocol(docId, true);
					deleteDocument(uaa.getUserNameForSession(sessionId), docId, up);
					
					output.write(DELETE_DOCUMENT);
					output.newLine();
					for (int e = 0; e < up.size(); e++) {
						output.write((String) up.get(e));
						output.newLine();
					}
				}
				catch (IOException ioe) {
					output.write(ioe.getMessage());
					output.newLine();
				}
			}
		};
		cal.add(ca);

		// release checked out document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return RELEASE_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {

				// check authentication
				String sessionId = input.readLine();
				if (!uaa.isValidSession(sessionId)) {
					output.write("Invalid session (" + sessionId + ")");
					output.newLine();
					logError("Request for invalid session - " + sessionId);
					return;
				}

				// get user
				String userName = uaa.getUserNameForSession(sessionId);

				// get document ID
				String docId = input.readLine();

				// release document
				releaseDocument(userName, docId);

				// send response
				output.write(RELEASE_DOCUMENT);
				output.newLine();
			}
		};
		cal.add(ca);
//
//		// list watched input folders
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return LIST_WATCHED_FOLDERS_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = { LIST_WATCHED_FOLDERS_COMMAND, "List the folders this GoldenGATE DIO is watching for new documents." };
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length == 0) {
//					this.reportResult("GoldenGATE DIO is currently watching the following folders for new documents:");
//					for (int i = 0; i < inputFolderWatchers.size(); i++) {
//						InputFolderWatcher ifw = ((InputFolderWatcher) inputFolderWatchers.get(i));
//						this.reportResult("- " + ifw.folderName + " (" + ifw.folderToWatch.getAbsolutePath() + ")");
//					}
//				}
//				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
//			}
//		};
//		cal.add(ca);
//
//		// watch a new folder for input documents
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return WATCH_FOLDER_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = { WATCH_FOLDER_COMMAND + " <folderName>", "Watch a folder for new documents to be added to this GoldenGATE DIO's storage:",
//						"- <folderName>: the folder to watch, either relative to the DIO's data path, or absolute." };
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length == 1) {
//					String error = watchFolder(arguments[0]);
//					if (error == null)
//						this.reportResult("Now watching folder " + arguments[0]);
//					else this.reportError(error);
//				}
//				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the folder to watch only.");
//			}
//		};
//		cal.add(ca);
//
//		// stop watching a folder for input documents
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return STOP_WATCH_FOLDER_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = {STOP_WATCH_FOLDER_COMMAND + " <folderName>", 
//						"Stop watching a folder for new documents to be added to this GoldenGATE DIO's storage space:",
//						"- <folderName> : the folder to stop watching, as listen in " + LIST_WATCHED_FOLDERS_COMMAND };
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length == 1) {
//					for (int i = 0; i < inputFolderWatchers.size(); i++) {
//						InputFolderWatcher ifw = ((InputFolderWatcher) inputFolderWatchers.get(i));
//						if (ifw.folderName.equals(arguments[0])) {
//							ifw.shutdown();
//							inputFolderWatchers.remove(i);
//							this.reportResult("Stopped watching folder " + ifw.folderName + " (" + ifw.folderToWatch.getAbsolutePath() + ").");
//							return;
//						}
//					}
//					this.reportError("Never watched folder " + arguments[0] + ", cannot stop doing so.");
//				}
//				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the folder to stop watching only.");
//			}
//		};
//		cal.add(ca);

		// import a specific document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_FILE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = { IMPORT_FILE_COMMAND + " <file>", "Import a document from a file:", "- <file>: the file to import, has to be an XML file" };
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					File file = new File(arguments[0]);
					if (file.exists() && file.isFile()) {
						try {
							DocumentRoot doc = SgmlDocumentReader.readDocument(file);
							String folderUserName = file.getAbsoluteFile().getParentFile().getName();

							if (!doc.hasAttribute(CHECKIN_USER_ATTRIBUTE)) doc.setAttribute(CHECKIN_USER_ATTRIBUTE, folderUserName);
							doc.setAttribute(UPDATE_USER_ATTRIBUTE, folderUserName);

							doc.setAttribute(DOCUMENT_NAME_ATTRIBUTE, file.getName());

							uploadDocument(folderUserName, doc, null);
						}
						catch (Exception e) {
							this.reportError("Error creating document from '" + file.getAbsolutePath() + "' - " + e.getMessage());
						}
					}
					else this.reportError(" Cannot import non-existent file or folder '" + file.getAbsolutePath() + "'");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the file to import only.");
			}
		};
		cal.add(ca);
		
		//	issue update events
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return ISSUE_EVENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						ISSUE_EVENT_COMMAND + " <docId>",
						"Issue an update event for a specific document ID:",
						"- <docId>: the ID of the document to issue an update event for"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1)
					issueEvent(arguments[0], this);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);

		// get actions from document store
		ComponentAction[] dstActions = this.dst.getActions();
		for (int a = 0; a < dstActions.length; a++)
			cal.add(dstActions[a]);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private void issueEvent(String docId, ComponentActionConsole cac) {
		StringBuffer query = new StringBuffer("SELECT " + DOCUMENT_ID_ATTRIBUTE + ", " + UPDATE_USER_ATTRIBUTE + ", " + UPDATE_TIME_ATTRIBUTE);
		query.append(" FROM " + DOCUMENT_TABLE_NAME);
		query.append(" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'");
		query.append(" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "");
		
		SqlQueryResult sqr = null;
		int count = 0;
		try {
			sqr = io.executeSelectQuery(query.toString(), true);
			while (sqr.next()) {
				String updateUser = sqr.getString(1);
				long updateTime = sqr.getLong(2);
				int docVersion = dst.getVersion(docId);
				GoldenGateServerEventService.notify(new DioDocumentEvent(updateUser, updateUser, docId, null, docVersion, GoldenGateDIO.class.getName(), updateTime, new EventLogger() {
					public void writeLog(String logEntry) {}
				}));
				count++;
			}
		}
		catch (SQLException sqle) {
			cac.reportError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while issuing document update events.");
			cac.reportError("  query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		cac.reportResult("Issued update events for " + count + " documents.");
	}
	
	private Map updateProtocolsByDocId = Collections.synchronizedMap(new LinkedHashMap(16, 0.9f, false) {
		protected boolean removeEldestEntry(Entry eldest) {
			return (this.size() > 128); // should be OK for starters
		}
	});
	
	private class UpdateProtocol extends ArrayList implements EventLogger {
		final String docId;
		final boolean isDeletion;
		UpdateProtocol(String docId, boolean isDeletion) {
			this.docId = docId;
			this.isDeletion = isDeletion;
			this.add("Document deleted"); // acts as placeholder for head entry in updates, which know their version number only later
			updateProtocolsByDocId.put(this.docId, this);
		}
		void setHead(String docName, int version) {
			if (this.isDeletion)
				this.set(0, "Document deleted");
			else if (version == -1) {
				this.set(0, ("Document '" + docName + "' unchanged"));
				this.close();
			}
			else this.set(0, ("Document '" + docName + "' stored as version " + version));
		}
		public void writeLog(String logEntry) {
			this.add(logEntry);
		}
		void close() {
			this.add(this.isDeletion ? DELETION_COMPLETE : UPDATE_COMPLETE);
		}
	}
//
//	private ArrayList inputFolderWatchers = new ArrayList();
//
//	private String watchFolder(String folderName) {
//		if (folderName != null) {
//
//			File folder;
//			if (folderName.startsWith("./") || folderName.startsWith(".\\")) folder = new File(this.dataPath, folderName.substring(2));
//			else folder = new File(folderName);
//
//			if (folder.exists()) {
//				
//				for (int i = 0; i < this.inputFolderWatchers.size(); i++) {
//					InputFolderWatcher ifw = ((InputFolderWatcher) this.inputFolderWatchers.get(i));
//					if (ifw.folderToWatch.getAbsolutePath().equals(folder.getAbsolutePath())) return ("Already watching folder " + folderName + " (" + folder.getAbsolutePath() + ")");
//				}
//
//				InputFolderWatcher ifw = new InputFolderWatcher(folder, folderName);
//				ifw.start();
//				this.inputFolderWatchers.add(ifw);
//
//				return null;
//			}
//			else return "Cannot watch non-existing folder.";
//		}
//		else return "Cannot watch null folder.";
//	}
//
//	// thread periodically checking input folders for new documents
//	private class InputFolderWatcher extends Thread {
//		private Object stopper = new Object();
//		private boolean shutdown = false;
//		private String folderName;
//		private File folderToWatch;
//		private File doneFolder;
//		
//		private InputFolderWatcher(File folder, String folderName) {
//			this.folderToWatch = folder;
//			this.folderName = folderName;
//
//			this.doneFolder = new File(this.folderToWatch, "Done");
//			this.doneFolder.mkdir();
//		}
//
//		public void run() {
//			while (!this.shutdown)
//				try {
//					synchronized (this.stopper) {
//						try {
//							this.stopper.wait(60000);
//						} catch (InterruptedException ie) {}
//					}
//
//					if (!this.shutdown) {
//						File[] files = this.folderToWatch.listFiles();
//						for (int f = 0; f < files.length; f++) {
//							if (files[f].isFile() && files[f].canRead()) try {
//								if (files[f].canWrite()) { // process file only if renaming possible
//									DocumentRoot doc = SgmlDocumentReader.readDocument(files[f]);
//									String folderUserName = this.folderToWatch.getName();
//									
//									if (!doc.hasAttribute(CHECKIN_USER_ATTRIBUTE))
//										doc.setAttribute(CHECKIN_USER_ATTRIBUTE, folderUserName);
//									doc.setAttribute(UPDATE_USER_ATTRIBUTE, folderUserName);
//									doc.setAttribute(DOCUMENT_NAME_ATTRIBUTE, files[f].getName());
//									
//									uploadDocument(folderUserName, doc, null);
//									
//									files[f].renameTo(new File(this.doneFolder, (files[f].getName())));
//								}
//							}
//							catch (IOException ioe) {
//								logError("Error creating document from '" + files[f].toString() + "' - " + ioe.getMessage());
//							}
//						}
//					}
//				}
//				catch (Exception e) {
//					logError("Error checking input folder '" + this.folderToWatch.toString() + "' - " + e.getMessage());
//				}
//		}
//
//		private void shutdown() {
//			this.shutdown = true;
//			synchronized (this.stopper) {
//				this.stopper.notifyAll();
//			}
//		}
//	}

	/**
	 * Add a document event listener to this GoldenGATE DIO so it receives
	 * notification when a document is checked out, updated, released, or
	 * deleted.
	 * @param del the document event listener to add
	 */
	public void addDocumentEventListener(DioDocumentEventListener del) {
		GoldenGateServerEventService.addServerEventListener(del);
	}
	
	/**
	 * Remove a document event listener from this GoldenGATE DIO.
	 * @param del the document event listener to remove
	 */
	public void removeDocumentEventListener(DioDocumentEventListener del) {
		GoldenGateServerEventService.removeServerEventListener(del);
	}
//	
//	/**
//	 * Add a document IO extension to add additional functionality and control to
//	 * DIO's IO operations.
//	 * @param die the document IO extension to add
//	 */
//	public synchronized void addDocumentIoExtension(DocumentIoExtension die) {
//		if ((die == null) || this.documentIoExtensionList.contains(die))
//			return;
//		
//		this.documentIoExtensions = null;
//		this.documentIoExtensionList.add(die);
//		Collections.sort(this.documentIoExtensionList);
//	}
//	
//	/**
//	 * Remove a document IO extension.
//	 * @param die the document IO extension to remove
//	 */
//	public synchronized void removeDocumentIoExtension(DocumentIoExtension die) {
//		this.documentIoExtensions = null;
//		this.documentIoExtensionList.remove(die);
//	}
//	
//	private ArrayList documentIoExtensionList = new ArrayList();
//	private DocumentIoExtension[] documentIoExtensions = null;
//	private synchronized DocumentIoExtension[] getDocumentIoExtensions() {
//		if (this.documentIoExtensions == null)
//			this.documentIoExtensions = ((DocumentIoExtension[]) this.documentIoExtensionList.toArray(new DocumentIoExtension[this.documentIoExtensionList.size()]));
//		return this.documentIoExtensions;
//	}
	
	/**
	 * Add a filter excluding application specific annotation types from document
	 * checksum computation. The latter serves as an indicator of whether or not
	 * an update actually changes a document from a content point of view and thus
	 * represents a new version. This helps prevent unnecessary propagation of
	 * updates and thus reduces load.
	 * @param tf the type filter to add
	 */
	public void addDocumentChecksumTypeFilter(TypeFilter tf) {
		this.checksumDigest.addTypeFilter(tf);
	}
	
	/**
	 * Remove a filter.
	 * @param tf the filter to remove
	 */
	public void removeDocumentChecksumTypeFilter(TypeFilter tf) {
		this.checksumDigest.removeTypeFilter(tf);
	}
	
	/**
	 * Add a filter excluding application specific annotation attributes from
	 * document checksum computation. The latter serves as an indicator of
	 * whether or not an update actually changes a document from a content point
	 * of view and thus represents a new version. This helps prevent unnecessary
	 * propagation of updates and thus reduces load.
	 * @param af the attribute filter to add
	 */
	public void addDocumentChecksumAttributeFilter(AttributeFilter af) {
		this.checksumDigest.addAttributeFilter(af);
	}
	
	/**
	 * Remove a filter.
	 * @param af the filter to remove
	 */
	public void removeDocumentChecksumAttributeFilter(AttributeFilter af) {
		this.checksumDigest.removeAttributeFilter(af);
	}
	
	private static class ChecksumFilterSet {
		private HashSet strings;
		private ArrayList prefixes;
		ChecksumFilterSet(String dataString) {
			String[] data = dataString.split("\\s+");
			for (int d = 0; d < data.length; d++) {
				if (data[d].endsWith("*"))
					this.addPrefix(data[d].substring(0, (data[d].length() - "*".length())));
				else this.addString(data[d]);
			}
		}
		void addString(String string) {
			if (this.strings == null)
				this.strings = new HashSet();
			this.strings.add(string);
		}
		void addPrefix(String prefix) {
			if (this.prefixes == null)
				this.prefixes = new ArrayList();
			this.prefixes.add(prefix);
		}
		boolean contains(String str) {
			if ((this.strings != null) && this.strings.contains(str))
				return true;
			if ((this.prefixes == null) || (str == null))
				return false;
			for (int p = 0; p < this.prefixes.size(); p++) {
				if (str.startsWith((String) this.prefixes.get(p)))
					return true;
			}
			return false;
		}
	}
	
	private String getChecksum(QueriableAnnotation document) {
		long start = System.currentTimeMillis();
		String checksum;
		try {
			checksum = this.checksumDigest.computeChecksum(document);
		}
		catch (IOException ioe) {
			this.logError(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while computing document checksum.");
			this.logError(ioe); // should not happen, but Java don't know ...
			return Gamta.getAnnotationID(); // use random value so a document is regarded as new or changed
		}
		this.logInfo("Checksum computed in " + (System.currentTimeMillis() - start) + " ms: " + checksum);
		return checksum;
	}
	
	/**
	 * Upload a new document, using its docId attribute as the storage ID (if
	 * the docId attribute is not set, the document's annotationId will be used
	 * instead). In case a document already exists with the same ID as the
	 * argument document's, an exception will be thrown. If not, the document
	 * will be created, but no lock will be granted to the uploading user. If a
	 * lock is required, use the updateDocument() method.
	 * @param userName the name of the user doing the update
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int uploadDocument(String userName, QueriableAnnotation doc, EventLogger logger) throws IOException {
		return this.uploadDocument(userName, doc, logger, EXTERNAL_IDENTIFIER_MODE_IGNORE);
	}
	
	/**
	 * Upload a new document, using its docId attribute as the storage ID (if
	 * the docId attribute is not set, the document's annotationId will be used
	 * instead). In case a document already exists with the same ID as the
	 * argument document's, an exception will be thrown. If not, the document
	 * will be created, but no lock will be granted to the uploading user. If a
	 * lock is required, use the updateDocument() method.
	 * @param userName the name of the user doing the update
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @param externalIdentifierMode how to handle external identifier conflicts?
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int uploadDocument(String userName, QueriableAnnotation doc, final EventLogger logger, String externalIdentifierMode) throws IOException {
		String docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE, doc.getAnnotationID()));
		long time = System.currentTimeMillis();
		
		int version = this.doUploadDocument(userName, doc, docId, logger, externalIdentifierMode, time);
		
		GoldenGateServerEventService.notify(new DioDocumentEvent(userName, userName, docId, doc, version, GoldenGateDIO.class.getName(), time, logger) {
			public void notificationComplete() {
				if (logger instanceof UpdateProtocol)
					((UpdateProtocol) logger).close();
			}
		});
		GoldenGateServerEventService.notify(new DioDocumentEvent(userName, docId, DioDocumentEvent.RELEASE_TYPE, GoldenGateDIO.class.getName(), time));
		
		return version;
	}
	private synchronized int doUploadDocument(String userName, final QueriableAnnotation doc, String docId, final EventLogger logger, String externalIdentifierMode, long time) throws IOException {
		
		// get checkout user (must be null if document is new)
		String checkoutUser = this.getCheckoutUser(docId);
		if (checkoutUser != null)
			throw new IOException("Document already exists, upload not possible.");
		
		
		// get timestamp
		String timeString = ("" + time);
		
		// update meta data
		doc.setAttribute(UPDATE_USER_ATTRIBUTE, userName);
		doc.setAttribute(CHECKIN_USER_ATTRIBUTE, userName);
		
		doc.setAttribute(UPDATE_TIME_ATTRIBUTE, timeString);
		doc.setAttribute(CHECKIN_TIME_ATTRIBUTE, timeString);
//		
//		
//		//	check extensions
//		DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//		for (int e = 0; e < dies.length; e++)
//			dies[e].extendUpload(doc, userName);
		
		
		//	get external identifier of document
		String externalIdentifier = null;
		String externalIdentifierName = null;
		for (int i = 0; i < this.extIdAttributeNames.length; i++) {
			externalIdentifier = ((String) doc.getAttribute(this.extIdAttributeNames[i]));
			if (externalIdentifier == null)
				continue;
			externalIdentifier = externalIdentifier.replaceAll("\\s", "");
			externalIdentifierName = this.extIdAttributeNames[i];
			if (externalIdentifier.length() > EXTERNAL_IDENTIFIER_LENGTH)
				externalIdentifier = externalIdentifier.substring(0, EXTERNAL_IDENTIFIER_LENGTH);
			break;
		}
		
		//	external identifier present, check conflicts if required
		if ((externalIdentifier != null) && EXTERNAL_IDENTIFIER_MODE_CHECK.equals(externalIdentifierMode)) {
			
			//	get list of conflicting documents
			DioDocumentList conflictList = this.getExternalIdentifierConflictList(externalIdentifier);
			
			//	we do have conflicts?
			if (conflictList.hasNextDocument())
				throw new DuplicateExternalIdentifierException(externalIdentifierName, externalIdentifier, conflictList);
		}
		
		
		// store document in DSS
		final int version = this.dst.storeDocument(doc);
		
		// check and (if necessary) truncate name
		String name = ((String) doc.getAttribute(DOCUMENT_NAME_ATTRIBUTE, docId));
		if (name.length() > DOCUMENT_NAME_COLUMN_LENGTH)
			name = name.substring(0, DOCUMENT_NAME_COLUMN_LENGTH);
		
		// check and (if necessary) truncate author
		String author = ((String) doc.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, docId));
		if (author.length() > DOCUMENT_AUTHOR_COLUMN_LENGTH)
			author = author.substring(0, DOCUMENT_AUTHOR_COLUMN_LENGTH);
		
		// check and (if necessary) truncate date
		int date = -1;
		try {
			date = Integer.parseInt((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1"));
		} catch (NumberFormatException nfe) {}
		
		// check and (if necessary) truncate title
		String title = ((String) doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, docId));
		if (title.length() > DOCUMENT_TITLE_COLUMN_LENGTH)
			title = title.substring(0, DOCUMENT_TITLE_COLUMN_LENGTH);
		
		// get update user
		String user = ((String) doc.getAttribute(UPDATE_USER_ATTRIBUTE, userName));
		if (user.length() > UserAccessAuthority.USER_NAME_MAX_LENGTH)
			user = user.substring(0, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		
		//	compute checksum
		String docChecksum = this.getChecksum(doc);
		
		// gather complete data for creating master table record
		StringBuffer fields = new StringBuffer(DOCUMENT_ID_ATTRIBUTE);
		StringBuffer fieldValues = new StringBuffer("'" + EasyIO.sqlEscape(docId) + "'");
		fields.append(", " + DOCUMENT_ID_HASH_NAME);
		fieldValues.append(", " + docId.hashCode() + "");
		
		//	store external identifier if present
		if (externalIdentifier != null) {
			if (externalIdentifierName.length() > EXTERNAL_IDENTIFIER_NAME_LENGTH)
				externalIdentifierName = externalIdentifierName.substring(0, EXTERNAL_IDENTIFIER_NAME_LENGTH);
			fields.append(", " + EXTERNAL_IDENTIFIER_NAME);
			fieldValues.append(", '" + EasyIO.sqlEscape(externalIdentifierName) + "'");
			
			if (externalIdentifier.length() > EXTERNAL_IDENTIFIER_LENGTH)
				externalIdentifier = externalIdentifier.substring(0, EXTERNAL_IDENTIFIER_LENGTH);
			fields.append(", " + EXTERNAL_IDENTIFIER_ATTRIBUTE);
			fieldValues.append(", '" + EasyIO.sqlEscape(externalIdentifier) + "'");
			
			fields.append(", " + EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME);
			fieldValues.append(", " + this.extIdAttributeNameList.hashCode() + "");
		}
		
		// set name
		fields.append(", " + DOCUMENT_NAME_ATTRIBUTE);
		fieldValues.append(", '" + EasyIO.sqlEscape(name) + "'");
		
		// set author
		fields.append(", " + DOCUMENT_AUTHOR_ATTRIBUTE);
		fieldValues.append(", '" + EasyIO.sqlEscape(author) + "'");
		
		// set date
		fields.append(", " + DOCUMENT_DATE_ATTRIBUTE);
		fieldValues.append(", " + date + "");
		
		// set keywords
		fields.append(", " + DOCUMENT_KEYWORDS_ATTRIBUTE);
		fieldValues.append(", '" + EasyIO.sqlEscape(this.getDocumentKeywordString(doc)) + "'");
		
		// set title
		fields.append(", " + DOCUMENT_TITLE_ATTRIBUTE);
		fieldValues.append(", '" + EasyIO.sqlEscape(title) + "'");
		
		// set checkin user
		fields.append(", " + CHECKIN_USER_ATTRIBUTE);
		fieldValues.append(", '" + EasyIO.sqlEscape(user) + "'");

		// set checkin/update time
		fields.append(", " + CHECKIN_TIME_ATTRIBUTE);
		fieldValues.append(", " + time);

		// set update user
		fields.append(", " + UPDATE_USER_ATTRIBUTE);
		fieldValues.append(", '" + EasyIO.sqlEscape(user) + "'");

		// set checkin/update time
		fields.append(", " + UPDATE_TIME_ATTRIBUTE);
		fieldValues.append(", " + time);

		// set version number
		fields.append(", " + DOCUMENT_VERSION_ATTRIBUTE);
		fieldValues.append(", " + version);
		
		//	set checksum
		fields.append(", " + DOCUMENT_CHECKSUM_COLUMN_NAME);
		fieldValues.append(", '" + EasyIO.sqlEscape(docChecksum) + "'");
		
		// set lock
		fields.append(", " + CHECKOUT_USER_ATTRIBUTE);
		fieldValues.append(", ''");
		fields.append(", " + CHECKOUT_TIME_ATTRIBUTE);
		fieldValues.append(", -1");

		// store data in collection main table
		String insertQuery = "INSERT INTO " + DOCUMENT_TABLE_NAME + 
				" (" + fields.toString() + ")" +
				" VALUES" +
				" (" + fieldValues.toString() + ")" +
				";";
		try {
			this.io.executeUpdateQuery(insertQuery);
			this.docIdSet.add(docId);
			this.cacheDocumentAttributeValues(doc);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing new document.");
			this.logError("  query was " + insertQuery);
			throw new IOException(sqle.getMessage());
		}
		
		// report new version
		return version;
	}

	/**
	 * Update an existing document, or store a new one, using its docId
	 * attribute as the storage ID (if the docId attribute is not set, the
	 * document's annotationId will be used instead). In case of an update, the
	 * updating user must have acquired the lock on the document in question
	 * (via one of the checkoutDocument() methods) prior to the invocation of
	 * this method. Otherwise, an IOException will be thrown. In case of a new
	 * document, the lock is automatically granted to the specified user, and
	 * remains with him until he yields it via the releaseDocument() method. If
	 * a lock is not desired for a new document, use the uploadDocument()
	 * method. Further, in case of an update, if the document is unchanged,
	 * no events are issued, and this method returns -1 to indicate so.
	 * @param userName the name of the user doing the update
	 * @param docId the ID of the document
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public synchronized int updateDocument(String userName, String docId, QueriableAnnotation doc, EventLogger logger) throws IOException {
		return this.updateDocument(userName, userName, docId, doc, logger, EXTERNAL_IDENTIFIER_MODE_IGNORE);
	}
	
	/**
	 * Update an existing document, or store a new one, using its docId
	 * attribute as the storage ID (if the docId attribute is not set, the
	 * document's annotationId will be used instead). In case of an update, the
	 * updating user must have acquired the lock on the document in question
	 * (via one of the checkoutDocument() methods) prior to the invocation of
	 * this method. Otherwise, an IOException will be thrown. In case of a new
	 * document, the lock is automatically granted to the specified user, and
	 * remains with him until he yields it via the releaseDocument() method. If
	 * a lock is not desired for a new document, use the uploadDocument()
	 * method. Further, in case of an update, if the document is unchanged,
	 * no events are issued, and this method returns -1 to indicate so.
	 * @param userName the name of the user doing the update
	 * @param authUserName the user name holding the checkout lock
	 * @param docId the ID of the document
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public synchronized int updateDocument(String userName, String authUserName, String docId, QueriableAnnotation doc, EventLogger logger) throws IOException {
		return this.updateDocument(userName, authUserName, docId, doc, logger, EXTERNAL_IDENTIFIER_MODE_IGNORE);
	}
	
	/**
	 * Update an existing document, or store a new one, using its docId
	 * attribute as the storage ID (if the docId attribute is not set, the
	 * document's annotationId will be used instead). In case of an update, the
	 * updating user must have acquired the lock on the document in question
	 * (via one of the checkoutDocument() methods) prior to the invocation of
	 * this method. Otherwise, an IOException will be thrown. In case of a new
	 * document, the lock is automatically granted to the specified user, and
	 * remains with him until he yields it via the releaseDocument() method. If
	 * a lock is not desired for a new document, use the uploadDocument()
	 * method. Further, in case of an update, if the document is unchanged,
	 * no events are issued, and this method returns -1 to indicate so.
	 * @param userName the name of the user doing the update
	 * @param docId the ID of the document
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @param externalIdentifierMode how to handle external identifier conflicts?
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int updateDocument(String userName, String docId, QueriableAnnotation doc, final EventLogger logger, String externalIdentifierMode) throws IOException {
		return this.updateDocument(userName, userName, docId, doc, logger, externalIdentifierMode);
	}
	
	/**
	 * Update an existing document, or store a new one, using its docId
	 * attribute as the storage ID (if the docId attribute is not set, the
	 * document's annotationId will be used instead). In case of an update, the
	 * updating user must have acquired the lock on the document in question
	 * (via one of the checkoutDocument() methods) prior to the invocation of
	 * this method. Otherwise, an IOException will be thrown. In case of a new
	 * document, the lock is automatically granted to the specified user, and
	 * remains with him until he yields it via the releaseDocument() method. If
	 * a lock is not desired for a new document, use the uploadDocument()
	 * method. Further, in case of an update, if the document is unchanged,
	 * no events are issued, and this method returns -1 to indicate so.
	 * @param userName the name of the user doing the update
	 * @param authUserName the user name holding the checkout lock
	 * @param docId the ID of the document
	 * @param doc the document to store
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @param externalIdentifierMode how to handle external identifier conflicts?
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int updateDocument(String userName, String authUserName, String docId, QueriableAnnotation doc, final EventLogger logger, String externalIdentifierMode) throws IOException {
		long time = System.currentTimeMillis();
		
		int version = this.doUpdateDocument(userName, authUserName, docId, doc, externalIdentifierMode, time);
		
		if (version == -1) // don't issue an event without any actual change
			this.logInfo("Document unchanged");
		else GoldenGateServerEventService.notify(new DioDocumentEvent(userName, authUserName, docId, doc, version, GoldenGateDIO.class.getName(), time, logger) {
			public void notificationComplete() {
				if (logger instanceof UpdateProtocol)
					((UpdateProtocol) logger).close();
			}
		});
		
		return version;
	}
	private synchronized int doUpdateDocument(String updateUserName, String authUserName, String docId, QueriableAnnotation doc, String externalIdentifierMode, long time) throws IOException {
		
		// check if document checked out
		if (!this.mayUpdateDocument(authUserName, docId))
			throw new IOException("Document checked out by other user, update not possible.");
		
		
		// get timestamp
		String timeString = ("" + time);
		
		// do not store checkout user info
		doc.removeAttribute(CHECKOUT_USER_ATTRIBUTE);
		doc.removeAttribute(CHECKOUT_TIME_ATTRIBUTE);

		// update meta data
		doc.setAttribute(UPDATE_USER_ATTRIBUTE, updateUserName);
		if (!doc.hasAttribute(CHECKIN_USER_ATTRIBUTE))
			doc.setAttribute(CHECKIN_USER_ATTRIBUTE, updateUserName);

		doc.setAttribute(UPDATE_TIME_ATTRIBUTE, timeString);
		if (!doc.hasAttribute(CHECKIN_TIME_ATTRIBUTE))
			doc.setAttribute(CHECKIN_TIME_ATTRIBUTE, timeString);
//		
//		
//		//	check extensions
//		DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//		for (int e = 0; e < dies.length; e++)
//			dies[e].extendUpdate(docId, doc, userName);
		
		
		//	compute and test checksum
		String docChecksum = this.getChecksum(doc);
		String exDocChecksumQuery = "SELECT " + DOCUMENT_CHECKSUM_COLUMN_NAME + 
				" FROM " + DOCUMENT_TABLE_NAME +
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
				" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() +
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(exDocChecksumQuery);
			if (sqr.next()) {
				String exDocChecksum = sqr.getString(0);
				if (docChecksum.equals(exDocChecksum))
					return -1;
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") fetching existing document checksum.");
			this.logError("  query was " + exDocChecksumQuery);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		
		//	get external identifier of document
		String externalIdentifier = null;
		String externalIdentifierName = null;
		for (int i = 0; i < this.extIdAttributeNames.length; i++) {
			externalIdentifier = ((String) doc.getAttribute(this.extIdAttributeNames[i]));
			if (externalIdentifier == null)
				continue;
			externalIdentifier = externalIdentifier.replaceAll("\\s", "");
			externalIdentifierName = this.extIdAttributeNames[i];
			if (externalIdentifier.length() > EXTERNAL_IDENTIFIER_LENGTH)
				externalIdentifier = externalIdentifier.substring(0, EXTERNAL_IDENTIFIER_LENGTH);
			break;
		}
		
		//	external identifier present, check conflicts if required
		if ((externalIdentifier != null) && EXTERNAL_IDENTIFIER_MODE_CHECK.equals(externalIdentifierMode)) {
			
			//	get list of conflicting documents
			DioDocumentList conflictList = this.getExternalIdentifierConflictList(externalIdentifier);
			
			//	do we have conflicts?
			if (conflictList.hasNextDocument()) {
				DuplicateExternalIdentifierException deie = new DuplicateExternalIdentifierException(externalIdentifierName, externalIdentifier, conflictList);
				DocumentListElement[] conflictDocs = deie.getConflictingDocuments();
				
				//	check if conflict has been ignored before
				boolean newDuplicate = true;
				for (int c = 0; c < conflictDocs.length; c++)
					if (docId.equals(conflictDocs[c].getAttribute(DOCUMENT_ID_ATTRIBUTE))) {
						newDuplicate = false;
						break;
					}
				
				//	if not, throw the exception
				if (newDuplicate) throw deie;
			}
		}
		
		//	clear cache
		this.documentMetaDataCache.remove(docId);
		
		
		// store document in DST
		final int version = this.dst.storeDocument(doc);
		
		StringVector assignments = new StringVector();

		//	store external identifier if present
		if (externalIdentifier != null) {
			if (externalIdentifierName.length() > EXTERNAL_IDENTIFIER_NAME_LENGTH)
				externalIdentifierName = externalIdentifierName.substring(0, EXTERNAL_IDENTIFIER_NAME_LENGTH);
			assignments.addElement(EXTERNAL_IDENTIFIER_NAME + " = '" + EasyIO.sqlEscape(externalIdentifierName) + "'");
			
			if (externalIdentifier.length() > EXTERNAL_IDENTIFIER_LENGTH)
				externalIdentifier = externalIdentifier.substring(0, EXTERNAL_IDENTIFIER_LENGTH);
			assignments.addElement(EXTERNAL_IDENTIFIER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(externalIdentifier) + "'");
			
			assignments.addElement(EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME + " = " + this.extIdAttributeNameList.hashCode() + "");
		}
		
		// check and (if necessary) truncate name
		String name = ((String) doc.getAttribute(DOCUMENT_NAME_ATTRIBUTE, docId));
		if (name.length() > DOCUMENT_NAME_COLUMN_LENGTH)
			name = name.substring(0, DOCUMENT_NAME_COLUMN_LENGTH);
		assignments.addElement(DOCUMENT_NAME_ATTRIBUTE + " = '" + EasyIO.sqlEscape(name) + "'");
		
		// check and (if necessary) truncate author
		String author = ((String) doc.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, docId));
		if (author.length() > DOCUMENT_AUTHOR_COLUMN_LENGTH)
			author = author.substring(0, DOCUMENT_AUTHOR_COLUMN_LENGTH);
		assignments.addElement(DOCUMENT_AUTHOR_ATTRIBUTE + " = '" + EasyIO.sqlEscape(author) + "'");
		
		// check and (if necessary) truncate date
		int date = -1;
		try {
			date = Integer.parseInt((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1"));
		} catch (NumberFormatException nfe) {}
		assignments.addElement(DOCUMENT_DATE_ATTRIBUTE + " = " + date + "");
		
		// set keywords
		String keywords = this.getDocumentKeywordString(doc);
		assignments.addElement(DOCUMENT_KEYWORDS_ATTRIBUTE + " = '" + EasyIO.sqlEscape(keywords) + "'");
		
		// check and (if necessary) truncate title
		String title = ((String) doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, docId));
		if (title.length() > DOCUMENT_TITLE_COLUMN_LENGTH)
			title = title.substring(0, DOCUMENT_TITLE_COLUMN_LENGTH);
		assignments.addElement(DOCUMENT_TITLE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(title) + "'");
		
		// get update user
		String updateUser = ((String) doc.getAttribute(UPDATE_USER_ATTRIBUTE, updateUserName));
		if (updateUser.length() > UserAccessAuthority.USER_NAME_MAX_LENGTH)
			updateUser = updateUser.substring(0, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		assignments.addElement(UPDATE_USER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(updateUser) + "'");
		String authUser = authUserName;
		if ((authUser != null) && (authUser.length() > UserAccessAuthority.USER_NAME_MAX_LENGTH))
			authUser = authUser.substring(0, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		
		// set update time
		assignments.addElement(UPDATE_TIME_ATTRIBUTE + " = " + time);

		// update version number
		assignments.addElement(DOCUMENT_VERSION_ATTRIBUTE + " = " + version);

		// update checksum
		assignments.addElement(DOCUMENT_CHECKSUM_COLUMN_NAME + " = '" + EasyIO.sqlEscape(docChecksum) + "'");

		// write new values
		String updateQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
				" SET " + assignments.concatStrings(", ") + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
					" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "" +
				";");

		try {
			
			//	update did not affect any rows ==> new document
			if (this.io.executeUpdateQuery(updateQuery) == 0) {
				
				//	gather complete data for creating master table record
				StringBuffer fields = new StringBuffer(DOCUMENT_ID_ATTRIBUTE);
				StringBuffer fieldValues = new StringBuffer("'" + EasyIO.sqlEscape(docId) + "'");
				fields.append(", " + DOCUMENT_ID_HASH_NAME);
				fieldValues.append(", " + docId.hashCode() + "");
				
				//	store external identifier if present
				if (externalIdentifier != null) {
					if (externalIdentifierName.length() > EXTERNAL_IDENTIFIER_NAME_LENGTH)
						externalIdentifierName = externalIdentifierName.substring(0, EXTERNAL_IDENTIFIER_NAME_LENGTH);
					fields.append(", " + EXTERNAL_IDENTIFIER_NAME);
					fieldValues.append(", '" + EasyIO.sqlEscape(externalIdentifierName) + "'");
					
					if (externalIdentifier.length() > EXTERNAL_IDENTIFIER_LENGTH)
						externalIdentifier = externalIdentifier.substring(0, EXTERNAL_IDENTIFIER_LENGTH);
					fields.append(", " + EXTERNAL_IDENTIFIER_ATTRIBUTE);
					fieldValues.append(", '" + EasyIO.sqlEscape(externalIdentifier) + "'");
					
					fields.append(", " + EXTERNAL_IDENTIFIER_CONFIG_HASH_NAME);
					fieldValues.append(", " + this.extIdAttributeNameList.hashCode() + "");
				}
				
				//	set name
				fields.append(", " + DOCUMENT_NAME_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(name) + "'");
				
				//	set author
				fields.append(", " + DOCUMENT_AUTHOR_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(author) + "'");
				
				//	set date
				fields.append(", " + DOCUMENT_DATE_ATTRIBUTE);
				fieldValues.append(", " + date + "");
				
				//	set title
				fields.append(", " + DOCUMENT_TITLE_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(title) + "'");
				
				//	set keywords
				fields.append(", " + DOCUMENT_KEYWORDS_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(keywords) + "'");
				
				//	set checkin user
				fields.append(", " + CHECKIN_USER_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(updateUser) + "'");

				//	set checkin time
				fields.append(", " + CHECKIN_TIME_ATTRIBUTE);
				fieldValues.append(", " + time);

				//	set update user
				fields.append(", " + UPDATE_USER_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(updateUser) + "'");

				//	set update time
				fields.append(", " + UPDATE_TIME_ATTRIBUTE);
				fieldValues.append(", " + time);

				//	update version number
				fields.append(", " + DOCUMENT_VERSION_ATTRIBUTE);
				fieldValues.append(", " + version);
				
				//	set checksum
				fields.append(", " + DOCUMENT_CHECKSUM_COLUMN_NAME);
				fieldValues.append(", '" + EasyIO.sqlEscape(docChecksum) + "'");

				// set lock
				fields.append(", " + CHECKOUT_USER_ATTRIBUTE);
				fieldValues.append(", '" + EasyIO.sqlEscape(authUser) + "'");
				fields.append(", " + CHECKOUT_TIME_ATTRIBUTE);
				fieldValues.append(", " + time);

				// store data in collection main table
				String insertQuery = "INSERT INTO " + DOCUMENT_TABLE_NAME + 
						" (" + fields.toString() + ")" +
						" VALUES" +
						" (" + fieldValues.toString() + ")" +
						";";
				try {
					this.io.executeUpdateQuery(insertQuery);
					this.docIdSet.add(docId);
					this.cacheDocumentAttributeValues(doc);
					this.cacheDocumentAttributeValue(CHECKOUT_USER_ATTRIBUTE, authUser);
				}
				catch (SQLException sqle) {
					this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing new document.");
					this.logError("  query was " + insertQuery);
					throw new IOException(sqle.getMessage());
				}
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating existing document.");
			this.logError("  query was " + updateQuery);
			throw new IOException(sqle.getMessage());
		}
		
		// report new version
		return version;
	}

	/**
	 * Delete a document from storage. If the document with the specified ID is
	 * checked out by a user other than the one doing the deletion, the document
	 * cannot be deleted and an IOException will be thrown.
	 * @param userName the user doing the deletion
	 * @param docId the ID of the document to delete
	 * @param logger a logger for obtaining detailed information on the deletion
	 *            process
	 * @throws IOException
	 */
	public void deleteDocument(String userName, String docId, final EventLogger logger) throws IOException {
		if (this.doDeleteDocument(userName, docId, logger))
			GoldenGateServerEventService.notify(new DioDocumentEvent(userName, docId, GoldenGateDIO.class.getName(), System.currentTimeMillis(), logger) {
				public void notificationComplete() {
					if (logger instanceof UpdateProtocol)
						((UpdateProtocol) logger).close();
				}
			});
	}
	private synchronized boolean doDeleteDocument(String userName, String docId, final EventLogger logger) throws IOException {
		String checkoutUser = this.getCheckoutUser(docId);
		
		//	check if document exists
		if (checkoutUser == null)
			throw new IOException("Document does not exist.");
//		
//		//	check extensions
//		DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//		for (int e = 0; e < dies.length; e++)
//			dies[e].extendDelete(docId, userName);
		
		//	check checkout state
		if (!checkoutUser.equals("") && !checkoutUser.equals(userName))
			throw new IOException("Document checked out by other user, delete not possible.");
		
		//	clear cache
		this.documentMetaDataCache.remove(docId);
		
		// delete document from DSS
		this.dst.deleteDocument(docId);

		// delete meta data
		String deleteQuery = "DELETE FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "'" +
					" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "" +
				";";
		try {
			DocumentListElement dle = this.getMetaData(docId);
			this.io.executeUpdateQuery(deleteQuery);
			this.uncacheDocumentAttributeValues(dle);
			this.checkoutUserCache.remove(docId);
			this.docIdSet.remove(docId);
			return true;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			this.logError("  query was " + deleteQuery);
			throw new IOException(sqle.getMessage());
		}
	}
	
	/**
	 * Check if a document with a given ID exists.
	 * @param documentId the ID of the document to check
	 * @return true if the document with the specified ID exists
	 * @see de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore#isDocumentAvailable(String)
	 */
	public boolean isDocumentAvailable(String documentId) {
		return this.dst.isDocumentAvailable(documentId);
	}

	/**
	 * Check if a document with a given ID exists and is free for checkout and
	 * update. A document is also editable for a user who already holds the
	 * checkout lock.
	 * @param docId the ID of the document to check
	 * @param userName the user name intending to edit the document
	 * @return true if the document with the specified ID exists and is free
	 *            for editing
	 */
	public boolean isDocumentEditable(String documentId, String userName) {
		String checkoutUser = this.getCheckoutUser(documentId);
		return ("".equals(checkoutUser) || ((userName != null) && userName.equals(checkoutUser)));
	}
	
	/**
	 * Retrieve the version history of a document. The returned object contains
	 * the update user and timestamp for each present and past version of the
	 * document. If a document with the specified ID does not exist, this
	 * method returns null.
	 * @param docId the ID of the document
	 * @return an holding the version history of the document with the
	 *         specified ID
	 * @throws IOException
	 */
	public DioDocumentVersionHistory getDocumentVersionHistory(String documentId) {
		
		//	load and return update users
		try {
			Properties docAttributes = this.dst.getDocumentAttributes(documentId, true);
			DioDocumentVersionHistory dvh = new DioDocumentVersionHistory(documentId);
			for (int version = 1;; version++) {
				String updateUser = docAttributes.getProperty(UPDATE_USER_ATTRIBUTE + "-" + version);
				String updateTime = docAttributes.getProperty(UPDATE_TIME_ATTRIBUTE + "-" + version);
				if ((updateUser == null) || (updateTime == null))
					break;
				dvh.addVersion(version, updateUser, Long.parseLong(updateTime));
			}
			return dvh;
		}
		catch (IOException ioe) {
			return null;
		}
	}
	
	/**
	 * The version history of a document stored in this DIO.
	 * 
	 * @author sautter
	 */
	public static class DioDocumentVersionHistory {
		
		/** the ID of the document the version history belongs to */
		public final String docId;
		
		private TreeMap versions = new TreeMap();
		
		DioDocumentVersionHistory(String docId) {
			this.docId = docId;
		}
		
		void addVersion(int version, String updateUser, long updateTime) {
			this.versions.put(new Integer(version), new DioDocumentVersion(this.docId, version, updateUser, updateTime));
		}
		
		/**
		 * Retrieve the name of the last user to update the document.
		 * @return the name of the last user to update the document
		 */
		public String getUpdateUser() {
			DioDocumentVersion lastVersion = ((DioDocumentVersion) this.versions.get(this.versions.lastKey()));
			return lastVersion.updateUser;
		}
		
		/**
		 * Retrieve the timestamp of the last update to the document.
		 * @return the timestamp of the last update to the document
		 */
		public long getUpdateTime() {
			DioDocumentVersion lastVersion = ((DioDocumentVersion) this.versions.get(this.versions.lastKey()));
			return lastVersion.updateTime;
		}
		
		/**
		 * Retrieve the current version number of the document.
		 * @return the current version number
		 */
		public int getVersion() {
			Integer lastVersion = ((Integer) this.versions.lastKey());
			return lastVersion.intValue();
		}
		
		/**
		 * Retrieve an individual version of the document.
		 * @param version the version number
		 * @return the version
		 */
		public DioDocumentVersion getVersion(int version) {
			return ((DioDocumentVersion) this.versions.get(new Integer(version)));
		}
		
		/**
		 * Retrieve the individual versions of the document. The versions in
		 * the returned array are in reverse chronological order.
		 * @return an array holding the versions
		 */
		public DioDocumentVersion[] getVersions() {
			LinkedList versions = new LinkedList();
			for (Iterator vit = this.versions.keySet().iterator(); vit.hasNext();) {
				DioDocumentVersion version = ((DioDocumentVersion) this.versions.get(vit.next()));
				versions.addFirst(version);
			}
			return ((DioDocumentVersion[]) versions.toArray(new DioDocumentVersion[versions.size()]));
		}
	}
	
	/**
	 * The version history of a document stored in this DIO.
	 * 
	 * @author sautter
	 */
	public static class DioDocumentVersion {
		
		/** the ID of the document the version belongs to */
		public final String docId;
		
		/** the sequential number of the version */
		public final int version;
		
		/** the name of the user who created the version */
		public final String updateUser;
		
		/** the timestamp of the version */
		public final long updateTime;
		
		DioDocumentVersion(String docId, int version, String updateUser, long updateTime) {
			this.docId = docId;
			this.version = version;
			this.updateUser = updateUser;
			this.updateTime = updateTime;
		}
	}
	
	/**
	 * Load a document from storage (the most recent version). This method loops
	 * through to the underlying DST, it exists so other components do not have
	 * to deal with two different storage components. The document is not
	 * locked, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore#loadDocument(String)
	 */
	public DocumentRoot getDocument(String documentId) throws IOException {
		return this.getDocument(documentId, 0);
	}
	
	/**
	 * Load a specific version of a document from storage. A positive version
	 * number indicates an actual version specifically, while a negative version
	 * number indicates a version backward relative to the most recent version.
	 * Version number 0 always returns the most recent version. This method
	 * loops through to the underlying DSS, it exists so other components do not
	 * have to deal with two different storage components. The document is not
	 * locked, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the version to load
	 * @return the document with the specified ID
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore#loadDocument(String,
	 *      int)
	 */
	public DocumentRoot getDocument(String documentId, int version) throws IOException {
		DocumentReader dr = this.getDocumentAsStream(documentId, version);
		try {
			return GenericGamtaXML.readDocument(dr);
		}
		finally {
			dr.close();
		}
	}
	
	/**
	 * Load a document from storage (the most recent version) as a character
	 * stream. In situations where a document is not required in its
	 * deserialized form, e.g. if it is intended to be written to some output
	 * stream, this method facilitates avoiding deserialization and
	 * reserialization. This method loops through to the underlying DST, it
	 * exists so other components do not have to deal with two different storage
	 * components. The document is not locked, so any attempt of an update will
	 * fail.
	 * @param documentId the ID of the document to load
	 * @return a reader providing the document with the specified ID in its
	 *         serialized form
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore#loadDocumentAsStream(String)
	 */
	public DocumentReader getDocumentAsStream(String documentId) throws IOException {
		return this.getDocumentAsStream(documentId, 0);
	}
	
	/**
	 * Load a specific version of a document from storage as a character stream.
	 * In situations where a document is not required in its deserialized form,
	 * e.g. if it is intended to be written to some output stream, this method
	 * facilitates avoiding deserialization and reserialization. A positive
	 * version number indicates an actual version specifically, while a negative
	 * version number indicates a version backward relative to the most recent
	 * version. Version number 0 always returns the most recent version. This
	 * method loops through to the underlying DSS, it exists so other components
	 * do not have to deal with two different storage components. The document
	 * is not locked, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the version to load
	 * @return a reader providing the document with the specified ID in its
	 *         serialized form
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore#loadDocumentAsStream(String,
	 *      int)
	 */
	public DocumentReader getDocumentAsStream(String documentId, int version) throws IOException {
		DocumentReader dr = this.dst.loadDocumentAsStream(documentId, version);
//		
//		//	check extensions
//		DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//		for (int e = 0; e < dies.length; e++) {
//			DocumentReader edr = dies[e].extendGet(documentId, dr);
//			if (edr != null)
//				dr = edr;
//		}
		
		return dr;
	}
	
	/**
	 * Check out a document from DIO. The document will be protected from
	 * editing by other users until it is released through the releaseDocument()
	 * method.
	 * @param userName the user checking out the document
	 * @param docId the ID of the document to check out
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public DocumentRoot checkoutDocument(String userName, String docId) throws IOException {
		return this.checkoutDocument(userName, docId, 0);
	}
	
	/**
	 * Check out a document from DIO. The document will be protected from
	 * editing by other users until it is released through the releaseDocument()
	 * method.
	 * @param userName the user checking out the document
	 * @param docId the ID of the document to check out
	 * @param version the version to load
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public DocumentRoot checkoutDocument(String userName, String docId, int version) throws IOException {
		DocumentReader dr = this.checkoutDocumentAsStream(userName, docId, version);
		try {
			return GenericGamtaXML.readDocument(dr);
		}
		finally {
			dr.close();
		}
	}
	
	/**
	 * Check out a document from DIO as a character stream. In situations where
	 * a document is not required in its deserialized form, e.g. if it is
	 * intended to be written to some output stream, this method facilitates
	 * avoiding deserialization and reserialization. The document will be
	 * protected from editing by other users until it is released through the
	 * releaseDocument() method.
	 * @param userName the user checking out the document
	 * @param docId the ID of the document to check out
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public DocumentReader checkoutDocumentAsStream(String userName, String docId) throws IOException {
		return this.checkoutDocumentAsStream(userName, docId, 0);
	}
	
	/**
	 * Check out a document from DIO as a character stream. In situations where
	 * a document is not required in its deserialized form, e.g. if it is
	 * intended to be written to some output stream, this method facilitates
	 * avoiding deserialization and reserialization. The document will be
	 * protected from editing by other users until it is released through the
	 * releaseDocument() method.
	 * @param userName the user checking out the document
	 * @param docId the ID of the document to check out
	 * @param version the version to load
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public DocumentReader checkoutDocumentAsStream(String userName, String docId, int version) throws IOException {
		long checkoutTime = System.currentTimeMillis();
		DocumentReader dr = this.doCheckoutDocumentAsStream(userName, docId, version, checkoutTime);
		GoldenGateServerEventService.notify(new DioDocumentEvent(userName, docId, DioDocumentEvent.CHECKOUT_TYPE, GoldenGateDIO.class.getName(), checkoutTime));
		return dr;
	}
	private synchronized DocumentReader doCheckoutDocumentAsStream(String userName, String docId, int version, long checkoutTime) throws IOException {
		String checkoutUser = this.getCheckoutUser(docId);
		
		//	check if document exists
		if (checkoutUser == null)
			throw new IOException("Document does not exist.");
		
		//	check if checkout possible for user
		if (!checkoutUser.equals("") && !checkoutUser.equals(userName))
			throw new DocumentCheckedOutException();
		
		//	mark document as checked out
		this.setCheckoutUser(docId, userName, checkoutTime);
		
		//	return document stream
		try {
			DocumentReader dr = this.dst.loadDocumentAsStream(docId, version);
			
			//	set checkout attributes
			dr.setAttribute(CHECKOUT_USER_ATTRIBUTE, userName);
			dr.setAttribute(CHECKOUT_TIME_ATTRIBUTE, ("" + checkoutTime));
//			
//			//	check extensions
//			DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//			for (int e = 0; e < dies.length; e++) {
//				DocumentReader edr = dies[e].extendCheckout(docId, dr, userName);
//				if (edr != null)
//					dr = edr;
//			}
			
			//	log checkout and notify listeners
			this.logInfo("document " + docId + " checked out by '" + userName + "'.");
//			this.eventNotifier.notify(new DioDocumentEvent(userName, docId, null, -1, DioDocumentEvent.CHECKOUT_TYPE, GoldenGateDIO.class.getName(), checkoutTime, null));
			
			return dr;
		}
		catch (IOException ioe) {
			this.setCheckoutUser(docId, "", -1);
			this.logError("GoldenGateDIO: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document " + docId + ".");
			this.logError(ioe);
			throw ioe;
		}
	}
	
	private static class DocumentCheckedOutException extends IOException {
		DocumentCheckedOutException() {
			super("Document checked out by other user, checkout not possible.");
		}
	}
	
	private boolean mayUpdateDocument(String userName, String docId) {
		String checkoutUser = this.getCheckoutUser(docId);
		
		//	document not known so far
		if (checkoutUser == null)
			return true;
		
		//	document checked out by user in question
		if (checkoutUser.equals(userName))
			return true;
		
		//	document checked out by other user
		if (!"".equals(checkoutUser))
			return false;
		
		//	try and check out document for current user
		return this.setCheckoutUser(docId, userName, System.currentTimeMillis());
	}
	
	/**
	 * Release a document. The lock on the document is released, so other users
	 * can check it out again.
	 * @param userName the name of the user holding the lock of the document to
	 *            release
	 * @param docId the ID of the document to release
	 */
	public void releaseDocument(String userName, String docId) {
		if (this.doReleaseDocument(userName, docId))
			GoldenGateServerEventService.notify(new DioDocumentEvent(userName, docId, DioDocumentEvent.RELEASE_TYPE, GoldenGateDIO.class.getName(), System.currentTimeMillis()));
	}
	private synchronized boolean doReleaseDocument(String userName, String docId) {
		String checkoutUser = this.getCheckoutUser(docId);
		
		//	check if document exists
		if (checkoutUser == null)
			return false;
//		
//		//	check extensions
//		DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//		for (int e = 0; e < dies.length; e++) try {
//			dies[e].extendRelease(docId, userName);
//		}
//		catch (IOException ioe) {
//			return false;
//		}
		
		//	release document if possible
		if (this.uaa.isAdmin(userName) || checkoutUser.equals(userName)) // admin user, or user holding the lock
			return this.setCheckoutUser(docId, "", -1);
		else return false;
	}

	private static final int checkoutUserCacheSize = 256;

	private LinkedHashMap checkoutUserCache = new LinkedHashMap(checkoutUserCacheSize, .9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return this.size() > checkoutUserCacheSize;
		}
	};
		
	private DioDocumentList getExternalIdentifierConflictList(String externalIdentifier) {
		
		// collect field names
		StringVector fieldNames = new StringVector();
		fieldNames.addContent(documentListFields);
		
		// assemble query
		String query = "SELECT " + fieldNames.concatStrings(", ") + 
				" FROM " + DOCUMENT_TABLE_NAME +
				" WHERE " + EXTERNAL_IDENTIFIER_ATTRIBUTE + " LIKE '" + EasyIO.prepareForLIKE(externalIdentifier) + "'" +
				" ORDER BY " + DOCUMENT_NAME_ATTRIBUTE +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			
			// return SQL backed list
			final SqlQueryResult finalSqr = sqr;
			return new DioDocumentList(fieldNames.toStringArray()) {
				SqlQueryResult sqr = finalSqr;
				DocumentListElement next = null;
				public boolean hasNextDocument() {
					if (this.next != null) return true;
					else if (this.sqr == null) return false;
					else if (this.sqr.next()) {
						this.next = new DocumentListElement();
						for (int f = 0; f < this.listFieldNames.length; f++)
							this.next.setAttribute(this.listFieldNames[f], this.sqr.getString(f));
						this.addListFieldValues(this.next); 
						return true;
					}
					else {
						this.sqr.close();
						this.sqr = null;
						return false;
					}
				}
				public DocumentListElement getNextDocument() {
					if (!this.hasNextDocument()) return null;
					DocumentListElement next = this.next;
					this.next = null;
					return next;
				}
			};
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents for external identifier conflict.");
			this.logError("  query was " + query);

			// return dummy list
			return new DioDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public DocumentListElement getNextDocument() {
					return null;
				}
			};
		}
	}
	
	/**
	 * Get the name of the user who has checked out a document with a given ID
	 * and therefore holds the lock for that document.
	 * @param docId the ID of the document in question
	 * @return the name of the user who has checked out the document with the
	 *         specified ID, the empty string if the document is currently not
	 *         checked out by any user, and null if there is no document with
	 *         the specified ID
	 */
	public String getCheckoutUser(String docId) {

		// do cache lookup
		String checkoutUser = ((String) this.checkoutUserCache.get(docId));

		// cache hit
		if (checkoutUser != null)
			return checkoutUser;

		// cache miss, prepare loading data
		String query = "SELECT " + CHECKOUT_USER_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
					" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "" +
				";";

		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			if (sqr.next()) {
				checkoutUser = sqr.getString(0);
				if (checkoutUser == null)
					checkoutUser = "";
				this.checkoutUserCache.put(docId, checkoutUser);
				return checkoutUser;
			}
			else return null;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading document checkout user.");
			this.logError("  query was " + query);
			return null;
		}
		finally {
			if (sqr != null) sqr.close();
		}
	}
	
	/**
	 * Retrieve the attributes of a document, as stored in the archive. There is
	 * no guarantee with regard to the attributes contained in the returned
	 * properties. If a document with the specified ID does not exist, this
	 * method returns null.
	 * @param docId the ID of the document
	 * @return a Properties object holding the attributes of the document with
	 *         the specified ID
	 * @throws IOException
	 */
	public Properties getDocumentAttributes(String docId) {
		try {
			return this.dst.getDocumentAttributes(docId);
		}
		catch (IOException ioe) {
			return null;
		}
	}
	
	/**
	 * Retrieve the meta data for a specific document. The attributes of the
	 * returned document list element correspond to those held in a document
	 * list retrieved via one of the getDocumentList() methods for a user
	 * without administrative privileges. If a document with the specified ID
	 * does not exist, this method returns null.
	 * @param docId the ID of the document to retrieve the meta data for
	 * @return the meta data for the document with the specified ID
	 */
	public DocumentListElement getMetaData(String docId) {
		
		//	do cache lookup
		DocumentListElement dle = ((DocumentListElement) this.documentMetaDataCache.get(docId));
		if (dle != null)
			return dle;
		
		// collect field names
		StringVector fieldNames = new StringVector();
		fieldNames.addContent(documentListFields);
		
		// assemble query
		String query = "SELECT " + fieldNames.concatStrings(", ") + 
				" FROM " + DOCUMENT_TABLE_NAME +
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
					" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "" +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			if (sqr.next()) {
				dle = new DocumentListElement();
				for (int f = 0; f < fieldNames.size(); f++)
					dle.setAttribute(fieldNames.get(f), sqr.getString(f));
				this.documentMetaDataCache.put(docId, dle);
				return dle;
			}
			else return null;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while reading meta data for document " + docId + ".");
			this.logError("  query was " + query);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	private static final int documentMetaDataCacheSize = 256;

	private LinkedHashMap documentMetaDataCache = new LinkedHashMap(documentMetaDataCacheSize, .9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return this.size() > documentMetaDataCacheSize;
		}
	};
	
	private boolean setCheckoutUser(String docId, String checkoutUser, long checkoutTime) {
		StringVector assignments = new StringVector();

		// set checkout user
		checkoutUser = ((checkoutUser == null) ? "" : checkoutUser);
		if (checkoutUser.length() > UserAccessAuthority.USER_NAME_MAX_LENGTH)
			checkoutUser = checkoutUser.substring(0, UserAccessAuthority.USER_NAME_MAX_LENGTH);
		assignments.addElement(CHECKOUT_USER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(checkoutUser) + "'");

		// set checkout time
		assignments.addElement(CHECKOUT_TIME_ATTRIBUTE + " = " + checkoutTime);

		// write new values
		String updateQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
				" SET " + assignments.concatStrings(", ") + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
					" AND " + DOCUMENT_ID_HASH_NAME + " = " + docId.hashCode() + "" +
				";");
		try {
			this.io.executeUpdateQuery(updateQuery);
			this.checkoutUserCache.put(docId, checkoutUser);
			return true;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while setting checkout user for document " + docId + ".");
			this.logError("  query was " + updateQuery);
			return false;
		}
	}

	private static final String[] documentListFields = {
			DOCUMENT_ID_ATTRIBUTE,
			EXTERNAL_IDENTIFIER_ATTRIBUTE,
			DOCUMENT_NAME_ATTRIBUTE,
			DOCUMENT_AUTHOR_ATTRIBUTE,
			DOCUMENT_DATE_ATTRIBUTE,
			DOCUMENT_TITLE_ATTRIBUTE,
			DOCUMENT_KEYWORDS_ATTRIBUTE,
			CHECKIN_USER_ATTRIBUTE,
			CHECKIN_TIME_ATTRIBUTE,
			UPDATE_USER_ATTRIBUTE,
			UPDATE_TIME_ATTRIBUTE,
			DOCUMENT_VERSION_ATTRIBUTE,
		};

	private static final String[] documentListFieldsAdmin = {
			CHECKOUT_USER_ATTRIBUTE,
			CHECKOUT_TIME_ATTRIBUTE
		};

	/**
	 * Retrieve a list of meta data for the documents available through this
	 * DIO. The list includes the document ID, name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list includes all available documents, regardless of their checkout
	 * state. That means all of the documents on the list can be retrieved from
	 * the getDocument() method for read access, but none is guaranteed to be
	 * available for checkout and editing.
	 * @return a list of meta data for the documents available through this DIO
	 */
	public DioDocumentList getDocumentListFull() {
		
		// collect field names
		StringVector fieldNames = new StringVector();
		fieldNames.addContent(documentListFields);
		
		// assemble query
		String query = "SELECT " + fieldNames.concatStrings(", ") + 
				" FROM " + DOCUMENT_TABLE_NAME +
				" ORDER BY " + DOCUMENT_NAME_ATTRIBUTE +
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			
			// return SQL backed list
			final SqlQueryResult finalSqr = sqr;
			return new DioDocumentList(fieldNames.toStringArray()) {
				SqlQueryResult sqr = finalSqr;
				DocumentListElement next = null;
				public boolean hasNextDocument() {
					if (this.next != null) return true;
					else if (this.sqr == null) return false;
					else if (this.sqr.next()) {
						this.next = new DocumentListElement();
						for (int f = 0; f < this.listFieldNames.length; f++)
							this.next.setAttribute(this.listFieldNames[f], this.sqr.getString(f));
						this.addListFieldValues(this.next);
						return true;
					}
					else {
						this.sqr.close();
						this.sqr = null;
						return false;
					}
				}
				public DocumentListElement getNextDocument() {
					if (!this.hasNextDocument()) return null;
					DocumentListElement next = this.next;
					this.next = null;
					return next;
				}
				public int getDocumentCount() {
					return docIdSet.size();
				}
			};
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
			this.logError("  query was " + query);

			// return dummy list
			return new DioDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public DocumentListElement getNextDocument() {
					return null;
				}
				public int getDocumentCount() {
					return 0;
				}
			};
		}
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this DIO.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @return a list of meta data for the document available through this DIO
	 */
	public DioDocumentList getDocumentList() {
		return this.getDocumentList("", false, null);
	}

	/**
	 * Retrieve a list of meta data for the document available through this DIO.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @param headOnly if set to true, this method returns an empty list, only
	 *            containing the header data (field names and attribute value
	 *            summaries)
	 * @return a list of meta data for the document available through this DIO
	 */
	public DioDocumentList getDocumentList(boolean headOnly) {
		return this.getDocumentList("", false, null);
	}

	/**
	 * Retrieve a list of meta data for the document available through this DIO.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. If the
	 * specified user has administrative privileges, checked-out documents are
	 * included in the list, and the list also provides the checkout user and
	 * checkout status of every document. Otherwise, the list only includes
	 * documents that can be checked out, i.e., ones that are not checked out,
	 * and ones checked out by the specified user. Use getDocumentListFull() for
	 * retrieving a comprehensive list of documents available, regardless of
	 * their checkout state.
	 * @param userName the user to retrieve the list for (used for filtering
	 *            based on document's checkout states, has no effect if the
	 *            specified user has administrative privileges)
	 * @return a list of meta data for the document available through this DIO
	 */
	public DioDocumentList getDocumentList(String userName) {
		return this.getDocumentList(userName, false, null);
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this DIO.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. If the
	 * specified user has administrative privileges, checked-out documents are
	 * included in the list, and the list also provides the checkout user and
	 * checkout status of every document. Otherwise, the list only includes
	 * documents that can be checked out, i.e., ones that are not checked out,
	 * and ones checked out by the specified user. Use getDocumentListFull() for
	 * retrieving a comprehensive list of documents available, regardless of
	 * their checkout state.
	 * @param userName the user to retrieve the list for (used for filtering
	 *            based on document's checkout states, has no effect if the
	 *            specified user has administrative privileges)
	 * @param headOnly if set to true, this method returns an empty list, only
	 *            containing the header data (field names and attribute value
	 *            summaries)
	 * @return a list of meta data for the document available through this DIO
	 */
	public DioDocumentList getDocumentList(String userName, boolean headOnly) {
		return this.getDocumentList(userName, headOnly, null);
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this DIO.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @return a list of meta data for the document available through this DIO
	 */
	public DioDocumentList getDocumentList(Properties filter) {
		return this.getDocumentList("", false, filter);
	}

	/**
	 * Retrieve a list of meta data for the document available through this DIO.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. The
	 * list only includes documents that can be checked out, i.e., ones that are
	 * not checked out. Use getDocumentListFull() for retrieving a comprehensive
	 * list of documents available, regardless of their checkout state.
	 * @param headOnly if set to true, this method returns an empty list, only
	 *            containing the header data (field names and attribute value
	 *            summaries)
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @return a list of meta data for the document available through this DIO
	 */
	public DioDocumentList getDocumentList(boolean headOnly, Properties filter) {
		return this.getDocumentList("", false, filter);
	}
	
	/**
	 * Retrieve a list of meta data for the document available through this DIO.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. If the
	 * specified user has administrative privileges, checked-out documents are
	 * included in the list, and the list also provides the checkout user and
	 * checkout status of every document. Otherwise, the list only includes
	 * documents that can be checked out, i.e., ones that are not checked out,
	 * and ones checked out by the specified user. Use getDocumentListFull() for
	 * retrieving a comprehensive list of documents available, regardless of
	 * their checkout state.
	 * @param userName the user to retrieve the list for (used for filtering
	 *            based on document's checkout states, has no effect if the
	 *            specified user has administrative privileges)
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @return a list of meta data for the document available through this DIO
	 */
	public DioDocumentList getDocumentList(String userName, Properties filter) {
		return this.getDocumentList(userName, false, filter);
	}
	
	/* TODO
reduce document list loading effort:
- for non-admin users, only transfer document list head with input suggestions for filters
- embed list size in header
  - facilitates displaying list loading progress
  - facilitates detailed message for selectivity of filter

in the long haul, ask DIO extensions to provide SQL snippets and column names to facilitate database joins
- use lower-cased extension component letter code as table alias in SQL query
- add get<Xyz>WherePredicates() --> String[] methods to DIO extension interface, one for each IO method
  - keep current way as fallback (some extensions might not use database ...)
  - convention: main table is named 'dio', extension table is named '<lower-cased extension letter code>'
- add get<Xyz>Columns() --> String[] methods to DIO extension interface, one for each IO method
  - column names may include 'AS <frontend name>' parts
  --> only use for columns part of query
- add ID equality predicate automatically if predicates or column names non-empty
  - convention: extensions must have docId in their table, preferably indexed to facilitate index joins
- 
	 */
	
	/**
	 * Retrieve a list of meta data for the document available through this DIO.
	 * The list includes the document ID, document name, checkin user, checkin
	 * time, last update user, last update time, and most recent version. If the
	 * specified user has administrative privileges, checked-out documents are
	 * included in the list, and the list also provides the checkout user and
	 * checkout status of every document. Otherwise, the list only includes
	 * documents that can be checked out, i.e., ones that are not checked out,
	 * and ones checked out by the specified user. Use getDocumentListFull() for
	 * retrieving a comprehensive list of documents available, regardless of
	 * their checkout state.
	 * @param userName the user to retrieve the list for (used for filtering
	 *            based on document's checkout states, has no effect if the
	 *            specified user has administrative privileges)
	 * @param headOnly if set to true, this method returns an empty list, only
	 *            containing the header data (field names and attribute value
	 *            summaries)
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @return a list of meta data for the document available through this DIO
	 */
	public DioDocumentList getDocumentList(String userName, boolean headOnly, Properties filter) {
		
		// get user status
		boolean isAdmin = this.uaa.isAdmin(userName);
		
		//	reduce filter
		if (filter != null) {
			Properties rFilter = new Properties();
			for (Iterator fit = filter.keySet().iterator(); fit.hasNext();) {
				String filterName = ((String) fit.next());
				if (!filterableAttributes.contains(filterName))
					continue;
				String filterValue = filter.getProperty(filterName, "").trim();
				if (filterValue.length() == 0)
					continue;
				
				String[] filterValues = filterValue.split("[\\r\\n]++");
				if (filterValues.length == 1) {
					if (numericAttributes.contains(filterName)) {
						if (filterValues[0].matches("[0-9]++")) {
							rFilter.setProperty(filterName, filterValue);
							String customOperator = filter.getProperty((filterName + "Operator"));
							if ((customOperator != null) && numericOperators.contains(customOperator))
								rFilter.setProperty((filterName + "Operator"), customOperator);
							else rFilter.setProperty((filterName + "Operator"), ">");
						}
					}
					else rFilter.setProperty(filterName, filterValue);
				}
				else if (filterValues.length > 1)
					rFilter.setProperty(filterName, filterValue);
			}
			filter = (rFilter.isEmpty() ? null : rFilter);
		}
		
		// estimate list size
		final int selectivity = this.getSelectivity(filter);
//		if (filter != null) {
//			DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//			for (int e = 0; e < dies.length; e++)
//				selectivity = Math.min(selectivity, dies[e].getSelectivity(selectivity, filter, userName));
//		}
		
		// collect field names
		StringVector fieldNames = new StringVector();
		fieldNames.addContent(documentListFields);
		if (isAdmin)
			fieldNames.addContent(documentListFieldsAdmin);
		
		// head only, or list too large for regular user, return empty list
		if ((headOnly) || (!isAdmin && !DOCUMENT_SERVLET_SESSION_ID.equals(userName) && (0 < this.documentListSizeThreshold) && (this.documentListSizeThreshold < selectivity))) {
			DioDocumentList dl = new DioDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public DocumentListElement getNextDocument() {
					return null;
				}
				public AttributeSummary getListFieldValues(String listFieldName) {
					return GoldenGateDIO.this.getListFieldSummary(listFieldName, true);
				}
			};
//			
//			DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//			for (int e = 0; e < dies.length; e++) {
//				DocumentList edl = dies[e].extendList(dl, userName, true, filter);
//				if (edl != null)
//					dl = edl;
//			}
			
			return dl;
		}
		
		// assemble query
		String query = "SELECT " + fieldNames.concatStrings(", ") + 
				" FROM " + DOCUMENT_TABLE_NAME +
				" WHERE " + this.getDocumentFilter(filter) +
				// filter out documents checked out by another user (if not admin)
				(isAdmin ? "" : (" AND ((" + CHECKOUT_TIME_ATTRIBUTE + " = -1) OR (" + CHECKOUT_USER_ATTRIBUTE + " LIKE '" + EasyIO.prepareForLIKE(userName) + "'))")) +
				" ORDER BY " + DOCUMENT_NAME_ATTRIBUTE + 
				";";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			
			// return SQL backed list
			final SqlQueryResult finalSqr = sqr;
			DioDocumentList dl = new DioDocumentList(fieldNames.toStringArray()) {
				SqlQueryResult sqr = finalSqr;
				DocumentListElement next = null;
				public boolean hasNextDocument() {
					if (this.next != null) return true;
					else if (this.sqr == null) return false;
					else if (this.sqr.next()) {
						this.next = new DocumentListElement();
						for (int f = 0; f < this.listFieldNames.length; f++)
							this.next.setAttribute(this.listFieldNames[f], this.sqr.getString(f));
						this.addListFieldValues(this.next);
						return true;
					}
					else {
						this.sqr.close();
						this.sqr = null;
						return false;
					}
				}
				public DocumentListElement getNextDocument() {
					if (!this.hasNextDocument()) return null;
					DocumentListElement next = this.next;
					this.next = null;
					return next;
				}
				public int getDocumentCount() {
					return selectivity;
				}
			};
//			
//			//	check extensions
//			DocumentIoExtension[] dies = this.getDocumentIoExtensions();
//			for (int e = 0; e < dies.length; e++) {
//				DocumentList edl = dies[e].extendList(dl, userName, false, filter);
//				if (edl != null)
//					dl = edl;
//			}
			
			//	return final wrapped list
			return dl;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
			this.logError("  query was " + query);

			// return dummy list
			return new DioDocumentList(fieldNames.toStringArray()) {
				public boolean hasNextDocument() {
					return false;
				}
				public DocumentListElement getNextDocument() {
					return null;
				}
				public int getDocumentCount() {
					return 0;
				}
			};
		}
	}
	
	private int getSelectivity(Properties filter) {
		if ((filter == null) || filter.isEmpty())
			return this.docIdSet.size();
		
		if (filter.size() == 1) {
			String filterField = ((String) filter.keySet().iterator().next());
			String filterValue = filter.getProperty(filterField);
			if (this.docAttributeValueCache.containsKey(filterField))
				return this.getListFieldSummary(filterField, false).getCount(filterValue);
		}
		
		String predicate = this.getDocumentFilter(filter);
		if ("1=1".equals(predicate))
			return this.docIdSet.size();
		
		String query = "SELECT count(*) FROM " + DOCUMENT_TABLE_NAME + " WHERE " + predicate;
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			return (sqr.next() ? Integer.parseInt(sqr.getString(0)) : 0);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIO: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting document list size.");
			this.logError("  query was " + query);
			return Integer.MAX_VALUE;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	private String getDocumentFilter(Properties filter) {
		if ((filter == null) || filter.isEmpty())
			return "1=1";
		
		StringBuffer whereString = new StringBuffer("1=1");
		for (Iterator fit = filter.keySet().iterator(); fit.hasNext();) {
			String filterName = ((String) fit.next());
			if (!filterableAttributes.contains(filterName))
				continue;
			
			String filterValue = filter.getProperty(filterName, "").trim();
			if (filterValue.length() == 0)
				continue;
			
			String[] filterValues = filterValue.split("[\\r\\n]++");
			if (filterValues.length == 1) {
				if (numericAttributes.contains(filterName)) {
					
					//	this should prevent SQL injection, as numeric fields are the only ones whose value is not auto-escaped
					if (filterValues[0].matches("[0-9]++")) {
						String customOperator = filter.getProperty((filterName + "Operator"), ">");
						whereString.append(" AND " + filterName + " " + (numericOperators.contains(customOperator) ? customOperator : ">") + " " + filterValues[0]);
					}
				}
				else whereString.append(" AND " + filterName + " LIKE '" + EasyIO.prepareForLIKE(filterValues[0]) + "'");
			}
			else if (filterValues.length > 1) {
				whereString.append(" AND (");
				for (int v = 0; v < filterValues.length; v++) {
					filterValue = filterValues[v].trim();
					if (v != 0)
						whereString.append(" OR ");
					whereString.append(filterName + " LIKE '" + EasyIO.prepareForLIKE(filterValues[v]) + "'");
				}
				whereString.append(")");
			}
		}
		return whereString.toString();
	}
	
	private String getDocumentKeywordString(String docId) {
		try {
			return this.getDocumentKeywordString(this.getDocument(docId));
		}
		catch (IOException ioe) {
			return "|";
		}
	}
	
	private String getDocumentKeywordString(QueriableAnnotation doc) {
		Iterator kit = this.getDocumentKeywords(doc).iterator();
		StringBuffer keywords = new StringBuffer("|");
		while (kit.hasNext()) {
			String keyword = ((String) kit.next());
			if ((keywords.length() + keyword.length() + 1) < DOCUMENT_KEYWORDS_COLUMN_LENGTH)
				keywords.append(keyword + "|");
			else break;
		}
		return keywords.toString();
	}
	
	private DocumentKeywordSet getDocumentKeywords(QueriableAnnotation doc) {
		DocumentKeywordSet dks = new DocumentKeywordSet();
		for (int k = 0; k < this.documentKeyworders.length; k++)
			dks.addAll(this.documentKeyworders[k].getDocumentKeywords(doc), false);
		return dks;
	}
	
	private DocumentKeyworder[] documentKeyworders = new DocumentKeyworder[0];
	private Dictionary stopWords = null;
	
	private static abstract class DocumentKeyworder {
		static final String NO_NORMALIZATION = "none";
		static final String LN_DOC_LENGTH_NORMALIZATION = "lnDocLength";
		static final String SQRT_DOC_LENGTH_NORMALIZATION = "sqrtDocLength";
		static final String DOC_LENGTH_NORMALIZATION = "docLength";
		String normalizationMode;
		int weightBoostFactor;
		DocumentKeyworder(String normalizationMode, int weightBoostFactor) {
			this.normalizationMode = ((normalizationMode == null) ? LN_DOC_LENGTH_NORMALIZATION : normalizationMode);
			this.weightBoostFactor = weightBoostFactor;
		}
		abstract DocumentKeywordSet getDocumentKeywords(QueriableAnnotation doc);
	}
	private static class ListKeyworder extends DocumentKeyworder {
		Dictionary list;
		ListKeyworder(String normalizationMode, int weightBoostFactor, Dictionary list) {
			super(normalizationMode, weightBoostFactor);
			this.list = list;
		}
		DocumentKeywordSet getDocumentKeywords(QueriableAnnotation doc) {
			DocumentKeywordSet dks = new DocumentKeywordSet();
			Annotation[] keywords = Gamta.extractAllContained(doc, this.list);
			for (int a = 0; a < keywords.length; a++)
				dks.add(keywords[a].getValue(), this.weightBoostFactor, true);
			return dks;
		}
	}
	private static class PatternKeyworder extends DocumentKeyworder {
		String pattern;
		Dictionary stopWords;
		PatternKeyworder(String normalizationMode, int weightBoostFactor, String pattern, Dictionary stopWords) {
			super(normalizationMode, weightBoostFactor);
			this.pattern = pattern;
			this.stopWords = stopWords;
		}
		DocumentKeywordSet getDocumentKeywords(QueriableAnnotation doc) {
			DocumentKeywordSet dks = new DocumentKeywordSet();
			Annotation[] keywords = Gamta.extractAllMatches(doc, this.pattern, this.stopWords, this.stopWords);
			for (int a = 0; a < keywords.length; a++)
				dks.add(keywords[a].getValue(), this.weightBoostFactor, true);
			return dks;
		}
	}
	private static class GPathKeyworder extends DocumentKeyworder {
		GPath path;
		GPathKeyworder(String normalizationMode, int weightBoostFactor, GPath path) {
			super(normalizationMode, weightBoostFactor);
			this.path = path;
		}
		DocumentKeywordSet getDocumentKeywords(QueriableAnnotation doc) {
			DocumentKeywordSet dks = new DocumentKeywordSet();
			Annotation[] keywords = this.path.evaluate(doc, null);
			for (int a = 0; a < keywords.length; a++)
				dks.add(keywords[a].getValue(), this.weightBoostFactor, true);
			return dks;
		}
	}
	
	/*
	 * A document keyword set represents a list of keywords in which every entry
	 * has a weight assigned to it. The iterators of a document keyword set
	 * return the keywords in order of decreasing weight, i.e., most important
	 * keywords first.
	 * 
	 * @author sautter
	 */
	private static class DocumentKeywordSet {
		
		private static final Comparator lengthFirstComparator = new Comparator() {
			public int compare(Object o1, Object o2) {
				String s1 = ((String) o1);
				String s2 = ((String) o2);
				int c = s1.length() - s2.length();
				return ((c == 0) ? s1.compareTo(s2) : c);
			}
		};
		
		private final Comparator keywordComparator = new Comparator() {
			public int compare(Object o1, Object o2) {
				String s1 = ((String) o1);
				String s2 = ((String) o2);
				int w1 = getWeight(s1);
				int w2 = getWeight(s2);
				return ((w2 == w1) ? lengthFirstComparator.compare(s1, s2) : (w2 - w1));
			}
		};
		
		private TreeMap keywords = new TreeMap();
		private TreeSet keywordsSorted = null;
		
		private int getWeight(String string) {
			Integer w = ((Integer) this.keywords.get(string));
			return ((w == null) ? 0 : w.intValue());
		}
		
		/*
		 * Add a keyword with a specific weight to the keyword set. If the
		 * specified keyword is already contained in the set, the higher wight
		 * is kept.
		 * @param keyword the keyword to add
		 * @param weight the keyword's weight
		 */
		synchronized void add(String keyword, int weight, boolean aggregate) {
			int w = this.getWeight(keyword);
			if (aggregate) {
				this.keywordsSorted = null;
				this.keywords.put(keyword, new Integer(w + weight));
			}
			else if (w < weight) {
				this.keywordsSorted = null;
				this.keywords.put(keyword, new Integer(weight));
			}
		}
		
		/*
		 * Retrieve the size of the keyword set, i.e., the number of keywords it
		 * contains.
		 * @return the current size of the keyword set
		 */
		int size() {
			return this.keywords.size();
		}
		
		/*
		 * Add the content of another document keyword set to this one.
		 * @param dks the document keyword set to add
		 */
		synchronized void addAll(DocumentKeywordSet dks, boolean aggregate) {
			for (Iterator kit = dks.keywords.keySet().iterator(); kit.hasNext();) {
				String keyword = ((String) kit.next());
				this.add(keyword, dks.getWeight(keyword), aggregate);
			}
		}
		
		/*
		 * Retrieve an iterator that returns the keywords in decreasing weight
		 * order, most important keywords first. The objects returned by the
		 * iterator's next() method are strings. The iterator does not support
		 * removal, and it will not reflect any changes to the keyword set made
		 * after it was retrieved.
		 * @return an iterator that returns the keywords in decreasing weight
		 *         order
		 */
		synchronized Iterator iterator() {
			if (this.keywordsSorted == null) {
				this.keywordsSorted = new TreeSet(this.keywordComparator);
				this.keywordsSorted.addAll(this.keywords.keySet());
			}
			final Iterator kit = this.keywordsSorted.iterator();
			return new Iterator() {
				public boolean hasNext() {
					return kit.hasNext();
				}
				public Object next() {
					return ((String) kit.next());
				}
				public void remove() {}
			};
		}
	}
}