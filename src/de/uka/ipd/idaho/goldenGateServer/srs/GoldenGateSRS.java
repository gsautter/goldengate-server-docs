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


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.AttributeUtils;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest.AttributeFilter;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest.TypeFilter;
import de.uka.ipd.idaho.gamta.util.AnnotationFilter;
import de.uka.ipd.idaho.gamta.util.CountingSet;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.ComponentInitializer;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.AsynchronousWorkQueue;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerActivityLogger;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerEventService;
import de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore;
import de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore.DocumentNotFoundException;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants.SrsDocumentEvent.SrsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousConsoleAction;
import de.uka.ipd.idaho.goldenGateServer.util.IdentifierKeyedDataObjectStore;
import de.uka.ipd.idaho.goldenGateServer.util.IdentifierKeyedDataObjectStore.DataObjectInputStream;
import de.uka.ipd.idaho.goldenGateServer.util.IdentifierKeyedDataObjectStore.DataObjectNotFoundException;
import de.uka.ipd.idaho.goldenGateServer.util.IdentifierKeyedDataObjectStore.DataObjectOutputStream;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * The GoldenGATE Search & Retrieval Server is intended for flexibly indexing
 * and searching a collection of XML documents stored in a GoldenGATE Document
 * Storage Server.
 * 
 * @author sautter
 */
public class GoldenGateSRS extends AbstractGoldenGateServerComponent implements GoldenGateSrsConstants {
	private static final String ANNOTATION_NESTING_ORDER_SETTING = "ANNOTATION_NESTING_ORDER";
	
	private static final String MASTER_DOCUMENT_ID_HASH_COLUMN_NAME = "masterDocIdHash";
	private static final String DOCUMENT_UUID_HASH_COLUMN_NAME = "docUuidHash";
	
	private Indexer[] indexers = new Indexer[0];
	private IndexerServiceThread indexerService = null;
	private AsynchronousWorkQueue indexerServiceMonitor = null;
	private IdentifierKeyedDataObjectStore indexDataStore;
	private CountingSet queriedIndexerFields = new CountingSet(new TreeMap(String.CASE_INSENSITIVE_ORDER));
	private Set sQueriedIndexerFields = Collections.synchronizedSet(this.queriedIndexerFields);
	private long indexersQueriedSince = System.currentTimeMillis(); // simply remember startup time
	private long indexersQueriedLastLogged = System.currentTimeMillis(); // simply remember startup time
	
	private StorageFilter[] filters = new StorageFilter[0];
	private DocumentSplitter[] splitters = new DocumentSplitter[0];
	private DocumentUuidFetcher[] uuidFetchers = new DocumentUuidFetcher[0];
	
	private IoProvider io;
	
	private int activityLogTimeout = 5000;
	
	/**
	 * The name of the SRS's document data table in the backing database. This
	 * constant is public in order to allow other components to perform joins
	 * with the SRS's data. It is NOT intended for external modification.
	 */
	public static final String DOCUMENT_TABLE_NAME = "GgSrsDocuments";
	
	private static final String DOCUMENT_CHECKSUM_COLUMN_NAME = "DocCheckSum";
	private static final int USER_LENGTH = 32;
	private static final int MASTER_DOCUMENT_TITLE_COLUMN_LENGTH = 192;
	private static final int DOCUMENT_TITLE_COLUMN_LENGTH = 192;
	private static final int DOCUMENT_AUTHOR_COLUMN_LENGTH = 128;
	private static final int DOCUMENT_ORIGIN_COLUMN_LENGTH = 192;
	private static final int DOCUMENT_SOURCE_LINK_COLUMN_LENGTH = 192;
	private static final String DOCUMENT_SIZE_ATTRIBUTE = "docSize";
	
	private AnnotationChecksumDigest checksumDigest;
	
	private DocumentStore dst;
	private IdentifierKeyedDataObjectStore dstCache; // TODO put latest version here, on cpool
	private long dstLastModified = -1;
	
	private HashMap documentListExtensions = new LinkedHashMap();
	
	/** Constructor passing 'SRS' as the letter code to super constructor
	 */
	public GoldenGateSRS() {
		super("SRS");
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateScf.AbstractServerComponent#initComponent()
	 */
	protected void initComponent() {
		System.out.println("GoldenGateSRS: starting up ...");
		
		//	get and check database connection
		this.io = this.host.getIoProvider();
		if (!this.io.isJdbcAvailable())
			throw new RuntimeException("GoldenGATE SRS cannot work without database access.");
		
		//	ensure document table
		TableDefinition td = new TableDefinition(DOCUMENT_TABLE_NAME);
		td.addColumn(Indexer.DOC_NUMBER_COLUMN);
		td.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DOCUMENT_TYPE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DOCUMENT_CHECKSUM_COLUMN_NAME, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DOCUMENT_UUID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DOCUMENT_UUID_HASH_COLUMN_NAME, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(DOCUMENT_UUID_SOURCE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(MASTER_DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(MASTER_DOCUMENT_ID_HASH_COLUMN_NAME, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(CHECKIN_USER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, USER_LENGTH);
		td.addColumn(CHECKIN_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		td.addColumn(UPDATE_USER_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, USER_LENGTH);
		td.addColumn(UPDATE_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		td.addColumn(MASTER_DOCUMENT_TITLE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, MASTER_DOCUMENT_TITLE_COLUMN_LENGTH);
		td.addColumn(DOCUMENT_TITLE_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_TITLE_COLUMN_LENGTH);
		td.addColumn(DOCUMENT_AUTHOR_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_AUTHOR_COLUMN_LENGTH);
		td.addColumn(DOCUMENT_ORIGIN_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_ORIGIN_COLUMN_LENGTH);
		td.addColumn(DOCUMENT_DATE_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(DOCUMENT_SOURCE_LINK_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_SOURCE_LINK_COLUMN_LENGTH);
		td.addColumn(PAGE_NUMBER_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(LAST_PAGE_NUMBER_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(MASTER_PAGE_NUMBER_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(MASTER_LAST_PAGE_NUMBER_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		td.addColumn(DOCUMENT_SIZE_ATTRIBUTE, TableDefinition.INT_DATATYPE, 0);
		if (!this.io.ensureTable(td, true))
			throw new RuntimeException("GoldenGATE SRS cannot work without database access.");
		
		//	index document table
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOC_NUMBER_COLUMN_NAME);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_UUID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_UUID_HASH_COLUMN_NAME);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, MASTER_DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, MASTER_DOCUMENT_ID_HASH_COLUMN_NAME);
		
		//	report status
		System.out.println("  - database connection established");
		
		//	set up checksum digest, filtering provenance attributes
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
		
		//	initialize document store
		this.dst = new DocumentStore("SrsDocuments", docFolder, this.configuration.getSetting("documentEncoding"), this);
		System.out.println("  - got document store");
		
		//	get document storage folder
		String docCacheFolderName = this.configuration.getSetting("documentCacheFolderName");
		if (docCacheFolderName != null) {
			while (docCacheFolderName.startsWith("./"))
				docCacheFolderName = docCacheFolderName.substring("./".length());
			File docCacheFolder = (((docCacheFolderName.indexOf(":\\") == -1) && (docCacheFolderName.indexOf(":/") == -1) && !docCacheFolderName.startsWith("/")) ? new File(this.dataPath, docCacheFolderName) : new File(docCacheFolderName));
			
			//	initialize document store
			this.dstCache = new IdentifierKeyedDataObjectStore("SrsDocumentCache", docCacheFolder, ".xml", false, this);
			System.out.println("  - got document cache");
		}
		
		
		//	load, initialize and sort plugins
		File pluginFolder = new File(this.dataPath, "Plugins");
		GoldenGateSrsPlugin[] plugins = this.createPlugins(pluginFolder);
		ArrayList indexers = new ArrayList();
		ArrayList filters = new ArrayList();
		ArrayList splitters = new ArrayList();
		ArrayList uuidFetchers = new ArrayList();
		for (int p = 0; p < plugins.length; p++) try {
			plugins[p].init();
			if (plugins[p] instanceof Indexer)
				indexers.add((Indexer) plugins[p]);
			if (plugins[p] instanceof StorageFilter)
				filters.add((StorageFilter) plugins[p]);
			if (plugins[p] instanceof DocumentSplitter)
				splitters.add((DocumentSplitter) plugins[p]);
			if (plugins[p] instanceof DocumentUuidFetcher)
				uuidFetchers.add((DocumentUuidFetcher) plugins[p]);
		}
		catch (Throwable t) {
			System.out.println("Error initializing plugin " + plugins[p].getClass().getName());
			t.printStackTrace(System.out);
		}
		this.indexers = ((Indexer[]) indexers.toArray(new Indexer[indexers.size()]));
		this.filters = ((StorageFilter[]) filters.toArray(new StorageFilter[filters.size()]));
		this.splitters = ((DocumentSplitter[]) splitters.toArray(new DocumentSplitter[splitters.size()]));
		this.uuidFetchers = ((DocumentUuidFetcher[]) uuidFetchers.toArray(new DocumentUuidFetcher[uuidFetchers.size()]));
		System.out.println("  - plugins initialized");
		
		//	get index data cache folder
		String indexDataFolderName = this.configuration.getSetting("indexDataFolderName", "IndexData");
		while (indexDataFolderName.startsWith("./"))
			indexDataFolderName = indexDataFolderName.substring("./".length());
		File indexDataFolder = (((indexDataFolderName.indexOf(":\\") == -1) && (indexDataFolderName.indexOf(":/") == -1) && !indexDataFolderName.startsWith("/")) ? new File(this.dataPath, indexDataFolderName) : new File(indexDataFolderName));
		
		//	initialize index data store
		this.indexDataStore = new IdentifierKeyedDataObjectStore("SrsIndexData", indexDataFolder, ".xml", false, this);
		System.out.println("  - got index data cache");
		
		//	start indexer service thread
		synchronized (this.indexerActionQueue) {
			this.indexerService = new IndexerServiceThread();
			this.indexerService.start();
			try {
				this.indexerActionQueue.wait();
			} catch (InterruptedException ie) {}
			this.indexerServiceMonitor = new AsynchronousWorkQueue("SrsIndexerService") {
				public String getStatus() {
					return (this.name + " indexing actions: " + allIndexerActionStats.toString());
				}
			};
		}
		System.out.println("  - indexer service started");
		
		
		//	read search configuration
		String mergeMode = this.configuration.getSetting(RESULT_MERGE_MODE_SETTING_NAME);
		if (USE_MIN_MERGE_MODE_NAME.equals(mergeMode)) this.resultMergeMode = QueryResult.USE_MIN;
		else if (USE_AVERAGE_MERGE_MODE_NAME.equals(mergeMode)) this.resultMergeMode = QueryResult.USE_AVERAGE;
		else if (USE_MAX_MERGE_MODE_NAME.equals(mergeMode)) this.resultMergeMode = QueryResult.USE_MAX;
		else if (MULTIPLY_MERGE_MODE_NAME.equals(mergeMode)) this.resultMergeMode = QueryResult.MULTIPLY;
		else if (INVERSE_MULTIPLY_MERGE_MODE_NAME.equals(mergeMode)) this.resultMergeMode = QueryResult.INVERSE_MULTIPLY;
		else { // USE_AVERAGE is default
			this.resultMergeMode = QueryResult.USE_AVERAGE;
			this.configuration.setSetting(RESULT_MERGE_MODE_SETTING_NAME, DEFAULT_RESULT_MERRGE_MODE_NAME);
		}
		
		String resultPivot = this.configuration.getSetting(RESULT_PIVOT_INDEX_SETTING_NAME);
		try {
			this.resultPivotIndex = Integer.parseInt(resultPivot);
		}
		catch (Exception e) {
			this.configuration.setSetting(RESULT_PIVOT_INDEX_SETTING_NAME, ("" + DEFAULT_RESULT_PIVOT_INDEX));
		}
		System.out.println("  - query configuration read");
		
		
		//	set annotation nesting order for data model
		Gamta.setAnnotationNestingOrder(this.configuration.getSetting(ANNOTATION_NESTING_ORDER_SETTING, Gamta.getAnnotationNestingOrder()));
		System.out.println("  - annotation nesting order set");
		
		
		//	read document list extensions
		try {
			StringVector docListExtensions = StringVector.loadList(new File(this.dataPath, "DocumentListExtensions.cnfg"));
			for (int e = 0; e < docListExtensions.size(); e++) {
				String docListExtension = docListExtensions.get(e).trim();
				
				//	has to be of the form "<documentListFieldName> = <indexName>.<indexFieldName>"
				if (docListExtension.matches("[a-zA-Z\\_]++\\s*+\\=\\s*+[a-zA-Z\\_]++\\.[a-zA-Z\\_]++")) {
					
					//	parse it
					String docListFieldName = docListExtension.substring(0, docListExtension.indexOf('=')).trim();
					docListExtension = docListExtension.substring(docListExtension.indexOf('=') + 1).trim();
					String indexName = docListExtension.substring(0, docListExtension.indexOf('.')).trim();
					docListExtension = docListExtension.substring(docListExtension.indexOf('.') + 1).trim();
					String indexFieldName = docListExtension;
					
					//	check if index exists
					boolean indexOk = false;
					for (int i = 0; i < this.indexers.length; i++)
						if (this.indexers[i].getIndexName().equals(indexName)) {
							indexOk = true;
							i = this.indexers.length;
						}
					
					//	if so, store data
					if (indexOk) {
						String[] docListExtensionData = {indexName, indexFieldName};
						this.documentListExtensions.put(docListFieldName, docListExtensionData);
					}
				}
			}
		}
		catch (IOException ioe) {
			System.out.println("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading doc list extensions.");
			ioe.printStackTrace(System.out);
		}
		
		
		//	create re-index action
		this.reIndexAction = new AsynchronousConsoleAction(REINDEX_COMMAND, "Re-index the document collection managed by this GoldenGATE SRS", "index", this.dataPath, "SrsIndexUpdateLog") {
			protected String[] getArgumentNames() {
				String[] argumentNames = {"indexerName"};
				return argumentNames;
			}
			protected String[] getArgumentExplanation(String argument) {
				if ("indexerName".equals(argument)) {
					String[] explanation = {
							"The name or comma separated list of names of the indexer(s) to update (optional parameter, if omitted, all indices will be updated)"
						};
					return explanation;
				}
				else return super.getArgumentExplanation(argument);
			}
			protected String checkArguments(String[] arguments) {
				if (arguments.length < 2)
					return null;
				else return ("Specify no arguments, or a single comma separated list of indexer names.");
			}
			protected String getActionName() {
				return (getLetterCode() + "." + super.getActionName());
			}
			protected void performAction(String[] arguments) throws Exception {
				
				//	read names of indices to rebuild
				Set reIndexNameSet = new HashSet();
				if (arguments.length == 0) {
					for (int i = 0; i < GoldenGateSRS.this.indexers.length; i++)
						reIndexNameSet.add(GoldenGateSRS.this.indexers[i].getIndexName());
					reIndexNameSet.add("doc");
				}
				else {
					String [] reIndexNames = arguments[0].split("\\,");
					for (int i = 0; i < reIndexNames.length; i++)
						reIndexNameSet.add(reIndexNames[i].trim());
				}
				
				//	start log
				this.log("GoldenGateSRS: start re-indexing document collection ...");
				
				//	shut down indexer service
				indexerService.shutdown();
				this.log("  - index update service stopped");
				
				//	process collection
				DocumentNumberResolver docNumberResolver = getDocumentNumberResolver();
				Indexer[] indexers = GoldenGateSRS.this.indexers;
				try {
					this.enteringMainLoop("0 of " + docNumberResolver.size() + " documents done");
					
					//	process documents
					int updateDocCount = 0;
					for (int d = 0; d < docNumberResolver.size(); d++) {
						
						//	read document number and ID
						long docNr = docNumberResolver.documentNumberAt(d);
						String docId = docNumberResolver.documentIdAt(d);
						this.log("  - processing document " + docNr);
						try {
							QueriableAnnotation doc = null;
							
							//	run document through indexers
							IndexData indexData = getIndexData(docNr, docId);
							for (int i = 0; i < indexers.length; i++) {
								if (!reIndexNameSet.contains(indexers[i].getIndexName()))
									continue;
								if (doc == null) {
									doc = getDocument(docId);
									this.log("    - document loaded");
								}
								this.log("    - doing indexer " + indexers[i].getClass().getName());
								
//								indexers[i].deleteDocument(docNr);
//								this.log("      - document un-indexed");
//								IndexResult ir = indexers[i].index(doc, docNr);
//								this.log("      - document re-indexed");
								IndexResult ir = indexers[i].reIndex(doc, docNr);
								this.log("      - document re-indexed");
								
								if ((ir == null) || !ir.hasNextElement())
									indexData.removeIndexResult(indexers[i].getIndexName());
								else indexData.addIndexResult(ir);
							}
							if (reIndexNameSet.contains("doc")) {
								Properties docAttributes = getDocumentAttributes(docId);
								if (docAttributes != null)
									setIndexDataDocAttributes(indexData, docAttributes);
							}
							this.log("    - document done");
							storeIndexData(indexData);
						}
						catch (Exception e) {
							this.log(("GoldenGateSRS: " + e.getClass().getName() + " (" + e.getMessage() + ") while re-indexing document " + docId), e);
						}
						catch (Error e) {
							this.log(("GoldenGateSRS: " + e.getClass().getName() + " (" + e.getMessage() + ") while re-indexing document " + docId), new Exception(e));
						}
						
						//	update status info
						this.loopRoundComplete((++updateDocCount) + " of " + docNumberResolver.size() + " documents done");
					}
				}
				finally {
					synchronized (indexerActionQueue) {
						indexerService = new IndexerServiceThread();
						indexerService.start();
						try {
							indexerActionQueue.wait();
						} catch (InterruptedException ie) {}
					}
					this.log("  - index update service restarted");
				}
			}
		};
		
		//	create index check action
		this.checkIndexAction = new AsynchronousConsoleAction(CHECK_INDEX_COMMAND, "Check cached index data for the document collection managed by this GoldenGATE SRS", "index", this.dataPath, "SrsIndexCheckLog") {
			protected String[] getArgumentNames() {
				String[] argumentNames = {"checkMode"};
				return argumentNames;
			}
			protected String[] getArgumentExplanation(String argument) {
				if ("checkMode".equals(argument)) {
					String[] explanation = {
							"The check mode, 'checkEmpty' to check for general presence of sub results, 'checkAll' to check each individual sub result, 'repairEmpty' to trigger re-indexing in absence of all sub results, 'repairAll' to trigger re-indexing for all individual missing sub results (optional parameter, defaults to 'shallow' if omitted)"
						};
					return explanation;
				}
				else return super.getArgumentExplanation(argument);
			}
			protected String checkArguments(String[] arguments) {
				if (arguments.length < 2)
					return null;
				else return ("Specify no arguments, or at most the check mode.");
			}
			protected String getActionName() {
				return (getLetterCode() + "." + super.getActionName());
			}
			protected void performAction(String[] arguments) throws Exception {
				
				//	get mode
				boolean shallowMode;
				boolean cacheOnlyMode;
				if (arguments.length == 0) {
					shallowMode = true;
					cacheOnlyMode = true;
				}
				else if ("checkAll".equals(arguments[0])) {
					shallowMode = false;
					cacheOnlyMode = true;
				}
				else if ("repairEmpty".equals(arguments[0])) {
					shallowMode = true;
					cacheOnlyMode = false;
				}
				else if ("repairAll".equals(arguments[0])) {
					shallowMode = false;
					cacheOnlyMode = false;
				}
				else {
					shallowMode = true;
					cacheOnlyMode = true;
				}
				
				//	start log
				this.log("GoldenGateSRS: start checking cached index data of document collection ...");
				
				//	process collection
				DocumentNumberResolver docNumberResolver = getDocumentNumberResolver();
				this.enteringMainLoop("0 of " + docNumberResolver.size() + " documents done");
				
				//	process documents
				int updateDocCount = 0;
				Indexer[] indexers = GoldenGateSRS.this.indexers;
				for (int d = 0; d < docNumberResolver.size(); d++) {
					long docNr = docNumberResolver.documentNumberAt(d);
					String docId = docNumberResolver.documentIdAt(d);
					this.log("  - processing document " + docNr + " (" + docId + ")");
					try {
						this.checkIndexData(docNr, docId, indexers, shallowMode, cacheOnlyMode);
						this.log("    - document done");
					}
					catch (Exception e) {
						this.log(("GoldenGateSRS: " + e.getClass().getName() + " (" + e.getMessage() + ") while re-indexing document " + docId), e);
					}
					catch (Error e) {
						this.log(("GoldenGateSRS: " + e.getClass().getName() + " (" + e.getMessage() + ") while re-indexing document " + docId), new Exception(e));
					}
					
					//	update status info
					this.loopRoundComplete((++updateDocCount) + " of " + docNumberResolver.size() + " documents done");
				}
			}
			private void checkIndexData(final long docNr, final String docId, Indexer[] indexers, boolean shallowMode, boolean cacheOnlyMode) {
				
				//	get existing index data (also makes sure document attributes are present)
				IndexData indexData = getIndexData(docNr, null);
				
				//	no index data at all, create it
				if (indexData == null) {
					indexData = createIndexData(docNr);
					if (cacheOnlyMode) {
						this.log("    - index data created");
						return;
					}
				}
				
				//	check presence of sub results
				IndexResult[] irs = indexData.getIndexResults();
				if (shallowMode && (irs.length != 0)) {
					this.log("    - index entries present");
					return;
				}
				
				//	check individual sub results, and add missing ones
				Query query = null;
				boolean indexDataModified = false;
				boolean reIndexingScheduled = false;
				final QueriableAnnotation[] reIndexDoc = {null};
				final IOException[] reIndexDocIoe = {null};
				final IndexData[] reIndexData = {null};
				for (int i = 0; i < indexers.length; i++) {
					final Indexer indexer = indexers[i];
					this.log("    - doing indexer " + indexer.getClass().getName());
					
					//	get sub result
					IndexResult ir = indexData.getIndexResult(indexer.getIndexName());
					if (ir != null) {
						this.log("      - index entries present");
						continue;
					}
					
					//	create sub result if missing
					if (query == null)
						query = new Query();
					ir = indexers[i].getIndexEntries(query, docNr, true);
					if ((ir != null) && ir.hasNextElement()) {
						indexData.addIndexResult(ir);
						indexDataModified = true;
						this.log("      - index entries added");
					}
					else if (cacheOnlyMode)
						this.log("      - no index entries found");
					else {
						enqueueIndexerAction(new IndexerAction(IndexerAction.RE_INDEX_NAME, indexer) {
							void performAction() {
								if (reIndexDocIoe[0] == null) try {
									if (reIndexDoc[0] == null)
										reIndexDoc[0] = getDocument(docId);
									if (reIndexData[0] == null)
										reIndexData[0] = getIndexData(docNr, docId);
//									this.indexer.deleteDocument(docNr);
//									IndexResult ir = this.indexer.index(reIndexDoc[0], docNr);
									IndexResult ir = this.indexer.reIndex(reIndexDoc[0], docNr);
									if ((ir == null) || !ir.hasNextElement())
										reIndexData[0].removeIndexResult(this.indexer.getIndexName());
									else reIndexData[0].addIndexResult(ir);
								}
								catch (IOException ioe) {
									logError("GoldenGateSRS: error re-indexing document '" + docId + "': " + ioe.getMessage());
									logError(ioe);
									reIndexDocIoe[0] = ioe;
								}
							}
						});
						reIndexingScheduled = true;
						this.log("      - no index entries found, re-indexing scheduled");
					}
				}
				
				//	check document attributes
				Properties docAttributes = getDocumentAttributes(docId);
				if ((docAttributes != null) && setIndexDataDocAttributes(indexData, docAttributes))
					indexDataModified = true;
				
				//	store index data if modified
				if (indexDataModified) {
					storeIndexData(indexData);
					this.log("    - index data stored");
				}
				else this.log("    - index data unchanged");
				
				//	any re-indexing to trigger?
				if (!reIndexingScheduled)
					return;
				
				//	check document data as well
				enqueueIndexerAction(new IndexerAction(IndexerAction.RE_INDEX_NAME) {
					void performAction() {
						if (reIndexDocIoe[0] != null)
							return;
						if (reIndexData[0] == null)
							reIndexData[0] = getIndexData(docNr, docId);
						Properties docAttributes = getDocumentAttributes(docId);
						if (docAttributes != null)
							setIndexDataDocAttributes(reIndexData[0], docAttributes);
					}
				});
				enqueueIndexerAction(new IndexerAction(IndexerAction.CACHE_INDEX_NAME) {
					void performAction() {
						if ((reIndexDocIoe[0] == null) && (reIndexData[0] != null))
							storeIndexData(reIndexData[0]);
					}
				});
			}
		};
		
		//	make sure to load modification time
		this.getLastModified();
		
		//	initialize document identifier resolvers (in dedicated thread to speed up startup, but blocking search requests via lock)
		Thread docIdResolverBuilder = new Thread() {
			public void run() {
				synchronized (this) {
					this.notify();
				}
				synchronized (docIdentifierResolverLock) {
					initDocumentIdentifierResolvers();
				}
			}
		};
		synchronized (docIdResolverBuilder) {
			docIdResolverBuilder.start();
			try {
				docIdResolverBuilder.wait();
			} catch (InterruptedException ie) {}
		}
	}
	
	private void initDocumentIdentifierResolvers() {
		DocumentNumberResolver docNumberResolver = new DocumentNumberResolver();
		TreeMap docUuidsToNrs = new TreeMap(signAwareHexStringOrder);
		TreeMap masterDocIdsToNrs = new TreeMap(signAwareHexStringOrder);
		String query = "SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_UUID_ATTRIBUTE + ", " + MASTER_DOCUMENT_ID_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME +
				" ORDER BY " + DOC_NUMBER_COLUMN_NAME + 
				";";
		SqlQueryResult sqr = null;
		try {
			System.out.println("GoldenGateSRS: initializing document identifier mappings.");
			long start = System.currentTimeMillis();
			sqr = io.executeSelectQuery(query, true);
			System.out.println(" - data loaded after " + (System.currentTimeMillis() - start) + "ms");
			while (sqr.next()) {
				long docNr = sqr.getLong(0);
				String docId = sqr.getString(1);
				docNumberResolver.mapDocumentNumber(docNr, docId);
				String docUuid = sqr.getString(2);
				if (docUuid != null) {
					docUuid = docUuid.trim();
					if (docUuid.length() != 0)
						docUuidsToNrs.put(docUuid, new Long(docNr));
				}
				String masterDocId = sqr.getString(3);
				Object docNrsObj = masterDocIdsToNrs.get(masterDocId);
				if (docNrsObj == null)
					masterDocIdsToNrs.put(masterDocId, new Long(docNr));
				else if (docNrsObj instanceof ArrayList)
					((ArrayList) docNrsObj).add(new Long(docNr));
				else if (docNrsObj instanceof Long) {
					ArrayList docNrs = new ArrayList(4);
					docNrs.add(docNrsObj);
					docNrs.add(new Long(docNr));
					masterDocIdsToNrs.put(masterDocId, docNrs);
				}
			}
			this.docNumberResolver = docNumberResolver.cloneToSize();
			System.out.println(" - document number resolver finished after " + (System.currentTimeMillis() - start) + "ms");
			
			DocumentUuidResolver docUuidResolver = new DocumentUuidResolver();
			for (Iterator duit = docUuidsToNrs.keySet().iterator(); duit.hasNext();) {
				String docUuid = ((String) duit.next());
				Long docNr = ((Long) docUuidsToNrs.get(docUuid));
				docUuidResolver.mapDocumentUuid(docUuid, docNr);
			}
			System.out.println(" - document UUID resolver populated after " + (System.currentTimeMillis() - start) + "ms");
			this.docUuidResolver = docUuidResolver.cloneToSize();
			docUuidsToNrs.clear(); // helps garbage collection
			System.out.println(" - document UUID resolver finished after " + (System.currentTimeMillis() - start) + "ms");
			
			MasterDocumentIdMapper masterDocIdMapper = new MasterDocumentIdMapper();
			for (Iterator ndidit = masterDocIdsToNrs.keySet().iterator(); ndidit.hasNext();) {
				String masterDocId = ((String) ndidit.next());
				Object docNrsObj = masterDocIdsToNrs.get(masterDocId);
				long[] docNrs;
				if (docNrsObj instanceof Long) {
					docNrs = new long[1];
					docNrs[0] = ((Long) docNrsObj).longValue();
				}
				else if (docNrsObj instanceof ArrayList) {
					ArrayList docNrsList = ((ArrayList) docNrsObj);
					docNrs = new long[docNrsList.size()];
					for (int d = 0; d < docNrsList.size(); d++)
						docNrs[d] = ((Long) docNrsList.get(d)).longValue();
				}
				else continue;
				masterDocIdMapper.mapMasterDocumentId(masterDocId, docNrs);
			}
			System.out.println(" - master document ID mapper populated after " + (System.currentTimeMillis() - start) + "ms");
			this.masterDocIdMapper = masterDocIdMapper.cloneToSize();
			masterDocIdsToNrs.clear(); // helps garbage collection
			System.out.println(" - master document ID mapper finished after " + (System.currentTimeMillis() - start) + "ms");
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while initializing document identifier mappings.");
			System.out.println("  Query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateScf.AbstractServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		System.out.println("GoldenGateSRS: shutting down ...");
		
		this.indexerServiceMonitor.dispose();
		this.indexerService.shutdown();
		System.out.println("  - indexer service shut down");
		
		for (int i = 0; i < this.indexers.length; i++)
			this.indexers[i].exit();
		this.indexers = new Indexer[0];
		System.out.println("  - indexers shut down");
		
		this.indexDataStore.shutdown();
		System.out.println("  - index data store shut down");
		
		if (this.dstCache != null) {
			this.dstCache.shutdown();
			System.out.println("  - document cache shut down");
		}
		
		for (int f = 0; f < this.filters.length; f++)
			this.filters[f].exit();
		this.filters = new StorageFilter[0];
		System.out.println("  - filters shut down");
		
		for (int s = 0; s < this.splitters.length; s++)
			this.splitters[s].exit();
		this.splitters = new DocumentSplitter[0];
		System.out.println("  - splitters shut down");
		
		for (int u = 0; u < this.uuidFetchers.length; u++)
			this.uuidFetchers[u].exit();
		this.uuidFetchers = new DocumentUuidFetcher[0];
		System.out.println("  - splitters shut down");
		
		System.gc();
		
		this.io.close();
		System.out.println("  - disconnected from database");
		
		this.dst.shutdown();
		System.out.println("  - document store shut down");
	}
	
	private static final String LIST_INDEXERS_COMMAND = "indexers";
	private static final String LIST_QUERIED_INDEXERS_FIELDS_COMMAND = "queried";
	private static final String LIST_FILTERS_COMMAND = "filters";
	private static final String LIST_SPLITTERS_COMMAND = "splitters";
	private static final String LIST_UUID_FETCHERS_COMMAND = "uuidFetchers";
	
	private static final String SET_ACTIVITY_LOG_TIMEOUT_COMMAND = "setAlt";
	
	private static final String REINDEX_COMMAND = "reIndex";
	private static final String CHECK_INDEX_COMMAND = "checkIndex";
	private static final String REINDEX_COMMAND_DOCUMENT = "reIndexDoc";
	private static final String INDEXER_STATS_COMMAND = "indexerStats";
	
	private static final String ISSUE_EVENTS_COMMAND = "issueEvents";
	
	private AsynchronousConsoleAction reIndexAction;
	private AsynchronousConsoleAction checkIndexAction;
	
	/* (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
		//	request for XML descriptors of search forms
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_SEARCH_FIELDS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	indicate definitions coming
				output.write(GET_SEARCH_FIELDS);
				output.newLine();
				
				//	write definitions
				for (int i = 0; i < indexers.length; i++) {
					SearchFieldGroup sfg = indexers[i].getSearchFieldGroup();
					sfg.writeXml(output);
					output.newLine();
				}
			}
		};
		cal.add(ca);
		
		//	document search query
		ca = new ComponentActionNetwork() {
			public long getActivityLogTimeout() {
				return activityLogTimeout;
			}
			public String getActionCommand() {
				return SEARCH_DOCUMENTS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read query
				String queryString = input.readLine();
				Query query = parseQuery(queryString);
				
				//	get result
				DocumentResult dr = searchDocuments(query, !"false".equals(query.getValue(INCLUDE_UPDATE_HISTORY_PARAMETER, "false")));
				logActivity("GgSRS: document search complete");
				logActivity("  query was " + query.toString());
				
				//	indicate result coming
				output.write(SEARCH_DOCUMENTS);
				output.newLine();
				
				//	send data
				dr.writeXml(output);
				
				//	make data go
				output.flush();
				logActivity("GgSRS: document search done\r\n    query: " + query);
			}
		};
		cal.add(ca);
		
		//	document metadata search
		ca = new ComponentActionNetwork() {
			public long getActivityLogTimeout() {
				return activityLogTimeout;
			}
			public String getActionCommand() {
				return SEARCH_DOCUMENT_DETAILS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read query
				String queryString = input.readLine();
				Query query = parseQuery(queryString);
				
				//	get result
				DocumentResult dr = searchDocumentDetails(query, !"false".equals(query.getValue(INCLUDE_UPDATE_HISTORY_PARAMETER, "false")));
				logActivity("GgSRS: document search complete");
				logActivity("  query was " + query.toString());
				
				//	send data
				dr.writeXml(output);
				
				//	make data go
				output.flush();
				logActivity("GgSRS: document detail search done\r\n    query: " + query);
			}
		};
		cal.add(ca);
		
		//	document metadata search
		ca = new ComponentActionNetwork() {
			public long getActivityLogTimeout() {
				return activityLogTimeout;
			}
			public String getActionCommand() {
				return SEARCH_DOCUMENT_DATA;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read query
				String queryString = input.readLine();
				Query query = parseQuery(queryString);
				
				//	get result
				DocumentResult docResult = searchDocumentData(query);
				
				//	indicate result coming
				output.write(SEARCH_DOCUMENT_DATA);
				output.newLine();
				
				//	write data
				docResult.writeXml(output);
				output.newLine();
				
				//	send data
				output.flush();
				logActivity("GgSRS: document data search done\r\n    query: " + query);
			}
		};
		cal.add(ca);
		
		//	document ID search
		ca = new ComponentActionNetwork() {
			public long getActivityLogTimeout() {
				return activityLogTimeout;
			}
			public String getActionCommand() {
				return SEARCH_DOCUMENT_IDS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read query
				String queryString = input.readLine();
				Query query = parseQuery(queryString);
				
				//	get result
				DocumentResult dr = searchDocumentIDs(query);
				
				//	indicate result coming
				output.write(SEARCH_DOCUMENT_IDS);
				output.newLine();
				
				//	send response
				dr.writeXml(output);
				
				//	make the data go
				output.flush();
				logActivity("GgSRS: document ID search done\r\n    query: " + query);
			}
		};
		cal.add(ca);
		
		//	index entry search
		ca = new ComponentActionNetwork() {
			public long getActivityLogTimeout() {
				return activityLogTimeout;
			}
			public String getActionCommand() {
				return SEARCH_INDEX;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read query
				String queryString = input.readLine();
				Query query = parseQuery(queryString);
				
				//	get result
				IndexResult result = searchIndex(query);
				
				//	indicate result coming
				output.write(SEARCH_INDEX);
				output.newLine();
				
				//	write data
				result.writeXml(output);
				output.newLine();
				
				//	send data
				output.flush();
				logActivity("GgSRS: index search done\r\n    query: " + query);
			}
		};
		cal.add(ca);
		
		//	request for individual document in plain XML
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_XML_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				String docId = input.readLine();
				try {
					DocumentReader dr = getDocumentAsStream(docId);
					try {
						
						//	indicate document coming
						output.write(GET_XML_DOCUMENT);
						output.newLine();
						
						//	write document
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
				catch (Exception e) {
					output.write("Could not find or load document with ID " + docId);
					output.newLine();
				}
			}
		};
		cal.add(ca);
		
		//	request for specific version of individual document in plain XML
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_XML_DOCUMENT_VERSION;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				String docId = input.readLine();
				int version = Integer.parseInt(input.readLine());
				try {
					DocumentReader dr = getDocumentAsStream(docId, version);
					try {
						
						//	indicate document coming
						output.write(GET_XML_DOCUMENT_VERSION);
						output.newLine();
						
						//	write document
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
				catch (Exception e) {
					output.write("Could not find or load version " + version + " of document with ID " + docId);
					output.newLine();
				}
			}
		};
		cal.add(ca);
		
		//	thesaurus search query
		ca = new ComponentActionNetwork() {
			public long getActivityLogTimeout() {
				return activityLogTimeout;
			}
			public String getActionCommand() {
				return SEARCH_THESAURUS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read query
				String queryString = input.readLine();
				Query query = parseQuery(queryString);
				
				//	process query
				ThesaurusResult result = searchThesaurus(query);
				
				//	check for result
				if (result == null) {
					
					//	indicate error
					output.write("No such index.");
					output.newLine();
					
				}
				
				//	send result
				else {
					
					//	indicate result coming
					output.write(SEARCH_THESAURUS);
					output.newLine();
					
					//	send actual data
					result.writeXml(output);
					output.newLine();
				}
				logActivity("GgSRS: thesaurus search done\r\n    query: " + query);
			}
		};
		cal.add(ca);
		
		//	statistics data request
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_STATISTICS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read temporal constraint
				String sinceString = input.readLine();
				
				//	get statistics
				CollectionStatistics scs = getStatistics(sinceString);
				
				//	indicate statistics coming
				output.write(GET_STATISTICS);
				output.newLine();
				
				//	send statistics
				scs.writeXml(output);
				output.newLine();
			}
		};
		cal.add(ca);
		
		//	request for last data modification
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_LAST_MODIFIED;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	get statistics
				long lastMod = getLastModified();
				
				//	indicate statistics coming
				output.write(GET_LAST_MODIFIED);
				output.newLine();
				output.write("" + lastMod);
				output.newLine();
			}
		};
		cal.add(ca);
		
		//	list documents
		ca = new ComponentActionNetwork() {
			public long getActivityLogTimeout() {
				return activityLogTimeout;
			}
			public String getActionCommand() {
				return LIST_DOCUMENTS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	get master document ID (for sub lists)
				String masterDocID = input.readLine();
				try {
					DocumentList docList = getDocumentList(((masterDocID == null) || (masterDocID.trim().length() == 0)) ? null : masterDocID);
					
					//	deliver response only if no errors occurred while reading data
					output.write(LIST_DOCUMENTS);
					output.newLine();
					
					//	send data
					docList.writeXml(output);
					
					//	make data go
					output.flush();
				}
				catch (IOException ioe) {
					logError("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while listing documents.");
					output.write("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while listing documents.");
					output.newLine();
				}
			}
		};
		cal.add(ca);
		
		//	list this.indexers
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_INDEXERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_INDEXERS_COMMAND,
						"List the indexers plugged in this GoldenGATE SRS."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					this.reportResult(" There " + ((indexers.length == 1) ? "is" : "are") + " " + ((indexers.length == 0) ? "no" : ("" + indexers.length)) + " Indexer" + ((indexers.length == 1) ? "" : "s") + " installed in GoldenGATE SRS:");
					for (int i = 0; i < indexers.length; i++)
						this.reportResult(" - " + indexers[i].getIndexName() + " (" + indexers[i].getClass().getName() + ")");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	list queried indexer fields
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_QUERIED_INDEXERS_FIELDS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_QUERIED_INDEXERS_FIELDS_COMMAND,
						"List the fields queried in the installed indexers."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				long queriedTime = (System.currentTimeMillis() - indexersQueriedSince);
				long queriedHours = ((queriedTime + ((1000 * 60 * 60) / 2)) / (1000 * 60 * 60));
				this.reportResult("These " + queriedIndexerFields.elementCount() + " indexer fields were queried in the past " + queriedHours + " hours (" + queriedTime + "ms):");
				for (Iterator qfnit = queriedIndexerFields.iterator(); qfnit.hasNext();) {
					String queriedFieldName = ((String) qfnit.next());
					this.reportResult(" - '" + queriedFieldName + "': " + queriedIndexerFields.getCount(queriedFieldName) + " timed");
				}
			}
		};
		cal.add(ca);
		
		//	list this.filters
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_FILTERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_FILTERS_COMMAND,
						"List the storage filters plugged in this GoldenGATE SRS."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					this.reportResult(" There " + ((filters.length == 1) ? "is" : "are") + " " + ((filters.length == 0) ? "no" : ("" + filters.length)) + " StorageFilter" + ((filters.length == 1) ? "" : "s") + " installed in GoldenGATE SRS:");
					for (int f = 0; f < filters.length; f++)
						this.reportResult(" - " + filters[f].getClass().getName());
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	list document splitters
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_SPLITTERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_SPLITTERS_COMMAND,
						"List the document splitters plugged in this GoldenGATE SRS."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					this.reportResult(" There " + ((splitters.length == 1) ? "is" : "are") + " " + ((splitters.length == 0) ? "no" : ("" + splitters.length)) + " DocumentSplitter" + ((splitters.length == 1) ? "" : "s") + " installed in GoldenGATE SRS:");
					for (int s = 0; s < splitters.length; s++)
						this.reportResult(" - " + splitters[s].getClass().getName());
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	list document UUID fetchers
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_UUID_FETCHERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_UUID_FETCHERS_COMMAND,
						"List the document UUID fetchers plugged in this GoldenGATE SRS."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					this.reportResult(" There " + ((uuidFetchers.length == 1) ? "is" : "are") + " " + ((uuidFetchers.length == 0) ? "no" : ("" + uuidFetchers.length)) + " DocumentUuidFetcher" + ((splitters.length == 1) ? "" : "s") + " installed in GoldenGATE SRS:");
					for (int u = 0; u < uuidFetchers.length; u++)
						this.reportResult(" - " + uuidFetchers[u].getClass().getName());
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	set activity log timeout
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return SET_ACTIVITY_LOG_TIMEOUT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						SET_ACTIVITY_LOG_TIMEOUT_COMMAND + " <timout>",
						"Set the timeout after which log messages from network actions become warnings:",
						"- <timeout>: the timeout in milliseconds",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					this.reportResult(" Activity log timeout currently is " + activityLogTimeout + " ms");
				else if (arguments.length != 1)
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the timeout as the only argument.");
				else try {
					activityLogTimeout = Integer.parseInt(arguments[0]);
				}
				catch (NumberFormatException nfe) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the timeout as an integer only.");
				}
			}
		};
		cal.add(ca);
		
		//	issue update events
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return REINDEX_COMMAND_DOCUMENT;
			}
			public String[] getExplanation() {
				String[] explanation = {
						REINDEX_COMMAND_DOCUMENT + " <masterDocOrDocId> <indexerName>",
						"Re-index all documents from a specific master document:",
						"- <masterDocOrDocId>: the ID of the master document or single document to re-index",
						"- <indexerName>: the name of the indexer to re-index the document with (optional)",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((arguments.length < 1) || (arguments.length > 2))
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the master document ID and indexer name as the only arguments.");
				else reIndexMasterDoc(arguments[0], ((arguments.length == 1) ? null : arguments[1]), this);
			}
		};
		cal.add(ca);
		
		//	output overview of indexer actions
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return INDEXER_STATS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						INDEXER_STATS_COMMAND,
						"Show overview of action queue and performance of inderer background service"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				ArrayList iass = new ArrayList(indexerActionStats.values());
				this.reportResult("Indexer background service actions enqueued and executed since startup:");
				for (int s = 0; s < iass.size(); s++)
					this.reportResult(" - " + iass.get(s));
			}
		};
		cal.add(ca);
		
		//	trigger generation of XML cached index results
		if (this.dstCache != null) {
			ca = new ComponentActionConsole() {
				public String getActionCommand() {
					return "cacheDocuments";
				}
				public String[] getExplanation() {
					String[] explanation = {
							"cacheDocuments" + " <masterDocId>",
							"Cache the latest version of all documents from a specific master document:",
							"- <masterDocId>: the ID of the master document to cache the documents for",
						};
					return explanation;
				}
				public void performActionConsole(String[] arguments) {
					if (arguments.length > 1) {
						this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at most the master document ID as the only argument.");
						return;
					}
					final String masterDocId = ((arguments.length == 0) ? null : arguments[0]);
					Thread scheduler = new Thread() {
						public void run() {
							scheduleCacheDocuments(masterDocId);
						}
					};
					scheduler.start();
				}
				void scheduleCacheDocuments(String masterDocId) {
					LinkedHashSet docNrs = new LinkedHashSet();
					String docNrsToDocIDsQuery = "SELECT " + Indexer.DOC_NUMBER_COLUMN_NAME + 
						" FROM " + DOCUMENT_TABLE_NAME + 
						" WHERE " + ((masterDocId == null) ? "1=1" : (MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'")) +
						";";
					SqlQueryResult sqr = null;
					try {
						sqr = io.executeSelectQuery(docNrsToDocIDsQuery);
						while (sqr.next()) {
							String docNr = sqr.getString(0);
							docNrs.add(docNr);
						}
					}
					catch (SQLException sqle) {
						logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading document IDs.");
						logError("Query was " + docNrsToDocIDsQuery);
					}
					finally {
						if (sqr != null)
							sqr.close();
					}
					this.reportResult("Got " + docNrs.size() + " document numbers");
					int scheduleDocCount = 0;
					DocumentNumberResolver dnr = getDocumentNumberResolver();
					for (Iterator dnrit = docNrs.iterator(); dnrit.hasNext();) {
						final String docNrStr = ((String) dnrit.next());
						final long docNr = Long.parseLong(docNrStr);
						final String docId = dnr.getDocumentId(docNr);
						if (dstCache.isDataObjectAvailable(docId))
							continue;
						enqueueIndexerAction(new IndexerAction(IndexerAction.CACHE_INDEX_NAME) {
							void performAction() {
								if (dstCache.isDataObjectAvailable(docId))
									return;
								try {
									DocumentReader dr = dst.loadDocumentAsStream(docId);
									DataObjectOutputStream docCacheOut = dstCache.getOutputStream(docId);
									Writer out = new OutputStreamWriter(docCacheOut, "UTF-8");
									char[] buffer = new char[1024];
									for (int r; (r = dr.read(buffer, 0, buffer.length)) != -1;)
										out.write(buffer, 0, r);
									out.flush();
									out.close();
									dr.close();
								}
								catch (IOException ioe) {
									ioe.printStackTrace();
								}
							}
						});
						scheduleDocCount++;
					}
					this.reportResult("Scheduled caching " + scheduleDocCount + " of " + docNrs.size() + " documents");
				}
			};
			cal.add(ca);
		}
//		
//		//	trigger generation of XML cached index results
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return "cacheIndexResults";
//			}
//			public String[] getExplanation() {
//				String[] explanation = {
//						"cacheIndexResults" + " <masterDocId>",
//						"Cache index results for all documents from a specific master document:",
//						"- <masterDocId>: the ID of the master document to cache the index result for",
//					};
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length > 1) {
//					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at most the master document ID as the only argument.");
//					return;
//				}
//				final String masterDocId = ((arguments.length == 0) ? null : arguments[0]);
//				Thread scheduler = new Thread() {
//					public void run() {
//						scheduleCreateIndexData(masterDocId);
//					}
//				};
//				scheduler.start();
//			}
//			void scheduleCreateIndexData(String masterDocId) {
//				LinkedHashMap docNrsToDocIDs = new LinkedHashMap();
//				String docNrsToDocIDsQuery = "SELECT " + Indexer.DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + 
//					" FROM " + DOCUMENT_TABLE_NAME + 
//					" WHERE " + ((masterDocId == null) ? "1=1" : (MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'")) +
//					";";
//				SqlQueryResult sqr = null;
//				try {
//					sqr = io.executeSelectQuery(docNrsToDocIDsQuery);
//					while (sqr.next()) {
//						String docNr = sqr.getString(0);
//						String docId = sqr.getString(1);
//						docNrsToDocIDs.put(docNr, docId);
//					}
//				}
//				catch (SQLException sqle) {
//					logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading document IDs.");
//					logError("Query was " + docNrsToDocIDsQuery);
//				}
//				finally {
//					if (sqr != null)
//						sqr.close();
//				}
//				this.reportResult("Got " + docNrsToDocIDs.size() + " document numbers");
//				int scheduleDocCount = 0;
//				for (Iterator dnrit = docNrsToDocIDs.keySet().iterator(); dnrit.hasNext();) {
//					final String docNrStr = ((String) dnrit.next());
//					final long docNr = Long.parseLong(docNrStr);
//					if (isIndexDataCached(docNr))
//						continue;
//					final String docId = ((String) docNrsToDocIDs.get(docNrStr));
//					enqueueIndexerAction(new IndexerAction(IndexerAction.CACHE_INDEX_NAME) {
//						void performAction() {
//							if (isIndexDataCached(docNr))
//								return;
//							createIndexData(docNr);
//						}
//					});
//					scheduleDocCount++;
//				}
//				this.reportResult("Scheduled caching index results for " + scheduleDocCount + " of " + docNrsToDocIDs.size() + " documents");
//			}
//		};
//		cal.add(ca);
//		
//		//	trigger generation of XML cached index results
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return "checkDocIds";
//			}
//			public String[] getExplanation() {
//				String[] explanation = {
//						"checkDocIds" + " <mode>",
//						"Check document IDs:",
//						"- <mode>: set to '-c' for cleanup",
//						"- <userName>: name of user whose records to delete (only relevant for cleanup)",
//					};
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length > 2) {
//					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at most the mode and filter user name as the only arguments.");
//					return;
//				}
//				final boolean cleanup = ((arguments.length != 0) && "-c".equals(arguments[0]));
//				final String cleanupUser = ((arguments.length == 2) ? arguments[1] : null);
//				Thread checker = new Thread() {
//					public void run() {
//						checkDocIds(cleanup, cleanupUser);
//					}
//				};
//				checker.start();
//			}
//			private void checkDocIds(boolean cleanup, String cleanupUser) {
//				DocumentNumberResolver dnr = getDocumentNumberResolver();
//				int validCount = 0;
//				int invalidCount = 0;
//				ArrayList invalidData = new ArrayList();
//				ArrayList removeDocNrs = new ArrayList();
//				for (int d = 0; d < dnr.size(); d++) {
//					String docId = dnr.documentIdAt(d);
//					if (dst.isDocumentAvailable(docId))
//						validCount++;
//					else {
//						invalidCount++;
//						this.logError("Found invalid document ID (" + invalidCount + "): " + docId);
//						invalidData.add(docId);
//						long docNr = dnr.documentNumberAt(d);
//						if (cleanup && this.handleInvalidDocId(docId, docNr, cleanupUser))
//							removeDocNrs.add(new Long(docNr));
//					}
//					if (((validCount + invalidCount) % 1000) == 0)
//						this.logResult("Checked " + (validCount + invalidCount) + " document IDs, found " + validCount + " valid ones and " + invalidCount + " invalid ones");
//				}
//				if (invalidCount == 0) {
//					this.logResult("Found " + invalidCount + " invalid document IDs, " + validCount + " valid ones");
//					return;
//				}
//				this.logError("Found " + invalidCount + " invalid document IDs, " + validCount + " valid ones");
//				
//				long[] removed = new long[removeDocNrs.size()];
//				for (int r = 0; r < removeDocNrs.size(); r++)
//					removed[r] = ((Long) removeDocNrs.get(r)).longValue();
//				dnr = getDocumentNumberResolver();
//				synchronized (docNumberResolverLock) {
//					docNumberResolver = dnr.cloneForChanges(new HashSet(), removed);
//				}
//			}
//			private boolean handleInvalidDocId(final String docId, final long docNr, String cleanupUser) {
//				StringBuffer query = new StringBuffer("SELECT ");
//				for (int f = 0; f < documentListQueryFields.length; f++) {
//					if (f != 0)
//						query.append(", ");
//					query.append(documentListQueryFields[f]);
//				}
//				query.append(" FROM " + DOCUMENT_TABLE_NAME);
//				query.append(" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'");
//				query.append(";");
//				SqlQueryResult sqr = null;
//				boolean trouble = (cleanupUser != null);
//				try {
//					sqr = io.executeSelectQuery(query.toString());
//					while (sqr.next()) {
//						StringBuffer invalidDocIdData = new StringBuffer();
//						for (int f = 0; f < documentListQueryFields.length; f++) {
//							if (f != 0)
//								invalidDocIdData.append(" | ");
//							invalidDocIdData.append(sqr.getString(f));
//							if ((cleanupUser != null) && CHECKIN_USER_ATTRIBUTE.equals(documentListQueryFields[f]) && cleanupUser.equals(sqr.getString(f)))
//								trouble = false;
//						}
//						if (trouble)
//							this.logError(invalidDocIdData.toString());
//					}
//				}
//				catch (SQLException sqle) {
//					logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading invalid document ID record.");
//					logError("Query was " + query.toString());
//				}
//				finally {
//					if (sqr != null)
//						sqr.close();
//				}
//				if (trouble)
//					return false;
//				
//				String deleteQuery = "DELETE FROM " + DOCUMENT_TABLE_NAME + " WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "';";
//				try {
//					io.executeUpdateQuery(deleteQuery);
//					this.logError("Deleted records for invalid document ID " + docId);
//				}
//				catch (SQLException sqle) {
//					logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting invalid document ID record.");
//					logError("Query was " + deleteQuery);
//					return false;
//				}
//				for (int i = 0; i < indexers.length; i++)
//					enqueueIndexerAction(new IndexerAction(IndexerAction.DELETE_NAME, indexers[i]) {
//						void performAction() {
//							this.indexer.deleteDocument(docNr);
//						}
//					});
//				enqueueIndexerAction(new IndexerAction(IndexerAction.DELETE_NAME) {
//					void performAction() {
//						deleteIndexData(docNr);
//					}
//				});
//				return true;
//			}
//		};
//		cal.add(ca);
		
		//	issue update events
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return ISSUE_EVENTS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						ISSUE_EVENTS_COMMAND + " <masterDocId>",
						"Issue update events for all documents from a specific master document:",
						"- <masterDocId>: the ID of the master document to issue update events for"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 1)
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the master document ID as the only argument.");
				else issueEvents(arguments[0]);
			}
		};
		cal.add(ca);
		
		//	re-index collection
		cal.add(this.reIndexAction);
		
		//	check index data
		cal.add(this.checkIndexAction);
		
		//	get actions from document store (prefixing with "doc" and upper-casing first command letter)
		ComponentAction[] dstActions = this.dst.getActions();
		for (int a = 0; a < dstActions.length; a++) {
			if (dstActions[a] instanceof ComponentActionConsole)
				cal.add(new ComponentActionConsolePrefixWrapper(((ComponentActionConsole) dstActions[a]), "doc"));
			else cal.add(dstActions[a]);
		}
		
		//	get actions from document cache (prefixing with "doc" and upper-casing first command letter)
		if (this.dstCache != null) {
			ComponentAction[] dstCacheActions = this.dstCache.getActions();
			for (int a = 0; a < dstCacheActions.length; a++) {
				if (dstCacheActions[a] instanceof ComponentActionConsole)
					cal.add(new ComponentActionConsolePrefixWrapper(((ComponentActionConsole) dstCacheActions[a]), "dcc"));
				else cal.add(dstCacheActions[a]);
			}
		}
		
		//	get actions from index data store (prefixing with "idx" and upper-casing first command letter)
		ComponentAction[] idxActions = this.indexDataStore.getActions();
		for (int a = 0; a < idxActions.length; a++) {
			if (idxActions[a] instanceof ComponentActionConsole)
				cal.add(new ComponentActionConsolePrefixWrapper(((ComponentActionConsole) idxActions[a]), "idx"));
			else cal.add(idxActions[a]);
		}
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private static class ComponentActionConsolePrefixWrapper extends ComponentActionConsole {
		ComponentActionConsole cac;
		String prefix;
		ComponentActionConsolePrefixWrapper(ComponentActionConsole cac, String prefix) {
			this.cac = cac;
			this.prefix = prefix;
		}
		public String getActionCommand() {
			String ac = this.cac.getActionCommand();
			return (this.prefix + ac.substring(0, 1).toUpperCase() + ac.substring(1));
		}
		public String[] getExplanation() {
			String[] expl = this.cac.getExplanation();
			expl[0] = (this.prefix + expl[0].substring(0, 1).toUpperCase() + expl[0].substring(1));
			return expl;
		}
		public void performActionConsole(String[] arguments, GoldenGateServerActivityLogger resultLogger) {
			this.cac.performActionConsole(arguments, resultLogger);
		}
		public void performActionConsole(String[] arguments) {
			this.cac.performActionConsole(arguments);
		}
	}
	
	private GoldenGateSrsPlugin[] createPlugins(final File pluginFolder) {
		
		//	get base directory
		if(!pluginFolder.exists()) pluginFolder.mkdir();
		
		//	load plugins
		Object[] pluginObjects = GamtaClassLoader.loadComponents(
				pluginFolder, 
				GoldenGateSrsPlugin.class, 
				new ComponentInitializer() {
					public void initialize(Object component, String componentJarName) throws Exception {
						File dataPath = new File(pluginFolder, (componentJarName.substring(0, (componentJarName.length() - 4)) + "Data"));
						if (!dataPath.exists()) dataPath.mkdir();
						GoldenGateSrsPlugin plugin = ((GoldenGateSrsPlugin) component);
						plugin.setDataPath(dataPath);
						plugin.setHost(GoldenGateSRS.this.host);
					}
				});
		
		//	store & return plugins
		GoldenGateSrsPlugin[] plugins = new GoldenGateSrsPlugin[pluginObjects.length];
		for (int c = 0; c < pluginObjects.length; c++)
			plugins[c] = ((GoldenGateSrsPlugin) pluginObjects[c]);
		return plugins;
	}
	
	private void reIndexMasterDoc(String masterDocId, String indexerName, ComponentActionConsole cac) {
		
		//	get document IDs
		MasterDocumentIdMapper masterDocIdMapper = this.getMasterDocumentIdMapper();
		long[] docNrs = masterDocIdMapper.getDocumentNumbers(masterDocId);
		int docCount = 0;
		int indexCount = 0;
		
		//	no documents found, must be document ID proper
		if ((docNrs == null) && masterDocId.matches("[0-9A-F]{32}")) {
			indexCount += this.reIndexDoc(masterDocId, indexerName);
			docCount++;
		}
		
		//	we actually have documents in list to re-index
		else if (docNrs != null) {
			cac.reportResult("Got " + docNrs.length + " document IDs.");
			DocumentNumberResolver docNumberResolver = this.getDocumentNumberResolver();
			for (int d = 0; d < docNrs.length; d++) {
				String docId = docNumberResolver.getDocumentId(docNrs[d]);
				if (docId == null)
					continue;
				indexCount += this.reIndexDoc(docId, indexerName);
				docCount++;
			}
		}
		cac.reportResult("Scheduled re-indexing of for " + docCount + " documents (" + indexCount + " index updates in total).");
	}
	
	private int reIndexDoc(final String docId, String indexerName) {
		final long docNr = getDocNr(docId);
		final IndexData indexData = getIndexData(docNr, docId);
		final QueriableAnnotation[] doc = {null};
		final IOException[] docIoe = {null};
		boolean updateIndexData = false;
		int indexCount = 0;
		for (int i = 0; i < this.indexers.length; i++) {
			if ((indexerName != null) && !indexerName.equals(this.indexers[i].getIndexName()))
				continue;
			this.enqueueIndexerAction(new IndexerAction(IndexerAction.RE_INDEX_NAME, this.indexers[i]) {
				void performAction() {
					if (docIoe[0] == null) try {
						if (doc[0] == null)
							doc[0] = getDocument(docId);
//						this.indexer.deleteDocument(docNr);
//						IndexResult ir = this.indexer.index(doc[0], docNr);
						IndexResult ir = this.indexer.reIndex(doc[0], docNr);
						if ((ir == null) || !ir.hasNextElement())
							indexData.removeIndexResult(this.indexer.getIndexName());
						else indexData.addIndexResult(ir);
					}
					catch (IOException ioe) {
						logError("GoldenGateSRS: error re-indexing document '" + docId + "': " + ioe.getMessage());
						logError(ioe);
						docIoe[0] = ioe;
					}
				}
			});
			indexCount++;
			updateIndexData = true;
		}
		if ((indexerName == null) || "doc".equals(indexerName)) {
			this.enqueueIndexerAction(new IndexerAction(IndexerAction.RE_INDEX_NAME) {
				void performAction() {
					if (docIoe[0] != null)
						return;
					Properties docAttributes = getDocumentAttributes(docId);
					if (docAttributes != null)
						setIndexDataDocAttributes(indexData, docAttributes);
				}
			});
			indexCount++;
			updateIndexData = true;
		}
		if (updateIndexData)
			this.enqueueIndexerAction(new IndexerAction(IndexerAction.CACHE_INDEX_NAME) {
				void performAction() {
					if (docIoe[0] == null)
						storeIndexData(indexData);
				}
			});
		
		//	finally ...
		return indexCount;
	}
	
	private Thread eventIssuer = null;
	private void issueEvents(final String masterDocId) {
		
		//	let's not knock out the server
		if (this.eventIssuer != null) {
			this.logResult("Already issuing update events, only one document can run at a time.");
			return;
		}
		
		//	create and start event issuer
		this.eventIssuer = new Thread() {
			public void run() {
				StringBuffer query = new StringBuffer("SELECT " + DOCUMENT_ID_ATTRIBUTE + ", " + UPDATE_USER_ATTRIBUTE + ", " + UPDATE_TIME_ATTRIBUTE);
				query.append(" FROM " + DOCUMENT_TABLE_NAME);
				query.append(" WHERE " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'");
				query.append(" AND " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocId.hashCode() + "");
				query.append(";");
				
				SqlQueryResult sqr = null;
				int count = 0;
				try {
					sqr = io.executeSelectQuery(query.toString(), true);
					while (sqr.next()) {
						String docId = sqr.getString(0);
						String updateUser = sqr.getString(1);
						long updateTime = sqr.getLong(2);
						int docVersion = dst.getVersion(docId);
						GoldenGateServerEventService.notify(new SrsDocumentEvent(updateUser, docId, masterDocId, null, docVersion, GoldenGateSRS.class.getName(), updateTime, new EventLogger() {
							public void writeLog(String logEntry) {}
						}));
						count++;
					}
				}
				catch (SQLException sqle) {
					logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
					logError("  query was " + query);
				}
				finally {
					if (sqr != null)
						sqr.close();
					eventIssuer = null;
				}
				logResult("Issued update events for " + count + " documents.");
			}
		};
		this.eventIssuer.start();
	}
	
//	private CollectionStatistics getStatistics() throws IOException {
//		return this.getStatistics(null);
//	}
	private CollectionStatistics getStatistics(String sinceString) throws IOException {
		
		//	compute 'since' parameter
		long since = -1;
		if (sinceString == null)
			sinceString = "-1";
		else try {
			since = Long.parseLong(sinceString);
		}
		catch (NumberFormatException nfe) {
			if (GET_STATISTICS_LAST_YEAR.equals(sinceString))
				since = (System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 365));
			else if (GET_STATISTICS_LAST_HALF_YEAR.equals(sinceString))
				since = (System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 183));
			else if (GET_STATISTICS_LAST_THREE_MONTHS.equals(sinceString))
				since = (System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 91));
			else if (GET_STATISTICS_LAST_MONTH.equals(sinceString))
				since = (System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 30));
		}
		
		//	do cache lookup
		Long cacheKey = new Long(since / (1000 * 60 * 10));
		synchronized (this.collectionsStatisticsCache) {
			CacheCollectionStatistics cachedStats = ((CacheCollectionStatistics) this.collectionsStatisticsCache.get(cacheKey));
			if (cachedStats != null)
				return cachedStats.getCollectionStatistics(sinceString);
		}
		
		//	create statistics from database (better one table scan here than four in database, with peculiar assembly)
		String statQuery = "SELECT " + CHECKIN_USER_ATTRIBUTE + 
				", " + CHECKIN_TIME_ATTRIBUTE + 
				", " + MASTER_DOCUMENT_ID_ATTRIBUTE + 
				", " + DOCUMENT_SIZE_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME +
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(statQuery);
			
			//	gather statistics
			CacheCollectionStatistics cachingStats = new CacheCollectionStatistics();
			HashSet masterDocIDs = new HashSet();
			HashSet masterDocIDsSince = new HashSet();
			HashMap cachingUserStats = new HashMap();
			while (sqr.next()) {
				
				//	overall ...
				masterDocIDs.add(sqr.getString(2));
				cachingStats.docCount++;
				cachingStats.wordCount += sqr.getInt(3);
				
				//	..., user specific, ...
				CacheUserStatistics cachingUserStat = ((CacheUserStatistics) cachingUserStats.get(sqr.getString(0)));
				if (cachingUserStat == null) {
					cachingUserStat = new CacheUserStatistics(sqr.getString(0));
					cachingUserStats.put(cachingUserStat.checkinUser, cachingUserStat);
				}
				cachingUserStat.masterDocIDs.add(sqr.getString(2));
				cachingUserStat.docCount++;
				cachingUserStat.wordCount += sqr.getInt(3);
				
				//	..., and time specific
				if (since <= sqr.getLong(1)) {
					masterDocIDsSince.add(sqr.getString(2));
					cachingStats.docCountSince++;
					cachingStats.wordCountSince += sqr.getInt(3);
					cachingUserStat.masterDocIDsSince.add(sqr.getString(2));
					cachingUserStat.docCountSince++;
					cachingUserStat.wordCountSince += sqr.getInt(3);
				}
			}
			
			//	finish overall statistics
			cachingStats.masterDocCount = masterDocIDs.size();
			cachingStats.masterDocCountSince = masterDocIDsSince.size();
			
			//	sort users and add them to cached statistics
			ArrayList cachingUserStatList = new ArrayList(cachingUserStats.values());
			Collections.sort(cachingUserStatList, new Comparator() {
				public int compare(Object o1, Object o2) {
					CacheUserStatistics userStat1 = ((CacheUserStatistics) o1);
					CacheUserStatistics userStat2 = ((CacheUserStatistics) o2);
					int c;
					c = (userStat2.docCountSince - userStat1.docCountSince);
					if (c != 0)
						return c;
					c = (userStat2.docCount - userStat1.docCount);
					if (c != 0)
						return c;
					return userStat1.checkinUser.compareToIgnoreCase(userStat2.checkinUser);
				}
			});
			for (int u = 0; u < cachingUserStatList.size(); u++)
				cachingStats.entries.add(((CacheUserStatistics) cachingUserStatList.get(u)).getUserCollectionStatistics());
			
			//	cache statistics
			synchronized (this.collectionsStatisticsCache) {
				this.collectionsStatisticsCache.put(cacheKey, cachingStats);
			}
			
			//	finally ...
			return cachingStats.getCollectionStatistics(sinceString);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading top 10 user statistics.");
			this.logError("  Query was " + statQuery);
			return null;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	private LinkedHashMap collectionsStatisticsCache = new LinkedHashMap(16, 0.9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return (this.size() > 64);
		}
	};
	
	private static abstract class CacheStatisticsElement {
		int docCount = 0;
		int wordCount = 0;
		int docCountSince = 0;
		int wordCountSince = 0;
	}
	
	private static class CacheUserStatistics extends CacheStatisticsElement {
		String checkinUser;
		HashSet masterDocIDs = new HashSet();
		HashSet masterDocIDsSince = new HashSet();
		CacheUserStatistics(String checkinUser) {
			this.checkinUser = checkinUser;
		}
		SrsSearchResultElement getUserCollectionStatistics() {
			SrsSearchResultElement ssre = new SrsSearchResultElement();
			ssre.setAttribute(CHECKIN_USER_ATTRIBUTE, this.checkinUser);
			ssre.setAttribute(MASTER_DOCUMENT_COUNT_ATTRIBUTE, ("" + this.masterDocIDs.size()));
			ssre.setAttribute(DOCUMENT_COUNT_ATTRIBUTE, ("" + this.docCount));
			ssre.setAttribute(WORD_COUNT_ATTRIBUTE, ("" + this.wordCount));
			ssre.setAttribute(MASTER_DOCUMENT_COUNT_SINCE_ATTRIBUTE, ("" + this.masterDocIDsSince.size()));
			ssre.setAttribute(DOCUMENT_COUNT_SINCE_ATTRIBUTE, ("" + this.docCountSince));
			ssre.setAttribute(WORD_COUNT_SINCE_ATTRIBUTE, ("" + this.wordCountSince));
			return ssre;
		}
	}
	
	private static class CacheCollectionStatistics extends CacheStatisticsElement {
		private static final String[] statisticsFieldNames = {CHECKIN_USER_ATTRIBUTE, MASTER_DOCUMENT_COUNT_ATTRIBUTE, DOCUMENT_COUNT_ATTRIBUTE, WORD_COUNT_ATTRIBUTE, GET_STATISTICS_SINCE_PARAMETER, MASTER_DOCUMENT_COUNT_SINCE_ATTRIBUTE, DOCUMENT_COUNT_SINCE_ATTRIBUTE, WORD_COUNT_SINCE_ATTRIBUTE};
		int masterDocCount = 0;
		int masterDocCountSince = 0;
		final ArrayList entries = new ArrayList();
		CacheCollectionStatistics() {}
		CollectionStatistics getCollectionStatistics(String since) {
			final Iterator entryInterator = this.entries.iterator();
			return new CollectionStatistics(statisticsFieldNames, this.masterDocCount, this.docCount, this.wordCount, since, this.masterDocCountSince, this.docCountSince, this.wordCountSince) {
				public boolean hasNextElement() {
					return entryInterator.hasNext();
				}
				public SrsSearchResultElement getNextElement() {
					return ((SrsSearchResultElement) entryInterator.next());
				}
			};
		}
	}
	
	private String resolveDocumentId(String docId) {
		//	TODO use lookup data structures instead
		
		//	resolve UUID
		String resolverQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE +
				" FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE (" + DOC_NUMBER_COLUMN_NAME + " = " + getDocNr(docId) + " AND " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "')" +
				" OR (" + DOCUMENT_UUID_HASH_COLUMN_NAME + " = " + docId.hashCode() + " AND " + DOCUMENT_UUID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "')" +
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(resolverQuery, true);
			if (sqr.next())
				return sqr.getString(0);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting document ID for '" + docId + "'.");
			this.logError("  Query was " + resolverQuery);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	nothing found for UUID
		return docId;
	}
	
	/**
	 * Retrieve the timestamp of the last modification to the document
	 * collection.
	 * @return the timestamp of the last modification
	 */
	public long getLastModified() {
		if (this.dstLastModified != -1)
			return this.dstLastModified;
		String lastModQuery = "SELECT max(" + UPDATE_TIME_ATTRIBUTE + ")" +
				" FROM " + DOCUMENT_TABLE_NAME +
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(lastModQuery);
			if (sqr.next()) {
				this.dstLastModified = sqr.getLong(0);
				return this.dstLastModified;
			}
			else return 0;
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading lat modification timestamp.");
			this.logError("  Query was " + lastModQuery);
			return 0;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
	}
	
	/**
	 * Retrieve the attributes of a document, as stored in the SRS's storage.
	 * There is no guarantee with regard to the attributes contained in the
	 * returned properties. If a document with the specified ID does not exist,
	 * this method returns null.
	 * @param docId the ID of the document
	 * @return a Properties object holding the attributes of the document with
	 *         the specified ID
	 * @throws IOException
	 */
	public Properties getDocumentAttributes(String docId) {
		return this.getDocumentAttributes(docId, false);
	}
	
	/**
	 * Retrieve the attributes of a document, as stored in the SRS's storage.
	 * There is no guarantee with regard to the attributes contained in the
	 * returned properties. If a document with the specified ID does not exist,
	 * this method returns null.
	 * @param docId the ID of the document
	 * @param includeUpdateHistory include former update users and timestamps?
	 * @return a Properties object holding the attributes of the document with
	 *         the specified ID
	 * @throws IOException
	 */
	public Properties getDocumentAttributes(String docId, boolean includeUpdateHistory) {
		
		//	normalize UUID
		docId = normalizeId(docId);
		
		//	try plain ID lookup first (way faster)
		try {
			return this.dst.getDocumentAttributes(docId, includeUpdateHistory);
		}
		
		//	catch invalid document ID errors, as those can just as well indicate the need for resolving a UUID
		catch (DocumentNotFoundException dnfe) {}
		
		//	if another IO error occurs, there is no use in trying any further
		catch (IOException dnfe) {
			return null;
		}
		
		//	resolve UUID
		docId = this.resolveDocumentId(docId);
		
		//	get attributes
		try {
			return this.dst.getDocumentAttributes(docId);
		}
		catch (IOException ioe) {
			return null;
		}
	}

	/**
	 * Check if a document with a given ID exists.
	 * @param docId the ID of the document to check
	 * @return true if the document with the specified ID exists
	 * @see de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore#isDocumentAvailable(String)
	 */
	public boolean isDocumentAvailable(String docId) {
		
		//	normalize UUID
		docId = normalizeId(docId);
		
		//	try plain ID lookup first (way faster)
		if (this.dst.isDocumentAvailable(docId))
			return true;
		
		//	resolve UUID
		docId = this.resolveDocumentId(docId);
		
		//	try resolved document ID
		return this.dst.isDocumentAvailable(docId);
	}
	
	/**
	 * Retrieve the current version of a document stored in the collection of
	 * this GoldenGATE SRS.
	 * @param documentId the ID of the document to get the version for
	 * @return the current version of the document with the argument ID
	 */
	public int getDocumentVersion(String documentId) {
		return this.dst.getVersion(documentId);
	}
	
	/**
	 * Retrieve a document from the SRS's storage.
	 * @param docId the ID of the document to load
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public QueriableAnnotation getDocument(String docId) throws IOException {
		return this.getDocument(docId, 0, false);
	}
	
	/**
	 * Retrieve a specific version of a document from the SRS's storage. 0
	 * always indicates the current version, negative values indicate versions
	 * relative to the current one (e.g. -1 for the second most current
	 * version), positive values indicate absolute versions.
	 * @param docId the ID of the document to load
	 * @param version the desired document version
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public QueriableAnnotation getDocument(String docId, int version) throws IOException {
		return this.getDocument(docId, version, false);
	}
	
	/**
	 * Retrieve a document from the SRS's storage.
	 * @param docId the ID of the document to load
	 * @param includeUpdateHistory include former update users and timestamps?
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public QueriableAnnotation getDocument(String docId, boolean includeUpdateHistory) throws IOException {
		return this.getDocument(docId, 0, includeUpdateHistory);
	}
	
	/**
	 * Retrieve a specific version of a document from the SRS's storage. 0
	 * always indicates the current version, negative values indicate versions
	 * relative to the current one (e.g. -1 for the second most current
	 * version), positive values indicate absolute versions.
	 * @param docId the ID of the document to load
	 * @param version the desired document version
	 * @param includeUpdateHistory include former update users and timestamps?
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public QueriableAnnotation getDocument(String docId, int version, boolean includeUpdateHistory) throws IOException {
		DocumentReader dr = this.getDocumentAsStream(docId, version, includeUpdateHistory);
		try {
			return GenericGamtaXML.readDocument(dr);
		}
		finally {
			dr.close();
		}
	}
	
	/**
	 * Retrieve a document from the SRS's storage, as a stream for sending out
	 * to some writer without instantiating it on this end.
	 * @param docId the ID of the document to load
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public DocumentReader getDocumentAsStream(String docId) throws IOException {
		return this.getDocumentAsStream(docId, 0, false);
	}
	
	/**
	 * Retrieve a specific version of a document from the SRS's storage, as a
	 * stream for sending out to some writer without instantiating it on this
	 * end. 0 always indicates the current version, negative values indicate
	 * versions relative to the current one (e.g. -1 for the second most
	 * current version), positive values indicate absolute versions.
	 * @param docId the ID of the document to load
	 * @param version the desired document version
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public DocumentReader getDocumentAsStream(String docId, int version) throws IOException {
		return this.getDocumentAsStream(docId, version, false);
	}
	
	/**
	 * Retrieve a document from the SRS's storage, as a stream for sending out
	 * to some writer without instantiating it on this end.
	 * @param docId the ID of the document to load
	 * @param includeUpdateHistory include former update users and timestamps?
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public DocumentReader getDocumentAsStream(String docId, boolean includeUpdateHistory) throws IOException {
		return this.getDocumentAsStream(docId, 0, includeUpdateHistory);
	}
	
	/**
	 * Retrieve a specific version of a document from the SRS's storage, as a
	 * stream for sending out to some writer without instantiating it on this
	 * end. 0 always indicates the current version, negative values indicate
	 * versions relative to the current one (e.g. -1 for the second most
	 * current version), positive values indicate absolute versions.
	 * @param docId the ID of the document to load
	 * @param version the desired document version
	 * @param includeUpdateHistory include former update users and timestamps?
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public DocumentReader getDocumentAsStream(String docId, int version, boolean includeUpdateHistory) throws IOException {
		
		//	normalize UUID
		this.logActivity("GoldenGateSRS getting document '" + docId + "' as stream");
		long start = System.currentTimeMillis();
		docId = normalizeId(docId);
		
		//	try plain ID cache lookup first (fastest option)
		if ((this.dstCache != null) && (version == 0) && !includeUpdateHistory && this.dstCache.isDataObjectAvailable(docId)) try {
			DataObjectInputStream docIn = dstCache.getInputStream(docId);
			DocumentReader dr = new DocumentReader(new InputStreamReader(docIn, "UTF-8"), docIn.getDataObjectSize());
			dr.setAttribute(DOCUMENT_ID_ATTRIBUTE, docId);
			dr.setDocumentProperty(DOCUMENT_ID_ATTRIBUTE, docId);
			dr.setAttribute(DOCUMENT_VERSION_ATTRIBUTE, 0);
			this.logActivity(" - unresolved cache document reader obtained after " + (System.currentTimeMillis() - start) + " ms");
			return dr;
		}
		
		//	catch invalid document ID errors, as those can just as well indicate the need for resolving a UUID
		catch (IOException dnfe) {}
		
		//	try plain ID lookup first (way faster)
		try {
			DocumentReader dr = this.dst.loadDocumentAsStream(docId, version, includeUpdateHistory);
			this.logActivity(" - unresolved document reader obtained after " + (System.currentTimeMillis() - start) + " ms");
			return dr;
		}
		
		//	catch invalid document ID errors, as those can just as well indicate the need for resolving a UUID
		catch (DocumentNotFoundException dnfe) {}
		
		//	resolve UUID
		docId = this.resolveDocumentId(docId);
		
		//	return document
		DocumentReader dr = this.dst.loadDocumentAsStream(docId, version, includeUpdateHistory);
		this.logActivity(" - document reader obtained after " + (System.currentTimeMillis() - start) + " ms");
		return dr;
	}
	
	/**
	 * Add a document to the SRS's retrievable collection, or update a document
	 * already in the collection.
	 * @param masterDoc the document to add to the collection (may be split into
	 *            smaller retrieval units)
	 * @param logger a logger for obtaining detailed information on the storage
	 *            process
	 * @return the number of retrievable documents newly added to or updated in
	 *         the collection
	 * @throws IOException
	 */
	public int storeDocument(QueriableAnnotation masterDoc, EventLogger logger) throws IOException {
		String masterDocId = masterDoc.getAttribute(DOCUMENT_ID_ATTRIBUTE, masterDoc.getAnnotationID()).toString();
		masterDocId = normalizeId(masterDocId);
		long updateTime = -1;
		try {
			updateTime = Long.parseLong(masterDoc.getAttribute(UPDATE_TIME_ATTRIBUTE, "-1").toString());
		} catch (NumberFormatException nfe) {}
		if (updateTime == -1)
			updateTime = System.currentTimeMillis();
		
		if (logger != null)
			logger.writeLog(" ===== SRS Collection Update Protocol ===== ");
		
		//	log duration of storage process and its steps
		this.logInfo("GoldenGATE SRS: storing document '" + masterDoc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE) + "'");
		long storageStart = System.currentTimeMillis();
		long storageStepStart;
		
		//	run document through this.filters
		storageStepStart = System.currentTimeMillis();
		for (int f = 0; f < this.filters.length; f++) {
			String error = this.filters[f].filter(masterDoc);
			if (error != null) {
				if (logger != null)
					logger.writeLog("Cannot store document: " + error);
				return 0;
			}
		}
		this.logInfo(" - filtering done in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	split master document
		storageStepStart = System.currentTimeMillis();
		QueriableAnnotation[] docs = {masterDoc};
		String splitResultLabel = "Document";
		for (int s = 0; s < this.splitters.length; s++) {
			ArrayList docList = new ArrayList();
			for (int p = 0; p < docs.length; p++) {
				QueriableAnnotation[] subDocs = this.splitters[s].split(docs[p], logger);
				for (int i = 0; i < subDocs.length; i++)
					docList.add(subDocs[i]);
				splitResultLabel = this.splitters[s].getSplitResultLabel();
			}
			docs = ((QueriableAnnotation[]) docList.toArray(new QueriableAnnotation[docList.size()]));
		}
		this.logInfo(" - splitting done in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	if no split occurred, copy document to facilitate modification
		if ((docs.length == 1) && (docs[0] == masterDoc))
			docs[0] = Gamta.copyDocument(masterDoc);
		
		//	filter part documents
		storageStepStart = System.currentTimeMillis();
		for (int f = 0; f < this.filters.length; f++)
			docs = this.filters[f].filter(docs, masterDoc);
		this.logInfo(" - parts filtered in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	try and import UUID if (a) not already there and (b) respective plugin given
		storageStepStart = System.currentTimeMillis();
		for (int d = 0; d < docs.length; d++) {
			if (docs[d].hasAttribute(DOCUMENT_UUID_ATTRIBUTE))
				continue;
			for (int u = 0; u < this.uuidFetchers.length; u++) {
				if (this.uuidFetchers[u].addUuid(docs[d]))
					break;
			}
		}
		this.logInfo(" - UUIDs fetched in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	load data of existing master document parts
		storageStepStart = System.currentTimeMillis();
		String exDocQuery = ExistingDocumentData.getExistingDocumentDataQuery(masterDocId);
		SqlQueryResult sqr = null;
		HashMap exDocDataById = new HashMap();
		HashSet removedDocUuids = new HashSet();
		try {
			sqr = this.io.executeSelectQuery(exDocQuery);
			while (sqr.next()) {
				ExistingDocumentData exDocData = new ExistingDocumentData(sqr);
				exDocDataById.put(exDocData.docId, exDocData);
				if ((exDocData.docUuid != null) && (exDocData.docUuid.length() != 0))
					removedDocUuids.add(exDocData.docUuid);
			}
			this.logInfo("   - data for " + exDocDataById.size() + " existing documents read in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading data of existing master document parts.");
			this.logError("  Query was " + exDocQuery);
			throw new IOException(sqle.getMessage());
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	initialize statistics
		int newDocCount = 0;
		int updateDocCount = 0;
		
		//	store part documents
		storageStepStart = System.currentTimeMillis();
		StringVector validIDs = new StringVector();
		HashSet newDocIDs = new HashSet();
		ArrayList validDocIds = new ArrayList();
		HashMap validDocUuidsToNrs = new HashMap();
		for (int d = 0; d < docs.length; d++) {
			
			//	check ID
			String docId = ((String) docs[d].getAttribute(DOCUMENT_ID_ATTRIBUTE, ""));
			if (docId.length() == 0)
				continue;
			docId = normalizeId(docId);
			validIDs.addElement(docId);
			long docStorageStart = System.currentTimeMillis();
			
			//	copy administrative attributes
			if (!masterDocId.equals(docId))
				AttributeUtils.copyAttributes(masterDoc, docs[d], AttributeUtils.ADD_ATTRIBUTE_COPY_MODE);
			
			//	add master document ID (copy document if attempt on original one fails, which can happen if splitters do not work properly)
			try {
				docs[d].setAttribute(MASTER_DOCUMENT_ID_ATTRIBUTE, masterDocId);
			}
			catch (RuntimeException re) {
				docs[d] = Gamta.copyDocument(docs[d]);
				docs[d].setAttribute(MASTER_DOCUMENT_ID_ATTRIBUTE, masterDocId);
			}
			
			//	store part document
			int docStatus = this.updateDocument(docs[d], docId, masterDocId, updateTime, ((ExistingDocumentData) exDocDataById.remove(docId)), logger);
			
			//	update statistics
			if (docStatus == NEW_DOC) {
				newDocCount++;
				newDocIDs.add(docId);
			}
			else if (docStatus == UPDATE_DOC)
				updateDocCount++;
			
			//	store new UUID
			String docUuid = ((String) docs[d].getAttribute(DOCUMENT_UUID_ATTRIBUTE));
			if ((docUuid != null) && (docUuid.trim().length() != 0)) {
				docUuid = normalizeId(docUuid);
				removedDocUuids.remove(docUuid);
				validDocUuidsToNrs.put(docUuid, new Long(getDocNr(docId)));
			}
			
			//	store document ID
			validDocIds.add(docId);
			
			//	how long did this take?
			this.logInfo("   - part " + (d+1) + " of " + docs.length + " stored in " + (System.currentTimeMillis() - docStorageStart) + " ms");
		}
		this.logInfo(" - parts stored in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	update document number resolver if required
		if ((exDocDataById.size() != 0) || (newDocIDs.size() != 0)) {
			storageStepStart = System.currentTimeMillis();
			long[] removedDocNrs = new long[exDocDataById.size()];
			int removeDocNrIndex = 0;
			for (Iterator didit = exDocDataById.keySet().iterator(); didit.hasNext();) {
				String docId = ((String) didit.next());
				ExistingDocumentData edd = ((ExistingDocumentData) exDocDataById.get(docId));
				removedDocNrs[removeDocNrIndex++] = edd.docNumber;
			}
			synchronized (this.docIdentifierResolverLock) {
				this.docNumberResolver = this.docNumberResolver.cloneForChanges(newDocIDs, removedDocNrs);
			}
			this.logInfo(" - document number resolver updated in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		}
		
		//	update document UUID resolver if required
		if ((validDocUuidsToNrs.size() != 0) || (removedDocUuids.size() != 0)) {
			storageStepStart = System.currentTimeMillis();
			synchronized (this.docIdentifierResolverLock) {
				this.docUuidResolver = this.docUuidResolver.cloneForChanges(validDocUuidsToNrs, removedDocUuids);
			}
			this.logInfo(" - document UUID resolver updated in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		}
		
		//	clean up document table
		storageStepStart = System.currentTimeMillis();
		String updateUser = ((String) masterDoc.getAttribute(UPDATE_USER_ATTRIBUTE, masterDoc.getAttribute(CHECKIN_USER_ATTRIBUTE, "Unknown User")));
		int deleteDocCount = this.cleanupMasterDocument(updateUser, masterDocId, exDocDataById, logger);
		this.logInfo(" - master document cleanup done in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	update master document ID mapper
		if ((newDocCount != 0) || (deleteDocCount != 0)) {
			storageStepStart = System.currentTimeMillis();
			long[] docNrs = new long[validDocIds.size()];
			for (int d = 0; d < validDocIds.size(); d++) {
				String docId = ((String) validDocIds.get(d));
				docNrs[d] = getDocNr(docId);
			}
			synchronized (this.docIdentifierResolverLock) {
				this.masterDocIdMapper = this.masterDocIdMapper.cloneForChanges(masterDocId, docNrs);
			}
			this.logInfo(" - master document ID mapper updated in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		}
		
		//	enqueue 'finish master document' indexer action
		if ((newDocCount != 0) || (updateDocCount != 0) || (deleteDocCount != 0))
			for (int i = 0; i < this.indexers.length; i++) {
				this.enqueueIndexerAction(new IndexerAction(IndexerAction.FINISH_INDEX_NAME, this.indexers[i]) {
					void performAction() {
						this.indexer.masterDocumentFinished();
					}
				});
			}
		
		//	write log
		if (logger != null) {
			logger.writeLog("Document successfully stored in SRS collection:");
			logger.writeLog("  - " + newDocCount + " new " + splitResultLabel.toLowerCase() + ((newDocCount == 1) ? "" : "s") + " added");
			logger.writeLog("  - " + updateDocCount + " " + splitResultLabel.toLowerCase() + ((updateDocCount == 1) ? "" : "s") + " updated");
			logger.writeLog("  - " + deleteDocCount + " " + splitResultLabel.toLowerCase() + ((deleteDocCount == 1) ? "" : "s") + " deleted");
		}
		
		this.logInfo(" - document stored in " + (System.currentTimeMillis() - storageStart) + " ms");
		return (newDocCount + updateDocCount);
	}
	
	private static class ExistingDocumentData {
		final long docNumber;
		final String docId;
		final String docChecksum;
		final String docUuid;
		final String docUuidSource;
		final String masterDocTitle;
		final String docTitle;
		final String docAuthor;
		final String docOrigin;
		final int docYear;
		final int pageNumber;
		final int lastPageNumber;
		final int masterDocPageNumber;
		final int masterDocLastPageNumber;
		final String docSourceLink;
		final String docType;
		ExistingDocumentData(SqlQueryResult sqr) {
			this.docNumber = sqr.getLong(0);
			this.docId = sqr.getString(1);
			this.docChecksum = sqr.getString(2);
			this.docUuid = sqr.getString(3);
			this.docUuidSource = sqr.getString(4);
			this.masterDocTitle = sqr.getString(5);
			this.docTitle = sqr.getString(6);
			this.docAuthor = sqr.getString(7);
			this.docOrigin = sqr.getString(8);
			this.docYear = sqr.getInt(9);
			this.pageNumber = sqr.getInt(10);
			this.lastPageNumber = sqr.getInt(11);
			this.masterDocPageNumber = sqr.getInt(12);
			this.masterDocLastPageNumber = sqr.getInt(13);
			this.docSourceLink = sqr.getString(14);
			this.docType = sqr.getString(15);
		}
		static String getExistingDocumentDataQuery(String masterDocId) {
			return  "SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + "," + DOCUMENT_CHECKSUM_COLUMN_NAME + ", " + DOCUMENT_UUID_ATTRIBUTE + ", " + DOCUMENT_UUID_SOURCE_ATTRIBUTE + ", " + MASTER_DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_AUTHOR_ATTRIBUTE + ", " + DOCUMENT_ORIGIN_ATTRIBUTE + ", " + DOCUMENT_DATE_ATTRIBUTE + ", " + PAGE_NUMBER_ATTRIBUTE + ", " + LAST_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_LAST_PAGE_NUMBER_ATTRIBUTE + ", " + DOCUMENT_SOURCE_LINK_ATTRIBUTE + ", " + DOCUMENT_TYPE_ATTRIBUTE + 
					" FROM " + DOCUMENT_TABLE_NAME +
					" WHERE " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocId.hashCode() + "" +
						" AND " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'" +
					";";
		}
	}
	
	private static final int KEEP_DOC = 0;
	private static final int UPDATE_DOC = 1;
	private static final int NEW_DOC = 2;
	private int updateDocument(final QueriableAnnotation doc, final String docId, String masterDocId, final long updateTime, final ExistingDocumentData exDocData, final EventLogger logger) throws IOException {
		long storageStepStart;
		final long docNr = getDocNr(docId);
		final int documentStatus;
		
		storageStepStart = System.currentTimeMillis();
		String docChecksum = this.getChecksum(doc);
		String userName = ((String) doc.getAttribute(UPDATE_USER_ATTRIBUTE, doc.getAttribute(CHECKIN_USER_ATTRIBUTE, "Unknown User")));
		
		//	new document
		if (exDocData == null) {
			this.logDebug("  - creating document " + docNr);
			documentStatus = NEW_DOC;
			
			//	get document type
			String type = ((String) doc.getAttribute(DOCUMENT_TYPE_ATTRIBUTE, "")).trim();
			if (type.length() > 32)
				type = type.substring(0, 32).trim();
			
			//	get UUID
			String uuid = normalizeId((String) doc.getAttribute(DOCUMENT_UUID_ATTRIBUTE, ""));
			if (uuid.length() > 32)
				uuid = uuid.substring(0, 32);
			
			//	get UUID source
			String uuidSource = ((String) doc.getAttribute(DOCUMENT_UUID_SOURCE_ATTRIBUTE, ""));
			if (uuidSource.length() > 32)
				uuidSource = uuidSource.substring(0, 32);
			
			//	get checkin user
			String checkinUser = ((String) doc.getAttribute(CHECKIN_USER_ATTRIBUTE, "Unknown User"));
			if (checkinUser.length() > USER_LENGTH)
				checkinUser = checkinUser.substring(0, USER_LENGTH);
			
			//	get and (if necessary) truncate title 
			String masterTitle = ((String) doc.getAttribute(MASTER_DOCUMENT_TITLE_ATTRIBUTE, "Unknown Document")).trim();
			if (masterTitle.length() > MASTER_DOCUMENT_TITLE_COLUMN_LENGTH)
				masterTitle = masterTitle.substring(0, MASTER_DOCUMENT_TITLE_COLUMN_LENGTH);
			
			//	get and (if necessary) truncate title 
			String title = ((String) doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, "")).trim();
			if (title.length() > DOCUMENT_TITLE_COLUMN_LENGTH)
				title = title.substring(0, DOCUMENT_TITLE_COLUMN_LENGTH);
			
			//	get and (if necessary) truncate author 
			String author = ((String) doc.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, "Unknown Author")).trim();
			if (author.length() > DOCUMENT_AUTHOR_COLUMN_LENGTH)
				author = author.substring(0, DOCUMENT_AUTHOR_COLUMN_LENGTH);
			
			//	get origin
			String origin = ((String) doc.getAttribute(DOCUMENT_ORIGIN_ATTRIBUTE, "Unknown Journal or Book")).trim();
			if (origin.length() > DOCUMENT_ORIGIN_COLUMN_LENGTH)
				origin = origin.substring(0, DOCUMENT_ORIGIN_COLUMN_LENGTH);
			
			//	get year
			String year = ((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1")).trim();
			
			//	get source link
			String sourceLink = ((String) doc.getAttribute(DOCUMENT_SOURCE_LINK_ATTRIBUTE, "")).trim();
			if (sourceLink.length() > DOCUMENT_SOURCE_LINK_COLUMN_LENGTH)
				sourceLink = sourceLink.substring(0, DOCUMENT_SOURCE_LINK_COLUMN_LENGTH);
			
			//	get page number(s)
			int pageNumber = -1;
			try {
				pageNumber = Integer.parseInt((String) doc.getAttribute(PAGE_NUMBER_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			int lastPageNumber = -1;
			try {
				lastPageNumber = Integer.parseInt((String) doc.getAttribute(LAST_PAGE_NUMBER_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			
			//	get page number(s) of parent doc
			int masterDocPageNumber = -1;
			try {
				masterDocPageNumber = Integer.parseInt((String) doc.getAttribute(MASTER_PAGE_NUMBER_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			int masterDocLastPageNumber = -1;
			try {
				masterDocLastPageNumber = Integer.parseInt((String) doc.getAttribute(MASTER_LAST_PAGE_NUMBER_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			this.logInfo("   - document data assembled in " + (System.currentTimeMillis() - storageStepStart) + " ms");
			
			//	store data in collection main table
			storageStepStart = System.currentTimeMillis();
			String insertQuery = "INSERT INTO " + DOCUMENT_TABLE_NAME + " (" + 
								DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_TYPE_ATTRIBUTE + ", " + DOCUMENT_UUID_ATTRIBUTE + ", " + DOCUMENT_UUID_HASH_COLUMN_NAME + ", " + DOCUMENT_UUID_SOURCE_ATTRIBUTE + ", " + DOCUMENT_CHECKSUM_COLUMN_NAME + ", " + MASTER_DOCUMENT_ID_ATTRIBUTE + ", " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + ", " + CHECKIN_USER_ATTRIBUTE + ", " + CHECKIN_TIME_ATTRIBUTE + ", " + UPDATE_USER_ATTRIBUTE + ", " + UPDATE_TIME_ATTRIBUTE + ", " + MASTER_DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_AUTHOR_ATTRIBUTE + ", " + DOCUMENT_ORIGIN_ATTRIBUTE + ", " + DOCUMENT_DATE_ATTRIBUTE + ", " + DOCUMENT_SOURCE_LINK_ATTRIBUTE + ", " + PAGE_NUMBER_ATTRIBUTE + ", " + LAST_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_LAST_PAGE_NUMBER_ATTRIBUTE + ", " + DOCUMENT_SIZE_ATTRIBUTE + 
								") VALUES (" +
								docNr + ", '" + EasyIO.sqlEscape(docId) + "', '" + EasyIO.sqlEscape(type) + "', '" + EasyIO.sqlEscape(uuid) + "', " + uuid.hashCode() + ", '" + EasyIO.sqlEscape(uuidSource) + "', '" + EasyIO.sqlEscape(docChecksum) + "', '" + EasyIO.sqlEscape(masterDocId) + "', " + masterDocId.hashCode() + ", '" + EasyIO.sqlEscape(checkinUser) + "', " + updateTime + ", '" + EasyIO.sqlEscape(checkinUser) + "', " + updateTime + ", '" + EasyIO.sqlEscape(masterTitle) + "', '" + EasyIO.sqlEscape(title) + "', '" + EasyIO.sqlEscape(author) + "', '" + EasyIO.sqlEscape(origin) + "', " + EasyIO.sqlEscape(year) + ", '" + EasyIO.sqlEscape(sourceLink) + "', " + pageNumber + ", " + lastPageNumber + ", " + masterDocPageNumber + ", " + masterDocLastPageNumber + ", " + doc.size() + 
								");";
			try {
				this.io.executeUpdateQuery(insertQuery);
				this.logInfo("   - document data stored in " + (System.currentTimeMillis() - storageStepStart) + " ms");
			}
			catch (SQLException sqle) {
				this.logError("   - storing document data failed in " + (System.currentTimeMillis() - storageStepStart) + " ms");
				this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing document.");
				this.logError("  Query was " + insertQuery);
				throw new IOException("Could not store document: Error writing management table."); // don't store file if document couldn't be entered in management table
			}
		}
		
		//	document unchanged (or we have an MD5 collision ...)
		else if (docChecksum.equals(exDocData.docChecksum)) {
			this.logDebug("    - document unmodified");
			return KEEP_DOC;
		}
		
		//	document exists
		else {
			documentStatus = UPDATE_DOC;
			StringVector assignments = new StringVector();
			
			//	update checksum
			assignments.addElement(DOCUMENT_CHECKSUM_COLUMN_NAME + " = '" + EasyIO.sqlEscape(docChecksum) + "'");
			
			//	check document type
			String type = ((String) doc.getAttribute(DOCUMENT_TYPE_ATTRIBUTE, "")).trim();
			if (type.length() > 32)
				type = type.substring(0, 32).trim();
			if (!type.equals(exDocData.docType))
				assignments.addElement(DOCUMENT_TYPE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(type) + "'");
			
			//	check UUID
			String uuid = normalizeId((String) doc.getAttribute(DOCUMENT_UUID_ATTRIBUTE, ""));
			if (uuid.length() > 32)
				uuid = uuid.substring(0, 32);
			if (!uuid.equals(exDocData.docUuid)) {
				assignments.addElement(DOCUMENT_UUID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(uuid) + "'");
				assignments.addElement(DOCUMENT_UUID_HASH_COLUMN_NAME + " = " + uuid.hashCode() + "");
			}
			
			//	check UUID source
			String uuidSource = ((String) doc.getAttribute(DOCUMENT_UUID_SOURCE_ATTRIBUTE, "")).trim();
			if (uuidSource.length() > 32)
				uuidSource = uuidSource.substring(0, 32);
			if (!uuidSource.equals(exDocData.docUuidSource))
				assignments.addElement(DOCUMENT_UUID_SOURCE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(uuidSource) + "'");
			
			//	check and (if necessary) truncate title
			String masterTitle = ((String) doc.getAttribute(MASTER_DOCUMENT_TITLE_ATTRIBUTE, "Unknown Document")).trim();
			if (masterTitle.length() > MASTER_DOCUMENT_TITLE_COLUMN_LENGTH)
				masterTitle = masterTitle.substring(0, MASTER_DOCUMENT_TITLE_COLUMN_LENGTH);
			if (!masterTitle.equals(exDocData.masterDocTitle))
				assignments.addElement(MASTER_DOCUMENT_TITLE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterTitle) + "'");
			
			//	check and (if necessary) truncate part title
			String title = ((String) doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, "")).trim();
			if (title.length() > DOCUMENT_TITLE_COLUMN_LENGTH)
				title = title.substring(0, DOCUMENT_TITLE_COLUMN_LENGTH);
			if (!title.equals(exDocData.docTitle))
				assignments.addElement(DOCUMENT_TITLE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(title) + "'");
			
			//	check and (if necessary) truncate author
			String author = ((String) doc.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, "Unknown Author")).trim();
			if (author.length() > DOCUMENT_AUTHOR_COLUMN_LENGTH)
				author = author.substring(0, DOCUMENT_AUTHOR_COLUMN_LENGTH);
			if (!author.equals(exDocData.docAuthor))
				assignments.addElement(DOCUMENT_AUTHOR_ATTRIBUTE + " = '" + EasyIO.sqlEscape(author) + "'");
			
			//	check origin
			String origin = ((String) doc.getAttribute(DOCUMENT_ORIGIN_ATTRIBUTE, "Unknown Journal or Book")).trim();
			if (origin.length() > DOCUMENT_ORIGIN_COLUMN_LENGTH)
				origin = origin.substring(0, DOCUMENT_ORIGIN_COLUMN_LENGTH);
			if (!origin.equals(exDocData.docOrigin))
				assignments.addElement(DOCUMENT_ORIGIN_ATTRIBUTE + " = '" + EasyIO.sqlEscape(origin) + "'");
			
			//	check year
			int year = -1;
			try {
				year = Integer.parseInt((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			if (year != exDocData.docYear)
				assignments.addElement(DOCUMENT_DATE_ATTRIBUTE + " = " + year);
			
			//	check source link
			String sourceLink = ((String) doc.getAttribute(DOCUMENT_SOURCE_LINK_ATTRIBUTE, "")).trim();
			if (sourceLink.length() > DOCUMENT_SOURCE_LINK_COLUMN_LENGTH)
				sourceLink = sourceLink.substring(0, DOCUMENT_SOURCE_LINK_COLUMN_LENGTH);
			if (!sourceLink.equals(exDocData.docSourceLink))
				assignments.addElement(DOCUMENT_SOURCE_LINK_ATTRIBUTE + " = '" + EasyIO.sqlEscape(sourceLink) + "'");
			
			//	check page number
			int pageNumber = -1;
			try {
				pageNumber = Integer.parseInt((String) doc.getAttribute(PAGE_NUMBER_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			if (pageNumber != exDocData.pageNumber)
				assignments.addElement(PAGE_NUMBER_ATTRIBUTE + " = " + pageNumber);
			
			//	check last page number
			int lastPageNumber = -1;
			try {
				lastPageNumber = Integer.parseInt((String) doc.getAttribute(LAST_PAGE_NUMBER_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			if (lastPageNumber != exDocData.lastPageNumber)
				assignments.addElement(LAST_PAGE_NUMBER_ATTRIBUTE + " = " + lastPageNumber);
			
			//	check parent document page number
			int masterDocPageNumber = -1;
			try {
				masterDocPageNumber = Integer.parseInt((String) doc.getAttribute(MASTER_PAGE_NUMBER_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			if (masterDocPageNumber != exDocData.masterDocPageNumber)
				assignments.addElement(MASTER_PAGE_NUMBER_ATTRIBUTE + " = " + masterDocPageNumber);
			
			//	check last page number
			int masterDocLastPageNumber = -1;
			try {
				masterDocLastPageNumber = Integer.parseInt((String) doc.getAttribute(MASTER_LAST_PAGE_NUMBER_ATTRIBUTE, "-1"));
			} catch (NumberFormatException nfe) {}
			if (masterDocLastPageNumber != exDocData.masterDocLastPageNumber)
				assignments.addElement(MASTER_LAST_PAGE_NUMBER_ATTRIBUTE + " = " + masterDocLastPageNumber);
			
			//	get update user
			String updateUser = userName;
			if (updateUser.length() > USER_LENGTH)
				updateUser = updateUser.substring(0, USER_LENGTH);
			if (updateUser.length() != 0)
				assignments.addElement(UPDATE_USER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(updateUser) + "'");
			
			//	get update timestamp
			assignments.addElement(UPDATE_TIME_ATTRIBUTE + " = " + updateTime);					
			this.logInfo("   - document data compared in " + (System.currentTimeMillis() - storageStepStart) + " ms");
			
			//	write new values
			if (assignments.size() != 0) {
				storageStepStart = System.currentTimeMillis();
				String updateQuery = "UPDATE " + DOCUMENT_TABLE_NAME + 
						" SET " + assignments.concatStrings(", ") + 
						" WHERE " + DOC_NUMBER_COLUMN_NAME + " = " + docNr + "" +
							" AND " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
						";";
				try {
					this.io.executeUpdateQuery(updateQuery);
					this.logDebug("    - updates written");
				}
				catch (SQLException sqle) {
					this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating data of document " + docNr + ".");
					this.logError("  Query was " + updateQuery);
				}
				this.logInfo("   - document data updated in " + (System.currentTimeMillis() - storageStepStart) + " ms");
			}
			
			//	remove document from indexers, will be re-indexed later
//			for (int i = 0; i < this.indexers.length; i++) {
//				this.enqueueIndexerAction(new IndexerAction(IndexerAction.DELETE_NAME, this.indexers[i]) {
//					void performAction() {
//						this.indexer.deleteDocument(docNr);
//					}
//				});
//			}
			deleteIndexData(docNr);
		}
		
		//	invalidate collection statistics
		storageStepStart = System.currentTimeMillis();
		synchronized (this.collectionsStatisticsCache) {
			this.collectionsStatisticsCache.clear();
		}
		this.logInfo("   - statistics cache cleared in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	store document
		storageStepStart = System.currentTimeMillis();
		final int version = this.dst.storeDocument(doc, docId);
		this.dstLastModified = System.currentTimeMillis();
		this.logInfo("   - document stored in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		if (this.dstCache != null) {
			storageStepStart = System.currentTimeMillis();
			DataObjectOutputStream docCacheOut = this.dstCache.getOutputStream(docId);
			Writer out = new OutputStreamWriter(docCacheOut, "UTF-8");
			GenericGamtaXML.storeDocument(doc, out);
			out.flush();
			out.close();
			this.logInfo("   - document cached in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		}
		
		//	run document through indexers asynchronously for better upload performance
		storageStepStart = System.currentTimeMillis();
		final IndexData indexData = getIndexData(docNr, docId);
		for (int i = 0; i < this.indexers.length; i++) {
			this.enqueueIndexerAction(new IndexerAction(((exDocData == null) ? IndexerAction.INDEX_NAME : IndexerAction.RE_INDEX_NAME), this.indexers[i]) {
				void performAction() {
					IndexResult ir = ((exDocData == null) ? this.indexer.index(doc, docNr) : this.indexer.reIndex(doc, docNr));
					if ((ir == null) || !ir.hasNextElement())
						indexData.removeIndexResult(this.indexer.getIndexName());
					else indexData.addIndexResult(ir);
				}
			});
		}
		this.enqueueIndexerAction(new IndexerAction(IndexerAction.INDEX_NAME) {
			void performAction() {
				for (int a = 0; a < IndexData.documentAttributeNames.length; a++) {
					String value = defaultDocumentAttribute(IndexData.documentAttributeNames[a], ((String) doc.getAttribute(IndexData.documentAttributeNames[a])));
					indexData.setDocAttribute(IndexData.documentAttributeNames[a], value);
				}
			}
		});
		this.enqueueIndexerAction(new IndexerAction(IndexerAction.CACHE_INDEX_NAME) {
			void performAction() {
				storeIndexData(indexData);
			}
		});
		this.logInfo("   - indexing scheduled in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	do event notification asynchronously for quick response
		storageStepStart = System.currentTimeMillis();
		GoldenGateServerEventService.notify(new SrsDocumentEvent(userName, docId, masterDocId, doc, version, this.getClass().getName(), updateTime, logger));
		this.logInfo("   - update notification done in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	report whether update or insert
		return documentStatus;
	}
	
	//	after a document update, delete the management table entries for the parts of a master document that no longer exist
	private int cleanupMasterDocument(String userName, String masterDocId, HashMap remainingExDocData, EventLogger logger) throws IOException {
		int masterDeleteDocCount = 0;
		if (remainingExDocData.isEmpty())
			return masterDeleteDocCount;
		
		//	delete document management data in batches of 100 or so
		while (remainingExDocData.size() != 0) {
			StringBuffer deleteDocNumbers = new StringBuffer();
//			StringBuffer deleteDocIDs = new StringBuffer();
			int deleteDocCount = 0;
			
			//	create batch of document numbers and IDs for deletion
			for (Iterator didit = remainingExDocData.keySet().iterator(); didit.hasNext();) {
				String docId = ((String) didit.next());
				final ExistingDocumentData edd = ((ExistingDocumentData) remainingExDocData.get(docId));
				didit.remove();
				
				//	add document number and ID to deletion list
				if (deleteDocCount != 0) {
					deleteDocNumbers.append(", ");
//					deleteDocIDs.append(", ");
				}
				deleteDocNumbers.append(edd.docNumber);
//				deleteDocIDs.append("'" + edd.docId + "'");
				deleteDocCount++;
				
				//	un-index document
				for (int i = 0; i < this.indexers.length; i++)
					this.enqueueIndexerAction(new IndexerAction(IndexerAction.DELETE_NAME, this.indexers[i]) {
						void performAction() {
							this.indexer.deleteDocument(edd.docNumber);
						}
					});
				deleteIndexData(edd.docNumber);
				
				//	delete document proper
				try {
					this.dst.deleteDocument(docId);
					this.dstLastModified = System.currentTimeMillis();
					if (this.dstCache != null)
						this.dstCache.deleteDataObject(docId);
				}
				catch (IOException ioe) {
					this.logError("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
					this.logError(ioe);
				}
				
				//	issue event
				GoldenGateServerEventService.notify(new SrsDocumentEvent(userName, docId, masterDocId, this.getClass().getName(), System.currentTimeMillis(), logger));
				
				//	current batch full?
				if (deleteDocCount == 100)
					break;
			}
			
			//	delete master table entries for current batch
			String deleteQuery = "DELETE FROM " + DOCUMENT_TABLE_NAME + 
					" WHERE " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocId.hashCode() + "" +
						" AND " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'" +
						" AND " + DOC_NUMBER_COLUMN_NAME + " IN (" + deleteDocNumbers + ")" +
					";";
			try {
				int deleted = this.io.executeUpdateQuery(deleteQuery);
				masterDeleteDocCount += deleted;
			}
			catch (SQLException sqle) {
				this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
				this.logError("  Query was " + deleteQuery);
			}
		}
		
		//	invalidate collection statistics
		synchronized (this.collectionsStatisticsCache) {
			this.collectionsStatisticsCache.clear();
		}
		
		//	report
		return masterDeleteDocCount;
	}
	
	/**
	 * Delete a document from the retrievable collection
	 * @param userName the name of the user who caused the deletion
	 * @param masterDocId the ID of the document to delete (if the document with
	 *            this ID was split into smaller retrieval units, all these
	 *            retrieval units will be deleted)
	 * @param logger a logger for obtaining detailed information on the deletion
	 *            process
	 * @return the number of retrievable document removed from the collection
	 * @throws IOException
	 */
	public int deleteDocument(String userName, String masterDocId, EventLogger logger) throws IOException {
		
		//	normalize UUID
		masterDocId = normalizeId(masterDocId);
		
		//	initialize counter
		int deleteCount = 0;
		
		//	get numbers and IDs of doc to delete, no matter if specified ID belongs to a specific part or a document as a whole
		String query = "SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocId.hashCode() + "" +
				" AND " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'" +
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			while (sqr.next()) {
				final String docId = sqr.getString(1);
				final long docNr = sqr.getLong(0);
				
				//	remove part from indices
				for (int i = 0; i < this.indexers.length; i++) {
					this.enqueueIndexerAction(new IndexerAction(IndexerAction.DELETE_NAME, this.indexers[i]) {
						void performAction() {
							this.indexer.deleteDocument(docNr);
						}
					});
				}
				deleteIndexData(docNr);
				
				try {
					this.dst.deleteDocument(docId);
					this.dstLastModified = System.currentTimeMillis();
					if (this.dstCache != null)
						this.dstCache.deleteDataObject(docId);
				}
				catch (IOException ioe) {
					this.logError("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
					this.logError(ioe);
				}
				
				//	issue event
				GoldenGateServerEventService.notify(new SrsDocumentEvent(userName, docId, masterDocId, this.getClass().getName(), System.currentTimeMillis(), logger));
				
				//	update statistics
				deleteCount++;
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			this.logError("  Query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	delete entries from management table
		query = "DELETE FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocId.hashCode() + "" +
				" AND " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'" +
				";";
		try {
			this.io.executeUpdateQuery(query);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			this.logError("  Query was " + query);
		}
		
		//	invalidate collection statistics
		synchronized (this.collectionsStatisticsCache) {
			this.collectionsStatisticsCache.clear();
		}
		
		return deleteCount;
	}
	
	private static abstract class IndexerAction {
		static final String INDEX_NAME = "Index";
		static final String RE_INDEX_NAME = "Index";
		static final String FINISH_INDEX_NAME = "Finish";
		static final String CACHE_INDEX_NAME = "CacheIndex";
		static final String DELETE_NAME = "Delete";
		static final String CHECK_DOCUMENT = "CheckDocument";
		final String name;
		final Indexer indexer;
		long startTime;
		IndexerAction(String name) {
			this(name, null);
		}
		IndexerAction(String name, Indexer indexer) {
			this.name = name;
			this.indexer = indexer;
		}
		abstract void performAction();
	}
	
	private class IndexerServiceThread extends Thread {
		private boolean keepRunning = true;
		public void run() {
			
			//	complete handshake with creator thread
			synchronized (indexerActionQueue) {
				indexerActionQueue.notify();
			}
			
			//	run until shutdown() is called
			while (this.keepRunning) {
				
				//	check if indexer action waiting
				IndexerAction indexerAction;
				synchronized (indexerActionQueue) {
					if (indexerActionQueue.isEmpty()) {
						try {
							indexerActionQueue.wait();
						} catch (InterruptedException ie) {}
					}
					if (indexerActionQueue.isEmpty())
						return; // woken up despite empty queue ==> shutdown
					else indexerAction = ((IndexerAction) indexerActionQueue.removeFirst());
				}
				
				//	execute index action
				this.performIndexerAction(indexerAction);
				
				//	give a little time to the others
				if (this.keepRunning)
					Thread.yield();
			}
			
			//	work off remaining index actions
			while (indexerActionQueue.size() != 0)
				this.performIndexerAction(((IndexerAction) indexerActionQueue.removeFirst()));
		}
		
		private void performIndexerAction(IndexerAction ia) {
			try {
				indexerActionStarted(ia);
				ia.performAction();
			}
			catch (Throwable t) {
				logError("Error on index update '" + ia.name + "': " + t.getMessage());
				logError(t);
			}
			finally {
				indexerActionFinished(ia);
			}
		}
		
		void shutdown() {
			synchronized (indexerActionQueue) {
				this.keepRunning = false;
				indexerActionQueue.notify();
			}
			try {
				this.join();
			} catch (InterruptedException ie) {}
		}
	}
	
	LinkedList indexerActionQueue = new LinkedList();
	Map indexerActionStats = Collections.synchronizedMap(new TreeMap());
	IndexerActionStats allIndexerActionStats = new IndexerActionStats(null);
	void enqueueIndexerAction(IndexerAction ia) {
		IndexerActionStats ias;
		synchronized (this.indexerActionQueue) {
			this.indexerActionQueue.addLast(ia);
			this.indexerActionQueue.notify();
			ias = ((IndexerActionStats) this.indexerActionStats.get(ia.name));
			if (ias == null) {
				ias = new IndexerActionStats(ia.name);
				this.indexerActionStats.put(ia.name, ias);
			}
		}
		ias.actionEnqueued(); // need to do this here, as it synchronizes on the stats object
		this.allIndexerActionStats.actionEnqueued();
	}
	void indexerActionStarted(IndexerAction ia) {
		IndexerActionStats ias;
		synchronized (this.indexerActionQueue) {
			ias = ((IndexerActionStats) this.indexerActionStats.get(ia.name));
		}
		if (ias != null)
			ias.actionStarted(); // need to do this here, as it synchronizes on the stats object
		this.allIndexerActionStats.actionStarted();
		ia.startTime = System.currentTimeMillis();
	}
	void indexerActionFinished(IndexerAction ia) {
		IndexerActionStats ias;
		synchronized (this.indexerActionQueue) {
			ias = ((IndexerActionStats) this.indexerActionStats.get(ia.name));
		}
		int iaExecTime = ((int) (System.currentTimeMillis() - ia.startTime));
		if (ias != null)
			ias.actionFinished(iaExecTime); // need to do this here, as it synchronizes on the stats object
		this.allIndexerActionStats.actionFinished(iaExecTime);
	}
	private static class IndexerActionStats {
		final String name;
		int pendingCount = 0;
		int execCount = 0;
		long execTimeSum = 0;
		int minExecTime = Integer.MAX_VALUE;
		int maxExecTime = 0;
		IndexerActionStats(String name) {
			this.name = name;
		}
		synchronized void actionEnqueued() {
			this.pendingCount++;
		}
		synchronized void actionStarted() {
			this.pendingCount--;
		}
		synchronized void actionFinished(int execTime) {
			this.execCount++;
			this.execTimeSum += execTime;
			this.minExecTime = Math.min(this.minExecTime, execTime);
			this.maxExecTime = Math.max(this.maxExecTime, execTime);
		}
		public String toString() {
			return (((this.name == null) ? "" : (this.name + ": " )) + this.pendingCount + " pending, " + this.execCount + " executed" + ((this.execCount == 0) ? "" : (" (" + (this.execTimeSum / this.execCount) + "ms on average [" + this.minExecTime + "," + this.maxExecTime + "])")));
		}
	}
	
	boolean isIndexDataCached(long docNr) {
		String idId = getIndexDataId(docNr);
		return this.indexDataStore.isDataObjectAvailable(idId);
	}
	
	IndexData getIndexData(long docNr, String docId) {
		String idId = getIndexDataId(docNr);
		DataObjectInputStream idIn = null;
		try {
			idIn = this.indexDataStore.getInputStream(idId);
			long idLoadStart = System.currentTimeMillis();
			IndexData id = this.readIndexData(new BufferedReader(new InputStreamReader(idIn, "UTF-8")));
			long idLoadTime = (System.currentTimeMillis() - idLoadStart);
			if (idLoadTime > 800)
				this.logWarning("Cached index data loaded SLOW (in " + idLoadTime + "ms) for document " + docNr + " (" + idId + "), " + idIn.getDataObjectSize() + " bytes at " + (idIn.getDataObjectSize() / idLoadTime) + " bytes/ms");
			else if (idLoadTime > 400)
				this.logWarning("Cached index data loaded SLOw (in " + idLoadTime + "ms) for document " + docNr + " (" + idId + "), " + idIn.getDataObjectSize() + " bytes at " + (idIn.getDataObjectSize() / idLoadTime) + " bytes/ms");
			else if (idLoadTime > 200)
				this.logWarning("Cached index data loaded SLow (in " + idLoadTime + "ms) for document " + docNr + " (" + idId + "), " + idIn.getDataObjectSize() + " bytes at " + (idIn.getDataObjectSize() / idLoadTime) + " bytes/ms");
			else if (idLoadTime > 100)
				this.logWarning("Cached index data loaded Slow (in " + idLoadTime + "ms) for document " + docNr + " (" + idId + "), " + idIn.getDataObjectSize() + " bytes at " + (idIn.getDataObjectSize() / idLoadTime) + " bytes/ms");
			else if (idLoadTime > 50)
				this.logWarning("Cached index data loaded slow (in " + idLoadTime + "ms) for document " + docNr + " (" + idId + "), " + idIn.getDataObjectSize() + " bytes at " + (idIn.getDataObjectSize() / idLoadTime) + " bytes/ms");
			if ((docId == null) /* only if not loading for update ... */ && this.checkIndexDataDocAttributes(id, id.docId))
				return this.getIndexData(docNr, docId); // need to reload from disk, as storing reads through contained index results
			return id;
		}
		catch (DataObjectNotFoundException donf) {
			if (docId == null)
				this.logError("Cached index data not found for document " + docNr + " (" + idId + ")");
		}
		catch (IOException ioe) {
			this.logError("Error loading cached index data for document " + docNr + " (" + idId + "): " + ioe.getMessage());
			this.logError(ioe);
		}
		finally {
			if (idIn != null) try {
				idIn.close();
			} catch (IOException ioe) {}
		}
		return ((docId == null) ? null : new IndexData(docNr, docId));
	}
	
	private boolean checkIndexDataDocAttributes(IndexData indexData, String docId) {
		if (indexData.hasDocAttributes())
			return false;
		Properties docAttributes = this.getDocumentAttributes(docId);
		if (docAttributes == null)
			return false;
		if (this.setIndexDataDocAttributes(indexData, docAttributes)) {
			this.storeIndexData(indexData);
			return true;
		}
		else return false;
	}
	
	private boolean setIndexDataDocAttributes(IndexData indexData, Properties docAttributes) {
		boolean indexDataModified = false;
		for (int a = 0; a < IndexData.documentAttributeNames.length; a++) {
			String value = docAttributes.getProperty(IndexData.documentAttributeNames[a]);
			value = defaultDocumentAttribute(IndexData.documentAttributeNames[a], value);
			if (value == null)
				continue;
			value = value.trim();
			if (value.length() == 0)
				continue;
			if (indexData.setDocAttribute(IndexData.documentAttributeNames[a], value))
				indexDataModified = true;
		}
		return indexDataModified;
	}
	
	private IndexData readIndexData(Reader in) throws IOException {
		final IndexData[] indexData = {null};
		parser.stream(in, new TokenReceiver() {
			IndexResult reSubResult = null;
			LinkedList reSubResultElementList = null;
			
			String sreType = null;
			String sreValue = null;
			TreeNodeAttributeSet sreAttributes = null;
			
			public void storeToken(String token, int treeDepth) throws IOException {
				if (grammar.isTag(token)) {
					String type = grammar.getType(token);
					if (DocumentRoot.DOCUMENT_TYPE.equals(type)) {
						if (grammar.isSingularTag(token) || grammar.isEndTag(token)) {}
						else /* start of sub result list */ {
							TreeNodeAttributeSet idTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
							long docNr = Long.parseLong(idTnas.getAttribute(IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE));
							String docId = idTnas.getAttribute(DOCUMENT_ID_ATTRIBUTE);
							indexData[0] = new IndexData(docNr, docId);
							for (int a = 0; a < IndexData.documentAttributeNames.length; a++) {
								String value = idTnas.getAttribute(IndexData.documentAttributeNames[a]);
								if (value != null)
									indexData[0].setDocAttribute(IndexData.documentAttributeNames[a], value);
							}
						}
					}
					else if (IndexResult.SUB_RESULTS_NODE_NAME.equals(type)) /* start of sub result list */ {
						if (grammar.isSingularTag(token) || grammar.isEndTag(token)) {
							if ((this.reSubResult != null) && (this.reSubResultElementList.size() != 0)) {
								Collections.sort(this.reSubResultElementList, this.reSubResult.getSortOrder());
								indexData[0].addIndexResult(this.reSubResult);
							}
							this.reSubResult = null;
							this.reSubResultElementList = null;
						}
						else /* start of sub result */ {
							TreeNodeAttributeSet srTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
							StringVector fieldNames = new StringVector();
							fieldNames.parseAndAddElements(srTnas.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""), " ");
							fieldNames.removeAll("");
							String indexName = srTnas.getAttribute(RESULT_INDEX_NAME_ATTRIBUTE);
							String indexLabel = srTnas.getAttribute(RESULT_INDEX_LABEL_ATTRIBUTE, (indexName + " Index Data"));
							
							final LinkedList subResultElements = new LinkedList();
							this.reSubResultElementList = subResultElements;
							this.reSubResult = new IndexResult(fieldNames.toStringArray(), indexName, indexLabel) {
								public boolean hasNextElement() {
									return (subResultElements.size() != 0);
								}
								public SrsSearchResultElement getNextElement() {
									return ((SrsSearchResultElement) subResultElements.removeFirst());
								}
							};
						}
					}
					else {
						if (grammar.isSingularTag(token)) {
							TreeNodeAttributeSet sreTnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
							IndexResultElement subIre = new IndexResultElement(indexData[0].docNr, type, "");
							String[] attributeNames = sreTnas.getAttributeNames();
							for (int a = 0; a < attributeNames.length; a++) {
								String attributeValue = sreTnas.getAttribute(attributeNames[a]);
								if ((attributeValue != null) && (attributeValue.length() != 0))
									subIre.setAttribute(attributeNames[a], attributeValue);
							}
							this.reSubResultElementList.add(subIre);
						}
						else if (grammar.isEndTag(token)) {
							if ((this.reSubResult != null) && (this.sreType != null) && (this.sreAttributes != null) && (this.sreValue != null)) {
								IndexResultElement subIre = new IndexResultElement(indexData[0].docNr, this.sreType, this.sreValue);
								String[] attributeNames = this.sreAttributes.getAttributeNames();
								for (int a = 0; a < attributeNames.length; a++) {
									String attributeValue = this.sreAttributes.getAttribute(attributeNames[a]);
									if ((attributeValue != null) && (attributeValue.length() != 0))
										subIre.setAttribute(attributeNames[a], attributeValue);
								}
								this.reSubResultElementList.add(subIre);
							}
							this.sreType = null;
							this.sreValue = null;
							this.sreAttributes = null;
						}
						else {
							this.sreType = type;
							this.sreAttributes = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						}
					}
				}
				else if (this.sreType != null)
					this.sreValue = token;
			}
			public void close() throws IOException {}
		});
		
		//	return completed element
		return indexData[0];
	}
	
	IndexData createIndexData(long docNr) {
		DocumentNumberResolver docNumberResolver = this.getDocumentNumberResolver();
		String docId = docNumberResolver.getDocumentId(docNr);
		if (docId == null)
			return null;
		IndexData indexData = new IndexData(docNr, docId);
		Properties docAttributes = this.getDocumentAttributes(docId);
		if (docAttributes != null)
			setIndexDataDocAttributes(indexData, docAttributes);
		Query query = new Query();
		for (int i = 0; i < this.indexers.length; i++) {
			IndexResult ir = this.indexers[i].getIndexEntries(query, docNr, true);
			if ((ir != null) && ir.hasNextElement())
				indexData.addIndexResult(ir);
		}
		this.storeIndexData(indexData);
		return this.getIndexData(docNr, docId); // need to reload from disk, as storing reads through contained index results
	}
	
	void storeIndexData(IndexData indexData) {
		String idId = getIndexDataId(indexData.docNr);
		BufferedWriter idBw = null;
		try {
			idBw = new BufferedWriter(new OutputStreamWriter(this.indexDataStore.getOutputStream(idId), "UTF-8"));
			this.writeIndexData(indexData, idBw);
			idBw.flush();
		}
		catch (IOException ioe) {
			this.logError("Error caching index data for document " + indexData.docNr + " (" + idId + "): " + ioe.getMessage());
			this.logError(ioe);
		}
		finally {
			if (idBw != null) try {
				idBw.close();
			} catch (IOException ioe) {}
		}
	}
	
	private void writeIndexData(IndexData indexData, BufferedWriter out) throws IOException {
		out.write("<" + DocumentRoot.DOCUMENT_TYPE + 
				" " + IndexResultElement.DOCUMENT_NUMBER_ATTRIBUTE + "=\"" + indexData.docNr + "\"" +
				" " + DOCUMENT_ID_ATTRIBUTE + "=\"" + indexData.docId + "\"" +
				"");
		for (int a = 0; a < IndexData.documentAttributeNames.length; a++) {
			String value = indexData.getDocAttribute(IndexData.documentAttributeNames[a]);
			if (value != null)
				out.write(" " + IndexData.documentAttributeNames[a] + "=\"" + AnnotationUtils.escapeForXml(value) + "\"");
		}
		out.write(">");
		out.newLine();
		
		//	add sub results
		IndexResult[] subResults = indexData.getIndexResults();
		for (int s = 0; s < subResults.length; s++) {
			
			//	get fields of sub result
			StringVector subIndexFields = new StringVector();
			subIndexFields.addContent(subResults[s].resultAttributes);
			out.write("<" + SUB_RESULTS_NODE_NAME + 
					" " + RESULT_INDEX_NAME_ATTRIBUTE + "=\"" + subResults[s].indexName + "\"" +
					" " + RESULT_INDEX_LABEL_ATTRIBUTE + "=\"" + subResults[s].indexLabel + "\"" +
					" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"" + subIndexFields.concatStrings(" ") + "\"" +
			">");
			out.newLine();
			
			//	write sub result entries
			while (subResults[s].hasNextElement()) {
				out.write(subResults[s].getNextIndexResultElement().toXML());
				out.newLine();
			}
			
			//	close sub result
			out.write("</" + SUB_RESULTS_NODE_NAME + ">");
			out.newLine();
		}
		
		//	flush Writer if wrapped here
		out.write("</" + DocumentRoot.DOCUMENT_TYPE + ">");
		out.newLine();
	}
	
	void deleteIndexData(long docNr) {
		String idId = getIndexDataId(docNr);
		try {
			/* just fully eradicate it, no version history to preserve, and
			 * we'd only overwrite it on recreation anyway */
			this.indexDataStore.destroyDataObject(idId);
		}
		catch (IOException ioe) {
			this.logError("Error deleting cached index data for document " + docNr + " (" + idId + "): " + ioe.getMessage());
			this.logError(ioe);
		}
	}
	
	private static String getIndexDataId(long docNr) {
		//	=== implementation using four parts with int->HEX conversion and padding ===
//		String[] hexParts = {
//			Integer.toString((((int) (-1L >>> 48)) & 0xFFFF), 16).toUpperCase(),
//			Integer.toString((((int) (-1L >>> 32)) & 0xFFFF), 16).toUpperCase(),
//			Integer.toString((((int) (-1L >>> 16)) & 0xFFFF), 16).toUpperCase(),
//			Integer.toString((((int) (-1L >>> 0)) & 0xFFFF), 16).toUpperCase(),
//		};
//		for (int p = 0; p < hexParts.length; p++) {
//			while (hexParts[p].length() < 4)
//				hexParts[p] = ("0" + hexParts[p]);
//		}
//		return (hexParts[0] + hexParts[1] + hexParts[2] + hexParts[3]);
		//	=== implementation using two parts with long->HEX conversion and padding ===
//		String high = Long.toString((((int) (-1L >>> 32)) & 0xFFFFFFFFL), 16);
//		while (high.length() < 8)
//			high = ("0" + high);
//		String low = Long.toString((((int) (-1L >>> 0)) & 0xFFFFFFFFL), 16);
//		while (low.length() < 8)
//			low = ("0" + low);
//		return (high + low);
//		//	=== direct bit and array based implementation, no padding or case conversion ===
//		char[] hex = new char[16];
//		for (int h = 0; h < 16; h++) {
//			int ch = ((int) ((docNr >>> ((16 - 1 - h) * 4)) & 0x000000000000000FL));
//			hex[h] = ((char) ((ch < 10) ? ('0' + ch) : ('A' + (ch - 10))));
//		}
//		return new String(hex);
		//	=== direct bit and array based implementation producing full 32 characters, no padding or case conversion ===
		char[] hex = new char[32];
		for (int h = 0; h < 16; h++) {
			int chi = ((int) ((docNr >>> ((16 - 1 - h) * 4)) & 0x000000000000000FL));
			char ch = ((char) ((chi < 10) ? ('0' + chi) : ('A' + (chi - 10))));
			//	keep hex digits in place for higher part
			hex[h] = ch;
			//	rotate hex digits right by one for lower part to create better diversity for folder paths (no further branching on plain repetition)
			hex[16 + ((h + 1) % 16)] = ch;
		}
		return new String(hex);
	}
	
	//	parser and grammar for cached index data
	private static final Grammar grammar = new StandardGrammar();
	private static final Parser parser = new Parser(grammar);
	private static class IndexData implements GoldenGateSrsConstants {
		static final String[] documentAttributeNames = {
			DOCUMENT_TYPE_ATTRIBUTE,
			DOCUMENT_UUID_ATTRIBUTE,
			DOCUMENT_UUID_SOURCE_ATTRIBUTE,
			MASTER_DOCUMENT_ID_ATTRIBUTE,
			CHECKIN_USER_ATTRIBUTE,
			CHECKIN_TIME_ATTRIBUTE,
			UPDATE_USER_ATTRIBUTE,
			UPDATE_TIME_ATTRIBUTE,
			MASTER_DOCUMENT_TITLE_ATTRIBUTE,
			DOCUMENT_TITLE_ATTRIBUTE,
			DOCUMENT_AUTHOR_ATTRIBUTE,
			DOCUMENT_ORIGIN_ATTRIBUTE,
			DOCUMENT_DATE_ATTRIBUTE,
			DOCUMENT_SOURCE_LINK_ATTRIBUTE,
			PAGE_NUMBER_ATTRIBUTE,
			LAST_PAGE_NUMBER_ATTRIBUTE,
			MASTER_PAGE_NUMBER_ATTRIBUTE,
			MASTER_LAST_PAGE_NUMBER_ATTRIBUTE,
		};
		final long docNr;
		final String docId;
		private Properties docData;
		private Map indexResultsByName = Collections.synchronizedMap(new LinkedHashMap());
		IndexData(long docNr, String docId) {
			this.docNr = docNr;
			this.docId = docId;
		}
		boolean hasDocAttributes() {
			return (this.docData != null);
		}
		String getDocAttribute(String name) {
			if (DOCUMENT_ID_ATTRIBUTE.equals(name))
				return this.docId;
			else if (DOC_NUMBER_COLUMN_NAME.equals(name))
				return ("" + this.docNr);
			else if (this.docData == null)
				return null;
			else return this.docData.getProperty(name);
		}
		boolean setDocAttribute(String name, String value) {
			if (DOCUMENT_ID_ATTRIBUTE.equals(name))
				return false;
			else if (DOC_NUMBER_COLUMN_NAME.equals(name))
				return false;
			else if (value == null) {
				if (this.docData == null)
					return false;
				Object oldValue = this.docData.remove(name);
				if (this.docData.isEmpty())
					this.docData = null;
				return (oldValue != null);
			}
			else {
				if (this.docData == null)
					this.docData = new Properties();
				Object oldValue = this.docData.setProperty(name, value);
				return ((oldValue == null) ? true : !oldValue.equals(value));
			}
		}
//		String[] getDocAttributeNames() {
//			if (this.docData == null)
//				return new String[0];
//			String[] dans = new String[this.docData.size()];
//			int dani = 0;
//			for (Iterator danit = this.docData.keySet().iterator(); danit.hasNext();) {
//				String dan = ((String) danit.next());
//				dans[dani++] = dan;
//			}
//			return dans;
//		}
		void addIndexResult(IndexResult indexResult) {
			this.indexResultsByName.put(indexResult.indexName, indexResult);
		}
		IndexResult getIndexResult(String indexName) {
			return ((IndexResult) this.indexResultsByName.get(indexName));
		}
		IndexResult[] getIndexResults() {
			return ((IndexResult[]) this.indexResultsByName.values().toArray(new IndexResult[this.indexResultsByName.size()]));
		}
		void removeIndexResult(String indexName) {
			this.indexResultsByName.remove(indexName);
		}
	}
	
	private static final String[] masterDocumentListFields = {
			DOCUMENT_ID_ATTRIBUTE,
			MASTER_DOCUMENT_ID_ATTRIBUTE,
			DOCUMENT_AUTHOR_ATTRIBUTE,
			DOCUMENT_ORIGIN_ATTRIBUTE,
			DOCUMENT_DATE_ATTRIBUTE,
			DOCUMENT_TITLE_ATTRIBUTE,
			PAGE_NUMBER_ATTRIBUTE,
			LAST_PAGE_NUMBER_ATTRIBUTE,
			DOCUMENT_SIZE_ATTRIBUTE,
		SUB_DOCUMENT_COUNT_ATTRIBUTE,
			CHECKIN_USER_ATTRIBUTE,
			CHECKIN_TIME_ATTRIBUTE,
			UPDATE_USER_ATTRIBUTE,
			UPDATE_TIME_ATTRIBUTE,
		};
	private static final String[][] masterDocumentListQueryFields = {
			{MASTER_DOCUMENT_ID_ATTRIBUTE, DOCUMENT_ID_ATTRIBUTE},
			{MASTER_DOCUMENT_ID_ATTRIBUTE, MASTER_DOCUMENT_ID_ATTRIBUTE},
			{"min(" + DOC_NUMBER_COLUMN_NAME + ")", DOC_NUMBER_COLUMN_NAME},
			
			{"count(" + DOCUMENT_ID_ATTRIBUTE + ")", SUB_DOCUMENT_COUNT_ATTRIBUTE},
			{"sum(" + DOCUMENT_SIZE_ATTRIBUTE + ")", DOCUMENT_SIZE_ATTRIBUTE},
			
			{"min(" + CHECKIN_USER_ATTRIBUTE + ")", CHECKIN_USER_ATTRIBUTE},
			{"min(" + CHECKIN_TIME_ATTRIBUTE + ")", CHECKIN_TIME_ATTRIBUTE},
			
			{"min(" + UPDATE_USER_ATTRIBUTE + ")", UPDATE_USER_ATTRIBUTE},
			{"min(" + UPDATE_TIME_ATTRIBUTE + ")", UPDATE_TIME_ATTRIBUTE},
			
			{"min(" + MASTER_DOCUMENT_TITLE_ATTRIBUTE + ")", DOCUMENT_TITLE_ATTRIBUTE},
			{"min(" + DOCUMENT_AUTHOR_ATTRIBUTE + ")", DOCUMENT_AUTHOR_ATTRIBUTE},
			{"min(" + DOCUMENT_ORIGIN_ATTRIBUTE + ")", DOCUMENT_ORIGIN_ATTRIBUTE},
			{"min(" + DOCUMENT_DATE_ATTRIBUTE + ")", DOCUMENT_DATE_ATTRIBUTE},
			
			{"min(" + MASTER_PAGE_NUMBER_ATTRIBUTE + ")", PAGE_NUMBER_ATTRIBUTE},
			{"max(" + MASTER_LAST_PAGE_NUMBER_ATTRIBUTE + ")", LAST_PAGE_NUMBER_ATTRIBUTE},
		};
	
	private static final String[] documentListFields = {
			DOCUMENT_ID_ATTRIBUTE,
			DOCUMENT_TYPE_ATTRIBUTE,
			DOCUMENT_UUID_ATTRIBUTE,
			DOCUMENT_UUID_SOURCE_ATTRIBUTE,
			MASTER_DOCUMENT_ID_ATTRIBUTE,
			DOCUMENT_AUTHOR_ATTRIBUTE,
			DOCUMENT_ORIGIN_ATTRIBUTE,
			DOCUMENT_DATE_ATTRIBUTE,
		MASTER_DOCUMENT_TITLE_ATTRIBUTE,
			DOCUMENT_TITLE_ATTRIBUTE,
			PAGE_NUMBER_ATTRIBUTE,
			LAST_PAGE_NUMBER_ATTRIBUTE,
		MASTER_PAGE_NUMBER_ATTRIBUTE,
		MASTER_LAST_PAGE_NUMBER_ATTRIBUTE,
			DOCUMENT_SIZE_ATTRIBUTE,
			CHECKIN_USER_ATTRIBUTE,
			CHECKIN_TIME_ATTRIBUTE,
			UPDATE_USER_ATTRIBUTE,
			UPDATE_TIME_ATTRIBUTE,
		};
	
	private static final String[] documentListQueryFields = {
			DOC_NUMBER_COLUMN_NAME,
			DOCUMENT_ID_ATTRIBUTE,
			DOCUMENT_TYPE_ATTRIBUTE,
			DOCUMENT_UUID_ATTRIBUTE,
			DOCUMENT_UUID_SOURCE_ATTRIBUTE,
			MASTER_DOCUMENT_ID_ATTRIBUTE,
			CHECKIN_USER_ATTRIBUTE,
			CHECKIN_TIME_ATTRIBUTE,
			UPDATE_USER_ATTRIBUTE,
			UPDATE_TIME_ATTRIBUTE,
			MASTER_DOCUMENT_TITLE_ATTRIBUTE,
			DOCUMENT_TITLE_ATTRIBUTE,
			DOCUMENT_AUTHOR_ATTRIBUTE,
			DOCUMENT_ORIGIN_ATTRIBUTE,
			DOCUMENT_DATE_ATTRIBUTE,
			PAGE_NUMBER_ATTRIBUTE,
			LAST_PAGE_NUMBER_ATTRIBUTE,
			MASTER_PAGE_NUMBER_ATTRIBUTE, 
			MASTER_LAST_PAGE_NUMBER_ATTRIBUTE,
			DOCUMENT_SIZE_ATTRIBUTE,
		};
	
	/**
	 * Retrieve the number of the documents hosted in GoldenGATE SRS for retrieval.
	 * @param masterDocId the ID of the parent document to count the retrievable
	 *            parts of (specifying null count all)
	 * @return the number of the documents in the SRS' collection
	 * @throws IOException
	 */
	public int getDocumentCount(String masterDocId) throws IOException {
		MasterDocumentIdMapper masterDocIdMapper = this.getMasterDocumentIdMapper();
		if (masterDocId == null)
			return masterDocIdMapper.getDocumentCount();
		masterDocId = normalizeId(masterDocId);
		return masterDocIdMapper.getDocumentCount(masterDocId);
	}
	
	/**
	 * Retrieve a list of the documents hosted in GoldenGATE SRS for retrieval.
	 * @param masterDocId the ID of the parent document to list the retrievable
	 *            parts of (specifying null will return the list of master
	 *            documents)
	 * @return the list of the documents in the SRS' collection
	 * @throws IOException
	 */
	public DocumentList getDocumentList(String masterDocId) throws IOException {
		final DocumentList docList;
		
		//	request for master list
		if (masterDocId == null) {
			
			final String[] queryFields = new String[masterDocumentListQueryFields.length];
			
			StringBuffer query = new StringBuffer("SELECT ");
			for (int q = 0; q < masterDocumentListQueryFields.length; q++) {
				if (q != 0)
					query.append(", ");
				query.append(masterDocumentListQueryFields[q][0] + " AS " + masterDocumentListQueryFields[q][1]);
				queryFields[q] = masterDocumentListQueryFields[q][1];
			}
			query.append(" FROM " + DOCUMENT_TABLE_NAME);
			query.append(" GROUP BY " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + ", " + MASTER_DOCUMENT_ID_ATTRIBUTE);
			query.append(" ORDER BY " + CHECKIN_TIME_ATTRIBUTE);
			query.append(";");
			
			SqlQueryResult sqr = null;
			try {
				sqr = this.io.executeSelectQuery(query.toString());
				docList = new SqlDocumentList(masterDocumentListFields, sqr) {
					protected DocumentListElement decodeListElement(String[] elementData) {
						DocumentListElement dle = new DocumentListElement();
						for (int f = 0; f < queryFields.length; f++) {
							if (elementData[f] != null)
								dle.setAttribute(queryFields[f], elementData[f]);
						}
						return dle;
					}
				};
			}
			catch (SQLException sqle) {
				this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
				this.logError("  query was " + query);
				throw new IOException(sqle.getMessage());
			}
		}
		
		//	request for sub document list
		else {
			masterDocId = normalizeId(masterDocId);
			
			StringBuffer query = new StringBuffer("SELECT ");
			for (int f = 0; f < documentListQueryFields.length; f++) {
				if (f != 0)
					query.append(", ");
				query.append(documentListQueryFields[f]);
			}
			query.append(" FROM " + DOCUMENT_TABLE_NAME);
			query.append(" WHERE " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocId.hashCode() + "");
			query.append(" AND " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'");
			query.append(" ORDER BY " + DOC_NUMBER_COLUMN_NAME);
			query.append(";");
			
			SqlQueryResult sqr = null;
			try {
				sqr = this.io.executeSelectQuery(query.toString());
				docList = new SqlDocumentList(documentListFields, sqr) {
					protected DocumentListElement decodeListElement(String[] elementData) {
						DocumentListElement dle = new DocumentListElement();
						for (int f = 0; f < documentListQueryFields.length; f++) {
							if ((elementData[f] == null) || (elementData[f].trim().length() == 0))
								continue;
							if (DOCUMENT_UUID_ATTRIBUTE.equals(documentListQueryFields[f]) && (elementData[f].length() >= 32))
								dle.setAttribute(documentListQueryFields[f], (elementData[f].substring(0, 8) + "-" + elementData[f].substring(8, 12) + "-" + elementData[f].substring(12, 16) + "-" + elementData[f].substring(16, 20) + "-" + elementData[f].substring(20)));
							else dle.setAttribute(documentListQueryFields[f], elementData[f]);
						}
						return dle;
					}
				};
			}
			catch (SQLException sqle) {
				this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
				this.logError("  query was " + query);
				throw new IOException(sqle.getMessage());
			}
		}
		
		//	no extensions to add
		if (this.documentListExtensions.isEmpty())
			return docList;
		
		//	build extended field list, collect data along the way
		StringVector docListFields = new StringVector();
		docListFields.addContent(docList.resultAttributes);
		final HashSet extensionIndexNames = new HashSet();
		final HashMap extensionFields = new HashMap();
		for (Iterator eit = this.documentListExtensions.keySet().iterator(); eit.hasNext();) {
			String fieldName = ((String) eit.next());
			if (!docListFields.containsIgnoreCase(fieldName)) {
				String[] data = ((String[]) this.documentListExtensions.get(fieldName));
				if (data != null) {
					docListFields.addElement(fieldName);
					extensionIndexNames.add(data[0]);
					extensionFields.put((data[0] + "." + data[1]), fieldName);
				}
			}
		}
		
		//	no valid extensions found, we're done
		if (extensionFields.isEmpty())
			return docList;
		
		//	wrap document list to add extension fields
		return new DocumentList(docListFields.toStringArray()) {
			public boolean hasNextElement() {
				return docList.hasNextElement();
			}
			public SrsSearchResultElement getNextElement() {
				DocumentListElement dle = docList.getNextDocumentListElement();
				if (dle == null)
					return dle;
				Long docNr = new Long((String) dle.getAttribute(DOC_NUMBER_COLUMN_NAME, "-1"));
				if (docNr == -1)
					return dle;
				IndexData indexData = getIndexData(docNr, null);
				if (indexData == null)
					indexData = createIndexData(docNr);
				IndexResult[] extensionIrs = indexData.getIndexResults();
				for (int e = 0; e < extensionIrs.length; e++) {
					if (!extensionIndexNames.contains(extensionIrs[e].indexName))
						continue;
					if (!extensionIrs[e].hasNextElement())
						continue;
					IndexResultElement extensionIre = extensionIrs[e].getNextIndexResultElement();
					String[] extensionIreAttributeNames = extensionIre.getAttributeNames();
					for (int a = 0; a < extensionIreAttributeNames.length; a++) {
						String extensionFieldName = ((String) extensionFields.get(extensionIrs[e].indexName + "." + extensionIreAttributeNames[a]));
						if (extensionFieldName != null)
							dle.setAttribute(extensionFieldName, extensionIre.getAttribute(extensionIreAttributeNames[a]));
					}
				}
				return dle;
			}
		};
	}
	
	/**
	 * Retrieve a list of the documents hosted in GoldenGATE SRS for retrieval.
	 * As this method retrieves all individual documents, it can incur
	 * considerable effort, both for generation and for processing. Use with
	 * care.
	 * @param orderField the field to order the list by (prepend with '-' for
	 *            descending order); an invalid field name will simply be
	 *            ignored
	 * @return the list of the documents in the SRS' collection
	 * @throws IOException
	 */
	public DocumentList getDocumentListFull(String orderField) throws IOException {
		StringBuffer query = new StringBuffer("SELECT ");
		for (int f = 0; f < documentListQueryFields.length; f++) {
			if (f != 0)
				query.append(", ");
			query.append(documentListQueryFields[f]);
		}
		query.append(" FROM " + DOCUMENT_TABLE_NAME);
		if ((orderField != null) && (orderField.length() != 0)) {
			String orderDirection = "";
			if (orderField.startsWith("-")) {
				orderField = orderField.substring("-".length());
				orderDirection = " DESC";
			}
			for (int f = 0; f < documentListQueryFields.length; f++)
				if (documentListQueryFields[f].equalsIgnoreCase(orderField)) {
					query.append(" ORDER BY " + orderField + orderDirection + ";");
					break;
				}
		}
		query.append(";");
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query.toString());
			return new SqlDocumentList(documentListFields, sqr) {
				protected DocumentListElement decodeListElement(String[] elementData) {
					DocumentListElement dle = new DocumentListElement();
					for (int f = 0; f < documentListQueryFields.length; f++) {
						if ((elementData[f] == null) || (elementData[f].trim().length() == 0))
							continue;
						if (DOCUMENT_UUID_ATTRIBUTE.equals(documentListQueryFields[f]) && (elementData[f].length() >= 32))
							dle.setAttribute(documentListQueryFields[f], (elementData[f].substring(0, 8) + "-" + elementData[f].substring(8, 12) + "-" + elementData[f].substring(12, 16) + "-" + elementData[f].substring(16, 20) + "-" + elementData[f].substring(20)));
						else dle.setAttribute(documentListQueryFields[f], elementData[f]);
					}
					return dle;
				}
			};
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
			this.logError("  query was " + query);
			throw new IOException(sqle.getMessage());
		}
	}
	
	private static abstract class SqlDocumentList extends DocumentList {
		protected SqlResultDataSource data;
		protected SqlDocumentList(String[] dataFieldNames, SqlQueryResult sqr) {
			super(dataFieldNames);
			this.data = new SqlResultDataSource(sqr);
		}
		public boolean hasNextElement() {
			return this.data.hasNextElement();
		}
		public SrsSearchResultElement getNextElement() {
			String[] elementData = this.data.getNextElementData();
			if (elementData == null)
				return null;
			return this.decodeListElement(elementData);
		}
		protected abstract DocumentListElement decodeListElement(String[] elementData);
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
			if ((this.sqr == null) || this.gotElementInBuffer())
				return;
			else if (this.sqr.next()) {
				String[] dataCollector = new String[this.sqr.getColumnCount()];
				for (int c = 0; c < dataCollector.length; c++)
					dataCollector[c] = this.sqr.getString(c);
				this.nextElementData = dataCollector;
			}
			else {
				this.sqr.close();
				this.sqr = null;
			}
		}
		
		private boolean gotElementInBuffer() {
			return (this.nextElementData != null);
		}
		
		private String[] getFromElementBuffer() {
			String[] nextElementData = this.nextElementData;
			this.nextElementData = null;
			return nextElementData;
		}
		
		protected void finalize() throws Throwable {
			if (this.sqr != null) {
				this.sqr.close();
				this.sqr = null;
			}
		}
	}
	
	private static final String USE_MIN_MERGE_MODE_NAME = "USE_MIN";
	private static final String USE_AVERAGE_MERGE_MODE_NAME = "USE_AVERAGE";
	private static final String USE_MAX_MERGE_MODE_NAME = "USE_MAX";
	private static final String MULTIPLY_MERGE_MODE_NAME = "MULTIPLY";
	private static final String INVERSE_MULTIPLY_MERGE_MODE_NAME = "INVERSE_MULTIPLY";
	
	private static final String RESULT_PIVOT_INDEX_SETTING_NAME = "RESULT_PIVOT_INDEX";
	private static final int DEFAULT_RESULT_PIVOT_INDEX = 20;
	private int resultPivotIndex = DEFAULT_RESULT_PIVOT_INDEX; // relevance of result at this index is used as result pruning threshold 
	
	private static final String RESULT_MERGE_MODE_SETTING_NAME = "RESULT_MERGE_MODE";
	private static final String DEFAULT_RESULT_MERRGE_MODE_NAME = USE_AVERAGE_MERGE_MODE_NAME;
	private static final int DEFAULT_RESULT_MERRGE_MODE = QueryResult.USE_AVERAGE;
	private int resultMergeMode = DEFAULT_RESULT_MERRGE_MODE;
	
	private static final String[] documentSearchResultAttributes = {
		DOCUMENT_ID_ATTRIBUTE,
		DOCUMENT_TYPE_ATTRIBUTE,
		DOCUMENT_UUID_ATTRIBUTE,
		DOCUMENT_UUID_SOURCE_ATTRIBUTE,
		MASTER_DOCUMENT_ID_ATTRIBUTE,
		CHECKIN_USER_ATTRIBUTE,
		MASTER_DOCUMENT_TITLE_ATTRIBUTE,
		DOCUMENT_TITLE_ATTRIBUTE,
		DOCUMENT_AUTHOR_ATTRIBUTE,
		DOCUMENT_ORIGIN_ATTRIBUTE,
		DOCUMENT_DATE_ATTRIBUTE,
		DOCUMENT_SOURCE_LINK_ATTRIBUTE,
		PAGE_NUMBER_ATTRIBUTE,
		LAST_PAGE_NUMBER_ATTRIBUTE,
		MASTER_PAGE_NUMBER_ATTRIBUTE,
		MASTER_LAST_PAGE_NUMBER_ATTRIBUTE
	};
	
	/* TODO Assess if we need to re-introduce result element buffering:
	 * - reduces switching back and forth between reading from disk and writing response to socket
	 * - see how performance changes, though ...
	 */
	
	/**
	 * Search documents. The elements of the returned document result contain
	 * both the basic document meta data and the documents themselves.
	 * @param query the query containing the search parameters
	 * @param includeUpdateHistory include all update users and timestamps?
	 * @return a document result to iterate over the matching documents
	 * @throws IOException
	 */
	public DocumentResult searchDocuments(Query query, final boolean includeUpdateHistory) throws IOException {
		this.logActivity("GgSRS: doing document search ...");
		
		//	get document numbers
		QueryResult docNrResult = this.searchDocumentNumbers(query);
		this.logActivity("  - got " + docNrResult.size() + " result document numbers");
		
		//	test if we have an ID query, and waive relevance elimination if so
		boolean isIdQuery = (query.getValue(ID_QUERY_FIELD_NAME) != null);
		final int docVersion;
		if (isIdQuery) {
			String queryVersion = query.getValue(VERSION_QUERY_FIELD_NAME);
			if (queryVersion == null)
				docVersion = 0;
			else {
				int dv = 0;
				try {
					dv = Integer.parseInt(queryVersion);
				} catch (NumberFormatException nfe) {}
				docVersion = dv;
			}
		}
		else docVersion = 0;
		
		//	read additional parameters
		int queryResultPivotIndex = (isIdQuery ? 0 : this.resultPivotIndex);
		try {
			queryResultPivotIndex = Integer.parseInt(query.getValue(RESULT_PIVOT_INDEX_PARAMETER, ("" + queryResultPivotIndex)));
		} catch (NumberFormatException nfe) {}
		double minRelevance = 0;
		try {
			minRelevance = Double.parseDouble(query.getValue(MINIMUM_RELEVANCE_PARAMETER, ("" + minRelevance)));
		} catch (NumberFormatException nfe) {}
		if (minRelevance > 1.0) minRelevance = 0;
		else if (minRelevance < 0) minRelevance = 0;
		
		//	do duplicate and relevance based elimination
		final QueryResult relevanceResult = this.doRelevanceElimination(docNrResult, minRelevance, queryResultPivotIndex);
		
		//	catch empty result
		if (relevanceResult.size() == 0) return new DocumentResult() {
			public boolean hasNextElement() {
				return false;
			}
			public SrsSearchResultElement getNextElement() {
				return null;
			}
		};
		
		//	add search attributes to document annotations?
		final boolean markSearcheables = (query.getValue(MARK_SEARCHABLES_PARAMETER) != null);
		
		//	return result fetching documents on the fly
		final DocumentNumberResolver docNumberResolver = this.getDocumentNumberResolver();
		final ArrayList staleDocNumbers = new ArrayList();
		DocumentResult dr = new DocumentResult(documentSearchResultAttributes) {
			private int loaded = 0;
			private int qreIndex = 0;
			private DocumentResultElement nextDre;
			public boolean hasNextElement() {
				if (this.nextDre != null)
					return true;
				
				//	produce next element
				while (this.qreIndex < relevanceResult.size()) {
					QueryResultElement qre = relevanceResult.getResult(this.qreIndex++);
					logActivity("  - adding result document " + qre.docNr + " (" + qre.relevance + ")");
					
					//	get document ID
					String docId = docNumberResolver.getDocumentId(qre.docNr);
					if (docId == null) {
//						checkStaleDocumentNumber(qre.docNr);
						staleDocNumbers.add(new Long(qre.docNr));
						continue;
					}
					
					//	load actual document (try cache first)
					DocumentRoot doc = null;
					if ((dstCache != null) && (docVersion == 0) && !includeUpdateHistory && dstCache.isDataObjectAvailable(docId)) try {
						DataObjectInputStream docIn = dstCache.getInputStream(docId);
						DocumentReader dr = new DocumentReader(new InputStreamReader(docIn, "UTF-8"), docIn.getDataObjectSize());
						dr.setAttribute(DOCUMENT_ID_ATTRIBUTE, docId);
						dr.setAttribute(DOCUMENT_VERSION_ATTRIBUTE, 0);
						doc = GenericGamtaXML.readDocument(dr);
						dr.close();
						logActivity("    - loaded document with " + doc.size() + " tokens from cache");
					}
					catch (IOException ioe) {
						logError("GoldenGateSRS: Error loading document '" + docId + "' from cache (" + ioe.getMessage() + ")");
						logError(ioe);
					}
					if (doc == null) try {
						doc = dst.loadDocument(docId, docVersion, includeUpdateHistory);
						logActivity("    - loaded document with " + doc.size() + " tokens");
					}
					catch (IOException ioe) {
						logError("GoldenGateSRS: Error loading document '" + docId + "' (" + ioe.getMessage() + ")");
						logError(ioe);
					}
					if (doc == null)
						continue;
					
					//	mark searchables if required
					if (markSearcheables) {
						for (int i = 0; i < indexers.length; i++)
							indexers[i].markSearchables(doc);
					}
					
					//	wrap new document result element around document
					DocumentResultElement dre = new DocumentResultElement(qre.docNr, docId, qre.relevance, doc);
					
					//	populate result attributes
					for (int a = 0; a < this.resultAttributes.length; a++) {
						String value = ((String) doc.getAttribute(this.resultAttributes[a]));
						if (value == null)
							continue;
						value = value.trim();
						if (value.length() == 0)
							continue;
						if (DOCUMENT_UUID_ATTRIBUTE.equals(this.resultAttributes[a]) && (value.indexOf('-') == -1) && (value.length() >= 32)) {
							value = (value.substring(0, 8) + "-" + value.substring(8, 12) + "-" + value.substring(12, 16) + "-" + value.substring(16, 20) + "-" + value.substring(20));							}
						dre.setAttribute(this.resultAttributes[a], value);
					}
					
					//	store result element, and remember last relevance for delaying cutoff
					this.nextDre = dre;
					this.loaded++;
					logActivity("    ==> loaded total of " + this.loaded + " documents");
					break;
				}
				
				//	anything to work with?
				return (this.nextDre != null);
			}
			public SrsSearchResultElement getNextElement() {
				DocumentResultElement dre = this.nextDre;
				this.nextDre = null;
				return dre;
			}
		};
		
		//	schedule checking stale document numbers
		this.checkStaleDocumentNumbers(staleDocNumbers);
		
		//	finally ...
		return dr;
	}
	
	/**
	 * Search documents. The elements of the returned document result contain
	 * both the basic document meta data and the documents themselves.
	 * @param query the query containing the search parameters
	 * @param includeUpdateHistory include all update users and timestamps?
	 * @return a document result to iterate over the matching documents
	 * @throws IOException
	 */
	public DocumentResult searchDocumentDetails(Query query, boolean includeUpdateHistory) throws IOException {
		
		//	get result
		final DocumentResult fullDr = searchDocuments(query, includeUpdateHistory);
		
		//	wrap document result to reduce documents
		return new DocumentResult() {
			public boolean hasNextElement() {
				return fullDr.hasNextElement();
			}
			public SrsSearchResultElement getNextElement() {
				DocumentResultElement dre = fullDr.getNextDocumentResultElement();
				
				//	extract essential details
				for (int i = 0; i < indexers.length; i++)
					indexers[i].markEssentialDetails(dre.document);
				AnnotationFilter.removeInner(dre.document, Indexer.DETAIL_ANNOTATION_TYPE);
				
				//	extract essential details
				QueriableAnnotation[] details = dre.document.getAnnotations(Indexer.DETAIL_ANNOTATION_TYPE);
				HashMap detailsByType = new HashMap();
				StringVector detailTypes = new StringVector();
				for (int d = 0; d < details.length; d++) {
					String detailType = details[d].getAttribute(Indexer.DETAIL_TYPE_ATTRIBUTE, "").toString();
					if (detailType != null) {
						detailTypes.addElementIgnoreDuplicates(detailType);
						ArrayList detailList = ((ArrayList) detailsByType.get(detailType));
						if (detailList == null) {
							detailList = new ArrayList();
							detailsByType.put(detailType, detailList);
						}
						detailList.add(details[d]);
					}
				}
				
				//	create new document
				DocumentRoot essentialDoc = Gamta.newDocument(dre.document.getTokenizer());
				
				//	copy document properties and attributes
				String[] dpNames = dre.document.getDocumentPropertyNames();
				for (int d = 0; d < dpNames.length; d++)
					essentialDoc.setDocumentProperty(dpNames[d], dre.document.getDocumentProperty(dpNames[d]));
				essentialDoc.copyAttributes(dre.document);
				
				//	copy details
				HashSet copied = new HashSet(); // for duplicate elimination
				for (int t = 0; t < detailTypes.size(); t++) {
					ArrayList detailList = ((ArrayList) detailsByType.get(detailTypes.get(t)));
					if (detailList != null)
						for (int d = 0; d < detailList.size(); d++) {
							QueriableAnnotation detail = ((QueriableAnnotation) detailList.get(d));
							if (copied.add(detail.getValue())) {
								int size = essentialDoc.size();
								if (size != 0)
									essentialDoc.addChar(' ');
								essentialDoc.addTokens(detail);
								Annotation[] detailAnnotations = detail.getAnnotations();
								for (int a = 0; a < detailAnnotations.length; a++) {
									if (!Indexer.DETAIL_ANNOTATION_TYPE.equals(detailAnnotations[a].getType()))
										essentialDoc.addAnnotation(detailAnnotations[a].getType(), (size + detailAnnotations[a].getStartIndex()), detailAnnotations[a].size()).copyAttributes(detailAnnotations[a]);
								}
								essentialDoc.lastToken().setAttribute(Token.PARAGRAPH_END_ATTRIBUTE, Token.PARAGRAPH_END_ATTRIBUTE);
							}
						}
				}
				
				//	let indexers mark searchables
				for (int i = 0; i < indexers.length; i++)
					indexers[i].markSearchables(essentialDoc);
				
				//	wrap new document result element around reduced document
				DocumentResultElement essentialDre = new DocumentResultElement(dre.docNr, dre.documentId, dre.relevance, essentialDoc);
				essentialDre.copyAttributes(dre);
				return essentialDre;
			}
		};
	}
	
	private static final String[] documentDataSearchResultAttributes = {
		DOCUMENT_ID_ATTRIBUTE,
		DOCUMENT_TYPE_ATTRIBUTE,
		DOCUMENT_UUID_ATTRIBUTE,
		DOCUMENT_UUID_SOURCE_ATTRIBUTE,
		MASTER_DOCUMENT_ID_ATTRIBUTE,
		CHECKIN_USER_ATTRIBUTE,
		CHECKIN_TIME_ATTRIBUTE,
		UPDATE_USER_ATTRIBUTE,
		UPDATE_TIME_ATTRIBUTE,
		MASTER_DOCUMENT_TITLE_ATTRIBUTE,
		DOCUMENT_TITLE_ATTRIBUTE,
		DOCUMENT_AUTHOR_ATTRIBUTE,
		DOCUMENT_ORIGIN_ATTRIBUTE,
		DOCUMENT_DATE_ATTRIBUTE,
		DOCUMENT_SOURCE_LINK_ATTRIBUTE,
		PAGE_NUMBER_ATTRIBUTE,
		LAST_PAGE_NUMBER_ATTRIBUTE,
		MASTER_PAGE_NUMBER_ATTRIBUTE,
		MASTER_LAST_PAGE_NUMBER_ATTRIBUTE,
	};
	
	/**
	 * Search document data. The elements of the returned document result
	 * contain the basic document meta data, but not the documents themselves.
	 * @param query the query containing the search parameters
	 * @return a document result to iterate over the matching documents
	 * @throws IOException
	 */
	public DocumentResult searchDocumentData(Query query) throws IOException {
		return ((DocumentResult) this.searchDocumentData(query, true));
	}
	
	private static final String[] documentIndexSearchResultAttributes = {
		DOCUMENT_ID_ATTRIBUTE,
		DOCUMENT_TYPE_ATTRIBUTE,
		DOCUMENT_UUID_ATTRIBUTE,
		DOCUMENT_UUID_SOURCE_ATTRIBUTE,
		MASTER_DOCUMENT_ID_ATTRIBUTE,
		DOCUMENT_AUTHOR_ATTRIBUTE,
		DOCUMENT_DATE_ATTRIBUTE,
		DOCUMENT_TITLE_ATTRIBUTE,
		MASTER_DOCUMENT_TITLE_ATTRIBUTE,
		DOCUMENT_ORIGIN_ATTRIBUTE,
		PAGE_NUMBER_ATTRIBUTE,
		LAST_PAGE_NUMBER_ATTRIBUTE,
		MASTER_PAGE_NUMBER_ATTRIBUTE,
		MASTER_LAST_PAGE_NUMBER_ATTRIBUTE,
		DOCUMENT_ID_ATTRIBUTE,
		CHECKIN_USER_ATTRIBUTE,
		CHECKIN_TIME_ATTRIBUTE,
		UPDATE_USER_ATTRIBUTE,
		UPDATE_TIME_ATTRIBUTE,
		DOCUMENT_SOURCE_LINK_ATTRIBUTE,
	};
	
	/**
	 * Search document data in an unranked index style fashion. The elements of
	 * the returned index result contain the basic document meta data, but not
	 * the documents themselves.
	 * @param query the query containing the search parameters
	 * @return an index result to iterate over the matching documents
	 * @throws IOException
	 */
	public IndexResult searchDocumentIndex(Query query) throws IOException {
		return ((IndexResult) this.searchDocumentData(query, false));
	}
	
	private SrsSearchResult searchDocumentData(final Query query, boolean rankedResult) throws IOException {
		this.logActivity("GgSRS: doing document data search ...\r\n    query: " + query);
		final long start = System.currentTimeMillis();
		
		//	get document numbers
		QueryResult docNrResult = this.searchDocumentNumbers(query);
		this.logActivity("  - finished document number search after " + (System.currentTimeMillis() - start) + "ms");
		this.logActivity("  - got " + docNrResult.size() + " result document numbers");
		
		//	get sub index IDs
		final StringVector subIndexNames = new StringVector();
		String subIndexNameString =  query.getValue(SUB_INDEX_NAME);
		if (subIndexNameString != null)
			subIndexNames.parseAndAddElements(subIndexNameString, "\n");
		subIndexNames.removeAll("0");
		subIndexNames.removeAll("");
		this.logActivity("  - sub index names are " + subIndexNames.concatStrings(","));
		
		//	get minimum sub result size
		int requestSubResultMinSize;
		try {
			requestSubResultMinSize = Integer.parseInt(query.getValue(SUB_RESULT_MIN_SIZE, "0"));
		}
		catch (NumberFormatException nfe) {
			requestSubResultMinSize = 0;
		}
		final int subResultMinSize = (subIndexNames.isEmpty() ? 0 : Math.max(requestSubResultMinSize, 0));
		
		//	got query for ranked result
		if (rankedResult) {
			
			//	test if we have an ID query, and waive relevance elimination if so
			boolean isIdQuery = (query.getValue(ID_QUERY_FIELD_NAME) != null);
			
			//	read additional parameters
			int queryResultPivotIndex = (isIdQuery ? 0 : this.resultPivotIndex);
			try {
				queryResultPivotIndex = Integer.parseInt(query.getValue(RESULT_PIVOT_INDEX_PARAMETER, ("" + queryResultPivotIndex)));
			} catch (NumberFormatException nfe) {}
			double minRelevance = 0;
			try {
				minRelevance = Double.parseDouble(query.getValue(MINIMUM_RELEVANCE_PARAMETER, ("" + minRelevance)));
			} catch (NumberFormatException nfe) {}
			if (minRelevance > 1.0)
				minRelevance = 1;
			else if (minRelevance < 0)
				minRelevance = 0;
			
			/*
			 * do duplicate and relevance based elimination (use pivot only if
			 * no minimum for sub result size, for with a minimum set, highly
			 * relevant results might be eliminated, shrinking the final result
			 * too severely)
			 */
			QueryResult relevanceResult = this.doRelevanceElimination(docNrResult, minRelevance, ((subResultMinSize == 0) ? queryResultPivotIndex : 0));
			this.logActivity("  - got " + docNrResult.size() + " result document numbers after relevance elimination");
			
			//	catch empty result
			if (relevanceResult.size() == 0) return new DocumentResult() {
				public boolean hasNextElement() {
					return false;
				}
				public SrsSearchResultElement getNextElement() {
					return null;
				}
			};
			
			//	proceed with reduced result
			docNrResult = relevanceResult;
		}
		
		//	add search attributes to index entries?
		final boolean markSearcheables = (query.getValue(MARK_SEARCHABLES_PARAMETER) != null);
		
		//	get sub indexers
		final HashMap subIndexersByName = new HashMap();
		for (int i = 0; i < this.indexers.length; i++) {
			String subIndexName = this.indexers[i].getIndexName();
			if (subIndexNames.contains(subIndexName)) {
				subIndexersByName.put(subIndexName, this.indexers[i]);
				this.logActivity("  - got sub indexer: " + subIndexName);
			}
		}
		
		// add sub results to main result elements block-wise and buffer main
		// result elements (reduces number of database queries for getting sub
		// results)
		final QueryResult docNumberResult = docNrResult;
		if (rankedResult) {
			
			//	read additional parameters
			int qrpi = docNrResult.size();
			if (subResultMinSize == 0) try {
				qrpi = this.resultPivotIndex;
				qrpi = Integer.parseInt(query.getValue(RESULT_PIVOT_INDEX_PARAMETER, ("" + qrpi)));
			} catch (NumberFormatException nfe) {}
			final int queryResultPivotIndex = qrpi;
			
			//	return result
			final ArrayList staleDocNumbers = new ArrayList();
			DocumentResult dr = new DocumentResult(documentDataSearchResultAttributes) {
				private int loaded = 0;
				private int returned = 0;
				private double lastRelevance = 0;
				private int qreIndex = 0;
				private DocumentResultElement nextDre;
				public boolean hasNextElement() {
					if (this.nextDre != null)
						return true;
					
					//	produce next element
					while (this.qreIndex < docNumberResult.size()) {
						QueryResultElement qre = docNumberResult.getResult(this.qreIndex++);
						logActivity("  - adding data for result document " + qre.docNr + " (" + qre.relevance + ")");
						
						//	omit all documents less relevant than the pivot document
						if ((queryResultPivotIndex <= this.returned) && (qre.relevance < this.lastRelevance))
							break;
						
						//	get cached sub results
						IndexData indexData = getIndexData(qre.docNr, null);
						if (indexData == null)
							indexData = createIndexData(qre.docNr);
						if (indexData == null) {
//							checkStaleDocumentNumber(qre.docNr);
							staleDocNumbers.add(new Long(qre.docNr));
							continue;
						}
						
						//	create result element
						DocumentResultElement dre = new DocumentResultElement(qre.docNr, indexData.docId, qre.relevance, null);
						
						//	add result attributes from index data
						for (int a = 0; a < this.resultAttributes.length; a++) {
							String value = indexData.getDocAttribute(this.resultAttributes[a]);
							if (value == null)
								continue;
							value = value.trim();
							if (value.length() == 0)
								continue;
							if (DOCUMENT_UUID_ATTRIBUTE.equals(this.resultAttributes[a]) && (value.indexOf('-') == -1) && (value.length() >= 32)) {
								value = (value.substring(0, 8) + "-" + value.substring(8, 12) + "-" + value.substring(12, 16) + "-" + value.substring(16, 20) + "-" + value.substring(20));							}
							dre.setAttribute(this.resultAttributes[a], value);
						}
						
						//	add sub results
						IndexResult[] subResults = indexData.getIndexResults();
						int subResultSize = 0;
						for (int s = 0; s < subResults.length; s++) {
							if (!subResults[s].hasNextElement())
								continue;
							Indexer subIndexer = ((Indexer) subIndexersByName.get(subResults[s].indexName));
							if (subIndexer == null)
								continue;
							query.setIndexNameMask(subResults[s].indexName);
							IndexResult subResult = subIndexer.filterIndexEntries(query, subResults[s], false);
							query.setIndexNameMask(null);
							if (!subResult.hasNextElement())
								continue;
							
							final ArrayList subResultElements = new ArrayList(16);
							while (subResult.hasNextElement()) {
								IndexResultElement subIre = subResult.getNextIndexResultElement();
								if (markSearcheables)
									subIndexer.addSearchAttributes(subIre);
								subResultElements.add(subIre);
							}
							subResultSize += subResultElements.size();
							
							dre.addSubResult(new IndexResult(subResult.resultAttributes, subResult.indexName, subResult.indexLabel) {
								int sreIndex = 0;
								public boolean hasNextElement() {
									return (this.sreIndex < subResultElements.size());
								}
								public SrsSearchResultElement getNextElement() {
									return ((SrsSearchResultElement) subResultElements.get(this.sreIndex++));
								}
							});	
						}
						
						//	do we have enough sub results?
						if (subResultSize < subResultMinSize)
							continue;
						
						//	store result element, and remember last relevance for delaying cutoff
						this.nextDre = dre;
						this.lastRelevance = dre.relevance;
						this.loaded++;
						logActivity("    ==> loaded total of " + this.loaded + " documents");
						break;
					}
					
					//	anything to work with?
					return (this.nextDre != null);
				}
				public SrsSearchResultElement getNextElement() {
					DocumentResultElement dre = this.nextDre;
					this.nextDre = null;
					this.returned++;
					return dre;
				}
			};
			
			//	schedule checking stale document numbers
			this.checkStaleDocumentNumbers(staleDocNumbers);
			
			//	finally ...
			return dr;
		}
		
		//	unranked (index) result, need to order elements by author name, publication date, and title
		else {
			final ArrayList staleDocNumbers = new ArrayList();
			IndexResult ir = new IndexResult(documentIndexSearchResultAttributes, DocumentRoot.DOCUMENT_TYPE, "Document Index") {
				private int loaded = 0;
				private int qreIndex = 0;
				private SrsSearchResultElement nextIre;
				public boolean hasNextElement() {
					if (this.nextIre != null)
						return true;
					
					//	produce next element
					while (this.qreIndex < docNumberResult.size()) {
						QueryResultElement qre = docNumberResult.getResult(this.qreIndex++);
						logActivity("  - adding data for result document " + qre.docNr + " (" + qre.relevance + ")");
						
						//	load index data
						IndexData indexData = getIndexData(qre.docNr, null);
						if (indexData == null)
							indexData = createIndexData(qre.docNr);
						if (indexData == null) {
//							checkStaleDocumentNumber(qre.docNr);
							staleDocNumbers.add(new Long(qre.docNr));
							continue;
						}
						
						//	build title
						StringBuffer ireValue = new StringBuffer();
						ireValue.append(defaultDocumentAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, indexData.getDocAttribute(DOCUMENT_AUTHOR_ATTRIBUTE)));
						ireValue.append(". ");
						ireValue.append(defaultDocumentAttribute(DOCUMENT_DATE_ATTRIBUTE, indexData.getDocAttribute(DOCUMENT_DATE_ATTRIBUTE)));
						ireValue.append(". ");
						ireValue.append(defaultDocumentAttribute(DOCUMENT_TITLE_ATTRIBUTE, indexData.getDocAttribute(DOCUMENT_TITLE_ATTRIBUTE)));
						
						//	create result element
						IndexResultElement ire = new IndexResultElement(indexData.docNr, DocumentRoot.DOCUMENT_TYPE, ireValue.toString());
						
						//	add result attributes from index data
						for (int a = 0; a < this.resultAttributes.length; a++) {
							String value = indexData.getDocAttribute(this.resultAttributes[a]);
							if (value == null)
								continue;
							value = value.trim();
							if (value.length() == 0)
								continue;
							if (DOCUMENT_UUID_ATTRIBUTE.equals(this.resultAttributes[a]) && (value.indexOf('-') == -1) && (value.length() >= 32)) {
								value = (value.substring(0, 8) + "-" + value.substring(8, 12) + "-" + value.substring(12, 16) + "-" + value.substring(16, 20) + "-" + value.substring(20));							}
							ire.setAttribute(this.resultAttributes[a], value);
						}
						
						//	add sub results
						IndexResult[] subResults = indexData.getIndexResults();
						int subResultSize = 0;
						for (int s = 0; s < subResults.length; s++) {
							if (!subResults[s].hasNextElement())
								continue;
							Indexer subIndexer = ((Indexer) subIndexersByName.get(subResults[s].indexName));
							if (subIndexer == null)
								continue;
							query.setIndexNameMask(subResults[s].indexName);
							IndexResult subResult = subIndexer.filterIndexEntries(query, subResults[s], false);
							query.setIndexNameMask(null);
							if (!subResult.hasNextElement())
								continue;
							
							final ArrayList subResultElements = new ArrayList(16);
							while (subResult.hasNextElement()) {
								IndexResultElement subIre = subResult.getNextIndexResultElement();
								if (markSearcheables)
									subIndexer.addSearchAttributes(subIre);
								subResultElements.add(subIre);
							}
							subResultSize += subResultElements.size();
							
							ire.addSubResult(new IndexResult(subResult.resultAttributes, subResult.indexName, subResult.indexLabel) {
								int sreIndex = 0;
								public boolean hasNextElement() {
									return (this.sreIndex < subResultElements.size());
								}
								public SrsSearchResultElement getNextElement() {
									return ((SrsSearchResultElement) subResultElements.get(this.sreIndex++));
								}
							});	
						}
						
						//	do we have enough sub results?
						if (subResultSize < subResultMinSize)
							continue;
						
						//	this one's good
						this.nextIre = ire;
						this.loaded++;
						logActivity("    ==> loaded total of " + this.loaded + " documents");
						break;
					}
					
					//	anything to work with?
					return (this.nextIre != null);
				}
				public SrsSearchResultElement getNextElement() {
					SrsSearchResultElement ire = this.nextIre;
					this.nextIre = null;
					return ire;
				}
			};
			
			//	schedule checking stale document numbers
			this.checkStaleDocumentNumbers(staleDocNumbers);
			
			//	finally ...
			return ir;
		}
	}
	
	private static final String[] documentIdSearchResultAttributes = {
		DOCUMENT_ID_ATTRIBUTE,
		RELEVANCE_ATTRIBUTE
	};
	
	/**
	 * Search document IDs. The elements of the returned document result only
	 * contain document identifiers and relevance values.
	 * @param query the query containing the search parameters
	 * @return a document result to iterate over the matching documents
	 * @throws IOException
	 */
	public DocumentResult searchDocumentIDs(Query query) throws IOException {
		this.logActivity("GgSRS: doing document ID search ...");
		
		//	get document numbers
		QueryResult docNrResult = this.searchDocumentNumbers(query);
		this.logActivity("  - got " + docNrResult.size() + " result document numbers");
		
		//	test if we have an ID query, and waive relevance elimination if so
		boolean isIdQuery = (query.getValue(ID_QUERY_FIELD_NAME) != null);
		
		//	read additional parameters
		int queryResultPivotIndex = (isIdQuery ? 0 : this.resultPivotIndex);
		try {
			queryResultPivotIndex = Integer.parseInt(query.getValue(RESULT_PIVOT_INDEX_PARAMETER, ("" + queryResultPivotIndex)));
		} catch (NumberFormatException nfe) {}
		double minRelevance = 0;
		try {
			minRelevance = Double.parseDouble(query.getValue(MINIMUM_RELEVANCE_PARAMETER, ("" + minRelevance)));
		} catch (NumberFormatException nfe) {}
		if (minRelevance > 1.0) minRelevance = 0;
		else if (minRelevance < 0) minRelevance = 0;
		
		//	do duplicate and relevance based elimination
		final QueryResult relevanceResult = this.doRelevanceElimination(docNrResult, minRelevance, queryResultPivotIndex);
		this.logActivity("  - got " + relevanceResult.size() + " duplicate free, relevance sorted result document numbers");
		
		//	catch empty result
		if (relevanceResult.size() == 0)
			return new DocumentResult(documentIdSearchResultAttributes) {
				public boolean hasNextElement() {
					return false;
				}
				public SrsSearchResultElement getNextElement() {
					return null;
				}
			};
		
		//	return result fetching document IDs on the fly
		final DocumentNumberResolver docNumberResolver = this.getDocumentNumberResolver();
		final ArrayList staleDocNumbers = new ArrayList();
		DocumentResult dr = new DocumentResult(documentIdSearchResultAttributes) {
			private HashSet docNumberDeDuplicator = new HashSet();
			private int loaded = 0;
			private int qreIndex = 0;
			private DocumentResultElement nextDre;
			public boolean hasNextElement() {
				if (this.nextDre != null)
					return true;
				
				//	produce next element
				while (this.qreIndex < relevanceResult.size()) {
					QueryResultElement qre = relevanceResult.getResult(this.qreIndex++);
					logActivity("  - adding ID of result document " + qre.docNr + " (" + qre.relevance + ")");
					if (!this.docNumberDeDuplicator.add(new Long(qre.docNr)))
						continue;
					String docId = docNumberResolver.getDocumentId(qre.docNr);
					if (docId == null) {
//						checkStaleDocumentNumber(qre.docNr);
						staleDocNumbers.add(new Long(qre.docNr));
						continue;
					}
					
					//	create result element
					DocumentResultElement dre = new DocumentResultElement(qre.docNr, docId, qre.relevance, null);
					
					//	store result element, and remember last relevance for delaying cutoff
					this.nextDre = dre;
					this.loaded++;
					logActivity("    ==> loaded total of " + this.loaded + " documents");
					break;
				}
				
				//	anything to work with?
				return (this.nextDre != null);
			}
			public SrsSearchResultElement getNextElement() {
				DocumentResultElement dre = this.nextDre;
				this.nextDre = null;
				return dre;
			}
		};
		
		//	schedule checking stale document numbers
		this.checkStaleDocumentNumbers(staleDocNumbers);
		
		//	finally ...
		return dr;
	}
	
	private QueryResult searchDocumentNumbers(Query query) throws IOException {
		this.logActivity("GgSRS Document Number Search ...");
		this.logActivity("  - query is " + query.toString());
		
		//	get merge mode
		int queryResultMergeMode = this.resultMergeMode;
		String queryResultMergeModeString = query.getValue(RESULT_MERGE_MODE_PARAMETER);
		if (USE_MIN_MERGE_MODE_NAME.equals(queryResultMergeModeString))
			queryResultMergeMode = QueryResult.USE_MIN;
		else if (USE_AVERAGE_MERGE_MODE_NAME.equals(queryResultMergeModeString))
			queryResultMergeMode = QueryResult.USE_AVERAGE;
		else if (USE_MAX_MERGE_MODE_NAME.equals(queryResultMergeModeString))
			queryResultMergeMode = QueryResult.USE_MAX;
		else if (MULTIPLY_MERGE_MODE_NAME.equals(queryResultMergeModeString))
			queryResultMergeMode = QueryResult.MULTIPLY;
		else if (INVERSE_MULTIPLY_MERGE_MODE_NAME.equals(queryResultMergeModeString))
			queryResultMergeMode = QueryResult.INVERSE_MULTIPLY;
		
		//	check for request by document id
		StringVector fixedResultDocIDs = new StringVector();
		fixedResultDocIDs.parseAndAddElements(query.getValue(ID_QUERY_FIELD_NAME, ""), "\n");
		for (int i = 0; i < fixedResultDocIDs.size(); i++) 
			fixedResultDocIDs.set(i, EasyIO.sqlEscape(normalizeId(fixedResultDocIDs.get(i))));
		fixedResultDocIDs.removeAll("");
		fixedResultDocIDs.removeDuplicateElements(false);
		this.logActivity("  - got " + fixedResultDocIDs.size() + " fixed IDs");
		
		//	process query
		for (int i = 0; i < this.indexers.length; i++) {
			query.setIndexNameMask(this.indexers[i].getIndexName());
			QueryResult qr = this.indexers[i].processQuery(query);
			if (qr != null) {
				query.addPartialResult(qr);
				this.logActivity("  - got " + qr.size() + " results from " + this.indexers[i].getIndexName());
			}
			else this.logActivity("  - got no results from " + this.indexers[i].getIndexName());
			query.setIndexNameMask(null);
		}
		
		//	retrieve timestamp filters
		long modifiedSince;
		try {
			String modifiedSinceString = query.getValue(LAST_MODIFIED_SINCE, "0");
			if (modifiedSinceString.matches("[12][0-9]{3}"))
				modifiedSinceString = (modifiedSinceString + "-01-01");
			else if (modifiedSinceString.matches("[12][0-9]{3}\\-[01]?[0-9]"))
				modifiedSinceString = (modifiedSinceString + "-01");
			if (modifiedSinceString.matches("[12][0-9]{3}\\-[01]?[0-9]\\-[0-3]?[0-9]"))
				modifiedSince = MODIFIED_DATE_FORMAT.parse(modifiedSinceString).getTime();
			else modifiedSince = Long.parseLong(query.getValue(LAST_MODIFIED_SINCE, "0"));
		}
		catch (NumberFormatException nfe) {
			modifiedSince = 0;
		}
		catch (ParseException pe) {
			modifiedSince = 0;
		}
		long modifiedBefore;
		try {
			String modifiedBeforeString = query.getValue(LAST_MODIFIED_BEFORE, "0");
			if (modifiedBeforeString.matches("[12][0-9]{3}"))
				modifiedBeforeString = (modifiedBeforeString + "-01-01");
			else if (modifiedBeforeString.matches("[12][0-9]{3}\\-[01]?[0-9]"))
				modifiedBeforeString = (modifiedBeforeString + "-01");
			if (modifiedBeforeString.matches("[12][0-9]{3}\\-[01]?[0-9]\\-[0-3]?[0-9]"))
				modifiedBefore = MODIFIED_DATE_FORMAT.parse(modifiedBeforeString).getTime();
			else modifiedBefore = Long.parseLong(query.getValue(LAST_MODIFIED_BEFORE, "0"));
		}
		catch (NumberFormatException nfe) {
			modifiedBefore = 0;
		}
		catch (ParseException pe) {
			modifiedBefore = 0;
		}
		
		//	retrieve user filter
		String user = query.getValue(UPDATE_USER_ATTRIBUTE, query.getValue(CHECKIN_USER_ATTRIBUTE));
		
		//	retrieve type filter
		String type = query.getValue(DOCUMENT_TYPE_ATTRIBUTE);
		
		//	do document meta data filtering
		QueryResult docDataResult = null;
		if ((modifiedSince != 0) || (modifiedBefore != 0) || (user != null)) {
			docDataResult = new QueryResult();
			String docDataQuery = "SELECT " + DOC_NUMBER_COLUMN_NAME + 
					" FROM " + DOCUMENT_TABLE_NAME + 
					" WHERE " + ((modifiedSince == 0) ? "0=0" : (UPDATE_TIME_ATTRIBUTE + " >= " + modifiedSince)) + 
					" AND " + ((modifiedBefore == 0) ? "0=0" : (UPDATE_TIME_ATTRIBUTE + " <= " + modifiedSince)) + 
					" AND " + ((user == null) ? "0=0" : ("(" + UPDATE_USER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(user) + "' OR " + CHECKIN_USER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(user) + "')")) +
					" AND " + ((type == null) ? "0=0" : (DOCUMENT_TYPE_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(type) + "'")) +
					";";
			SqlQueryResult sqr = null;
			try {
				sqr = this.io.executeSelectQuery(docDataQuery);
				while (sqr.next())
					docDataResult.addResultElement(new QueryResultElement(sqr.getLong(0), 1));
			}
			catch (SQLException sqle) {
				this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document IDs.");
				this.logError("  Query was " + docDataQuery);
			}
			finally {
				if (sqr != null)
					sqr.close();
			}
		}
		
		//	merge partial results
		LinkedList partialDocNrResults = new LinkedList();
		QueryResult[] qr = query.getPartialResults();
		for (int r = 0; r < qr.length; r++)
			partialDocNrResults.addLast(qr[r]);
		
		//	no result, or document data filter only
		if (partialDocNrResults.isEmpty() && fixedResultDocIDs.isEmpty()) {
			
			//	no result at all
			if (docDataResult == null) {
				this.logActivity("  - got empty result");
				return new QueryResult();
			}
			
			//	document meta data result
			else {
				this.logActivity("  - got " + docDataResult.size() + " document data based results");
				return docDataResult;
			}
		}
		
		//	ID query only
		else if (partialDocNrResults.isEmpty()) {
			QueryResult result = new QueryResult();
			
			//	do document ID lookup (even though we theoretically have a document number, we still have to verify it's valid)
			if (result.size() == 0) {
				DocumentNumberResolver docNumberResolver = this.getDocumentNumberResolver();
				for (int i = 0; i < fixedResultDocIDs.size(); i++) {
					String docId = fixedResultDocIDs.get(i);
					long docNr = getDocNr(docId);
					String exDocId = docNumberResolver.getDocumentId(docNr);
					if ((exDocId != null) && exDocId.equalsIgnoreCase(docId))
						result.addResultElement(new QueryResultElement(docNr, 1.0));
				}
			}
			
			//	do document UUID lookup
			if (result.size() == 0) {
				DocumentUuidResolver docUuidResolver = this.getDocumentUuidResolver();
				DocumentNumberResolver docNumberResolver = this.getDocumentNumberResolver();
				for (int i = 0; i < fixedResultDocIDs.size(); i++) try {
					String docUuid = fixedResultDocIDs.get(i);
					long docNr = docUuidResolver.getDocumentNumber(docUuid);
					String docId = ((docNr == -1) ? null : docNumberResolver.getDocumentId(docNr));
					if (docId != null)
						result.addResultElement(new QueryResultElement(docNr, 1.0));
				}
				catch (IllegalArgumentException iae) {
					this.logError("Invalid document UUID: " + iae.getMessage());
				}
			}
			
			//	do master document ID lookup
			if (result.size() == 0) {
				MasterDocumentIdMapper masterDocIdMapper = this.getMasterDocumentIdMapper();
				DocumentNumberResolver docNumberResolver = this.getDocumentNumberResolver();
				for (int i = 0; i < fixedResultDocIDs.size(); i++) try {
					String masterDocId = fixedResultDocIDs.get(i);
					long[] docNrs = masterDocIdMapper.getDocumentNumbers(masterDocId);
					if (docNrs == null)
						continue;
					for (int n = 0; n < docNrs.length; n++) {
						String docId = docNumberResolver.getDocumentId(docNrs[n]);
						if (docId != null)
							result.addResultElement(new QueryResultElement(docNrs[n], 1.0));
					}
				}
				catch (IllegalArgumentException iae) {
					this.logError("Invalid master document ID: " + iae.getMessage());
				}
			}
			
			//	apply document data filter if given
			if (docDataResult != null)
				result = QueryResult.merge(result, docDataResult, QueryResult.USE_MIN, 0);
			
			//	throw out relevance 0 results
			result.pruneByRelevance(Double.MIN_VALUE);
			
			//	return result
			this.logActivity("  - got " + result.size() + " document numbers in result");
			return result;
		}
		
		//	ranked result
		else {
			
			//	merge retrieval results from individual indexers
			this.logActivity("  - merging " + partialDocNrResults.size() + " partial results, mode is " + queryResultMergeMode);
			while (partialDocNrResults.size() > 1) {
				QueryResult qr1 = ((QueryResult) partialDocNrResults.removeFirst());
				QueryResult qr2 = ((QueryResult) partialDocNrResults.removeFirst());
				QueryResult mqr = QueryResult.merge(qr1, qr2, queryResultMergeMode, 0);
				this.logActivity("  - got " + mqr.size() + " document numbers in merge result");
				partialDocNrResults.addLast(mqr);
			}
			QueryResult result = ((QueryResult) partialDocNrResults.removeFirst());
			this.logActivity("  - got " + result.size() + " document numbers in final merge result");
			
			//	apply user and timestamp filter if given
			if (docDataResult != null) {
				result = QueryResult.merge(result, docDataResult, QueryResult.USE_MIN, 0);
				this.logActivity("  - got " + result.size() + " document numbers after merging in document data");
			}
			
			//	no results left after merge
			if (result.size() == 0)
				return new QueryResult();
			
			//	throw out relevance 0 results
			result.pruneByRelevance(Double.MIN_VALUE);
			this.logActivity("  - got " + result.size() + " document numbers after relevance pruning");
			
			//	sort & return joint result
			result.sortByRelevance(true);
			this.logActivity("  - got " + result.size() + " document numbers in result");
			return result;
		}
	}
	
	private QueryResult doRelevanceElimination(QueryResult baseResult, double minRelevance, int resultPivotIndex) {
		if (baseResult.size() == 0)
			return baseResult;
		
		baseResult.sortByRelevance(true);
		if (resultPivotIndex <= 0)
			resultPivotIndex = baseResult.size();
		
		HashSet resultDocNumbers = new HashSet();
		QueryResult reducedResult = new QueryResult();
		double lastRelevance = 1;
		for (int r = 0; r < baseResult.size(); r++) {
			QueryResultElement qre = baseResult.getResult(r);
			
			//	no more QREs with sufficiently high relevance to expect, we're done here
			if (qre.relevance < minRelevance)
				break;
			
			//	we're beyond the pivot QRE, and current QRE is less relevant than latter, we're done here
			if ((resultPivotIndex <= reducedResult.size()) && (qre.relevance < lastRelevance))
				break;
			
			//	we're beyond 100 and beyond thrice the pivot index, this is enough
			if ((100 <= reducedResult.size()) && ((resultPivotIndex * 3) <= reducedResult.size()))
				break;
			
			//	avoid duplicates
			if (resultDocNumbers.add(new Long(qre.docNr))) {
				
				//	store result element
				reducedResult.addResultElement(qre);
				
				//	remember last relevance for delaying cutoff
				lastRelevance = qre.relevance;
			}
		}
		
		//	return remaining documents
		return reducedResult;
	}
	
	private IndexResult searchIndex(final Query query) throws IOException {
		this.logActivity("GgSRS Index Search ...");
		this.logActivity("  - query is " + query.toString());
		
		//	get index ID
		String searchIndexName = query.getValue(INDEX_NAME);
		if (searchIndexName == null)
			throw new IOException("No such index.");
		this.logActivity("  - index name is " + searchIndexName);
		
		//	search for document index, search document data and wrap it in IndexResult
		if ("0".equals(searchIndexName) || "doc".equals(searchIndexName) || "document".equals(searchIndexName))
			return this.searchDocumentIndex(query);
		
		//	find indexer
		Indexer mainIndexer = null;
		for (int i = 0; i < this.indexers.length; i++) {
			String indexName = this.indexers[i].getIndexName();
			if (searchIndexName.equals(indexName)) {
				mainIndexer = this.indexers[i];
				this.logActivity("  - got indexer");
			}
		}
		
		//	index not found
		if (mainIndexer == null) {
			this.logError("GoldenGateSRS: request for non-existing index '" + searchIndexName + "'");
			this.logError("Query is " + query.toString());
			throw new IOException("No such index: '" + searchIndexName + "'.");
		}
		
		//	get sub index IDs
		StringVector subIndexNames = new StringVector();
		String subIndexNameString =  query.getValue(SUB_INDEX_NAME);
		if (subIndexNameString != null)
			subIndexNames.parseAndAddElements(subIndexNameString, "\n");
		subIndexNames.removeAll(searchIndexName);
		subIndexNames.removeAll("0");
		subIndexNames.removeAll("");
		this.logActivity("  - subIndexName(s) are " + subIndexNames.concatStrings(","));
		
		//	get minimum sub result size
		int srms;
		try {
			srms = Integer.parseInt(query.getValue(SUB_RESULT_MIN_SIZE, "0"));
		}
		catch (NumberFormatException nfe) {
			srms = 0;
		}
		final int subResultMinSize = (subIndexNames.isEmpty() ? 0 : Math.max(srms, 0));
		
		//	add search attributes to index entries?
		final boolean markSearcheables = (query.getValue(MARK_SEARCHABLES_PARAMETER) != null);
		
		//	get document numbers
		QueryResult qr = this.searchDocumentNumbers(query);
		this.logActivity("  - got " + qr.size() + " result doc numbers");
		if (qr.size() == 0) {
			String[] emptyFields = {mainIndexer.getIndexName()};
			return new IndexResult(emptyFields, mainIndexer.getIndexName(), mainIndexer.getIndexName()) {
				public boolean hasNextElement() {
					return false;
				}
				public SrsSearchResultElement getNextElement() {
					return null;
				}
			};
		}
		
		//	get sub indexers
		final HashMap subIndexersByName = new HashMap();
		for (int i = 0; i < this.indexers.length; i++) {
			String subIndexName = this.indexers[i].getIndexName();
			if (subIndexNames.contains(subIndexName)) {
				subIndexersByName.put(subIndexName, this.indexers[i]);
				this.logActivity("  - got sub indexer: " + subIndexName);
			}
		}
		
		//	filter and aggregate result data
		String indexResultLabel = null;
		String[] indexResultAttributes = null;
		final LinkedHashMap indexResultElementData = new LinkedHashMap();
		final ArrayList staleDocNumbers = new ArrayList();
		for (int r = 0; r < qr.size(); r++) {
			QueryResultElement qre = qr.getResult(r);
			IndexData indexData = getIndexData(qre.docNr, null);
			if (indexData == null)
				indexData = createIndexData(qre.docNr);
			if (indexData == null) {
//				checkStaleDocumentNumber(qre.docNr);
				staleDocNumbers.add(new Long(qre.docNr));
				continue;
			}
			
			IndexResult result = indexData.getIndexResult(mainIndexer.getIndexName());
			if (result == null)
				continue;
			if (indexResultLabel == null)
				indexResultLabel = result.indexLabel;
			if (indexResultAttributes == null)
				indexResultAttributes = result.resultAttributes;
			
			query.setIndexNameMask(mainIndexer.getIndexName());
			result = mainIndexer.filterIndexEntries(query, result, true);
			query.setIndexNameMask(null);
			if (result == null)
				continue;
			if (!result.hasNextElement())
				continue;
			
			//	add individual result elements
			while (result.hasNextElement()) {
				IndexResultElement ire = result.getNextIndexResultElement();
				String ireSortKey = ire.getSortString(result.resultAttributes);
				IndexResultElementData ireData = ((IndexResultElementData) indexResultElementData.get(ireSortKey));
				if (ireData == null) {
					if (markSearcheables)
						mainIndexer.addSearchAttributes(ire);
					ireData = new IndexResultElementData(ire);
					indexResultElementData.put(ireSortKey, ireData);
				}
				if (subIndexersByName.isEmpty())
					continue;
				
				//	add sub result data
				IndexResult[] subResults = indexData.getIndexResults();
				for (int s = 0; s < subResults.length; s++) {
					if (!subResults[s].hasNextElement())
						continue;
					if (subResults[s].indexName.equals(result.indexName))
						continue;
					Indexer subIndexer = ((Indexer) subIndexersByName.get(subResults[s].indexName));
					if (subIndexer == null)
						continue;
					query.setIndexNameMask(subResults[s].indexName);
					IndexResult subResult = subIndexer.filterIndexEntries(query, subResults[s], false);
					query.setIndexNameMask(null);
					if (!subResult.hasNextElement())
						continue;
					
					SubIndexResultData subResultData = ireData.getSubResultData(subResult);
					while (subResult.hasNextElement()) {
						IndexResultElement subIre = subResult.getNextIndexResultElement();
						if (markSearcheables)
							subIndexer.addSearchAttributes(subIre);
						subResultData.addIndexResultElement(subIre);
					}
				}
			}
		}
		
		//	schedule checking stale document numbers
		this.checkStaleDocumentNumbers(staleDocNumbers);
		
		//	anything to work with at all?
		if ((indexResultLabel == null) || (indexResultAttributes == null)) {
			String[] emptyFields = {mainIndexer.getIndexName()};
			return new IndexResult(emptyFields, mainIndexer.getIndexName(), mainIndexer.getIndexName()) {
				public boolean hasNextElement() {
					return false;
				}
				public SrsSearchResultElement getNextElement() {
					return null;
				}
			};
		}
		
		//	return actual result, wrapping data on the fly
		return new IndexResult(indexResultAttributes, mainIndexer.getIndexName(), indexResultLabel) {
			private Iterator ireIterator = indexResultElementData.values().iterator();
			private IndexResultElementData nextSubIreData = null;
			public boolean hasNextElement() {
				if (this.nextSubIreData != null)
					return true;
				while (this.ireIterator.hasNext()) {
					IndexResultElementData ired = ((IndexResultElementData) this.ireIterator.next());
					if (subResultMinSize < ired.getSubResultSize()) {
						this.nextSubIreData = ired;
						break;
					}
				}
				return (this.nextSubIreData != null);
			}
			public SrsSearchResultElement getNextElement() {
				if (this.hasNextElement()) {
					IndexResultElementData ired = this.nextSubIreData;
					this.nextSubIreData = null;
					return ired.getIndexResultElement();
				}
				else return null;
			}
		};
	}
	
	private static class IndexResultElementData {
		IndexResultElement ire;
		HashMap subResultsByName = new HashMap(3);
		IndexResultElementData(IndexResultElement ire) {
			this.ire = ire;
		}
		SubIndexResultData getSubResultData(IndexResult subResult) {
			SubIndexResultData sird = ((SubIndexResultData) this.subResultsByName.get(subResult.indexName));
			if (sird == null) {
				sird = new SubIndexResultData(subResult.indexLabel, subResult.resultAttributes);
				this.subResultsByName.put(subResult.indexName, sird);
			}
			return sird;
		}
		int getSubResultSize() {
			int subResultSize = 0;
			for (Iterator srIt = this.subResultsByName.values().iterator(); srIt.hasNext();) {
				SubIndexResultData subResultData = ((SubIndexResultData) srIt.next());
				subResultSize += subResultData.subResultsElements.size();
			}
			return subResultSize;
		}
		IndexResultElement getIndexResultElement() {
			for (Iterator srnIt = this.subResultsByName.keySet().iterator(); srnIt.hasNext();) {
				String subResultName = ((String) srnIt.next());
				SubIndexResultData subResultData = ((SubIndexResultData) this.subResultsByName.get(subResultName));
				this.ire.addSubResult(subResultData.getSubIndexResult(subResultName, ire));
			}
			return this.ire;
		}
	}
	
	private static class SubIndexResultData {
		String subIndexLabel;
		String[] subResultAttributes;
		HashSet subResultElementKeys = new HashSet();
		LinkedList subResultsElements = new LinkedList();
		SubIndexResultData(String subIndexLabel, String[] subResultAttributes) {
			this.subIndexLabel = subIndexLabel;
			this.subResultAttributes = subResultAttributes;
		}
		void addIndexResultElement(IndexResultElement subIre) {
			if (this.subResultElementKeys.add(subIre.getSortString(this.subResultAttributes)))
				this.subResultsElements.add(subIre);
		}
		IndexResult getSubIndexResult(String subIndexName, final IndexResultElement ire) {
			return new IndexResult(this.subResultAttributes, subIndexName, this.subIndexLabel) {
				private Iterator subIreIterator = subResultsElements.iterator();
				public boolean hasNextElement() {
					return this.subIreIterator.hasNext();
				}
				public SrsSearchResultElement getNextElement() {
					IndexResultElement subIre = ((IndexResultElement) this.subIreIterator.next());
					subIre.setParent(ire);
					return subIre;
				}
			};
		}
	}
	
	private ThesaurusResult searchThesaurus(Query query) {
		this.logActivity("GgSRS Thesaurus Search ...");
		this.logActivity("  - query is " + query.toString());
		
		//	process query through this.indexers one by one
		String thesaurusName = query.getValue(INDEX_NAME);
		this.logActivity("  - thesaurus name is " + thesaurusName);
		ThesaurusResult result = null;
		for (int i = 0; i < this.indexers.length; i++) {
			this.logActivity("  - indexer is " + this.indexers[i].getClass().getName());
			String indexName = this.indexers[i].getIndexName();
			this.logActivity("  - index name is " + indexName);
			if ((thesaurusName == null) || thesaurusName.equals(indexName)) {
				this.logActivity("  - got indexer");
				query.setIndexNameMask(indexName);
				result = this.indexers[i].doThesaurusLookup(query);
				query.setIndexNameMask(null);
				if (result != null) {
					this.logActivity("  - got result!");
					return result;
				}
			}
		}
		
		return null;
	}
	
	private Query parseQuery(String queryString) throws IOException {
		Query query = new Query();
		
		//	parse query
		StringVector parser = new StringVector();
		parser.parseAndAddElements(queryString, "&");
		for (int p = 0; p < parser.size(); p++) {
			String[] pair = parser.get(p).split("\\=");
			if (pair.length < 2)
				continue;
			String name = pair[0].trim();
			String value = URLDecoder.decode(pair[1].trim(), ENCODING).trim();
			
			String existingValue = query.getValue(name);
			if (existingValue != null)
				value = existingValue + "\n" + value;
			
			query.setValue(name, value);
			if (name.indexOf(".") != -1)
				this.sQueriedIndexerFields.add(name);
		}
		
		//	check if it's time for logging query fields
		long time = System.currentTimeMillis();
		if ((this.indexersQueriedLastLogged + (1000 * 60 * 60)) < time) try {
			long queriedTime = (time - this.indexersQueriedSince);
			long queriedHours = ((queriedTime + ((1000 * 60 * 60) / 2)) / (1000 * 60 * 60));
			ArrayList qfns = new ArrayList(this.sQueriedIndexerFields); // need to use intermediate list to avoid concurrent modification exceptions of iterator
			this.logAlways("These " + qfns.size() + " indexer fields were queried in the past " + queriedHours + " hours (" + queriedTime + "ms):");
			for (int f = 0; f < qfns.size(); f++) {
				String queriedFieldName = ((String) qfns.get(f));
				this.logAlways(" - '" + queriedFieldName + "': " + this.queriedIndexerFields.getCount(queriedFieldName) + " timed");
			}
			this.indexersQueriedLastLogged = time;
		}
		catch (RuntimeException re) {
			this.logError("GoldenGateSRS: error logging queried indexer fields: " + re.getMessage());
		}
		
		return query;
	}
	
	private static final long getDocNr(String docId) {
		long docNrLow = 0;
		for (int c = (docId.length() / 2); c < docId.length(); c++) {
			char ch = docId.charAt(c);
			if (('0' <= ch) && (ch <= '9')) {
				docNrLow <<= 4;
				docNrLow |= ((long) (ch - '0'));
			}
			else if (('A' <= ch) && (ch <= 'F')) {
				docNrLow <<= 4;
				docNrLow |= ((long) (ch - 'A' + 10));
			}
			else if (('a' <= ch) && (ch <= 'f')) {
				docNrLow <<= 4;
				docNrLow |= ((long) (ch - 'a' + 10));
			}
			else throw new IllegalArgumentException("Illegal HEX character in '" + docId + "': " + ch);
		}
		long docNrHigh = 0;
		for (int c = 0; c < (docId.length() / 2); c++) {
			char ch = docId.charAt(c);
			if (('0' <= ch) && (ch <= '9')) {
				docNrHigh <<= 4;
				docNrHigh |= ((long) (ch - '0'));
			}
			else if (('A' <= ch) && (ch <= 'F')) {
				docNrHigh <<= 4;
				docNrHigh |= ((long) (ch - 'A' + 10));
			}
			else if (('a' <= ch) && (ch <= 'f')) {
				docNrHigh <<= 4;
				docNrHigh |= ((long) (ch - 'a' + 10));
			}
			else throw new IllegalArgumentException("Illegal HEX character in '" + docId + "': " + ch);
		}
		return (docNrHigh ^ docNrLow);
	}
	
	private static final String normalizeId(String id) {
		if (id == null)
			return null;
		id = id.trim(); // truncate whitespace (there should be none, but we never know)
		id = id.substring(id.lastIndexOf(':') + 1).trim(); // remove URI prefixes
		if (id.indexOf('.') >= 32) // truncate any file extensions that might have come in
			id = id.substring(0, id.indexOf('.'));
		id = id.replaceAll("[^0-9a-fA-F]", "").toUpperCase(); // generate compact version
		if (id.length() > 32) // truncate any excess characters
			id = id.substring(0, 32);
		return id;
	}
	
	private void checkStaleDocumentNumbers(final ArrayList docNrs) {
		if (docNrs.isEmpty())
			return;
		this.logInfo("Checking " + docNrs.size() + " potentially stale document numbers");
		this.enqueueIndexerAction(new IndexerAction(IndexerAction.CHECK_DOCUMENT) {
			void performAction() {
				for (int n = 0; n < docNrs.size(); n++)
					checkStaleDocumentNumber(((Long) docNrs.get(n)).longValue());
			}
		});
	}
	
	private void checkStaleDocumentNumber(final long docNr) {
		this.logInfo("Checking for stale document number: " + docNr);
		long lastModified = this.getLastModified();
		long time = System.currentTimeMillis();
		if ((time - lastModified) < (1000 * 60 * 10)) {
			this.logInfo(" ==> last update only " + (time - lastModified) + "ms ago");
			return;
		}
		DocumentNumberResolver docNumberResolver = this.getDocumentNumberResolver();
		String docId = docNumberResolver.getDocumentId(docNr);
		if (docId != null) {
			this.logInfo(" ==> found document ID " + docId + " after " + (System.currentTimeMillis() - time) + "ms");
			return;
		}
		String checkQuery = "SELECT " + DOCUMENT_ID_ATTRIBUTE +
				" FROM " + DOCUMENT_TABLE_NAME +
				" WHERE " + DOC_NUMBER_COLUMN_NAME + " = " + docNr +
				";";
		SqlQueryResult checkSqr = null;
		try {
			checkSqr = this.io.executeSelectQuery(checkQuery, true);
			this.logInfo(" - checked database after " + (System.currentTimeMillis() - time) + "ms");
			if (checkSqr.next()) {
				this.logInfo(" ==> found document ID " + docId + " in database");
				this.enqueueIndexerAction(new IndexerAction(IndexerAction.RE_INDEX_NAME) {
					void performAction() {
						synchronized (docIdentifierResolverLock) {
							initDocumentIdentifierResolvers();
						}
					}
				});
				this.logInfo(" ==> triggered ID resolver reload");
			}
			else this.logInfo(" - document ID " + docId + " not found in database");
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while checking document number '" + docNr + "'.");
			this.logError("  Query was " + checkQuery);
		}
		finally {
			if (checkSqr != null)
				checkSqr.close();
		}
		if (this.reIndexAction.isRunning()) {
			this.logInfo(" ==> got re-indexing operation running after " + (System.currentTimeMillis() - time) + "ms");
			return;
		}
		if (this.indexerActionQueue.size() != 0) {
			this.logInfo(" ==> got pending indexing actions after " + (System.currentTimeMillis() - time) + "ms");
			return;
		}
		for (int i = 0; i < this.indexers.length; i++) {
			this.enqueueIndexerAction(new IndexerAction(IndexerAction.DELETE_NAME, this.indexers[i]) {
				void performAction() {
					this.indexer.deleteDocument(docNr);
				}
			});
			this.logInfo(" - enqueued un-indexing from " + this.indexers[i].getIndexName() + " after " + (System.currentTimeMillis() - time) + "ms");
		}
		this.enqueueIndexerAction(new IndexerAction(IndexerAction.DELETE_NAME) {
			void performAction() {
				deleteIndexData(docNr);
			}
		});
		this.logInfo(" - enqueued removal of cached index data after " + (System.currentTimeMillis() - time) + "ms");
		this.logInfo(" ==> stale document number erased after " + (System.currentTimeMillis() - time) + "ms");
	}
	
	private Object docIdentifierResolverLock = new Object();
	
	private DocumentNumberResolver docNumberResolver;
	private DocumentNumberResolver getDocumentNumberResolver() {
		synchronized (this.docIdentifierResolverLock) {
			return this.docNumberResolver;
		}
	}
	
	private DocumentUuidResolver docUuidResolver;
	private DocumentUuidResolver getDocumentUuidResolver() {
		synchronized (this.docIdentifierResolverLock) {
			return this.docUuidResolver;
		}
	}
	
	private MasterDocumentIdMapper masterDocIdMapper;
	private MasterDocumentIdMapper getMasterDocumentIdMapper() {
		synchronized (this.docIdentifierResolverLock) {
			return this.masterDocIdMapper;
		}
	}
	
	private static class DocumentNumberResolver {
		private long[] index;
		private long[] data;
		private int size;
		DocumentNumberResolver() {
			this.index = new long[32];
			this.data = new long[64];
			this.size = 0;
		}
		private DocumentNumberResolver(long[] index, long[] data, int size) {
			this.index = index;
			this.data = data;
			this.size = size;
		}
		boolean containsDocumentNumber(long docNr) {
			int pos = Arrays.binarySearch(this.index, 0, this.size, docNr);
			if ((pos < 0) || (this.size <= pos))
				return false;
			return (this.index[pos] == docNr);
		}
		String getDocumentId(long docNr) {
			return getDocumentId(docNr, null);
		}
		String getDocumentId(long docNr, char[] docId) {
			int pos = Arrays.binarySearch(this.index, 0, this.size, docNr);
			if ((pos < 0) || (this.size <= pos))
				return null;
			if (this.index[pos] != docNr)
				return null;
			if (docId == null)
				docId = new char[32];
			decodeHex(this.data[pos * 2], this.data[(pos * 2) + 1], docId);
			return new String(docId);
		}
//		String[] getDocumentIDs(long[] docNrs) {
//			String[] docIds = new String[docNrs.length];
//			Arrays.fill(docIds, null);
//			char[] docId = new char[32]; // String constructor copies array content, so we can keep using same helper array over and over and thus save allocating new ones for every document number
//			for (int n = 0; n < docNrs.length; n++)
//				docIds[n] = this.getDocumentId(docNrs[n], docId);
//			return docIds;
//		}
		long documentNumberAt(int index) {
			return this.index[index];
		}
		String documentIdAt(int index) {
			char[] docId = new char[32];
			decodeHex(this.data[index * 2], this.data[(index * 2) + 1], docId);
			return new String(docId);
		}
		int size() {
			return this.size;
		}
		private static void decodeHex(long high, long low, char[] hex) {
			for (int hexPos = 15; hexPos >= 0; hexPos--) {
				byte b = ((byte) (high & 0x000000000000000F));
				if (b < 10)
					hex[hexPos] = ((char) ('0' + b));
				else hex[hexPos] = ((char) ('A' + (b - 10)));
				high >>>= 4;
			}
			for (int hexPos = 31; hexPos >= 16; hexPos--) {
				byte b = ((byte) (low & 0x000000000000000F));
				if (b < 10)
					hex[hexPos] = ((char) ('0' + b));
				else hex[hexPos] = ((char) ('A' + (b - 10)));
				low >>>= 4;
			}
		}
		
		DocumentNumberResolver cloneForChanges(HashSet addedDocIDs, long[] removed) {
			
			//	line up to-add document numbers, and filter out ones that are already contained
			long[] added = new long[addedDocIDs.size()];
			HashMap addedMapping = new HashMap();
			int a = 0;
			for (Iterator didit = addedDocIDs.iterator(); didit.hasNext();) {
				String docId = ((String) didit.next());
				long docNr = getDocNr(docId);
				if (this.containsDocumentNumber(docNr)) {
					long[] cAdded = new long[added.length - 1];
					System.arraycopy(added, 0, cAdded, 0, a);
					added = cAdded;
				}
				else {
					added[a++] = docNr;
					addedMapping.put(new Long(docNr), docId);
				}
			}
			
			//	filter out to-remove document numbers that are not contained or will be added
			for (int r = 0; r < removed.length; r++) {
				if (!addedMapping.containsKey(new Long(removed[r])) && this.containsDocumentNumber(removed[r]))
					continue; // this one's clear for removal
				long[] cRemoved = new long[removed.length - 1];
				System.arraycopy(removed, 0, cRemoved, 0, r);
				System.arraycopy(removed, (r+1), cRemoved, r, (cRemoved.length - r));
				removed = cRemoved;
			}
			
			//	anything left to do at all?
			if ((added.length == 0) && (removed.length == 0))
				return this;
			
			//	sort to-add and to-remove document numbers
			a = 0;
			Arrays.sort(added);
			int r = 0;
			Arrays.sort(removed);
			
			//	merge data into new array (we're only ever reading the main ones, for thread safety)
			int cSize = (this.size + added.length - removed.length);
			long[] cIndex = new long[cSize];
			long[] cData = new long[cSize * 2];
			int cn = 0;
			for (int n = 0; n < this.size; n++) {
				
				//	handle insertions before current document number
				while ((a < added.length) && (added[a] < this.index[n])) {
					String docId = ((String) addedMapping.get(new Long(added[a])));
					cIndex[cn] = added[a];
					encodeHex(cData, (cn * 2), docId);
					cn++;
					a++;
				}
				
				//	handle removals
				if (r == removed.length) {} // all removals handled
				else if (removed[r] == this.index[n]) {
					r++; // move on to next to-remove document number
					continue; // skip over copying to-remove document number
				}
				
				//	copy current document number
				cIndex[cn] = this.index[n];
				cData[cn * 2] = this.data[n * 2];
				cData[(cn * 2) + 1] = this.data[(n * 2) + 1];
				cn++;
			}
			
			//	handle any additions after last document number
			while (a < added.length) {
				String docId = ((String) addedMapping.get(new Long(added[a])));
				cIndex[cn] = added[a];
				encodeHex(cData, (cn * 2), docId);
				cn++;
				a++;
			}
			
			//	finally ...
			return new DocumentNumberResolver(cIndex, cData, cSize);
		}
		
		void mapDocumentNumber(long docNr, String docId) {
			if ((this.size != 0) && (docNr <= this.index[this.size-1]))
				throw new IllegalArgumentException("Map document numbers in incremental order !!!");
			if (this.index.length == this.size) {
				long[] cIndex = new long[this.size * 2];
				System.arraycopy(this.index, 0, cIndex, 0, this.size);
				this.index = cIndex;
				long[] cData = new long[this.size * 2 * 2];
				System.arraycopy(this.data, 0, cData, 0, (this.size * 2));
				this.data = cData;
			}
			this.index[this.size] = docNr;
			encodeHex(this.data, (this.size * 2), docId);
			this.size++;
		}
		DocumentNumberResolver cloneToSize() {
			if (this.index.length == this.size)
				return this;
			long[] cIndex = new long[this.size];
			System.arraycopy(this.index, 0, cIndex, 0, this.size);
			long[] cData = new long[this.size * 2];
			System.arraycopy(this.data, 0, cData, 0, (this.size * 2));
			return new DocumentNumberResolver(cIndex, cData, this.size);
		}
	}
	
	private static class DocumentUuidResolver {
		private long[] indexHigh;
		private long[] indexLow;
		private long[] data;
		private int size;
		DocumentUuidResolver() {
			this.indexHigh = new long[32];
			this.indexLow = new long[32];
			this.data = new long[32];
			this.size = 0;
		}
		private DocumentUuidResolver(long[] indexHigh, long[] indexLow, long[] data, int size) {
			this.indexHigh = indexHigh;
			this.indexLow = indexLow;
			this.data = data;
			this.size = size;
		}
		private int findDocumentUuidPos(String docUuid) {
			long uuidHigh = encodeHex(docUuid, 0);
			int pos = Arrays.binarySearch(this.indexHigh, 0, this.size, uuidHigh);
			if ((pos < 0) || (this.size <= pos))
				return -1;
			if (this.indexHigh[pos] != uuidHigh)
				return -1;
			while ((pos != 0) && (this.indexHigh[pos-1] == uuidHigh))
				pos--;
			long uuidLow = encodeHex(docUuid, 16);
			while ((pos < this.size) && (this.indexHigh[pos] == uuidHigh)) {
				if (this.indexLow[pos] < uuidLow)
					pos++;
				else if (this.indexLow[pos] == uuidLow)
					return pos;
				else break;
			}
			return -1;
		}
		boolean containsDocumentUuid(String docUuid) {
			return (this.findDocumentUuidPos(docUuid) != -1);
		}
		long getDocumentNumber(String docUuid) {
			int pos = this.findDocumentUuidPos(docUuid);
			return ((pos == -1) ? -1 : this.data[pos]);
		}
		
		boolean updateDocumentUuidMapping(String docUuid, long docNr) {
			int pos = this.findDocumentUuidPos(docUuid);
			if (pos == -1)
				return false;
			this.data[pos] = docNr;
			return true;
		}
		DocumentUuidResolver cloneForChanges(HashMap addedDocUuidsToNrs, HashSet removedDocUuids) {
			
			//	line up to-add document UUIDs
			String[] added = new String[addedDocUuidsToNrs.size()];
			int a = 0;
			for (Iterator duit = addedDocUuidsToNrs.keySet().iterator(); duit.hasNext();) {
				String docUuid = ((String) duit.next());
				Long docNr = ((Long) addedDocUuidsToNrs.get(docUuid));
				if (!this.updateDocumentUuidMapping(docUuid, docNr.longValue()))
					added[a++] = docUuid;
			}
			if (a < added.length) {
				String[] cAdded = new String[a];
				System.arraycopy(added, 0, cAdded, 0, a);
				added = cAdded;
			}
			
			//	filter out to-remove document UUIDs that are not contained or will be added
			String[] removed = new String[removedDocUuids.size()];
			int r = 0;
			for (Iterator duit = removedDocUuids.iterator(); duit.hasNext();) {
				String docUuid = ((String) duit.next());
				if (addedDocUuidsToNrs.containsKey(docUuid))
					continue; // this one's being re-added
				if (!this.containsDocumentUuid(docUuid))
					continue; // not removing this one
				removed[r++] = docUuid;
			}
			if (r < removed.length) {
				String[] cRemoved = new String[r];
				System.arraycopy(removed, 0, cRemoved, 0, r);
				removed = cRemoved;
			}
			
			//	anything left to do at all?
			if ((added.length == 0) && (removed.length == 0))
				return this;
			
			//	line up to-add document UUID bytes
			Arrays.sort(added, signAwareHexStringOrder);
			long[] addedHigh = new long[added.length];
			long[] addedLow = new long[added.length];
			for (a = 0; a < added.length; a++) {
				addedHigh[a] = encodeHex(added[a], 0);
				addedLow[a] = encodeHex(added[a], 16);
			}
			
			//	line up to-remove document UUID bytes
			Arrays.sort(removed, signAwareHexStringOrder);
			long[] removedHigh = new long[removed.length];
			long[] removedLow = new long[removed.length];
			for (r = 0; r < removed.length; r++) {
				removedHigh[r] = encodeHex(removed[r], 0);
				removedLow[r] = encodeHex(removed[r], 16);
			}
			
			//	sort to-add and to-remove document numbers
			a = 0;
			r = 0;
			
			//	merge data into new array (we're only ever reading the main ones, for thread safety)
			int cSize = (this.size + added.length - removed.length);
			long[] cIndexHigh = new long[cSize];
			long[] cIndexLow = new long[cSize];
			long[] cData = new long[cSize];
			int cn = 0;
			for (int n = 0; n < this.size; n++) {
				
				//	handle insertions before current document UUID
				while ((a < added.length) && ((addedHigh[a] < this.indexHigh[n]) || ((addedHigh[a] == this.indexHigh[n]) && (addedLow[a] < this.indexLow[n])))) {
					Long docNr = ((Long) addedDocUuidsToNrs.get(added[a]));
					cIndexHigh[cn] = addedHigh[a];
					cIndexLow[cn] = addedLow[a];
					cData[cn] = docNr.longValue();
					cn++;
					a++;
				}
				
				//	handle removals
				if (r == removed.length) {} // all removals handled
				else if ((removedHigh[r] == this.indexHigh[n]) && (removedLow[r] == this.indexLow[n])) {
					r++; // move on to next to-remove document number
					continue; // skip over copying to-remove document number
				}
				
				//	copy current document UUID
				cIndexHigh[cn] = this.indexHigh[n];
				cIndexLow[cn] = this.indexLow[n];
				cData[cn] = this.data[n];
				cn++;
			}
			
			//	handle any additions after last document number
			while (a < added.length) {
				Long docNr = ((Long) addedDocUuidsToNrs.get(added[a]));
				cIndexHigh[cn] = addedHigh[a];
				cIndexLow[cn] = addedLow[a];
				cData[cn] = docNr.longValue();
				cn++;
				a++;
			}
			
			//	finally ...
			return new DocumentUuidResolver(cIndexHigh, cIndexLow, cData, cSize);
		}
		
		void mapDocumentUuid(String docUuid, long docNr) {
			long[] uuid = new long[2];
			encodeHex(uuid, 0, docUuid);
			if (this.size == 0) {}
			else if (uuid[0] < this.indexHigh[this.size-1])
				throw new IllegalArgumentException("Map document UUIDs in incremental order !!!");
			else if ((uuid[0] == this.indexHigh[this.size-1]) && (uuid[1] <= this.indexLow[this.size-1]))
				throw new IllegalArgumentException("Map document UUIDs in incremental order !!!");
			if (this.data.length == this.size) {
				long[] cIndexHigh = new long[this.size * 2];
				System.arraycopy(this.indexHigh, 0, cIndexHigh, 0, this.size);
				this.indexHigh = cIndexHigh;
				long[] cIndexLow = new long[this.size * 2];
				System.arraycopy(this.indexLow, 0, cIndexLow, 0, this.size);
				this.indexLow = cIndexLow;
				long[] cData = new long[this.size * 2];
				System.arraycopy(this.data, 0, cData, 0, this.size);
				this.data = cData;
			}
			this.indexHigh[this.size] = uuid[0];
			this.indexLow[this.size] = uuid[1];
			this.data[this.size] = docNr;
			this.size++;
		}
		DocumentUuidResolver cloneToSize() {
			if (this.data.length == this.size)
				return this;
			long[] cIndexHigh = new long[this.size];
			System.arraycopy(this.indexHigh, 0, cIndexHigh, 0, this.size);
			long[] cIndexLow = new long[this.size];
			System.arraycopy(this.indexLow, 0, cIndexLow, 0, this.size);
			long[] cData = new long[this.size];
			System.arraycopy(this.data, 0, cData, 0, this.size);
			return new DocumentUuidResolver(cIndexHigh, cIndexLow, cData, this.size);
		}
	}
	
	private static class MasterDocumentIdMapper {
		private long[] indexHigh;
		private long[] indexLow;
		private int[] dataOffsets;
		private int size;
		private long[] data;
		private int dataSize;
		MasterDocumentIdMapper() {
			this.indexHigh = new long[32];
			this.indexLow = new long[32];
			this.dataOffsets = new int[32];
			this.size = 0;
			this.data = new long[1024];
			this.dataSize = 0;
		}
		private MasterDocumentIdMapper(long[] indexHigh, long[] indexLow, int[] dataOffsets, int size, long[] data, int dataSize) {
			this.indexHigh = indexHigh;
			this.indexLow = indexLow;
			this.dataOffsets = dataOffsets;
			this.size = size;
			this.data = data;
			this.dataSize = dataSize;
		}
		private int findMasterDocumentIdPos(String masterDocId) {
			long mdidHigh = encodeHex(masterDocId, 0);
			int pos = Arrays.binarySearch(this.indexHigh, 0, this.size, mdidHigh);
			if ((pos < 0) || (this.size <= pos))
				return -1;
			if (this.indexHigh[pos] != mdidHigh)
				return -1;
			while ((pos != 0) && (this.indexHigh[pos-1] == mdidHigh))
				pos--;
			long mdidLow = encodeHex(masterDocId, 16);
			while ((pos < this.size) && (this.indexHigh[pos] == mdidHigh)) {
				if (this.indexLow[pos] < mdidLow)
					pos++;
				else if (this.indexLow[pos] == mdidLow)
					return pos;
				else break;
			}
			return -1;
		}
//		boolean containsMasterDocumentId(String masterDocId) {
//			return (this.findMasterDocumentIdPos(masterDocId) != -1);
//		}
		long[] getDocumentNumbers(String masterDocId) {
			int pos = this.findMasterDocumentIdPos(masterDocId);
			if (pos == -1)
				return null;
			int dataStart = this.dataOffsets[pos];
			int dataEnd = (((pos + 1) < this.size) ? this.dataOffsets[pos + 1] : this.dataSize);
			long[] docNrs = new long[dataEnd - dataStart];
			System.arraycopy(this.data, dataStart, docNrs, 0, docNrs.length);
			return docNrs;
		}
		int getDocumentCount() {
			return this.dataSize;
		}
		int getDocumentCount(String masterDocId) {
			int pos = this.findMasterDocumentIdPos(masterDocId);
			if (pos == -1)
				return -1;
			int dataStart = this.dataOffsets[pos];
			int dataEnd = (((pos + 1) < this.size) ? this.dataOffsets[pos + 1] : this.dataSize);
			return (dataEnd - dataStart);
		}
		
		boolean updateMasterDocumentIdMapping(String masterDocId, long[] docNrs) {
			int pos = this.findMasterDocumentIdPos(masterDocId);
			if (pos == -1)
				return false;
			int dataStart = this.dataOffsets[pos];
			int dataEnd = (((pos + 1) < this.size) ? this.dataOffsets[pos + 1] : this.dataSize);
			if (docNrs.length != (dataEnd - dataStart))
				return false;
			System.arraycopy(docNrs, 0, this.data, dataStart, docNrs.length);
			return true;
		}
		MasterDocumentIdMapper cloneForChanges(String masterDocId, long[] docNrs) {
			
			//	try to perform update in place
			if ((docNrs != null) && (docNrs.length != 0) && this.updateMasterDocumentIdMapping(masterDocId, docNrs))
				return this;
			
			//	extract binary key, and get number of existing entries
			int exDocNrCount = this.getDocumentCount(masterDocId);
			
			//	no use removing non-existing entry
			if ((exDocNrCount == -1) && ((docNrs == null) || (docNrs.length == 0)))
				return this;
			
			//	compute size of cloned arrays
			int cSize;
			int cDataSize;
			
			//	new entry, also need to manipulate index arrays
			if (exDocNrCount == -1) {
				cSize = (this.size + 1);
				cDataSize = (this.dataSize + docNrs.length);
			}
			
			//	removal of existing entry
			else if ((docNrs == null) || (docNrs.length == 0)) {
				cSize = (this.size - 1);
				cDataSize = (this.dataSize - exDocNrCount);
			}
			
			//	existing entry with changed content
			else {
				cSize = this.size;
				cDataSize = (this.dataSize + docNrs.length - exDocNrCount);
			}
			
			//	get the bytes we're aiming for
			long addedHigh = encodeHex(masterDocId, 0);
			long addedLow = encodeHex(masterDocId, 16);
			
			//	merge data into new array (we're only ever reading the main ones, for thread safety)
			long[] cIndexHigh = new long[cSize];
			long[] cIndexLow = new long[cSize];
			int[] cDataOffsets = new int[cSize];
			int cn = 0;
			long[] cData = new long[cDataSize];
			int cdn = 0;
			for (int n = 0; n < this.size; n++) {
				
				//	handle insertion before current master document ID
				if ((exDocNrCount == -1) && (addedHigh < this.indexHigh[n]) || ((addedHigh == this.indexHigh[n]) && (addedLow < this.indexLow[n]))) {
					cIndexHigh[cn] = addedHigh;
					cIndexLow[cn] = addedLow;
					cDataOffsets[cn] = cdn;
					System.arraycopy(docNrs, 0, cData, cdn, docNrs.length);
					cn++;
					cdn += docNrs.length;
					exDocNrCount = docNrs.length; // need to mark as done
				}
				
				//	handle update or skip removal
				else if ((addedHigh == this.indexHigh[n]) && (addedLow == this.indexLow[n])) {
					if ((docNrs == null) || (docNrs.length == 0))
						continue;
					cDataOffsets[cn] = cdn;
					System.arraycopy(docNrs, 0, cData, cdn, docNrs.length);
					cn++;
					cdn += docNrs.length;
					continue;
				}
				
				//	copy current master document ID and associated document numbers
				cIndexHigh[cn] = this.indexHigh[n];
				cIndexLow[cn] = this.indexLow[n];
				cDataOffsets[cn] = cdn;
				int dataStart = this.dataOffsets[n];
				int dataEnd = (((n + 1) < this.size) ? this.dataOffsets[n + 1] : this.dataSize);
				System.arraycopy(this.data, dataStart, cData, cdn, (dataEnd - dataStart));
				cn++;
				cdn += (dataEnd - dataStart);
			}
			
			//	handle any additions after last document number
			if (exDocNrCount == -1) {
				cIndexHigh[cn] = addedHigh;
				cIndexLow[cn] = addedLow;
				cDataOffsets[cn] = cdn;
				System.arraycopy(docNrs, 0, cData, cdn, docNrs.length);
				cn++;
				cdn += docNrs.length;
			}
			
			//	finally ...
			return new MasterDocumentIdMapper(cIndexHigh, cIndexLow, cDataOffsets, cSize, cData, cDataSize);
		}
		
		void mapMasterDocumentId(String masterDocId, long[] docNrs) {
			long mdidHigh = encodeHex(masterDocId, 0);
			long mdidLow = encodeHex(masterDocId, 16);
			if (this.size == 0) {}
			else if (mdidHigh < this.indexHigh[this.size-1])
				throw new IllegalArgumentException("Map master document IDs in incremental order !!!");
			else if ((mdidHigh == this.indexHigh[this.size-1]) && (mdidLow <= this.indexLow[this.size-1]))
				throw new IllegalArgumentException("Map master document IDs in incremental order !!!");
			if (this.dataOffsets.length == this.size) {
				long[] cIndexHigh = new long[this.size * 2];
				System.arraycopy(this.indexHigh, 0, cIndexHigh, 0, this.size);
				this.indexHigh = cIndexHigh;
				long[] cIndexLow = new long[this.size * 2];
				System.arraycopy(this.indexLow, 0, cIndexLow, 0, this.size);
				this.indexLow = cIndexLow;
				int[] cDataOffsets = new int[this.size * 2];
				System.arraycopy(this.dataOffsets, 0, cDataOffsets, 0, this.size);
				this.dataOffsets = cDataOffsets;
			}
			this.indexHigh[this.size] = mdidHigh;
			this.indexLow[this.size] = mdidLow;
			this.dataOffsets[this.size] = this.dataSize;
			this.size++;
			if (this.data.length < (this.dataSize + docNrs.length)) {
				int scaleUp = 2;
				while ((this.data.length * scaleUp) < (this.dataSize + docNrs.length))
					scaleUp *= 2;
				long[] cData = new long[this.data.length * scaleUp];
				System.arraycopy(this.data, 0, cData, 0, this.dataSize);
				this.data = cData;
			}
			System.arraycopy(docNrs, 0, this.data, this.dataSize, docNrs.length);
			this.dataSize += docNrs.length;
		}
		MasterDocumentIdMapper cloneToSize() {
			if ((this.dataOffsets.length == this.size) && (this.data.length == this.dataSize))
				return this;
			long[] cIndexHigh = new long[this.size];
			System.arraycopy(this.indexHigh, 0, cIndexHigh, 0, this.size);
			long[] cIndexLow = new long[this.size];
			System.arraycopy(this.indexLow, 0, cIndexLow, 0, this.size);
			int[] cDataOffsets = new int[this.size];
			System.arraycopy(this.dataOffsets, 0, cDataOffsets, 0, this.size);
			long[] cData = new long[this.dataSize];
			System.arraycopy(this.data, 0, cData, 0, this.dataSize);
			return new MasterDocumentIdMapper(cIndexHigh, cIndexLow, cDataOffsets, this.size, cData, this.dataSize);
		}
	}
	
	static final Comparator signAwareHexStringOrder = new Comparator() {
		public int compare(Object obj1, Object obj2) {
			String hex1 = ((String) obj1);
			String hex2 = ((String) obj2);
			/* need to sort 8-F starts before 0-7 starts, as former
			 * become negative on encoding in primitive longs */
			long high1 = encodeHex(hex1, 0);
			long high2 = encodeHex(hex2, 0);
			if (high1 != high2)
				return ((high1 < high2) ? -1 : 1);
			long low1 = encodeHex(hex1, 16);
			long low2 = encodeHex(hex2, 16);
			if (low1 == low2)
				return 0;
			return ((low1 < low2) ? -1 : 1);
		}
	};
	
	static void encodeHex(long[] data, int dataPos, String hex) {
		data[dataPos] = encodeHex(hex, 0);
		data[dataPos + 1] = encodeHex(hex, 16);
	}
	static long encodeHex(String hex, int fromChar) {
		if (hex.length() < (fromChar + 16))
			throw new IllegalArgumentException("Cannot encode '" + hex + "' from position " + fromChar + ", only " + (hex.length() - fromChar) + " chars left, need 16.");
		long binary = 0;
		for (int hexPos = fromChar; hexPos < (fromChar + 16); hexPos++) {
			char ch = hex.charAt(hexPos);
			binary <<= 4;
			byte b;
			if (('0' <= ch) && (ch <= '9'))
				b = ((byte) (ch - '0'));
			else if (('A' <= ch) && (ch <= 'F'))
				b = ((byte) ((ch - 'A') + 10));
			else if (('a' <= ch) && (ch <= 'f'))
				b = ((byte) ((ch - 'a') + 10));
			else throw new IllegalArgumentException("Illegal HEX character in '" + hex + "': " + ch);
			binary |= b;
		}
		return binary;
	}
	
	private static String defaultDocumentAttribute(String name, String value) {
		if (value != null)
			return value;
		if (PAGE_NUMBER_ATTRIBUTE.equals(name))
			return "-1";
		if (LAST_PAGE_NUMBER_ATTRIBUTE.equals(name))
			return "-1";
		if (MASTER_PAGE_NUMBER_ATTRIBUTE.equals(name))
			return "-1";
		if (MASTER_LAST_PAGE_NUMBER_ATTRIBUTE.equals(name))
			return "-1";
		if (CHECKIN_USER_ATTRIBUTE.equals(name))
			return "Unknown User";
		if (UPDATE_USER_ATTRIBUTE.equals(name))
			return "Unknown User";
		if (DOCUMENT_AUTHOR_ATTRIBUTE.equals(name))
			return "Unknown Author";
		if (DOCUMENT_ORIGIN_ATTRIBUTE.equals(name))
			return "Unknown Journal or Book";
		if (MASTER_DOCUMENT_TITLE_ATTRIBUTE.equals(name))
			return "Unknown Document";
		return "";
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
	 * Add a document event listener to the GoldenGATE SRS so it receives
	 * notification of changes to the document stored.
	 * @param del the document event listener to add
	 */
	public void addDocumentEventListener(SrsDocumentEventListener del) {
		GoldenGateServerEventService.addServerEventListener(del);
	}
	
	/**
	 * Remove a document storage listener from the GoldenGATE SRS.
	 * @param del the document event listener to remove
	 */
	public void removeDocumentEventListener(SrsDocumentEventListener del) {
		GoldenGateServerEventService.removeServerEventListener(del);
	}
	
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
//	
//	//	FOR TESTING ONLY !!!
//	public static void main(String[] args) throws Exception {
//		DocumentNumberResolver dnr = new DocumentNumberResolver();
//		HashMap docNrsToIDs = new HashMap();
//		for (int d = 0; d < 10000; d++) {
//			long docNr = d;
//			String docId = RandomByteSource.getRandomHex(128);
//			dnr.mapDocumentNumber(docNr, docId);
//			docNrsToIDs.put(new Long(docNr), docId);
//		}
//		System.out.println(dnr.size + " (array is " + dnr.index.length + ")");
//		dnr = dnr.cloneToSize();
//		System.out.println(dnr.size + " (array is " + dnr.index.length + ")");
//		testDocumentNumberResolver(dnr, docNrsToIDs);
//		HashSet addDocIDs = new HashSet();
//		long[] removeDocNrs = new long[6];
//		for (int r = 0; r < removeDocNrs.length; r++) {
//			removeDocNrs[r] = ((long) (10000 * Math.random()));
//			docNrsToIDs.remove(new Long(removeDocNrs[r]));
//		}
//		for (int a = 0; a < 27; a++) {
//			String docId = RandomByteSource.getRandomHex(128);
//			addDocIDs.add(docId);
//			docNrsToIDs.put(new Long(getDocNr(docId)), docId);
//		}
//		dnr = dnr.cloneForChanges(addDocIDs, removeDocNrs);
//		System.out.println(dnr.size + " (array is " + dnr.index.length + ")");
//		testDocumentNumberResolver(dnr, docNrsToIDs);
//		for (int r = 0; r < removeDocNrs.length; r++) {
//			String docId = dnr.getDocumentId(removeDocNrs[r]);
//			System.out.println(removeDocNrs[r] + " ==> " + docId);
//		}
//	}
//	private static void testDocumentNumberResolver(DocumentNumberResolver dnr, HashMap docNrsToIDs) {
//		long maxDocNr = dnr.index[dnr.size - 1];
//		for (int d = 0; d < 200; d++) {
//			long docNr = ((long) (maxDocNr * 1.3 * Math.random()));
//			String docId = dnr.getDocumentId(docNr);
//			System.out.println(docNr + " ==> " + docId);
//			String cDocId = ((String) docNrsToIDs.get(new Long(docNr)));
//			if (docId == null) {
//				if (cDocId == null)
//					System.out.println(" ==> MATCH");
//				else System.out.println(" ==> FUCK");
//			}
//			else {
//				if (docId.equals(cDocId))
//					System.out.println(" ==> MATCH");
//				else System.out.println(" ==> FUCK");
//			}
//		}
//	}
//	
//	//	FOR TESTING ONLY !!!
//	public static void main(String[] args) throws Exception {
//		DocumentUuidResolver dur = new DocumentUuidResolver();
//		TreeMap docUuidsToNrs = new TreeMap(signAwareHexStringOrder);
//		for (int d = 0; d < 10000; d++) {
//			String docUuid = RandomByteSource.getRandomHex(128);
//			long docNr = getDocNr(docUuid);
//			docUuidsToNrs.put(docUuid, new Long(docNr));
//		}
//		for (Iterator duit = docUuidsToNrs.keySet().iterator(); duit.hasNext();) {
//			String docUuid = ((String) duit.next());
//			Long docNr = ((Long) docUuidsToNrs.get(docUuid));
//			dur.mapDocumentUuid(docUuid, docNr);
//		}
//		System.out.println(dur.size + " (array is " + dur.data.length + ")");
//		dur = dur.cloneToSize();
//		System.out.println(dur.size + " (array is " + dur.data.length + ")");
//		testDocumentUuidResolver(dur, docUuidsToNrs);
//		HashSet removeDocUuids = new HashSet();
//		for (Iterator duit = docUuidsToNrs.keySet().iterator(); duit.hasNext();) {
//			String docUuid = ((String) duit.next());
//			if (Math.random() < 0.01)
//				removeDocUuids.add(docUuid);
//		}
//		for (Iterator duit = removeDocUuids.iterator(); duit.hasNext();) {
//			String docUuid = ((String) duit.next());
//			docUuidsToNrs.remove(docUuid);
//		}
//		System.out.println("Removing " + removeDocUuids.size() + " UUIDs");
//		HashMap addDocUuids = new HashMap();
//		for (int a = 0; a < 1000; a++) {
//			String docUuid = RandomByteSource.getRandomHex(128);
//			long docNr = getDocNr(docUuid);
//			docUuidsToNrs.put(docUuid, new Long(docNr));
//			addDocUuids.put(docUuid, new Long(docNr));
//		}
//		dur = dur.cloneForChanges(addDocUuids, removeDocUuids);
//		System.out.println(dur.size + " (array is " + dur.data.length + ")");
//		testDocumentUuidResolver(dur, docUuidsToNrs);
//		for (Iterator duit = removeDocUuids.iterator(); duit.hasNext();) {
//			String docUuid = ((String) duit.next());
//			System.out.println(docUuid + " ==> " + dur.getDocumentNumber(docUuid));
//		}
//	}
//	private static void testDocumentUuidResolver(DocumentUuidResolver dur, TreeMap docUuidsToNrs) {
//		for (Iterator duit = docUuidsToNrs.keySet().iterator(); duit.hasNext();) {
//			String docUuid = ((String) duit.next());
//			if (Math.random() < 0.01) {
//				Long docNr = ((Long) docUuidsToNrs.get(docUuid));
//				System.out.println(docUuid + " ==> " + docNr);
//				if (docNr.longValue() == dur.getDocumentNumber(docUuid))
//					System.out.println(" ==> MATCH");
//				else System.out.println(" ==> FUCK");
//			}
//		}
//	}
//	
//	//	FOR TESTING ONLY !!!
//	public static void main(String[] args) throws Exception {
//		MasterDocumentIdMapper mdim = new MasterDocumentIdMapper();
//		TreeMap masterDocIdsToNrs = new TreeMap(signAwareHexStringOrder);
//		for (int md = 0; md < 1000; md++) {
//			String masterDocId = RandomByteSource.getRandomHex(128);
//			int docCount = ((int) (Math.random() * 1000));
//			long[] docNrs = new long[Math.max(docCount, 1)];
//			for (int d = 0; d < docNrs.length; d++)
//				docNrs[d] = Double.doubleToLongBits(Math.random());
//			masterDocIdsToNrs.put(masterDocId, docNrs);
//		}
//		for (Iterator mdidit = masterDocIdsToNrs.keySet().iterator(); mdidit.hasNext();) {
//			String masterDocId = ((String) mdidit.next());
//			long[] docNrs = ((long[]) masterDocIdsToNrs.get(masterDocId));
//			mdim.mapMasterDocumentId(masterDocId, docNrs);
//		}
//		System.out.println(mdim.size + " (array is " + mdim.dataOffsets.length + "), " + mdim.dataSize + " (array is " + mdim.data.length + ")");
//		mdim = mdim.cloneToSize();
//		System.out.println(mdim.size + " (array is " + mdim.dataOffsets.length + "), " + mdim.dataSize + " (array is " + mdim.data.length + ")");
//		testMasterDocumentIdMapper(mdim, masterDocIdsToNrs);
//		String removeMasterDocId = null;
//		for (Iterator mdidit = masterDocIdsToNrs.keySet().iterator(); mdidit.hasNext();) {
//			String masterDocId = ((String) mdidit.next());
//			if (Math.random() < 0.01)
//				removeMasterDocId = masterDocId;
//		}
//		if (removeMasterDocId != null) {
//			System.out.println("Removing master document ID " + removeMasterDocId);
//			mdim = mdim.cloneForChanges(removeMasterDocId, null);
//			System.out.println(mdim.size + " (array is " + mdim.dataOffsets.length + "), " + mdim.dataSize + " (array is " + mdim.data.length + ")");
//			testMasterDocumentIdMapper(mdim, masterDocIdsToNrs);
//		}
//		String addMasterDocId = RandomByteSource.getRandomHex(128);
//		int addDocCount = ((int) (Math.random() * 1000));
//		long[] addDocNrs = new long[Math.max(addDocCount, 1)];
//		for (int d = 0; d < addDocNrs.length; d++)
//			addDocNrs[d] = Double.doubleToLongBits(Math.random());
//		masterDocIdsToNrs.put(addMasterDocId, addDocNrs);
//		System.out.println("Adding master document ID " + addMasterDocId + " with " + addDocNrs.length + " document numbers");
//		mdim = mdim.cloneForChanges(addMasterDocId, addDocNrs);
//		System.out.println(mdim.size + " (array is " + mdim.dataOffsets.length + "), " + mdim.dataSize + " (array is " + mdim.data.length + ")");
//		testMasterDocumentIdMapper(mdim, masterDocIdsToNrs);
//		String updateMasterDocId = null;
//		for (Iterator mdidit = masterDocIdsToNrs.keySet().iterator(); mdidit.hasNext();) {
//			String masterDocId = ((String) mdidit.next());
//			if (Math.random() < 0.01)
//				updateMasterDocId = masterDocId;
//		}
//		if (updateMasterDocId != null) {
//			int updateDocCount = ((int) (Math.random() * 1000));
//			long[] updateDocNrs = new long[Math.max(updateDocCount, 1)];
//			for (int d = 0; d < updateDocNrs.length; d++)
//				updateDocNrs[d] = Double.doubleToLongBits(Math.random());
//			long[] docNrs = ((long[]) masterDocIdsToNrs.put(updateMasterDocId, updateDocNrs));
//			System.out.println("Updating master document ID " + updateMasterDocId + " from " + docNrs.length + " to " + updateDocNrs.length + " document numbers");
//			mdim = mdim.cloneForChanges(updateMasterDocId, updateDocNrs);
//			System.out.println(mdim.size + " (array is " + mdim.dataOffsets.length + "), " + mdim.dataSize + " (array is " + mdim.data.length + ")");
//			testMasterDocumentIdMapper(mdim, masterDocIdsToNrs);
//		}
//	}
//	private static void testMasterDocumentIdMapper(MasterDocumentIdMapper mdim, TreeMap masterDocIdsToNrs) {
//		for (Iterator mdidit = masterDocIdsToNrs.keySet().iterator(); mdidit.hasNext();) {
//			String masterDocId = ((String) mdidit.next());
//			if (Math.random() < 0.05) {
//				long[] docNrs = ((long[]) masterDocIdsToNrs.get(masterDocId));
//				System.out.println(masterDocId + " ==> " + docNrs);
//				long[] rDocNrs = mdim.getDocumentNumbers(masterDocId);
//				if (rDocNrs == null)
//					System.out.println(" ==> FUCK");
//				else if (docNrs.length != rDocNrs.length)
//					System.out.println(" ==> FUCK");
//				else if (Arrays.equals(docNrs, rDocNrs))
//					System.out.println(" ==> MATCH");
//				else System.out.println(" ==> FUCK");
//			}
//		}
//	}
}
