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
import java.net.URLDecoder;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AttributeUtils;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest.AttributeFilter;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest.TypeFilter;
import de.uka.ipd.idaho.gamta.util.AnnotationFilter;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.ComponentInitializer;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.AsynchronousWorkQueue;
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
	
	private StorageFilter[] filters = new StorageFilter[0];
	private DocumentSplitter[] splitters = new DocumentSplitter[0];
	private DocumentUuidFetcher[] uuidFetchers = new DocumentUuidFetcher[0];
	
	private IoProvider io;
	
	private int activityLogTimeout = 5000;
	
	// TODO in the long haul, consider processing index queries directly against database where possible
	
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
		this.io.indexColumn(DOCUMENT_TABLE_NAME, Indexer.DOC_NUMBER_COLUMN_NAME);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_UUID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, DOCUMENT_UUID_HASH_COLUMN_NAME);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, MASTER_DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_TABLE_NAME, MASTER_DOCUMENT_ID_HASH_COLUMN_NAME);
		
		//	report status
		System.out.println("  - database connection established");
		
		
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
		
		//	get document storage folder
		String docFolderName = this.configuration.getSetting("documentFolderName", "Documents");
		while (docFolderName.startsWith("./"))
			docFolderName = docFolderName.substring("./".length());
		File docFolder = (((docFolderName.indexOf(":\\") == -1) && (docFolderName.indexOf(":/") == -1) && !docFolderName.startsWith("/")) ? new File(this.dataPath, docFolderName) : new File(docFolderName));
		
		//	initialize document store
		this.dst = new DocumentStore("SrsDocuments", docFolder, this.configuration.getSetting("documentEncoding"), this);
		System.out.println("  - got document store");
		
		
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
		
		//	start indexer service thread
		synchronized (this.indexerActionQueue) {
			this.indexerService = new IndexerServiceThread();
			this.indexerService.start();
			try {
				this.indexerActionQueue.wait();
			} catch (InterruptedException ie) {}
			this.indexerServiceMonitor = new AsynchronousWorkQueue("SrsIndexerService") {
				public String getStatus() {
					return (this.name + ": " + indexerActionQueue.size() + " indexin actions pending");
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
		this.reindexAction = new AsynchronousConsoleAction("update", "Re-index the document collection managed by this GoldenGATE SRS", "index", this.dataPath, "SrsIndexUpdateLog") {
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
			protected void performAction(String[] arguments) throws Exception {
				
				//	read names of indices to rebuild
				Set reIndexNameSet = new HashSet();
				if (arguments.length == 0) {
					for (int i = 0; i < GoldenGateSRS.this.indexers.length; i++)
						reIndexNameSet.add(GoldenGateSRS.this.indexers[i].getIndexName());
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
				String query = "SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + 
						" FROM " + DOCUMENT_TABLE_NAME +
						" ORDER BY " + DOC_NUMBER_COLUMN_NAME + 
						";";
				
				SqlQueryResult sqr = null;
				try {
					
					//	read document list
					sqr = io.executeSelectQuery(query, true);
					
					//	get collection statistics for status info 
					CollectionStatistics stat = getStatistics();
					this.enteringMainLoop("0 of " + stat.docCount + " documents done");
					
					//	process documents
					int updateDocCount = 0;
					while (sqr.next()) {
						
						//	read doc ID
						String docId = sqr.getString(1);
						try {
							//	read data
							long docNr = sqr.getLong(0);
							this.log("  - processing document " + docNr);
							
							MutableAnnotation doc = dst.loadDocument(docId);
							this.log("    - document loaded");
							
							//	run document through indexers
							for (int i = 0; i < GoldenGateSRS.this.indexers.length; i++)
								if (reIndexNameSet.contains(GoldenGateSRS.this.indexers[i].getIndexName())) {
									Indexer indexer = GoldenGateSRS.this.indexers[i];
									this.log("    - doing indexer " + indexer.getClass().getName());
									
									indexer.deleteDocument(docNr);
									this.log("      - document un-indexed");
									
									indexer.index(doc, docNr);
									this.log("      - document re-indexed");
								}
							this.log("    - document done");
						}
						catch (Exception e) {
							this.log(("GoldenGateSRS: " + e.getClass().getName() + " (" + e.getMessage() + ") while re-indexing document " + docId), e);
						}
						catch (Error e) {
							this.log(("GoldenGateSRS: " + e.getClass().getName() + " (" + e.getMessage() + ") while re-indexing document " + docId), new Exception(e));
						}
						
						//	update status info
						this.loopRoundComplete((++updateDocCount) + " of " + stat.docCount + " documents done");
					}
				}
				catch (SQLException sqle) {
					this.log("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while re-indexing document collection.");
					this.log("  Query was " + query);
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
		
		//	make sure to load modification time
		this.getLastModified();
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
	private static final String LIST_FILTERS_COMMAND = "filters";
	private static final String LIST_SPLITTERS_COMMAND = "splitters";
	private static final String LIST_UUID_FETCHERS_COMMAND = "uuidFetchers";
	
	private static final String SET_ACTIVITY_LOG_TIMEOUT_COMMAND = "setAlt";
	
	private static final String REINDEX_COMMAND = "reIndex";
	
	private static final String ISSUE_EVENTS_COMMAND = "issueEvents";
	
	private AsynchronousConsoleAction reindexAction;
	
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
				return REINDEX_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						REINDEX_COMMAND + " <masterDocId> <indexerName>",
						"Re-index all documents from a specific master document:",
						"- <masterDocId>: the ID of the master document to re-index",
						"- <indexerName>: the name of the indexer to re-index the document with (optional)",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if ((arguments.length < 1) || (arguments.length > 2))
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the master document ID and indexer name as the only arguments.");
				else reIndex(arguments[0], ((arguments.length == 1) ? null : arguments[1]));
			}
		};
		cal.add(ca);
		
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
		cal.add(this.reindexAction);
		
		//	get actions from document store
		ComponentAction[] dstActions = this.dst.getActions();
		for (int a = 0; a < dstActions.length; a++)
			cal.add(dstActions[a]);
//		
//		//	add event queue monitoring action
//		cal.add(this.eventNotifier.getQueueSizeAction());
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
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
	
	private void reIndex(final String masterDocId, String indexerName) {
		
		//	assemble query
		StringBuffer query = new StringBuffer("SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE);
		query.append(" FROM " + DOCUMENT_TABLE_NAME);
		if (masterDocId.matches("[0-9A-F]{32}")) {
			query.append(" WHERE " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocId) + "'");
			query.append(" AND " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocId.hashCode() + "");
		}
		else query.append(" WHERE " + MASTER_DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(masterDocId) + "'");
		query.append(";");
		
		//	get document IDs
		SqlQueryResult sqr = null;
		int count = 0;
		try {
			sqr = io.executeSelectQuery(query.toString(), true);
			while (sqr.next()) {
				final long docNr = sqr.getLong(0);
				final String docId = sqr.getString(1);
				for (int i = 0; i < this.indexers.length; i++) {
					if ((indexerName != null) && !indexerName.equals(this.indexers[i].getIndexName()))
						continue;
					final Indexer indexer = this.indexers[i];
					this.enqueueIndexerAction(new Runnable() {
						public void run() {
							try {
								QueriableAnnotation doc = dst.loadDocument(docId);
								indexer.deleteDocument(docNr);
								indexer.index(doc, docNr);
							}
							catch (IOException ioe) {
								logError("GoldenGateSRS: error re-indexing document '" + docId + "': " + ioe.getMessage());
								logError(ioe);
							}
						}
					});
					count++;
				}
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
			this.logError("  query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		this.logResult("Scheduled re-indexing of for " + count + " documents.");
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
						GoldenGateServerEventService.notify(new SrsDocumentEvent(updateUser, docId, null, docVersion, GoldenGateSRS.class.getName(), updateTime, new EventLogger() {
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
	
	private CollectionStatistics getStatistics() throws IOException {
		return this.getStatistics(null);
	}
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
		
		//	initialize statistics
		int newDocCount = 0;
		int updateDocCount = 0;
		
		//	store part documents
		storageStepStart = System.currentTimeMillis();
		StringVector validIDs = new StringVector();
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
			int docStatus = this.updateDocument(docs[d], docId, masterDocId, updateTime, logger);
			
			//	update statistics
			if (docStatus == NEW_DOC)
				newDocCount++;
			else if (docStatus == UPDATE_DOC)
				updateDocCount++;
			
			//	how long did this take?
			this.logInfo("   - part " + (d+1) + " of " + docs.length + " stored in " + (System.currentTimeMillis() - docStorageStart) + " ms");
		}
		this.logInfo(" - parts stored in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	clean up document table
		storageStepStart = System.currentTimeMillis();
		String updateUser = ((String) masterDoc.getAttribute(UPDATE_USER_ATTRIBUTE, masterDoc.getAttribute(CHECKIN_USER_ATTRIBUTE, "Unknown User")));
		int deleteDocCount = this.cleanupMasterDocument(updateUser, masterDocId, validIDs, logger);
		this.logInfo(" - master document cleanup done in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
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
	
	private static final int KEEP_DOC = 0;
	private static final int UPDATE_DOC = 1;
	private static final int NEW_DOC = 2;
	private int updateDocument(final QueriableAnnotation doc, final String docId, String masterDocId, final long updateTime, final EventLogger logger) throws IOException {
		long storageStepStart;
		final long docNr = getDocNr(docId);
		final int documentStatus;
		
		storageStepStart = System.currentTimeMillis();
		String docChecksum = this.getChecksum(doc);
		String userName = ((String) doc.getAttribute(UPDATE_USER_ATTRIBUTE, doc.getAttribute(CHECKIN_USER_ATTRIBUTE, "Unknown User")));
		
		String existQuery = "SELECT " + DOCUMENT_CHECKSUM_COLUMN_NAME + ", " + DOCUMENT_UUID_ATTRIBUTE + ", " + DOCUMENT_UUID_SOURCE_ATTRIBUTE + ", " + MASTER_DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_AUTHOR_ATTRIBUTE + ", " + DOCUMENT_ORIGIN_ATTRIBUTE + ", " + DOCUMENT_DATE_ATTRIBUTE + ", " + PAGE_NUMBER_ATTRIBUTE + ", " + LAST_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_LAST_PAGE_NUMBER_ATTRIBUTE + ", " + DOCUMENT_SOURCE_LINK_ATTRIBUTE + ", " + DOCUMENT_TYPE_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME +
				" WHERE " + DOC_NUMBER_COLUMN_NAME + " = " + docNr + "" +
				" AND " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		
		SqlQueryResult sqr = null;
		storageStepStart = System.currentTimeMillis();
		try {
			sqr = this.io.executeSelectQuery(existQuery);
			this.logInfo("   - existing document data read in " + (System.currentTimeMillis() - storageStepStart) + " ms");
			
			//	document exists
			if (sqr.next()) {
				
				//	read data
				storageStepStart = System.currentTimeMillis();
				this.logDebug("  - updating document " + docNr);
				
				String oldDocChecksum = sqr.getString(0);
				
				//	document unchanged (or we have an MD5 collision ...)
				if (docChecksum.equals(oldDocChecksum)) {
					this.logDebug("    - document unmodified");
					documentStatus = KEEP_DOC;
				}
				
				//	we have an update
				else {
					this.logDebug("    - document modified");
					documentStatus = UPDATE_DOC;
					
					//	read remaining data
					String dataDocUuid = sqr.getString(1);
					String dataDocUuidSource = sqr.getString(2);
					String dataMasterDocTitle = sqr.getString(3);
					String dataDocTitle = sqr.getString(4);
					String dataDocAuthor = sqr.getString(5);
					String dataDocOrigin = sqr.getString(6);
					String dataDocYear = sqr.getString(7);
					String dataPageNumber = sqr.getString(8);
					String dataLastPageNumber = sqr.getString(9);
					String dataMasterDocPageNumber = sqr.getString(10);
					String dataMasterDocLastPageNumber = sqr.getString(11);
					String dataDocSourceLink = sqr.getString(12);
					String dataDocType = sqr.getString(13);
					this.logDebug("    - got document data");
					
					StringVector assignments = new StringVector();
					
					//	update checksum
					assignments.addElement(DOCUMENT_CHECKSUM_COLUMN_NAME + " = '" + EasyIO.sqlEscape(docChecksum) + "'");
					
					//	check document type
					String type = ((String) doc.getAttribute(DOCUMENT_TYPE_ATTRIBUTE, "")).trim();
					if (type.length() > 32)
						type = type.substring(0, 32).trim();
					if (!type.equals(dataDocType))
						assignments.addElement(DOCUMENT_TYPE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(type) + "'");
					
					//	check UUID
					String uuid = normalizeId((String) doc.getAttribute(DOCUMENT_UUID_ATTRIBUTE, ""));
					if (uuid.length() > 32)
						uuid = uuid.substring(0, 32);
					if (!uuid.equals(dataDocUuid)) {
						assignments.addElement(DOCUMENT_UUID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(uuid) + "'");
						assignments.addElement(DOCUMENT_UUID_HASH_COLUMN_NAME + " = " + uuid.hashCode() + "");
					}
					
					//	check UUID source
					String uuidSource = ((String) doc.getAttribute(DOCUMENT_UUID_SOURCE_ATTRIBUTE, "")).trim();
					if (uuidSource.length() > 32)
						uuidSource = uuidSource.substring(0, 32);
					if (!uuidSource.equals(dataDocUuidSource))
						assignments.addElement(DOCUMENT_UUID_SOURCE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(uuidSource) + "'");
					
					//	check and (if necessary) truncate title
					String masterTitle = ((String) doc.getAttribute(MASTER_DOCUMENT_TITLE_ATTRIBUTE, "Unknown Document")).trim();
					if (masterTitle.length() > MASTER_DOCUMENT_TITLE_COLUMN_LENGTH)
						masterTitle = masterTitle.substring(0, MASTER_DOCUMENT_TITLE_COLUMN_LENGTH);
					if (!masterTitle.equals(dataMasterDocTitle))
						assignments.addElement(MASTER_DOCUMENT_TITLE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterTitle) + "'");
					
					//	check and (if necessary) truncate part title
					String title = ((String) doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, "")).trim();
					if (title.length() > DOCUMENT_TITLE_COLUMN_LENGTH)
						title = title.substring(0, DOCUMENT_TITLE_COLUMN_LENGTH);
					if (!title.equals(dataDocTitle))
						assignments.addElement(DOCUMENT_TITLE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(title) + "'");
					
					//	check and (if necessary) truncate author
					String author = ((String) doc.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, "Unknown Author")).trim();
					if (author.length() > DOCUMENT_AUTHOR_COLUMN_LENGTH)
						author = author.substring(0, DOCUMENT_AUTHOR_COLUMN_LENGTH);
					if (!author.equals(dataDocAuthor))
						assignments.addElement(DOCUMENT_AUTHOR_ATTRIBUTE + " = '" + EasyIO.sqlEscape(author) + "'");
					
					//	check origin
					String origin = ((String) doc.getAttribute(DOCUMENT_ORIGIN_ATTRIBUTE, "Unknown Journal or Book")).trim();
					if (origin.length() > DOCUMENT_ORIGIN_COLUMN_LENGTH)
						origin = origin.substring(0, DOCUMENT_ORIGIN_COLUMN_LENGTH);
					if (!origin.equals(dataDocOrigin))
						assignments.addElement(DOCUMENT_ORIGIN_ATTRIBUTE + " = '" + EasyIO.sqlEscape(origin) + "'");
					
					//	check year
					String year = ((String) doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1")).trim();
					if (!year.equals(dataDocYear))
						assignments.addElement(DOCUMENT_DATE_ATTRIBUTE + " = " + EasyIO.sqlEscape(year));
					
					//	check source link
					String sourceLink = ((String) doc.getAttribute(DOCUMENT_SOURCE_LINK_ATTRIBUTE, "")).trim();
					if (sourceLink.length() > DOCUMENT_SOURCE_LINK_COLUMN_LENGTH)
						sourceLink = sourceLink.substring(0, DOCUMENT_SOURCE_LINK_COLUMN_LENGTH);
					if (!sourceLink.equals(dataDocSourceLink))
						assignments.addElement(DOCUMENT_SOURCE_LINK_ATTRIBUTE + " = '" + EasyIO.sqlEscape(sourceLink) + "'");
					
					//	check page number
					int pageNumber = -1;
					try {
						pageNumber = Integer.parseInt((String) doc.getAttribute(PAGE_NUMBER_ATTRIBUTE, "-1"));
					} catch (NumberFormatException nfe) {}
					if (!("" + pageNumber).equals(dataPageNumber))
						assignments.addElement(PAGE_NUMBER_ATTRIBUTE + " = " + pageNumber);
					
					//	check last page number
					int lastPageNumber = -1;
					try {
						lastPageNumber = Integer.parseInt((String) doc.getAttribute(LAST_PAGE_NUMBER_ATTRIBUTE, "-1"));
					} catch (NumberFormatException nfe) {}
					if (!("" + lastPageNumber).equals(dataLastPageNumber))
						assignments.addElement(LAST_PAGE_NUMBER_ATTRIBUTE + " = " + lastPageNumber);
					
					//	check parent document page number
					int masterDocPageNumber = -1;
					try {
						masterDocPageNumber = Integer.parseInt((String) doc.getAttribute(MASTER_PAGE_NUMBER_ATTRIBUTE, "-1"));
					} catch (NumberFormatException nfe) {}
					if (!("" + masterDocPageNumber).equals(dataMasterDocPageNumber))
						assignments.addElement(MASTER_PAGE_NUMBER_ATTRIBUTE + " = " + masterDocPageNumber);
					
					//	check last page number
					int masterDocLastPageNumber = -1;
					try {
						masterDocLastPageNumber = Integer.parseInt((String) doc.getAttribute(MASTER_LAST_PAGE_NUMBER_ATTRIBUTE, "-1"));
					} catch (NumberFormatException nfe) {}
					if (!("" + masterDocLastPageNumber).equals(dataMasterDocLastPageNumber))
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
					for (int i = 0; i < this.indexers.length; i++) {
						final Indexer indexer = this.indexers[i];
						this.enqueueIndexerAction(new Runnable() {
							public void run() {
								indexer.deleteDocument(docNr);
							}
						});
					}
				}
			}
			
			//	new document
			else {
				storageStepStart = System.currentTimeMillis();
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
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while checking if document exists.");
			this.logError("  Query was " + existQuery);
			throw new IOException(sqle.getMessage());
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	in case of no update, we're done here
		if (documentStatus == KEEP_DOC)
			return documentStatus;
		
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
		
		//	run document through indexers asynchronously for better upload performance
		storageStepStart = System.currentTimeMillis();
		for (int i = 0; i < this.indexers.length; i++) {
			final Indexer indexer = this.indexers[i];
			this.enqueueIndexerAction(new Runnable() {
				public void run() {
					indexer.index(doc, docNr);
				}
			});
		}
		this.logInfo("   - indexing scheduled in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	do event notification asynchronously for quick response
		storageStepStart = System.currentTimeMillis();
		GoldenGateServerEventService.notify(new SrsDocumentEvent(userName, docId, doc, version, this.getClass().getName(), updateTime, logger));
		this.logInfo("   - update notification done in " + (System.currentTimeMillis() - storageStepStart) + " ms");
		
		//	report whether update or insert
		return documentStatus;
	}
	
	//	after a document update, delete the management table entries for the parts of a master document that no longer exist
	private int cleanupMasterDocument(String userName, String masterDocId, StringVector docIDs, EventLogger logger) throws IOException {
		
		//	get document numbers to delete
		String dataQuery = "SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + 
			" FROM " + DOCUMENT_TABLE_NAME + 
			" WHERE " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + masterDocId + "'" +
			" AND " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocId.hashCode() + "" +
				(docIDs.isEmpty() ? "" : (" AND " + DOCUMENT_ID_ATTRIBUTE + " NOT IN ('" + docIDs.concatStrings("', '") + "')")) +
			";";
		int deleteDocCount = 0;
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(dataQuery);
			while (sqr.next()) {
				final long docNr = sqr.getLong(0);
				final String docId = sqr.getString(1);
				
				//	un-index document
				for (int i = 0; i < this.indexers.length; i++) {
					final Indexer indexer = this.indexers[i];
					this.enqueueIndexerAction(new Runnable() {
						public void run() {
							indexer.deleteDocument(docNr);
						}
					});
				}
				
				try {
					this.dst.deleteDocument(docId);
					this.dstLastModified = System.currentTimeMillis();
				}
				catch (IOException ioe) {
					this.logError("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
					this.logError(ioe);
				}
				
				//	issue event
				GoldenGateServerEventService.notify(new SrsDocumentEvent(userName, docId, this.getClass().getName(), System.currentTimeMillis(), logger));
				
				//	delete master table entry
				String deleteQuery = "DELETE FROM " + DOCUMENT_TABLE_NAME + " WHERE " + DOC_NUMBER_COLUMN_NAME + " = " + docNr + ";";
				try {
					this.io.executeUpdateQuery(deleteQuery);
					deleteDocCount++;
				}
				catch (SQLException sqle) {
					this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
					this.logError("  Query was " + deleteQuery);
				}
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while cleaning document table.");
			this.logError("  Query was " + dataQuery);
			return 0;
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	invalidate collection statistics
		synchronized (this.collectionsStatisticsCache) {
			this.collectionsStatisticsCache.clear();
		}
		
		//	report
		return deleteDocCount;
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
					final Indexer indexer = this.indexers[i];
					this.enqueueIndexerAction(new Runnable() {
						public void run() {
							indexer.deleteDocument(docNr);
						}
					});
				}
				
				try {
					this.dst.deleteDocument(docId);
					this.dstLastModified = System.currentTimeMillis();
				}
				catch (IOException ioe) {
					this.logError("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
					this.logError(ioe);
				}
				
				//	issue event
				GoldenGateServerEventService.notify(new SrsDocumentEvent(userName, docId, this.getClass().getName(), System.currentTimeMillis(), logger));
				
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
	
	
	/**
	 * Background service thread for asynchronous indexing
	 * 
	 * @author sautter
	 */
	private class IndexerServiceThread extends Thread {
		private boolean keepRunning = true;
		public void run() {
			
			//	wake up creator thread
			synchronized (indexerActionQueue) {
				indexerActionQueue.notify();
			}
			
			//	run until shutdown() is called
			while (this.keepRunning) {
				
				//	check if indexer action waiting
				Runnable indexerAction;
				synchronized (indexerActionQueue) {
					
					//	wait if no indexing actions pending
					if (indexerActionQueue.isEmpty()) {
						try {
							indexerActionQueue.wait();
						} catch (InterruptedException ie) {}
					}
					
					//	woken up despite empty queue ==> shutdown
					if (indexerActionQueue.isEmpty()) return;
					
					//	get update
					else indexerAction = ((Runnable) indexerActionQueue.removeFirst());
				}
				
				//	execute index action
				this.doIndexerAction(indexerAction);
				
				//	give a little time to the others
				if (this.keepRunning)
					Thread.yield();
			}
			
			//	work off remaining index actions
			while (indexerActionQueue.size() != 0)
				this.doIndexerAction(((Runnable) indexerActionQueue.removeFirst()));
		}
		
		private void doIndexerAction(Runnable indexerAction) {
			try {
				indexerAction.run();
			}
			catch (Throwable t) {
				logError("Error on index update - " + t.getClass().getName() + " (" + t.getMessage() + ")");
				logError(t);
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
	
	private LinkedList indexerActionQueue = new LinkedList();
	
	private void enqueueIndexerAction(Runnable indexerAction) {
		synchronized (this.indexerActionQueue) {
			this.indexerActionQueue.addLast(indexerAction);
			this.indexerActionQueue.notify();
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
	 * Retrieve a list of the documents hosted in GoldenGATE SRS for retrieval.
	 * @param masterDocID the ID of the parent document to list the retrievable
	 *            parts of (specifying null will return the list of master
	 *            documents)
	 * @return the list of the documents in the SRS' collection
	 * @throws IOException
	 */
	public DocumentList getDocumentList(String masterDocID) throws IOException {
		final DocumentList docList;
		
		//	request for master list
		if (masterDocID == null) {
			
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
			query.append(" ORDER BY " + CHECKIN_TIME_ATTRIBUTE + ";");
			
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
			masterDocID = normalizeId(masterDocID);
			
			StringBuffer query = new StringBuffer("SELECT ");
			for (int f = 0; f < documentListQueryFields.length; f++) {
				if (f != 0)
					query.append(", ");
				query.append(documentListQueryFields[f]);
			}
			query.append(" FROM " + DOCUMENT_TABLE_NAME);
			query.append(" WHERE " + MASTER_DOCUMENT_ID_HASH_COLUMN_NAME + " = " + masterDocID.hashCode() + "");
			query.append(" AND " + MASTER_DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterDocID) + "'");
			query.append(" ORDER BY " + DOC_NUMBER_COLUMN_NAME + ";");
			
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
		HashSet extensionIndexNames = new HashSet();
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
		
		//	get extension indexers
		ArrayList extensionIndexerList = new ArrayList();
		for (int i = 0; i < this.indexers.length; i++)
			if (extensionIndexNames.contains(this.indexers[i].getIndexName()))
				extensionIndexerList.add(this.indexers[i]);
		final Indexer[] extensionIndexers = ((Indexer[]) extensionIndexerList.toArray(new Indexer[extensionIndexerList.size()]));
		final Query extensionIndexerDummy = new Query();
		
		//	wrap document list to add extension fields
		return new DocumentList(docListFields.toStringArray()) {
			private LinkedList dleBuffer = new LinkedList();
			private int dleBufferSize = 100;
			
			public boolean hasNextElement() {
				this.checkDleBuffer();
				return (this.dleBuffer.size() != 0);
			}
			public SrsSearchResultElement getNextElement() {
				return ((this.hasNextElement()) ? ((DocumentListElement) this.dleBuffer.removeFirst()) : null);
			}
			
			private void checkDleBuffer() {
				if (this.dleBuffer.size() <= (this.dleBufferSize / 10))
					this.fillDleBuffer(this.dleBufferSize - this.dleBuffer.size());
			}
			private void addToDleBuffer(DocumentListElement dle) {
				this.dleBuffer.addLast(dle);
			}
			
			private void fillDleBuffer(int dleCount) {
				
				//	get main result elements for next chunk
				ArrayList dleList = new ArrayList();
				while ((dleList.size() < dleCount) && docList.hasNextElement())
					dleList.add(docList.getNextDocumentListElement());
				
				//	collect document numbers and index list entries
				long[] dleDocNrs = new long[dleList.size()];
				HashMap dlesByDocNr = new HashMap();
				for (int d = 0; d < dleList.size(); d++) {
					DocumentListElement dle = ((DocumentListElement) dleList.get(d));
					Long docNr = new Long((String) dle.getAttribute(DOC_NUMBER_COLUMN_NAME, "-1"));
					dleDocNrs[d] = docNr.longValue();
					dlesByDocNr.put(docNr, dle);
				}
				
				//	go indexer by indexer
				for (int e = 0; e < extensionIndexers.length; e++) {
					String extensionIndexName = extensionIndexers[e].getIndexName();
					HashSet deDuplicator = new HashSet();
					
					//	let indexer process query
					IndexResult extensionIndexResult = extensionIndexers[e].getIndexEntries(extensionIndexerDummy, dleDocNrs, false);
					
					//	got a sub result
					if (extensionIndexResult != null) {
						
						//	bucketize index entries, eliminating duplicates along the way
						while (extensionIndexResult.hasNextElement()) {
							IndexResultElement extensionIre = extensionIndexResult.getNextIndexResultElement();
							Long dleKey = new Long(extensionIre.docNr);
							if (deDuplicator.add(dleKey)) {
								DocumentListElement dle = ((DocumentListElement) dlesByDocNr.get(dleKey));
								String[] extensionIreAttributeNames = extensionIre.getAttributeNames();
								for (int a = 0; a < extensionIreAttributeNames.length; a++) {
									String extensionFieldName = ((String) extensionFields.get(extensionIndexName + "." + extensionIreAttributeNames[a]));
									if (extensionFieldName != null)
										dle.setAttribute(extensionFieldName, extensionIre.getAttribute(extensionIreAttributeNames[a]));
								}
							}
						}
					}
				}
				
				//	clean up
				dlesByDocNr.clear();
				
				//	store elements
				for (int d = 0; d < dleList.size(); d++)
					this.addToDleBuffer((DocumentListElement) dleList.get(d));
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
		QueryResult relevanceResult = this.doRelevanceElimination(docNrResult, minRelevance, queryResultPivotIndex);
		
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
		
		//	return result fetching document IDs on the fly (block wise, though, to reduce number of DB queries)
		final DocumentResult dataDr = new SqlDocumentResult(documentSearchResultAttributes, this.io, relevanceResult, 100) {
			private HashSet docNumberDeDuplicator = new HashSet();
			protected void fillDreBuffer(int dreCount) {
				
				//	get query result elements for current block
				StringVector docNumberCollector = new StringVector();
				ArrayList qreList = new ArrayList();
				QueryResultElement qre;
				while ((qreList.size() < dreCount) && ((qre = this.getNextQre()) != null)) {
					if (this.docNumberDeDuplicator.add(new Long(qre.docNr))) {
						docNumberCollector.addElement("" + qre.docNr);
						qreList.add(qre);
					}
				}
				if (docNumberCollector.isEmpty())
					return;
				
				//	obtain document IDs and other data
				StringBuffer docNrToDocDataQuery = new StringBuffer("SELECT " + DOC_NUMBER_COLUMN_NAME);
				for (int c = 0; c < this.resultAttributes.length; c++) {
					docNrToDocDataQuery.append(", ");
					docNrToDocDataQuery.append(this.resultAttributes[c]);
				}
				docNrToDocDataQuery.append(" FROM " + DOCUMENT_TABLE_NAME);
				docNrToDocDataQuery.append(" WHERE " + DOC_NUMBER_COLUMN_NAME + " IN (" + docNumberCollector.concatStrings(", ") + ")");
				docNrToDocDataQuery.append(";");
				
				//	execute query
				HashMap docNrsToDocData = new HashMap();
				SqlQueryResult sqr = null;
				try {
					sqr = this.io.executeSelectQuery(docNrToDocDataQuery.toString());
					
					//	read & store data
					while (sqr.next()) {
						String docNr = sqr.getString(0);
						String[] docData = new String[this.resultAttributes.length];
						for (int d = 0; d < this.resultAttributes.length; d++) {
							if (DOCUMENT_UUID_ATTRIBUTE.equals(this.resultAttributes[d]) && (sqr.getString(d+1).length() >= 32)) {
								String docUuid = sqr.getString(d+1);
								docData[d] = (docUuid.substring(0, 8) + "-" + docUuid.substring(8, 12) + "-" + docUuid.substring(12, 16) + "-" + docUuid.substring(16, 20) + "-" + docUuid.substring(20));							}
							else docData[d] = sqr.getString(d+1);
						}
						docNrsToDocData.put(docNr, docData);
					}
				}
				catch (SQLException sqle) {
					logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document data.");
					logError("  query was " + docNrToDocDataQuery.toString());
				}
				finally {
					if (sqr != null)
						sqr.close();
				}
				
				//	generate DREs from QREs and stored data
				for (int q = 0; q < qreList.size(); q++) {
					qre = ((QueryResultElement) qreList.get(q));
					String[] docData = ((String[]) docNrsToDocData.remove("" + qre.docNr));
					if (docData != null) {
						DocumentResultElement dre = new DocumentResultElement(qre.docNr, docData[0], qre.relevance, null);
						for (int a = 0; a < this.resultAttributes.length; a++) {
							if ((docData[a] != null) && (docData[a].trim().length() != 0))
								dre.setAttribute(this.resultAttributes[a], docData[a]);
						}
						this.addToDreBuffer(dre);
					}
				}
			}
		};
		return new DocumentResult() {
			public boolean hasNextElement() {
				return dataDr.hasNextElement();
			}
			public SrsSearchResultElement getNextElement() {
				DocumentResultElement dre = dataDr.getNextDocumentResultElement();
				
				//	load actual document
				DocumentRoot doc;
				try {
					doc = dst.loadDocument(dre.documentId, docVersion, includeUpdateHistory);
				}
				catch (IOException ioe) {
					logError("GoldenGateSRS: Error loading document '" + dre.documentId + "' (" + ioe.getMessage() + ")");
					logError(ioe);
					doc = Gamta.newDocument(Gamta.newTokenSequence(ioe.getMessage(), null));
				}
				
				//	mark searchables if required
				if (markSearcheables)
					for (int i = 0; i < indexers.length; i++)
						indexers[i].markSearchables(doc);
				
				//	wrap new document result element around document
				DocumentResultElement docDre = new DocumentResultElement(dre.docNr, dre.documentId, dre.relevance, doc);
				docDre.copyAttributes(dre);
				return docDre;
			}
		};
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
								if (size != 0) essentialDoc.addChar(' ');
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
		StringVector subIndexNames = new StringVector();
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
			if (minRelevance > 1.0) minRelevance = 0;
			else if (minRelevance < 0) minRelevance = 0;
			
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
			else docNrResult = relevanceResult;
		}
		
		//	add search attributes to index entries?
		final boolean markSearcheables = (query.getValue(MARK_SEARCHABLES_PARAMETER) != null);
		
		//	get sub indexers
		ArrayList subIndexerList = new ArrayList();
		for (int i = 0; i < indexers.length; i++) {
			String subIndexName = indexers[i].getIndexName();
			
			//	are we interested in this index?
			if (subIndexNames.contains(subIndexName)) {
				subIndexerList.add(this.indexers[i]);
				this.logActivity("  - got sub indexer: " + subIndexName);
			}
		}
		final Indexer[] subIndexers = ((Indexer[]) subIndexerList.toArray(new Indexer[subIndexerList.size()]));
		
		// add sub results to main result elements block-wise and buffer main
		// result elements (reduces number of database queries for getting sub
		// results)
		if (rankedResult) {
			
			//	read additional parameters
			int qrpi = docNrResult.size();
			if (subResultMinSize == 0) try {
				qrpi = this.resultPivotIndex;
				qrpi = Integer.parseInt(query.getValue(RESULT_PIVOT_INDEX_PARAMETER, ("" + qrpi)));
			} catch (NumberFormatException nfe) {}
			final int queryResultPivotIndex = qrpi;
			
			// compute buffer size (if relevance elimination is over (no sub
			// results), use large buffer, otherwise, use result pivot as buffer
			// size (might cause a second lookup, but this is OK))
			int bufferSize = ((subResultMinSize == 0) ? Math.min(docNrResult.size(), 100) : queryResultPivotIndex);
			
			//	return result
			return new SqlDocumentResult(documentDataSearchResultAttributes, this.io, docNrResult, bufferSize) {
				private int loaded = 0;
				private int returned = 0;
				private double lastRelevance = 0;
				private DocumentResultElement nextDre = null;
				private void ensureNextElement() {
					if (this.nextDre != null)
						return;
					
					if (super.hasNextElement()) {
						DocumentResultElement dre = ((DocumentResultElement) super.getNextElement());
						
						//	omit all documents less relevant than the pivot document
						if ((this.returned < queryResultPivotIndex) || (dre.relevance >= this.lastRelevance)) {
							
							//	remember last relevance for delaying cutoff
							this.lastRelevance = dre.relevance;
							this.returned++;
							
							//	store result element
							this.nextDre = dre;
						}
					}
				}
				
				public boolean hasNextElement() {
					if (subResultMinSize == 0)
						return super.hasNextElement();
					this.ensureNextElement();
					return (this.nextDre != null);
				}
				
				public SrsSearchResultElement getNextElement() {
					if (subResultMinSize == 0)
						return super.getNextElement();
					DocumentResultElement dre = (this.hasNextElement() ? this.nextDre : null);
					this.nextDre = null;
					return dre;
				}
				
				protected void fillDreBuffer(int dreCount) {
					logActivity("SqlDocumentResult: filling buffer with at most " + dreCount + " elements, returned " + this.returned + " thus far.");
					int dreAdded = 0;
					
					//	get query result elements for next chunk
					ArrayList qreList = new ArrayList();
					while ((qreList.size() < dreCount) && this.hasNextQre())
						qreList.add(this.getNextQre());
					
					//	get document numbers
					StringVector docNumberCollector = new StringVector();
					long[] docNumbers = new long[qreList.size()];
					for (int r = 0; r < qreList.size(); r++) {
						QueryResultElement qre = ((QueryResultElement) qreList.get(r));
						docNumberCollector.addElement("" + qre.docNr);
						docNumbers[r] = qre.docNr;
					}
					if (docNumberCollector.isEmpty()) {
						logActivity(" - no further documents");
						return;
					}
					logActivity(" - got " + docNumberCollector.size() + " further document numbers");
					
					//	get document data
					StringBuffer docNrToDocDataQuery = new StringBuffer("SELECT " + DOC_NUMBER_COLUMN_NAME);
					for (int c = 0; c < this.resultAttributes.length; c++) {
						docNrToDocDataQuery.append(", ");
						docNrToDocDataQuery.append(this.resultAttributes[c]);
					}
					docNrToDocDataQuery.append(" FROM " + DOCUMENT_TABLE_NAME);
					docNrToDocDataQuery.append(" WHERE " + DOC_NUMBER_COLUMN_NAME + " IN (" + docNumberCollector.concatStrings(", ") + ")");
					docNrToDocDataQuery.append(";");
					
					//	do database lookup
					HashMap docNrsToDocData = new HashMap();
					SqlQueryResult sqr = null;
					try {
						logActivity("  - NR to doc data query is " + docNrToDocDataQuery.toString());
						sqr = io.executeSelectQuery(docNrToDocDataQuery.toString());
						
						//	read & store data
						while (sqr.next()) {
							String docNr = sqr.getString(0);
							String[] docData = new String[this.resultAttributes.length];
							for (int d = 0; d < this.resultAttributes.length; d++) {
								if (DOCUMENT_UUID_ATTRIBUTE.equals(this.resultAttributes[d]) && (sqr.getString(d+1).length() >= 32)) {
									String docUuid = sqr.getString(d+1);
									docData[d] = (docUuid.substring(0, 8) + "-" + docUuid.substring(8, 12) + "-" + docUuid.substring(12, 16) + "-" + docUuid.substring(16, 20) + "-" + docUuid.substring(20));							}
								else docData[d] = sqr.getString(d+1);
							}
							docNrsToDocData.put(docNr, docData);
							this.loaded++;
						}
					}
					catch (SQLException sqle) {
						logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document data.");
						logError("  query was " + docNrToDocDataQuery.toString());
					}
					finally {
						if (sqr != null)
							sqr.close();
					}
					logActivity("  - got " + docNrsToDocData.size() + " doc data sets, for total of " + this.loaded);
					
					//	collect sub results
					final ArrayList subResultNames = new ArrayList();
					final HashMap subResultLabels = new HashMap();
					final HashMap subResultAttributes = new HashMap();
					final HashMap subResultBuckets = new HashMap();
					
					//	go indexer by indexer
					for (int s = 0; s < subIndexers.length; s++) {
						String subIndexName = subIndexers[s].getIndexName();
						
						//	let indexer process query
						query.setIndexNameMask(subIndexName); // TODO if more than one indexer is concerned in search, allow specifying this, so fuzzy matching results get their entries as well
						final IndexResult subIndexResult = subIndexers[s].getIndexEntries(query, docNumbers, false);
						query.setIndexNameMask(null);
						
						//	got a sub result
						if (subIndexResult != null) {
							logActivity("  - got sub result from " + subIndexName);
							
							//	store index meta data
							subResultNames.add(subIndexResult.indexName);
							subResultLabels.put(subIndexResult.indexName, subIndexResult.indexLabel);
							subResultAttributes.put(subIndexResult.indexName, subIndexResult.resultAttributes);
							
							//	bucketize index entries, eliminating duplicates along the way
							while (subIndexResult.hasNextElement()) {
								IndexResultElement subIre = subIndexResult.getNextIndexResultElement();
								if (markSearcheables)
									subIndexers[s].addSearchAttributes(subIre);
								String subIreKey = (subIre.docNr + "." + subIndexResult.indexName);
								TreeSet subIreSet = ((TreeSet) subResultBuckets.get(subIreKey));
								if (subIreSet == null) {
									subIreSet = new TreeSet(subIndexResult.getSortOrder());
									subResultBuckets.put(subIreKey, subIreSet);
								}
								if (!subIreSet.contains(subIre)) // do not replace any elements (first one wins)
									subIreSet.add(subIre);
							}
						}
					}
					
					//	generate DREs from QREs and stored data
					for (int i = 0; i < qreList.size(); i++) {
						QueryResultElement qre = ((QueryResultElement) qreList.get(i));
						logActivity("  - adding result document " + qre.docNr + " (" + qre.relevance + ")");
						
						String[] docData = ((String[]) docNrsToDocData.remove("" + qre.docNr));
						if (docData != null) {
							DocumentResultElement dre = new DocumentResultElement(qre.docNr, docData[0], qre.relevance, null);
							for (int a = 0; a < this.resultAttributes.length; a++) {
								if ((docData[a] != null) && (docData[a].trim().length() != 0))
									dre.setAttribute(this.resultAttributes[a], docData[a]);
							}
							
							//	get sub results
							int subResultSize = 0;
							for (int s = 0; s < subResultNames.size(); s++) {
								String subIndexName = ((String) subResultNames.get(s));
								String subResultKey = (dre.docNr + "." + subIndexName);
								TreeSet subResultData = ((TreeSet) subResultBuckets.get(subResultKey));
								
								//	got sub result for current element
								if (subResultData != null) {
									subResultSize += subResultData.size();
									final ArrayList subResultElements = new ArrayList(subResultData);
									dre.addSubResult(new IndexResult(((String[]) subResultAttributes.get(subIndexName)), subIndexName, ((String) subResultLabels.get(subIndexName))) {
										int sreIndex = 0;
										public boolean hasNextElement() {
											return (this.sreIndex < subResultElements.size());
										}
										public SrsSearchResultElement getNextElement() {
											return ((SrsSearchResultElement) subResultElements.get(this.sreIndex++));
										}
									});	
								}
							}
							
							//	do we have sufficient sub results?
							if (subResultSize >= subResultMinSize) {
								this.addToDreBuffer(dre);
								dreAdded++;
							}
						}
					}
					logActivity("  - added " + dreAdded + " result documents");
					
					//	clean up
					docNrsToDocData.clear();
					subResultNames.clear();
					subResultLabels.clear();
					subResultAttributes.clear();
					subResultBuckets.clear();
					
					//	did we add sufficient new elements to the buffer?
					//	in case that not, are there more elements left in the backing result?
					if (dreAdded < (dreCount / 2) && this.hasNextQre())
						
						//	need more elements, and more available, process another block of elements from backing result
						this.fillDreBuffer(dreCount);
				}
			};
		}
		
		//	unranked (index) result, need to order elements by author name, publication date, and title
		else {
			
			//	get document numbers
			StringVector docNumberCollector = new StringVector();
			for (int r = 0; r < docNrResult.size(); r++)
				docNumberCollector.addElement("" + docNrResult.getResult(r).docNr);
			if (docNumberCollector.isEmpty())return new IndexResult(documentIndexSearchResultAttributes, DocumentRoot.DOCUMENT_TYPE, "Document Index") {
				public boolean hasNextElement() {
					return false;
				}
				public SrsSearchResultElement getNextElement() {
					return null;
				}
			};
			
			//	get document data
			StringBuffer docNrToDocDataQuery = new StringBuffer("SELECT " + DOC_NUMBER_COLUMN_NAME);
			for (int c = 0; c < documentIndexSearchResultAttributes.length; c++) {
				docNrToDocDataQuery.append(", ");
				docNrToDocDataQuery.append(documentIndexSearchResultAttributes[c]);
			}
			docNrToDocDataQuery.append(" FROM " + DOCUMENT_TABLE_NAME);
			docNrToDocDataQuery.append(" WHERE " + DOC_NUMBER_COLUMN_NAME + " IN (" + docNumberCollector.concatStrings(", ") + ")");
			docNrToDocDataQuery.append(" ORDER BY ");
			for (int c = 0; c < documentIndexSearchResultAttributes.length; c++) {
				if (c != 0) docNrToDocDataQuery.append(", ");
				docNrToDocDataQuery.append(documentIndexSearchResultAttributes[c]);
			}
			docNrToDocDataQuery.append(";");
			
			//	do database lookup
			SqlQueryResult sqr = null;
			try {
				sqr = io.executeSelectQuery(docNrToDocDataQuery.toString());
				final SqlResultDataSource sds = new SqlResultDataSource(sqr);
				
				//	build basic index result for document data
				IndexResult baseResult = new IndexResult(documentIndexSearchResultAttributes, DocumentRoot.DOCUMENT_TYPE, "Document Index") {
					public boolean hasNextElement() {
						return sds.hasNextElement();
					}
					public SrsSearchResultElement getNextElement() {
						String[] elementData = sds.getNextElementData();
						if (elementData == null) return null;
						
						//	order data
						Attributed data = new AbstractAttributed();
						for (int a = 0; a < this.resultAttributes.length; a++) {
							if (elementData[a+1] != null)
								data.setAttribute(this.resultAttributes[a], elementData[a+1]);
						}
						
						//	build title
						StringBuffer value = new StringBuffer();
						value.append(data.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, "Unknown Author").toString());
						value.append(". ");
						value.append(data.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "Unknown Year").toString());
						value.append(". ");
						value.append(data.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, "Unknown Title").toString());
						
						//	create result element
						IndexResultElement ire = new IndexResultElement(Long.parseLong(elementData[0]), DocumentRoot.DOCUMENT_TYPE, value.toString());
						ire.copyAttributes(data);
						return ire;
					}
				};
				
				//	no sub results required, we're done
				if (subIndexers.length == 0) return baseResult;
				
				//	wrap result in buffer for adding sub results chunk-wise
				return new BufferedIndexResult(baseResult, 100) {
					protected void fillIreBuffer(int ireCount) {
						logActivity("GgSRS: not finished index search after " + (System.currentTimeMillis() - start) + "ms\r\n        query: " + query);
						int ireAdded = 0;
						
						//	get main result elements for next chunk
						ArrayList ireList = new ArrayList();
						while ((ireList.size() < ireCount) && this.data.hasNextElement())
							ireList.add(this.data.getNextIndexResultElement());
						
						//	get document numbers
						long[] ireDocNrs = new long[ireList.size()];
						for (int i = 0; i < ireList.size(); i++)
							ireDocNrs[i] = ((IndexResultElement) ireList.get(i)).docNr;
						
						//	collect sub results
						final ArrayList subResultNames = new ArrayList();
						final HashMap subResultLabels = new HashMap();
						final HashMap subResultAttributes = new HashMap();
						final HashMap subResultBuckets = new HashMap();
						
						//	go indexer by indexer
						for (int s = 0; s < subIndexers.length; s++) {
							String subIndexName = subIndexers[s].getIndexName();
							
							//	let indexer process query
							query.setIndexNameMask(subIndexName);
							final IndexResult subIndexResult = subIndexers[s].getIndexEntries(query, ireDocNrs, true);
							query.setIndexNameMask(null);
							
							//	got a sub result
							if (subIndexResult != null) {
								logActivity("  - got sub result from " + subIndexName);
								
								//	store index meta data
								subResultNames.add(subIndexResult.indexName);
								subResultLabels.put(subIndexResult.indexName, subIndexResult.indexLabel);
								subResultAttributes.put(subIndexResult.indexName, subIndexResult.resultAttributes);
								
								//	bucketize index entries, eliminating duplicates along the way
								while (subIndexResult.hasNextElement()) {
									IndexResultElement subIre = subIndexResult.getNextIndexResultElement();
									if (markSearcheables)
										subIndexers[s].addSearchAttributes(subIre);
									String subIreKey = (subIre.docNr + "." + subIndexResult.indexName);
									TreeSet subIreSet = ((TreeSet) subResultBuckets.get(subIreKey));
									if (subIreSet == null) {
										subIreSet = new TreeSet(subIndexResult.getSortOrder());
										subResultBuckets.put(subIreKey, subIreSet);
									}
									if (!subIreSet.contains(subIre)) // do not replace any elements (first one wins)
										subIreSet.add(subIre);
								}
							}
						}
						
						//	join elements with sub results and store them
						for (int i = 0; i < ireList.size(); i++) {
							IndexResultElement ire = ((IndexResultElement) ireList.get(i));
							
							//	get sub results
							int subResultSize = 0;
							for (int s = 0; s < subResultNames.size(); s++) {
								String subIndexName = ((String) subResultNames.get(s));
								String subResultKey = (ire.docNr + "." + subIndexName);
								TreeSet subResultData = ((TreeSet) subResultBuckets.get(subResultKey));
								
								//	got sub result for current element
								if (subResultData != null) {
									subResultSize += subResultData.size();
									final ArrayList subResultElements = new ArrayList(subResultData);
									ire.addSubResult(new IndexResult(((String[]) subResultAttributes.get(subIndexName)), subIndexName, ((String) subResultLabels.get(subIndexName))) {
										int sreIndex = 0;
										public boolean hasNextElement() {
											return (this.sreIndex < subResultElements.size());
										}
										public SrsSearchResultElement getNextElement() {
											return ((SrsSearchResultElement) subResultElements.get(this.sreIndex++));
										}
									});	
								}
							}
							
							//	do we have sufficient sub results?
							if (subResultSize >= subResultMinSize) {
								this.addToIreBuffer(ire);
								ireAdded++;
							}
						}
						
						//	clean up
						subResultNames.clear();
						subResultLabels.clear();
						subResultAttributes.clear();
						subResultBuckets.clear();
						
						//	did we add sufficient new elements to the buffer?
						//	in case that not, are there more elements left in the backing result?
						if (ireAdded < (ireCount / 2) && this.data.hasNextElement())
							
							//	need more elements, and more available, process another block of elements from backing result
							this.fillIreBuffer(ireCount);
					}
				};
			}
			catch (SQLException sqle) {
				this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document data.");
				this.logError("  query was " + docNrToDocDataQuery.toString());
				throw new IOException(sqle.getMessage());
			}
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
		QueryResult relevanceResult = this.doRelevanceElimination(docNrResult, minRelevance, queryResultPivotIndex);
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
		
		//	return result fetching document IDs on the fly (block wise, though, to reduce number of DB queries)
		else return new SqlDocumentResult(documentIdSearchResultAttributes, this.io, relevanceResult, 100) {
			private HashSet docNumberDeDuplicator = new HashSet();
			protected void fillDreBuffer(int dreCount) {
				
				//	get query result elements for current block
				StringVector docNumberCollector = new StringVector();
				ArrayList qreList = new ArrayList();
				QueryResultElement qre;
				while ((qreList.size() < dreCount) && ((qre = this.getNextQre()) != null)) {
					if (this.docNumberDeDuplicator.add(new Long(qre.docNr))) {
						docNumberCollector.addElement("" + qre.docNr);
						qreList.add(qre);
					}
				}
				if (docNumberCollector.isEmpty())
					return;
				
				//	get IDs for current document numbers
				HashMap docNrsToDocIDs = new HashMap();
				String docNrsToDocIDsQuery = "SELECT " + Indexer.DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + 
					" FROM " + DOCUMENT_TABLE_NAME + 
					" WHERE " + Indexer.DOC_NUMBER_COLUMN_NAME + " IN (" + docNumberCollector.concatStrings(", ") + ");";
				SqlQueryResult sqr = null;
				try {
					sqr = io.executeSelectQuery(docNrsToDocIDsQuery);
					
					//	read & store data
					while (sqr.next()) {
						String docNr = sqr.getString(0);
						String docId = sqr.getString(1);
						docNrsToDocIDs.put(docNr, docId);
					}
				}
				catch (SQLException sqle) {
					logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document IDs.");
					logError("Query was " + docNrsToDocIDsQuery);
				}
				finally {
					if (sqr != null)
						sqr.close();
				}
				
				//	generate DREs from QREs and stored data
				for (int q = 0; q < qreList.size(); q++) {
					qre = ((QueryResultElement) qreList.get(q));
					String docId = ((String) docNrsToDocIDs.remove("" + qre.docNr));
					if (docId != null)
						this.addToDreBuffer(new DocumentResultElement(qre.docNr, docId, qre.relevance, null));
				}
			}
		};
	}
	
	private abstract static class SqlDocumentResult extends DocumentResult {
		protected IoProvider io;
		
		private QueryResult docNrResult;
		private int docNrResultIndex = 0;
		
		private LinkedList dreBuffer = new LinkedList();
		private int dreBufferSize;
		
		protected SqlDocumentResult(String[] resultAttributes, IoProvider io, QueryResult docNrResult, int bufferSize) {
			super(resultAttributes);
			this.io = io;
			this.docNrResult = docNrResult;
			this.dreBufferSize = bufferSize;
		}
		
		public boolean hasNextElement() {
			this.fillDreBuffer();
			return (this.dreBuffer.size() != 0);
		}
		public SrsSearchResultElement getNextElement() {
			return ((this.hasNextElement()) ? ((DocumentResultElement) this.dreBuffer.removeFirst()) : null);
		}
		
		protected boolean hasNextQre() {
			return (this.docNrResultIndex < this.docNrResult.size());
		}
		protected QueryResultElement getNextQre() {
			return (this.hasNextQre() ? this.docNrResult.getResult(this.docNrResultIndex++) : null);
		}
		
		protected void fillDreBuffer() {
			if (this.dreBuffer.size() <= (this.dreBufferSize / 10))
				this.fillDreBuffer(this.dreBufferSize - this.dreBuffer.size());
		}
		protected void addToDreBuffer(DocumentResultElement dre) {
			this.dreBuffer.addLast(dre);
		}
		protected abstract void fillDreBuffer(int dreCount);
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
//			
//			String docIdToDocNrQuery = "SELECT " + DOC_NUMBER_COLUMN_NAME + " FROM " + DOCUMENT_TABLE_NAME + " WHERE " + DOCUMENT_ID_ATTRIBUTE + " IN ('" + fixedResultDocIDs.concatStrings("', '") + "') OR " + DOCUMENT_UUID_ATTRIBUTE + " IN ('" + fixedResultDocIDs.concatStrings("', '") + "') OR " + MASTER_DOCUMENT_ID_ATTRIBUTE + " IN ('" + fixedResultDocIDs.concatStrings("', '") + "');";
//			SqlQueryResult sqr = null;
//			try {
//				sqr = this.io.executeSelectQuery(docIdToDocNrQuery);
//				while (sqr.next())
//					result.addResultElement(new QueryResultElement(sqr.getLong(0), 1));
//			}
//			catch (SQLException sqle) {
//				System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document IDs.");
//				System.out.println("  Query was " + docIdToDocNrQuery);
//			}
//			finally {
//				if (sqr != null)
//					sqr.close();
//			}
			
			//	do document ID lookup (even though we theoretically have a document number, we still have to verify it's valid)
			if (result.size() == 0)
				result = this.doDocIdQuery(fixedResultDocIDs, DOCUMENT_ID_ATTRIBUTE, null);
			
			//	do document UUID lookup
			if (result.size() == 0)
				result = this.doDocIdQuery(fixedResultDocIDs, DOCUMENT_UUID_ATTRIBUTE, DOCUMENT_UUID_HASH_COLUMN_NAME);
			
			//	do master document ID lookup
			if (result.size() == 0)
				result = this.doDocIdQuery(fixedResultDocIDs, MASTER_DOCUMENT_ID_ATTRIBUTE, MASTER_DOCUMENT_ID_HASH_COLUMN_NAME);
			
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
	
	private QueryResult doDocIdQuery(StringVector fixedResultDocIDs, String idFieldName, String idHashFieldName) {
		QueryResult result = new QueryResult();
		if (fixedResultDocIDs.isEmpty())
			return result;
		
		//	assemble query
		StringBuffer docIdToDocNrQuery = new StringBuffer("SELECT " + DOC_NUMBER_COLUMN_NAME);
		docIdToDocNrQuery.append(" FROM " + DOCUMENT_TABLE_NAME);
		if (fixedResultDocIDs.size() == 1) {
			docIdToDocNrQuery.append(" WHERE " + idFieldName + " = '" + EasyIO.sqlEscape(fixedResultDocIDs.get(0)) + "'");
			if (idHashFieldName == null)
				docIdToDocNrQuery.append(" AND " + DOC_NUMBER_COLUMN_NAME + " = " + getDocNr(fixedResultDocIDs.get(0)));
			else docIdToDocNrQuery.append(" AND " + idHashFieldName + " = " + fixedResultDocIDs.get(0).hashCode());
		}
		else {
			docIdToDocNrQuery.append(" WHERE " + idFieldName + " IN (");
			for (int i = 0; i < fixedResultDocIDs.size(); i++)
				docIdToDocNrQuery.append(((i == 0) ? "" : ", ") + "'" + EasyIO.sqlEscape(fixedResultDocIDs.get(i)) + "'");
			docIdToDocNrQuery.append(")");
			if (idHashFieldName == null) {
				docIdToDocNrQuery.append(" AND " + DOC_NUMBER_COLUMN_NAME + " IN (");
				for (int i = 0; i < fixedResultDocIDs.size(); i++)
					docIdToDocNrQuery.append(((i == 0) ? "" : ", ") + getDocNr(fixedResultDocIDs.get(i)));
			}
			else {
				docIdToDocNrQuery.append(" AND " + idHashFieldName + " IN (");
				for (int i = 0; i < fixedResultDocIDs.size(); i++)
					docIdToDocNrQuery.append(((i == 0) ? "" : ", ") + fixedResultDocIDs.get(i).hashCode());
			}
			docIdToDocNrQuery.append(")");
		}
		docIdToDocNrQuery.append(";");
		
		//	do lookup
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(docIdToDocNrQuery.toString());
			while (sqr.next())
				result.addResultElement(new QueryResultElement(sqr.getLong(0), 1));
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document IDs.");
			this.logError("  Query was " + docIdToDocNrQuery);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	return result
		this.logActivity("  - got " + result.size() + " document numbers in result for " + idFieldName);
		return result;
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
		
		//	index found, keep it
		final Indexer indexer = mainIndexer;
		
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
			String[] emptyFields = {indexer.getIndexName()};
			return new IndexResult(emptyFields, indexer.getIndexName(), indexer.getIndexName()) {
				public boolean hasNextElement() {
					return false;
				}
				public SrsSearchResultElement getNextElement() {
					return null;
				}
			};
		}
		
		//	get index entries
		long[] docNumbers = new long[qr.size()];
		for (int d = 0; d < docNumbers.length; d++)
			docNumbers[d] = qr.getResult(d).docNr;
		
		query.setIndexNameMask(indexer.getIndexName());
		IndexResult indexResult = indexer.getIndexEntries(query, docNumbers, true);
		query.setIndexNameMask(null);
		
		//	query failed
		if (indexResult == null)
			throw new IOException("No such index.");
		
		//	get sub indexers
		ArrayList subIndexerList = new ArrayList();
		for (int i = 0; i < indexers.length; i++) {
			String subIndexName = indexers[i].getIndexName();
			
			//	are we interested in this index?
			if (subIndexNames.contains(subIndexName)) {
				subIndexerList.add(this.indexers[i]);
				this.logActivity("  - got sub indexer: " + subIndexName);
			}
		}
		final Indexer[] subIndexers = ((Indexer[]) subIndexerList.toArray(new Indexer[subIndexerList.size()]));
		
		//	no sub results, only need deduplication
		if (subIndexers.length == 0)
			return new BufferedIndexResult(indexResult, 25) {
				private Set ireDeDuplicator = new HashSet();
				protected void fillIreBuffer(int ireCount) {
					while ((ireCount > 0) && this.data.hasNextElement()) {
						IndexResultElement ire = this.data.getNextIndexResultElement();
						if (this.ireDeDuplicator.add(ire.getSortString(this.resultAttributes))) {
							if (markSearcheables)
								indexer.addSearchAttributes(ire);
							this.addToIreBuffer(ire);
							ireCount--;
						}
					}
				}
			};
		
		//	de-duplicate index entries, collecting their document numbers along the way
		final LinkedList distinctIreList = new LinkedList();
		final HashMap ireDocNrs = new HashMap();
		while (indexResult.hasNextElement()) {
			IndexResultElement ire = indexResult.getNextIndexResultElement();
			String ireSortString = ire.getSortString(indexResult.resultAttributes);
			Set ireDocNrSet = ((Set) ireDocNrs.get(ireSortString));
			if (ireDocNrSet == null) {
				distinctIreList.add(ire);
				ireDocNrSet = new TreeSet();
				ireDocNrs.put(ireSortString, ireDocNrSet);
			}
			ireDocNrSet.add(new Long(ire.docNr));
		}
		this.logActivity("  - got " + distinctIreList.size() + " distinct index entries");
		
		//	create duplicate-free index result
		indexResult = new IndexResult(indexResult.resultAttributes, indexResult.indexName, indexResult.indexLabel) {
			public boolean hasNextElement() {
				return (distinctIreList.size() != 0);
			}
			public SrsSearchResultElement getNextElement() {
				return (distinctIreList.isEmpty() ? null : ((IndexResultElement) distinctIreList.removeFirst()));
			}
		};
		
		//	fetch sub result entries en block
		return new BufferedIndexResult(indexResult, 100) {
			protected void fillIreBuffer(int ireCount) {
				int ireAdded = 0;
				
				//	get main result elements for next chunk
				ArrayList ireBlock = new ArrayList();
				Set ireBlockDocNrSet = new HashSet();
				while ((ireBlock.size() < ireCount) && this.data.hasNextElement()) {
					IndexResultElement ire = this.data.getNextIndexResultElement();
					ireBlock.add(ire);
					ireBlockDocNrSet.addAll((Set) ireDocNrs.get(ire.getSortString(this.resultAttributes)));
				}
				
				//	get document numbers
				long[] ireBlockDocNrs = new long[ireBlockDocNrSet.size()];
				int docNrIndex = 0;
				for (Iterator dnrit = ireBlockDocNrSet.iterator(); dnrit.hasNext();)
					ireBlockDocNrs[docNrIndex++] = ((Long) dnrit.next()).longValue();
				
				//	collect sub results
				final ArrayList subResultNames = new ArrayList();
				final HashMap subResultLabels = new HashMap();
				final HashMap subResultAttributes = new HashMap();
				final HashMap docNrSubIreBuckets = new HashMap();
				
				//	go indexer by indexer
				for (int s = 0; s < subIndexers.length; s++) {
					String subIndexName = subIndexers[s].getIndexName();
					
					//	let indexer process query
					query.setIndexNameMask(subIndexName);
					final IndexResult subIndexResult = subIndexers[s].getIndexEntries(query, ireBlockDocNrs, true);
					query.setIndexNameMask(null);
					
					//	got a sub result
					if (subIndexResult != null) {
						logActivity("  - got sub result from " + subIndexName);
						
						//	store index meta data
						subResultNames.add(subIndexResult.indexName);
						subResultLabels.put(subIndexResult.indexName, subIndexResult.indexLabel);
						subResultAttributes.put(subIndexResult.indexName, subIndexResult.resultAttributes);
						
						//	bucketize index entries
						while (subIndexResult.hasNextElement()) {
							IndexResultElement subIre = subIndexResult.getNextIndexResultElement();
							if (markSearcheables)
								subIndexers[s].addSearchAttributes(subIre);
							String subIreKey = (subIre.docNr + "." + subIndexResult.indexName);
							LinkedList subIreList = ((LinkedList) docNrSubIreBuckets.get(subIreKey));
							if (subIreList == null) {
								subIreList = new LinkedList();
								docNrSubIreBuckets.put(subIreKey, subIreList);
							}
							subIreList.add(subIre);
						}
						logActivity("  - sub result from " + subIndexName + " bucketized");
					}
				}
				
				//	join elments with sub results and store them
				for (int i = 0; i < ireBlock.size(); i++) {
					IndexResultElement ire = ((IndexResultElement) ireBlock.get(i));
					Set ireDocNrSet = ((Set) ireDocNrs.get(ire.getSortString(this.resultAttributes)));
					
					//	get sub results
					int subResultSize = 0;
					for (int s = 0; s < subResultNames.size(); s++) {
						String subIndexName = ((String) subResultNames.get(s));
						String[] subIndexAttributes = ((String[]) subResultAttributes.get(subIndexName));
						final LinkedList subIndexElementList = new LinkedList();
						Set subIndexDeduplicator = new HashSet();
						
						//	aggregate and de-duplicate sub result elements
						for (Iterator dnrit = ireDocNrSet.iterator(); dnrit.hasNext();) {
							LinkedList docNrSubIreList = ((LinkedList) docNrSubIreBuckets.get(((Long) dnrit.next()).longValue() + "." + subIndexName));
							if (docNrSubIreList != null)
								for (Iterator siit = docNrSubIreList.iterator(); siit.hasNext();) {
									IndexResultElement subIre = ((IndexResultElement) siit.next());
									if (subIndexDeduplicator.add(subIre.getSortString(subIndexAttributes)))
										subIndexElementList.add(subIre);
								}
						}
						
						//	got sub result for current element?
						if (subIndexElementList.size() != 0) {
							subResultSize += subIndexElementList.size();
							ire.addSubResult(new IndexResult(subIndexAttributes, subIndexName, ((String) subResultLabels.get(subIndexName))) {
								public boolean hasNextElement() {
									return (subIndexElementList.size() != 0);
								}
								public SrsSearchResultElement getNextElement() {
									return (subIndexElementList.isEmpty() ? null : ((IndexResultElement) subIndexElementList.removeFirst()));
								}
							});	
						}
					}
					
					//	do we have sufficient sub results?
					if (subResultSize >= subResultMinSize) {
						if (markSearcheables)
							indexer.addSearchAttributes(ire);
						this.addToIreBuffer(ire);
						ireAdded++;
					}
				}
				
				//	clean up
				subResultNames.clear();
				subResultLabels.clear();
				subResultAttributes.clear();
				docNrSubIreBuckets.clear();
				
				//	did we add sufficient new elements to the buffer?
				//	in case that not, are there more elements left in the backing result?
				if (ireAdded < (ireCount / 2) && this.data.hasNextElement())
					
					//	need more elements, and more available, process another block of elements from backing result
					this.fillIreBuffer(ireCount);
			}
		};
	}
	
	private abstract static class BufferedIndexResult extends IndexResult {
		
		protected IndexResult data;
		
		private LinkedList ireBuffer = new LinkedList();
		private int ireBufferSize;
		
		protected BufferedIndexResult(IndexResult data, int bufferSize) {
			super(data.resultAttributes, data.indexName, data.indexLabel);
			this.data = data;
			this.ireBufferSize = bufferSize;
		}
		
		public boolean hasNextElement() {
			this.checkIreBuffer();
			return (this.ireBuffer.size() != 0);
		}
		public SrsSearchResultElement getNextElement() {
			return ((this.hasNextElement()) ? ((IndexResultElement) this.ireBuffer.removeFirst()) : null);
		}
		
		private void checkIreBuffer() {
			if (this.ireBuffer.size() <= (this.ireBufferSize / 10))
				this.fillIreBuffer(this.ireBufferSize - this.ireBuffer.size());
		}
		protected void addToIreBuffer(IndexResultElement ire) {
			this.ireBuffer.addLast(ire);
		}
		
		protected abstract void fillIreBuffer(int ireCount);
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
			if (pair.length == 2) {
				String name = pair[0].trim();
				String value = URLDecoder.decode(pair[1].trim(), ENCODING).trim();
				
				String existingValue = query.getValue(name);
				if (existingValue != null)
					value = existingValue + "\n" + value;
				
				query.setValue(name, value);
			}
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
}
