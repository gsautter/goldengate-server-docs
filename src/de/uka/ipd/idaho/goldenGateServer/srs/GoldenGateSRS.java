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


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.easyIO.util.RandomByteSource;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AttributeUtils;
import de.uka.ipd.idaho.gamta.Attributed;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.gamta.defaultImplementation.AbstractAttributed;
import de.uka.ipd.idaho.gamta.util.AnnotationFilter;
import de.uka.ipd.idaho.gamta.util.AnnotationInputStream;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.ComponentInitializer;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerEventService;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants.SrsDocumentEvent.SrsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatisticsElement;
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
	
	private Indexer[] indexers = new Indexer[0];
	private IndexerServiceThread indexerService = null;
	
	private StorageFilter[] filters = new StorageFilter[0];
	private DocumentSplitter[] splitters = new DocumentSplitter[0];
	
	private IoProvider io;
	
	private static int nextDocNr = 0;
	
//	private String maintenance = null;
//	
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
	
	private DocumentStore dst;
	
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
		td.addColumn(DOCUMENT_CHECKSUM_COLUMN_NAME, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(MASTER_DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
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
		this.io.indexColumn(DOCUMENT_TABLE_NAME, MASTER_DOCUMENT_ID_ATTRIBUTE);
		
		//	get next document number
		try {
			SqlQueryResult sqr = null;
			try {
				sqr = this.io.executeSelectQuery(("SELECT max(" + DOC_NUMBER_COLUMN_NAME + ") FROM " + DOCUMENT_TABLE_NAME + ";"), true);
				if (sqr.next()) nextDocNr = Integer.parseInt(sqr.getString(0));
				else nextDocNr = 0;
			}
			catch (NumberFormatException nfe) {
				nextDocNr = 0;
			}
			finally {
				if (sqr != null)
					sqr.close();
			}
		}
		catch (Exception e) {}
		System.out.println("  - database connection established");
		
		
		//	initialize document store
		this.dst = new DocumentStore(new File(this.dataPath, "Documents"), this.configuration.getSetting("DocumentEncoding"));
		System.out.println("  - got document store");
		
		
		//	load, initialize and sort plugins
		File pluginFolder = new File(this.dataPath, "Plugins");
		GoldenGateSrsPlugin[] plugins = this.createPlugins(pluginFolder);
		ArrayList indexers = new ArrayList();
		ArrayList filters = new ArrayList();
		ArrayList splitters = new ArrayList();
		for (int p = 0; p < plugins.length; p++) {
			plugins[p].init();
			if (plugins[p] instanceof Indexer)
				indexers.add((Indexer) plugins[p]);
			if (plugins[p] instanceof StorageFilter)
				filters.add((StorageFilter) plugins[p]);
			if (plugins[p] instanceof DocumentSplitter)
				splitters.add((DocumentSplitter) plugins[p]);
		}
		this.indexers = ((Indexer[]) indexers.toArray(new Indexer[indexers.size()]));
		this.filters = ((StorageFilter[]) filters.toArray(new StorageFilter[filters.size()]));
		this.splitters = ((DocumentSplitter[]) splitters.toArray(new DocumentSplitter[splitters.size()]));
		System.out.println("  - plugins initialized");
		
		//	start indexer service thread
		synchronized (this.indexerActionQueue) {
			this.indexerService = new IndexerServiceThread();
			this.indexerService.start();
			try {
				this.indexerActionQueue.wait();
			} catch (InterruptedException ie) {}
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
		Gamta.setAnnotationNestingOrder(this.configuration.getSetting(ANNOTATION_NESTING_ORDER_SETTING));
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
				else return ("Specify no arguments or a single comma separated list of indexer names.");
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
							int docNr = Integer.parseInt(sqr.getString(0));
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
	}
	
	/* (non-Javadoc)
	 * @see de.goldenGateScf.AbstractServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		System.out.println("GoldenGateSRS: shutting down ...");
		
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
		
		System.gc();
		
		this.io.close();
		System.out.println("  - disconnected from database");
	}
	
	private static final String LIST_INDEXERS_COMMAND = "indexers";
	private static final String LIST_FILTERS_COMMAND = "filters";
	private static final String LIST_SPLITTERS_COMMAND = "splitters";
	
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
			public String getActionCommand() {
				return SEARCH_DOCUMENTS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read query
				String queryString = input.readLine();
				Query query = parseQuery(queryString);
				
				//	get result
				DocumentResult dr = searchDocuments(query);
				if (DEBUG_DOCUMENT_SEARCH) System.out.println("GgSRS: document search complete");
				if (DEBUG_DOCUMENT_SEARCH) System.out.println("  query was " + query.toString());
				
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
			public String getActionCommand() {
				return SEARCH_DOCUMENT_DETAILS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				//	read query
				String queryString = input.readLine();
				Query query = parseQuery(queryString);
				
				//	get result
				DocumentResult dr = searchDocumentDetails(query);
				if (DEBUG_DOCUMENT_SEARCH) System.out.println("GgSRS: document search complete");
				if (DEBUG_DOCUMENT_SEARCH) System.out.println("  query was " + query.toString());
				
				//	send data
				dr.writeXml(output);
				
				//	make data go
				output.flush();
//				System.out.println("  data sent");
			}
		};
		cal.add(ca);
		
		//	document metadata search
		ca = new ComponentActionNetwork() {
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
					QueriableAnnotation doc = getDocument(docId);
					
					//	indicate document coming
					output.write(GET_XML_DOCUMENT);
					output.newLine();
					
					//	write document
//					Utils.writeDocument(doc, output);
					GenericGamtaXML.storeDocument(doc, output);
					output.newLine();
					
				} catch (Exception e) {
					output.write("Could not find or load document with ID " + docId);
					output.newLine();
				}
			}
		};
		cal.add(ca);
		
		//	thesaurus search query
		ca = new ComponentActionNetwork() {
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
		
		//	statistica data request
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_STATISICS;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				
				CollectionStatistics scs = getStatistics();
				
				output.write(GET_STATISICS);
				output.newLine();
				
				scs.writeXml(output);
				output.newLine();
			}
		};
		cal.add(ca);
		
		//	list documents
		ca = new ComponentActionNetwork() {
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
					System.out.println("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while listing documents.");
					
					//	report error
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
					System.out.println(" There " + ((indexers.length == 1) ? "is" : "are") + " " + ((indexers.length == 0) ? "no" : ("" + indexers.length)) + " Indexer" + ((indexers.length == 1) ? "" : "s") + " installed in GoldenGATE SRS:");
					for (int i = 0; i < indexers.length; i++)
						System.out.println(" - " + indexers[i].getIndexName() + " (" + indexers[i].getClass().getName() + ")");
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
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
					System.out.println(" There " + ((filters.length == 1) ? "is" : "are") + " " + ((filters.length == 0) ? "no" : ("" + filters.length)) + " StorageFilter" + ((filters.length == 1) ? "" : "s") + " installed in GoldenGATE SRS:");
					for (int f = 0; f < filters.length; f++)
						System.out.println(" - " + filters[f].getClass().getName());
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	list document this.splitters
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
					System.out.println(" There " + ((splitters.length == 1) ? "is" : "are") + " " + ((splitters.length == 0) ? "no" : ("" + splitters.length)) + " DocumentSplitter" + ((splitters.length == 1) ? "" : "s") + " installed in GoldenGATE SRS:");
					for (int s = 0; s < splitters.length; s++)
						System.out.println(" - " + splitters[s].getClass().getName());
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	re-index collection
		cal.add(this.reindexAction);
		
		//	get actions from document store
		ComponentAction[] dstActions = this.dst.getActions();
		for (int a = 0; a < dstActions.length; a++)
			cal.add(dstActions[a]);
		
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
	
	private CollectionStatistics getStatistics() throws IOException {
		String docCountQuery = "SELECT" +
				" count(DISTINCT " + MASTER_DOCUMENT_ID_ATTRIBUTE + ") AS " + MASTER_DOCUMENT_COUNT_ATTRIBUTE +
				", count(" + DOCUMENT_ID_ATTRIBUTE + ") AS " + DOCUMENT_COUNT_ATTRIBUTE +
				", sum(" + DOCUMENT_SIZE_ATTRIBUTE + ") AS " + WORD_COUNT_ATTRIBUTE +
				" FROM " + DOCUMENT_TABLE_NAME + ";";
		
		int masterCount = 0;
		int docCount = 0;
		int wordCount = 0;
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(docCountQuery);
			
			//	read & write number of master documents
			String masterDocCountString = sqr.getString(0, 0);
			masterCount = ((masterDocCountString == null) ? 0 : Integer.parseInt(masterDocCountString));
			
			//	read & write number of documents
			String docCountString = sqr.getString(0, 1);
			docCount = ((docCountString == null) ? 0 : Integer.parseInt(docCountString));
			
			//	read & write number of words
			String wordCountString = sqr.getString(0, 2);
			wordCount = ((wordCountString == null) ? 0 : Integer.parseInt(wordCountString));
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading doc count statistics.");
			System.out.println("  query was " + docCountQuery);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		String[] columns = {CHECKIN_USER_ATTRIBUTE, MASTER_DOCUMENT_COUNT_ATTRIBUTE, DOCUMENT_COUNT_ATTRIBUTE, WORD_COUNT_ATTRIBUTE};
		
		String topTenQuery = "SELECT" +
				" " + CHECKIN_USER_ATTRIBUTE + "" +
				", count(DISTINCT " + MASTER_DOCUMENT_ID_ATTRIBUTE + ") AS " + MASTER_DOCUMENT_COUNT_ATTRIBUTE +
				", count(" + DOCUMENT_ID_ATTRIBUTE + ") AS " + DOCUMENT_COUNT_ATTRIBUTE +
				", sum(" + DOCUMENT_SIZE_ATTRIBUTE + ") AS " + WORD_COUNT_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME + 
				" GROUP BY " + CHECKIN_USER_ATTRIBUTE + 
				" ORDER BY " + DOCUMENT_COUNT_ATTRIBUTE + " DESC;";
		
		try {
			return new SqlCollectionStatistics(columns, masterCount, docCount, wordCount, this.io.executeSelectQuery(topTenQuery));
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading top 10 user statistics.");
			System.out.println("  Query was " + topTenQuery);
			return null;
		}
	}
	
	private class SqlCollectionStatistics extends CollectionStatistics {
		private SqlQueryResult sqr;
		private CollectionStatisticsElement cse = null;
		
		SqlCollectionStatistics(String[] statisticsFieldNames, int masterDocCount, int docCount, int wordCount, SqlQueryResult sqr) {
			super(statisticsFieldNames, masterDocCount, docCount, wordCount);
			this.sqr = sqr;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#getNextElement()
		 */
		public SrsSearchResultElement getNextElement() {
			if (this.hasNextElement()) {
				CollectionStatisticsElement cse = this.cse;
				this.cse = null;
				return cse;
			}
			else return null;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#hasNextElement()
		 */
		public boolean hasNextElement() {
			if (this.cse != null)
				return true;
			
			else if (this.sqr == null)
				return false;
			
			else if (this.sqr.next()) {
				this.cse = new CollectionStatisticsElement();
				for (int a = 0; a < this.resultAttributes.length; a++) {
					String value = this.sqr.getString(a);
					if (value != null)
						this.cse.setAttribute(this.resultAttributes[a], value);
				}
				return true;
			}
			
			else {
				this.sqr.close();
				this.sqr = null;
				return false;
			}
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#finalize()
		 */
		protected void finalize() throws Throwable {
			if (this.sqr != null) {
				this.sqr.close();
				this.sqr = null;
			}
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
		try {
			return this.dst.getDocumentAttributes(docId);
		}
		catch (IOException ioe) {
			return null;
		}
	}
	
	/**
	 * Retrieve a document from the SRS's storage.
	 * @param docId the ID of the document to load
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public QueriableAnnotation getDocument(String docId) throws IOException {
		return this.dst.loadDocument(docId);
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
		long updateTime = -1;
		try {
			updateTime = Long.parseLong(masterDoc.getAttribute(UPDATE_TIME_ATTRIBUTE, "-1").toString());
		} catch (NumberFormatException nfe) {}
		if (updateTime == -1)
			updateTime = System.currentTimeMillis();
		
		if (logger != null)
			logger.writeLog(" ===== SRS Collection Update Protocol ===== ");
		
		//	run document through this.filters
		for (int f = 0; f < this.filters.length; f++) {
			String error = this.filters[f].filter(masterDoc);
			if (error != null) {
				if (logger != null)
					logger.writeLog("Cannot store document: " + error);
				return 0;
			}
		}
		
		//	split master document
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
		
		//	if no split occurred, copy document to facilitate modification
		if ((docs.length == 1) && (docs[0] == masterDoc))
			docs[0] = Gamta.copyDocument(masterDoc);
		
		//	filter part documents
		for (int f = 0; f < this.filters.length; f++)
			docs = this.filters[f].filter(docs, masterDoc);
		
		//	initialize statistics
		int newDocCount = 0;
		int updateDocCount = 0;
		
		//	store part documents
		StringVector validIDs = new StringVector();
		for (int d = 0; d < docs.length; d++) {
			
			//	check ID
			String docId = docs[d].getAttribute(DOCUMENT_ID_ATTRIBUTE, "").toString();
			if (docId.length() != 0) {
				validIDs.addElement(docId);
				
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
				int docStat = this.updateDocument(docs[d], docId, masterDocId, updateTime, logger);
				
				//	update statistics
				if (docStat == NEW_DOC)
					newDocCount++;
				else if (docStat == UPDATE_DOC)
					updateDocCount++;
			}
		}
		
		//	clean up document table
		int deleteDocCount = this.cleanupMasterDocument(masterDocId, validIDs);
		
		//	write log
		if (logger != null) {
			logger.writeLog("Document successfully stored in SRS collection:");
			logger.writeLog("  - " + newDocCount + " new " + splitResultLabel.toLowerCase() + ((newDocCount == 1) ? "" : "s") + " added");
			logger.writeLog("  - " + updateDocCount + " " + splitResultLabel.toLowerCase() + ((updateDocCount == 1) ? "" : "s") + " updated");
			logger.writeLog("  - " + deleteDocCount + " " + splitResultLabel.toLowerCase() + ((deleteDocCount == 1) ? "" : "s") + " deleted");
		}
		
		return (newDocCount + updateDocCount);
	}
	
	private static final boolean DEBUG_UPDATE_DOCUMENT = false;
	
	private static final int KEEP_DOC = 0;
	private static final int UPDATE_DOC = 1;
	private static final int NEW_DOC = 2;
	private int updateDocument(final QueriableAnnotation doc, String docId, String masterDocId, long updateTime, EventLogger logger) throws IOException {
		final int docNr;
		final int documentStatus;
		
		String docCheckSum = getChecksum(doc);
		String updateUser;
		
		String existQuery = "SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_CHECKSUM_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + ", " + MASTER_DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_AUTHOR_ATTRIBUTE + ", " + DOCUMENT_ORIGIN_ATTRIBUTE + ", " + DOCUMENT_DATE_ATTRIBUTE + ", " + PAGE_NUMBER_ATTRIBUTE + ", " + LAST_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_LAST_PAGE_NUMBER_ATTRIBUTE + ", " + DOCUMENT_SOURCE_LINK_ATTRIBUTE + 
		" FROM " + DOCUMENT_TABLE_NAME +
		" WHERE " + DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docId) + "';";
		
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(existQuery);
			
			//	document exists
			if (sqr.next()) {
				
				//	read data
				docNr = Integer.parseInt(sqr.getString(0));
				if (DEBUG_UPDATE_DOCUMENT) System.out.println("  - updating document " + docNr);
				
				String oldDocCheckSum = sqr.getString(1);
				
				//	document unchanged (or we have an MD5 collision ...)
				if (docCheckSum.equals(oldDocCheckSum)) {
					if (DEBUG_UPDATE_DOCUMENT) System.out.println("    - document unmodified");
					documentStatus = KEEP_DOC;
					updateUser = "";
				}
				
				//	we have an update
				else {
					if (DEBUG_UPDATE_DOCUMENT) System.out.println("    - document modified");
					documentStatus = UPDATE_DOC;
					
					//	read remaining data
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
					if (DEBUG_UPDATE_DOCUMENT) System.out.println("    - got document data");
					
					StringVector assignments = new StringVector();
					
					//	update checksum
					assignments.addElement(DOCUMENT_CHECKSUM_COLUMN_NAME + " = '" + EasyIO.sqlEscape(docCheckSum) + "'");
					
					//	check and (if necessary) truncate title 
					String masterTitle = doc.getAttribute(MASTER_DOCUMENT_TITLE_ATTRIBUTE, "Unknown Document").toString().trim();
					if (masterTitle.length() > MASTER_DOCUMENT_TITLE_COLUMN_LENGTH)
						masterTitle = masterTitle.substring(0, MASTER_DOCUMENT_TITLE_COLUMN_LENGTH);
					if (!masterTitle.equals(dataMasterDocTitle))
						assignments.addElement(MASTER_DOCUMENT_TITLE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(masterTitle) + "'");
					
					//	check and (if necessary) truncate part title 
					String title = doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, "").toString().trim();
					if (title.length() > DOCUMENT_TITLE_COLUMN_LENGTH)
						title = title.substring(0, DOCUMENT_TITLE_COLUMN_LENGTH);
					if (!title.equals(dataDocTitle))
						assignments.addElement(DOCUMENT_TITLE_ATTRIBUTE + " = '" + EasyIO.sqlEscape(title) + "'");
					
					//	check and (if necessary) truncate author
					String author = doc.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, "Unknown Author").toString().trim();
					if (author.length() > DOCUMENT_AUTHOR_COLUMN_LENGTH)
						author = author.substring(0, DOCUMENT_AUTHOR_COLUMN_LENGTH);
					if (!author.equals(dataDocAuthor))
						assignments.addElement(DOCUMENT_AUTHOR_ATTRIBUTE + " = '" + EasyIO.sqlEscape(author) + "'");
					
					//	check origin
					String origin = doc.getAttribute(DOCUMENT_ORIGIN_ATTRIBUTE, "Unknown Journal or Book").toString().trim();
					if (origin.length() > DOCUMENT_ORIGIN_COLUMN_LENGTH)
						origin = origin.substring(0, DOCUMENT_ORIGIN_COLUMN_LENGTH);
					if (!origin.equals(dataDocOrigin))
						assignments.addElement(DOCUMENT_ORIGIN_ATTRIBUTE + " = '" + EasyIO.sqlEscape(origin) + "'");
					
					//	check year
					String year = doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1").toString().trim();
					if (!year.equals(dataDocYear))
						assignments.addElement(DOCUMENT_DATE_ATTRIBUTE + " = " + EasyIO.sqlEscape(year));
					
					//	check source link
					String sourceLink = doc.getAttribute(DOCUMENT_SOURCE_LINK_ATTRIBUTE, "").toString().trim();
					if (sourceLink.length() > DOCUMENT_SOURCE_LINK_COLUMN_LENGTH)
						sourceLink = sourceLink.substring(0, DOCUMENT_SOURCE_LINK_COLUMN_LENGTH);
					if (!sourceLink.equals(dataDocSourceLink))
						assignments.addElement(DOCUMENT_SOURCE_LINK_ATTRIBUTE + " = '" + EasyIO.sqlEscape(sourceLink) + "'");
					
					//	check page number
					int pageNumber = -1;
					try {
						pageNumber = Integer.parseInt(doc.getAttribute(PAGE_NUMBER_ATTRIBUTE, "-1").toString());
					} catch (NumberFormatException nfe) {}
					if (!("" + pageNumber).equals(dataPageNumber))
						assignments.addElement(PAGE_NUMBER_ATTRIBUTE + " = " + pageNumber);
					
					//	check last page number
					int lastPageNumber = -1;
					try {
						lastPageNumber = Integer.parseInt(doc.getAttribute(LAST_PAGE_NUMBER_ATTRIBUTE, "-1").toString());
					} catch (NumberFormatException nfe) {}
					if (!("" + lastPageNumber).equals(dataLastPageNumber))
						assignments.addElement(LAST_PAGE_NUMBER_ATTRIBUTE + " = " + lastPageNumber);
					
					//	check parent document page number
					int masterDocPageNumber = -1;
					try {
						masterDocPageNumber = Integer.parseInt(doc.getAttribute(MASTER_PAGE_NUMBER_ATTRIBUTE, "-1").toString());
					} catch (NumberFormatException nfe) {}
					if (!("" + masterDocPageNumber).equals(dataMasterDocPageNumber))
						assignments.addElement(MASTER_PAGE_NUMBER_ATTRIBUTE + " = " + masterDocPageNumber);
					
					//	check last page number
					int masterDocLastPageNumber = -1;
					try {
						masterDocLastPageNumber = Integer.parseInt(doc.getAttribute(MASTER_LAST_PAGE_NUMBER_ATTRIBUTE, "-1").toString());
					} catch (NumberFormatException nfe) {}
					if (!("" + masterDocLastPageNumber).equals(dataMasterDocLastPageNumber))
						assignments.addElement(MASTER_LAST_PAGE_NUMBER_ATTRIBUTE + " = " + masterDocLastPageNumber);
					
					//	get update user
					updateUser = doc.getAttribute(UPDATE_USER_ATTRIBUTE, "").toString();
					if (updateUser.length() > USER_LENGTH)
						updateUser = updateUser.substring(0, USER_LENGTH);
					if (updateUser.length() != 0)
						assignments.addElement(UPDATE_USER_ATTRIBUTE + " = '" + EasyIO.sqlEscape(updateUser) + "'");
					
					//	get update timestamp
					assignments.addElement(UPDATE_TIME_ATTRIBUTE + " = " + updateTime);					
					
					//	write new values
					if (!assignments.isEmpty()) {
						String updateQuery = ("UPDATE " + DOCUMENT_TABLE_NAME + 
								" SET " + assignments.concatStrings(", ") + 
								" WHERE " + DOC_NUMBER_COLUMN_NAME + " = " + docNr + ";");
						try {
							this.io.executeUpdateQuery(updateQuery);
							if (DEBUG_UPDATE_DOCUMENT) System.out.println("    - updates written");
						}
						catch (SQLException sqle) {
							System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while updating data of document " + docNr + ".");
							System.out.println("  Query was " + updateQuery);
						}
					}
					
					//	remove document from this.indexers, will be re-indexed later
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
				docNr = nextDocNr();
				documentStatus = NEW_DOC;
				
				//	get checkin user
				String checkinUser = doc.getAttribute(CHECKIN_USER_ATTRIBUTE, "Unknown User").toString();
				if (checkinUser.length() > USER_LENGTH)
					checkinUser = checkinUser.substring(0, USER_LENGTH);
				updateUser = checkinUser;
				
				//	get and (if necessary) truncate title 
				String masterTitle = doc.getAttribute(MASTER_DOCUMENT_TITLE_ATTRIBUTE, "Unknown Document").toString().trim();
				if (masterTitle.length() > MASTER_DOCUMENT_TITLE_COLUMN_LENGTH)
					masterTitle = masterTitle.substring(0, MASTER_DOCUMENT_TITLE_COLUMN_LENGTH);
				
				//	get and (if necessary) truncate title 
				String title = doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, "").toString().trim();
				if (title.length() > DOCUMENT_TITLE_COLUMN_LENGTH)
					title = title.substring(0, DOCUMENT_TITLE_COLUMN_LENGTH);
				
				//	get and (if necessary) truncate author 
				String author = doc.getAttribute(DOCUMENT_AUTHOR_ATTRIBUTE, "Unknown Author").toString().trim();
				if (author.length() > DOCUMENT_AUTHOR_COLUMN_LENGTH)
					author = author.substring(0, DOCUMENT_AUTHOR_COLUMN_LENGTH);
				
				//	get origin
				String origin = doc.getAttribute(DOCUMENT_ORIGIN_ATTRIBUTE, "Unknown Journal or Book").toString().trim();
				if (origin.length() > DOCUMENT_ORIGIN_COLUMN_LENGTH)
					origin = origin.substring(0, DOCUMENT_ORIGIN_COLUMN_LENGTH);
				
				//	get year
				String year = doc.getAttribute(DOCUMENT_DATE_ATTRIBUTE, "-1").toString().trim();
				
				//	get source link
				String sourceLink = doc.getAttribute(DOCUMENT_SOURCE_LINK_ATTRIBUTE, "").toString().trim();
				if (sourceLink.length() > DOCUMENT_SOURCE_LINK_COLUMN_LENGTH)
					sourceLink = masterTitle.substring(0, DOCUMENT_SOURCE_LINK_COLUMN_LENGTH);
				
				//	get page number(s)
				int pageNumber = -1;
				try {
					pageNumber = Integer.parseInt(doc.getAttribute(PAGE_NUMBER_ATTRIBUTE, "-1").toString());
				} catch (NumberFormatException nfe) {}
				int lastPageNumber = -1;
				try {
					lastPageNumber = Integer.parseInt(doc.getAttribute(LAST_PAGE_NUMBER_ATTRIBUTE, "-1").toString());
				} catch (NumberFormatException nfe) {}
				
				//	get page number(s) of parent doc
				int masterDocPageNumber = -1;
				try {
					masterDocPageNumber = Integer.parseInt(doc.getAttribute(MASTER_PAGE_NUMBER_ATTRIBUTE, "-1").toString());
				} catch (NumberFormatException nfe) {}
				int masterDocLastPageNumber = -1;
				try {
					masterDocLastPageNumber = Integer.parseInt(doc.getAttribute(MASTER_LAST_PAGE_NUMBER_ATTRIBUTE, "-1").toString());
				} catch (NumberFormatException nfe) {}
				
				//	store data in collection main table
				String insertQuery = "INSERT INTO " + DOCUMENT_TABLE_NAME + " (" + 
									DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_CHECKSUM_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + ", " + MASTER_DOCUMENT_ID_ATTRIBUTE + ", " + CHECKIN_USER_ATTRIBUTE + ", " + CHECKIN_TIME_ATTRIBUTE + ", " + UPDATE_USER_ATTRIBUTE + ", " + UPDATE_TIME_ATTRIBUTE + ", " + MASTER_DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_TITLE_ATTRIBUTE + ", " + DOCUMENT_AUTHOR_ATTRIBUTE + ", " + DOCUMENT_ORIGIN_ATTRIBUTE + ", " + DOCUMENT_DATE_ATTRIBUTE + ", " + DOCUMENT_SOURCE_LINK_ATTRIBUTE + ", " + PAGE_NUMBER_ATTRIBUTE + ", " + LAST_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_PAGE_NUMBER_ATTRIBUTE + ", " + MASTER_LAST_PAGE_NUMBER_ATTRIBUTE + ", " + DOCUMENT_SIZE_ATTRIBUTE + 
									") VALUES (" +
									docNr + ", '" + EasyIO.sqlEscape(docCheckSum) + "', '" + EasyIO.sqlEscape(docId) + "', '" + EasyIO.sqlEscape(masterDocId) + "', '" + EasyIO.sqlEscape(checkinUser) + "', " + updateTime + ", '" + EasyIO.sqlEscape(checkinUser) + "', " + updateTime + ", '" + EasyIO.sqlEscape(masterTitle) + "', '" + EasyIO.sqlEscape(title) + "', '" + EasyIO.sqlEscape(author) + "', '" + EasyIO.sqlEscape(origin) + "', " + EasyIO.sqlEscape(year) + ", '" + EasyIO.sqlEscape(sourceLink) + "', " + pageNumber + ", " + lastPageNumber + ", " + masterDocPageNumber + ", " + masterDocLastPageNumber + ", " + doc.size() + 
									");";
				try {
					this.io.executeUpdateQuery(insertQuery);
				}
				catch (SQLException sqle) {
					System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while storing document.");
					System.out.println("  Query was " + insertQuery);
					throw new IOException("Could not store document: Error writing management table."); // don't store file if document couldn't be entered in management table
				}
			}
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while checking if document exists.");
			System.out.println("  Query was " + existQuery);
			throw new IOException(sqle.getMessage());
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	in case of no update, we're done here
		if (documentStatus == KEEP_DOC)
			return documentStatus;
		
		//	store document
		int version = this.dst.storeDocument(doc, docId);
		
		//	run document through indexers asynchronously for better upload performance
		for (int i = 0; i < this.indexers.length; i++) {
			final Indexer indexer = this.indexers[i];
			this.enqueueIndexerAction(new Runnable() {
				public void run() {
					indexer.index(doc, docNr);
				}
			});
		}
		
		//	issue event
		GoldenGateServerEventService.notify(new SrsDocumentEvent(updateUser, docId, doc, version, this.getClass().getName(), updateTime, logger));
		
		//	report whether update or insert
		return documentStatus;
	}
	
	//	after a document update, delete the management table entries for the parts of a master document that no longer exist
	private int cleanupMasterDocument(String masterDocId, StringVector docIDs) throws IOException {
		
		//	get document numbers to delete
		String dataQuery = "SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + 
			" FROM " + DOCUMENT_TABLE_NAME + 
			" WHERE " + MASTER_DOCUMENT_ID_ATTRIBUTE + " LIKE '" + masterDocId + "'" +
				(docIDs.isEmpty() ? "" : (" AND " + DOCUMENT_ID_ATTRIBUTE + " NOT IN ('" + docIDs.concatStrings("', '") + "')")) +
			";";
		int deleteDocCount = 0;
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(dataQuery);
			while (sqr.next()) {
				
				//	get data
				final int docNr = Integer.parseInt(sqr.getString(0));
				String docId = sqr.getString(1);
				
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
				}
				catch (IOException ioe) {
					System.out.println("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
					ioe.printStackTrace(System.out);
				}
				
				//	delete master table entry
				String deleteQuery = "DELETE FROM " + DOCUMENT_TABLE_NAME + " WHERE " + DOC_NUMBER_COLUMN_NAME + " = " + docNr + ";";
				try {
					this.io.executeUpdateQuery(deleteQuery);
					deleteDocCount++;
				}
				catch (SQLException sqle) {
					System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
					System.out.println("  Query was " + deleteQuery);
				}
			}
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while cleaning document table.");
			System.out.println("  Query was " + dataQuery);
			return 0;
		}
		finally {
			if (sqr != null)
				sqr.close();
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
		
		//	initialize counter
		int deleteCount = 0;
		
		//	get numbers and IDs of doc to delete, no matter if specified ID belongs to a specific part or a document as a whole
		String query = "SELECT " + DOC_NUMBER_COLUMN_NAME + ", " + DOCUMENT_ID_ATTRIBUTE + 
				" FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE " + MASTER_DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(masterDocId) + "'" +
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			while (sqr.next()) {
				
				//	remove part from indices
				final int docNr = Integer.parseInt(sqr.getString(0));
				for (int i = 0; i < this.indexers.length; i++) {
					final Indexer indexer = this.indexers[i];
					this.enqueueIndexerAction(new Runnable() {
						public void run() {
							indexer.deleteDocument(docNr);
						}
					});
				}
				
				String docId = sqr.getString(1);
				try {
					this.dst.deleteDocument(docId);
				}
				catch (IOException ioe) {
					System.out.println("GoldenGateSRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
					ioe.printStackTrace(System.out);
				}
				
				//	issue event
				GoldenGateServerEventService.notify(new SrsDocumentEvent(userName, docId, this.getClass().getName(), System.currentTimeMillis(), logger));
				
				//	update statistics
				deleteCount++;
			}
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			System.out.println("  Query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		//	delete entries from management table
		query = "DELETE FROM " + DOCUMENT_TABLE_NAME + 
				" WHERE " + MASTER_DOCUMENT_ID_ATTRIBUTE + 
				" LIKE '" + EasyIO.sqlEscape(masterDocId) + "'" +
				";";
		try {
			this.io.executeUpdateQuery(query);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while deleting document.");
			System.out.println("  Query was " + query);
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
				System.out.println("Error on index update - " + t.getClass().getName() + " (" + t.getMessage() + ")");
				t.printStackTrace(System.out);
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
			query.append(" GROUP BY " + MASTER_DOCUMENT_ID_ATTRIBUTE);
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
				System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
				System.out.println("  query was " + query);
				if (sqr != null)
					sqr.close();
				throw new IOException(sqle.getMessage());
			}
		}
		
		//	request for sub document list
		else {
			
			StringBuffer query = new StringBuffer("SELECT ");
			for (int f = 0; f < documentListQueryFields.length; f++) {
				if (f != 0)
					query.append(", ");
				query.append(documentListQueryFields[f]);
			}
			query.append(" FROM " + DOCUMENT_TABLE_NAME);
			query.append(" WHERE " + MASTER_DOCUMENT_ID_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(masterDocID) + "'");
			query.append(" ORDER BY " + DOC_NUMBER_COLUMN_NAME + ";");
			
			SqlQueryResult sqr = null;
			try {
				sqr = this.io.executeSelectQuery(query.toString());
				docList = new SqlDocumentList(documentListFields, sqr) {
					protected DocumentListElement decodeListElement(String[] elementData) {
						DocumentListElement dle = new DocumentListElement();
						for (int f = 0; f < documentListQueryFields.length; f++) {
							if (elementData[f] != null)
								dle.setAttribute(documentListQueryFields[f], elementData[f]);
						}
						return dle;
					}
				};
			}
			catch (SQLException sqle) {
				System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while listing documents.");
				System.out.println("  query was " + query);
				if (sqr != null)
					sqr.close();
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
				int[] dleDocNrs = new int[dleList.size()];
				HashMap dlesByDocNr = new HashMap();
				for (int d = 0; d < dleList.size(); d++) {
					DocumentListElement dle = ((DocumentListElement) dleList.get(d));
					Integer docNr = new Integer(dle.getAttribute(DOC_NUMBER_COLUMN_NAME, "-1").toString());
					dleDocNrs[d] = docNr.intValue();
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
							Integer dleKey = new Integer(extensionIre.docNr);
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
			if (elementData == null) return null;
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
	
	private static final boolean DEBUG_DOCUMENT_SEARCH = false;
	private static final String[] documentSearchResultAttributes = {
		DOCUMENT_ID_ATTRIBUTE, 
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
	private DocumentResult searchDocuments(Query query) throws IOException {
		if (DEBUG_DOCUMENT_SEARCH) System.out.println("GgSRS: doing document search ...");
		
		//	get document numbers
		QueryResult docNrResult = this.searchDocumentNumbers(query);
		if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - got " + docNrResult.size() + " result document numbers");
		
		
		//	read additional parameters
		int queryResultPivotIndex = this.resultPivotIndex;
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
		QueryResult relevanceResult = doRelevanceElimination(docNrResult, minRelevance, queryResultPivotIndex);
		
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
					if (this.docNumberDeDuplicator.add(new Integer(qre.docNr))) {
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
					sqr = io.executeSelectQuery(docNrToDocDataQuery.toString());
					
					//	read & store data
					while (sqr.next()) {
						String docNr = sqr.getString(0);
						String[] docData = new String[this.resultAttributes.length];
						for (int d = 0; d < this.resultAttributes.length; d++)
							docData[d] = sqr.getString(d+1);
						docNrsToDocData.put(docNr, docData);
					}
				}
				catch (SQLException sqle) {
					System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document data.");
					System.out.println("  query was " + docNrToDocDataQuery.toString());
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
							if (docData[a] != null)
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
					doc = dst.loadDocument(dre.documentId);
				}
				catch (IOException ioe) {
					System.out.println("GoldenGateSRS: Error loading document '" + dre.documentId + "' (" + ioe.getMessage() + ")");
					ioe.printStackTrace(System.out);
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
	
	private DocumentResult searchDocumentDetails(Query query) throws IOException {
		
		//	get result
		final DocumentResult fullDr = searchDocuments(query);
		
		//	wrap document result in order to reduce documents
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
	private DocumentResult searchDocumentData(Query query) throws IOException {
		return ((DocumentResult) this.searchDocumentData(query, true));
	}
	
	private static final String[] documentIndexSearchResultAttributes = {
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
	private IndexResult searchDocumentIndex(Query query) throws IOException {
		return ((IndexResult) this.searchDocumentData(query, false));
	}
	
	private SrsSearchResult searchDocumentData(final Query query, boolean rankedResult) throws IOException {
		if (DEBUG_INDEX_SEARCH || DEBUG_DOCUMENT_SEARCH) System.out.println("GgSRS: doing document data search ...");
		
		//	get document numbers
		QueryResult docNrResult = this.searchDocumentNumbers(query);
		if (DEBUG_INDEX_SEARCH || DEBUG_DOCUMENT_SEARCH) System.out.println("  - got " + docNrResult.size() + " result document numbers");
		
		//	get sub index IDs
		StringVector subIndexNames = new StringVector();
		String subIndexNameString =  query.getValue(SUB_INDEX_NAME);
		if (subIndexNameString != null)
			subIndexNames.parseAndAddElements(subIndexNameString, "\n");
		subIndexNames.removeAll("0");
		subIndexNames.removeAll("");
		if (DEBUG_INDEX_SEARCH || DEBUG_DOCUMENT_SEARCH) System.out.println("  - sub index names are " + subIndexNames.concatStrings(","));
		
		//	get minimum sub result size
		int srms;
		try {
			srms = Integer.parseInt(query.getValue(SUB_RESULT_MIN_SIZE, "0"));
		}
		catch (NumberFormatException nfe) {
			srms = 0;
		}
		final int subResultMinSize = (subIndexNames.isEmpty() ? 0 : Math.max(srms, 0));
		
		//	got query for ranked result
		if (rankedResult) {
			
			//	read additional parameters
			int queryResultPivotIndex = this.resultPivotIndex;
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
			QueryResult relevanceResult = doRelevanceElimination(docNrResult, minRelevance, ((subResultMinSize == 0) ? queryResultPivotIndex : 0));
			if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - got " + docNrResult.size() + " result document numbers after relevance elimination");
			
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
				if (DEBUG_INDEX_SEARCH || DEBUG_DOCUMENT_SEARCH) System.out.println("  - got sub indexer: " + subIndexName);
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
				private int returned = 0;
				private double lastRelevance = 0;
				private DocumentResultElement nextDre = null;
				private void ensureNextElement() {
					if (this.nextDre != null) return;
					
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
					if (subResultMinSize == 0) return super.hasNextElement();
					
					this.ensureNextElement();
					return (this.nextDre != null);
				}
				public SrsSearchResultElement getNextElement() {
					if (subResultMinSize == 0) return super.getNextElement();
					
					DocumentResultElement dre = (this.hasNextElement() ? this.nextDre : null);
					this.nextDre = null;
					return dre;
				}
				
				protected void fillDreBuffer(int dreCount) {
					if (DEBUG_DOCUMENT_SEARCH) System.out.println("SqlDocumentResult: filling buffer with at most " + dreCount + " elements");
					int dreAdded = 0;
					
					//	get query result elements for next chunk
					ArrayList qreList = new ArrayList();
					while ((qreList.size() < dreCount) && this.hasNextQre())
						qreList.add(this.getNextQre());
					
					//	get document numbers
					StringVector docNumberCollector = new StringVector();
					int[] docNumbers = new int[qreList.size()];
					for (int r = 0; r < qreList.size(); r++) {
						QueryResultElement qre = ((QueryResultElement) qreList.get(r));
						docNumberCollector.addElement("" + qre.docNr);
						docNumbers[r] = qre.docNr;
					}
					if (docNumberCollector.isEmpty()) {
						if (DEBUG_DOCUMENT_SEARCH) System.out.println(" - no further documents");
						return;
					}
					if (DEBUG_DOCUMENT_SEARCH) System.out.println(" - got " + docNumberCollector.size() + " further document numbers");
					
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
						if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - NR to doc data query is " + docNrToDocDataQuery.toString());
						sqr = io.executeSelectQuery(docNrToDocDataQuery.toString());
						
						//	read & store data
						while (sqr.next()) {
							String docNr = sqr.getString(0);
							String[] docData = new String[this.resultAttributes.length];
							for (int d = 0; d < this.resultAttributes.length; d++)
								docData[d] = sqr.getString(d+1);
							docNrsToDocData.put(docNr, docData);
						}
					}
					catch (SQLException sqle) {
						System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document data.");
						System.out.println("  query was " + docNrToDocDataQuery.toString());
					}
					finally {
						if (sqr != null)
							sqr.close();
					}
					if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - got " + docNrsToDocData.size() + " doc data sets");
					
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
							if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - got sub result from " + subIndexName);
							
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
						if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - adding result document " + qre.docNr + " (" + qre.relevance + ")");
						
						String[] docData = ((String[]) docNrsToDocData.remove("" + qre.docNr));
						if (docData != null) {
							DocumentResultElement dre = new DocumentResultElement(qre.docNr, docData[0], qre.relevance, null);
							for (int a = 0; a < this.resultAttributes.length; a++) {
								if (docData[a] != null)
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
					if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - added " + dreAdded + " result documents");
					
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
						IndexResultElement ire = new IndexResultElement(Integer.parseInt(elementData[0]), DocumentRoot.DOCUMENT_TYPE, value.toString());
						ire.copyAttributes(data);
						return ire;
					}
				};
				
				//	no sub results required, we're done
				if (subIndexers.length == 0) return baseResult;
				
				//	wrap result in buffer for adding sub results chunk-wise
				return new BufferedIndexResult(baseResult, 100) {
					protected void fillIreBuffer(int ireCount) {
						int ireAdded = 0;
						
						//	get main result elements for next chunk
						ArrayList ireList = new ArrayList();
						while ((ireList.size() < ireCount) && this.data.hasNextElement())
							ireList.add(this.data.getNextIndexResultElement());
						
						//	get document numbers
						int[] ireDocNrs = new int[ireList.size()];
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
								if (DEBUG_INDEX_SEARCH) System.out.println("  - got sub result from " + subIndexName);
								
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
						
						//	join elments with sub results and store them
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
				System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document data.");
				System.out.println("  query was " + docNrToDocDataQuery.toString());
				if (sqr != null)
					sqr.close();
				throw new IOException(sqle.getMessage());
			}
		}
	}
	
	private static final String[] documentIdSearchResultAttributes = {
			DOCUMENT_ID_ATTRIBUTE,
			RELEVANCE_ATTRIBUTE
		};
	private DocumentResult searchDocumentIDs(Query query) throws IOException {
		if (DEBUG_DOCUMENT_SEARCH) System.out.println("GgSRS: doing document ID search ...");
		
		//	get document numbers
		QueryResult docNrResult = this.searchDocumentNumbers(query);
		if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - got " + docNrResult.size() + " result document numbers");
		
		//	read additional parameters
		int queryResultPivotIndex = this.resultPivotIndex;
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
		QueryResult relevanceResult = doRelevanceElimination(docNrResult, minRelevance, queryResultPivotIndex);
		if (DEBUG_DOCUMENT_SEARCH) System.out.println("  - got " + relevanceResult.size() + " duplicate free, relevance sorted result document numbers");
		
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
					if (this.docNumberDeDuplicator.add(new Integer(qre.docNr))) {
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
					System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document IDs.");
					System.out.println("Query was " + docNrsToDocIDsQuery);
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
	
	private static final boolean DEBUG_DOCUMENT_NR_SEARCH = false;
	private QueryResult searchDocumentNumbers(Query query) throws IOException {
		if (DEBUG_DOCUMENT_NR_SEARCH) System.out.println("GgSRS Document Number Search ...");
		if (DEBUG_DOCUMENT_NR_SEARCH) System.out.println("  - query is " + query.toString());
		
		//	get merge mode
		int queryResultMergeMode = this.resultMergeMode;
		String queryResultMergeModeString = query.getValue(RESULT_MERGE_MODE_PARAMETER);
		if (USE_MIN_MERGE_MODE_NAME.equals(queryResultMergeModeString)) queryResultMergeMode = QueryResult.USE_MIN;
		else if (USE_AVERAGE_MERGE_MODE_NAME.equals(queryResultMergeModeString)) queryResultMergeMode = QueryResult.USE_AVERAGE;
		else if (USE_MAX_MERGE_MODE_NAME.equals(queryResultMergeModeString)) queryResultMergeMode = QueryResult.USE_MAX;
		else if (MULTIPLY_MERGE_MODE_NAME.equals(queryResultMergeModeString)) queryResultMergeMode = QueryResult.MULTIPLY;
		else if (INVERSE_MULTIPLY_MERGE_MODE_NAME.equals(queryResultMergeModeString)) queryResultMergeMode = QueryResult.INVERSE_MULTIPLY;
		
		//	check for request by document id
		StringVector fixedResultDocIDs = new StringVector();
		fixedResultDocIDs.parseAndAddElements(query.getValue(ID_QUERY_FIELD_NAME, ""), "\n");
		for (int i = 0; i < fixedResultDocIDs.size(); i++) 
			fixedResultDocIDs.set(i, EasyIO.sqlEscape(fixedResultDocIDs.get(i).trim()));
		fixedResultDocIDs.removeAll("");
		fixedResultDocIDs.removeDuplicateElements(false);
		if (DEBUG_DOCUMENT_NR_SEARCH) System.out.println("  - got " + fixedResultDocIDs.size() + " fixed IDs");
		
		//	process query
		for (int i = 0; i < this.indexers.length; i++) {
			query.setIndexNameMask(this.indexers[i].getIndexName());
			QueryResult qr = this.indexers[i].processQuery(query);
			if (qr != null) query.addPartialResult(qr);
			query.setIndexNameMask(null);
		}
		
		//	retrieve timestamp filters
		long modifiedSince;
		try {
			modifiedSince = Long.parseLong(query.getValue(LAST_MODIFIED_SINCE, "0"));
		}
		catch (NumberFormatException e) {
			modifiedSince = 0;
		}
		long modifiedBefore;
		try {
			modifiedBefore = Long.parseLong(query.getValue(LAST_MODIFIED_BEFORE, "0"));
		}
		catch (NumberFormatException e) {
			modifiedBefore = 0;
		}
		
		//	do timstamp filtering
		QueryResult timestampResult = null;
		if ((modifiedSince != 0) || (modifiedBefore != 0)) {
			timestampResult = new QueryResult();
			String docTimestampQuery = "SELECT " + DOC_NUMBER_COLUMN_NAME + 
					" FROM " + DOCUMENT_TABLE_NAME + 
					" WHERE " + ((modifiedSince == 0) ? "0=0" : (UPDATE_TIME_ATTRIBUTE + " >= " + modifiedSince)) + 
						" AND " + ((modifiedBefore == 0) ? "0=0" : (UPDATE_TIME_ATTRIBUTE + " <= " + modifiedSince)) + 
					";";
			SqlQueryResult sqr = null;
			try {
				sqr = this.io.executeSelectQuery(docTimestampQuery);
				while (sqr.next()) {
					
					//	read data
					timestampResult.addResultElement(new QueryResultElement(Integer.parseInt(sqr.getString(0)), 1));
				}
			}
			catch (SQLException sqle) {
				System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document IDs.");
				System.out.println("  Query was " + docTimestampQuery);
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
		
		//	no result, or timestamp filter only
		if (partialDocNrResults.isEmpty() && fixedResultDocIDs.isEmpty()) {
			
			//	no result at all
			if (timestampResult == null) {
				if (DEBUG_DOCUMENT_NR_SEARCH) System.out.println("  - got empty result");
				return new QueryResult();
			}
			
			//	timestamp filter result
			else {
				if (DEBUG_DOCUMENT_NR_SEARCH) System.out.println("  - got " + timestampResult.size() + " results time based results");
				return timestampResult;
			}
		}
		
		//	ID query only
		else if (partialDocNrResults.isEmpty()) {
			QueryResult result = new QueryResult();
			
			String docIdToDocNrQuery = "SELECT " + DOC_NUMBER_COLUMN_NAME + " FROM " + DOCUMENT_TABLE_NAME + " WHERE " + DOCUMENT_ID_ATTRIBUTE + " IN ('" + fixedResultDocIDs.concatStrings("', '") + "');";
			SqlQueryResult sqr = null;
			try {
				sqr = this.io.executeSelectQuery(docIdToDocNrQuery);
				while (sqr.next()) {
					
					//	read data
					result.addResultElement(new QueryResultElement(Integer.parseInt(sqr.getString(0)), 1));
				}
			}
			catch (SQLException sqle) {
				System.out.println("GoldenGateSRS: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while loading result document IDs.");
				System.out.println("  Query was " + docIdToDocNrQuery);
			}
			finally {
				if (sqr != null)
					sqr.close();
			}
			
			//	apply timestamp filter if given
			if (timestampResult != null)
				result = QueryResult.merge(result, timestampResult, QueryResult.USE_MIN, 0);
			
			//	return result
			if (DEBUG_DOCUMENT_NR_SEARCH) System.out.println("  - got " + result.size() + " document numbers in result");
			return result;
		}
		
		//	ranked result
		else {
			QueryResult docNrResult = null;
			
			//	merge retrieval results from individual this.indexers
			QueryResult qr1 = null;
			QueryResult qr2 = null;
			while (partialDocNrResults.size() > 1) {
				qr1 = ((QueryResult) partialDocNrResults.removeFirst());
				qr2 = ((QueryResult) partialDocNrResults.removeFirst());
				partialDocNrResults.addLast(QueryResult.merge(qr1, qr2, queryResultMergeMode, 0));
			}
			docNrResult = ((QueryResult) partialDocNrResults.removeFirst());
			
			//	apply timestamp filter if given
			if (timestampResult != null)
				docNrResult = QueryResult.merge(docNrResult, timestampResult, QueryResult.USE_MIN, 0);
			
			//	no results left after merge
			if (docNrResult.size() == 0) return new QueryResult();
			
			//	sort & return joint result
			docNrResult.sortByRelevance(true);
			if (DEBUG_DOCUMENT_NR_SEARCH) System.out.println("  - got " + docNrResult.size() + " document numbers in result");
			return docNrResult;
		}
	}
	
	private final QueryResult doRelevanceElimination(QueryResult baseResult, double minRelevance, int resultPivotIndex) {
		if (baseResult.size() == 0) return baseResult;
		
		baseResult.sortByRelevance(true);
		if (resultPivotIndex <= 0)
			resultPivotIndex = baseResult.size();
		
		HashSet resultDocNumbers = new HashSet();
		QueryResult reducedResult = new QueryResult();
		double lastRelevance = 1;
		for (int r = 0; r < baseResult.size(); r++) {
			QueryResultElement qre = baseResult.getResult(r);
			
			//	omit all documents less relevant than the pivot document
			if ((reducedResult.size() < resultPivotIndex) || (qre.relevance >= lastRelevance)) {
				
				Integer docNr = new Integer(qre.docNr);
				
				//	omit document less relevant than the minimum relevance
				if ((qre.relevance >= minRelevance) && resultDocNumbers.add(docNr)) {
					
					//	store result element
					reducedResult.addResultElement(qre);
					
					//	remember last relevance for delaying cutoff
					lastRelevance = qre.relevance;
				}
			}
		}
		
		//	return remaining documents
		return reducedResult;
	}
	
	private static final boolean DEBUG_INDEX_SEARCH = false;
	private IndexResult searchIndex(final Query query) throws IOException {
		if (DEBUG_INDEX_SEARCH) System.out.println("GgSRS Index Search ...");
		if (DEBUG_INDEX_SEARCH) System.out.println("  - query is " + query.toString());
		
		//	get index ID
		String searchIndexName = query.getValue(INDEX_NAME);
		if (searchIndexName == null)
			throw new IOException("No such index.");
		if (DEBUG_INDEX_SEARCH) System.out.println("  - index name is " + searchIndexName);
		
		//	search for document index, search document data and wrap it in IndexResult
		if ("0".equals(searchIndexName))
			return this.searchDocumentIndex(query);
		
		//	find indexer
		Indexer mainIndexer = null;
		for (int i = 0; i < this.indexers.length; i++) {
			String indexName = this.indexers[i].getIndexName();
			if (searchIndexName.equals(indexName)) {
				mainIndexer = this.indexers[i];
				if (DEBUG_INDEX_SEARCH) System.out.println("  - got indexer");
			}
		}
		
		//	index not found
		if (mainIndexer == null) throw new IOException("No such index.");
		
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
		if (DEBUG_INDEX_SEARCH) System.out.println("  - subIndexName(s) are " + subIndexNames.concatStrings(","));
		
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
		if (DEBUG_INDEX_SEARCH) System.out.println("  - got " + qr.size() + " result doc numbers");
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
		int[] docNumbers = new int[qr.size()];
		for (int d = 0; d < docNumbers.length; d++)
			docNumbers[d] = qr.getResult(d).docNr;
		
		query.setIndexNameMask(indexer.getIndexName());
		IndexResult indexResult = indexer.getIndexEntries(query, docNumbers, true);
		query.setIndexNameMask(null);
		
		//	query failed
		if (indexResult == null) throw new IOException("No such index.");
		
		//	get sub indexers
		ArrayList subIndexerList = new ArrayList();
		for (int i = 0; i < indexers.length; i++) {
			String subIndexName = indexers[i].getIndexName();
			
			//	are we interested in this index?
			if (subIndexNames.contains(subIndexName)) {
				subIndexerList.add(this.indexers[i]);
				if (DEBUG_INDEX_SEARCH) System.out.println("  - got sub indexer: " + subIndexName);
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
			ireDocNrSet.add(new Integer(ire.docNr));
		}
		if (DEBUG_INDEX_SEARCH) System.out.println("  - got " + distinctIreList.size() + " distinct index entries");
		
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
				int[] ireBlockDocNrs = new int[ireBlockDocNrSet.size()];
				int docNrIndex = 0;
				for (Iterator dnrit = ireBlockDocNrSet.iterator(); dnrit.hasNext();)
					ireBlockDocNrs[docNrIndex++] = ((Integer) dnrit.next()).intValue();
				
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
						if (DEBUG_INDEX_SEARCH) System.out.println("  - got sub result from " + subIndexName);
						
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
						if (DEBUG_INDEX_SEARCH) System.out.println("  - sub result from " + subIndexName + " bucketized");
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
							LinkedList docNrSubIreList = ((LinkedList) docNrSubIreBuckets.get(((Integer) dnrit.next()).intValue() + "." + subIndexName));
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
	
	private static final boolean DEBUG_THESAURUS_SEARCH = false;
	private ThesaurusResult searchThesaurus(Query query) {
		if (DEBUG_THESAURUS_SEARCH) System.out.println("GgSRS Thesaurus Search ...");
		if (DEBUG_THESAURUS_SEARCH) System.out.println("  - query is " + query.toString());
		
		//	process query through this.indexers one by one
		String thesaurusName = query.getValue(INDEX_NAME);
		if (DEBUG_THESAURUS_SEARCH) System.out.println("  - thesaurus name is " + thesaurusName);
		ThesaurusResult result = null;
		for (int i = 0; i < this.indexers.length; i++) {
			if (DEBUG_THESAURUS_SEARCH) System.out.println("  - indexer is " + this.indexers[i].getClass().getName());
			String indexName = this.indexers[i].getIndexName();
			if (DEBUG_THESAURUS_SEARCH) System.out.println("  - index name is " + indexName);
			if ((thesaurusName == null) || thesaurusName.equals(indexName)) {
				if (DEBUG_THESAURUS_SEARCH) System.out.println("  - got indexer");
				query.setIndexNameMask(indexName);
				result = this.indexers[i].doThesaurusLookup(query);
				query.setIndexNameMask(null);
				if (result != null) {
					if (DEBUG_THESAURUS_SEARCH) System.out.println("  - got result!");
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
	
	private synchronized static int nextDocNr() {
		return ++nextDocNr;
	}
	
	private static MessageDigest checksumDigester = null;
	private static String getChecksum(QueriableAnnotation document) {
		if (checksumDigester == null) {
			try {
				checksumDigester = MessageDigest.getInstance("MD5");
			}
			catch (NoSuchAlgorithmException nsae) {
				System.out.println(nsae.getClass().getName() + " (" + nsae.getMessage() + ") while creating checksum digester.");
				nsae.printStackTrace(System.out); // should not happen, but Java don't know ...
				return Gamta.getAnnotationID(); // use random value so a document is regarded as new
			}
		}
//		long start = System.currentTimeMillis();
		checksumDigester.reset();
		//	omit updateUser & updateTimestamp in checksum computation
		AnnotationInputStream ais = new AnnotationInputStream(document, ENCODING, null, new HashSet() {
			public boolean contains(Object o) {
				return (!UPDATE_USER_ATTRIBUTE.equals(o) && !UPDATE_TIME_ATTRIBUTE.equals(o));
			}
		});
		try {
			byte[] buffer = new byte[1024];
			int read;
			while ((read = ais.read(buffer)) != -1)
				checksumDigester.update(buffer, 0, read);
		}
		catch (IOException ioe) {
			System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while computing document checksum.");
			ioe.printStackTrace(System.out); // should not happen, but Java don't know ...
			return Gamta.getAnnotationID(); // use random value so a document is regarded as new
		}
		byte[] checksumBytes = checksumDigester.digest();
		String checksum = new String(RandomByteSource.getHexCode(checksumBytes));
//		System.out.println("Checksum computed in " + (System.currentTimeMillis() - start) + " ms: " + checksum);
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
}
