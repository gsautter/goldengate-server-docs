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
package de.uka.ipd.idaho.goldenGateServer.dic;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.TreeMap;

import de.uka.ipd.idaho.easyIO.EasyIO;
import de.uka.ipd.idaho.easyIO.IoProvider;
import de.uka.ipd.idaho.easyIO.SqlQueryResult;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.ComponentInitializer;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.AsynchronousWorkQueue;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerActivityLogger;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.dic.DicDocumentImporter.ImportDocument;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DuplicateExternalIdentifierException;

/**
 * GoldenGATE Document Import Controller (DIC) is responsible for periodically
 * importing documents from remote sources into the local GoldenGATE server's
 * DIO. Getting the documents from their sources is the responsibility of
 * Document Importer plug-ins. DIC periodically asks these plug-ins to get their
 * documents and them imports them into DIO under a user name provided by the
 * source. DIC automatically keeps track of which documents have been imported;
 * plug-ins can ask this.
 * 
 * @author sautter
 */
public class GoldenGateDIC extends AbstractGoldenGateServerComponent implements LiteratureConstants {
	
	private static final String DOCUMENT_IMPORT_TABLE_NAME = "GgDicImports";
	
	private static final String DOCUMENT_SOURCE_NAME_ATTRIBUTE = "docSource";
	private static final int DOCUMENT_SOURCE_NAME_LENGTH = 32;
	private static final String DOCUMENT_IMPORT_ID_ATTRIBUTE = "importId";
	private static final String DOCUMENT_IMPORT_TIME_ATTRIBUTE = "importTime";
	
	private static final int milliSecondsPerDay = (24 * 60 * 60 * 1000);
	private static final int minRetryInterval = (10 * 60 * 1000);
	private static final int minBetweenImportsInterval = (60 * 60 * 1000);
	private static final int defaultImporterTimeout = (10 * 60 * 1000);
	
	private IoProvider io;
	
	private GoldenGateDIO dio;
	
	private TreeMap importers = new TreeMap();
	private ImportThread[] activeImporter = {null};
	
	private int importerTimeout = defaultImporterTimeout;
	
	/**
	 * Constructor passing 'DIC' as the letter code to super constructor
	 */
	public GoldenGateDIC() {
		super("DIC");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	get and check database connection
		this.io = this.host.getIoProvider();
		if (!this.io.isJdbcAvailable())
			throw new RuntimeException("GoldenGateDIC: Cannot work without database access.");
		
		//	create/update document import table
		TableDefinition td = new TableDefinition(DOCUMENT_IMPORT_TABLE_NAME);
		td.addColumn(DOCUMENT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DOCUMENT_SOURCE_NAME_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, DOCUMENT_SOURCE_NAME_LENGTH);
		td.addColumn(DOCUMENT_IMPORT_ID_ATTRIBUTE, TableDefinition.VARCHAR_DATATYPE, 32);
		td.addColumn(DOCUMENT_IMPORT_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		if (!this.io.ensureTable(td, true))
			throw new RuntimeException("GoldenGateDIC: Cannot work without database access.");
		
		//	index import table
		this.io.indexColumn(DOCUMENT_IMPORT_TABLE_NAME, DOCUMENT_ID_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_IMPORT_TABLE_NAME, DOCUMENT_SOURCE_NAME_ATTRIBUTE);
		this.io.indexColumn(DOCUMENT_IMPORT_TABLE_NAME, DOCUMENT_IMPORT_ID_ATTRIBUTE);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	get document IO server
		this.dio = ((GoldenGateDIO) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateDIO.class.getName()));
		
		//	check success
		if (this.dio == null) throw new RuntimeException(GoldenGateDIO.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		this.loadImporters(this);
	}
	
	private synchronized void loadImporters(final GoldenGateServerActivityLogger log) {
		
		//	if reload, shut down importers
		if (log != this)
			this.shutdownImporters(log);
		
		//	load importers
		log.logInfo("Loading importers ...");
		Object[] importers = GamtaClassLoader.loadComponents(
				dataPath,
				DicDocumentImporter.class, 
				new ComponentInitializer() {
					public void initialize(Object component, String componentJarName) throws Exception {
						File dataPath = new File(GoldenGateDIC.this.dataPath, (componentJarName.substring(0, (componentJarName.length() - 4)) + "Data"));
						if (!dataPath.exists()) dataPath.mkdir();
						DicDocumentImporter importer = ((DicDocumentImporter) component);
						log.logInfo(" - initializing importer " + importer.getName() + " ...");
						importer.setDataPath(dataPath);
						importer.setParent(GoldenGateDIC.this);
						importer.init();
						log.logInfo(" - importer " + importer.getName() + " initialized");
					}
				});
		log.logInfo("Importers loaded");
		
		//	store importers
		for (int i = 0; i < importers.length; i++)
			this.importers.put(((DicDocumentImporter) importers[i]).getName(), importers[i]);
		log.logInfo("Importers registered");
		
		//	start timer thread
		log.logInfo("Starting import service ...");
		synchronized (this.importers) {
			this.importService = new ImportService();
			this.importService.start();
			try {
				this.importers.wait();
			} catch (InterruptedException ie) {}
			this.importMonitor = new AsynchronousWorkQueue("DicImportService") {
				public String getStatus() {
					ImportThread it = activeImporter[0];
					return (this.name + ": " + ((it == null) ? "no import running at this time" : ("currently importing from " + it.importer.getName() + ", " + it.queueSize() + " documents pending")));
				}
			};
		}
		log.logInfo("Import service started");
	}
	
	private synchronized void shutdownImporters(GoldenGateServerActivityLogger log) {
		log.logInfo("Shutting down import service ...");
		this.importMonitor.dispose();
		this.importService.shutdown();
		this.importService = null;
		log.logInfo("Import service shut down");
		
		log.logInfo("Finalizing importers");
		for (Iterator iit = this.importers.keySet().iterator(); iit.hasNext();) {
			String importerName = ((String) iit.next());
			DicDocumentImporter importer = ((DicDocumentImporter) this.importers.get(importerName));
			importer.exit();
		}
		this.importers.clear();
		log.logInfo("Importers finalized");
		
		this.importedIdCache.clear();
		this.importedDocumentIdCache.clear();
		log.logInfo("Cache cleared");
	}
//	
//	private void logImporterInfo(ComponentActionConsole cac, String message) {
//		if (cac == null)
//			System.out.println(message);
//		else cac.reportResult(message);
//	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		this.shutdownImporters(null);
		
		//	disconnect from database
		this.io.close();
	}
	
	private ImportService importService = null;
	private AsynchronousWorkQueue importMonitor = null;
	private class ImportService extends Thread {
		boolean keepRunning = true;
		public void run() {
			
			//	wake up creator thread
			synchronized (importers) {
				importers.notify();
			}
			
			//	run until shutdown() is called
			while (this.keepRunning) {
				
				//	check if import waiting
				DicDocumentImporter importer = null;
				long currentTime = System.currentTimeMillis();
				
				//	check if some importer due
				synchronized (importers) {
					long minLastImportStart = Long.MAX_VALUE;
					
					//	find 'most due' importer
					for (Iterator iit = importers.values().iterator(); (importer == null) && iit.hasNext();) {
						DicDocumentImporter ddi = ((DicDocumentImporter) iit.next());
						
						//	we've already run an import in the last 24 hours
						if (currentTime < (ddi.getLastImportStart() + milliSecondsPerDay))
							continue;
						
						//	we've tried in vain less than 10 minutes ago
						if (currentTime < (ddi.getLastImportAttempt() + minRetryInterval))
							continue;
						
						//	last successful import was less than an hour ago
						if (currentTime < (ddi.getLastImportComplete() + minBetweenImportsInterval))
							continue;
						
						//	we've got more pressing imports to deal with
						if (ddi.getLastImportStart() > minLastImportStart)
							continue;
						
						//	this importer looks about due
						importer = ddi;
						minLastImportStart = ddi.getLastImportStart();
					}
				}
				
				//	we're being shut down
				if (!this.keepRunning)
					return;
				
				//	no imports due for now, come back in a minute
				if (importer == null) {
					try {
						Thread.sleep(1000 * 60);
					} catch (InterruptedException ie) {}
					if (this.keepRunning)
						continue;
					else return;
				}
				
				//	start slave importer thread
				ImportThread it = new ImportThread(this, importer);
				it.start();
				logInfo("Doing import from " + importer.getName());
				
				//	monitor running importer thread
				while (this.keepRunning && it.isAlive()) {
					
					//	sleep for 10 seconds
					try {
						Thread.sleep(1000 * 10);
					} catch (InterruptedException ie) {}
					
					//	time since last import thread activity report exceeds timeout plus slave communication tolerance ==> kill import
					if ((it.lastActivity + importerTimeout + (importerTimeout / 10)) < System.currentTimeMillis()) {
						it.keepRunning = false;
						logWarning("Import in sub class call for over " + (importerTimeout / (1000 * 60)) + " minutes, now at:");
						StackTraceElement[] itStes = it.getStackTrace();
						for (int e = 0; e < itStes.length; e++)
							logWarning("  " + itStes[e].toString());
						it.interrupt();
						logWarning("Import from " + importer.getName() + " abandoned due to timeout");
						break;
					}
					
					//	we're being shut down, kill import & terminate
					else if (!this.keepRunning) {
						it.keepRunning = false;
						it.interrupt();
						logWarning("Import from " + importer.getName() + " abandoned due to shutdown");
						return;
					}
				}
			}
		}
		void shutdown() {
			this.keepRunning = false;
			this.interrupt();
			try {
				this.join();
			} catch (InterruptedException ie) {}
		}
	}
	
	private class ImportThread extends Thread {
		final ImportService parent;
		final DicDocumentImporter importer;
		boolean keepRunning = true;
		long lastActivity = System.currentTimeMillis();
		private LinkedList queue = new LinkedList();
		
		ImportThread(ImportService parent, DicDocumentImporter importer) {
			this.parent = parent;
			this.importer = importer;
		}
		
		int queueSize() {
			return this.queue.size();
		}
		
		void clearQueue() {
			this.queue.clear();
		}
		
		private void fillQueue() throws Exception {
			
			//	fetch all new document IDs first
			this.importer.setLastImportAttempt();
			ImportDocument[] ids = this.importer.getImportDocuments(this.importer.getImportParameters());
			this.lastActivity = System.currentTimeMillis();
			logInfo(" - got " + ids.length + " documents to import");
			
			//	fetch previously imported documents from database
			HashMap imported = getImportedIDs(this.importer.getName());
			logInfo(" - got " + imported.size() + " documents imported before");
			
			//	put pending imports in queue
			for (int d = 0; d < ids.length; d++) {
				if (!imported.containsKey(ids[d].importId))
					this.queue.addLast(ids[d]);
			}
			this.lastActivity = System.currentTimeMillis();
			logInfo(" - got " + this.queue.size() + " documents left to import");
		}
		
		private ImportDocument downloadDocument(ImportDocument id) throws Exception {
			logInfo(" - importing " + id.importId + " ...");
			
			//	start document import
			DocumentImportThread dit = new DocumentImportThread(this.importer, id);
			dit.start();
			this.lastActivity = System.currentTimeMillis();
			logInfo("   - getting document");
			
			//	monitor running importer thread
			while (this.keepRunning && this.parent.keepRunning && dit.isAlive()) {
				
				//	sleep for a seconds
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {}
				
				//	time since last import thread activity report exceeds timeout ==> abandon document download, and re-initialize importer
				if ((this.lastActivity + importerTimeout) < System.currentTimeMillis()) {
					logWarning("   - document download in sub class call for over " + (importerTimeout / (1000 * 60)) + " minutes, now at:");
					StackTraceElement[] ditStes = dit.getStackTrace();
					for (int e = 0; e < ditStes.length; e++)
						logWarning("  " + ditStes[e].toString());
					dit.interrupt();
					logWarning("   - document download from " + this.importer.getName() + " abandoned due to timeout");
					this.importer.exit();
					this.importer.init();
					logWarning("   - importer " + this.importer.getName() + " re-initialized, staring over");
					return null;
				}
				
				//	we're being shut down, kill import
				else if (!this.keepRunning || !this.parent.keepRunning) {
					dit.interrupt();
					logWarning("Import from " + importer.getName() + " abandoned due to shutdown");
					return null;
				}
			}
			
			//	return cache file
			return dit.importDoc;
		}
		
		private void storeDocument(ImportDocument id) throws Exception {
			
			//	load document from importer cache
			DocumentRoot doc = GenericGamtaXML.readDocument(id.docFile);
			
			//	default document ID to import ID
			String docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE, id.importId));
			doc.setAttribute(DOCUMENT_ID_ATTRIBUTE, docId);
			doc.setDocumentProperty(DOCUMENT_ID_ATTRIBUTE, docId);
			doc.setAttribute(GoldenGateDIO.DOCUMENT_NAME_ATTRIBUTE, id.docName);
			logInfo("   - got document, storing:");
			
			//	store document in DIO
			boolean attemptUpdate = false;
			try {
				dio.uploadDocument(this.importer.getImportUserName(), doc, new EventLogger() {
					public void writeLog(String logEntry) {
						logInfo("     - " + logEntry);
					}
				}, GoldenGateDIO.EXTERNAL_IDENTIFIER_MODE_CHECK);
				logInfo("   - document stored");
			}
			catch (DuplicateExternalIdentifierException deie) {
				logWarning("   - document stored before (1): " + deie.getMessage());
			}
			catch (IOException ioe) {
				if (ioe.getMessage().startsWith("Document already exists,")) {
					logInfo("   - document stored before (2): " + ioe.getMessage());
					attemptUpdate = true;
				}
				else throw ioe;
			}
			
			//	try and update document in DIO if importer was last to touch it (there should be a reason for the re-import)
			if (attemptUpdate) {
				Properties docAttributes = dio.getDocumentAttributes(docId);
				if (this.importer.getImportUserName().equals(docAttributes.getProperty(GoldenGateDIO.UPDATE_USER_ATTRIBUTE))) try {
					doc.setAttribute(GoldenGateDIO.CHECKIN_USER_ATTRIBUTE, docAttributes.getProperty(GoldenGateDIO.CHECKIN_USER_ATTRIBUTE));
					doc.setAttribute(GoldenGateDIO.CHECKIN_TIME_ATTRIBUTE, docAttributes.getProperty(GoldenGateDIO.CHECKIN_TIME_ATTRIBUTE));
					dio.updateDocument(this.importer.getImportUserName(), docId, doc, new EventLogger() {
						public void writeLog(String logEntry) {
							logInfo("     - " + logEntry);
						}
					}, GoldenGateDIO.EXTERNAL_IDENTIFIER_MODE_CHECK);
					logInfo("   - document updated");
					dio.releaseDocument(this.importer.getImportUserName(), docId);
					logInfo("   - document released");
				}
				catch (DuplicateExternalIdentifierException deie) {
					logWarning("   - document stored before (3): " + deie.getMessage());
				}
			}
			
			//	remember document imported
			rememberDocumentImported(docId, this.importer.getName(), id.importId);
			this.importer.setLastImportComplete();
			logInfo("   - import remembered");
		}
		
		public void run() {
			try {
				synchronized (activeImporter) {
					activeImporter[0] = this;
				}
				logInfo("Importing from " + this.importer.getName());
				this.importer.setLastImportStart();
				
				//	fill importer queue
				this.fillQueue();
				
				//	fetch documents one by one
				for (ImportDocument id = null; this.parent.keepRunning && this.keepRunning && (this.queue.size() != 0);) try {
					id = ((ImportDocument) this.queue.removeFirst());
					id = this.downloadDocument(id);
					
					//	could not get document for some reason, move this one to the end
					if (id == null)
						continue;
					
					//	store document
					this.storeDocument(id);
					
					//	give a little time to the others
					if (this.keepRunning && this.parent.keepRunning) try {
						Thread.sleep(1000);
					} catch (InterruptedException ie) {}
				}
				
				//	looks as if sometimes importer breaks down due to Exceptions other than IOExceptions ==> catch them all
				catch (Exception e) {
					logError("Error on getting update " + id.importId + " from " + this.importer.getName() + " - " + e.getClass().getName() + " (" + e.getMessage() + ")");
					logError(e);
				}
			}
			
			//	looks as if sometimes importer breaks down due to Exceptions other than IOExceptions ==> catch them all
			catch (Exception e) {
				logError("Error on getting updates from " + this.importer.getName() + " - " + e.getClass().getName() + " (" + e.getMessage() + ")");
				logError(e);
			}
			
			//	unregister in parent, and reset importer arguments
			finally {
				synchronized (activeImporter) {
					activeImporter[0] = null;
				}
				this.importer.setImportParameters(null);
			}
		}
	}
	
	private class DocumentImportThread extends Thread {
		private DicDocumentImporter importer;
		private ImportDocument importDoc;
		DocumentImportThread(DicDocumentImporter importer, ImportDocument importDoc) {
			this.importer = importer;
			this.importDoc = importDoc;
		}
		public void run() {
			try {
				this.importDoc = this.importer.importDocument(this.importDoc, this.importer.getImportParameters());
			}
			catch (Exception e) {
				logError("Error on getting document from " + this.importer.getName() + " - " + e.getClass().getName() + " (" + e.getMessage() + ")");
				logError(e);
			}
		}
	}
	
	/**
	 * Check whether or not a document has been imported before.
	 * @param importId the ID of the import / document source to check
	 * @param docSource the name of the document's source
	 * @return true if the document was imported before, false otherwise
	 */
	public boolean wasImported(String importId, String docSource) {
		return this.getImportedIDs(docSource).containsKey(importId);
	}
	
	private HashMap importedIdCache = new HashMap();
	private HashMap getImportedIDs(String docSource) {
		HashMap imported;
		synchronized (this.importedIdCache) {
			imported = ((HashMap) this.importedIdCache.get(docSource));
			if (imported != null)
				return imported;
			
			imported = new HashMap();
			this.importedIdCache.put(docSource, imported);
		}
		
		String query = "SELECT " + DOCUMENT_IMPORT_ID_ATTRIBUTE + ", " + DOCUMENT_ID_ATTRIBUTE +
				" FROM " + DOCUMENT_IMPORT_TABLE_NAME + 
				" WHERE " + DOCUMENT_SOURCE_NAME_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docSource) + "'" + 
				";";
		SqlQueryResult sqr = null;
		try {
			sqr = this.io.executeSelectQuery(query);
			while (sqr.next()) {
				String importId = sqr.getString(0);
				String docId = sqr.getString(1);
				imported.put(importId, docId);
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIC: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting imported for '" + docSource + "'.");
			this.logError("  query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		return imported;
	}
	
	/**
	 * Check whether or not a document has been imported before.
	 * @param docId the ID of the document to check
	 * @param docSource the name of the document's source
	 * @return true if the document was imported before, false otherwise
	 */
	public boolean wasDocumentImported(String docId, String docSource) {
		return this.getImportedDocumentIDs(docSource).containsKey(docId);
	}
	
	private HashMap importedDocumentIdCache = new HashMap();
	private HashMap getImportedDocumentIDs(String docSource) {
		HashMap imported;
		synchronized (this.importedDocumentIdCache) {
			imported = ((HashMap) this.importedDocumentIdCache.get(docSource));
			if (imported != null)
				return imported;
			
			imported = new HashMap();
			this.importedDocumentIdCache.put(docSource, imported);
		}
		
		SqlQueryResult sqr = null;
		String query = "SELECT " + DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_IMPORT_ID_ATTRIBUTE +
				" FROM " + DOCUMENT_IMPORT_TABLE_NAME + 
				" WHERE " + DOCUMENT_SOURCE_NAME_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docSource) + "'" + 
				";";
		try {
			sqr = this.io.executeSelectQuery(query);
			while (sqr.next()) {
				String docId = sqr.getString(0);
				String importId = sqr.getString(1);
				imported.put(docId, importId);
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIC: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting imported documents for '" + docSource + "'.");
			this.logError("  query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		return imported;
	}
	
	private void rememberDocumentImported(String docId, String docSource, String importId) {
		String insertQuery = "INSERT INTO " + DOCUMENT_IMPORT_TABLE_NAME + " (" +
				DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_SOURCE_NAME_ATTRIBUTE + ", " + DOCUMENT_IMPORT_ID_ATTRIBUTE + ", " + DOCUMENT_IMPORT_TIME_ATTRIBUTE + 
				") VALUES (" +
				"'" + EasyIO.sqlEscape(docId) + "', '" + EasyIO.sqlEscape(docSource) + "', '" + EasyIO.sqlEscape(importId) + "', " + System.currentTimeMillis() + 
				");";
		try {
			this.io.executeUpdateQuery(insertQuery);
			synchronized (this.importedIdCache) {
				if (this.importedIdCache.containsKey(docSource))
					((HashMap) this.importedIdCache.get(docSource)).put(importId, docId);
			}
			synchronized (this.importedDocumentIdCache) {
				if (this.importedDocumentIdCache.containsKey(docSource))
					((HashMap) this.importedDocumentIdCache.get(docSource)).put(docId, importId);
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIC: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while remembering document import.");
			this.logError("  Query was " + insertQuery);
		}
	}
	
	/**
	 * Data describing the import of a document
	 * 
	 * @author sautter
	 */
	public static class ImportData {
		
		/** the ID of the imported document */
		public final String docId;
		
		/** the source the document was imported from */
		public final String docSource;
		
		/** the ID of the import / document source */
		public final String importId;
		
		/** the time the document was imported */
		public final long importTime;
		
		ImportData(String docId, String docSource, String importId, long importTime) {
			this.docId = docId;
			this.docSource = docSource;
			this.importId = importId;
			this.importTime = importTime;
		}
	}
	
	/**
	 * Retrieve the data on a document import.
	 * @param docId the ID of the document
	 * @return the import data
	 */
	public ImportData getImportDataForDocument(String docId) {
		SqlQueryResult sqr = null;
		String query = "SELECT " + DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_SOURCE_NAME_ATTRIBUTE + ", " + DOCUMENT_IMPORT_ID_ATTRIBUTE + ", " + DOCUMENT_IMPORT_TIME_ATTRIBUTE +
				" FROM " + DOCUMENT_IMPORT_TABLE_NAME + 
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" + 
				";";
		try {
			sqr = this.io.executeSelectQuery(query);
			if (sqr.next()) {
				docId = sqr.getString(0);
				String docSource = sqr.getString(1);
				String importId = sqr.getString(2);
				long importTime = sqr.getLong(3);
				return new ImportData(docId, docSource, importId, importTime);
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIC: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting import data for document '" + docId + "'.");
			this.logError("  query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		return null;
	}
	
	/**
	 * Retrieve the data on a document import.
	 * @param importId the ID of the import
	 * @return the import data
	 */
	public ImportData getDataForImport(String importId) {
		SqlQueryResult sqr = null;
		String query = "SELECT " + DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_SOURCE_NAME_ATTRIBUTE + ", " + DOCUMENT_IMPORT_ID_ATTRIBUTE + ", " + DOCUMENT_IMPORT_TIME_ATTRIBUTE +
				" FROM " + DOCUMENT_IMPORT_TABLE_NAME + 
				" WHERE " + DOCUMENT_IMPORT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(importId) + "'" + 
				";";
		try {
			sqr = this.io.executeSelectQuery(query);
			if (sqr.next()) {
				String docId = sqr.getString(0);
				String docSource = sqr.getString(1);
				importId = sqr.getString(2);
				long importTime = sqr.getLong(3);
				return new ImportData(docId, docSource, importId, importTime);
			}
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIC: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting data for import '" + importId + "'.");
			this.logError("  query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		
		return null;
	}
	
	private void forgetDocumentImported(String docId) {
		String deleteQuery = "DELETE FROM " + DOCUMENT_IMPORT_TABLE_NAME +
				" WHERE " + DOCUMENT_ID_ATTRIBUTE + " = '" + EasyIO.sqlEscape(docId) + "'" +
				";";
		try {
			this.io.executeUpdateQuery(deleteQuery);
		}
		catch (SQLException sqle) {
			this.logError("GoldenGateDIC: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while forgetting document import.");
			this.logError("  Query was " + deleteQuery);
		}
	}
	
	private static final String SET_IMPORTER_TIMEOUT_COMMAND = "setImporterTimeout";
	private static final String LIST_IMPORTERS_COMMAND = "list";
	private static final String RELOAD_IMPORTERS_COMMAND = "reload";
	private static final String IMPORT_COMMAND = "import";
	private static final String IMPORT_HISTORY_COMMAND = "history";
	private static final String FORGET_IMPORT_COMMAND = "forget";
	private static final String QUEUE_SIZE_COMMAND = "queueSize";
	private static final String CLEAR_QUEUE_COMMAND = "clearQueue";
	private static final DateFormat importTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
		//	report size of import queue
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return QUEUE_SIZE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						QUEUE_SIZE_COMMAND,
						"Report the number of document imports pending in this DIC."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					synchronized (activeImporter) {
						if (activeImporter[0] == null)
							this.reportResult(" No import running at the moment");
						else this.reportResult(" Running import from " + activeImporter[0].importer.getName() + ", " + activeImporter[0].queueSize() + " documents pending");
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	clear import queue
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return CLEAR_QUEUE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						CLEAR_QUEUE_COMMAND,
						"Clear the queue of document imports pending in this DIC."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					synchronized (activeImporter) {
						if (activeImporter[0] == null)
							this.reportResult(" No import running at the moment");
						else {
							activeImporter[0].clearQueue();
							this.reportResult(" Queue of imports from " + activeImporter[0].importer.getName() + " cleared");
						}
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	set importer timeout (in minutes)
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return SET_IMPORTER_TIMEOUT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						SET_IMPORTER_TIMEOUT_COMMAND + " <timeout>",
						"Set the timeout for importing a single document:",
						"- <timeout>: the timeout in minutes"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					try {
						int timeout = (1000 * 60 * Integer.parseInt(arguments[0]));
						if (timeout < 1)
							this.reportError(" Invalid import timeout " + arguments[0]);
						else {
							importerTimeout = timeout;
							this.reportResult(" Import timeout set to " + arguments[0] + " minutes");
						}
					}
					catch (NumberFormatException nfe) {
						this.reportError(" Invalid import timeout " + arguments[0]);
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the timeout as the only argument.");
			}
		};
		cal.add(ca);
		
		//	list importers
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_IMPORTERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_IMPORTERS_COMMAND,
						"List all importers currently installed in this DIC."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					this.reportResult(" There " + ((importers.size() == 1) ? "is" : "are") + " " + ((importers.size() == 0) ? "no" : ("" + importers.size())) + " importer" + ((importers.size() == 1) ? "" : "s") + " connected" + ((importers.size() == 0) ? "." : ":"));
					for (Iterator iit = importers.keySet().iterator(); iit.hasNext();) {
						String importerName = ((String) iit.next());
						DicDocumentImporter importer = ((DicDocumentImporter) importers.get(importerName));
						this.reportResult(" - " + importerName + ": " + importer.getImportUserName());
						String[] paramExplanations = importer.getParameterExplanations();
						if (paramExplanations != null) {
							for (int p = 0; p < paramExplanations.length; p++)
								this.reportResult("   - " + paramExplanations[p]);
						}
					}
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	force instant import
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_COMMAND + " <importerName>",
						"Run an import for a specific importer immediately:",
						"- <importerName>: The name of the importer to run the import for",
						"- <importParams>: any additional parameters for the importer (optional)",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at least the name of the importer to run.");
					return;
				}
				DicDocumentImporter ddi = ((DicDocumentImporter) importers.get(arguments[0]));
				if (ddi == null) {
					this.reportError(" No importer found for name " + arguments[0]);
					return;
				}
				String[] ddiParams = null;
				if (arguments.length > 1) {
					ddiParams = new String[arguments.length - 1];
					System.arraycopy(arguments, 1, ddiParams, 0, ddiParams.length);
				}
				ddi.resetLastImport(ddiParams);
				this.reportResult(" Starting import from " + arguments[0] + " at the next possible time");
			}
		};
		cal.add(ca);
		
		//	reload importers
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return RELOAD_IMPORTERS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						RELOAD_IMPORTERS_COMMAND,
						"Reload the importers currently installed in this DIC."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					/*
					 * TODOne, as reloading importers will shut down importer
					 * thread first, which will not return before running import
					 * finished
					 * figure out how to prevent unloading and reloading importers
					 * that are running
					 */
					loadImporters(this);
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	forget having imported some document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return IMPORT_HISTORY_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						IMPORT_HISTORY_COMMAND + " <docId>",
						"Show the import history of a document:",
						"- <docId>: the ID of the document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					ImportData id = getImportDataForDocument(arguments[0]);
					if (id == null)
						this.reportError("Invalid document ID '" + arguments[0] + "'");
					else this.reportResult("Found document imported from " + id.docSource + " as " + id.importId + " at " + importTimeFormat.format(new Date(id.importTime)));
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);
		
		//	forget having imported some document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return FORGET_IMPORT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						FORGET_IMPORT_COMMAND + " <docId>",
						"Forget importing a document (e.g. to make way for a re-import):",
						"- <docId>: the ID of the document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					ImportData id = getImportDataForDocument(arguments[0]);
					if (id == null) {
						this.reportError("Invalid document ID '" + arguments[0] + "'");
						return;
					}
					this.reportResult("Found document imported from " + id.docSource + " as " + id.importId + " at " + importTimeFormat.format(new Date(id.importTime)));
					
					//	clear cache file
					DicDocumentImporter importer = ((DicDocumentImporter) importers.get(id.docSource));
					if (importer == null)
						this.reportError(" - could not find importer to clear cache");
					else {
						File importCacheFile = importer.getImportDocumentCacheFile(id.importId);
						if (importCacheFile.exists())
							importCacheFile.renameTo(new File(importCacheFile.getAbsolutePath() + "." + System.currentTimeMillis() + ".old"));
						this.reportResult(" - cache cleared");
					}
					
					//	clear import table
					forgetDocumentImported(id.docId);
					this.reportResult(" - database cleared");
					
					//	clear ID cache
					synchronized (importedIdCache) {
						if (importedIdCache.containsKey(id.docSource))
							((HashMap) importedIdCache.get(id.docSource)).remove(id.importId);
					}
					synchronized (importedDocumentIdCache) {
						if (importedDocumentIdCache.containsKey(id.docSource))
							((HashMap) importedDocumentIdCache.get(id.docSource)).remove(id.docId);
					}
					this.reportResult(" - ID cache cleared");
				}
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID as the only argument.");
			}
		};
		cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
}
