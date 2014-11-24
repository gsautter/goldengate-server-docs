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
package de.uka.ipd.idaho.goldenGateServer.dic;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.TreeSet;

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
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.dic.DicDocumentImporter.ImportDocument;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;

/**
 * GoldenGATE Document Import Controler (DIC) is responsible for periodically
 * importing documents from remote sources into the local GoldenGATE server's
 * DIO. Getting the documents from their sources is the responsibility of
 * Document Importer plugins. DIC periodically asks these plugins to get their
 * documents and them imports them into DIO under a user name provided by the
 * source. DIC automatically keeps track of which documents have been imported;
 * plugins can ask this.
 * 
 * @author sautter
 */
public class GoldenGateDIC extends AbstractGoldenGateServerComponent implements LiteratureConstants {
	
	private static final String DOCUMENT_IMPORT_TABLE_NAME = "GgDicImports";
	
	private static final String DOCUMENT_SOURCE_NAME_ATTRIBUTE = "docSource";
	private static final int DOCUMENT_SOURCE_NAME_LENGTH = 32;
	private static final String DOCUMENT_IMPORT_TIME_ATTRIBUTE = "importTime";
	
	private static final int milliSecondsPerDay = (24 * 60 * 60 * 1000);
	private static final int minRetryInterval = (10 * 60 * 1000);
	private static final int minBetweenImportsInterval = (60 * 60 * 1000);
	private static final int defaultImporterTimeout = (10 * 60 * 1000);
	
	private IoProvider io;
	
	private GoldenGateDIO dio;
	
	private TreeMap importers = new TreeMap();
	
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
		td.addColumn(DOCUMENT_IMPORT_TIME_ATTRIBUTE, TableDefinition.BIGINT_DATATYPE, 0);
		if (!this.io.ensureTable(td, true))
			throw new RuntimeException("GoldenGateDIC: Cannot work without database access.");
		
		//	index import table
		this.io.indexColumn(DOCUMENT_IMPORT_TABLE_NAME, DOCUMENT_SOURCE_NAME_ATTRIBUTE);
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
		this.loadImporters(false);
	}
	
	private synchronized void loadImporters(boolean isReload) {
		//	if reload, shut down importers
		if (isReload)
			this.shutdownImporters();
		
		//	load importers
		System.out.println("Loading importers ...");
		Object[] importers = GamtaClassLoader.loadComponents(
				dataPath,
				DicDocumentImporter.class, 
				new ComponentInitializer() {
					public void initialize(Object component, String componentJarName) throws Exception {
						File dataPath = new File(GoldenGateDIC.this.dataPath, (componentJarName.substring(0, (componentJarName.length() - 4)) + "Data"));
						if (!dataPath.exists()) dataPath.mkdir();
						DicDocumentImporter importer = ((DicDocumentImporter) component);
						System.out.println(" - initializing importer " + importer.getName() + " ...");
						importer.setDataPath(dataPath);
						importer.setParent(GoldenGateDIC.this);
						importer.init();
						System.out.println(" - importer " + importer.getName() + " initialized");
					}
				});
		System.out.println("Importers loaded");
		
		//	store importers
		for (int i = 0; i < importers.length; i++)
			this.importers.put(((DicDocumentImporter) importers[i]).getName(), importers[i]);
		System.out.println("Importers registered");
		
		//	start timer thread
		System.out.println("Starting import service ...");
		synchronized (this.importers) {
			this.importService = new ImportService();
			this.importService.start();
			try {
				this.importers.wait();
			} catch (InterruptedException ie) {}
		}
		System.out.println("Import service started");
	}
	
	private synchronized void shutdownImporters() {
		System.out.println("Shutting down import service ...");
		this.importService.shutdown();
		this.importService = null;
		System.out.println("Import service shut down");
		
		System.out.println("Finalizing importers");
		for (Iterator iit = this.importers.keySet().iterator(); iit.hasNext();) {
			String importerName = ((String) iit.next());
			DicDocumentImporter importer = ((DicDocumentImporter) this.importers.get(importerName));
			importer.exit();
		}
		this.importers.clear();
		System.out.println("Importers finalized");
		
		this.importedDocumentIdCache.clear();
		System.out.println("Cache cleared");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		this.shutdownImporters();
		
		//	disconnect from database
		this.io.close();
	}
	
	private ImportService importService = null;
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
				System.out.println("Doing import from " + importer.getName());
				
				//	monitor running importer thread
				while (this.keepRunning && it.isAlive()) {
					
					//	sleep for 10 seconds
					try {
						Thread.sleep(1000 * 10);
					} catch (InterruptedException ie) {}
					
					//	time since last import thread activity report exceeds timeout plus slave communication tolerance ==> kill import
					if ((it.lastActivity + importerTimeout + (importerTimeout / 10)) < System.currentTimeMillis()) {
						it.keepRunning = false;
						System.out.println("Import in sub class call for over 10 minutes, now at:");
						StackTraceElement[] itStes = it.getStackTrace();
						for (int e = 0; e < itStes.length; e++)
							System.out.println("  " + itStes[e].toString());
						it.interrupt();
						System.out.println("Import from " + importer.getName() + " abandoned due to timeout");
						break;
					}
					
					//	we're being shut down, kill import & terminate
					else if (!this.keepRunning) {
						it.keepRunning = false;
						it.interrupt();
						System.out.println("Import from " + importer.getName() + " abandoned due to shutdown");
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
		ImportThread(ImportService parent, DicDocumentImporter importer) {
			this.parent = parent;
			this.importer = importer;
		}
		public void run() {
			try {
				System.out.println("Importing from " + this.importer.getName());
				this.importer.setLastImportStart();
				
				//	fetch all new document IDs first
				this.importer.setLastImportAttempt();
				ImportDocument[] ids = this.importer.getImportDocuments();
				this.lastActivity = System.currentTimeMillis();
				System.out.println(" - got " + ids.length + " documents to import");
				
				//	fetch previously imported documents from database
				TreeSet imported = getImportedDocumentIDs(this.importer.getName());
				System.out.println(" - got " + imported.size() + " documents imported before");
				
				//	importer component fetch documents one by one
				for (int d = 0; this.parent.keepRunning && this.keepRunning && (d < ids.length); d++) try {
					System.out.println(" - importing " + ids[d].docId + " ...");
					
					//	we'dealt with seen this one
					if (imported.contains(ids[d].docId)) {
						this.lastActivity = System.currentTimeMillis();
						System.out.println("   - imported before, skipping");
						continue;
					}
					
					//	start document import
					DocumentImportThread dit = new DocumentImportThread(this.importer, ids[d]);
					dit.start();
					this.lastActivity = System.currentTimeMillis();
					System.out.println("   - getting document");
					
					//	monitor running importer thread
					boolean downloadFailed = false;
					while (this.keepRunning && this.parent.keepRunning && dit.isAlive()) {
						
						//	sleep for a seconds
						try {
							Thread.sleep(1000);
						} catch (InterruptedException ie) {}
						
						//	time since last import thread activity report exceeds timeout ==> abandon document download, and re-initialize importer
						if ((this.lastActivity + importerTimeout) < System.currentTimeMillis()) {
							System.out.println("   - document download in sub class call for over 10 minutes, now at:");
							StackTraceElement[] ditStes = dit.getStackTrace();
							for (int e = 0; e < ditStes.length; e++)
								System.out.println("  " + ditStes[e].toString());
							dit.interrupt();
							System.out.println("   - document download from " + this.importer.getName() + " abandoned due to timeout");
							this.importer.exit();
							this.importer.init();
							d--;
							downloadFailed = true;
							System.out.println("   - importer " + this.importer.getName() + " re-initialized, staring over");
							break;
						}
						
						//	we're being shut down, kill import
						else if (!this.keepRunning || !this.parent.keepRunning) {
							dit.interrupt();
							System.out.println("Import from " + importer.getName() + " abandoned due to shutdown");
							return;
						}
					}
					
					//	download failed
					if (downloadFailed)
						continue;
					
					//	could not get document for some reason
					if (dit.importDoc == null)
						continue;
					
					//	load document from importer cache
					DocumentRoot doc = GenericGamtaXML.readDocument(ids[d].docFile);
					doc.setAttribute(DOCUMENT_ID_ATTRIBUTE, ids[d].docId);
					doc.setDocumentProperty(DOCUMENT_ID_ATTRIBUTE, ids[d].docId);
					doc.setAttribute(GoldenGateDIO.DOCUMENT_NAME_ATTRIBUTE, ids[d].docName);
					System.out.println("   - got document, storing:");
					
					//	store document in DIO
					dio.uploadDocument(this.importer.getImportUserName(), doc, new EventLogger() {
						public void writeLog(String logEntry) {
							System.out.println("     - " + logEntry);
						}
					}, GoldenGateDIO.EXTERNAL_IDENTIFIER_MODE_IGNORE);
					System.out.println("   - document stored");
					
					//	remember document imported
					imported.add(ids[d].docId);
					rememberDocumentImported(ids[d].docId, this.importer.getName());
					this.importer.setLastImportComplete();
					System.out.println("   - import remembered");
					
					//	give a little time to the others
					if (this.keepRunning && this.parent.keepRunning) try {
						Thread.sleep(1000);
					} catch (InterruptedException ie) {}
				}
				catch (Exception e) {
					System.out.println("Error on getting update " + ids[d].docId + " from " + this.importer.getName() + " - " + e.getClass().getName() + " (" + e.getMessage() + ")");
					e.printStackTrace(System.out);
				}
			}
			
			//	looks as if sometimes importer breaks down due to Exceptions other than IOExceptions ==> catch them all
			catch (Exception e) {
				System.out.println("Error on getting updates from " + this.importer.getName() + " - " + e.getClass().getName() + " (" + e.getMessage() + ")");
				e.printStackTrace(System.out);
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
				this.importDoc = this.importer.importDocument(this.importDoc);
			}
			catch (Exception e) {
				System.out.println("Error on getting document from " + this.importer.getName() + " - " + e.getClass().getName() + " (" + e.getMessage() + ")");
				e.printStackTrace(System.out);
			}
		}
	}
	
	/**
	 * Check whether or not a document has been imported before.
	 * @param docId the ID of the document to check
	 * @param docSource the name of the document's source
	 * @return true if the document was imported before, false otherwise
	 */
	public boolean wasDocumentImported(String docId, String docSource) {
		return this.getImportedDocumentIDs(docSource).contains(docId);
	}
	
	private TreeMap importedDocumentIdCache = new TreeMap();
	private TreeSet getImportedDocumentIDs(String docSource) {
		TreeSet imported = ((TreeSet) this.importedDocumentIdCache.get(docSource));
		if (imported != null)
			return imported;
		
		imported = new TreeSet();
		this.importedDocumentIdCache.put(docSource, imported);
		
		SqlQueryResult sqr = null;
		String query = "SELECT " + DOCUMENT_ID_ATTRIBUTE +
				" FROM " + DOCUMENT_IMPORT_TABLE_NAME + 
				" WHERE " + DOCUMENT_SOURCE_NAME_ATTRIBUTE + " LIKE '" + EasyIO.sqlEscape(docSource) + "'" + 
				";";
		try {
			sqr = this.io.executeSelectQuery(query);
			while (sqr.next()) {
				String docId = sqr.getString(0);
				imported.add(docId);
			}
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateDIC: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while getting imported documents for '" + docSource + "'.");
			System.out.println("  query was " + query);
		}
		finally {
			if (sqr != null)
				sqr.close();
		}
		return imported;
	}
	
	private void rememberDocumentImported(String docId, String docSource) {
		String insertQuery = "INSERT INTO " + DOCUMENT_IMPORT_TABLE_NAME + " (" + 
				DOCUMENT_ID_ATTRIBUTE + ", " + DOCUMENT_SOURCE_NAME_ATTRIBUTE + ", " + DOCUMENT_IMPORT_TIME_ATTRIBUTE + 
				") VALUES (" +
				"'" + EasyIO.sqlEscape(docId) + "', '" + EasyIO.sqlEscape(docSource) + "', " + System.currentTimeMillis() + 
				");";
		try {
			this.io.executeUpdateQuery(insertQuery);
		}
		catch (SQLException sqle) {
			System.out.println("GoldenGateDIC: " + sqle.getClass().getName() + " (" + sqle.getMessage() + ") while remembering document import.");
			System.out.println("  Query was " + insertQuery);
		}
	}
	
	private static final String SET_IMPORTER_TIMEOUT_COMMAND = "setImporterTimeout";
	private static final String LIST_IMPORTERS_COMMAND = "list";
	private static final String RELOAD_IMPORTERS_COMMAND = "reload";
	private static final String IMPORT_COMMAND = "import";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList();
		ComponentAction ca;
		
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
							System.out.println(" Invalid import timeout " + arguments[0]);
						else {
							importerTimeout = timeout;
							System.out.println(" Import timeout set to " + arguments[0] + " minutes");
						}
					}
					catch (NumberFormatException nfe) {
						System.out.println(" Invalid import timeout " + arguments[0]);
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify the timeout as the only argument.");
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
					System.out.println(" There " + ((importers.size() == 1) ? "is" : "are") + " " + ((importers.size() == 0) ? "no" : ("" + importers.size())) + " importer" + ((importers.size() == 1) ? "" : "s") + " connected" + ((importers.size() == 0) ? "." : ":"));
					for (Iterator iit = importers.keySet().iterator(); iit.hasNext();) {
						String importerName = ((String) iit.next());
						DicDocumentImporter importer = ((DicDocumentImporter) importers.get(importerName));
						System.out.println(" - " + importerName + ": " + importer.getImportUserName());
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
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
						"- <importerName>: The name of the importer to run the import for"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 1) {
					DicDocumentImporter ddi = ((DicDocumentImporter) importers.get(arguments[0]));
					if (ddi == null)
						System.out.println(" No importer found for name " + arguments[0]);
					else {
						ddi.resetLastImport();
						System.out.println(" Starting import from " + arguments[0] + " at the next possible time");
					}
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify the name of the importer to run as the only argument.");
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
					loadImporters(true);
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
}
