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
 *     * Neither the name of the Universitaet Karlsruhe (TH) / KIT nor the
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
package de.uka.ipd.idaho.goldenGateServer.dpr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Arrays;

import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.easyIO.util.HashUtils.MD5;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousDataActionHandler;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveErrorRecorder;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveInstallerUtils;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveJob;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveProcessInterface;

/**
 * GoldenGATE Processor provides automated processing of XML documents stored
 * in a GoldenGATE DIO. Processing happens in a slave JVM.
 * 
 * @author sautter
 */
public class GoldenGateDPR extends AbstractGoldenGateServerComponent {
	private GoldenGateDIO dio;
	
	private String updateUserName;
	private int maxSlaveMemory;
	
	private File workingFolder;
	private File logFolder;
	private File cacheFolder;
	private AsynchronousDataActionHandler documentProcessor;
	
	private Process batchRun = null;
	private DprSlaveProcessInterface batchInterface = null;
	private String processingDocId = null;
	private long processingStart = -1;
	private String processorName = null;
	private long processorStart = -1;
	private String processingStep = null;
	private long processingStepStart = -1;
	private String processingInfo = null;
	private long processingInfoStart = -1;
	private int processingProgress = 0;
	
	private String ggConfigHost;
	private String ggConfigName;
	
	/** Constructor passing 'DPR' as the letter code to super constructor
	 */
	public GoldenGateDPR() {
		super("DPR");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 */
	protected void initComponent() {
		
		//	get default import user name
		this.updateUserName = this.configuration.getSetting("updateUserName", "GgDPR");
		
		//	get maximum memory for slave process
		try {
			this.maxSlaveMemory = Integer.parseInt(this.configuration.getSetting("maxSlaveMemory", "1024"));
		} catch (RuntimeException re) {}
		
		//	get working folder
		String workingFolderName = this.configuration.getSetting("workingFolderName", "Processor");
		while (workingFolderName.startsWith("./"))
			workingFolderName = workingFolderName.substring("./".length());
		this.workingFolder = (((workingFolderName.indexOf(":\\") == -1) && (workingFolderName.indexOf(":/") == -1) && !workingFolderName.startsWith("/")) ? new File(this.dataPath, workingFolderName) : new File(workingFolderName));
		this.workingFolder.mkdirs();
		
		//	get log folder
		String logFolderName = this.configuration.getSetting("logFolderName", "Logs");
		while (logFolderName.startsWith("./"))
			logFolderName = logFolderName.substring("./".length());
		this.logFolder = (((logFolderName.indexOf(":\\") == -1) && (logFolderName.indexOf(":/") == -1) && !logFolderName.startsWith("/")) ? new File(this.workingFolder, logFolderName) : new File(logFolderName));
		this.logFolder.mkdirs();
		
		//	get document transfer cache folder (RAM disc !!!)
		String cacheFolderName = this.configuration.getSetting("cacheFolderName", "Cache");
		while (cacheFolderName.startsWith("./"))
			cacheFolderName = cacheFolderName.substring("./".length());
		this.cacheFolder = (((cacheFolderName.indexOf(":\\") == -1) && (cacheFolderName.indexOf(":/") == -1) && !cacheFolderName.startsWith("/")) ? new File(this.dataPath, cacheFolderName) : new File(cacheFolderName));
		this.cacheFolder.mkdirs();
		
		//	load GG config host & name
		this.ggConfigHost = this.configuration.getSetting("ggConfigHost");
		this.ggConfigName = this.configuration.getSetting("ggConfigName");
		
		//	install GG slave JAR
		SlaveInstallerUtils.installSlaveJar("GgServerDprSlave.jar", this.dataPath, this.workingFolder, true);
		
		//	create asynchronous worker
		TableColumnDefinition[] argCols = {
			new TableColumnDefinition("DpName", TableDefinition.VARCHAR_DATATYPE, 128),
			new TableColumnDefinition("UserName", TableDefinition.VARCHAR_DATATYPE, 32),
		};
		this.documentProcessor = new AsynchronousDataActionHandler("Dpr", argCols, this, this.host.getIoProvider()) {
			protected void performDataAction(String dataId, String[] arguments) throws Exception {
//				processDocument(dataId, arguments[0]);
				String dpName = ((arguments[0].length() == 0) ? null : arguments[0]);
				String userName = (((arguments.length < 2) || (arguments[1].length() == 0)) ? null : arguments[1]);
				processDocument(dataId, dpName, userName);
			}
		};
		
		//	TODO get list of documents from DIO
		
		//	TODO release all documents we still hold the lock for
		
		//	set up collecting of errors from our slave processes
		String slaveErrorPath = this.host.getServerProperty("SlaveProcessErrorPath");
		if (slaveErrorPath != null)
			SlaveErrorRecorder.setErrorPath(slaveErrorPath);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	link up to IMS
		this.dio = ((GoldenGateDIO) this.host.getServerComponent(GoldenGateDIO.class.getName()));
		
		//	check success
		if (this.dio == null) throw new RuntimeException(GoldenGateDIO.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	start processing handler thread
		this.documentProcessor.start();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down processing handler thread
		this.documentProcessor.shutdown();
	}
	
	private static final String PROCESS_DOCUMENT_COMMAND = "process";
	private static final String PROCESS_STATUS_COMMAND = "procStatus";
	private static final String PROCESS_THREADS_COMMAND = "procThreads";
	private static final String PROCESS_THREAD_GROUPS_COMMAND = "procThreadGroups";
	private static final String PROCESS_STACK_COMMAND = "procStack";
	private static final String PROCESS_WAKE_COMMAND = "procWake";
	private static final String PROCESS_KILL_COMMAND = "procKill";
	private static final String LIST_PROCESSORS_COMMAND = "processors";
	private static final String QUEUE_SIZE_COMMAND = "queueSize";
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(this.documentProcessor.getActions()));
		ComponentAction ca;
		
		//	sort out generic scheduling actions (we need to validate and adjust parameters)
		for (int a = 0; a < cal.size(); a++) {
			ca = ((ComponentAction) cal.get(a));
			if ("scheduleAction".equals(ca.getActionCommand()))
				cal.remove(a--);
			else if ("enqueueAction".equals(ca.getActionCommand()))
				cal.remove(a--);
		}
		
		//	schedule processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_DOCUMENT_COMMAND + " <documentId> <processorName> <userName>",
						"Schedule a document for processing:",
						"- <documentId>: The ID of the document to process",
						"- <processorName>: The name of the document processor to use",
						"- <userName>: The name of the user to attribute processing to (optional)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 2)
					scheduleProcessing(arguments[0], arguments[1], null);
				else if (arguments.length == 3)
					scheduleProcessing(arguments[0], arguments[1], arguments[2]);
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID and processor name as the only arguments.");
			}
		};
		cal.add(ca);
		
		//	check processing status of a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_STATUS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_STATUS_COMMAND,
						"Show the status of a document that is processing"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				//	TODO list status of all running slaves
				//	TODO include slave IDs
				//	TODO list more detailed status of individual slave if ID specified
				long time = System.currentTimeMillis();
				this.reportResult("Processing document " + processingDocId + " (started " + (time - processingStart) + "ms ago)");
				this.reportResult(" - current processor is " + processorName + " (since " + (time - processorStart) + "ms, at " + processingProgress + "%)");
				this.reportResult(" - current step is " + processingStep + " (since " + (time - processingStepStart) + "ms)");
				this.reportResult(" - current info is " + processingInfo + " (since " + (time - processingInfoStart) + "ms)");
			}
		};
		cal.add(ca);
		
		//	list the threads of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_THREADS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_THREADS_COMMAND,
						"Show the threads of the batch processing a document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				batchInterface.setReportTo(this);
				batchInterface.listThreads();
				batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	list the thread groups of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_THREAD_GROUPS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_THREAD_GROUPS_COMMAND,
						"Show the thread groups of the batch processing a document"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length != 0) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				batchInterface.setReportTo(this);
				batchInterface.listThreadGroups();
				batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	check the stack of a batch processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_STACK_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_STACK_COMMAND + " <threadName>",
						"Show the stack trace of the batch processing a document:",
						"- <threadName>: The name of the thread whose stack to show (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length > 1) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify at most the name of the target thread.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				batchInterface.setReportTo(this);
				batchInterface.printThreadStack((arguments.length == 0) ? null : arguments[0]);
				batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	wake a batch processing a document, or a thread therein
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_WAKE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_WAKE_COMMAND + " <threadName>",
						"Wake the batch processing a document, or a thread therein:",
						"- <threadName>: The name of the thread to wake (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length > 1) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				batchInterface.setReportTo(this);
				batchInterface.wakeThread((arguments.length == 0) ? null : arguments[0]);
				batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	kill a batch processing a document, or a thread therein
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_KILL_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_KILL_COMMAND + " <threadName>",
						"Kill the batch processing a document, or a thread therein:",
						"- <threadName>: The name of the thread to kill (optional, omitting targets main thread)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length > 1) {
					this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
					return;
				}
				if (processingStart == -1) {
					this.reportResult("There is no document processing at the moment");
					return;
				}
				batchInterface.setReportTo(this);
				batchInterface.killThread((arguments.length == 0) ? null : arguments[0]);
				batchInterface.setReportTo(null);
			}
		};
		cal.add(ca);
		
		//	list available document processors
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return LIST_PROCESSORS_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						LIST_PROCESSORS_COMMAND + " <reload> <managers>",
						"List all available document processors:",
						"- <reload>: Set to '-r' to force reloading the list (optional)",
						"- <managers>: Set to '-m' to only show document processor managers (optional)"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				boolean forceReload = false;
				boolean managersOnly = false;
				String managerPrefix = null;
				for (int a = 0; a < arguments.length; a++) {
					if ("-r".equals(arguments[a]))
						forceReload = true;
					else if ("-m".equals(arguments[a]))
						managersOnly = true;
					else if (managerPrefix == null)
						managerPrefix = arguments[a];
					else managerPrefix = (managerPrefix + " " + arguments[a]);
				}
				//	TODO do this in separate thread to keep console responsive
				listDocumentProcessors(forceReload, managersOnly, managerPrefix, this);
			}
		};
		cal.add(ca);
		
		//	check processing queue
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return QUEUE_SIZE_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						QUEUE_SIZE_COMMAND,
						"Show current size of processing queue, i.e., number of documents waiting to be processed."
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0)
					this.reportResult(documentProcessor.getDataActionsPending() + " documents waiting to be processed.");
				else this.reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	finally ...
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	private String[][] docProcessorList = null;
	private void listDocumentProcessors(boolean forceReload, boolean managersOnly, String managerPrefix, ComponentActionConsole cac) {
		
		//	load document processor list on demand
		if ((this.docProcessorList == null) || forceReload) try {
			this.docProcessorList = this.loadDocumentProcessorList(cac);
		}
		catch (IOException ioe) {
			cac.reportError("Could not load document processor list: " + ioe.getMessage());
			cac.reportError(ioe);
			return;
		}
		
		//	print out (filtered) document processor list
		for (int m = 0; m < this.docProcessorList.length; m++) {
			if ((managerPrefix != null) && !this.docProcessorList[m][0].startsWith(managerPrefix))
				continue;
			cac.reportResult(this.docProcessorList[m][0]);
			if (managersOnly)
				continue;
			for (int p = 1; p < this.docProcessorList[m].length; p++)
				cac.reportResult(" - " + this.docProcessorList[m][p]);
		}
	}
	
	private String[][] loadDocumentProcessorList(final ComponentActionConsole cac) throws IOException {
		cac.reportResult("Loading document processor list ...");
		
		//	assemble slave job
		DprSlaveJob dsj = new DprSlaveJob(Gamta.getAnnotationID(), null); // TODO use persistent UUID
		dsj.setMaxMemory(1024);
		
		//	start batch processor slave process
		Process dpLister = Runtime.getRuntime().exec(dsj.getCommand(null), new String[0], this.workingFolder);
		
		//	collect document processor listing
		final ArrayList docProcessorManagerList = new ArrayList();
		SlaveProcessInterface spi = new SlaveProcessInterface(dpLister, ("DprBatch" + "ListDocProcessors")) {
			private ArrayList docProcessorList = new ArrayList();
			protected void handleInput(String input) {
				if (input.startsWith("DPM:")) {
					this.finalizeDocProcessorManager();
					this.docProcessorList.add(input.substring("DPM:".length()));
				}
				else if (input.startsWith("DP:"))
					this.docProcessorList.add(input.substring("DP:".length()));
				else cac.reportResult(input);
			}
			protected void handleResult(String result) {
				cac.reportResult(result);
			}
			protected void finalizeSystemOut() {
				this.finalizeDocProcessorManager();
			}
			protected void handleError(String error, boolean fromSysErr) {
				cac.reportError(error);;
			}
			protected void finalizeSystemErr() {
				this.finalizeDocProcessorManager();
			}
			private synchronized void finalizeDocProcessorManager() {
				if (this.docProcessorList.size() != 0)
					docProcessorManagerList.add(this.docProcessorList.toArray(new String[this.docProcessorList.size()]));
				this.docProcessorList.clear();
			}
		};
		spi.start();
		
		//	wait for processing to finish
		while (true) try {
			dpLister.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	finally ...
		return ((String[][]) docProcessorManagerList.toArray(new String[docProcessorManagerList.size()][]));
	}
	
	private void scheduleProcessing(String docId, String dpName, String userName) {
		String[] args = {
			dpName,
			((userName == null) ? "" : userName)
		};
		this.documentProcessor.enqueueDataAction(docId, args);
	}
	
//	private void processDocument(String docId, String processorName) throws IOException {
	private void processDocument(String docId, String processorName, String userName) throws IOException {
		
		//	check out document as data, process it, and clean up
		DocumentReader docIn = null;
		try {
			this.processingDocId = docId;
			this.processingStart = System.currentTimeMillis();
			docIn = this.dio.checkoutDocumentAsStream(this.updateUserName, docId);
			this.processDocument(docId, docIn, processorName, userName);
		}
		catch (IOException ioe) {
			this.dio.releaseDocument(this.updateUserName, docId); // need to release here in case respective code not reached in processing
			throw ioe;
		}
		finally {
			this.batchRun = null;
			this.batchInterface = null;
			this.processingDocId = null;
			this.processingStart = -1;
			this.processorName = null;
			this.processorStart = -1;
			this.processingStep = null;
			this.processingStepStart = -1;
			this.processingInfo = null;
			this.processingInfoStart = -1;
			this.processingProgress = -1;
		}
	}
	
	private void processDocument(String docId, DocumentReader docIn, String processorName, String userName) throws IOException {
		
		//	preserve original update user (unless user name explicitly specified)
		String docUpdateUser = ((userName == null) ? ((String) docIn.getAttribute(GoldenGateDIO.UPDATE_USER_ATTRIBUTE, this.updateUserName)) : userName);
		
		//	create document cache file
		File cacheFile = new File(this.cacheFolder, ("cache-" + docId + ".gamta.xml"));
		
		//	copy document to cache, computing hash along the way
		DataHashOutputStream docHashOut = new DataHashOutputStream(new FileOutputStream(cacheFile));
		BufferedWriter docOut = new BufferedWriter(new OutputStreamWriter(docHashOut, "UTF-8"));
		char[] buffer = new char[1024];
		for (int r; (r = docIn.read(buffer, 0, buffer.length)) != -1;)
			docOut.write(buffer, 0, r);
		docOut.flush();
		docOut.close();
		docIn.close();
		String docHash = docHashOut.getDataHash();
		
		//	start batch processor slave process
		DprSlaveJob dsj = new DprSlaveJob(Gamta.getAnnotationID(), processorName); // TODO use persistent UUID
		dsj.setDataPath(cacheFile.getAbsolutePath());
		
		//	start batch processor slave process
		this.batchRun = Runtime.getRuntime().exec(dsj.getCommand(null), new String[0], this.workingFolder);
		
		//	get output channel
		this.batchInterface = new DprSlaveProcessInterface(this.batchRun, ("DprBatch" + docId), docId);
		this.batchInterface.setProgressMonitor(new ProgressMonitor() {
			public void setStep(String step) {
				logInfo("DPR: " + step);
				processingStep = step;
				processingStepStart = System.currentTimeMillis();
			}
			public void setInfo(String info) {
				logDebug("DPR: " + info);
				processingInfo = info;
				processingInfoStart = System.currentTimeMillis();
			}
			private int baseProgress = 0;
			private int maxProgress = 0;
			public void setBaseProgress(int baseProgress) {
				this.baseProgress = baseProgress;
			}
			public void setMaxProgress(int maxProgress) {
				this.maxProgress = maxProgress;
			}
			public void setProgress(int progress) {
				processingProgress = (this.baseProgress + (((this.maxProgress - this.baseProgress) * progress) / 100));
			}
		});
		this.batchInterface.start();
		
		//	wait for batch process to finish
		while (true) try {
			this.batchRun.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	load processed document, computing hash along the way
		DataHashInputStream pDocHashIn = new DataHashInputStream(new FileInputStream(cacheFile));
		BufferedReader pDocIn = new BufferedReader(new InputStreamReader(pDocHashIn, "UTF-8"));
		DocumentRoot pDoc = GenericGamtaXML.readDocument(pDocIn);
		pDocIn.close();
		String pDocHash = pDocHashIn.getDataHash();
		
		//	update and release document in DIO (update only if document hash actually changed)
		if (pDocHash.equals(docHash))
			this.logInfo("Document unchanged");
		else this.dio.updateDocument(docUpdateUser, this.updateUserName, docId, pDoc, new EventLogger() {
			public void writeLog(String logEntry) {
				logInfo(logEntry);
			}
		});
		this.dio.releaseDocument(this.updateUserName, docId);
		
		//	clean up cache and document data
		cacheFile.delete();
	}
	
	private static class DataHashInputStream extends FilterInputStream {
		private MD5 dataHasher = new MD5();
		private String dataHash = null;
		DataHashInputStream(InputStream in) {
			super(in);
		}
		public int read() throws IOException {
			int r = super.read();
			if (r != -1)
				this.dataHasher.update((byte) r);
			return r;
		}
		public int read(byte[] b) throws IOException {
			return this.read(b, 0, b.length);
		}
		public int read(byte[] b, int off, int len) throws IOException {
			int r = super.read(b, off, len);
			if (r != -1)
				this.dataHasher.update(b, off, r);
			return r;
		}
		public void close() throws IOException {
			super.close();
			
			//	we have been closed before
			if (this.dataHasher == null)
				return;
			
			//	finalize hash and rename file
			this.dataHash = this.dataHasher.digestHex();
			
			//	return digester to instance pool
			this.dataHasher = null;
		}
		String getDataHash() {
			return this.dataHash;
		}
	}
	
	private static class DataHashOutputStream extends FilterOutputStream {
		private MD5 dataHasher = new MD5();
		private String dataHash = null;
		DataHashOutputStream(OutputStream out) {
			super(out);
		}
		public void write(int b) throws IOException {
			this.out.write(b);
			this.dataHasher.update((byte) b);
		}
		public void write(byte[] b) throws IOException {
			this.write(b, 0, b.length);
		}
		public synchronized void write(byte[] b, int off, int len) throws IOException {
			this.out.write(b, off, len);
			this.dataHasher.update(b, off, len);
		}
		public void close() throws IOException {
			super.close();
			
			//	we have been closed before
			if (this.dataHasher == null)
				return;
			
			//	finalize hash and rename file
			this.dataHash = this.dataHasher.digestHex();
			
			//	return digester to instance pool
			this.dataHasher = null;
		}
		String getDataHash() {
			return this.dataHash;
		}
	}
	
	private class DprSlaveJob extends SlaveJob {
		DprSlaveJob(String slaveJobId, String processorName) {
			super(slaveJobId, "GgServerDprSlave.jar");
			if (maxSlaveMemory > 512)
				this.setMaxMemory(maxSlaveMemory);
			this.setMaxCores(1);
			this.setLogPath(logFolder.getAbsolutePath());
			if (ggConfigHost != null)
				this.setProperty("CONFHOST", ggConfigHost);
			this.setProperty("CONFNAME", ggConfigName);
			this.setProperty("DPNAME", ((processorName == null) ? "LISTDPS" : processorName));
		}
	}
	
	private class DprSlaveProcessInterface extends SlaveProcessInterface {
		private ComponentActionConsole reportTo = null;
		private String docId;
		DprSlaveProcessInterface(Process slave, String slaveName, String docId) {
			super(slave, slaveName);
			this.docId = docId;
		}
		void setReportTo(ComponentActionConsole reportTo) {
			this.reportTo = reportTo;
		}
		protected void handleInput(String input) {
			if (input.startsWith("PR:")) {
				input = input.substring("PR:".length());
				logInfo("Running Document Processor '" + input + "'");
				processorName = input;
				processorStart = System.currentTimeMillis();
			}
			else logInfo("DPR: " + input);
		}
		protected void handleResult(String result) {
			ComponentActionConsole cac = this.reportTo;
			if (cac == null)
				logInfo("DPR: " + result);
			else cac.reportResult(result);
		}
		private ArrayList outStackTrace = new ArrayList();
		protected void handleStackTrace(String stackTraceLine) {
			if (stackTraceLine.trim().length() == 0)
				this.reportError(this.outStackTrace);
			else {
				this.outStackTrace.add(stackTraceLine);
				super.handleStackTrace(stackTraceLine);
			}
		}
		protected void finalizeSystemOut() {
			this.reportError(this.outStackTrace);
		}
		private ArrayList errStackTrace = new ArrayList();
		protected void handleError(String error, boolean fromSysErr) {
			ComponentActionConsole cac = this.reportTo;
			if (fromSysErr && (cac == null)) {
				if (error.startsWith("CR\t") || error.startsWith("LA\t") || error.startsWith("Stale "))
					return; // TODO remove this once server fixed
				if (error.matches("(Im|Gamta)Document(Root)?Guard\\:.*"))
					return; // TODO remove this once server fixed
			}
			if (cac == null) {
				if (fromSysErr)
					this.errStackTrace.add(error);
				logError("DPR: " + error);
			}
			else cac.reportError(error);
		}
		protected void finalizeSystemErr() {
			this.reportError(this.errStackTrace);
		}
		private void reportError(ArrayList stackTrace) {
			if (stackTrace.size() == 0)
				return;
			String classAndMessge = ((String) stackTrace.get(0));
			String errorClassName;
			String errorMessage;
			if (classAndMessge.indexOf(":") == -1) {
				errorClassName = classAndMessge;
				errorMessage = "";
			}
			else {
				errorClassName = classAndMessge.substring(0, classAndMessge.indexOf(":")).trim();
				errorMessage = classAndMessge.substring(classAndMessge.indexOf(":") + ":".length()).trim();
			}
			String[] errorStackTrace = ((String[]) stackTrace.toArray(new String[this.outStackTrace.size()]));
			stackTrace.clear();
			SlaveErrorRecorder.recordError(getLetterCode(), this.docId, errorClassName, errorMessage, errorStackTrace);
		}
	}
}