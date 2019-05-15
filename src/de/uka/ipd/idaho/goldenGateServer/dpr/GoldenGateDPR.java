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
 *     * Neither the name of the Universität Karlsruhe (TH) / KIT nor the
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
package de.uka.ipd.idaho.goldenGateServer.dpr;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
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
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import de.uka.ipd.idaho.easyIO.sql.TableColumnDefinition;
import de.uka.ipd.idaho.easyIO.sql.TableDefinition;
import de.uka.ipd.idaho.easyIO.util.RandomByteSource;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousDataActionHandler;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * GoldenGATE Processor provides automated processing of XML documents stored
 * in a GoldenGATE DIO. Processing happens in a slave JVM.
 * 
 * @author sautter
 */
public class GoldenGateDPR extends AbstractGoldenGateServerComponent {
	
	private GoldenGateDIO dio;
	
	private String updateUserName;
	
	private File workingFolder;
	private File cacheFolder;
	private AsynchronousDataActionHandler documentProcessor;
	
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
		
		//	get working folder
		String workingFolderName = this.configuration.getSetting("workingFolderName", "Processor");
		while (workingFolderName.startsWith("./"))
			workingFolderName = workingFolderName.substring("./".length());
		this.workingFolder = (((workingFolderName.indexOf(":\\") == -1) && (workingFolderName.indexOf(":/") == -1) && !workingFolderName.startsWith("/")) ? new File(this.dataPath, workingFolderName) : new File(workingFolderName));
		this.workingFolder.mkdirs();
		
		//	get document transfer cache folder (RAM disc !!!)
		String cacheFolderName = this.configuration.getSetting("cacheFolderName", "Cache");
		while (cacheFolderName.startsWith("./"))
			cacheFolderName = cacheFolderName.substring("./".length());
		this.cacheFolder = (((cacheFolderName.indexOf(":\\") == -1) && (cacheFolderName.indexOf(":/") == -1) && !cacheFolderName.startsWith("/")) ? new File(this.dataPath, cacheFolderName) : new File(cacheFolderName));
		this.cacheFolder.mkdirs();
		
		//	load GG config host & name
		this.ggConfigHost = this.configuration.getSetting("ggConfigHost");
		this.ggConfigName = this.configuration.getSetting("ggConfigName");
		
		//	install base JARs
		this.installJar("StringUtils.jar");
		this.installJar("HtmlXmlUtil.jar");
		this.installJar("Gamta.jar");
		this.installJar("mail.jar");
		this.installJar("EasyIO.jar");
		
		//	install GG JAR
		this.installJar("GoldenGATE.jar");
		
		//	install GG slave JAR
		this.installJar("GgServerDprSlave.jar");
		
		//	create asynchronous worker
		TableColumnDefinition[] argCols = {new TableColumnDefinition("DpName", TableDefinition.VARCHAR_DATATYPE, 128)};
		this.documentProcessor = new AsynchronousDataActionHandler("Dpr", argCols, this, this.host.getIoProvider()) {
			protected void performDataAction(String dataId, String[] arguments) throws Exception {
				processDocument(dataId, arguments[0]);
			}
		};
		
		//	TODO get list of documents from DIO
		
		//	TODO release all documents we still hold the lock for
	}
	
	private void installJar(String name) {
		System.out.println("Installing JAR '" + name + "'");
		File source = new File(this.dataPath, name);
		if (!source.exists())
			throw new RuntimeException("Missing JAR: " + name);
		
		File target = new File(this.workingFolder, name);
		if ((target.lastModified() + 1000) > source.lastModified()) {
			System.out.println(" ==> up to date");
			return;
		}
		
		try {
			InputStream sourceIn = new BufferedInputStream(new FileInputStream(source));
			OutputStream targetOut = new BufferedOutputStream(new FileOutputStream(target));
			byte[] buffer = new byte[1024];
			for (int r; (r = sourceIn.read(buffer, 0, buffer.length)) != -1;)
				targetOut.write(buffer, 0, r);
			targetOut.flush();
			targetOut.close();
			sourceIn.close();
			System.out.println(" ==> installed");
		}
		catch (IOException ioe) {
			throw new RuntimeException("Could not install JAR '" + name + "': " + ioe.getMessage());
		}
		catch (Exception e) {
			e.printStackTrace(System.out);
			throw new RuntimeException("Could not install JAR '" + name + "': " + e.getMessage());
		}
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
//		Thread batchRunner = new DocumentProcessingThread();
//		batchRunner.start();
		this.documentProcessor.start();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#exitComponent()
	 */
	protected void exitComponent() {
		
		//	shut down processing handler thread
//		synchronized (this.processingRequestQueue) {
//			this.processingRequestQueue.clear();
//			this.processingRequestQueue.notify();
//		}
		this.documentProcessor.shutdown();
	}
	
	private static final String PROCESS_DOCUMENT_COMMAND = "process";
	private static final String LIST_PROCESSORS_COMMAND = "processors";
	private static final String QUEUE_SIZE_COMMAND = "queueSize";
	
	/*
	 * (non-Javadoc)
	 * @see de.goldenGateScf.ServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(this.documentProcessor.getActions()));
		ComponentAction ca;
		
		//	schedule processing a document
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return PROCESS_DOCUMENT_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						PROCESS_DOCUMENT_COMMAND + " <documentId> <processorName>",
						"Schedule a document for processing:",
						"- <documentId>: The ID of the document to process",
						"- <processorName>: The name of the document processor to use"
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 2)
					scheduleProcessing(arguments[0], arguments[1]);
				else reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify the document ID and processor name as the only arguments.");
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
					reportResult(documentProcessor.getDataActionsPending() + " documents waiting to be processed.");
				else reportError(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
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
				cac.reportResult(this.docProcessorList[m][p]);
		}
	}
	
	private String[][] loadDocumentProcessorList(final ComponentActionConsole cac) throws IOException {
		cac.reportResult("Loading document processor list ...");
		
		//	assemble command
		StringVector command = new StringVector();
		command.addElement("java");
		command.addElement("-jar");
		command.addElement("-Xmx1024m");
		command.addElement("GgServerDprSlave.jar");
		
		//	add parameters
		if (this.ggConfigHost != null)
			command.addElement("CONFHOST=" + this.ggConfigHost); // config host (if any)
		command.addElement("CONFNAME=" + this.ggConfigName); // config name
		command.addElement("DPNAME=" + "LISTDPS"); // constant returning list
		command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
		
		//	start batch processor slave process
		Process processing = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
		
		//	loop through error messages
		final BufferedReader processorError = new BufferedReader(new InputStreamReader(processing.getErrorStream()));
		new Thread() {
			public void run() {
				try {
					for (String errorLine; (errorLine = processorError.readLine()) != null;)
						cac.reportError(errorLine);
				}
				catch (Exception e) {
					cac.reportError(e);
				}
			}
		}.start();
		
		//	collect document processor listing
		ArrayList docProcessorManagerList = new ArrayList();
		ArrayList docProcessorList = new ArrayList();
		BufferedReader processorIn = new BufferedReader(new InputStreamReader(processing.getInputStream()));
		for (String inLine; (inLine = processorIn.readLine()) != null;) {
			if (inLine.startsWith("DPM:")) {
				if (docProcessorList.size() != 0)
					docProcessorManagerList.add(docProcessorList.toArray(new String[docProcessorList.size()]));
				docProcessorList.clear();
				docProcessorList.add(inLine.substring("DPM:".length()));
			}
			else if (inLine.startsWith("DP:"))
				docProcessorList.add(inLine.substring("DP:".length()));
			else cac.reportResult(inLine);
		}
		if (docProcessorList.size() != 0)
			docProcessorManagerList.add(docProcessorList.toArray(new String[docProcessorList.size()]));
		
		//	wait for processing to finish
		while (true) try {
			processing.waitFor();
			break;
		} catch (InterruptedException ie) {}
		
		//	finally ...
		return ((String[][]) docProcessorManagerList.toArray(new String[docProcessorManagerList.size()][]));
	}
	
	private void scheduleProcessing(String docId, String prName) {
//		this.enqueueProcessingRequest(new DocumentProcessingRequest(docId, prName));
		String[] args = {prName};
		this.documentProcessor.enqueueDataAction(docId, args);
	}
//	
//	private LinkedList processingRequestQueue = new LinkedList() {
//		private HashSet deduplicator = new HashSet();
//		public Object removeFirst() {
//			Object e = super.removeFirst();
//			this.deduplicator.remove(e);
//			return e;
//		}
//		public void addLast(Object e) {
//			if (this.deduplicator.add(e))
//				super.addLast(e);
//		}
//	};
//	private void enqueueProcessingRequest(DocumentProcessingRequest dpr) {
//		synchronized (this.processingRequestQueue) {
//			this.processingRequestQueue.addLast(dpr);
//			this.processingRequestQueue.notify();
//		}
//	}
//	private DocumentProcessingRequest getProcessingRequest() {
//		synchronized (this.processingRequestQueue) {
//			if (this.processingRequestQueue.isEmpty()) try {
//				this.processingRequestQueue.wait();
//			} catch (InterruptedException ie) {}
//			return (this.processingRequestQueue.isEmpty() ? null : ((DocumentProcessingRequest) this.processingRequestQueue.removeFirst()));
//		}
//	}
//	
//	private static class DocumentProcessingRequest {
//		final String documentId;
//		final String processorName;
//		DocumentProcessingRequest(String documentId, String processorName) {
//			this.documentId = documentId;
//			this.processorName = processorName;
//		}
//	}
//	
//	private class DocumentProcessingThread extends Thread {
//		public void run() {
//			
//			//	don't start right away
//			try {
//				sleep(1000 * 15);
//			} catch (InterruptedException ie) {}
//			
//			//	keep going until shutdown
//			while (true) {
//				
//				//	get next document ID to process
//				DocumentProcessingRequest dpr = getProcessingRequest();
//				if (dpr == null)
//					return; // only happens on shutdown
//				
//				//	process document
//				try {
//					handleProcessingRequest(dpr);
//				}
//				catch (Exception e) {
//					logError(e);
//				}
//				
//				//	give the others a little time
//				try {
//					sleep(1000 * 5);
//				} catch (InterruptedException ie) {}
//			}
//		}
//	}
//	
//	private void handleProcessingRequest(DocumentProcessingRequest dpr) throws IOException {
//		
//		//	check out document as stream
//		DocumentReader docIn = this.dio.checkoutDocumentAsStream(this.updateUserName, dpr.documentId);
//		
//		//	create document cache file
//		File cacheFile = new File(this.cacheFolder, ("cache-" + dpr.documentId + ".gamta.xml"));
//		
//		//	copy document to cache, computing hash along the way
//		DataHashOutputStream docHashOut = new DataHashOutputStream(new FileOutputStream(cacheFile));
//		BufferedWriter docOut = new BufferedWriter(new OutputStreamWriter(docHashOut, "UTF-8"));
//		char[] buffer = new char[1024];
//		for (int r; (r = docIn.read(buffer, 0, buffer.length)) != -1;)
//			docOut.write(buffer, 0, r);
//		docOut.flush();
//		docOut.close();
//		docIn.close();
//		String docHash = docHashOut.getDataHash();
//		
//		//	assemble command
//		StringVector command = new StringVector();
//		command.addElement("java");
//		command.addElement("-jar");
//		command.addElement("-Xmx1024m");
//		command.addElement("GgServerDprSlave.jar");
//		
//		//	add parameters
//		command.addElement("DATA=" + cacheFile.getAbsolutePath()); // cache folder
//		if (this.ggConfigHost != null)
//			command.addElement("CONFHOST=" + this.ggConfigHost); // config host (if any)
//		command.addElement("CONFNAME=" + this.ggConfigName); // config name
//		command.addElement("DPNAME=" + dpr.processorName); // document processor to run
//		command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
//		
//		//	start batch processor slave process
//		Process processing = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
//		
//		//	loop through error messages
//		final BufferedReader processorError = new BufferedReader(new InputStreamReader(processing.getErrorStream()));
//		new Thread() {
//			public void run() {
//				try {
//					for (String errorLine; (errorLine = processorError.readLine()) != null;)
//						logError(errorLine);
//				}
//				catch (Exception e) {
//					logError(e);
//				}
//			}
//		}.start();
//		
//		//	loop through step information only
//		BufferedReader processorIn = new BufferedReader(new InputStreamReader(processing.getInputStream()));
//		for (String inLine; (inLine = processorIn.readLine()) != null;) {
//			if (inLine.startsWith("S:"))
//				logInfo(inLine.substring("S:".length()));
//			else if (inLine.startsWith("I:")) {}
//			else if (inLine.startsWith("P:")) {}
//			else if (inLine.startsWith("BP:")) {}
//			else if (inLine.startsWith("MP:")) {}
//			else logInfo(inLine);
//		}
//		
//		//	wait for processing to finish
//		while (true) try {
//			processing.waitFor();
//			break;
//		} catch (InterruptedException ie) {}
//		
//		//	load processed document, computing hash along the way
//		DataHashInputStream pDocHashIn = new DataHashInputStream(new FileInputStream(cacheFile));
//		BufferedReader pDocIn = new BufferedReader(new InputStreamReader(pDocHashIn, "UTF-8"));
//		DocumentRoot pDoc = GenericGamtaXML.readDocument(pDocIn);
//		pDocIn.close();
//		String pDocHash = pDocHashIn.getDataHash();
//		
//		//	update and release document in DIO (update only if document hash actually changed)
//		if (pDocHash.equals(docHash))
//			this.logInfo("Document unchanged");
//		else this.dio.updateDocument(this.updateUserName, dpr.documentId, pDoc, new EventLogger() {
//			public void writeLog(String logEntry) {
//				logInfo(logEntry);
//			}
//		});
//		this.dio.releaseDocument(this.updateUserName, dpr.documentId);
//		
//		//	clean up cache and document data
//		cacheFile.delete();
//	}
	private void processDocument(String docId, String processorName) throws IOException {
		
		//	check out document as stream
		DocumentReader docIn = this.dio.checkoutDocumentAsStream(this.updateUserName, docId);
		
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
		
		//	assemble command
		StringVector command = new StringVector();
		command.addElement("java");
		command.addElement("-jar");
		command.addElement("-Xmx1024m");
		command.addElement("GgServerDprSlave.jar");
		
		//	add parameters
		command.addElement("DATA=" + cacheFile.getAbsolutePath()); // cache folder
		if (this.ggConfigHost != null)
			command.addElement("CONFHOST=" + this.ggConfigHost); // config host (if any)
		command.addElement("CONFNAME=" + this.ggConfigName); // config name
		command.addElement("DPNAME=" + processorName); // document processor to run
		command.addElement("SINGLECORE"); // run on single CPU core only (we don't want to knock out the whole server, do we?)
		
		//	start batch processor slave process
		Process processing = Runtime.getRuntime().exec(command.toStringArray(), new String[0], this.workingFolder);
		
		//	loop through error messages
		final BufferedReader processorError = new BufferedReader(new InputStreamReader(processing.getErrorStream()));
		new Thread() {
			public void run() {
				try {
					for (String errorLine; (errorLine = processorError.readLine()) != null;)
						logError(errorLine);
				}
				catch (Exception e) {
					logError(e);
				}
			}
		}.start();
		
		//	loop through step information only
		BufferedReader processorIn = new BufferedReader(new InputStreamReader(processing.getInputStream()));
		for (String inLine; (inLine = processorIn.readLine()) != null;) {
			if (inLine.startsWith("S:"))
				logInfo(inLine.substring("S:".length()));
			else if (inLine.startsWith("I:")) {}
			else if (inLine.startsWith("P:")) {}
			else if (inLine.startsWith("BP:")) {}
			else if (inLine.startsWith("MP:")) {}
			else logInfo(inLine);
		}
		
		//	wait for processing to finish
		while (true) try {
			processing.waitFor();
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
		else this.dio.updateDocument(this.updateUserName, docId, pDoc, new EventLogger() {
			public void writeLog(String logEntry) {
				logInfo(logEntry);
			}
		});
		this.dio.releaseDocument(this.updateUserName, docId);
		
		//	clean up cache and document data
		cacheFile.delete();
	}
	
	private static class DataHashInputStream extends FilterInputStream {
		private MessageDigest dataHasher = getDataHasher();
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
			this.dataHash = new String(RandomByteSource.getHexCode(this.dataHasher.digest()));
			
			//	return digester to instance pool
			returnDataHash(this.dataHasher);
			this.dataHasher = null;
		}
		
		String getDataHash() {
			return this.dataHash;
		}
	}
	
	private static class DataHashOutputStream extends FilterOutputStream {
		private MessageDigest dataHasher = getDataHasher();
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
			this.dataHash = new String(RandomByteSource.getHexCode(this.dataHasher.digest()));
			
			//	return digester to instance pool
			returnDataHash(this.dataHasher);
			this.dataHasher = null;
		}
		
		String getDataHash() {
			return this.dataHash;
		}
	}
	
	private static LinkedList dataHashPool = new LinkedList();
	private static synchronized MessageDigest getDataHasher() {
		if (dataHashPool.size() != 0) {
			MessageDigest dataHash = ((MessageDigest) dataHashPool.removeFirst());
			dataHash.reset();
			return dataHash;
		}
		try {
			MessageDigest dataHash = MessageDigest.getInstance("MD5");
			dataHash.reset();
			return dataHash;
		}
		catch (NoSuchAlgorithmException nsae) {
			System.out.println(nsae.getClass().getName() + " (" + nsae.getMessage() + ") while creating checksum digester.");
			nsae.printStackTrace(System.out); // should not happen, but Java don't know ...
			return null;
		}
	}
	private static synchronized void returnDataHash(MessageDigest dataHash) {
		dataHashPool.addLast(dataHash);
	}
}