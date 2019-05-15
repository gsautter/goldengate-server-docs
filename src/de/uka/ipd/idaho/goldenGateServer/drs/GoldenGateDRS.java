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
package de.uka.ipd.idaho.goldenGateServer.drs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.AnnotationChecksumDigest.AttributeFilter;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerEventService;
import de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES.RemoteEventList;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES.ResEventFilter;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES.ResRemoteEvent;
import de.uka.ipd.idaho.goldenGateServer.res.GoldenGateRES.ResRemoteEvent.ResRemoteEventListener;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousConsoleAction;

/**
 * GoldenGATE Document Replication Service replicates document updates and
 * deletions between GoldenGATE DIOs installed in two GoldenGATE Servers. It
 * relies on GoldenGATE Remote Event Service for event forwarding.
 * 
 * @author sautter
 */
public class GoldenGateDRS extends GoldenGateAEP implements GoldenGateDioConstants {
	
	private static final String ORIGINAL_UPDATE_USER_ATTRIBUTE = "originalUpdateUser";
	private static final String ORIGINAL_UPDATE_TIME_ATTRIBUTE = "originalUpdateTime";
	private static final String ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE = "originalUpdateDomain";
	
	private static final String GET_DOCUMENT = "DRS_GET_DOCUMENT";
	private static final String GET_DOCUMENT_LIST = "DRS_GET_DOCUMENT_LIST";
	
	private static final String defaultPassPhrase = "DRS provides remote access!";
	
	/** The GoldenGATE DIO to work with */
	protected GoldenGateDIO dio;
	private GoldenGateRES res;
	
	private String localPassPhrase = null;
	private Properties remotePassPhrases = new Properties();
	
	/** Constructor passing 'DRS' as the letter code to super constructor
	 */
	public GoldenGateDRS() {
		super("DRS");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#getEventProcessorName()
	 */
	protected String getEventProcessorName() {
		return "DioReplicator";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#persistProcessingEvents()
	 */
	protected boolean persistProcessingEvents() {
		return true; // fetching a document can take a little while, so let's be safe here
	}
	
	/**
	 * This implementation reads the pass phrases. Sub classes overwriting this
	 * method have to make the super call.
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#initComponent()
	 */
	protected void initComponent() {
		
		//	initialize super class
		super.initComponent();
		
		//	read our own access data
		this.readPassPhrases();
	}
	
	private void readPassPhrases() {
		Settings passPhrases = Settings.loadSettings(new File(this.dataPath, "passPhrases.cnfg"));
		
		//	load pass phrases for incoming connections
		this.localPassPhrase = passPhrases.getSetting("localPassPhrase", defaultPassPhrase);
		
		//	load pass phrases for accessing remote DRS's
		Settings remotePassPhrases = passPhrases.getSubset("remotePassPhrase");
		String[] remoteDomainNames = remotePassPhrases.getKeys();
		for (int d = 0; d < remoteDomainNames.length; d++)
			this.remotePassPhrases.setProperty(remoteDomainNames[d], remotePassPhrases.getSetting(remoteDomainNames[d]));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
	 */
	public void link() {
		
		//	get DIO
		this.dio = ((GoldenGateDIO) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateDIO.class.getName()));
		
		//	check success
		if (this.dio == null) throw new RuntimeException(GoldenGateDIO.class.getName());
		
		//	hook up to local RES
		this.res = ((GoldenGateRES) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateRES.class.getName()));
		
		//	check success
		if (this.res == null) throw new RuntimeException(GoldenGateRES.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
	 */
	public void linkInit() {
		
		//	listen for events
		GoldenGateServerEventService.addServerEventListener(new ResRemoteEventListener() {
			public void notify(ResRemoteEvent rre) {
				if (!DioDocumentEvent.class.getName().equals(rre.eventClassName))
					return;
				
				//	reconstruct and handle document event
				DioDocumentEvent dde = DioDocumentEvent.parseEvent(rre.paramString);
				handleDocumentEvent(rre, dde);
			}
		});
		
		//	make sure remote domain data, etc. don't influence checksum
		this.dio.addDocumentChecksumAttributeFilter(new AttributeFilter() {
			public boolean filterAttribute(String attributeName) {
				return (false
						|| ORIGINAL_UPDATE_USER_ATTRIBUTE.equals(attributeName)
						|| ORIGINAL_UPDATE_TIME_ATTRIBUTE.equals(attributeName)
						|| ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE.equals(attributeName)
						);
			}
		});
		
		//	prevent remote document updates from being re-published
		this.res.addEventFilter(new ResEventFilter() {
			public boolean allowPublishEvent(GoldenGateServerEvent gse) {
				if ((gse instanceof DioDocumentEvent) && ((DioDocumentEvent) gse).user.startsWith("DRS."))
					return false;
				return true;
			}
		});
		
		//	link initialize super class
		super.linkInit();
		
		//	diff events with remote DRS
		this.diffAction = new AsynchronousConsoleAction(DIFF_FROM_DRS_COMMAND, "Run a full diff with a specific remote GoldenGATE DRS, i.e., compare document update events and handle unhandled ones", "update event", null, null) {
			protected String[] getArgumentNames() {
				String[] args = {"remoteDomain"};
				return args;
			}
			protected String[] getArgumentExplanation(String argument) {
				if ("remoteDomain".equals(argument)) {
					String[] explanation = {"The alias of the remote GoldenGATE DRS to compare the document list with"};
					return explanation;
				}
				else return super.getArgumentExplanation(argument);
			}
			protected void checkRunnable() {
				if (syncAction.isRunning())
					throw new RuntimeException("Document list sync running, diff cannot run in parallel");
			}
			protected String checkArguments(String[] arguments) {
				if (arguments.length != 1)
					return ("Invalid arguments for '" + this.getActionCommand() + "', specify the alias of the DRS to diff with as the only argument.");
				String address = res.getRemoteDomainAddress(arguments[0]);
				if (address == null)
					return ("No remote DRS found for name " + arguments[0]);
				else return null;
			}
			protected void performAction(String[] arguments) throws Exception {
				String remoteDomain = arguments[0];
				
				//	get remote events
				RemoteEventList rel = res.getRemoteEventList(remoteDomain, 0, GoldenGateDIO.class.getName());
				if (rel == null)
					return;
				this.enteringMainLoop("Got event list from " + remoteDomain);
				int handleCount = 0;
				int skipCount = 0;
				
				//	do diff
				while (this.continueAction() && rel.hasNextEvent()) {
					ResRemoteEvent rre = rel.getNextEvent();
					if (!DioDocumentEvent.class.getName().equals(rre.eventClassName))
						continue;
					
					//	reconstruct document event
					DioDocumentEvent dde = DioDocumentEvent.parseEvent(rre.paramString);
					
					//	check against local update time
					Properties docAttributes = dio.getDocumentAttributes(dde.documentId);
					if (docAttributes != null) {
						long updateTime = Long.parseLong(docAttributes.getProperty(ORIGINAL_UPDATE_TIME_ATTRIBUTE, docAttributes.getProperty(UPDATE_TIME_ATTRIBUTE, "0")));
						if (rre.eventTime < updateTime) {
							skipCount++;
							continue;
						}
					}
					
					//	handle document event
					dataUpdated(dde.documentId, false, remoteDomain, PRIORITY_LOW);
					handleCount++;
					
					//	update status
					this.loopRoundComplete("Handled " + handleCount + " update events, skipped " + skipCount + " ones.");
				}
			}
		};
		
		//	sync documents with a remote DRS
		this.syncAction = new AsynchronousConsoleAction(SYNC_WITH_DRS_COMMAND, "Run a full sync with a specific remote GoldenGATE DRS, i.e., compare the document lists and fetch missing updates", "document list", null, null) {
			protected String[] getArgumentNames() {
				String[] args = {"remoteDomain", "mode"};
				return args;
			}
			protected String[] getArgumentExplanation(String argument) {
				if ("remoteDomain".equals(argument)) {
					String[] explanation = {"The alias of the remote GoldenGATE DRS to compare the document list with"};
					return explanation;
				}
				else if ("mode".equals(argument)) {
					String[] explanation = {"The sync mode: '-u' for 'update' (the default), '-d' for 'delete', or '-ud' for both"};
					return explanation;
				}
				else return super.getArgumentExplanation(argument);
			}
			protected void checkRunnable() {
				if (diffAction.isRunning())
					throw new RuntimeException("Update event diff running, sync cannot run in parallel");
			}
			protected String checkArguments(String[] arguments) {
				if ((arguments.length < 1) || (arguments.length > 2)) 
					return ("Invalid arguments for '" + this.getActionCommand() + "', specify the alias of the DRS to sync with, and optionally the sync mode, as the only arguments.");
				String address = res.getRemoteDomainAddress(arguments[0]);
				if (address == null)
					return ("No remote DRS found for name " + arguments[0]);
				else if ((arguments.length == 2) && ("-u -d -ud".indexOf(arguments[1]) == -1))
					return ("Invalid sync mode " + arguments[1]);
				else return null;
			}
			protected void performAction(String[] arguments) throws Exception {
				String remoteDomain = arguments[0];
				boolean update = ((arguments.length == 1) || (arguments[1].indexOf("u") != -1));
				boolean delete = ((arguments.length == 2) && (arguments[1].indexOf("d") != -1));
				
				//	get remote domain access data
				String remoteAddress = res.getRemoteDomainAddress(remoteDomain);
				int remotePort = res.getRemoteDomainPort(remoteDomain);
				
				//	get document list from remote domain, and index document records by ID
				DocumentList remoteDl = getDocumentList(remoteAddress, remotePort);
				HashMap remoteDlesById = new HashMap();
				while (remoteDl.hasNextDocument()) {
					DocumentListElement dle = remoteDl.getNextDocument();
					remoteDlesById.put(((String) dle.getAttribute(DOCUMENT_ID_ATTRIBUTE)), dle);
				}
				
				//	iterate over local document list, collecting IDs of documents to update or delete
				HashSet updateDocIDs = new HashSet();
				HashSet deleteDocIDs = new HashSet();
				DocumentList localDl = dio.getDocumentListFull();
				while (localDl.hasNextDocument()) {
					DocumentListElement localDle = localDl.getNextDocument();
					String docId = ((String) localDle.getAttribute(DOCUMENT_ID_ATTRIBUTE));
					DocumentListElement remoteDle = ((DocumentListElement) remoteDlesById.remove(docId));
					
					//	this one doesn't even exist in the remote domain
					if (remoteDle == null) {
						if (delete)
							deleteDocIDs.add(docId);
						continue;
					}
					
					//	extract update timestamps for comparison
					long localUpdateTime;
					long remoteUpdateTime;
					try {
						localUpdateTime = Long.parseLong((String) localDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
						remoteUpdateTime = Long.parseLong((String) remoteDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
					}
					catch (Exception e) {
						this.log(("Could not parse update timestamps for document '" + docId + "'"), e);
						continue;
					}
					
					//	remote version is newer than local one, even with a one second tolerance), mark for update
					if (update && ((localUpdateTime + 1000) < remoteUpdateTime))
						updateDocIDs.add(docId);
				}
				
				//	add updates for new document not yet available locally
				if (update)
					updateDocIDs.addAll(remoteDlesById.keySet());
				
				//	do updates and deletions
				int updateCount = updateDocIDs.size();
				int deleteCount = deleteDocIDs.size();
				this.enteringMainLoop("Got event list from " + remoteDomain + ", " + updateCount + " updates, " + deleteCount + " deletions");
				while (this.continueAction() && ((updateDocIDs.size() + deleteDocIDs.size()) != 0)) {
					
					//	do deletions first ...
					if (deleteDocIDs.size() != 0) {
						String docId = ((String) deleteDocIDs.iterator().next());
						deleteDocIDs.remove(docId);
						this.reportResult("GoldenGateDRS: forwarding deletion from " + remoteDomain + " (" + remoteAddress + ":" + remotePort + ") ...");
						try {
							
							//	get update user, and reuse if starting with 'DRS.'
							Properties docAttributes = dio.getDocumentAttributes(docId);
							String updateUser = docAttributes.getProperty(UPDATE_USER_ATTRIBUTE);
							if ((updateUser == null) || !updateUser.startsWith("DRS."))
								updateUser = ("DRS." + remoteDomain);
							
							//	delete document
							dio.deleteDocument(updateUser, docId, null);
						}
						catch (IOException ioe) {
							this.reportError("GoldenGateDRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document " + docId + ".");
							this.reportError(ioe);
						}
					}
					
					//	... and updates second
					else if (updateDocIDs.size() != 0) {
						String docId = ((String) updateDocIDs.iterator().next());
						updateDocIDs.remove(docId);
						this.reportResult("GoldenGateDRS: getting update from " + remoteDomain + " (" + remoteAddress + ":" + remotePort + ") ...");
						dataUpdated(docId, false, remoteDomain, PRIORITY_LOW);
					}
					
					//	update status
					this.loopRoundComplete("Handled " + (updateCount - updateDocIDs.size()) + " of " + updateCount + " updates, " + (deleteCount - deleteDocIDs.size()) + " of " + deleteCount + " deletions.");
				}
			}
		};
	}
	
	/**
	 * Handle a document event. Sub classes can overwrite this method to filter
	 * events and then call this implementation.
	 * @param rre the remote event the document event was wrapped in
	 * @param dde the document event to handle
	 */
	void handleDocumentEvent(ResRemoteEvent rre, DioDocumentEvent dde) {
		
		//	handle update event
		if (dde.type == DioDocumentEvent.UPDATE_TYPE) {
			this.logInfo("GoldenGateDRS: scheduling update from " + rre.sourceDomainAlias + " (" + rre.sourceDomainAddress + ":" + rre.sourceDomainPort + ") ...");
			this.dataUpdated(dde.documentId, (dde.version == 1), rre.sourceDomainAlias, PRIORITY_LOW);
		}
		
		//	handle delete event
		else if (dde.type == DioDocumentEvent.DELETE_TYPE) {
			try {
				this.dio.deleteDocument(("DRS." + rre.originDomainName), dde.documentId, null);
			}
			catch (IOException ioe) {
				System.out.println("GoldenGateDRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
				ioe.printStackTrace(System.out);
			}
		}
		
		//	we are not interested in checkouts and releases (for now)
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#doUpdate(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doUpdate(String dataId, String user, Properties dataAttributes, long params) throws IOException {
		
		//	get remote domain access data
		String remoteAddress = this.res.getRemoteDomainAddress(user);
		int remotePort = this.res.getRemoteDomainPort(user);
		
		//	get document
		this.logInfo("GoldenGateDRS: updating from " + user + " (" + remoteAddress + ":" + remotePort + ") ...");
		QueriableAnnotation doc = this.getDocument(dataId, remoteAddress, remotePort, user);
		
		//	get original update domain
		String updateDomain = ((String) doc.getAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE));
		if (updateDomain == null)
			updateDomain = user;
		
		//	store document
		this.dio.updateDocument(("DRS." + updateDomain), dataId, doc, null);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.aep.GoldenGateAEP#doDelete(java.lang.String, java.lang.String, java.util.Properties, long)
	 */
	protected void doDelete(String dataId, String user, Properties dataAttributes, long params) throws IOException {
		//	deletions are handled synchronously
	}
	
	private static final String READ_PASS_PHRASES_COMMAND = "readPassPhrases";
	private static final String DIFF_FROM_DRS_COMMAND = "diff";
	private static final String SYNC_WITH_DRS_COMMAND = "sync";
	
	private AsynchronousConsoleAction diffAction;
	private AsynchronousConsoleAction syncAction;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
	 */
	public ComponentAction[] getActions() {
		ArrayList cal = new ArrayList(Arrays.asList(super.getActions()));
		ComponentAction ca;
		
		//	add console action for issuing updates events for all existing documents
		ca = new AsynchronousConsoleAction("publishEvents", "Publish update events for the documents in the local DIO.", "update event", null, null) {
			protected String[] getArgumentNames() {
				return new String[0];
			}
			protected String checkArguments(String[] arguments) {
				return ((arguments.length == 0) ? null : " Specify no arguments.");
			}
			protected void performAction(String[] arguments) throws Exception {
				DocumentList dl = dio.getDocumentListFull();
				if (dl.hasNextDocument()) {
					DocumentListElement de = dl.getNextDocument();
					RemoteEventList rel = ((GoldenGateRES) res).getEventList(0, GoldenGateDIO.class.getName());
					HashSet deDuplicator = new HashSet();
					while (rel.hasNextEvent()) {
						ResRemoteEvent re = rel.getNextEvent();
						if (re.type == DioDocumentEvent.UPDATE_TYPE)
							deDuplicator.add(re.eventId);
					}
					int existingEventCount = deDuplicator.size();
					this.enteringMainLoop("Got " + existingEventCount + " existing document update events, enqueued 0 new ones");
					
					int newEventCount = 0;
					do {
						String docId = ((String) de.getAttribute(DOCUMENT_ID_ATTRIBUTE));
						String updateUser = ((String) de.getAttribute(UPDATE_USER_ATTRIBUTE));
						long updateTime = Long.parseLong((String) de.getAttribute(UPDATE_TIME_ATTRIBUTE));
						int version = Integer.parseInt((String) de.getAttribute(DOCUMENT_VERSION_ATTRIBUTE));
						
						//	import local updates only, as importing remote updates might create new update IDs and thus cause unnecessary traffic
						if (!updateUser.startsWith("DRS.")) {
							DioDocumentEvent dde = new DioDocumentEvent(updateUser, docId, null, version, DioDocumentEvent.UPDATE_TYPE, GoldenGateDIO.class.getName(), updateTime, null);
							if (deDuplicator.add(dde.eventId)) {
								((GoldenGateRES) res).publishEvent(dde);
								this.loopRoundComplete("Got " + existingEventCount + " existing update events, enqueued " + (++newEventCount) + " new ones");
							}
						}
					}
					while (this.continueAction() && ((de = dl.getNextDocument()) != null));
				}
				else this.log(" There are no documents available in DIO.");
			}
		};
		cal.add(ca);
		
		//	request for document
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				String passPhraseHash = input.readLine();
				String docId = input.readLine();
				
				if (!passPhraseHash.equals("" + (docId + localPassPhrase).hashCode())) {
					output.write("Invalid pass phrase for loading document with ID " + docId);
					output.newLine();
					return;
				}
				
				DocumentReader dr = dio.getDocumentAsStream(docId);
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
		
		//	request for document list
		ca = new ComponentActionNetwork() {
			public String getActionCommand() {
				return GET_DOCUMENT_LIST;
			}
			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
				DocumentList dl = dio.getDocumentListFull();
				
				output.write(GET_DOCUMENT_LIST);
				output.newLine();
				
				dl.writeData(output);
			}
		};
		cal.add(ca);
		
		//	re-read pass phrases
		ca = new ComponentActionConsole() {
			public String getActionCommand() {
				return READ_PASS_PHRASES_COMMAND;
			}
			public String[] getExplanation() {
				String[] explanation = {
						READ_PASS_PHRASES_COMMAND,
						"Re-read the local and remote pass phrases from the config file.",
					};
				return explanation;
			}
			public void performActionConsole(String[] arguments) {
				if (arguments.length == 0) {
					readPassPhrases();
					System.out.println(" Pass phrases re-read.");
				}
				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
			}
		};
		cal.add(ca);
		
		//	diff events with remote DRS
		cal.add(this.diffAction);
		
		//	sync documents with a remote DRS
		cal.add(this.syncAction);
		
		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
	}
	
	/**
	 * Retrieve a document from a connected remote DRS.
	 * @param docId the ID of the document
	 * @param remoteAddress the address of the remote GoldenGATE Server
	 * @param remotePort the port of the remote GoldenGATE Server
	 * @param remoteDomain the name of the remote DRS domain
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	private MutableAnnotation getDocument(String docId, String remoteAddress, int remotePort, String remoteDomain) throws IOException {
		ServerConnection sc = ((remotePort == -1) ? ServerConnection.getServerConnection(remoteAddress) : ServerConnection.getServerConnection(remoteAddress, remotePort));
		String passPhrase = this.remotePassPhrases.getProperty(remoteDomain, defaultPassPhrase);
		Connection con = null;
		
		//	get document
		MutableAnnotation doc;
		try {
			con = sc.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_DOCUMENT);
			bw.newLine();
			bw.write("" + (docId + passPhrase).hashCode());
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_DOCUMENT.equals(error))
				doc = GenericGamtaXML.readDocument(br);
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
		
		/* get remote provenance attributes (if not set, remote domain is
		 * immediate source of update, and we default to respective attributes
		 * or values) */
		String remoteUpdateUser = ((String) doc.getAttribute(ORIGINAL_UPDATE_USER_ATTRIBUTE, doc.getAttribute(UPDATE_USER_ATTRIBUTE)));
		String remoteUpdateTime = ((String) doc.getAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, doc.getAttribute(UPDATE_TIME_ATTRIBUTE)));
		String remoteUpdateDomain = ((String) doc.getAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE, remoteDomain));
		
		/* set provenance attributes (DIO will overwrite update user and time,
		 * but we need the user name for storing the document) */
		doc.setAttribute(UPDATE_USER_ATTRIBUTE, remoteUpdateUser);
		doc.setAttribute(UPDATE_TIME_ATTRIBUTE, remoteUpdateTime);
		doc.setAttribute(ORIGINAL_UPDATE_USER_ATTRIBUTE, remoteUpdateUser);
		doc.setAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, remoteUpdateTime);
		doc.setAttribute(ORIGINAL_UPDATE_DOMAIN_ATTRIBUTE, remoteUpdateDomain);
		
		//	finally ...
		return doc;
	}
	
	private DocumentList getDocumentList(String remoteAddress, int remotePort) throws IOException {
		ServerConnection sc = ((remotePort == -1) ? ServerConnection.getServerConnection(remoteAddress) : ServerConnection.getServerConnection(remoteAddress, remotePort));
		Connection con = sc.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(GET_DOCUMENT_LIST);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (GET_DOCUMENT_LIST.equals(error))
			return DocumentList.readDocumentList(br);
		else {
			con.close();
			throw new IOException(error);
		}
	}
}
//public class GoldenGateDRS extends AbstractGoldenGateServerComponent implements GoldenGateDioConstants {
//	
//	private static final String ORIGINAL_UPDATE_TIME_ATTRIBUTE = "originalUpdateTime";
//	
//	private static final String GET_DOCUMENT = "DRS_GET_DOCUMENT";
//	private static final String GET_DOCUMENT_LIST = "DRS_GET_DOCUMENT_LIST";
//	
//	private static final String defaultPassPhrase = "DRS provides remote access!";
//	
//	/** The GoldenGATE DIO to work with */
//	protected GoldenGateDIO dio;
//	private GoldenGateRES res;
//	
//	private String localPassPhrase = null;
//	private Properties remotePassPhrases = new Properties();
//	
//	/** Constructor passing 'DRS' as the letter code to super constructor
//	 */
//	public GoldenGateDRS() {
//		super("DRS");
//	}
//	
//	/**
//	 * This implementation reads the pass phrases. Sub classes overwriting this
//	 * method have to make the super call.
//	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#initComponent()
//	 */
//	protected void initComponent() {
//		this.readPassPhrases();
//	}
//	
//	private void readPassPhrases() {
//		Settings passPhrases = Settings.loadSettings(new File(this.dataPath, "passPhrases.cnfg"));
//		
//		//	load pass phrases for incoming connections
//		this.localPassPhrase = passPhrases.getSetting("localPassPhrase", defaultPassPhrase);
//		
//		//	load pass phrases for accessing remote DRS's
//		Settings remotePassPhrases = passPhrases.getSubset("remotePassPhrase");
//		String[] remoteDomainNames = remotePassPhrases.getKeys();
//		for (int d = 0; d < remoteDomainNames.length; d++)
//			this.remotePassPhrases.setProperty(remoteDomainNames[d], remotePassPhrases.getSetting(remoteDomainNames[d]));
//	}
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
//	 */
//	public void link() {
//		
//		//	get DIO
//		this.dio = ((GoldenGateDIO) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateDIO.class.getName()));
//		
//		//	check success
//		if (this.dio == null) throw new RuntimeException(GoldenGateDIO.class.getName());
//		
//		//	hook up to local RES
//		this.res = ((GoldenGateRES) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateRES.class.getName()));
//		
//		//	check success
//		if (this.res == null) throw new RuntimeException(GoldenGateRES.class.getName());
//	}
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
//	 */
//	public void linkInit() {
//		
//		//	listen for events
//		GoldenGateServerEventService.addServerEventListener(new ResRemoteEventListener() {
//			public void notify(ResRemoteEvent rre) {
//				if (!DioDocumentEvent.class.getName().equals(rre.eventClassName))
//					return;
//				
//				//	reconstruct and handle document event
//				DioDocumentEvent dde = DioDocumentEvent.parseEvent(rre.paramString);
//				handleDocumentEvent(rre, dde);
//			}
//		});
//		
//		//	prevent remote document updates from being re-published
//		this.res.addEventFilter(new ResEventFilter() {
//			public boolean allowPublishEvent(GoldenGateServerEvent gse) {
//				if ((gse instanceof DioDocumentEvent) && ((DioDocumentEvent) gse).user.startsWith("DRS."))
//					return false;
//				return true;
//			}
//		});
//	}
//	
//	/**
//	 * Handle a document event. Sub classes can overwrite this method to filter
//	 * events and then call this implementation.
//	 * @param rre the remote event the document event was wrapped in
//	 * @param dde the document event to handle
//	 */
//	protected void handleDocumentEvent(ResRemoteEvent rre, DioDocumentEvent dde) {
//		
//		//	handle update event
//		if (dde.type == DioDocumentEvent.UPDATE_TYPE) {
//			try {
//				System.out.println("GoldenGateDRS: updating from " + rre.sourceDomainAlias + " (" + rre.sourceDomainAddress + ":" + rre.sourceDomainPort + ") ...");
//				
//				//	get document
//				QueriableAnnotation doc = getDocument(dde.documentId, rre.sourceDomainAddress, rre.sourceDomainPort, rre.sourceDomainAlias);
//				
//				//	if original update time not set, document comes from its home domain ==> use update time
//				if (!doc.hasAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE))
//					doc.setAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, doc.getAttribute(UPDATE_TIME_ATTRIBUTE));
//				
//				//	store document
//				dio.updateDocument(("DRS." + rre.originDomainName), dde.documentId, doc, null);
//			}
//			catch (IOException ioe) {
//				System.out.println("GoldenGateDRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while updating document " + dde.documentId + ".");
//				ioe.printStackTrace(System.out);
//			}
//		}
//		
//		//	handle delete event
//		else if (dde.type == DioDocumentEvent.DELETE_TYPE) {
//			try {
//				dio.deleteDocument(("DRS." + rre.originDomainName), dde.documentId, null);
//			}
//			catch (IOException ioe) {
//				System.out.println("GoldenGateDRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document.");
//				ioe.printStackTrace(System.out);
//			}
//		}
//		
//		//	we are not interested in checkouts and releases (for now)
//	}
//	
//	private static final String READ_PASS_PHRASES_COMMAND = "readPassPhrases";
//	private static final String DIFF_FROM_DRS_COMMAND = "diff";
//	private static final String SYNC_WITH_DRS_COMMAND = "sync";
//	
//	private AsynchronousConsoleAction diffAction;
//	private AsynchronousConsoleAction syncAction;
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
//	 */
//	public ComponentAction[] getActions() {
//		ArrayList cal = new ArrayList();
//		ComponentAction ca;
//		
//		//	if local RES given, add console action for issuing updates events for all existing documents
//		if (this.res != null) {
//			ca = new AsynchronousConsoleAction("publishEvents", "Publish update events for the documents in the local DIO.", "update event", null, null) {
//				protected String[] getArgumentNames() {
//					return new String[0];
//				}
//				protected String checkArguments(String[] arguments) {
//					return ((arguments.length == 0) ? null : " Specify no arguments.");
//				}
//				protected void performAction(String[] arguments) throws Exception {
//					DocumentList dl = dio.getDocumentListFull();
//					if (dl.hasNextDocument()) {
//						DocumentListElement de = dl.getNextDocument();
//						RemoteEventList rel = ((GoldenGateRES) res).getEventList(0, GoldenGateDIO.class.getName());
//						HashSet deDuplicator = new HashSet();
//						while (rel.hasNextEvent()) {
//							ResRemoteEvent re = rel.getNextEvent();
//							if (re.type == DioDocumentEvent.UPDATE_TYPE)
//								deDuplicator.add(re.eventId);
//						}
//						int existingEventCount = deDuplicator.size();
//						this.enteringMainLoop("Got " + existingEventCount + " existing document update events, enqueued 0 new ones");
//						
//						int newEventCount = 0;
//						do {
//							String docId = ((String) de.getAttribute(DOCUMENT_ID_ATTRIBUTE));
//							String updateUser = ((String) de.getAttribute(UPDATE_USER_ATTRIBUTE));
//							long updateTime = Long.parseLong((String) de.getAttribute(UPDATE_TIME_ATTRIBUTE));
//							int version = Integer.parseInt((String) de.getAttribute(DOCUMENT_VERSION_ATTRIBUTE));
//							
//							//	import local updates only, as importing remote updates might create new update IDs and thus cause unnecessary traffic
//							if (!updateUser.startsWith("DRS.")) {
//								DioDocumentEvent dde = new DioDocumentEvent(updateUser, docId, null, version, DioDocumentEvent.UPDATE_TYPE, GoldenGateDIO.class.getName(), updateTime, null);
//								if (deDuplicator.add(dde.eventId)) {
//									((GoldenGateRES) res).publishEvent(dde);
//									this.loopRoundComplete("Got " + existingEventCount + " existing update events, enqueued " + (++newEventCount) + " new ones");
//								}
//							}
//						}
//						while (this.continueAction() && ((de = dl.getNextDocument()) != null));
//					}
//					else this.log(" There are no documents available in DIO.");
//				}
//			};
//			cal.add(ca);
//		}
//		
//		//	request for document
//		ca = new ComponentActionNetwork() {
//			public String getActionCommand() {
//				return GET_DOCUMENT;
//			}
//			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
//				String passPhraseHash = input.readLine();
//				String docId = input.readLine();
//				
//				if (!passPhraseHash.equals("" + (docId + localPassPhrase).hashCode())) {
//					output.write("Invalid pass phrase for loading document with ID " + docId);
//					output.newLine();
//					return;
//				}
//				
//				DocumentReader dr = dio.getDocumentAsStream(docId);
//				try {
//					output.write(GET_DOCUMENT);
//					output.newLine();
//					
//					char[] cbuf = new char[1024];
//					int read;
//					while ((read = dr.read(cbuf, 0, cbuf.length)) != -1)
//						output.write(cbuf, 0, read);
//					output.newLine();
//				}
//				catch (IOException ioe) {
//					output.write(ioe.getMessage());
//					output.newLine();
//				}
//				finally {
//					dr.close();
//				}
//			}
//		};
//		cal.add(ca);
//		
//		//	request for document list
//		ca = new ComponentActionNetwork() {
//			public String getActionCommand() {
//				return GET_DOCUMENT_LIST;
//			}
//			public void performActionNetwork(BufferedReader input, BufferedWriter output) throws IOException {
//				DocumentList dl = dio.getDocumentListFull();
//				
//				output.write(GET_DOCUMENT_LIST);
//				output.newLine();
//				
//				dl.writeData(output);
//			}
//		};
//		cal.add(ca);
//		
//		//	re-read pass phrases
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return READ_PASS_PHRASES_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = {
//						READ_PASS_PHRASES_COMMAND,
//						"Re-read the local and remote pass phrases from the config file.",
//					};
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length == 0) {
//					readPassPhrases();
//					System.out.println(" Pass phrases re-read.");
//				}
//				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
//			}
//		};
//		cal.add(ca);
//		
//		//	diff events with remote DRS
//		this.diffAction = new AsynchronousConsoleAction(DIFF_FROM_DRS_COMMAND, "Run a full diff with a specific remote GoldenGATE DRS, i.e., compare document update events and handle unhandled ones", "update event", null, null) {
//			protected String[] getArgumentNames() {
//				String[] args = {"remoteDomain"};
//				return args;
//			}
//			protected String[] getArgumentExplanation(String argument) {
//				if ("remoteDomain".equals(argument)) {
//					String[] explanation = {"The alias of the remote GoldenGATE DRS to compare the document list with"};
//					return explanation;
//				}
//				else return super.getArgumentExplanation(argument);
//			}
//			protected void checkRunnable() {
//				if (syncAction.isRunning())
//					throw new RuntimeException("Document list sync running, diff cannot run in parallel");
//			}
//			protected String checkArguments(String[] arguments) {
//				if (arguments.length != 1)
//					return ("Invalid arguments for '" + this.getActionCommand() + "', specify the alias of the DRS to diff with as the only argument.");
//				String address = res.getRemoteDomainAddress(arguments[0]);
//				if (address == null)
//					return ("No remote DRS found for name " + arguments[0]);
//				else return null;
//			}
//			protected void performAction(String[] arguments) throws Exception {
//				String remoteDomain = arguments[0];
//				
//				//	get remote events
//				RemoteEventList rel = res.getRemoteEventList(remoteDomain, 0, GoldenGateDIO.class.getName());
//				if (rel == null)
//					return;
//				this.enteringMainLoop("Got event list from " + remoteDomain);
//				int handleCount = 0;
//				int skipCount = 0;
//				
//				//	do diff
//				while (this.continueAction() && rel.hasNextEvent()) {
//					ResRemoteEvent rre = rel.getNextEvent();
//					if (!DioDocumentEvent.class.getName().equals(rre.eventClassName))
//						continue;
//					
//					//	reconstruct document event
//					DioDocumentEvent dde = DioDocumentEvent.parseEvent(rre.paramString);
//					
//					//	check against local update time
//					Properties docAttributes = dio.getDocumentAttributes(dde.documentId);
//					if (docAttributes != null) {
//						long updateTime = Long.parseLong(docAttributes.getProperty(ORIGINAL_UPDATE_TIME_ATTRIBUTE, docAttributes.getProperty(UPDATE_TIME_ATTRIBUTE, "0")));
//						if (rre.eventTime < updateTime) {
//							skipCount++;
//							continue;
//						}
//					}
//					
//					//	handle document event
//					handleDocumentEvent(new ResRemoteEvent(rre, remoteDomain, res.getRemoteDomainAddress(remoteDomain), res.getRemoteDomainPort(remoteDomain)), dde);
//					handleCount++;
//					
//					//	update status
//					this.loopRoundComplete("Handled " + handleCount + " update events, skipped " + skipCount + " ones.");
//				}
//			}
//		};
//		cal.add(this.diffAction);
//		
//		//	sync documents with a remote DRS
//		this.syncAction = new AsynchronousConsoleAction(SYNC_WITH_DRS_COMMAND, "Run a full sync with a specific remote GoldenGATE DRS, i.e., compare the document lists and fetch missing updates", "document list", null, null) {
//			protected String[] getArgumentNames() {
//				String[] args = {"remoteDomain", "mode"};
//				return args;
//			}
//			protected String[] getArgumentExplanation(String argument) {
//				if ("remoteDomain".equals(argument)) {
//					String[] explanation = {"The alias of the remote GoldenGATE DRS to compare the document list with"};
//					return explanation;
//				}
//				else if ("mode".equals(argument)) {
//					String[] explanation = {"The sync mode: '-u' for 'update' (the default), '-d' for 'delete', or '-ud' for both"};
//					return explanation;
//				}
//				else return super.getArgumentExplanation(argument);
//			}
//			protected void checkRunnable() {
//				if (diffAction.isRunning())
//					throw new RuntimeException("Update event diff running, sync cannot run in parallel");
//			}
//			protected String checkArguments(String[] arguments) {
//				if ((arguments.length < 1) || (arguments.length > 2)) 
//					return ("Invalid arguments for '" + this.getActionCommand() + "', specify the alias of the DRS to sync with, and optionally the sync mode, as the only arguments.");
//				String address = res.getRemoteDomainAddress(arguments[0]);
//				if (address == null)
//					return ("No remote DRS found for name " + arguments[0]);
//				else if ((arguments.length == 2) && ("-u -d -ud".indexOf(arguments[1]) == -1))
//					return ("Invalid sync mode " + arguments[1]);
//				else return null;
//			}
//			protected void performAction(String[] arguments) throws Exception {
//				String remoteDomain = arguments[0];
//				boolean update = ((arguments.length == 1) || (arguments[1].indexOf("u") != -1));
//				boolean delete = ((arguments.length == 2) && (arguments[1].indexOf("d") != -1));
//				
//				//	get remote domain access data
//				String remoteAddress = res.getRemoteDomainAddress(remoteDomain);
//				int remotePort = res.getRemoteDomainPort(remoteDomain);
//				
//				//	get document list from remote domain, and index document records by ID
//				DocumentList remoteDl = getDocumentList(remoteAddress, remotePort);
//				HashMap remoteDlesById = new HashMap();
//				while (remoteDl.hasNextDocument()) {
//					DocumentListElement dle = remoteDl.getNextDocument();
//					remoteDlesById.put(((String) dle.getAttribute(DOCUMENT_ID_ATTRIBUTE)), dle);
//				}
//				
//				//	iterate over local document list, collecting IDs of documents to update or delete
//				HashSet updateDocIDs = new HashSet();
//				HashSet deleteDocIDs = new HashSet();
//				DocumentList localDl = dio.getDocumentListFull();
//				while (localDl.hasNextDocument()) {
//					DocumentListElement localDle = localDl.getNextDocument();
//					String docId = ((String) localDle.getAttribute(DOCUMENT_ID_ATTRIBUTE));
//					DocumentListElement remoteDle = ((DocumentListElement) remoteDlesById.remove(docId));
//					
//					//	this one doesn't even exist in the remote domain
//					if (remoteDle == null) {
//						if (delete)
//							deleteDocIDs.add(docId);
//						continue;
//					}
//					
//					//	extract update timestamps for comparison
//					long localUpdateTime;
//					long remoteUpdateTime;
//					try {
//						localUpdateTime = Long.parseLong((String) localDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
//						remoteUpdateTime = Long.parseLong((String) remoteDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
//					}
//					catch (Exception e) {
//						this.log(("Could not parse update timestamps for document '" + docId + "'"), e);
//						continue;
//					}
//					
//					//	remote version is newer than local one, even with a one second tolerance), mark for update
//					if (update && ((localUpdateTime + 1000) < remoteUpdateTime))
//						updateDocIDs.add(docId);
//				}
//				
//				//	add updates for new document not yet available locally
//				if (update)
//					updateDocIDs.addAll(remoteDlesById.keySet());
//				
//				//	do updates and deletions
//				int updateCount = updateDocIDs.size();
//				int deleteCount = deleteDocIDs.size();
//				this.enteringMainLoop("Got event list from " + remoteDomain + ", " + updateCount + " updates, " + deleteCount + " deletions");
//				while (this.continueAction() && ((updateDocIDs.size() + deleteDocIDs.size()) != 0)) {
//					
//					//	do deletions first ...
//					if (deleteDocIDs.size() != 0) {
//						String docId = ((String) deleteDocIDs.iterator().next());
//						deleteDocIDs.remove(docId);
//						System.out.println("GoldenGateDRS: forwarding deletion from " + remoteDomain + " (" + remoteAddress + ":" + remotePort + ") ...");
//						try {
//							
//							//	get update user, and reuse if starting with 'DRS.'
//							Properties docAttributes = dio.getDocumentAttributes(docId);
//							String updateUser = docAttributes.getProperty(UPDATE_USER_ATTRIBUTE);
//							if ((updateUser == null) || !updateUser.startsWith("DRS."))
//								updateUser = ("DRS." + remoteDomain);
//							
//							//	delete document
//							dio.deleteDocument(updateUser, docId, null);
//						}
//						catch (IOException ioe) {
//							System.out.println("GoldenGateDRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while deleting document " + docId + ".");
//							ioe.printStackTrace(System.out);
//						}
//					}
//					
//					//	... and updates second
//					else if (updateDocIDs.size() != 0) {
//						String docId = ((String) updateDocIDs.iterator().next());
//						updateDocIDs.remove(docId);
//						System.out.println("GoldenGateDRS: getting update from " + remoteDomain + " (" + remoteAddress + ":" + remotePort + ") ...");
//						try {
//							
//							//	get document
//							QueriableAnnotation doc = getDocument(docId, remoteAddress, remotePort, remoteDomain);
//							
//							//	get update user, and reuse if starting with 'DRS.'
//							String updateUser = ((String) doc.getAttribute(UPDATE_USER_ATTRIBUTE));
//							if ((updateUser == null) || !updateUser.startsWith("DRS."))
//								updateUser = ("DRS." + remoteDomain);
//								
//							//	if original update time not set, document comes from its home domain ==> use update time
//							if (!doc.hasAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE))
//								doc.setAttribute(ORIGINAL_UPDATE_TIME_ATTRIBUTE, doc.getAttribute(UPDATE_TIME_ATTRIBUTE));
//							
//							//	store document
//							dio.updateDocument(updateUser, docId, doc, null);
//						}
//						catch (IOException ioe) {
//							System.out.println("GoldenGateDRS: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while updating document " + docId + ".");
//							ioe.printStackTrace(System.out);
//						}
//					}
//					
//					//	update status
//					this.loopRoundComplete("Handled " + (updateCount - updateDocIDs.size()) + " of " + updateCount + " updates, " + (deleteCount - deleteDocIDs.size()) + " of " + deleteCount + " deletions.");
//				}
//			}
//		};
//		cal.add(this.syncAction);
//		
//		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
//	}
//	
//	/**
//	 * Retrieve a document from a connected remote DRS.
//	 * @param docId the ID of the document
//	 * @param remoteAddress the address of the remote GoldenGATE Server
//	 * @param remotePort the port of the remote GoldenGATE Server
//	 * @param remoteDomain the name of the remote DRS domain
//	 * @return the document with the specified ID
//	 * @throws IOException
//	 */
//	protected MutableAnnotation getDocument(String docId, String remoteAddress, int remotePort, String remoteDomain) throws IOException {
//		ServerConnection sc = ((remotePort == -1) ? ServerConnection.getServerConnection(remoteAddress) : ServerConnection.getServerConnection(remoteAddress, remotePort));
//		String passPhrase = this.remotePassPhrases.getProperty(remoteDomain, defaultPassPhrase);
//		Connection con = null;
//		try {
//			con = sc.getConnection();
//			BufferedWriter bw = con.getWriter();
//			
//			bw.write(GET_DOCUMENT);
//			bw.newLine();
//			bw.write("" + (docId + passPhrase).hashCode());
//			bw.newLine();
//			bw.write(docId);
//			bw.newLine();
//			bw.flush();
//			
//			BufferedReader br = con.getReader();
//			String error = br.readLine();
//			if (GET_DOCUMENT.equals(error))
//				return GenericGamtaXML.readDocument(br);
//			else throw new IOException(error);
//		}
//		finally {
//			if (con != null)
//				con.close();
//		}
//	}
//	
//	private DocumentList getDocumentList(String remoteAddress, int remotePort) throws IOException {
//		ServerConnection sc = ((remotePort == -1) ? ServerConnection.getServerConnection(remoteAddress) : ServerConnection.getServerConnection(remoteAddress, remotePort));
//		Connection con = sc.getConnection();
//		BufferedWriter bw = con.getWriter();
//		
//		bw.write(GET_DOCUMENT_LIST);
//		bw.newLine();
//		bw.flush();
//		
//		BufferedReader br = con.getReader();
//		String error = br.readLine();
//		if (GET_DOCUMENT_LIST.equals(error))
//			return DocumentList.readDocumentList(br);
//		else {
//			con.close();
//			throw new IOException(error);
//		}
//	}
//}