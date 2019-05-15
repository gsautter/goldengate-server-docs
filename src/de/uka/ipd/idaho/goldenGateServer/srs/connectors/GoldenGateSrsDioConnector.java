///*
// * Copyright (c) 2006-, IPD Boehm, Universitaet Karlsruhe (TH) / KIT, by Guido Sautter
// * All rights reserved.
// *
// * Redistribution and use in source and binary forms, with or without
// * modification, are permitted provided that the following conditions are met:
// *
// *     * Redistributions of source code must retain the above copyright
// *       notice, this list of conditions and the following disclaimer.
// *     * Redistributions in binary form must reproduce the above copyright
// *       notice, this list of conditions and the following disclaimer in the
// *       documentation and/or other materials provided with the distribution.
// *     * Neither the name of the Universität Karlsruhe (TH) nor the
// *       names of its contributors may be used to endorse or promote products
// *       derived from this software without specific prior written permission.
// *
// * THIS SOFTWARE IS PROVIDED BY UNIVERSITÄT KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
// * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
// * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// */
//package de.uka.ipd.idaho.goldenGateServer.srs.connectors;
//
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.LinkedHashSet;
//
//import de.uka.ipd.idaho.gamta.QueriableAnnotation;
//import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
//import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
//import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
//import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;
//import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;
//import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent;
//import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent.DioDocumentEventListener;
//import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentListElement;
//import de.uka.ipd.idaho.goldenGateServer.dio.util.AsynchronousDioAction;
//import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSRS;
//import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;
//
///**
// * Connector component to forward DocumentStorageEvents from GoldenGATE DIO to
// * GoldenGATE SRS. This has to be implemented in a separate server component in
// * order to allow SRS to exist even without DIO: If implemented directly in SRS,
// * loading SRS would result in a NoClassDefFoundError if DIO is not present.
// * With this separate implementation, the dependency on DIO is removed from the
// * SRS main class, so it will work without the DIO present.
// * 
// * @author sautter
// */
//public class GoldenGateSrsDioConnector extends AbstractGoldenGateServerComponent implements LiteratureConstants {
//	
//	private GoldenGateDIO dio;
//	private AsynchronousDioAction updateAction;
//	private AsynchronousDioAction diffAction;
//	private GoldenGateSRS srs;
//	
//	/** Constructor passing 'DIO-SRS' as the letter code to super constructor
//	 */
//	public GoldenGateSrsDioConnector() {
//		super("DIO-SRS");
//	}
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#link()
//	 */
//	public void link() {
//		
//		//	get document IO server
//		this.dio = ((GoldenGateDIO) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateDIO.class.getName()));
//		
//		//	check success
//		if (this.dio == null) throw new RuntimeException(GoldenGateDIO.class.getName());
//		
//		//	get storage & retrieval server
//		this.srs = ((GoldenGateSRS) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateSRS.class.getName()));
//		
//		//	check success
//		if (this.srs == null) throw new RuntimeException(GoldenGateSRS.class.getName());
//		
//		//	establish connection
//		this.dio.addDocumentEventListener(new DioDocumentEventListener() {
//			
//			/* (non-Javadoc)
//			 * @see de.uka.ipd.idaho.goldenGateServer.dio.DioDocumentStorageListener#documentCheckedOut(de.uka.ipd.idaho.goldenGateServer.dst.DocumentStorageEvent)
//			 */
//			public void documentCheckedOut(DioDocumentEvent dse) {}
//			
//			/* (non-Javadoc)
//			 * @see de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent.DioDocumentEventListener#documentUpdated(de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent)
//			 */
//			public void documentUpdated(final DioDocumentEvent dse) {
//				if (dse.sourceClassName.equals(GoldenGateDIO.class.getName()))
//					try {
//						srs.storeDocument(dse.document, new EventLogger() {
//							public void writeLog(String logEntry) {
//								if (logEntry != null)
//									dse.writeLog(logEntry);
//							}
//						});
//					}
//					catch (IOException ioe) {
//						dse.writeLog("Could not store document in SRS: " + ioe.getMessage());
//					}
//			}
//			
//			/* (non-Javadoc)
//			 * @see de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent.DioDocumentEventListener#documentDeleted(de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent)
//			 */
//			public void documentDeleted(final DioDocumentEvent dse) {
//				if (dse.sourceClassName.equals(GoldenGateDIO.class.getName()))
//					try {
//						srs.deleteDocument(dse.user, dse.documentId, new EventLogger() {
//							public void writeLog(String logEntry) {
//								if (logEntry != null)
//									dse.writeLog(logEntry);
//							}
//						});
//					}
//					catch (IOException ioe) {
//						dse.writeLog("Could not delete document from SRS: " + ioe.getMessage());
//					}
//			}
//			
//			/* (non-Javadoc)
//			 * @see de.uka.ipd.idaho.goldenGateServer.dio.DioDocumentStorageListener#documentReleased(de.uka.ipd.idaho.goldenGateServer.dst.DocumentStorageEvent)
//			 */
//			public void documentReleased(DioDocumentEvent dse) {}
//		});
//	}
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent#linkInit()
//	 */
//	public void linkInit() {
//		
//		//	create update action
//		this.updateAction = new AsynchronousDioAction(
//				null,
//				this.dio,
//				"Build the collection of retrievable documents in an SRS from the documents in a DIO. This is mainly for administrative purposes, namely for adding an SRS to an existing DIO that already has a document collection",
//				"collection",
//				this.dataPath,
//				"DioSrsCollectionUpdateLog"
//			) {
//			
//			protected void checkRunnable() {
//				if (diffAction.isRunning())
//					throw new RuntimeException("A collection diff is running, cannot update at the same time.");
//			}
//			
//			protected void update(StringTupel docData) throws IOException {
//				
//				//	get document from DIO
//				String docId = docData.getValue(DOCUMENT_ID_ATTRIBUTE);
//				QueriableAnnotation doc = dio.getDocument(docId);
//				this.log("    - document loaded");
//				
//				//	store document in SRS
//				srs.storeDocument(doc, new EventLogger() {
//					public void writeLog(String logEntry) {
//						if (logEntry != null)
//							log("    - " + logEntry);
//					}
//				});
//				this.log("    - document stored in SRS");
//				
//				//	give the others some time
//				try {
//					Thread.sleep(2000);
//				} catch (InterruptedException ie) {}
//			}
//		};
//		
//		//	create diff action
//		this.diffAction = new AsynchronousDioAction(
//				"diff",
//				this.dio,
//				"Diff the collection of retrievable documents in an SRS with the documents in a DIO. This is mainly for administrative purposes, namely for updating an SRS collection after changes to filters, etc.",
//				"collection",
//				this.dataPath,
//				"DioSrsCollectionDiffLog"
//			) {
//			
//			protected void checkRunnable() {
//				if (updateAction.isRunning())
//					throw new RuntimeException("A collection update is running, cannot diff at the same time.");
//			}
//			
//			protected void update(StringTupel docData) throws IOException {
//				
//				//	get document ID
//				String docId = docData.getValue(DOCUMENT_ID_ATTRIBUTE);
//				
//				//	check if document in SRS
//				de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentList dl = srs.getDocumentList(docId);
//				if (dl.hasNextElement()) {
//					this.log("    - document already stored in SRS");
//					return;
//				}
//				
//				//	get document from DIO
//				QueriableAnnotation doc = dio.getDocument(docId);
//				this.log("    - document loaded");
//				
//				//	store document in SRS
//				srs.storeDocument(doc, new EventLogger() {
//					public void writeLog(String logEntry) {
//						if (logEntry != null)
//							log("    - " + logEntry);
//					}
//				});
//				this.log("    - document stored in SRS");
//				
//				//	give the others some time
//				try {
//					Thread.sleep(2000);
//				} catch (InterruptedException ie) {}
//			}
//		};
//	}
//	
//	/* (non-Javadoc)
//	 * @see de.goldenGateScf.ServerComponent#getActions()
//	 */
//	public ComponentAction[] getActions() {
//		ArrayList cal = new ArrayList();
//		ComponentAction ca;
//		
//		//	initialize an SRS collection from a DIO's collection
//		ca = this.updateAction;
//		cal.add(ca);
//		
//		//	diff an SRS collection with a DIO's collection
//		ca = this.diffAction;
//		cal.add(ca);
//		
//		//	add filtered update actions (document ID, external ID, year of publication, checkin user)
//		cal.add(new FilteredUpdateAction(GoldenGateDIO.DOCUMENT_ID_ATTRIBUTE));
//		cal.add(new FilteredUpdateAction(GoldenGateDIO.EXTERNAL_IDENTIFIER_ATTRIBUTE));
//		cal.add(new FilteredUpdateAction(GoldenGateDIO.DOCUMENT_DATE_ATTRIBUTE));
//		cal.add(new FilteredUpdateAction(GoldenGateDIO.CHECKIN_USER_ATTRIBUTE));
//		
//		//	finally ...
//		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
//	}
//	
//	private LinkedHashSet updateDocIDs = new LinkedHashSet();
//	private FilteredUpdateThread updateThread = null;
//	
//	private class FilteredUpdateAction extends ComponentActionConsole  {
//		private String filterField;
//		private String command;
//		FilteredUpdateAction(String filterField) {
//			this.command = ("update" + filterField.substring(0, 1).toUpperCase() + filterField.substring(1));
//			this.filterField = filterField;
//		}
//		public String getActionCommand() {
//			return this.command;
//		}
//		public String[] getExplanation() {
//			String[] explanation = {(this.command + " <" + this.filterField + ">"), 
//					("Update all documents with a specific " + this.filterField + ":"),
//					("- <" + this.filterField + "> : the " + this.filterField + " of the document(s) to update")};
//			return explanation;
//		}
//		public void performActionConsole(String[] arguments) {
//			
//			//	display queue size for no arguments
//			if (arguments.length == 0) {
//				System.out.println(" " + updateDocIDs.size() + " document pending for update.");
//				return;
//			}
//			
//			//	report error
//			if (arguments.length != 1) {
//				System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify the " + this.filterField + " to update as the only argument.");
//				return;
//			}
//			
//			//	clear queue ('STOP' for a filter value is pretty unlikely, especially as we're matching case insensitive)
//			if ("STOP".equals(arguments[0])) {
//				synchronized (updateDocIDs) {
//					updateDocIDs.clear();
//				}
//				System.out.println(" Update queue cleared.");
//				return;
//			}
//			
//			//	parse filter value
//			String[] filterValues = arguments[0].toLowerCase().split("\\s+");
//			
//			//	run through document list and schedule matches
//			de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentList dl = dio.getDocumentListFull();
//			int updateDocCount = 0;
//			while (dl.hasNextDocument()) {
//				DocumentListElement dle = dl.getNextDocument();
//				String filterFieldValue = ((String) dle.getAttribute(this.filterField));
//				if (filterFieldValue == null)
//					continue;
//				filterFieldValue = filterFieldValue.toLowerCase();
//				int fvo = 0;
//				for (int f = 0; f < filterValues.length; f++)
//					if ((fvo = filterFieldValue.indexOf(filterValues[f], fvo)) == -1) {
//						dle = null;
//						break;
//					}
//				if (dle != null) synchronized (updateDocIDs) {
//					updateDocIDs.add(dle.getAttribute(GoldenGateDIO.DOCUMENT_ID_ATTRIBUTE));
//					updateDocCount++;
//				}
//			}
//			System.out.println(" Scheduled " + updateDocCount + " document(s) to update");
//			
//			//	start updater thread if none is running
//			synchronized (updateDocIDs) {
//				if ((updateDocIDs.size() != 0) && ((updateThread == null) || !updateThread.isAlive())) {
//					updateThread = new FilteredUpdateThread();
//					updateThread.start();
//				}
//			}
//		}
//	}
//	
//	private class FilteredUpdateThread extends Thread {
//		public void run() {
//			while (true) {
//				
//				//	get next document ID to update (use new iterator each time to avoid concurrent modification)
//				String docId;
//				synchronized (updateDocIDs) {
//					Iterator didit = updateDocIDs.iterator();
//					if (didit.hasNext()) {
//						docId = ((String) didit.next());
//						didit.remove();
//					}
//					else {
//						updateThread = null;
//						return;
//					}
//				}
//				
//				//	get and update document
//				try {
//					QueriableAnnotation doc = dio.getDocument(docId);
//					srs.storeDocument(doc, new EventLogger() {
//						public void writeLog(String logEntry) {
//							if (logEntry != null)
//								System.out.println("    - " + logEntry);
//						}
//					});
//				}
//				catch (Exception e) {
//					System.out.println("Error updating document  '" + docId + "': " + e.getMessage());
//					e.printStackTrace(System.out);
//				}
//				
//				//	give the others some time
//				try {
//					Thread.sleep(2000);
//				} catch (InterruptedException ie) {}
//			}
//		}
//	};
//}