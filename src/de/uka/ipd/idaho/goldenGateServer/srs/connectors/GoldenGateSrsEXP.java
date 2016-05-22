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
package de.uka.ipd.idaho.goldenGateServer.srs.connectors;

import java.io.IOException;
import java.util.LinkedList;

import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent.ComponentActionConsole;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSRS;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants.SrsDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants.SrsDocumentEvent.SrsDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousConsoleAction;

/**
 * GoldenGATE Server Exporter binding for GoldenGATE Search and Retrieval
 * Service. It provides an asynchronous console action for re-ingesting the
 * entire document collection from the backing SRS.
 * 
 * @author sautter
 */
public class GoldenGateSrsEXP extends GoldenGateExpBinding {
	
	private GoldenGateSRS srs;
	
	/**
	 * Constructor
	 * @param exp the GoldenGATE EXP the binding belongs to
	 */
	public GoldenGateSrsEXP(GoldenGateEXP exp) {
		super(exp);
		
		//	get document IO server
		this.srs = ((GoldenGateSRS) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateSRS.class.getName()));
		
		//	check success
		if (this.srs == null) throw new RuntimeException(GoldenGateSRS.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#connect()
	 */
	public void connect() {
		
		//	establish DIO uplink
		this.srs.addDocumentEventListener(new SrsDocumentEventListener() {
			public void documentUpdated(SrsDocumentEvent dse) {
				host.documentUpdated(dse.documentId, dse.document);
			}
			public void documentDeleted(SrsDocumentEvent dse) {
				host.documentDeleted(dse.documentId);
			}
		});
	}
	
	private static final String UPDATE_DOCS_COMMAND = "updateDocs";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#getReingestAction()
	 */
	public ComponentActionConsole getReingestAction() {
		return new AsynchronousConsoleAction(UPDATE_DOCS_COMMAND, "Re-ingest the whole SRS document collection (high effort).", "document", null, null) {
			protected String[] getArgumentNames() {
				return new String[0];
			}
			protected String checkArguments(String[] arguments) {
				if (arguments.length != 0)
					return ("Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
				else return null;
			}
			protected void performAction(String[] arguments) throws Exception {
				DocumentList mDocList = srs.getDocumentList(null);
				
				LinkedList mDocIdList = new LinkedList();
				while (mDocList.hasNextElement()) {
					DocumentListElement mDle = mDocList.getNextDocumentListElement();
					String mDocId = ((String) mDle.getAttribute(LiteratureConstants.DOCUMENT_ID_ATTRIBUTE));
					if (mDocId != null)
						mDocIdList.addLast(mDocId);
				}
				this.enteringMainLoop("Got " + mDocIdList.size() + " master document IDs");
				int count = 0;
				
				while (this.continueAction() && (mDocIdList.size() != 0)) {
					String mDocId = ((String) mDocIdList.removeFirst());
					DocumentList docList = srs.getDocumentList(mDocId);
					int subCount = 0;
					while (this.continueAction() && docList.hasNextElement()) {
						DocumentListElement dle = docList.getNextDocumentListElement();
						String docId = ((String) dle.getAttribute(LiteratureConstants.DOCUMENT_ID_ATTRIBUTE));
						if (docId != null) {
							host.documentUpdated(docId, null);
							count++;
							subCount++;
							this.loopRoundComplete("Re-ingested " + subCount + " documents for master document ID '" + mDocId + "', " + count + " documents in total.");
						}
					}
				}
			}
		};
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#isDocumentAvailable(java.lang.String)
	 */
	public boolean isDocumentAvailable(String docId) {
		return this.srs.isDocumentAvailable(docId);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#getDocument(java.lang.String)
	 */
	public QueriableAnnotation getDocument(String docId) throws IOException {
		return this.srs.getDocument(docId);
	}
}