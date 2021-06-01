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
package de.uka.ipd.idaho.goldenGateServer.dio.connectors;

import java.io.IOException;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListBuffer;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent.ComponentActionConsole;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent.DioDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP;
import de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding;
import de.uka.ipd.idaho.goldenGateServer.util.AsynchronousConsoleAction;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * GoldenGATE Server Exporter binding for GoldenGATE Document IO Service. It
 * provides an asynchronous console action for re-ingesting the entire document
 * collection from the backing DIO.
 * 
 * @author sautter
 */
public class GoldenGateDioEXP extends GoldenGateExpBinding {
	private GoldenGateDIO dio;
	
	/**
	 * Constructor
	 * @param exp the GoldenGATE EXP the binding belongs to
	 */
	public GoldenGateDioEXP(GoldenGateEXP exp) {
		super(exp);
		
		//	get document IO server
		this.dio = ((GoldenGateDIO) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateDIO.class.getName()));
		
		//	check success
		if (this.dio == null) throw new RuntimeException(GoldenGateDIO.class.getName());
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#connect()
	 */
	public void connect() {
		
		//	establish DIO uplink
		this.dio.addDocumentEventListener(new DioDocumentEventListener() {
			public void documentCheckedOut(DioDocumentEvent dse) {}
			public void documentReleased(DioDocumentEvent dse) {}
			public void documentUpdated(DioDocumentEvent dse) {
				host.documentUpdated(dse.dataId, dse.document, dse.user);
			}
			public void documentDeleted(DioDocumentEvent dse) {
				host.documentDeleted(dse.dataId, dse.user);
			}
		});
	}
	
	private static final String UPDATE_DOCS_COMMAND = "updateDocs";
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#getReingestAction()
	 */
	public ComponentActionConsole getReingestAction() {
		return new AsynchronousConsoleAction(UPDATE_DOCS_COMMAND, "Re-ingest the whole DIO document collection (high effort).", "document", null, null) {
			protected String[] getArgumentNames() {
				return new String[0];
			}
			protected String checkArguments(String[] arguments) {
				if (arguments.length != 0)
					return ("Invalid arguments for '" + this.getActionCommand() + "', specify no argument.");
				else return null;
			}
			protected String getActionName() {
				return (host.getLetterCode() + "." + super.getActionName());
			}
			protected void performAction(String[] arguments) throws Exception {
//				DioDocumentList docList = dio.getDocumentListFull();
				DocumentListBuffer dlb = new DocumentListBuffer(dio.getDocumentListFull());
//				this.enteringMainLoop("Got document list");
				this.enteringMainLoop("Got list with " + dlb.size() + " documents");
				int count = 0;
//				while (this.continueAction() && docList.hasNextDocument()) {
				for (int d = 0; this.continueAction() && (d < dlb.size()); d++) {
//					DocumentListElement dle = docList.getNextDocument();
					StringTupel docData = dlb.get(d);
//					String docId = ((String) dle.getAttribute(LiteratureConstants.DOCUMENT_ID_ATTRIBUTE));
					String docId = docData.getValue(LiteratureConstants.DOCUMENT_ID_ATTRIBUTE);
					if (docId != null) {
//						host.documentUpdated(docId, null, ((String) dle.getAttribute(GoldenGateDIO.UPDATE_USER_ATTRIBUTE)));
						host.documentUpdated(docId, null, docData.getValue(GoldenGateDIO.UPDATE_USER_ATTRIBUTE));
						count++;
						this.loopRoundComplete("Re-ingested document ID '" + docId + "', " + count + " of " + dlb.size() + " documents in total.");
					}
				}
			}
		};
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#isDocumentAvailable(java.lang.String)
	 */
	public boolean isDocumentAvailable(String docId) {
		return this.dio.isDocumentAvailable(docId);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#getDocumentAttributes(java.lang.String)
	 */
	public Properties getDocumentAttributes(String docId) throws IOException {
		return this.dio.getDocumentAttributes(docId);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.exp.GoldenGateEXP.GoldenGateExpBinding#getDocument(java.lang.String)
	 */
	public QueriableAnnotation getDocument(String docId) throws IOException {
		return this.dio.getDocument(docId);
	}
}