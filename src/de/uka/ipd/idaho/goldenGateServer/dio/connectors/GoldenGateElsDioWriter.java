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
package de.uka.ipd.idaho.goldenGateServer.dio.connectors;

import java.io.IOException;
import java.util.ArrayList;

import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationListener;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponentRegistry;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDIO;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DioDocumentEvent.DioDocumentEventListener;
import de.uka.ipd.idaho.goldenGateServer.els.GoldenGateELS.LinkWriter;

/**
 * @author sautter
 */
public class GoldenGateElsDioWriter extends LinkWriter {
	private GoldenGateDIO dio;
	private DioDocumentEventListener dioListener;
	
	/** zero-argument default constructor for class loading */
	public GoldenGateElsDioWriter() {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#init()
	 */
	protected void init() {
		
		//	link up to DIO
		this.dio = ((GoldenGateDIO) GoldenGateServerComponentRegistry.getServerComponent(GoldenGateDIO.class.getName()));
		if (this.dio == null)
			throw new RuntimeException("GoldenGateElsDio: cannot work without image document store");
		
		//	provide release notifications
		this.dioListener = new DioDocumentEventListener() {
			public void documentUpdated(DioDocumentEvent dse) {}
			public void documentReleased(DioDocumentEvent dse) {
				GoldenGateElsDioWriter.this.dataObjectUnlocked(dse.dataId);
			}
			public void documentDeleted(DioDocumentEvent dse) {}
			public void documentCheckedOut(DioDocumentEvent dse) {}
		};
		this.dio.addDocumentEventListener(this.dioListener);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#exit()
	 */
	protected void exit() {
		this.dio.removeDocumentEventListener(this.dioListener);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#getPriority()
	 */
	public int getPriority() {
		return 7; // we're not always the root, but sometimes ...
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#canHandleLinks(java.lang.String)
	 */
	public boolean canHandleLinks(String dataId) {
		return this.dio.isDocumentAvailable(dataId);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#canWriteLinks(java.lang.String)
	 */
	public boolean canWriteLinks(String dataId) {
		return this.dio.isDocumentEditable(dataId, UPDATE_USER_NAME);
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#providesUnlockNotifications()
	 */
	protected boolean providesUnlockNotifications() {
		return true; // yes, we do
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateELS.LinkWriter#writeLinks(java.lang.String, de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateElsConstants.ExternalLink[], de.uka.ipd.idaho.goldenGateServer.plazi.extLinks.GoldenGateElsConstants.ExternalLinkHandler[])
	 */
	public ExternalLink[] writeLinks(String dataId, ExternalLink[] links, ExternalLinkHandler[] handlers) {
		
		//	check out document
		DocumentRoot doc;
		DocumentChangeTracker docTracker;
		try {
			doc = this.dio.checkoutDocument(UPDATE_USER_NAME, dataId);
			docTracker = new DocumentChangeTracker();
			doc.addAnnotationListener(docTracker);
		}
		catch (IOException ioe) {
			this.host.logError("Could not check out docment '" + dataId + "': " + ioe.getMessage());
			this.host.logError(ioe);
			return links; // this will re-schedule
		}
		
		//	handle links
		try {
			ArrayList remainingLinks = new ArrayList();
			
			//	try handling on IMF document first
			this.host.logInfo("Handling " + links.length + " links on XML document:");
			for (int l = 0; l < links.length; l++) {
				ExternalLink link = links[l];
				for (int h = 0; h < handlers.length; h++)
					if (handlers[h].handleLink(doc, link)) {
						link = null;
						break;
					}
				if (link == null)
					this.host.logInfo(" - handled " + links[l].type + " " + links[l].link);
				else remainingLinks.add(link);
			}
			
			//	anything left?
			if (remainingLinks.size() < links.length)
				links = ((ExternalLink[]) remainingLinks.toArray(new ExternalLink[remainingLinks.size()]));
			remainingLinks.clear();
			this.host.logInfo(" ==> " + links.length + " links remaining");
			
			//	store any changes
			if (docTracker.docChanged)
				this.dio.updateDocument(this.getUpdateUserName(), UPDATE_USER_NAME, dataId, doc, this);
			
			//	return whatever is left
			return links;
		}
		
		catch (Exception e) {
			this.host.logError("Could not handle links on docment '" + dataId + "': " + e.getMessage());
			this.host.logError(e);
			return links; // this will re-schedule
		}
		
		//	make sure to remove listener and release document under all circumstances
		finally {
			doc.removeAnnotationListener(docTracker);
			this.dio.releaseDocument(UPDATE_USER_NAME, dataId);
		}
	}
	
	private static class DocumentChangeTracker implements AnnotationListener {
		boolean docChanged = false;
		public void annotationAdded(QueriableAnnotation doc, Annotation annotation) {
			this.docChanged = true;
		}
		public void annotationRemoved(QueriableAnnotation doc, Annotation annotation) {
			this.docChanged = true;
		}
		public void annotationTypeChanged(QueriableAnnotation doc, Annotation annotation, String oldType) {
			this.docChanged = true;
		}
		public void annotationAttributeChanged(QueriableAnnotation doc, Annotation annotation, String attributeName, Object oldValue) {
			this.docChanged = true;
		}
	}
}
