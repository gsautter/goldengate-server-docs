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
package de.uka.ipd.idaho.goldenGateServer.srs;


import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerConstants.GoldenGateServerEvent.EventLogger;

/**
 * Document splitter may pre-process documents before they are entered into the
 * archive. This pre-processing includes, but is not restricted to, splitting
 * the documents into smaller portions (e.g. extracting individual articles from
 * a newspaper).
 * 
 * @author sautter
 */
public interface DocumentSplitter extends GoldenGateSrsPlugin {
	
	/**
	 * Split a document into smaller parts to be stored in the SRS archive.
	 * Typically, the returned documents are parts copied from the argument
	 * document. The docId attribute of the documents returned should be set to
	 * the annotation ID of the parts copied from the argument document, or be
	 * produced in another non-random, repeatable fashion. This is in order to
	 * facilitate part-wise updates.
	 * @param doc the document to process
	 * @return the parts extracted from the specified document Note: if this
	 *         method writes class or instance fields, it should be synchronized
	 */
	public QueriableAnnotation[] split(QueriableAnnotation doc);
	
	/**
	 * Split a document into smaller parts to be stored in the SRS archive.
	 * Typically, the returned documents are parts copied from the argument
	 * document. The docId attribute of the documents returned should be set to
	 * the annotation ID of the parts copied from the argument document, or be
	 * produced in another non-random, repeatable fashion. This is in order to
	 * facilitate part-wise updates.
	 * @param doc the document to process
	 * @param logger a logger for obtaining detailed information on the
	 *            splitting process (may be null)
	 * @return the parts extracted from the specified document Note: if this
	 *         method writes class or instance fields, it should be synchronized
	 */
	public abstract QueriableAnnotation[] split(QueriableAnnotation doc, EventLogger logger);
	
	/**
	 * Retrieve a nice name for the partial documents the splitter divides a
	 * master document into, e.g. 'article', if this splitter divides newspaper
	 * pages into individual articles.
	 * @return a nice name for the retrievable sub documents
	 */
	public abstract String getSplitResultLabel();
}