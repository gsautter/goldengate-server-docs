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

/**
 * Storage filters check if documents as a whole, or parts that have been split
 * from them, are suitable for being stored in the SRS document collection. This
 * may, for instance, be XML schema validation.
 * 
 * @author sautter
 * 
 */
public interface StorageFilter extends GoldenGateSrsPlugin {
	
	/**	check if a document is suitable for being stored in the SRS archive
	 * @param	doc		the document to check
	 * @return an error messages, or null, if the document is OK
	 * Note: if this method writes class or instance fields, it should be synchronized 
	 */
	public abstract String filter(QueriableAnnotation doc);
	
	/**	from an array of documents (result of splitting) pick the ones suitable for being stored in the collection
	 * @param	parts	the documents to filter
	 * @param	doc		the master document the parts belong to
	 * @return an array holding the parts that pass the filter
	 * Note: if this method writes class or instance fields, it should be synchronized 
	 */
	public abstract QueriableAnnotation[] filter(QueriableAnnotation[] parts, QueriableAnnotation doc);
}
