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
package de.uka.ipd.idaho.goldenGateServer.dio.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.util.HashSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.XmlDocumentReader;
import de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentList;


/**
 * Servlet for exposing the document collection hosted in a GoldenGATE DIO in a
 * read-only fashion, free of login.
 * 
 * @author sautter
 */
public class DocumentServlet extends GgServerClientServlet implements GoldenGateDioConstants {

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String docId = request.getPathInfo();
		
		//	request for document list
		if (docId == null) {
			DocumentList dl = this.getDocumentList();
			response.setContentType("text/xml");
			response.setHeader("Cache-Control", "no-cache");
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), ENCODING));
			dl.writeXml(out);
			out.flush();
		}
		
		//	request for document
		else {
			while (docId.startsWith("/"))
				docId = docId.substring(1);
			
			int version;
			if (docId.indexOf('/') == -1)
				version = 0;
			
			else {
				try {
					version = Integer.parseInt(docId.substring(docId.indexOf('/') + 1));
					docId = docId.substring(0, docId.indexOf('/'));
				}
				catch (NumberFormatException nfe) {
					response.sendError(HttpServletResponse.SC_BAD_REQUEST);
					return;
				}
			}
			
			DocumentReader dr = this.getDocumentAsStream(docId, version);
			response.setContentType("text/xml");
			response.setHeader("Cache-Control", "no-cache");
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), ENCODING));
			XmlDocumentReader xdr = new XmlDocumentReader(dr, null, new HashSet() {
				public boolean contains(Object o) {
					return ((o != null) && (o instanceof String) && !((String) o).startsWith("_"));
				}
			});
			char[] cbuf = new char[1024];
			int read;
			while ((read = xdr.read(cbuf, 0, cbuf.length)) != -1)
				out.write(cbuf, 0, read);
			out.flush();
			xdr.close();
		}
	}
	
	private DocumentList getDocumentList() throws IOException {
		final Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(GET_DOCUMENT_LIST);
		bw.newLine();
		bw.write(DOCUMENT_SERVLET_SESSION_ID);
		bw.newLine();
		bw.flush();
		
		final BufferedReader br = con.getReader();
		String error = br.readLine();
		if (GET_DOCUMENT_LIST.equals(error))
			return DocumentList.readDocumentList(new Reader() {
				public void close() throws IOException {
					br.close();
					con.close();
				}
				public int read(char[] cbuf, int off, int len) throws IOException {
					return br.read(cbuf, off, len);
				}
			});
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	private DocumentReader getDocumentAsStream(String documentId, int version) throws IOException {
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_DOCUMENT_AS_STREAM);
			bw.newLine();
			bw.write(DOCUMENT_SERVLET_SESSION_ID);
			bw.newLine();
			bw.write(documentId + ((version == 0) ? "" : ("." + version)));
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_DOCUMENT_AS_STREAM.equals(error)) {
				int docLength = Integer.parseInt(br.readLine());
				return new DocumentReader(br, docLength);
			}
			
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
}