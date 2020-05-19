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
package de.uka.ipd.idaho.goldenGateServer.dio.client;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.gamta.util.ControllingProgressMonitor;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.gamta.util.GenericQueriableAnnotationWrapper;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DioDocumentList;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * A client for remotely accessing the documents in a GoldenGATE DIO.
 * 
 * @author sautter
 */
public class GoldenGateDioClient implements GoldenGateDioConstants {
	
	private AuthenticatedClient authClient;
	
	/**
	 * Constructor
	 * @param authClient the authenticated client to use for authentication and
	 *            connection
	 */
	public GoldenGateDioClient(AuthenticatedClient authClient) {
		this.authClient = authClient;
	}
	
	/**
	 * Obtain the list of documents available from the DIO. This method will
	 * return only documents that are not checked out by any user (including the
	 * user connected through this client). This is to prevent a document from
	 * being opened more than once simultaneously. If the user this client is
	 * authenticated with has administrative privileges, however, the list will
	 * contain all document, plus the information which user has currently
	 * checked them out. This is to enable administrators to release documents
	 * blocked by other users. If the size of the document list would exceed the
	 * limit configured in the backing DIO and the user this client is
	 * authenticated with does not have administrative privileges, the returned
	 * list will not contain any elements. However, the getListFieldValues()
	 * method of the document list will provide summary sets that can be used as
	 * filter suggestions for the user.
	 * @param pm a progress monitor to observe the loading process
	 * @return the list of documents available from the backing DIO
	 */
	public DioDocumentList getDocumentList(ProgressMonitor pm) throws IOException {
		return this.getDocumentList(null, pm);
	}
	
	/**
	 * Obtain the list of documents available from the DIO. This method will
	 * return only documents that are not checked out by any user (including the
	 * user connected through this client). This is to prevent a document from
	 * being opened more than once simultaneously. If the user this client is
	 * authenticated with has administrative privileges, however, the list will
	 * contain all document, plus the information which user has currently
	 * checked them out. This is to enable administrators to release documents
	 * blocked by other users. If the size of the document list would exceed the
	 * limit configured in the backing DIO and the user this client is
	 * authenticated with does not have administrative privileges, the returned
	 * list will not contain any elements. However, the getListFieldValues()
	 * method of the document list will provide summary sets that can be used as
	 * filter suggestions for the user.<br>
	 * The filter values specified for for string valued attributes will be
	 * treated as infix filters, e.g. for some part of the name of a document
	 * author. Including the value 'ish' as a filter for document author, for
	 * instance, will return a list that includes documents by all of 'Fisher',
	 * 'Bishop', 'Singuish', and 'Ishgil'. All filter predicates on time-related
	 * attributes will be interpreted as 'later than' or '&gt;' by default. To
	 * specify a custom comparison operator (one of '=', '&lt;', '&lt;=' and
	 * '&gt;='), set an additional property named &lt;attributeName&gt;Operator
	 * to the respective comparison symbol. For specifying several alternatives
	 * for the value of a filter predicate (which makes sense only for
	 * predicates on string valued attributes), separate the individual
	 * alternatives with a line break. All specified filter predicates will be
	 * conjuncted.
	 * @param filter a properties object containing filter predicates for the
	 *            document list
	 * @param pm a progress monitor to observe the loading process
	 * @return the list of documents available from the backing DIO
	 */
	public DioDocumentList getDocumentList(Properties filter, ProgressMonitor pm) throws IOException {
		if (!this.authClient.isLoggedIn())
			throw new IOException("Not logged in.");
		
		final Connection con = this.authClient.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(GET_DOCUMENT_LIST);
		bw.newLine();
		bw.write(this.authClient.getSessionID());
		bw.newLine();
		if ((filter == null) || filter.isEmpty())
			bw.write("");
		else {
			StringBuffer filterString = new StringBuffer();
			for (Iterator fit = filter.keySet().iterator(); fit.hasNext();) {
				String filterName = ((String) fit.next());
				String filterValue = filter.getProperty(filterName, "");
				String[] filterValues = filterValue.split("[\\r\\n]++");
				for (int v = 0; v < filterValues.length; v++) {
					filterValue = filterValues[v].trim();
					if (filterValue.length() == 0)
						continue;
					if (filterString.length() != 0)
						filterString.append("&");
					filterString.append(filterName + "=" + URLEncoder.encode(filterValues[v], ENCODING));
				}
			}
			bw.write(filterString.toString());
		}
		bw.newLine();
		bw.flush();
		
		final BufferedReader br = con.getReader();
		String error = br.readLine();
		if (GET_DOCUMENT_LIST.equals(error))
			return DioDocumentList.readDocumentList(new Reader() {
				public void close() throws IOException {
					br.close();
					con.close();
				}
				public int read(char[] cbuf, int off, int len) throws IOException {
					return br.read(cbuf, off, len);
				}
			}, pm);
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Obtain a document from the DIO. The valid document IDs can be read from
	 * the document list returned by getDocumentList(). The document is not
	 * locked at the backing DIO, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 */
	public DocumentRoot getDocument(String documentId) throws IOException {
		return this.getDocument(documentId, 0);
	}
	
	/**
	 * Obtain a document from the DIO. The valid document IDs can be read from
	 * the document list returned by getDocumentList(). The document is not
	 * locked at the backing DIO, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 */
	public DocumentRoot getDocument(String documentId, ProgressMonitor pm) throws IOException {
		return this.getDocument(documentId, 0, pm);
	}
	
	/**
	 * Obtain a document from the DIO. The valid document IDs and respective
	 * current version numbers can be read from the document list returned by
	 * getDocumentList(). The document is not locked at the backing DIO, so any
	 * attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load
	 * @return the specified version of the document with the specified ID
	 */
	public DocumentRoot getDocument(String documentId, int version) throws IOException {
		return this.getDocument(documentId, version, null);
	}
	
	/**
	 * Obtain a document from the DIO. The valid document IDs and respective
	 * current version numbers can be read from the document list returned by
	 * getDocumentList(). The document is not locked at the backing DIO, so any
	 * attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load
	 * @return the specified version of the document with the specified ID
	 */
	public DocumentRoot getDocument(String documentId, int version, final ProgressMonitor pm) throws IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		if (pm != null)
			pm.setInfo("Getting document stream ...");
		final DocumentReader dr = this.getDocumentAsStream(documentId, version);
		if (pm != null)
			pm.setInfo("Reading document ...");
		try {
			DocumentRoot doc = GenericGamtaXML.readDocument((pm == null) ? ((Reader) dr) : new Reader() {
				int read = 0;
				public int read(char[] cbuf, int off, int len) throws IOException {
					int r = dr.read(cbuf, off, len);
					if (r != -1) {
						this.read += r;
						pm.setProgress((this.read * 100) / dr.docLength());
					}
					return r;
				}
				public void close() throws IOException { /* we're closing dr directly below */ }
			});
			if (pm != null) {
				pm.setInfo("Document read.");
				pm.setProgress(100);
			}
			return doc;
		}
		finally {
			dr.close();
		}
	}
	
	/**
	 * Obtain a document from the DIO as a character stream. In situations where
	 * a document is not required in its deserialized form, e.g. if it is
	 * intended to be written to some output stream, this method facilitates
	 * avoiding deserialization and reserialization. The valid document IDs can
	 * be read from the document list returned by getDocumentList(). The
	 * document is not locked at the backing DIO, so any attempt of an update
	 * will fail.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 */
	public DocumentReader getDocumentAsStream(String documentId) throws IOException {
		return this.getDocumentAsStream(documentId, 0);
	}
	
	/**
	 * Obtain a document from the DIO as a character stream. In situations where
	 * a document is not required in its deserialized form, e.g. if it is
	 * intended to be written to some output stream, this method facilitates
	 * avoiding deserialization and reserialization. The valid document IDs and
	 * respective current version numbers can be read from the document list
	 * returned by getDocumentList(). The document is not locked at the backing
	 * DIO, so any attempt of an update will fail.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load
	 * @return the specified version of the document with the specified ID
	 */
	public DocumentReader getDocumentAsStream(String documentId, int version) throws IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_DOCUMENT_AS_STREAM);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(documentId + ((version == 0) ? "" : ("." + version)));
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_DOCUMENT_AS_STREAM.equals(error)) {
				int docLength = Integer.parseInt(br.readLine());
				DocumentReader dr = new DocumentReader(br, docLength);
				con = null;
				return dr;
			}
			
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Upload a new document to the backing DIO. If the specified document has
	 * actually been loaded from the backing DIO, the upload will fail. If the
	 * document was loaded from some other source (e.g. a local file on the client
	 * machine), DIO will store the document, but will not mark it as checked
	 * out by the user this client is authenticated with. To acquire a lock for
	 * a newly uploaded document, use the updateDocument() method.
	 * @param document the document to store
	 * @param documentName the name of the document
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws DuplicateExternalIdentifierException
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient#updateDocument(QueriableAnnotation, String)
	 */
	public String[] uploadDocument(QueriableAnnotation document, String documentName) throws DuplicateExternalIdentifierException, IOException {
		return this.uploadDocument(document, documentName, EXTERNAL_IDENTIFIER_MODE_CHECK);
	}
	
	/**
	 * Upload a new document to the backing DIO. If the specified document has
	 * actually been loaded from the backing DIO, the upload will fail. If the
	 * document was loaded from some other source (e.g. a local file on the client
	 * machine), DIO will store the document, but will not mark it as checked
	 * out by the user this client is authenticated with. To acquire a lock for
	 * a newly uploaded document, use the updateDocument() method. If the
	 * argument progress monitor is a controlling progress monitor, this method
	 * disables pausing and aborting after the document is sent. If the
	 * controlling functionality of the progress monitor is to be used again
	 * later, client code has to re-enable it.
	 * @param document the document to store
	 * @param documentName the name of the document
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @param pm a progress monitor to observe the upload process
	 * @throws DuplicateExternalIdentifierException
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient#updateDocument(QueriableAnnotation,
	 *      String)
	 */
	public String[] uploadDocument(QueriableAnnotation document, String documentName, ProgressMonitor pm) throws DuplicateExternalIdentifierException, IOException {
		return this.uploadDocument(document, documentName, EXTERNAL_IDENTIFIER_MODE_CHECK, pm);
	}
	
	/**
	 * Upload a new document to the backing DIO. If the specified document has
	 * actually been loaded from the backing DIO, the upload will fail. If the
	 * document was loaded from some other source (e.g. a local file on the client
	 * machine), DIO will store the document, but will not mark it as checked
	 * out by the user this client is authenticated with. To acquire a lock for
	 * a newly uploaded document, use the updateDocument() method.
	 * @param document the document to store
	 * @param documentName the name of the document
	 * @param externalIdentifierMode how to handle external identifier conflicts?
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws DuplicateExternalIdentifierException
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient#updateDocument(QueriableAnnotation, String)
	 */
	public String[] uploadDocument(QueriableAnnotation document, String documentName, String externalIdentifierMode) throws DuplicateExternalIdentifierException, IOException {
		return this.uploadDocument(document, documentName, externalIdentifierMode, null);
	}
	
	/**
	 * Upload a new document to the backing DIO. If the specified document has
	 * actually been loaded from the backing DIO, the upload will fail. If the
	 * document was loaded from some other source (e.g. a local file on the client
	 * machine), DIO will store the document, but will not mark it as checked
	 * out by the user this client is authenticated with. To acquire a lock for
	 * a newly uploaded document, use the updateDocument() method. If the
	 * argument progress monitor is a controlling progress monitor, this method
	 * disables pausing and aborting after the document is sent. If the
	 * controlling functionality of the progress monitor is to be used again
	 * later, client code has to re-enable it.
	 * @param document the document to store
	 * @param documentName the name of the document
	 * @param externalIdentifierMode how to handle external identifier
	 *            conflicts?
	 * @param pm a progress monitor to observe the upload process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws DuplicateExternalIdentifierException
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient#updateDocument(QueriableAnnotation,
	 *      String)
	 */
	public String[] uploadDocument(QueriableAnnotation document, String documentName, String externalIdentifierMode, final ProgressMonitor pm) throws DuplicateExternalIdentifierException, IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		//	get document name
		String docName = ((documentName == null) ? document.getAttribute(DOCUMENT_NAME_ATTRIBUTE, "Unknown Document").toString() : documentName);
		
		//	wrap document to measure upload progress
		if (pm != null)
			document = new GenericQueriableAnnotationWrapper(document) {
				public Token tokenAt(int index) {
					pm.setProgress(((index+1) * 100) / this.size());
					if ((index+1) == this.size())
						pm.setInfo("Document upload complete, waiting for server ...");
					return super.tokenAt(index);
				}
				public String valueAt(int index) {
					pm.setProgress(((index+1) * 100) / this.size());
					if ((index+1) == this.size())
						pm.setInfo("Document upload complete, waiting for server ...");
					return super.valueAt(index);
				}
			};
		
		if (pm != null)
			pm.setInfo("Connecting to server ...");
		
		//	change password
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			if (pm != null)
				pm.setInfo("Uploading document ...");
			
			bw.write(UPLOAD_DOCUMENT);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write("" + document.size());
			bw.newLine();
			bw.write(docName);
			bw.newLine();
			bw.write((externalIdentifierMode == null) ? EXTERNAL_IDENTIFIER_MODE_IGNORE : externalIdentifierMode);
			bw.newLine();
			GenericGamtaXML.storeDocument(document, bw);
			bw.newLine();
			bw.flush();
			
			if (pm != null) {
				pm.setInfo("Receiving upload result ...");
				if (pm instanceof ControllingProgressMonitor) {
					((ControllingProgressMonitor) pm).setPauseResumeEnabled(false);
					((ControllingProgressMonitor) pm).setAbortEnabled(false);
				}
			}
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (UPLOAD_DOCUMENT.equals(error)) {
				StringVector log = new StringVector();
				for (String logEntry; (logEntry = br.readLine()) != null;)
					log.addElement(logEntry);
				if (pm != null) {
					pm.setInfo("Upload complete.");
					pm.setProgress(100);
				}
				return log.toStringArray();
			}
			else if (DUPLICATE_EXTERNAL_IDENTIFIER.equals(error))
				throw new DuplicateExternalIdentifierException(br.readLine(), br.readLine(), DioDocumentList.readDocumentList(br));
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Check out a document from the backing DIO. The valid document IDs can be
	 * read from the document list returned by getDocumentList(). The document
	 * will be marked as checked out and will not be able to be worked on until
	 * released by the user checking it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 */
	public DocumentRoot checkoutDocument(String documentId) throws IOException {
		return this.checkoutDocument(documentId, 0);
	}
	
	/**
	 * Check out a document from the backing DIO. The valid document IDs can be
	 * read from the document list returned by getDocumentList(). The document
	 * will be marked as checked out and will not be able to be worked on until
	 * released by the user checking it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @param pm a progress monitor to observe the checkout process
	 * @return the document with the specified ID
	 */
	public DocumentRoot checkoutDocument(String documentId, ProgressMonitor pm) throws IOException {
		return this.checkoutDocument(documentId, 0, pm);
	}
	
	/**
	 * Check out a document from the backing DIO. The valid document IDs and
	 * respective current version numbers can be read from the document list
	 * returned by getDocumentList(). The document will be marked as checked out
	 * and will not be able to be worked on until released by the user checking
	 * it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load (version number
	 *            0 always marks the most recent version, positive integers
	 *            indicate absolute versions numbers, while negative integers
	 *            indicate a version number backward from the most recent one)
	 * @return the specified version of the document with the specified ID
	 */
	public DocumentRoot checkoutDocument(String documentId, int version) throws IOException {
		return this.checkoutDocument(documentId, version, null);
	}
	
	/**
	 * Check out a document from the backing DIO. The valid document IDs and
	 * respective current version numbers can be read from the document list
	 * returned by getDocumentList(). The document will be marked as checked out
	 * and will not be able to be worked on until released by the user checking
	 * it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load (version number
	 *            0 always marks the most recent version, positive integers
	 *            indicate absolute versions numbers, while negative integers
	 *            indicate a version number backward from the most recent one)
	 * @param pm a progress monitor to observe the checkout process
	 * @return the specified version of the document with the specified ID
	 */
	public DocumentRoot checkoutDocument(String documentId, int version, final ProgressMonitor pm) throws IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		if (pm != null)
			pm.setInfo("Getting document stream ...");
		final DocumentReader dr = this.checkoutDocumentAsStream(documentId, version);
		if (pm != null)
			pm.setInfo("Reading document ...");
		try {
			DocumentRoot doc = GenericGamtaXML.readDocument((pm == null) ? ((Reader) dr) : new Reader() {
				int read = 0;
				public int read(char[] cbuf, int off, int len) throws IOException {
					int r = dr.read(cbuf, off, len);
					if (r != -1) {
						this.read += r;
						pm.setProgress((this.read * 100) / dr.docLength());
					}
					return r;
				}
				public void close() throws IOException { /* we're closing dr directly below */ }
			});
			if (pm != null) {
				pm.setInfo("Document read.");
				pm.setProgress(100);
			}
			return doc;
		}
		finally {
			dr.close();
		}
	}
	
	/**
	 * Check out a document from the backing DIO as a character stream. In
	 * situations where a document is not required in its deserialized form,
	 * e.g. if it is intended to be written to some output stream, this method
	 * facilitates avoiding deserialization and reserialization. The valid
	 * document IDs can be read from the document list returned by
	 * getDocumentList(). The document will be marked as checked out and will
	 * not be able to be worked on until released by the user checking it out,
	 * or an administrator.
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 */
	public DocumentReader checkoutDocumentAsStream(String documentId) throws IOException {
		return this.checkoutDocumentAsStream(documentId, 0);
	}
	
	/**
	 * Check out a document from the backing DIO as a character stream. In
	 * situations where a document is not required in its deserialized form,
	 * e.g. if it is intended to be written to some output stream, this method
	 * facilitates avoiding deserialization and reserialization. The valid
	 * document IDs and respective current version numbers can be read from the
	 * document list returned by getDocumentList().The document will be marked
	 * as checked out and will not be able to be worked on until released by the
	 * user checking it out, or an administrator.
	 * @param documentId the ID of the document to load
	 * @param version the number of the document version to load (version number
	 *            0 always marks the most recent version, positive integers
	 *            indicate absolute versions numbers, while negative integers
	 *            indicate a version number backward from the most recent one)
	 * @return the specified version of the document with the specified ID
	 */
	public DocumentReader checkoutDocumentAsStream(String documentId, int version) throws IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(CHECKOUT_DOCUMENT_AS_STREAM);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(documentId + ((version == 0) ? "" : ("." + version)));
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (CHECKOUT_DOCUMENT_AS_STREAM.equals(error)) {
				int docLength = Integer.parseInt(br.readLine());
				DocumentReader dr = new DocumentReader(br, docLength);
				con = null;
				return dr;
			}
			
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Store/update a document in the backing DIO. If the specified document is
	 * not marked as checked out by the user this client is authenticated with,
	 * the upload will fail, with one exception: If the document was not loaded
	 * from DIO but from some other source (e.g. a local file on the client
	 * machine), DIO will store the document and subsequently mark it as checked
	 * out by the user this client is authenticated with. This implies that
	 * newly uploaded documents have to be released before any other users can
	 * work on them. For uploading a new document without acquiring the lock, use
	 * the uploadDocument() method.
	 * @param document the document to store
	 * @param documentName the name of the document (may be null if the document
	 *            was loaded from the backing DIO, for then it has the name as an
	 *            attribute)
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @see de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient#uploadDocument(QueriableAnnotation, String)
	 */
	public String[] updateDocument(QueriableAnnotation document, String documentName) throws IOException {
		return this.updateDocument(document, documentName, EXTERNAL_IDENTIFIER_MODE_CHECK);
	}
	
	/**
	 * Store/update a document in the backing DIO. If the specified document is
	 * not marked as checked out by the user this client is authenticated with,
	 * the upload will fail, with one exception: If the document was not loaded
	 * from DIO but from some other source (e.g. a local file on the client
	 * machine), DIO will store the document and subsequently mark it as checked
	 * out by the user this client is authenticated with. This implies that
	 * newly uploaded documents have to be released before any other users can
	 * work on them. For uploading a new document without acquiring the lock, use
	 * the uploadDocument() method. If the argument progress monitor is a
	 * controlling progress monitor, this method disabled pausing and aborting
	 * after the document is sent. If the controlling functionality of the
	 * progress monitor is to be used again later, client code has to re-enable
	 * it.
	 * @param document the document to store
	 * @param documentName the name of the document (may be null if the document
	 *            was loaded from the backing DIO, for then it has the name as an
	 *            attribute)
	 * @param pm a progress monitor to observe the update process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @see de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient#uploadDocument(QueriableAnnotation, String)
	 */
	public String[] updateDocument(QueriableAnnotation document, String documentName, ProgressMonitor pm) throws IOException {
		return this.updateDocument(document, documentName, EXTERNAL_IDENTIFIER_MODE_CHECK, pm);
	}
	
	/**
	 * Store/update a document in the backing DIO. If the specified document is
	 * not marked as checked out by the user this client is authenticated with,
	 * the upload will fail, with one exception: If the document was not loaded
	 * from DIO but from some other source (e.g. a local file on the client
	 * machine), DIO will store the document and subsequently mark it as checked
	 * out by the user this client is authenticated with. This implies that
	 * newly uploaded documents have to be released before any other users can
	 * work on them. For uploading a new document without acquiring a lock, use
	 * the uploadDocument() method.
	 * @param document the document to store
	 * @param documentName the name of the document (may be null if the document
	 *            was loaded from the backing DIO, for then it has the name as an
	 *            attribute)
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @param externalIdentifierMode how to handle external identifier conflicts?
	 * @throws DuplicateExternalIdentifierException
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient#uploadDocument(QueriableAnnotation, String)
	 */
	public String[] updateDocument(QueriableAnnotation document, String documentName, String externalIdentifierMode) throws DuplicateExternalIdentifierException, IOException {
		return this.updateDocument(document, documentName, externalIdentifierMode, null);
	}
	
	/**
	 * Store/update a document in the backing DIO. If the specified document is
	 * not marked as checked out by the user this client is authenticated with,
	 * the upload will fail, with one exception: If the document was not loaded
	 * from DIO but from some other source (e.g. a local file on the client
	 * machine), DIO will store the document and subsequently mark it as checked
	 * out by the user this client is authenticated with. This implies that
	 * newly uploaded documents have to be released before any other users can
	 * work on them. For uploading a new document without acquiring a lock, use
	 * the uploadDocument() method. If the argument progress monitor is a
	 * controlling progress monitor, this method disabled pausing and aborting
	 * after the document is sent. If the controlling functionality of the
	 * progress monitor is to be used again later, client code has to re-enable
	 * it.
	 * @param document the document to store
	 * @param documentName the name of the document (may be null if the document
	 *            was loaded from the backing DIO, for then it has the name as
	 *            an attribute)
	 * @param externalIdentifierMode how to handle external identifier
	 *            conflicts?
	 * @param pm a progress monitor to observe the update process
	 * @return an array holding the logging messages collected during the
	 *         storage process.
	 * @throws DuplicateExternalIdentifierException
	 * @throws IOException
	 * @see de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient#uploadDocument(QueriableAnnotation,
	 *      String)
	 */
	public String[] updateDocument(QueriableAnnotation document, String documentName, String externalIdentifierMode, final ProgressMonitor pm) throws DuplicateExternalIdentifierException, IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		//	get document name
		String docName = ((documentName == null) ? document.getAttribute(DOCUMENT_NAME_ATTRIBUTE, "Unknown Document").toString() : documentName);
		
		//	wrap document to measure upload progress
		if (pm != null)
			document = new GenericQueriableAnnotationWrapper(document) {
				public Token tokenAt(int index) {
					pm.setProgress((index * 100) / this.size());
					if ((index+1) == this.size())
						pm.setInfo("Document update complete, waiting for server ...");
					return super.tokenAt(index);
				}
				public String valueAt(int index) {
					pm.setProgress((index * 100) / this.size());
					if ((index+1) == this.size())
						pm.setInfo("Document update complete, waiting for server ...");
					return super.valueAt(index);
				}
			};
		
		if (pm != null)
			pm.setInfo("Connecting to server ...");
		
		//	do update
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			if (pm != null)
				pm.setInfo("Updating document ...");
			
			bw.write(UPDATE_DOCUMENT);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write("" + document.size());
			bw.newLine();
			bw.write(docName);
			bw.newLine();
			bw.write((externalIdentifierMode == null) ? EXTERNAL_IDENTIFIER_MODE_IGNORE : externalIdentifierMode);
			bw.newLine();
			GenericGamtaXML.storeDocument(document, bw);
			bw.newLine();
			bw.flush();
			
			if (pm != null) {
				pm.setInfo("Receiving update result ...");
				if (pm instanceof ControllingProgressMonitor) {
					((ControllingProgressMonitor) pm).setPauseResumeEnabled(false);
					((ControllingProgressMonitor) pm).setAbortEnabled(false);
				}
			}
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (UPDATE_DOCUMENT.equals(error)) {
				StringVector log = new StringVector();
				for (String logEntry; (logEntry = br.readLine()) != null;)
					log.addElement(logEntry);
				if (pm != null) {
					pm.setInfo("Update complete.");
					pm.setProgress(100);
				}
				return log.toStringArray();
			}
			else if (DUPLICATE_EXTERNAL_IDENTIFIER.equals(error))
				throw new DuplicateExternalIdentifierException(br.readLine(), br.readLine(), DioDocumentList.readDocumentList(br));
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Retrieve the update protocol of document, i.e. an array of messages that
	 * describe which other modifications the update to the latest version
	 * incurred throughout the server. This includes only modifications that
	 * happen synchronously on update notification, though.
	 * @param docId the ID of the document to get the update protocol for
	 * @return the update protocol
	 * @throws IOException
	 */
	public String[] getUpdateProtocol(String docId) throws IOException {
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_UPDATE_PROTOCOL);
			bw.newLine();
			bw.write(docId);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_UPDATE_PROTOCOL.equals(error)) {
				StringVector log = new StringVector();
				for (String logEntry; (logEntry = br.readLine()) != null;)
					log.addElement(logEntry);
				return log.toStringArray();
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Delete a documents from the DIO. If a user other than the one this client
	 * is authenticated with holds the lock for the document with the specified
	 * ID, the deletion fails and an IOException will be thrown.
	 * @param documentId the ID of the document to delete
	 * @return an array holding the logging messages collected during the
	 *         deletion process.
	 */
	public String[] deleteDocument(String documentId) throws IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(DELETE_DOCUMENT);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(documentId);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (DELETE_DOCUMENT.equals(error)) {
				StringVector log = new StringVector();
				for (String logEntry; (logEntry = br.readLine()) != null;)
					log.addElement(logEntry);
				return log.toStringArray();
			}
			else throw new IOException(error);
		}
		catch (Exception e) {
			throw new IOException(e.getMessage());
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Release a document so other users can work on it again. This is possible
	 * only under two conditions: (1) the document was checked out by the user
	 * this client is authenticated with, which is the normal use, or (2) with
	 * administrative privileges, which should be done only in rare cases
	 * because it possibly annihilates all the work the checkout user has done
	 * on the document.
	 * @param documentId the ID of the document to release
	 * @throws IOException
	 */
	public void releaseDocument(String documentId) throws IOException {
		if (!this.authClient.isLoggedIn()) throw new IOException("Not logged in.");
		
		Connection con = null;
		try {
			con = this.authClient.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(RELEASE_DOCUMENT);
			bw.newLine();
			bw.write(this.authClient.getSessionID());
			bw.newLine();
			bw.write(documentId);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (!RELEASE_DOCUMENT.equals(error))
				throw new IOException("Document release failed: " + error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
}