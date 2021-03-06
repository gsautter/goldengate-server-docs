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
package de.uka.ipd.idaho.goldenGateServer.dst;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.Iterator;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerActivityLogger;
import de.uka.ipd.idaho.goldenGateServer.util.IdentifierKeyedDataObjectStore;

/**
 * Component for storing and retrieving documents by ID, named GoldenGATE
 * Document STorage. On purpose, this component is not a
 * GoldenGateServerComponent, so every component requiring to store documents
 * using their ID as the key can create their own instance without causing
 * registry conflicts.
 * 
 * @author sautter
 */
public class DocumentStore extends IdentifierKeyedDataObjectStore implements DocumentStoreConstants {
	
	/**
	 * Exception indicating that a document or a specific version of one does
	 * not exist in this document store.
	 * 
	 * @author sautter
	 */
	public static class DocumentNotFoundException extends DataObjectNotFoundException {
		DocumentNotFoundException(String docId) {
			super(docId);
		}
		DocumentNotFoundException(String docId, int version) {
			super(docId, version);
		}
	}
	
	private String encoding = "UTF-8";
	
	/**
	 * Constructor
	 * @param name a name for the document store, for distinguishing instances
	 *            in log and console (defaults to root folder name if null)
	 * @param docFolder the folder to store the documents in, which will be the
	 *            root folder for the DocumentStore's internal folder hierarchy,
	 *            a B-Tree style folder structure.
	 * @param logger logger for background maintenance activity
	 */
	public DocumentStore(String name, File docFolder, GoldenGateServerActivityLogger logger) {
		this(name, docFolder, null, logger);
	}
	
	/**
	 * Constructor
	 * @param name a name for the document store, for distinguishing instances
	 *            in log and console (defaults to root folder name if null)
	 * @param docFolder the folder to store the documents in, which will be the
	 *            root folder for the DocumentStore's internal folder hierarchy,
	 *            a B-Tree style folder structure.
	 * @param encoding the character encoding to use for storing the documents
	 *            (specifying null results in the default being used, namely
	 *            UTF-8)
	 * @param logger logger for background maintenance activity
	 */
	public DocumentStore(String name, File docFolder, String encoding, GoldenGateServerActivityLogger logger) {
		super(name, docFolder, ".xml", logger);
		if (encoding != null)
			this.encoding = encoding;
	}
	
	/**
	 * Store a document using its docId attribute as the storage ID (if the
	 * docId attribute is not set, the document's annotationId will be used
	 * instead)
	 * @param doc the document to store
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int storeDocument(QueriableAnnotation doc) throws IOException {
		String docId = doc.getAttribute(DOCUMENT_ID_ATTRIBUTE, doc.getAnnotationID()).toString();
		return this.storeDocument(doc, docId);
	}
	
	/**
	 * Store a document using a custom ID (when reloading the document, its
	 * docId attribute will be set to the ID specified here)
	 * @param doc the document to store
	 * @param documentId the ID to store the document with
	 * @return the new version number of the document just updated
	 * @throws IOException
	 */
	public int storeDocument(QueriableAnnotation doc, String documentId) throws IOException {
		
		//	write document
		DataObjectOutputStream docOut = this.getOutputStream(documentId);
		Writer out = new OutputStreamWriter(docOut, this.encoding);
		GenericGamtaXML.storeDocument(doc, out);
		out.flush();
		out.close();
		
		//	return incremented version number
		return docOut.getVersion();
	}
	
	/**
	 * Delete a document from storage
	 * @param	documentId	the ID of the document to delete
	 * @throws IOException if the specified document ID is invalid (no document
	 *             is stored by this ID) or any IOException occurs
	 */
	public void deleteDocument(String documentId) throws IOException {
		this.deleteDataObject(documentId);
	}
	
	/**
	 * Check if a document exists.
	 * @param documentId the ID of the document
	 * @return true if the document exists, false otherwise
	 * @throws IOException
	 */
	public boolean isDocumentAvailable(String documentId) {
		return this.isDataObjectAvailable(documentId);
	}
	
	/**
	 * Retrieve the attributes of a document.
	 * @param documentId the ID of the document
	 * @return a Properties object holding the attributes of the document with
	 *         the specified ID
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public Properties getDocumentAttributes(String documentId) throws DocumentNotFoundException, IOException {
		return this.getDocumentAttributes(documentId, false);
	}
	
	/**
	 * Retrieve the attributes of a document. If the argument boolean is set to
	 * <code>true</code>, the returned Properties includes the update users and
	 * timestamps from prior versions in <code>updateUser&lt;version&gt;</code>
	 * and <code>updateTime&lt;version&gt;</code> attributes, e.g.
	 * <code>updateUser-3</code> for the user who created version 3 of the
	 * document with the argument ID.
	 * @param documentId the ID of the document
	 * @param includeUpdateHistory include former update users and timestamps?
	 * @return a Properties object holding the attributes of the document with
	 *         the specified ID
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public Properties getDocumentAttributes(String documentId, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
		
		//	load current version attributes
		DocumentRoot doc = this.getDocumentHead(documentId, 0);
		String[] attributeNames = doc.getAttributeNames();
		Properties attributes = new Properties();
		for (int a = 0; a < attributeNames.length; a++) {
			Object value = doc.getAttribute(attributeNames[a]);
			if (value instanceof String)
				attributes.setProperty(attributeNames[a], ((String) value));
		}
		
		//	add version history if asked to
		if (includeUpdateHistory)
			this.addUpdateHistory(documentId, this.getVersion(documentId), attributes);
		
		//	finally ...
		return attributes;
	}
	
	private void addUpdateHistory(String docId, int version, Properties attributes) throws IOException {
		
		//	check if update history already stored in current version
		if (attributes.containsKey(UPDATE_USER_ATTRIBUTE + "-" + version) && attributes.containsKey(UPDATE_TIME_ATTRIBUTE + "-" + version))
			return;
		
		//	add update history the hard way
		for (int v = 1; v < version; v++) {
			DocumentRoot vDoc = this.getDocumentHead(docId, v);
			Object vUpdateUser = vDoc.getAttribute(UPDATE_USER_ATTRIBUTE);
			if (vUpdateUser instanceof String)
				attributes.setProperty((UPDATE_USER_ATTRIBUTE + "-" + v), ((String) vUpdateUser));
			Object vUpdateTime = vDoc.getAttribute(UPDATE_TIME_ATTRIBUTE);
			if (vUpdateTime instanceof String)
				attributes.setProperty((UPDATE_TIME_ATTRIBUTE + "-" + v), ((String) vUpdateTime));
		}
		String vUpdateUser = attributes.getProperty(UPDATE_USER_ATTRIBUTE);
		if (vUpdateUser != null)
			attributes.setProperty((UPDATE_USER_ATTRIBUTE + "-" + version), vUpdateUser);
		String vUpdateTime = attributes.getProperty(UPDATE_TIME_ATTRIBUTE);
		if (vUpdateTime != null)
			attributes.setProperty((UPDATE_TIME_ATTRIBUTE + "-" + version), vUpdateTime);
	}
	
	/**
	 * Retrieve the document properties of a document.
	 * @param documentId the ID of the document
	 * @return a Properties object holding the document properties of the
	 *         document with the specified ID
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public Properties getDocumentProperties(String documentId) throws DocumentNotFoundException, IOException {
		DocumentRoot doc = this.getDocumentHead(documentId, 0);
		String[] documentPropertyNames = doc.getDocumentPropertyNames();
		Properties documentProperties = new Properties();
		for (int a = 0; a < documentPropertyNames.length; a++) {
			String value = doc.getDocumentProperty(documentPropertyNames[a]);
			if (value != null)
				documentProperties.setProperty(documentPropertyNames[a], value);
		}
		return documentProperties;
	}
	
	private DocumentRoot getDocumentHead(String documentId, int version) throws DocumentNotFoundException, IOException {
		Reader in = null;
		try {
			final DataObjectInputStream docIn = this.getInputStream(documentId, version);
			InputStream is = new InputStream() {
				private int last = '\u0000';
				public int read() throws IOException {
					if (this.last == '>')
						return -1;
					this.last = docIn.read();
					return this.last;
				}
				public void close() throws IOException {
					docIn.close();
				}
			};
			in = new InputStreamReader(is, this.encoding);
			DocumentRoot doc = GenericGamtaXML.readDocument(in);
			doc.setAttribute(DOCUMENT_ID_ATTRIBUTE, documentId);
			doc.setDocumentProperty(DOCUMENT_ID_ATTRIBUTE, documentId);
			doc.setAttribute(DOCUMENT_VERSION_ATTRIBUTE, ("" + docIn.getVersion()));
			doc.setDocumentProperty(DOCUMENT_VERSION_ATTRIBUTE, ("" + docIn.getVersion()));
			return doc;
		}
		catch (DataObjectNotFoundException donfe) {
			throw new DocumentNotFoundException(documentId, version);
		}
		finally {
			if (in != null)
				in.close();
		}
	}
	
	/**
	 * Load a document from storage (the most recent version)
	 * @param documentId the ID of the document to load
	 * @return the document with the specified ID
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public DocumentRoot loadDocument(String documentId) throws DocumentNotFoundException, IOException {
		return this.loadDocument(documentId, 0, false);
	}
	
	/**
	 * Load a document from storage (the most recent version)
	 * @param documentId the ID of the document to load
	 * @param includeUpdateHistory include former update users and timestamps?
	 * @return the document with the specified ID
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public DocumentRoot loadDocument(String documentId, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
		return this.loadDocument(documentId, 0, includeUpdateHistory);
	}
	
	/**
	 * Load a specific version of a document from storage. A positive version
	 * number indicates an actual version specifically, while a negative version
	 * number indicates a version backward relative to the most recent version.
	 * Version number 0 always returns the most recent version.
	 * @param documentId the ID of the document to load
	 * @param version the version to load
	 * @return the document with the specified ID
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public DocumentRoot loadDocument(String documentId, int version) throws DocumentNotFoundException, IOException {
		return this.loadDocument(documentId, version, false);
	}
	
	/**
	 * Load a specific version of a document from storage. A positive version
	 * number indicates an actual version specifically, while a negative version
	 * number indicates a version backward relative to the most recent version.
	 * Version number 0 always returns the most recent version.
	 * @param documentId the ID of the document to load
	 * @param version the version to load
	 * @param includeUpdateHistory include former update users and timestamps?
	 * @return the document with the specified ID
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public DocumentRoot loadDocument(String documentId, int version, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
		DocumentReader dr = this.loadDocumentAsStream(documentId, version, includeUpdateHistory);
		try {
			return GenericGamtaXML.readDocument(dr);
		}
		finally {
			dr.close();
		}
	}
	
	/**
	 * Load a document from storage (the most recent version) as a character
	 * stream. In situations where a document is not required in its
	 * deserialized form, e.g. if it is intended to be written to some output
	 * stream, this method facilitates avoiding deserialization and
	 * reserialization.
	 * @param documentId the ID of the document to load
	 * @return a reader providing the document with the specified ID in its
	 *         serialized form
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public DocumentReader loadDocumentAsStream(String documentId) throws DocumentNotFoundException, IOException {
		return this.loadDocumentAsStream(documentId, 0, false);
	}
	
	/**
	 * Load a document from storage (the most recent version) as a character
	 * stream. In situations where a document is not required in its
	 * deserialized form, e.g. if it is intended to be written to some output
	 * stream, this method facilitates avoiding deserialization and
	 * reserialization.
	 * @param documentId the ID of the document to load
	 * @return a reader providing the document with the specified ID in its
	 *         serialized form
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public DocumentReader loadDocumentAsStream(String documentId, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
		return this.loadDocumentAsStream(documentId, 0, includeUpdateHistory);
	}
	
	/**
	 * Load a document from storage as a character stream. In situations where a
	 * document is not required in its deserialized form, e.g. if it is intended
	 * to be written to some output stream, this method facilitates avoiding
	 * deserialization and reserialization. A positive version number indicates
	 * an actual version specifically, while a negative version number indicates
	 * a version backward relative to the most recent version. Version number 0
	 * always returns the most recent version.
	 * @param documentId the ID of the document to load
	 * @param version the version to load
	 * @return a reader providing the document with the specified ID in its
	 *         serialized form
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public DocumentReader loadDocumentAsStream(String documentId, int version) throws DocumentNotFoundException, IOException {
		return this.loadDocumentAsStream(documentId, version, false);
	}
	
	/**
	 * Load a document from storage as a character stream. In situations where a
	 * document is not required in its deserialized form, e.g. if it is intended
	 * to be written to some output stream, this method facilitates avoiding
	 * deserialization and reserialization. A positive version number indicates
	 * an actual version specifically, while a negative version number indicates
	 * a version backward relative to the most recent version. Version number 0
	 * always returns the most recent version.
	 * @param documentId the ID of the document to load
	 * @param version the version to load
	 * @return a reader providing the document with the specified ID in its
	 *         serialized form
	 * @throws DocumentNotFoundException if the specified document ID is invalid
	 *             (no document is stored by this ID, or specified version does
	 *             not exist)
	 * @throws IOException if any other IOException occurs
	 */
	public DocumentReader loadDocumentAsStream(String documentId, int version, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
		try {
			DataObjectInputStream docIn = this.getInputStream(documentId, version);
			DocumentReader dr = new DocumentReader(new InputStreamReader(docIn, this.encoding), docIn.getDataObjectSize());
			dr.setAttribute(DOCUMENT_ID_ATTRIBUTE, documentId);
			dr.setDocumentProperty(DOCUMENT_ID_ATTRIBUTE, documentId);
			dr.setAttribute(DOCUMENT_VERSION_ATTRIBUTE, ("" + docIn.getVersion()));
			if (includeUpdateHistory) {
				Properties duh = new Properties();
				String[] dans = dr.getAttributeNames();
				for (int a = 0; a < dans.length; a++) {
					if (dans[a].startsWith(UPDATE_USER_ATTRIBUTE) || dans[a].startsWith(UPDATE_TIME_ATTRIBUTE)) {
						Object av = dr.getAttribute(dans[a]);
						if (av instanceof String)
							duh.setProperty(dans[a], ((String) av));
					}
				}
				this.addUpdateHistory(documentId, docIn.getVersion(), duh);
				for (Iterator kit = duh.keySet().iterator(); kit.hasNext();) {
					String dan = ((String) kit.next());
					if (!dr.hasAttribute(dan))
						dr.setAttribute(dan, duh.getProperty(dan));
				}
			}
			return dr;
		}
		catch (DataObjectNotFoundException donfe) {
			throw new DocumentNotFoundException(documentId, version);
		}
	}
	
	/**
	 * Retrieve a list of the IDs of all the document stored in this DST
	 * @return a list of the IDs of all the document stored in this DST
	 */
	public String[] getDocumentIDs() {
		return this.getDataObjectIDs();
	}
//	
//	public static void main(String[] args) throws Exception {
//		File docFolder = new File("./Components/GgServerSRSData/DocumentsGone/");
//		DocumentStore dst = new DocumentStore(docFolder);
//		//	dst.zipOldDocumentVersions();
//		String docId = "0000 C505 BB5D 484C 76BE 9AB6 999D EB23".replaceAll("\\s", "");
//		DocumentRoot doc = dst.loadDocument(docId, -1);
//		doc.setAttribute("testTime", ("" + System.currentTimeMillis()));
//		int version = dst.storeDocument(doc, docId);
//		System.out.println("Stored as version " + version);
//	}
}
//public class DocumentStore implements LiteratureConstants, GoldenGateServerDocConstants {
//	
//	/**
//	 * Exception indicating that a document or a specific version of one does
//	 * not exist in this document store.
//	 * 
//	 * @author sautter
//	 */
//	public static class DocumentNotFoundException extends IOException {
//		DocumentNotFoundException(String docId) {
//			this(docId, 0);
//		}
//		DocumentNotFoundException(String docId, int version) {
//			super("Invalid document ID '" + docId + "'" + ((version == 0) ? (".") : (", or version '" + version + "' does not exist.")));
//		}
//	}
//	
//	private File docFolder;
//	private String encoding = "UTF-8";
//	
//	/**
//	 * Constructor
//	 * @param docFolder the folder to store the documents in, which will be the
//	 *            root folder for the DocumentStore's internal folder hierarchy,
//	 *            a B-Tree style folder structure.
//	 */
//	public DocumentStore(File docFolder) {
//		this(docFolder, null);
//	}
//	
//	/**
//	 * Constructor
//	 * @param docFolder the folder to store the documents in, which will be the
//	 *            root folder for the DocumentStore's internal folder hierarchy,
//	 *            a B-Tree style folder structure.
//	 * @param encoding the character encoding to use for storing the documents
//	 *            (specifying null results in the default being used, namely
//	 *            UTF-8)
//	 */
//	public DocumentStore(File docFolder, String encoding) {
//		this.docFolder = docFolder;
//		if (!this.docFolder.exists())
//			this.docFolder.mkdirs();
//		if (encoding != null)
//			this.encoding = encoding;
//		
//		this.backupAction = new AsynchronousConsoleAction("backup", "Backup document archive of this GoldenGATE DST", "collection", null, null) {
//			protected void performAction(String[] arguments) throws Exception {
//				String backupName = ("Backup." + backupTimestamper.format(new Date()) + ".zip");
//				
//				String target = ((arguments.length == 0) ? null : arguments[0]);
//				File backupFile;
//				if (target == null)
//					backupFile = new File(DocumentStore.this.docFolder, backupName);
//				else if ((target.indexOf(':') == -1) && !target.startsWith("/"))
//					backupFile = new File(new File(DocumentStore.this.docFolder, target), backupName);
//				else backupFile = new File(target, backupName);
//				
//				boolean full = ((arguments.length < 2) || "-f".equals(arguments[1]));
//				
//				this.log("GoldenGateDST: start backing up document archive ...");
//				
//				//	collect files to add to backup
//				ArrayList backupFileNames = new ArrayList();
//				
//				//	process primary level folders
//				File[] primaryLevel = DocumentStore.this.docFolder.listFiles(archiveLevelFilter);
//				this.log("  - got " + primaryLevel.length + " primary level folders");
//				for (int p = 0; p < primaryLevel.length; p++) {
//					
//					//	process secondary level folders
//					File[] secondaryLevel = primaryLevel[p].listFiles(archiveLevelFilter);
//					this.log("    - (" + (p+1) + ") got " + secondaryLevel.length + " secondary level folders");
//					for (int s = 0; s < secondaryLevel.length; s++) {
//						
//						//	inspect files in secondary level folders
//						File[] archiveContent = secondaryLevel[s].listFiles(archiveFileFilter);
//						this.log("      - (" + (p+1) + "-" + (s+1) + ") got " + archiveContent.length + " archive content files");
//						for (int a = 0; a < archiveContent.length; a++) {
//							String contentFileName = archiveContent[a].getName();
//							boolean addToBackup = false;
//							
//							//	older document version
//							if (contentFileName.matches(".*\\.[0-9]++\\.xml"))
//								addToBackup = full;
//							
//							//	recent file
//							else if (contentFileName.matches(".*\\.xml"))
//								addToBackup = true;
//							
//							//	this one goes to the backup
//							if (addToBackup)
//								backupFileNames.add(primaryLevel[p].getName() + "/" + secondaryLevel[s].getName() + "/" + contentFileName);
//						}
//					}
//				}
//				this.log(" - got " + backupFileNames.size() + " files to backup.");
//				this.enteringMainLoop("0 of " + backupFileNames.size() + " documents added to backup.");
//				
//				//	create backup file
//				backupFile.getParentFile().mkdirs();
//				backupFile.createNewFile();
//				ZipOutputStream backup = new ZipOutputStream(new FileOutputStream(backupFile));
//				
//				//	add document files to backup
//				for (int f = 0; this.continueAction() && (f < backupFileNames.size()); f++) {
//					String backupFileName = ((String) backupFileNames.get(f));
//					FileInputStream backupEntrySource = new FileInputStream(new File(DocumentStore.this.docFolder, backupFileName));
//					
//					ZipEntry backupEntry = new ZipEntry(backupFileName);
//					backup.putNextEntry(backupEntry);
//					
//					byte[] buffer = new byte[1024];
//					int read;
//					while ((read = backupEntrySource.read(buffer)) != -1)
//						backup.write(buffer, 0, read);
//					
//					backup.closeEntry();
//					backupEntrySource.close();
//					
//					this.loopRoundComplete((f+1) + " of " + backupFileNames.size() + " documents added to backup.");
//				}
//				
//				//	finalize backup
//				backup.flush();
//				backup.close();
//			}
//			protected String[] getArgumentNames() {
//				String[] argumentNames = {"target", "mode"};
//				return argumentNames;
//			}
//			protected String[] getArgumentExplanation(String argument) {
//				if ("target".equals(argument)) {
//					String[] explanation = {
//							"the file to write the backup to (an absolute file path or a relative file path (relative to the archive root), will backup to archive root if not specified)"
//						};
//					return explanation;
//				}
//				else if ("mode".equals(argument)) {
//					String[] explanation = {
//							"the backup mode, specifying what to include in the backup:",
//							"'-c': backup of current document versions, the default",
//							"'-f': full backup including all versions of all documents"
//						};
//					return explanation;
//				}
//				else return super.getArgumentExplanation(argument);
//			}
//			protected String checkArguments(String[] arguments) {
//				if (arguments.length < 2)
//					return null;
//				else if (arguments.length > 2)
//					return "Specify target and mode only.";
//				else if ("-c".equals(arguments[1]) || "-f".equals(arguments[1]))
//					return null;
//				else return ("Invalid backup mode '" + arguments[1] + "', use '-c' and '-f' only.");
//			}
//		};	
//	}
//	
//	private AsynchronousConsoleAction backupAction;
//	
//	private static final String ZIP_OLD_DOCUMENT_VERSIONS_COMMAND = "zipOldVersions";
//	
//	/**
//	 * Retrieve the actions for accessing the document store from the component
//	 * server console in the scope of a host server component. Access though
//	 * network is not possible without a respective host server component, and
//	 * is not intended to be.
//	 * @return an array holding actions for accessing the document store from
//	 *         the component server console
//	 */
//	public ComponentActionConsole[] getActions() {
//		ArrayList cal = new ArrayList();
//		ComponentActionConsole ca;
//		
//		//	backup archive
//		cal.add(this.backupAction);
//		
//		//	add console action zipping up older versions of XML files
//		ca = new ComponentActionConsole() {
//			private Thread zipper = null;
//			public String getActionCommand() {
//				return ZIP_OLD_DOCUMENT_VERSIONS_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = {
//						ZIP_OLD_DOCUMENT_VERSIONS_COMMAND,
//						"Zip up all but the current version of each document."
//					};
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length != 0)
//					System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
//				else if (this.zipper != null)
//					System.out.println(" Already zipping up old document versions");
//				else {
//					this.zipper = new Thread() {
//						public void run() {
//							try {
//								zipOldDocumentVersions();
//							}
//							finally {
//								zipper = null;
//							}
//						}
//					};
//					this.zipper.start();
//				}
//			}
//			
//		};
//		cal.add(ca);
//		
//		//	finally ...
//		return ((ComponentActionConsole[]) cal.toArray(new ComponentActionConsole[cal.size()]));
//	}
//	
//	private void zipOldDocumentVersions() {
//		System.out.println("GoldenGateDST: start zipping up old document versions ...");
//		
//		//	collect files to add to backup
//		ArrayList toZipFileNames = new ArrayList();
//		
//		//	process primary level folders
//		File[] primaryLevel = DocumentStore.this.docFolder.listFiles(archiveLevelFilter);
//		System.out.println("  - got " + primaryLevel.length + " primary level folders");
//		for (int p = 0; p < primaryLevel.length; p++) {
//			
//			//	process secondary level folders
//			File[] secondaryLevel = primaryLevel[p].listFiles(archiveLevelFilter);
//			System.out.println("    - (" + (p+1) + ") got " + secondaryLevel.length + " secondary level folders");
//			for (int s = 0; s < secondaryLevel.length; s++) {
//				
//				//	inspect files in secondary level folders
//				File[] archiveContent = secondaryLevel[s].listFiles(archiveFileFilter);
//				System.out.println("      - (" + (p+1) + "-" + (s+1) + ") got " + archiveContent.length + " archive content files");
//				for (int a = 0; a < archiveContent.length; a++) {
//					String docFileName = archiveContent[a].getName();
//					if (docFileName.matches(".*\\.[0-9]++\\.xml"))
//						toZipFileNames.add(primaryLevel[p].getName() + "/" + secondaryLevel[s].getName() + "/" + docFileName);
//				}
//			}
//		}
//		System.out.println("Got " + toZipFileNames.size() + " files to zip up.");
//		
//		//	zip up document files
//		for (int f = 0; f < toZipFileNames.size(); f++) {
//			this.zipDocumentFile(((String) toZipFileNames.get(f)));
//			System.out.println((f+1) + " of " + toZipFileNames.size() + " old document versions zipped up.");
//		}
//	}
//	
//	private static final SimpleDateFormat backupTimestamper = new SimpleDateFormat("yyyyMMdd-HHmm");
//	
//	//	file filter protecting all non-archive folders
//	private static final FileFilter archiveLevelFilter = new FileFilter() {
//		public boolean accept(File file) {
//			return ((file != null) && file.isDirectory() && (file.getName().length() == 2));
//		}
//	};
//	
//	//	file filter protecting all non-archive files
//	private static final FileFilter archiveFileFilter = new FileFilter() {
//		private Pattern hexFileName = Pattern.compile("[0-9a-zA-Z]{32}+\\.([0-9]++\\.)?+xml(\\.old)?+");
//		public boolean accept(File file) {
//			return ((file != null) && file.isFile() && hexFileName.matcher(file.getName()).matches());
//		}
//	};
//	
//	/**
//	 * Store a document using its docId attribute as the storage ID (if the
//	 * docId attribute is not set, the document's annotationId will be used
//	 * instead)
//	 * @param doc the document to store
//	 * @return the new version number of the document just updated
//	 * @throws IOException
//	 */
//	public int storeDocument(QueriableAnnotation doc) throws IOException {
//		String docId = doc.getAttribute(DOCUMENT_ID_ATTRIBUTE, doc.getAnnotationID()).toString();
//		return this.storeDocument(doc, docId);
//	}
//	
//	/**
//	 * Store a document using a custom ID (when reloading the document, its
//	 * docId attribute will be set to the ID specified here)
//	 * @param doc the document to store
//	 * @param documentId the ID to store the document with
//	 * @return the new version number of the document just updated
//	 * @throws IOException
//	 */
//	public int storeDocument(QueriableAnnotation doc, String documentId) throws IOException {
//		
//		//	build storage ID
//		String docId = this.checkDocId(documentId);
//		
//		//	create two-layer storage folder structure
//		String primaryFolderName = docId.substring(0, 2);
//		File primaryFolder = new File(this.docFolder, primaryFolderName);
//		if (!primaryFolder.exists())
//			primaryFolder.mkdir();
//		
//		String secondaryFolderName = docId.substring(2, 4);
//		File secondaryFolder = new File(primaryFolder, secondaryFolderName);
//		if (!secondaryFolder.exists())
//			secondaryFolder.mkdir();
//		
//		//	compute current version
//		int version = this.computeCurrentVersion(docId);
//		
//		//	create document file
//		File docFile = new File(secondaryFolder, (docId + ".xml"));
//		final String previousVersionDocFileName;
//		
//		//	file exists (we have an update), make way
//		if (docFile.exists()) {
//			previousVersionDocFileName = (primaryFolderName + "/" + secondaryFolderName + "/" + docId + "." + version + ".xml");
//			File previousVersionDocFile = new File(secondaryFolder, (docId + "." + version + ".xml"));
//			docFile.renameTo(previousVersionDocFile);
//			docFile = new File(secondaryFolder, (docId + ".xml"));
//		}
//		else previousVersionDocFileName = null;
//		docFile.createNewFile();
//		
//		//	write document
//		Writer out = new OutputStreamWriter(new FileOutputStream(docFile), this.encoding);
//		GenericGamtaXML.storeDocument(doc, out);
//		out.flush();
//		out.close();
//		
//		//	zip up old version (if any)
//		if (previousVersionDocFileName != null) {
//			Thread zipper = new Thread() {
//				public void run() {
//					zipDocumentFile(previousVersionDocFileName);
//				}
//			};
//			zipper.start();
//		}
//		
//		//	return incremented version number
//		return (version + 1);
//	}
//	
//	/**
//	 * Delete a document from storage
//	 * @param	documentId	the ID of the document to delete
//	 * @throws IOException if the specified document ID is invalid (no document
//	 *             is stored by this ID) or any IOException occurs
//	 */
//	public void deleteDocument(String documentId) throws IOException {
//		final String docId = this.checkDocId(documentId);
//		
//		String primaryFolderName = docId.substring(0, 2);
//		String secondaryFolderName = docId.substring(2, 4);
//		
//		File primaryFolder = new File(this.docFolder, primaryFolderName);
//		if (!primaryFolder.exists())
//			throw new IOException("Invalid document ID '" + documentId + "'");
//		
//		File secondaryFolder = new File(primaryFolder, secondaryFolderName);
//		if (!secondaryFolder.exists())
//			throw new IOException("Invalid document ID '" + documentId + "'");
//		
//		File[] docFiles = secondaryFolder.listFiles(new FileFilter() {
//			public boolean accept(File file) {
////				return ((file != null) && file.isFile() && file.getName().startsWith(docId + ".") && file.getName().endsWith(".xml"));
//				return ((file != null) && file.isFile() && file.getName().startsWith(docId + ".") && (file.getName().endsWith(".xml") || file.getName().endsWith(".xml.zip")));
//			}
//		});
//		for (int f = 0; f < docFiles.length; f++)
//			docFiles[f].renameTo(new File(secondaryFolder, (docFiles[f].getName() + ".old")));
//	}
//	
//	/**
//	 * Retrieve the most recent version number of a document. If no document
//	 * exists with the specified ID, this method returns 0.
//	 * @param documentId the ID of the document
//	 * @return the most recent version number of the document with the specified
//	 *         ID
//	 * @throws IOException
//	 */
//	public int getVersion(String documentId) throws IOException {
//		return this.computeCurrentVersion(this.checkDocId(documentId));
//	}
//	
//	private int computeCurrentVersion(final String docId) {
//		String primaryFolderName = docId.substring(0, 2);
//		String secondaryFolderName = docId.substring(2, 4);
//		
//		//	get storage folder
//		File docFolder = new File(this.docFolder, (primaryFolderName + "/" + secondaryFolderName + "/"));
//		
//		//	check if exists
//		if (!docFolder.exists()) return 0;
//		
//		//	get files belonging to document
//		File[] docFiles = docFolder.listFiles(new FileFilter() {
//			public boolean accept(File file) {
////				return ((file != null) && file.isFile() && file.getName().startsWith(docId + ".") && file.getName().endsWith(".xml"));
//				return ((file != null) && file.isFile() && file.getName().startsWith(docId + ".") && (file.getName().endsWith(".xml") || file.getName().endsWith(".xml.zip")));
//			}
//		});
//		
//		//	no files at all, current version is 0
//		if (docFiles.length == 0) return 0;
//		
//		//	find most recent version number
//		int version = 0;
//		for (int f = 0; f < docFiles.length; f++) {
//			String docFileName = docFiles[f].getName();
//			docFileName = docFileName.substring(docId.length() + ".".length()); // cut ID and dot
////			if (docFileName.length() > 3) { // there's left more than the 'xml' file extension, which will be the case for the most recent version
////				docFileName = docFileName.substring(0, (docFileName.length() - ".xml".length())); // cut file extension
////				try {
////					version = Math.max(version, Integer.parseInt(docFileName));
////				} catch (NumberFormatException nfe) {}
////			}
//			if (docFileName.endsWith(".xml")) { // there's left more than the 'xml' file extension proper, which will be the case for the most recent version
//				docFileName = docFileName.substring(0, (docFileName.length() - ".xml".length())); // cut file extension
//				try {
//					version = Math.max(version, Integer.parseInt(docFileName));
//				} catch (NumberFormatException nfe) {}
//			}
//			else if (docFileName.endsWith(".xml.zip")) { // there's left more than the 'xml' file extension, which will be the case for the most recent version
//				docFileName = docFileName.substring(0, (docFileName.length() - ".xml.zip".length())); // cut file extension
//				try {
//					version = Math.max(version, Integer.parseInt(docFileName));
//				} catch (NumberFormatException nfe) {}
//			}
//		}
//		
//		//	extrapolate to most recent version
//		return (version + 1);
//	}
//	
//	/**
//	 * Check if a document exists.
//	 * @param documentId the ID of the document
//	 * @return true if the document exists, false otherwise
//	 * @throws IOException
//	 */
//	public boolean isDocumentAvailable(String documentId) {
//		String primaryFolderName = documentId.substring(0, 2);
//		String secondaryFolderName = documentId.substring(2, 4);
//		File docFile = new File(this.docFolder, (primaryFolderName + "/" + secondaryFolderName + "/" + documentId + ".xml"));
//		return docFile.exists();
//	}
//	
//	/**
//	 * Retrieve the attributes of a document.
//	 * @param documentId the ID of the document
//	 * @return a Properties object holding the attributes of the document with
//	 *         the specified ID
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public Properties getDocumentAttributes(String documentId) throws DocumentNotFoundException, IOException {
//		return this.getDocumentAttributes(documentId, false);
//	}
//	
//	/**
//	 * Retrieve the attributes of a document. If the argument boolean is set to
//	 * <code>true</code>, the returned Properties includes the update users and
//	 * timestamps from prior versions in <code>updateUser&lt;version&gt;</code>
//	 * and <code>updateTime&lt;version&gt;</code> attributes, e.g.
//	 * <code>updateUser3</code> for the user who created version 3 of the
//	 * document with the argument ID.
//	 * @param documentId the ID of the document
//	 * @param includeUpdateHistory include former update users and timestamps?
//	 * @return a Properties object holding the attributes of the document with
//	 *         the specified ID
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public Properties getDocumentAttributes(String documentId, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
//		
//		//	load current version attributes
//		DocumentRoot doc = this.getDocumentHead(documentId, 0);
//		String[] attributeNames = doc.getAttributeNames();
//		Properties attributes = new Properties();
//		for (int a = 0; a < attributeNames.length; a++) {
//			Object value = doc.getAttribute(attributeNames[a]);
//			if (value instanceof String)
//				attributes.setProperty(attributeNames[a], ((String) value));
//		}
//		
//		//	add version history if asked to
//		if (includeUpdateHistory)
//			this.addUpdateHistory(documentId, this.computeCurrentVersion(documentId), attributes);
//		
//		//	finally ...
//		return attributes;
//	}
//	
//	private void addUpdateHistory(String docId, int version, Properties attributes) throws IOException {
//		
//		//	check if update history already stored in current version
//		if (attributes.containsKey(UPDATE_USER_ATTRIBUTE + "-" + version) && attributes.containsKey(UPDATE_TIME_ATTRIBUTE + "-" + version))
//			return;
//		
//		//	add update history the hard way
//		for (int v = 1; v < version; v++) {
//			DocumentRoot vDoc = this.getDocumentHead(docId, v);
//			Object vUpdateUser = vDoc.getAttribute(UPDATE_USER_ATTRIBUTE);
//			if (vUpdateUser instanceof String)
//				attributes.setProperty((UPDATE_USER_ATTRIBUTE + "-" + v), ((String) vUpdateUser));
//			Object vUpdateTime = vDoc.getAttribute(UPDATE_TIME_ATTRIBUTE);
//			if (vUpdateTime instanceof String)
//				attributes.setProperty((UPDATE_TIME_ATTRIBUTE + "-" + v), ((String) vUpdateTime));
//		}
//		String vUpdateUser = attributes.getProperty(UPDATE_USER_ATTRIBUTE);
//		if (vUpdateUser != null)
//			attributes.setProperty((UPDATE_USER_ATTRIBUTE + "-" + version), vUpdateUser);
//		String vUpdateTime = attributes.getProperty(UPDATE_TIME_ATTRIBUTE);
//		if (vUpdateTime != null)
//			attributes.setProperty((UPDATE_TIME_ATTRIBUTE + "-" + version), vUpdateTime);
//	}
//	
//	/**
//	 * Retrieve the document properties of a document.
//	 * @param documentId the ID of the document
//	 * @return a Properties object holding the document properties of the
//	 *         document with the specified ID
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public Properties getDocumentProperties(String documentId) throws DocumentNotFoundException, IOException {
//		DocumentRoot doc = this.getDocumentHead(documentId, 0);
//		String[] documentPropertyNames = doc.getDocumentPropertyNames();
//		Properties documentProperties = new Properties();
//		for (int a = 0; a < documentPropertyNames.length; a++) {
//			String value = doc.getDocumentProperty(documentPropertyNames[a]);
//			if (value != null)
//				documentProperties.setProperty(documentPropertyNames[a], value);
//		}
//		return documentProperties;
//	}
//	
//	private DocumentRoot getDocumentHead(String documentId, int version) throws DocumentNotFoundException, IOException {
//		String docId = this.checkDocId(documentId);
//		
//		String primaryFolderName = docId.substring(0, 2);
//		String secondaryFolderName = docId.substring(2, 4);
//		
//		int fileVersion = 0;
//		if (version < 0)
//			fileVersion = (this.computeCurrentVersion(docId) + version);
//		else if (version > 0)
//			fileVersion = version;
//		
//		Reader in = null;
//		try {
////			final InputStream fis = new FileInputStream(new File(this.docFolder, (primaryFolderName + "/" + secondaryFolderName + "/" + docId + ((fileVersion == 0) ? "" : ("." + fileVersion)) + ".xml")));
////			InputStream is = new InputStream() {
////				private int last = '\u0000';
////				public int read() throws IOException {
////					if (this.last == '>')
////						return -1;
////					this.last = fis.read();
////					return this.last;
////				}
////				public void close() throws IOException {
////					fis.close();
////				}
////			};
//			File docFile = new File(this.docFolder, (primaryFolderName + "/" + secondaryFolderName + "/" + docId + ((fileVersion == 0) ? "" : ("." + fileVersion)) + ".xml"));
//			final InputStream docIn;
//			if (docFile.exists())
//				docIn = new FileInputStream(docFile);
//			else {
//				File docZipFile = new File(this.docFolder, (primaryFolderName + "/" + secondaryFolderName + "/" + docId + ((fileVersion == 0) ? "" : ("." + fileVersion)) + ".xml.zip"));
//				docIn = new ZipInputStream(new FileInputStream(docZipFile));
//			}
//			InputStream is = new InputStream() {
//				private int last = '\u0000';
//				public int read() throws IOException {
//					if (this.last == '>')
//						return -1;
//					this.last = docIn.read();
//					return this.last;
//				}
//				public void close() throws IOException {
//					docIn.close();
//				}
//			};
//			in = new InputStreamReader(is, this.encoding);
//			DocumentRoot doc = GenericGamtaXML.readDocument(in);
//			doc.setAttribute(DOCUMENT_ID_ATTRIBUTE, documentId);
//			doc.setDocumentProperty(DOCUMENT_ID_ATTRIBUTE, documentId);
//			return doc;
//		}
//		catch (FileNotFoundException fnfe) {
//			throw new DocumentNotFoundException(documentId, fileVersion);
//		}
//		finally {
//			if (in != null)
//				in.close();
//		}
//	}
//	
//	/**
//	 * Load a document from storage (the most recent version)
//	 * @param documentId the ID of the document to load
//	 * @return the document with the specified ID
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public DocumentRoot loadDocument(String documentId) throws DocumentNotFoundException, IOException {
//		return this.loadDocument(documentId, 0, false);
//	}
//	
//	/**
//	 * Load a document from storage (the most recent version)
//	 * @param documentId the ID of the document to load
//	 * @param includeUpdateHistory include former update users and timestamps?
//	 * @return the document with the specified ID
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public DocumentRoot loadDocument(String documentId, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
//		return this.loadDocument(documentId, 0, includeUpdateHistory);
//	}
//	
//	/**
//	 * Load a specific version of a document from storage. A positive version
//	 * number indicates an actual version specifically, while a negative version
//	 * number indicates a version backward relative to the most recent version.
//	 * Version number 0 always returns the most recent version.
//	 * @param documentId the ID of the document to load
//	 * @param version the version to load
//	 * @return the document with the specified ID
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public DocumentRoot loadDocument(String documentId, int version) throws DocumentNotFoundException, IOException {
//		return this.loadDocument(documentId, version, false);
//	}
//	
//	/**
//	 * Load a specific version of a document from storage. A positive version
//	 * number indicates an actual version specifically, while a negative version
//	 * number indicates a version backward relative to the most recent version.
//	 * Version number 0 always returns the most recent version.
//	 * @param documentId the ID of the document to load
//	 * @param version the version to load
//	 * @param includeUpdateHistory include former update users and timestamps?
//	 * @return the document with the specified ID
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public DocumentRoot loadDocument(String documentId, int version, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
//		DocumentReader dr = this.loadDocumentAsStream(documentId, version, includeUpdateHistory);
//		try {
//			return GenericGamtaXML.readDocument(dr);
//		}
//		finally {
//			dr.close();
//		}
//	}
//	
//	/**
//	 * Load a document from storage (the most recent version) as a character
//	 * stream. In situations where a document is not required in its
//	 * deserialized form, e.g. if it is intended to be written to some output
//	 * stream, this method facilitates avoiding deserialization and
//	 * reserialization.
//	 * @param documentId the ID of the document to load
//	 * @return a reader providing the document with the specified ID in its
//	 *         serialized form
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public DocumentReader loadDocumentAsStream(String documentId) throws DocumentNotFoundException, IOException {
//		return this.loadDocumentAsStream(documentId, 0, false);
//	}
//	
//	/**
//	 * Load a document from storage (the most recent version) as a character
//	 * stream. In situations where a document is not required in its
//	 * deserialized form, e.g. if it is intended to be written to some output
//	 * stream, this method facilitates avoiding deserialization and
//	 * reserialization.
//	 * @param documentId the ID of the document to load
//	 * @return a reader providing the document with the specified ID in its
//	 *         serialized form
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public DocumentReader loadDocumentAsStream(String documentId, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
//		return this.loadDocumentAsStream(documentId, 0, includeUpdateHistory);
//	}
//	
//	/**
//	 * Load a document from storage as a character stream. In situations where a
//	 * document is not required in its deserialized form, e.g. if it is intended
//	 * to be written to some output stream, this method facilitates avoiding
//	 * deserialization and reserialization. A positive version number indicates
//	 * an actual version specifically, while a negative version number indicates
//	 * a version backward relative to the most recent version. Version number 0
//	 * always returns the most recent version.
//	 * @param documentId the ID of the document to load
//	 * @param version the version to load
//	 * @return a reader providing the document with the specified ID in its
//	 *         serialized form
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public DocumentReader loadDocumentAsStream(String documentId, int version) throws DocumentNotFoundException, IOException {
//		return this.loadDocumentAsStream(documentId, version, false);
//	}
//	
//	/**
//	 * Load a document from storage as a character stream. In situations where a
//	 * document is not required in its deserialized form, e.g. if it is intended
//	 * to be written to some output stream, this method facilitates avoiding
//	 * deserialization and reserialization. A positive version number indicates
//	 * an actual version specifically, while a negative version number indicates
//	 * a version backward relative to the most recent version. Version number 0
//	 * always returns the most recent version.
//	 * @param documentId the ID of the document to load
//	 * @param version the version to load
//	 * @return a reader providing the document with the specified ID in its
//	 *         serialized form
//	 * @throws DocumentNotFoundException if the specified document ID is invalid
//	 *             (no document is stored by this ID, or specified version does
//	 *             not exist)
//	 * @throws IOException if any other IOException occurs
//	 */
//	public DocumentReader loadDocumentAsStream(String documentId, int version, boolean includeUpdateHistory) throws DocumentNotFoundException, IOException {
//		String docId = this.checkDocId(documentId);
//		
//		String primaryFolderName = docId.substring(0, 2);
//		String secondaryFolderName = docId.substring(2, 4);
//		
//		int fileVersion = 0;
//		if (version < 0)
//			fileVersion = (this.computeCurrentVersion(docId) + version);
//		else if (version > 0)
//			fileVersion = version;
//		
//		try {
//			File docFile = new File(this.docFolder, (primaryFolderName + "/" + secondaryFolderName + "/" + docId + ((fileVersion == 0) ? "" : ("." + fileVersion)) + ".xml"));
//			InputStream docIn;
//			int docSize;
//			if (docFile.exists()) {
//				docIn = new FileInputStream(docFile);
//				docSize = ((int) docFile.length());
//			}
//			else {
//				File docZipFile = new File(this.docFolder, (primaryFolderName + "/" + secondaryFolderName + "/" + docId + ((fileVersion == 0) ? "" : ("." + fileVersion)) + ".xml.zip"));
//				ZipInputStream docZipIn = new ZipInputStream(new FileInputStream(docZipFile));
//				ZipEntry docZipEntry = docZipIn.getNextEntry();
//				docSize = ((int) docZipEntry.getSize());
//				docIn = docZipIn;
//			}
//			DocumentReader dr = new DocumentReader(new InputStreamReader(docIn, this.encoding), docSize);
//			dr.setAttribute(DOCUMENT_ID_ATTRIBUTE, documentId);
//			dr.setDocumentProperty(DOCUMENT_ID_ATTRIBUTE, documentId);
//			if (includeUpdateHistory) {
//				Properties duh = new Properties();
//				String[] dans = dr.getAttributeNames();
//				for (int a = 0; a < dans.length; a++) {
//					if (dans[a].startsWith(UPDATE_USER_ATTRIBUTE) || dans[a].startsWith(UPDATE_TIME_ATTRIBUTE)) {
//						Object av = dr.getAttribute(dans[a]);
//						if (av instanceof String)
//							duh.setProperty(dans[a], ((String) av));
//					}
//				}
//				this.addUpdateHistory(docId, version, duh);
//				for (Iterator kit = duh.keySet().iterator(); kit.hasNext();) {
//					String dan = ((String) kit.next());
//					if (!dr.hasAttribute(dan))
//						dr.setAttribute(dan, duh.getProperty(dan));
//				}
//			}
//			return dr;
//		}
//		catch (FileNotFoundException fnfe) {
//			throw new DocumentNotFoundException(documentId, fileVersion);
//		}
//	}
//	
//	/**
//	 * Retrieve a list of the IDs of all the document stored in this DST
//	 * @return a list of the IDs of all the document stored in this DST
//	 */
//	public String[] getDocumentIDs() {
//		LinkedHashSet docIdList = new LinkedHashSet();
//		
//		//	process primary level folders
//		File[] primaryLevel = docFolder.listFiles(archiveLevelFilter);
//		for (int p = 0; p < primaryLevel.length; p++) {
//			
//			//	process secondary level folders
//			File[] secondaryLevel = primaryLevel[p].listFiles(archiveLevelFilter);
//			for (int s = 0; s < secondaryLevel.length; s++) {
//				
//				//	inspect files in secondary level folders
//				File[] archiveContent = secondaryLevel[s].listFiles(archiveFileFilter);
//				for (int a = 0; a < archiveContent.length; a++) {
//					
//					//	parse ID from file name
//					String contentFileName = archiveContent[a].getName();
//					docIdList.add(contentFileName.substring(0, 32));
//				}
//			}
//		}
//		
//		//	return list
//		return ((String[]) docIdList.toArray(new String[docIdList.size()]));
//	}
//	
//	private String checkDocId(String documentId) throws IOException {
//		if ((documentId == null) || (documentId.trim().length() == 0))
//			throw new IOException("Document ID must not be null or empty.");
//		else if (!documentId.trim().matches("[a-zA-Z0-9]++")) 
//			throw new IOException("Invalid document ID '" + documentId + "' - document ID must consist of letters and digits only.");
//		else if (documentId.trim().length() == 32)
//			return documentId.trim();
//		else {
//			documentId = documentId.trim();
//			String docId = documentId;
//			while (docId.length() < 32)
//				docId = (docId + documentId);
//			return docId.substring(0, 32);
//		}
//	}
//	
//	private void zipDocumentFile(String fileName) {
//		File docFile = new File(this.docFolder, fileName);
//		File docZipFile = new File(this.docFolder, (fileName + ".zip"));
//		
//		try {
//			//	zip up document file ...
//			FileInputStream docFileIn = new FileInputStream(docFile);
//			ZipOutputStream docZipFileOut = new ZipOutputStream(new FileOutputStream(docZipFile));
//			docZipFileOut.putNextEntry(new ZipEntry(docFile.getName()));
//			byte[] buffer = new byte[1024];
//			for (int r; (r = docFileIn.read(buffer, 0, buffer.length)) != -1;)
//				docZipFileOut.write(buffer, 0, r);
//			docZipFileOut.closeEntry();
//			docZipFileOut.flush();
//			docZipFileOut.close();
//			docFileIn.close();
//			
//			//	... and delete it afterwards
//			docFile.delete();
//		}
//		catch (Exception e) {
//			System.out.println("Error zipping up document file '" + docFile.getAbsolutePath() + "': " + e.getMessage());
//			e.printStackTrace(System.out);
//		}
//	}
////	
////	public static void main(String[] args) throws Exception {
////		File docFolder = new File("./Components/GgServerSRSData/DocumentsGone/");
////		DocumentStore dst = new DocumentStore(docFolder);
////		//	dst.zipOldDocumentVersions();
////		String docId = "0000 C505 BB5D 484C 76BE 9AB6 999D EB23".replaceAll("\\s", "");
////		DocumentRoot doc = dst.loadDocument(docId, -1);
////		doc.setAttribute("testTime", ("" + System.currentTimeMillis()));
////		int version = dst.storeDocument(doc, docId);
////		System.out.println("Stored as version " + version);
////	}
//}