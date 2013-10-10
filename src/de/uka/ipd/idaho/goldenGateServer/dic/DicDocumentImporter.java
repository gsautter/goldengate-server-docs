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
package de.uka.ipd.idaho.goldenGateServer.dic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import de.uka.ipd.idaho.easyIO.util.RandomByteSource;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.constants.LiteratureConstants;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;

/**
 * Abstract super class for all GoldenGATE DIC document importer plugins. This
 * class provides a reference to the parent DIC and the importer's data path.
 * 
 * @author sautter
 */
public abstract class DicDocumentImporter implements LiteratureConstants {
	
	/**
	 * Wrapper for an imported document ID, and later the respective document
	 * file, plus sub class specific extensions.
	 * 
	 * @author sautter
	 */
	public static class ImportDocument {
		
		/** the file the document is stored / cached in */
		public final File docFile;
		
		/**
		 * the ID of the document (to be specified by the importer, must be
		 * constant and unique for any given document; can be, for instance, an
		 * MD5 hash of the URL a document was imported from)
		 */
		public final String docId;
		
		/**
		 * the name of the document (defaults to ID if null)
		 */
		public final String docName;
		
		/**
		 * @param docFile the file the document is stored in
		 * @param docId the ID of the document
		 * @param docName the name of the document
		 */
		public ImportDocument(File docFile, String docId, String docName) {
			this.docFile = docFile;
			this.docId = docId;
			this.docName = ((docName == null) ? docId : docName);
		}
	}
	
	/** the importer's data path */
	protected File dataPath;
	
	/** the GoldenGATE DIC this importer is run in */
	protected GoldenGateDIC parent;
	
	private String name;
	
	/** the folder used to cache documents in after import */
	protected File cacheFolder;

	/**
	 * Constructor
	 * @param name the name identifying the importer inside DIC (to be provided
	 *            by implementing classes); the name must not be empty, must
	 *            consist of 32 characters at most, and must consist only of
	 *            upper and lower case Latin letters, no whitespaces.
	 */
	protected DicDocumentImporter(String name) {
		this.name = name;
	}
	
	/**
	 * Make the importer know its parent GoldenGATE DIC.
	 * @param parent the parent DIC
	 */
	public void setParent(GoldenGateDIC parent) {
		this.parent = parent;
	}
	
	/**
	 * Make the importer know its data path.
	 * @param dataPath the importer's data path
	 */
	public void setDataPath(File dataPath) {
		this.dataPath = dataPath;
		this.cacheFolder = new File(this.dataPath, "cache");
		if (!this.cacheFolder.exists())
			this.cacheFolder.mkdirs();
	}
	
	/**
	 * Initialize the importer. This method is invoked by the parent DIC after
	 * parent and data path are set. This default implementation does nothing,
	 * sub classes are welcome to overwrite it as needed.
	 */
	public void init() {}
	
	/**
	 * Shut down the importer. This method is invoked by the parent DIC right
	 * before shutdown. This default implementation does nothing, sub classes
	 * are welcome to overwrite it as needed.
	 */
	public void exit() {}
	
	/**
	 * Get the importer's name (the one provided to the constructor).
	 * @return the importer's name
	 */
	public String getName() {
		return this.name;
	}
	
	/**
	 * Provide the user name with which to store documents coming from this
	 * importer in the target DIO. The returned String must not be empty, must
	 * consist of 32 characters at most, and must consist only of upper and
	 * lower case Latin letters, no whitespaces.
	 * @return the import user name
	 */
	public abstract String getImportUserName();
	
	private static final int milliSecondsPerDay = (24 * 60 * 60 * 1000);
	private long lastImportStart = (System.currentTimeMillis() - ((int) (Math.random() * milliSecondsPerDay))); // simply pretend last import was some random time within last 24 hours TODO figure out if this makes sense
	void setLastImportStart() {
		this.lastImportStart = System.currentTimeMillis();
	}
	long getLastImportStart() {
		return this.lastImportStart;
	}
	private long lastImportAttempt = this.lastImportStart;
	void setLastImportAttempt() {
		this.lastImportAttempt = System.currentTimeMillis();
	}
	long getLastImportAttempt() {
		return this.lastImportAttempt;
	}
	private long lastImportComplete = this.lastImportStart;
	void setLastImportComplete() {
		this.lastImportComplete = System.currentTimeMillis();
		this.lastImportAttempt = this.lastImportComplete;
	}
	long getLastImportComplete() {
		return this.lastImportComplete;
	}
	void resetLastImport() {
		this.lastImportAttempt = 0;
		this.lastImportStart = 0;
		this.lastImportComplete = 0;
	}
	
//	/**
//	 * Trigger document import. This method is invoked by the parent DIC to
//	 * trigger the import. If import is successful, the last import timestamp is
//	 * updated. In particular, this method first invokes importDocuments(), and
//	 * then updates the timestamp.
//	 * @return an array of descriptors for the freshly imported documents
//	 * @throws IOException
//	 */
//	public final ImportDocument[] doImport() throws IOException {
//		this.lastImportAttempt = System.currentTimeMillis();
//		ImportDocument[] imported = this.importDocuments();
//		this.lastImportComplete = System.currentTimeMillis();
//		this.lastImportAttempt = this.lastImportComplete;
//		return imported;
//	}
//	
//	/**
//	 * Import documents, i.e. download them to the importer's data path (or some
//	 * folder below it) and return descriptor objects pointing to the documents.
//	 * The parent DIC expects the documents to be in generic GAMTA document
//	 * format. Importers are recommended, but not required to use the
//	 * cacheDocument() method, which fulfills this requirement automatically.
//	 * @return an array of descriptors for the freshly imported documents
//	 * @throws IOException
//	 */
//	protected abstract ImportDocument[] importDocuments() throws IOException;
//	
	/**
	 * Get the documents to import, i.e. fetch their identifiers, plus possible
	 * importer specific data, e.g. URLs. Implementations of this method should
	 * not download the documents proper, however, as downloading large numbers
	 * of documents in a single method call leaves too little control to the
	 * import coordinator. Download is intended to be implemented in the
	 * <code>importDocument()</code> method. The runtime type of the document
	 * descriptors handed to the latter method is the same as the one returned
	 * by the respective implementation of this method.
	 * @return an array of descriptors for the to-import documents
	 * @throws IOException
	 */
	protected abstract ImportDocument[] getImportDocuments() throws IOException;
	
	/**
	 * Import the document associated with a given descriptor. Implementations
	 * should download documents to the importer's data path (or some folder
	 * below it) and return descriptor objects pointing to the documents.
	 * The parent DIC expects the documents to be in generic GAMTA document
	 * format. Importers are recommended, but not required to use the
	 * <code>cacheDocument()</code> method, which fulfills this requirement
	 * automatically. The runtime type of the document descriptors handed to
	 * this method is the same as the one of the descriptors returned by the
	 * <code>getImportDocuments()</code> method.
	 * @param id the descriptor of the document to import
	 * @return the argument descriptor
	 * @throws IOException
	 */
	protected abstract ImportDocument importDocument(ImportDocument id) throws IOException;
	
	/**
	 * Store an imported document in the importer's cache folder to be later
	 * handed over to the parent DIC. In particular, the file is stored as
	 * '&lt;docId&gt;.cached' in the importer's cache folder. If the specified
	 * document ID is null, the 'docId' attribute of the argument document will
	 * be used. If the latter is null as well, an exception will be thrown.
	 * @param doc the document to store
	 * @param docId the ID to store the document with
	 * @return a descriptor for the document
	 * @throws IOException
	 */
	protected ImportDocument cacheDocument(DocumentRoot doc, String docId) throws IOException {
		return this.cacheDocument(doc, docId, null);
	}
	
	/**
	 * Store an imported document in the importer's cache folder to be later
	 * handed over to the parent DIC. In particular, the file is stored as
	 * '&lt;docId&gt;.cached' in the importer's cache folder. If the specified
	 * document ID is null, the 'docId' attribute of the argument document will
	 * be used. If the latter is null as well, an exception will be thrown. If
	 * the specified document name is null, the document's 'docName' attribute
	 * will be used. If the latter is null as well, the document ID will be
	 * used.
	 * @param doc the document to store
	 * @param docId the ID to store the document with
	 * @param docId the name of the document (defaults to ID if null)
	 * @return a descriptor for the document
	 * @throws IOException
	 */
	protected ImportDocument cacheDocument(DocumentRoot doc, String docId, String docName) throws IOException {
		if (docId == null)
			docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE));
		if (docId == null)
			throw new IOException("Invalid document ID.");
		
		if (docName == null)
			docName = ((String) doc.getAttribute(GoldenGateDioConstants.DOCUMENT_NAME_ATTRIBUTE));
		
		File cacheFile = this.getDocumentCacheFile(docId);
		
		FileOutputStream cacheOut = new FileOutputStream(cacheFile);
		GenericGamtaXML.storeDocument(doc, cacheOut);
		cacheOut.flush();
		cacheOut.close();
		
		return new ImportDocument(cacheFile, docId, docName);
	}
	
	/**
	 * Check whether a document is already cached. In particular, this method
	 * checks whether a file named '&lt;docId&gt;.cached' exists in the
	 * importer's cache folder. Therefore, sub classes not using the
	 * cacheDocument() method, and not caching documents the same way as the
	 * cacheDocument() method have to overwrite this method as well if they want
	 * it to remain meaningful.
	 * @param docId the ID of the document to look up
	 * @return true if the document is cached, false otherwise
	 * @throws IOException
	 */
	protected boolean isDocumentCached(String docId) throws IOException {
		return this.getDocumentCacheFile(docId).exists();
	}
	
	/**
	 * Retrieve a file object pointing to the cache file of a document with a
	 * given ID.
	 * @param docId the document ID to get the file for
	 * @return a file object pointing to the cache file of a document with the
	 *         specified ID
	 */
	protected File getDocumentCacheFile(String docId) {
		return new File(this.cacheFolder, (docId + ".cached"));
	}
	
	/**
	 * Obtain the MD5 hash of a string, in hex representation.
	 * @param str the string to hash
	 * @return the MD5 hash of the argument string
	 */
	public static String hash(String str) {
		if (md5Digester == null) {
			try {
				md5Digester = MessageDigest.getInstance("MD5");
			}
			catch (NoSuchAlgorithmException nsae) {
				System.out.println(nsae.getClass().getName() + " (" + nsae.getMessage() + ") while creating MD5 digester.");
				nsae.printStackTrace(System.out); // should not happen, but Java don't know ...
				return Gamta.getAnnotationID(); // use random value so a document is regarded as new
			}
		}
		md5Digester.reset();
		try {
			byte[] bytes = str.getBytes("UTF-8");
			md5Digester.update(bytes);
		}
		catch (IOException ioe) {
			System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while hashing '" + str + "'.");
			ioe.printStackTrace(System.out); // should not happen, but Java don't know ...
			return Gamta.getAnnotationID(); // use random value so a document is regarded as new
		}
		byte[] checksumBytes = md5Digester.digest();
		return new String(RandomByteSource.getHexCode(checksumBytes));
	}
	private static MessageDigest md5Digester = null;
}
