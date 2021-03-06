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
package de.uka.ipd.idaho.goldenGateServer.dic;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.easyIO.util.HashUtils;
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
		
		/**
		 * the ID of the import or document import source (to be specified by
		 * the importer, must be constant and unique for any given document;
		 * can be, for instance, an MD5 hash of the URL a document was imported
		 * from)
		 */
		public final String importId;
		
		/** the file the document is stored / cached in */
		public final File docFile;
		
		/**
		 * the name of the document (defaults to ID if null)
		 */
		public final String docName;
		
		/**
		 * @param importId the ID of the import / document source
		 * @param docFile the file the document is stored in
		 * @param docName the name of the document
		 */
		public ImportDocument(String importId, File docFile, String docName) {
			this.importId = importId;
			this.docFile = docFile;
			this.docName = ((docName == null) ? importId : docName);
		}
	}
	
	/** the importer's data path */
	protected File dataPath;
	
	/** the importer's configuration */
	protected Settings configuration;
	
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
		
		//	load configuration
		File configFile = new File(this.dataPath, "config.cnfg");
		if (configFile.exists()) {
			this.configuration = Settings.loadSettings(configFile);
			String cacheFolderName = this.configuration.getSetting("cacheFolderName", "cache");
			while (cacheFolderName.startsWith("./"))
				cacheFolderName = cacheFolderName.substring("./".length());
			this.cacheFolder = (((cacheFolderName.indexOf(":\\") == -1) && (cacheFolderName.indexOf(":/") == -1) && !cacheFolderName.startsWith("/")) ? new File(this.dataPath, cacheFolderName) : new File(cacheFolderName));
		}
		else this.configuration = new Settings();
		
		//	make sure we have a cache folder
		if (this.cacheFolder == null)
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
	
	private String[] importParams = null;
	String[] getImportParameters() {
		return this.importParams;
	}
	void setImportParameters(String[] importParams) {
		this.importParams = importParams;
	}
	
	void resetLastImport(String[] importParams) {
		this.lastImportAttempt = 0;
		this.lastImportStart = 0;
		this.lastImportComplete = 0;
		this.importParams = importParams;
	}
	
	/**
	 * Provide explanations for the custom import parameters an importer can
	 * use. If parameters are disregarded altogether, this method may return
	 * null. This default implementation does return null, sub classes that do
	 * use import parameters are welcome to overwrite it as needed.
	 * @return an array with explanations of import parameters
	 */
	protected String[] getParameterExplanations() {
		return null;
	}
	
	/**
	 * Get the documents to import, i.e. fetch their identifiers, plus possible
	 * importer specific data, e.g. URLs. Implementations of this method should
	 * not download the documents proper, however, as downloading large numbers
	 * of documents in a single method call leaves too little control to the
	 * import coordinator. Download is intended to be implemented in the
	 * <code>importDocument()</code> method. The runtime type of the document
	 * descriptors handed to the latter method is the same as the one returned
	 * by the respective implementation of this method.
	 * @param parameters any parameters specified via the console (may be null)
	 * @return an array of descriptors for the to-import documents
	 * @throws IOException
	 */
	protected abstract ImportDocument[] getImportDocuments(String[] parameters) throws IOException;
	
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
	 * @param parameters any parameters specified via the console (may be null)
	 * @return the argument descriptor
	 * @throws IOException
	 */
	protected abstract ImportDocument importDocument(ImportDocument id, String[] parameters) throws IOException;
	
	/**
	 * Store an imported document in the importer's cache folder to be later
	 * handed over to the parent DIC. In particular, the file is stored as
	 * '&lt;importId&gt;.cached' in the importer's cache folder. If the latter
	 * is null as well, an exception will be thrown.
	 * @param importId the ID of the import / document source
	 * @param doc the document to store
	 * @return a descriptor for the document
	 * @throws IOException
	 */
	protected ImportDocument cacheDocument(String importId, DocumentRoot doc) throws IOException {
		return this.cacheDocument(importId, doc, null);
	}
	
	/**
	 * Store an imported document in the importer's cache folder to be later
	 * handed over to the parent DIC. In particular, the file is stored as
	 * '&lt;importId&gt;.cached' in the importer's cache folder. If the
	 * specified document name is null, the document's 'docName' attribute
	 * will be used. If the latter is null as well, the import ID will be used.
	 * @param importId the ID of the import / document source
	 * @param doc the document to store
	 * @param docName the name of the document (defaults to ID if null)
	 * @return a descriptor for the document
	 * @throws IOException
	 */
	protected ImportDocument cacheDocument(String importId, DocumentRoot doc, String docName) throws IOException {
		if (importId == null)
			throw new IOException("Invalid document import ID.");
		
		if (docName == null)
			docName = ((String) doc.getAttribute(GoldenGateDioConstants.DOCUMENT_NAME_ATTRIBUTE));
		
		File cacheFile = this.getImportDocumentCacheFile(importId);
		
		FileOutputStream cacheOut = new FileOutputStream(cacheFile);
		GenericGamtaXML.storeDocument(doc, cacheOut);
		cacheOut.flush();
		cacheOut.close();
		
		return new ImportDocument(importId, cacheFile, docName);
	}
	
	/**
	 * Check whether a document is already cached. In particular, this method
	 * checks whether a file named '&lt;importId&gt;.cached' exists in the
	 * importer's cache folder. Therefore, sub classes not using the
	 * cacheDocument() method, and not caching documents the same way as the
	 * cacheDocument() method have to overwrite this method as well if they want
	 * it to remain meaningful.
	 * @param importId the ID of the import / document source
	 * @return true if the document is cached, false otherwise
	 * @throws IOException
	 */
	protected boolean isImportDocumentCached(String importId) throws IOException {
		return this.getImportDocumentCacheFile(importId).exists();
	}
	
	/**
	 * Retrieve a file object pointing to the cache file of the document from an
	 * import with a given ID.
	 * @param importId the import ID to get the document cache file for
	 * @return a file object pointing to the cache file of the document from an
	 *         import with the specified ID
	 */
	protected File getImportDocumentCacheFile(String importId) {
		return new File(this.cacheFolder, (importId + ".cached"));
	}
	
	/**
	 * Obtain the MD5 hash of a string, in hex representation.
	 * @param str the string to hash
	 * @return the MD5 hash of the argument string
	 */
	public static String hash(String str) {
		String hash = HashUtils.getMd5(str);
		return ((hash == null) ? Gamta.getAnnotationID() : hash); // use random value so a document is regarded as new
	}
}
