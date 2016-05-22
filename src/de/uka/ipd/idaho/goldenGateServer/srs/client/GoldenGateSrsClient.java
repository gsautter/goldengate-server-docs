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
package de.uka.ipd.idaho.goldenGateServer.srs.client;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.WeakHashMap;

import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants;
import de.uka.ipd.idaho.goldenGateServer.srs.data.CollectionStatistics;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;

/**
 * Client for searching documents in the collection indexed by a GoldenGATE SRS
 * server component.
 * 
 * @author sautter
 */
public class GoldenGateSrsClient implements GoldenGateSrsConstants {
	
	private static WeakHashMap cachingInstances = new WeakHashMap();
	private static HashSet cacheFolders = new HashSet();
	private static GoldenGateSrsClient cacheDataFetcher = null;
	private static Thread cacheCleaner = null;
	private static void registerCachingInstance(GoldenGateSrsClient srsc) {
		cachingInstances.put(srsc, "");
		cacheFolders.add(srsc.cacheFolder);
		
		//	start maintenance thread if not already running
		if (cacheDataFetcher == null) {
			cacheDataFetcher = srsc;
			cacheCleaner = new Thread() {
				public void run() {
					
					//	run as long as there are active instances
					while (cachingInstances.size() != 0) {
						try {
							Thread.sleep(10 * 60 * 1000);
						} catch (InterruptedException ie) {}
						System.out.println("Running SRS Client cache cleanup");
						
						//	compute last collection update from master document list
						long lastUpdate = 0;
						try {
							DocumentList mdl = cacheDataFetcher.getDocumentList(null);
							while (mdl.hasNextElement()) {
								DocumentListElement dle = mdl.getNextDocumentListElement();
								try {
									lastUpdate = Math.max(lastUpdate, Long.parseLong((String) dle.getAttribute(UPDATE_TIME_ATTRIBUTE)));
								} catch (NumberFormatException nfe) {}
							}
						}
						catch (Throwable t) {
							t.printStackTrace(System.out);
						}
						System.out.println("Last update time is " + lastUpdate);
						
						//	no documents, or communication error
						if (lastUpdate == 0)
							continue;
						
						//	trigger cleanup
						cleanUpCacheFolders(lastUpdate);
					}
					
					//	make way in case new clients are produced
					cacheCleaner = null;
					cacheDataFetcher = null;
				}
			};
			cacheCleaner.start();
			System.out.println("GoldenGATE SRS Client cache cleaner created");
		}
	}
	
	private static void cleanUpCacheFolders(final long lastUpdate) {
		final ArrayList cachingInstanceList = new ArrayList(cachingInstances.keySet());
		final ArrayList cacheFolderList = new ArrayList(cacheFolders);
		Thread cacheCleaner = new Thread() {
			public void run() {
				
				//	invalidate statistics and search field caches
				for (Iterator ciit = cachingInstanceList.iterator(); ciit.hasNext();) {
					GoldenGateSrsClient cachingInstance = ((GoldenGateSrsClient) ciit.next());
					if (cachingInstance.sfgCacheTimestamp < lastUpdate)
						cachingInstance.sfgCache = null;
					synchronized (cachingInstance.csCache) {
						HashSet csCacheKeys = new LinkedHashSet(cachingInstance.csCache.keySet());
						for (Iterator csceit = csCacheKeys.iterator(); csceit.hasNext();) {
							Long csCacheKey = ((Long) csceit.next());
							CsCacheEntry cce = ((CsCacheEntry) cachingInstance.csCache.get(csCacheKey));
							if (cce.retrieved < lastUpdate)
								cachingInstance.csCache.remove(csCacheKey);
						}
					}
				}
				System.out.println("Statistics and search fields cleaned");
				
				//	invalidate all cache files older than last update
				int tRetainFileCount = 0;
				int tCleanupFileCount = 0;
				int tErrorFileCount = 0;
				for (Iterator cfit = cacheFolderList.iterator(); cfit.hasNext();) {
					File cacheFolder = ((File) cfit.next());
					File[] cacheFiles = cacheFolder.listFiles(new FileFilter() {
						public boolean accept(File cacheFile) {
							return cacheFile.getName().endsWith(".cached");
						}
					});
					int retainFileCount = 0;
					int cleanupFileCount = 0;
					int errorFileCount = 0;
					for (int f = 0; f < cacheFiles.length; f++) {
						if (cacheFiles[f].lastModified() >= lastUpdate)
							retainFileCount++;
						else try {
							cacheFiles[f].delete();
							cleanupFileCount++;
						}
						catch (Exception e) {
							System.out.println("Could not delete cache file '" + cacheFiles[f].getAbsolutePath() + "': " + e.getMessage());
							errorFileCount++;
						}
					}
					System.out.println("Cleaned up " + cleanupFileCount + " cache files, retained " + retainFileCount + ", failed to clean up " + errorFileCount);
					tRetainFileCount += retainFileCount;
					tCleanupFileCount += cleanupFileCount;
					tErrorFileCount += errorFileCount;
				}
				System.out.println("In total cleaned up " + tCleanupFileCount + " cache files, retained " + tRetainFileCount + ", failed to clean up " + tErrorFileCount);
				
				//	invalidate all caching files older than 15 minutes (caching should never take that long)
				final long currentTime = System.currentTimeMillis();
				tRetainFileCount = 0;
				tCleanupFileCount = 0;
				tErrorFileCount = 0;
				for (Iterator cfit = cacheFolderList.iterator(); cfit.hasNext();) {
					File cacheFolder = ((File) cfit.next());
					File[] cacheFiles = cacheFolder.listFiles(new FileFilter() {
						public boolean accept(File cacheFile) {
							return cacheFile.getName().endsWith(".caching");
						}
					});
					int retainFileCount = 0;
					int cleanupFileCount = 0;
					int errorFileCount = 0;
					for (int f = 0; f < cacheFiles.length; f++) {
						if (cacheFiles[f].lastModified() >= (currentTime - (1000 * 60 * 15)))
							retainFileCount++;
						else try {
							cacheFiles[f].delete();
							cleanupFileCount++;
						}
						catch (Exception e) {
							System.out.println("Could not delete stale caching file '" + cacheFiles[f].getAbsolutePath() + "': " + e.getMessage());
							errorFileCount++;
						}
					}
					System.out.println("Cleaned up " + cleanupFileCount + " stale caching files, retained " + retainFileCount + ", failed to clean up " + errorFileCount);
					tRetainFileCount += retainFileCount;
					tCleanupFileCount += cleanupFileCount;
					tErrorFileCount += errorFileCount;
				}
				System.out.println("In total cleaned up " + tCleanupFileCount + " stale caching files, retained " + tRetainFileCount + ", failed to clean up " + tErrorFileCount);
			}
		};
		cacheCleaner.start();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#finalize()
	 */
	protected void finalize() throws Throwable {
		cachingInstances.remove(this);
	}
	
	private File cacheFolder = null;
	
	/**
	 * Test whether caching is enabled for this GoldenGATE SRS Client, i.e.,
	 * whether the cache folder is set to a valid directory.
	 * @return true is caching is enabled, false otherwise
	 */
	public boolean isCachingEnabled() {
		return (this.cacheFolder != null);
	}
	
	/**
	 * Make the GoldenGATE SRS Client know the folder to use for caching result
	 * data. If the specified file is null or does not denote a directory, the
	 * cache folder is set to null, disabling caching. If caching is enabled
	 * (the cache folder is set to a non-null directory), search results (a)
	 * will be cached so subsequent requests using the same query parameters can
	 * be answered based on the cache and are therefore faster and (b) results
	 * can be paged.
	 * @param cacheFolder the cache folder
	 */
	public void setCacheFolder(File cacheFolder) {
		
		//	if argument file is not a directory, set it to null
		if ((cacheFolder != null) && cacheFolder.exists() && !cacheFolder.isDirectory())
			cacheFolder = null;
		
		//	set cache folder
		this.cacheFolder = cacheFolder;
		
		//	make sure cache folder exists, disable cache if creation fails
		if (this.cacheFolder != null) {
			this.cacheFolder.mkdirs();
			if (!this.cacheFolder.exists() || !this.cacheFolder.isDirectory())
				this.cacheFolder = null;
			else registerCachingInstance(this);
		}
	}
	
	Reader getCacheReader(String command, int cacheEntryId) {
		
		//	cache disabled
		if (this.cacheFolder == null) return null;
		
		//	create file & check timestamp
		File cacheFile = new File(this.cacheFolder, (command + "." + cacheEntryId + ".cached"));
		if (cacheFile.exists()) try {
			System.out.println("GoldenGateSrsClient: cache hit");
			return new BufferedReader(new InputStreamReader(new FileInputStream(cacheFile), "UTF-8"));
		}
		catch (IOException ioe) {
			System.out.println("GoldenGateSrsClient: cache lookup error " + ioe.getClass().getName() + " (" + ioe.getMessage() + ")");
			ioe.printStackTrace(System.out);
		}
		
		//	cache miss, or lookup error
		return null;
	}
	
	Writer getCacheWriter(String command, int cacheEntryId) {
		
		//	cache disabled
		if (this.cacheFolder == null)
			return null;
		
		//	delete existing cache file (likely cache agnostic request)
		File existingCacheFile = new File(this.cacheFolder, (command + "." + cacheEntryId + ".cached"));
		if (existingCacheFile.exists()) {
			existingCacheFile.delete();
			
			//	deletion failed
			if (existingCacheFile.exists())
				return null;
		}
		
		//	create file & wrap writer around it
		try {
			final File cachingFile = new File(this.cacheFolder, (command + "." + cacheEntryId + ".caching"));
			
			//	delete existing file
			if (cachingFile.exists()) {
				cachingFile.delete();
				
				//	deletion failed
				if (cachingFile.exists())
					return null;
			}
			
			//	create cache file
			cachingFile.createNewFile();
			
			//	create readable cache file & timestamp
			final File cacheFile = new File(this.cacheFolder, (command + "." + cacheEntryId + ".cached"));
			final long cacheTimestamp = System.currentTimeMillis();
			
			//	create writer doing the work
			return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(cachingFile), "UTF-8")) {
				public void close() throws IOException {
					super.flush();
					super.close();
					cachingFile.setLastModified(cacheTimestamp);
					cachingFile.renameTo(cacheFile);
					System.out.println("SRS Cache Writer closed, cache file is " + cachingFile.getName());
				}
			};
		}
		catch (IOException ioe) {
			System.out.println("GoldenGateSrsClient: cache write preparation error " + ioe.getClass().getName() + " (" + ioe.getMessage() + ")");
			ioe.printStackTrace(System.out);
		}
		
		//	cache write error
		return null;
	}
	
	//	TODO use MD5 hash or document ID for cache entry name, not Java string hash (too high risk of collisions)
	Reader wrapDataReader(Reader dataReader, String command, int cacheEntryId) {
		Writer cacheWriter = this.getCacheWriter(command, cacheEntryId);
		
		//	cache disabled, or other error, return argument reader
		if (cacheWriter == null)
			return dataReader;
		
		//	cache enabled, return reader looping all data through to cache file
		else return new CacheWritingReader(dataReader, cacheWriter);
	}
	
	static class ConnectionGuardReader extends Reader {
		private Reader data;
		private Connection con;
		ConnectionGuardReader(Reader data, Connection con) {
			this.data = data;
			this.con = con;
		}
		public void close() throws IOException {
			if (this.data != null) {
				this.data.close();
				this.data = null;
			}
			if (this.con != null) {
				this.con.close();
				this.con = null;
				System.out.println("SRS connection closed explicitly");
			}
		}
		public int read(char[] cbuf, int off, int len) throws IOException {
			if (this.data == null)
				return -1;
			int read = this.data.read(cbuf, off, len);
			if (read == -1) try {
				this.data.close();
				this.data = null;
				this.con.close();
				this.con = null;
				System.out.println("SRS connection closed at end of input");
			}
			catch (Exception e) {
				System.out.println("Error closing SRS connection " + e.getMessage());
				e.printStackTrace(System.out);
			}
			return read;
		}
	}
	
	static class CacheWritingReader extends Reader {
		private Reader data;
		private Writer cache;
		CacheWritingReader(Reader data, Writer cache) {
			this.data = data;
			this.cache = cache;
		}
		public void close() throws IOException {
			if (this.data != null) {
				this.data.close();
				this.data = null;
			}
			if (this.cache != null) {
				this.cache.flush();
				this.cache.close();
				this.cache = null;
				System.out.println("SRS Cache Writer closed explicitly");
			}
		}
		public int read(char[] cbuf, int off, int len) throws IOException {
			if (this.data == null)
				return -1;
			int read = this.data.read(cbuf, off, len);
			if (read != -1)
				this.cache.write(cbuf, off, read);
			else try {
				this.data.close();
				this.data = null;
				this.cache.flush();
				this.cache.close();
				this.cache = null;
				System.out.println("SRS Cache Writer closed at end of input");
			}
			catch (Exception e) {
				System.out.println("Error closing SRS Cache Writer " + e.getMessage());
				e.printStackTrace(System.out);
			}
			return read;
		}
	}
	
	static class CacheAccessData {
		String command;
		int cacheKey;
		GoldenGateSrsClient cacheHolder;
		CacheAccessData(String command, int cacheKey, GoldenGateSrsClient cacheHolder) {
			this.command = command;
			this.cacheKey = cacheKey;
			this.cacheHolder = cacheHolder;
		}
		Reader getCacheReader() {
			return this.cacheHolder.getCacheReader(this.command, this.cacheKey);
		}
	}
	
	private ServerConnection serverConnection;
	
	/**
	 * Constructor
	 * @param serverConnection the ServerConnection to use for communication
	 *            with the backing SRS
	 */
	public GoldenGateSrsClient(ServerConnection serverConnection) {
		this.serverConnection = serverConnection;
	}
	
	/**
	 * Retrieve the search field groups for searching documents and other data
	 * in the backing GoldenGATE SRS.
	 * @return the search field groups for this SRS
	 * @throws IOException
	 */
	public SearchFieldGroup[] getSearchFieldGroups() throws IOException {
		return this.getSearchFieldGroups(true);
	}
	
	/**
	 * Retrieve the search field groups for searching documents and other data
	 * in the backing GoldenGATE SRS.
	 * @param allowCache allow returning cached result if available?
	 * @return the search field groups for this SRS
	 * @throws IOException
	 */
	public SearchFieldGroup[] getSearchFieldGroups(boolean allowCache) throws IOException {
		
		//	do cache lookup
		if (allowCache && (this.sfgCache != null))
			return this.sfgCache;
		
		//	cache miss, get search field groups from server
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_SEARCH_FIELDS);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_SEARCH_FIELDS.equals(error)) {
				this.sfgCache = SearchFieldGroup.readFieldGroups(br);
				this.sfgCacheTimestamp = System.currentTimeMillis();
				return this.sfgCache;
			}
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	private SearchFieldGroup[] sfgCache = null;
	private long sfgCacheTimestamp = 0;
	
	/**
	 * Obtain a document by its ID.
	 * @param docId the ID of the document to retrieve
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public MutableAnnotation getXmlDocument(String docId) throws IOException {
		return this.getXmlDocument(docId, true);
	}
	
	/**
	 * Obtain a document by its ID.
	 * @param docId the ID of the document to retrieve
	 * @param allowCache allow returning cached result if available?
	 * @return the document with the specified ID
	 * @throws IOException
	 */
	public MutableAnnotation getXmlDocument(String docId, boolean allowCache) throws IOException {
		
		if (allowCache) {
			Reader cacheReader = this.getCacheReader(GET_XML_DOCUMENT, docId.hashCode());
			if (cacheReader != null)
				return GenericGamtaXML.readDocument(cacheReader);
		}
		
		Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(GET_XML_DOCUMENT);
		bw.newLine();
		bw.write(docId);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (GET_XML_DOCUMENT.equals(error)) {
			final Reader dataReader = this.wrapDataReader(new ConnectionGuardReader(br, con), GET_XML_DOCUMENT, docId.hashCode());
			return GenericGamtaXML.readDocument(new Reader() {
				private boolean closed = false;
				public void close() throws IOException {
					dataReader.close();
					this.closed = true;
				}
				public int read(char[] cbuf, int off, int len) throws IOException {
					if (this.closed)
						return -1;
					int read = dataReader.read(cbuf, off, len);
					if (read == -1)
						this.close();
					return read;
				}
			});
		}
		else {
			con.close();
			throw new IOException(error);
		}
	}

	private String getParameterString(Properties parameters) throws IOException {
		StringBuffer parameterString = new StringBuffer();
		for (Iterator pit = parameters.keySet().iterator(); pit.hasNext();) {
			String parameterName = ((String) pit.next());
			String parameterValue = parameters.getProperty(parameterName, "");
			String[] parameterValues = parameterValue.split("[\\r\\n]++");
			for (int v = 0; v < parameterValues.length; v++) {
				parameterValue = parameterValues[v].trim();
				if (parameterValue.length() == 0)
					continue;
				if (parameterString.length() != 0)
					parameterString.append("&");
				parameterString.append(parameterName + "=" + URLEncoder.encode(parameterValues[v], "UTF-8"));
			}
		}
		return parameterString.toString();
	}
	
	/**
	 * Search for complete documents.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the documents and their
	 *            annotations?
	 * @return the result of the search
	 * @throws IOException
	 */
	public DocumentResult searchDocuments(Properties parameters, boolean markSearchables) throws IOException {
		return this.searchDocuments(parameters, markSearchables, true);
	}
	
	/**
	 * Search for complete documents.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the documents and their
	 *            annotations?
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search
	 * @throws IOException
	 */
	public DocumentResult searchDocuments(Properties parameters, boolean markSearchables, boolean allowCache) throws IOException {
		return this.searchDocuments(parameters, markSearchables, allowCache, null);
	}
	
	DocumentResult searchDocuments(Properties parameters, boolean markSearchables, boolean allowCache, CacheAccessData[] cad) throws IOException {
		if (parameters.isEmpty()) return new DocumentResult() {
			public boolean hasNextElement() {
				return false;
			}
			public SrsSearchResultElement getNextElement() {
				return null;
			}
		};
		
		if (markSearchables) { // wrap original parameters so they are not modified
			Properties allParameters = new Properties();
			allParameters.putAll(parameters);
			allParameters.setProperty(MARK_SEARCHABLES_PARAMETER, MARK_SEARCHABLES_PARAMETER);
			parameters = allParameters;
		}
		
		String parameterString = this.getParameterString(parameters);
		
		if (allowCache) {
			Reader cacheReader = this.getCacheReader(SEARCH_DOCUMENTS, parameterString.hashCode());
			if (cacheReader != null)
				return DocumentResult.readDocumentResult(cacheReader);
		}
		
		Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(SEARCH_DOCUMENTS);
		bw.newLine();
		bw.write(parameterString);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (SEARCH_DOCUMENTS.equals(error)) {
			Reader resultReader = this.wrapDataReader(new ConnectionGuardReader(br, con), SEARCH_DOCUMENTS, parameterString.hashCode());
			if ((cad != null) && (resultReader instanceof CacheWritingReader))
				cad[0] = new CacheAccessData(SEARCH_DOCUMENTS, parameterString.hashCode(), this);
			return DocumentResult.readDocumentResult(resultReader);
		}
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Search for document details that are especially annotated. You can
	 * specify the names of the indices whose details you are interested in the
	 * INDEX_NAME parameter as a comma separated list, not specifying an index
	 * name will result in all details being returned.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the documents and their
	 *            annotations?
	 * @return the result of the search
	 * @throws IOException
	 */
	public DocumentResult searchDocumentDetails(Properties parameters, boolean markSearchables) throws IOException {
		return this.searchDocumentDetails(parameters, markSearchables, true);
	}
	
	/**
	 * Search for document details that are especially annotated. You can
	 * specify the names of the indices whose details you are interested in the
	 * INDEX_NAME parameter as a comma separated list, not specifying an index
	 * name will result in all details being returned.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the documents and their
	 *            annotations?
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search
	 * @throws IOException
	 */
	public DocumentResult searchDocumentDetails(Properties parameters, boolean markSearchables, boolean allowCache) throws IOException {
		return this.searchDocumentDetails(parameters, markSearchables, allowCache, null);
	}
	
	DocumentResult searchDocumentDetails(Properties parameters, boolean markSearchables, boolean allowCache, CacheAccessData[] cad) throws IOException {
		if (parameters.isEmpty()) return new DocumentResult() {
			public boolean hasNextElement() {
				return false;
			}
			public SrsSearchResultElement getNextElement() {
				return null;
			}
		};
		
		if (markSearchables) { // wrap original parameters so they are not modified
			Properties allParameters = new Properties();
			allParameters.putAll(parameters);
			allParameters.setProperty(MARK_SEARCHABLES_PARAMETER, MARK_SEARCHABLES_PARAMETER);
			parameters = allParameters;
		}
		
		String parameterString = this.getParameterString(parameters);
		
		if (allowCache) {
			Reader cacheReader = this.getCacheReader(SEARCH_DOCUMENT_DETAILS, parameterString.hashCode());
			if (cacheReader != null)
				return DocumentResult.readDocumentResult(cacheReader);
		}
		
		Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(SEARCH_DOCUMENT_DETAILS);
		bw.newLine();
		bw.write(parameterString);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (SEARCH_DOCUMENT_DETAILS.equals(error)) {
			Reader resultReader = this.wrapDataReader(new ConnectionGuardReader(br, con), SEARCH_DOCUMENT_DETAILS, parameterString.hashCode());
			if ((cad != null) && (resultReader instanceof CacheWritingReader))
				cad[0] = new CacheAccessData(SEARCH_DOCUMENT_DETAILS, parameterString.hashCode(), this);
			return DocumentResult.readDocumentResult(resultReader);
		}
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Search for basic document meta data, like title and author.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to eventual sub result
	 *            annotations?
	 * @return the result of the search (the document of the
	 *         DocumentResultElement objects will be null, only attributes are
	 *         used in this search mode)
	 * @throws IOException
	 */
	public DocumentResult searchDocumentData(Properties parameters, boolean markSearchables) throws IOException {
		return this.searchDocumentData(parameters, markSearchables, true);
	}
	
	/**
	 * Search for basic document meta data, like title and author.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to eventual sub result
	 *            annotations?
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search (the document of the
	 *         DocumentResultElement objects will be null, only attributes are
	 *         used in this search mode)
	 * @throws IOException
	 */
	public DocumentResult searchDocumentData(Properties parameters, boolean markSearchables, boolean allowCache) throws IOException {
		return this.searchDocumentData(parameters, markSearchables, allowCache, null);
	}
	
	DocumentResult searchDocumentData(Properties parameters, boolean markSearchables, boolean allowCache, CacheAccessData[] cad) throws IOException {
		if (parameters.isEmpty()) return new DocumentResult() {
			public boolean hasNextElement() {
				return false;
			}
			public SrsSearchResultElement getNextElement() {
				return null;
			}
		};
		
		if (markSearchables) { // wrap original parameters so they are not modified
			Properties allParameters = new Properties();
			allParameters.putAll(parameters);
			allParameters.setProperty(MARK_SEARCHABLES_PARAMETER, MARK_SEARCHABLES_PARAMETER);
			parameters = allParameters;
		}
		
		String parameterString = this.getParameterString(parameters);
		
		if (allowCache) {
			Reader cacheReader = this.getCacheReader(SEARCH_DOCUMENT_DATA, parameterString.hashCode());
			if (cacheReader != null)
				return DocumentResult.readDocumentDataResult(cacheReader);
		}
		
		Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(SEARCH_DOCUMENT_DATA);
		bw.newLine();
		bw.write(parameterString);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (SEARCH_DOCUMENT_DATA.equals(error)) {
			Reader resultReader = this.wrapDataReader(new ConnectionGuardReader(br, con), SEARCH_DOCUMENT_DATA, parameterString.hashCode());
			if ((cad != null) && (resultReader instanceof CacheWritingReader))
				cad[0] = new CacheAccessData(SEARCH_DOCUMENT_DATA, parameterString.hashCode(), this);
			return DocumentResult.readDocumentDataResult(resultReader);
		}
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Search for plain document IDs. The elements of the returned document
	 * result will be plain pairs of document ID and relevance. This type of
	 * query is useful in situations where an application intends to retrieve
	 * the complete documents after the search, e.g. via the getXmlDocument()
	 * method, use a local cache, etc.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @return the result of the search (the document of the
	 *         DocumentResultElement objects will be null, only document ID and
	 *         relevance are used in this search mode)
	 * @throws IOException
	 */
	public DocumentResult searchDocumentIDs(Properties parameters) throws IOException {
		return this.searchDocumentIDs(parameters, true);
	}
	
	/**
	 * Search for plain document IDs. The elements of the returned document
	 * result will be plain pairs of document ID and relevance. This type of
	 * query is useful in situations where an application intends to retrieve
	 * the complete documents after the search, e.g. via the getXmlDocument()
	 * method, use a local cache, etc.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search (the document of the
	 *         DocumentResultElement objects will be null, only document ID and
	 *         relevance are used in this search mode)
	 * @throws IOException
	 */
	public DocumentResult searchDocumentIDs(Properties parameters, boolean allowCache) throws IOException {
		return this.searchDocumentIDs(parameters, allowCache, null);
	}
	
	DocumentResult searchDocumentIDs(Properties parameters, boolean allowCache, CacheAccessData[] cad) throws IOException {
		if (parameters.isEmpty()) return new DocumentResult() {
			public boolean hasNextElement() {
				return false;
			}
			public SrsSearchResultElement getNextElement() {
				return null;
			}
		};
		
		String parameterString = this.getParameterString(parameters);
		
		if (allowCache) {
			Reader cacheReader = this.getCacheReader(SEARCH_DOCUMENT_IDS, parameterString.hashCode());
			if (cacheReader != null)
				return DocumentResult.readDocumentDataResult(cacheReader);
		}
		
		Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(SEARCH_DOCUMENT_IDS);
		bw.newLine();
		bw.write(parameterString);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (SEARCH_DOCUMENT_IDS.equals(error)) {
			Reader resultReader = this.wrapDataReader(new ConnectionGuardReader(br, con), SEARCH_DOCUMENT_IDS, parameterString.hashCode());
			if ((cad != null) && (resultReader instanceof CacheWritingReader))
				cad[0] = new CacheAccessData(SEARCH_DOCUMENT_IDS, parameterString.hashCode(), this);
			return DocumentResult.readDocumentDataResult(resultReader);
		}
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Retrieve the basic meta data on all documents modified since a given
	 * point in time. The actual point in time can be specified in two way,
	 * absolute or relative. A positive value (including 0) for the timestamp
	 * argument is interpreted as an absolute time, while a negative value is
	 * interpreted as relatively, namely as the number of milliseconds backward
	 * from the current time. Due to the time constraint, this method does not
	 * use the cache, even if it's enabled.
	 * @param timestamp the limit for the modification timestamp (relative or
	 *            absolute, see above)
	 * @return a DocumentResult containing the basic meta data of the documents
	 *         modified since the specified timestamp
	 * @throws IOException
	 */
	public DocumentResult getModifiedSince(long timestamp) throws IOException {
		Properties parameters = new Properties();
		parameters.setProperty(LAST_MODIFIED_SINCE, ("" + timestamp));
		return this.searchDocumentData(parameters, false);
	}
	
	/**
	 * Obtain the list of the documents in the SRS collection. This method does
	 * not offer using a cache since the document list is mainly intended for
	 * administrative purposes, not for searching.
	 * @param masterDocID the ID of the parent document to list the retrievable
	 *            parts of (specifying null will return the list of master
	 *            documents)
	 * @return the list of the documents in the SRS' collection
	 */
	public DocumentList getDocumentList(String masterDocID) throws IOException {
		Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(LIST_DOCUMENTS);
		bw.newLine();
		bw.write((masterDocID == null) ? "" : masterDocID);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (LIST_DOCUMENTS.equals(error))
			return DocumentList.readDocumentList(new ConnectionGuardReader(br, con));
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Search entries of an index table belonging to documents matching a query.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the annotations?
	 * @return the index entries of the index specified with the indexId
	 *         parameter, which belong to documents matching the rest of the
	 *         query
	 * @throws IOException
	 */
	public IndexResult searchIndex(Properties parameters, boolean markSearchables) throws IOException {
		return this.searchIndex(parameters, markSearchables, true);
	}
	
	/**
	 * Search entries of an index table belonging to documents matching a query.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the annotations?
	 * @param allowCache allow returning cached result if available?
	 * @return the index entries of the index specified with the indexId
	 *         parameter, which belong to documents matching the rest of the
	 *         query
	 * @throws IOException
	 */
	public IndexResult searchIndex(Properties parameters, boolean markSearchables, boolean allowCache) throws IOException {
		return this.searchIndex(parameters, markSearchables, allowCache, null);
	}
	
	IndexResult searchIndex(Properties parameters, boolean markSearchables, boolean allowCache, CacheAccessData[] cad) throws IOException {
		if (parameters.isEmpty()) throw new IOException("No such index");
		
		if (markSearchables) { // wrap original parameters so they are not modified
			Properties allParameters = new Properties();
			allParameters.putAll(parameters);
			allParameters.setProperty(MARK_SEARCHABLES_PARAMETER, MARK_SEARCHABLES_PARAMETER);
			parameters = allParameters;
		}
		
		String parameterString = this.getParameterString(parameters);
		
		if (allowCache) {
			Reader cacheReader = this.getCacheReader(SEARCH_INDEX, parameterString.hashCode());
			if (cacheReader != null)
				return IndexResult.readIndexResult(cacheReader);
		}
		
		Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(SEARCH_INDEX);
		bw.newLine();
		bw.write(parameterString);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (SEARCH_INDEX.equals(error)) {
			Reader resultReader = this.wrapDataReader(new ConnectionGuardReader(br, con), SEARCH_INDEX, parameterString.hashCode());
			if ((cad != null) && (resultReader instanceof CacheWritingReader))
				cad[0] = new CacheAccessData(SEARCH_INDEX, parameterString.hashCode(), this);
			return IndexResult.readIndexResult(resultReader);
		}
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Search an index table in thesaurus style.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @return the result of the search
	 * @throws IOException
	 */
	public ThesaurusResult searchThesaurus(Properties parameters) throws IOException {
		return this.searchThesaurus(parameters, true);
	}
	
	/**
	 * Search an index table in thesaurus style.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search
	 * @throws IOException
	 */
	public ThesaurusResult searchThesaurus(Properties parameters, boolean allowCache) throws IOException {
		return this.searchThesaurus(parameters, allowCache, null);
	}
	
	ThesaurusResult searchThesaurus(Properties parameters, boolean allowCache, CacheAccessData[] cad) throws IOException {
		if (parameters.isEmpty()) throw new IOException("No such index");
		
		String parameterString = this.getParameterString(parameters);
		
		if (allowCache) {
			Reader cacheReader = this.getCacheReader(SEARCH_THESAURUS, parameterString.hashCode());
			if (cacheReader != null)
				return ThesaurusResult.readThesaurusResult(cacheReader);
		}
		
		Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(SEARCH_THESAURUS);
		bw.newLine();
		bw.write(parameterString);
		bw.newLine();
		bw.flush();
		
		BufferedReader br = con.getReader();
		String error = br.readLine();
		if (SEARCH_THESAURUS.equals(error)) {
			Reader resultReader = this.wrapDataReader(new ConnectionGuardReader(br, con), SEARCH_THESAURUS, parameterString.hashCode());
			if ((cad != null) && (resultReader instanceof CacheWritingReader))
				cad[0] = new CacheAccessData(SEARCH_THESAURUS, parameterString.hashCode(), this);
			return ThesaurusResult.readThesaurusResult(resultReader);
		}
		
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	/**
	 * Obtain the collection statistics from the SRS.
	 * @return the statistics of the document collection in the underlying SRS,
	 *         including top contributors
	 * @throws IOException
	 */
	public CollectionStatistics getStatistics() throws IOException {
		return this.getStatistics(-1, null, true);
	}
	
	/**
	 * Obtain the collection statistics from the SRS.
	 * @param since the relative time constant since which to get the statistics
	 * @return the statistics of the document collection in the underlying SRS,
	 *         including top contributors
	 * @throws IOException
	 */
	public CollectionStatistics getStatistics(String since) throws IOException {
		return this.getStatistics(-1, since, true);
	}
	
	/**
	 * Obtain the collection statistics from the SRS.
	 * @param since the time since which to get the statistics (UTC milliseconds)
	 * @return the statistics of the document collection in the underlying SRS,
	 *         including top contributors
	 * @throws IOException
	 */
	public CollectionStatistics getStatistics(long since) throws IOException {
		return this.getStatistics(since, null, true);
	}
	
	/**
	 * Obtain the collection statistics from the SRS.
	 * @param allowCache allow returning cached result if available?
	 * @return the statistics of the document collection in the underlying SRS,
	 *         including top contributors
	 * @throws IOException
	 */
	public CollectionStatistics getStatistics(boolean allowCache) throws IOException {
		return this.getStatistics(-1, null, allowCache);
	}
	
	/**
	 * Obtain the collection statistics from the SRS.
	 * @param since the relative time constant since which to get the statistics
	 * @param allowCache allow returning cached result if available?
	 * @return the statistics of the document collection in the underlying SRS,
	 *         including top contributors
	 * @throws IOException
	 */
	public CollectionStatistics getStatistics(String since, boolean allowCache) throws IOException {
		return this.getStatistics(-1, since, allowCache);
	}
	
	/**
	 * Obtain the collection statistics from the SRS.
	 * @param since the time since which to get the statistics (UTC milliseconds)
	 * @param allowCache allow returning cached result if available?
	 * @return the statistics of the document collection in the underlying SRS,
	 *         including top contributors
	 * @throws IOException
	 */
	public CollectionStatistics getStatistics(long since, boolean allowCache) throws IOException {
		return this.getStatistics(since, null, allowCache);
	}
	
	private CollectionStatistics getStatistics(long since, String sinceString, boolean allowCache) throws IOException {
		
		//	adjust parameters
		if (sinceString == null)
			sinceString = ("" + since);
		else if (since < 0) {
			if (GET_STATISTICS_LAST_YEAR.equals(sinceString))
				since = (System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 365));
			else if (GET_STATISTICS_LAST_HALF_YEAR.equals(sinceString))
				since = (System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 183));
			else if (GET_STATISTICS_LAST_THREE_MONTHS.equals(sinceString))
				since = (System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 91));
			else if (GET_STATISTICS_LAST_MONTH.equals(sinceString))
				since = (System.currentTimeMillis() - (1000 * 60 * 60 * 24 * 30));
		}
		
		//	do cache lookup
		if (allowCache) {
			final Long csCacheKey = new Long(since / (1000 * 60 * 10));
			synchronized (this.csCache) {
				CsCacheEntry cce = ((CsCacheEntry) this.csCache.get(csCacheKey));
				if (cce != null)
					return CollectionStatistics.readCollectionStatistics(new StringReader(cce.data));
			}
		}
		
		//	cache miss, do server lookup
		final Connection con = this.serverConnection.getConnection();
		BufferedWriter bw = con.getWriter();
		
		bw.write(GET_STATISTICS);
		bw.newLine();
		bw.write(sinceString);
		bw.newLine();
		bw.flush();
		
		final BufferedReader br = con.getReader();
		String error = br.readLine();
		if (GET_STATISTICS.equals(error)) {
			Reader csReader;
			
			//	cache disabled, read directly
			if (this.cacheFolder == null)
				csReader = new ConnectionGuardReader(br, con);
			
			//	cache enabled, stream data to cache simultaneously
			else {
				final long csTimestamp = System.currentTimeMillis();
				final Long csCacheKey = new Long(since / (1000 * 60 * 10));
				final StringWriter csWriter = new StringWriter() {
					public void close() throws IOException {
						super.close();
						CsCacheEntry cce = new CsCacheEntry(this.toString(), csTimestamp);
						synchronized (csCache) {
							csCache.put(csCacheKey, cce);
						}
					}
				};
				csReader = new CacheWritingReader(new ConnectionGuardReader(br, con), csWriter);
			}
			return CollectionStatistics.readCollectionStatistics(csReader);
		}
		else {
			con.close();
			throw new IOException(error);
		}
	}
	
	private LinkedHashMap csCache = new LinkedHashMap(16, 0.9f, true) {
		protected boolean removeEldestEntry(Entry eldest) {
			return this.size() > 64;
		}
	};
	private static class CsCacheEntry {
		final String data;
		final long retrieved;
		CsCacheEntry(String data, long retrieved) {
			this.data = data;
			this.retrieved = retrieved;
		}
	}
}