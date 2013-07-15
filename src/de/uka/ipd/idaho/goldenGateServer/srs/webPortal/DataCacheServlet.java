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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet.ReInitializableServlet;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResultElement;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.XsltUtils;

/**
 * This servlet provides raw XML documents from the backing GoldenGATE SRS,
 * transformed through custom XSLT stylesheets. This allows other parties to
 * retrieve the documents in their favored XML schema. As opposed to te
 * XsltServlet, this servlet is intended for serving large numbers of documents
 * for a single request (up to complete dumps of the entire collection hosted by
 * the backing GoldenGATE SRS), instead of individual documents. Therefore, it
 * makes massive use of caching, allowing for some time gap between updates to
 * the backing SRS collection and those updates being reflected in the output of
 * the servlet. This servlet also provides zip-compressed dumps of the caches,
 * which it re-creates periodically to reflect updates to the backing
 * collection.<br>
 * For retrieving (parts of) the document collection in a specific schema, use
 * the configured name of the schema (see below) for invoking the servlet. For
 * filtering the collection, append the actual query to the servlet invokation
 * path. <br>
 * Despite this intended use, this servlet also alows for retrieving individual
 * documents based on their ID. The ID of the document to retrieve and transform
 * can be specified as the value of the 'idQuery' request parameter. The XSLT
 * stylesheet to use for transformation has to be specified by appending its
 * configured name (see below) to the servlet path invoking this servlet.<br>
 * Individual XSLT stylesheets have to be installed in this servlet to
 * facilitate calling them by an intuitive name instead of the URL. This is
 * achieved by entering a setting 'XSLT.&lt;cacheName&gt;' in the servlets
 * config file, the value of the setting being the URL of the XSLT stylesheet to
 * be invoked throug &lt;cacheName&gt;. If that URL starts with 'http://', it's
 * assumed to be absolute. In contrast, if the URL does not start with
 * 'http://', the URL is interpreted as a local path, relative to the web-apps
 * context path.<br>
 * As a result, getting documents from this servlet transformed through an XSLT
 * stylesheet with a given configured name (in the following referred to as
 * '&lt;cacheName&gt;', the configured name 'xml' always refers to the
 * untransformed document) works as follows ('&lt;contextUrl&gt;' always
 * corresponds to the URL address of the servlet's web-app context,
 * <code>&lt;contextUrl&gt;/&lt;servletPath&gt;/*</code> has to be mapped to
 * the servlet in the web.xml for this way of invocation to work):
 * <ul>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;cacheName&gt;?lastModifiedSince=&lt;updateTimestamp&gt;</code>
 * retrieves the documents modified since '&lt;updateTimestamp&gt;' from cache
 * '&lt;cacheName&gt;' as a stream of XML documents</li>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;cacheName&gt;/&lt;documntId&gt;</code>
 * retrieves the document with ID '&lt;documntId&gt;' from the cache
 * '&lt;cacheName&gt;'</li>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;cacheName&gt;.zip</code>
 * retrieves a complete content of cache '&lt;cacheName&gt;' as a single
 * zip-compressed file</li>
 * </ul>
 * The servlet also understands two special-purpose requests to each of the
 * hosted caches:
 * <ul>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;cacheName&gt;/refresh</code>
 * forces cache '&lt;cacheName&gt;' to be refreshed to the current status of the
 * master cache; if the specified cache is the master cache, it will be updated
 * to the state of the backing collection (might cause considerable
 * computational effort)</li>
 * <li><code>&lt;contextUrl&gt;/&lt;servletPath&gt;/&lt;cacheName&gt;/rezip</code>
 * forces the zip-compressed dump cache '&lt;cacheName&gt;' to be refreshed to
 * the current status of the that cache (might cause considerable computational
 * effort)</li>
 * </ul>
 * In addition to the super class' parameters, this servlet has two (optional)
 * additional parameters:
 * <ul>
 * <li><b>cacheFolder</b>: the folder to use as the root for the cache (has to
 * be an absolute path name; if not specified, the servlet will use the
 * 'dataCache' sub folder of the surrounding web-app's context path as its cache
 * root)</li>
 * <li><b>cacheUpdateInterval</b>: the timespan between two SRS lookups for
 * updated documents (in seconds; defaults to 600 seconds (= 10 minutes) if not
 * specified; minimum is 60 seconds (= 1 minute))</li>
 * </ul>
 * 
 * @author sautter
 */
public class DataCacheServlet extends AbstractSrsWebPortalServlet implements ReInitializableServlet, SearchPortalConstants {
	
	private static final String MASTER_CACHE_UPDATE_ID = "masterCacheUpdate";
	
	private File cacheRoot;
	
	private Cache masterCache;
	private HashMap cachesByName = new HashMap();
	
	private class Cache {
		String name;
		
		File cacheFolder;
		
		String xsltUrl = null;
		Transformer xsltTransformer = null;
		//	TODO: allow for multiple transformers to be chained together
		
		long lastUpdateTimestamp;
		long lastZipUpdateTimestamp;
		
		boolean zipping = false;
		boolean regularZipping = false;
		
		Cache(String name, File cacheFolder, String xsltUrl) throws IOException {
			this.name = name;
			this.cacheFolder = cacheFolder;
			this.xsltUrl = xsltUrl;
			this.xsltTransformer = getTransformer(this.xsltUrl);
			this.lastUpdateTimestamp = computeCacheTimestamp(this.cacheFolder);
			this.lastZipUpdateTimestamp = (new File(this.cacheFolder.getParentFile(), (this.name + ".zip"))).lastModified();
		}
	}
	
	private static final long MINIMUM_CACHE_UPDATE_INTERVAL = (1000 * 60 * 1);
	private static final long DEFAULT_CACHE_UPDATE_INTERVAL = (1000 * 60 * 10);
	private long cacheUpdateInterval = DEFAULT_CACHE_UPDATE_INTERVAL;
	private long lastSrsLookupTimestamp = 0;
	
	private UpdateFetcherThread updateFetcher;
	private CacheUpdaterThread cacheUpdater;
	
	private LinkedList updateQueue = new LinkedList();
	
	private static Set pendingUpdateIDs = new HashSet();
	private static abstract class Update {
		final String id;
		Update(String updateId) {
			this.id = updateId;
			pendingUpdateIDs.add(this.id);
		}
		void update() {
			if (pendingUpdateIDs.contains(this.id)) {
				this.doUpdate();
				pendingUpdateIDs.remove(this.id);
			}
		}
		abstract void doUpdate();
		public boolean equals(Object obj) {
			return (this.hashCode() == obj.hashCode());
		}
		public int hashCode() {
			return this.id.hashCode();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.AbstractSrsWebPortalServlet#init(de.uka.ipd.idaho.easyIO.settings.Settings)
	 */
	protected void init(Settings config) {
		super.init(config);
		
		//	initialize cache root
		String cacheRoot = config.getSetting("cacheFolder");
		this.cacheRoot = ((cacheRoot == null) ? new File(new File(this.rootFolder, "caches"), "srsDataCache") : new File(cacheRoot));
		if (!this.cacheRoot.exists())
			this.cacheRoot.mkdirs();
		
		//	initialize master cache
		try {
			this.masterCache = new Cache("xml", new File(this.cacheRoot, "xml"), null);
		}
		catch (IOException ioe) {
			System.out.println("Exception loading master cache: " + ioe.getMessage());
			ioe.printStackTrace(System.out);
		}
		this.lastSrsLookupTimestamp = this.masterCache.lastUpdateTimestamp;
		
		//	load custom caches
		this.reInit(config);
		
		//	start update polling thread
		this.updateFetcher = new UpdateFetcherThread();
		this.updateFetcher.start();
		this.cacheUpdater = new CacheUpdaterThread();
		this.cacheUpdater.start();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.client.GgServerClientServlet.ReInitializableServlet#reInit(de.uka.ipd.idaho.easyIO.settings.Settings)
	 */
	public void reInit(Settings config) {
		
		//	clear registers
		this.cachesByName.clear();
//		this.transformerCache.clear();
		
		//	load custom cache definitions
		Settings cacheSet = config.getSubset("CACHE");
		String[] cacheNames = cacheSet.getKeys();
		for (int c = 0; c < cacheNames.length; c++) {
			String cacheXsltUrl = cacheSet.getSetting(cacheNames[c]);
			if (cacheXsltUrl != null) try {
				this.cachesByName.put(cacheNames[c], new Cache(cacheNames[c], new File(this.cacheRoot, cacheNames[c]), cacheXsltUrl));
			}
			catch (IOException ioe) {
				System.out.println("Exception loading cache " + cacheNames[c] + ": " + ioe.getMessage());
				ioe.printStackTrace(System.out);
			}
		}
		
		//	get cache update interval
		try {
			this.cacheUpdateInterval = (1000 * Long.parseLong(config.getSetting("cacheUpdateInterval", "-1")));
			if (this.cacheUpdateInterval < 0)
				this.cacheUpdateInterval = DEFAULT_CACHE_UPDATE_INTERVAL;
		} catch (NumberFormatException nfe) {}
		if (this.cacheUpdateInterval < MINIMUM_CACHE_UPDATE_INTERVAL)
			this.cacheUpdateInterval = MINIMUM_CACHE_UPDATE_INTERVAL;
	}
	
	private Transformer getTransformer(String xsltUrl) throws IOException {
		if (xsltUrl == null)
			return null;
		try {
			Transformer transformer = (xsltUrl.startsWith("http://") ? XsltUtils.getTransformer(new URL(xsltUrl), false) : XsltUtils.getTransformer(new File(this.rootFolder, xsltUrl), false));
			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			return transformer;
		}
		
		catch (Exception e) {
			throw new IOException(e.getClass().getName() + " (" + e.getMessage() + ") while creating XSL Transformer from '" + xsltUrl + "'.");
		}
	}
//	private HashMap transformerCache = new HashMap();
//	private TransformerFactory transformerFactory = null;
//	
//	private Transformer getTransformer(String xsltUrl) throws IOException {
//		if (xsltUrl == null) return null;
//		
//		if (this.transformerCache.containsKey(xsltUrl))
//			return ((Transformer) this.transformerCache.get(xsltUrl));
//		
//		if (this.transformerFactory == null)
//			this.transformerFactory = TransformerFactory.newInstance();
//		
//		try {
//			InputStream tis;
//			if (xsltUrl.startsWith("http://"))
//				tis = new URL(xsltUrl).openStream();
//			else tis = new FileInputStream(new File(this.rootFolder, xsltUrl));
//			Transformer transformer = this.transformerFactory.newTransformer(new StreamSource(new InputStreamReader(new ByteOrderMarkFilterInputStream(tis), "UTF-8")));
//			transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
//			this.transformerCache.put(xsltUrl, transformer);
//			tis.close();
//			return transformer;
//		}
//		
//		catch (Exception e) {
//			throw new IOException(e.getClass().getName() + " (" + e.getMessage() + ") while creating XSL Transformer from '" + xsltUrl + "'.");
//		}
//	}
	
	private long computeCacheTimestamp(File cacheRoot) {
		long cacheUpdateTimestamp = -1;
		File[] cacheFiles = cacheRoot.listFiles();
		for (int f = 0; f < cacheFiles.length; f++)
			cacheUpdateTimestamp = Math.min(cacheFiles[f].lastModified(), cacheUpdateTimestamp);
		return cacheUpdateTimestamp;
	}
	
	private class UpdateFetcherThread extends Thread {
		private boolean getUpdates = true;
		private final Object lock = new Object();
		public void run() {
			
			//	give the rest a little time
			try {
				this.lock.wait(1000 * 55);
			} catch (InterruptedException ie) {}
			
			//	create registers
			final Cache[] caches = ((Cache[]) cachesByName.values().toArray(new Cache[cachesByName.size()]));
			final long[] cacheZipUpdateTimes = new long[caches.length];
			
			//	update custom caches to state of master cache before starting to pull updates from SRS
			updateCaches(caches, false);
			
			//	create schedule for zip updates
			long time = System.currentTimeMillis();
			for (int c = 0; c < caches.length; c++)
				cacheZipUpdateTimes[c] = (time + ((long) (Math.random() * (24 * 60 * 60 * 1000))));
			
			//	run in infinity
			while (this.getUpdates) {
				
				//	update master cache
				updateMasterCache(false);
				
				//	priodically schedule creating zip files
				time = System.currentTimeMillis();
				for (int c = 0; c < caches.length; c++)
					if (cacheZipUpdateTimes[c] < time) {
						final Cache cache = caches[c];
						Thread zipUpdater = new Thread() {
							public void run() {
								try {
									createCacheZipFile(cache, false);
								}
								catch (IOException ioe) {
									System.out.println(ioe.getClass().getName() + " while updating zip-compressed dump of cache '" + cache.name + "': " + ioe.getMessage());
									ioe.printStackTrace(System.out);
								}
							}
						};
						zipUpdater.start();
						cacheZipUpdateTimes[c] += (24 * 60 * 60 * 1000);
					}
				
				//	wait until it's time for the next lookup
				synchronized(this.lock) {
					try {
						this.lock.wait(cacheUpdateInterval);
					} catch (InterruptedException ie) {}
				}
			}
		}
		void shutdown() {
			synchronized(this.lock) {
				this.getUpdates = false;
				this.lock.notify();
			}
		}
	}
	
	void updateMasterCache(boolean force) {
		try {
			
			//	get document list and put it in array
			DocumentResult dr = this.srsClient.getModifiedSince(lastSrsLookupTimestamp + 1);
			ArrayList dreList = new ArrayList();
			while (dr.hasNextElement())
				dreList.add(dr.getNextElement());
			final DocumentResultElement[] dres = ((DocumentResultElement[]) dreList.toArray(new DocumentResultElement[dreList.size()]));
			
			//	make sure latest (oldest) update executed first
			Arrays.sort(dres, new Comparator() {
				public int compare(Object o1, Object o2) {
					return (new Long((String) ((DocumentResultElement) o1).getAttribute(UPDATE_TIME_ATTRIBUTE))).compareTo(new Long((String) ((DocumentResultElement) o2).getAttribute(UPDATE_TIME_ATTRIBUTE)));
				}
			});
			
			if (force) {
				String docId = null;
				try {
					
					//	do the updates
					for (int d = 0; d < dres.length; d++) {
						docId = dres[d].documentId;
						updateMasterCacheDocument(docId, Long.parseLong((String) dres[d].getAttribute(UPDATE_TIME_ATTRIBUTE)));
					}
					
					//	remember last lookup
					if (dres.length != 0)
						lastSrsLookupTimestamp = Long.parseLong((String) dres[dres.length - 1].getAttribute(UPDATE_TIME_ATTRIBUTE));
				}
				
				/*
				 * catch all the exceptions together, so if an
				 * update throws an exception, the later updates
				 * are no executed
				 */
				catch (Exception e) {
					System.out.println(e.getClass().getName() + " while updating document '" + docId + "': " + e.getMessage());
					e.printStackTrace(System.out);
					
					/*
					 * reset lookup timestamp so failed update
					 * (and all the later ones) are retried;
					 * subtract 1 in order to catch cases of two
					 * documents having the same update
					 * timestamp
					 */
					this.lastSrsLookupTimestamp = this.masterCache.lastUpdateTimestamp - 1;
				}
			}
			
			else {
				this.enqueueCacheUpdate(this.masterCache, new Update(MASTER_CACHE_UPDATE_ID) {
					public void doUpdate() {
						String docId = null;
						try {
							
							//	do the updates
							for (int d = 0; d < dres.length; d++) {
								docId = dres[d].documentId;
								updateMasterCacheDocument(docId, Long.parseLong((String) dres[d].getAttribute(UPDATE_TIME_ATTRIBUTE)));
							}
						}
						
						/*
						 * catch all the exceptions together, so if an
						 * update throws an exception, the later updates
						 * are not executed
						 */
						catch (Exception e) {
							System.out.println(e.getClass().getName() + " while updating document '" + docId + "': " + e.getMessage());
							e.printStackTrace(System.out);
							
							/*
							 * reset lookup timestamp so failed update
							 * (and all the later ones) are retried;
							 * subtract 1 in order to catch cases of two
							 * documents having the same update
							 * timestamp
							 */
							lastSrsLookupTimestamp = masterCache.lastUpdateTimestamp - 1;
						}
					}
				});
				
				/*
				 * remembering last lookup is OK before actual files are
				 * written, since if the latter fails, the timestamp is
				 * reset, and on a restart after a system shutdown, the last
				 * lookup will be set to the timestamp of the last updated
				 * document
				 */
				if (dres.length != 0)
					lastSrsLookupTimestamp = Long.parseLong((String) dres[dres.length - 1].getAttribute(UPDATE_TIME_ATTRIBUTE));
			}
		}
		catch (Exception e) {
			System.out.println(e.getClass().getName() + " while getting updates from SRS: " + e.getMessage());
			e.printStackTrace(System.out);
		}
	}
	
	void updateMasterCacheDocument(final String docId, final long documentTimestamp) throws Exception {
		
		//	get writer
		BufferedWriter out = this.getCacheFileWriter(this.masterCache.cacheFolder, docId, documentTimestamp);
		
		//	document up to date, or update in progress
		if (out == null) return;
		
		//	get document
		MutableAnnotation doc = this.srsClient.getXmlDocument(docId, false);
		
		//	create cache file
//		AnnotationUtils.writeXML(doc, out, null, null, true);
		AnnotationUtils.writeXML(doc, out);
		this.masterCache.lastUpdateTimestamp = documentTimestamp;
		
		//	distribute master cache updates
		Cache[] caches = ((Cache[]) cachesByName.values().toArray(new Cache[cachesByName.size()]));
		for (int c = 0; c < caches.length; c++) {
			final Cache cache = caches[c];
			this.enqueueCacheUpdate(cache, new Update(cache.name + "." + docId) {
				public void doUpdate() {
					try {
						updateCacheDocument(cache, docId, documentTimestamp);
					}
					catch (Exception e) {
						System.out.println(e.getClass().getName() + " while updating document '" + docId + "' in cache '" + cache.name + "': " + e.getMessage());
						e.printStackTrace(System.out);
					}
				}
			});
		}
	}
	
	void updateCache(Cache cache, boolean force) {
		Cache[] caches = {cache};
		this.updateCaches(caches, force);
	}
	
	void updateCaches(final Cache[] caches, boolean force) {
		
		//	initialize registers
		final Map updateDocIDsAndTimestamps = new HashMap();
		final Set[] cacheUpdateDocIDs = new Set[caches.length];
		for (int c = 0; c < caches.length; c++)
			cacheUpdateDocIDs[c] = new HashSet();
		
		//	compute delta to master cache
		File[] masterFirstLevel = this.masterCache.cacheFolder.listFiles(new FileFilter() {
			public boolean accept(File file) {
//				return (file.isDirectory() && (file.lastModified() > cache.lastUpdateTimestamp));
				return file.isDirectory();
			}
		});
		for (int fl = 0; fl < masterFirstLevel.length; fl++) {
			String flName = masterFirstLevel[fl].getName();
			File[] masterSecondLevel = masterFirstLevel[fl].listFiles(new FileFilter() {
				public boolean accept(File file) {
//					return (file.isDirectory() && (file.lastModified() > cache.lastUpdateTimestamp));
					return file.isDirectory();
				}
			});
			for (int sl = 0; sl < masterSecondLevel.length; sl++) {
				String slName = masterSecondLevel[sl].getName();
				File[] masterCacheFiles = masterSecondLevel[sl].listFiles(new FileFilter() {
					public boolean accept(File file) {
//						return (file.isFile() && file.getName().endsWith(".xml") && (file.lastModified() > cache.lastUpdateTimestamp));
						return (file.isFile() && file.getName().endsWith(".xml"));
					}
				});
				for (int cf = 0; cf < masterCacheFiles.length; cf++) {
					String cfName = masterCacheFiles[cf].getName();
					for (int c = 0; c < caches.length; c++) {
						File cacheFile = new File(caches[c].cacheFolder, (flName + "/" + slName + "/" + cfName));
						if (!cacheFile.exists() || (cacheFile.lastModified() < masterCacheFiles[cf].lastModified())) {
							String docId = cfName.substring(0, (cfName.length() - 4));
							cacheUpdateDocIDs[c].add(docId);
							updateDocIDsAndTimestamps.put(docId, new Long(masterCacheFiles[cf].lastModified()));
						}
					}
				}
			}
		}
		
		//	order document IDs by timestamp
		String[] updateDocIDs = ((String[]) updateDocIDsAndTimestamps.keySet().toArray(new String[updateDocIDsAndTimestamps.size()]));
		Arrays.sort(updateDocIDs, new Comparator() {
			public int compare(Object o1, Object o2) {
				return ((Long) updateDocIDsAndTimestamps.get(o1)).compareTo((Long) updateDocIDsAndTimestamps.get(o2));
			}
		});
		
		//	enqueue updates for all missing or outdated documents
		for (int d = 0; d < updateDocIDs.length; d++) {
			final String updateDocId = updateDocIDs[d];
			for (int c = 0; c < caches.length; c++) {
				final Cache cache = caches[c];
				if (cacheUpdateDocIDs[c].contains(updateDocId)) {
					
					//	do update immediately
					if (force) {
						try {
							updateCacheDocument(cache, updateDocId, ((Long) updateDocIDsAndTimestamps.get(updateDocId)).longValue());
						}
						catch (Exception e) {
							System.out.println(e.getClass().getName() + " while updating document '" + updateDocId + "' in cache '" + cache.name + "': " + e.getMessage());
							e.printStackTrace(System.out);
						}
					}
					
					//	enqueue update
					else this.enqueueCacheUpdate(cache, new Update(cache.name + "." + updateDocId) {
						public void doUpdate() {
							try {
								updateCacheDocument(cache, updateDocId, ((Long) updateDocIDsAndTimestamps.get(updateDocId)).longValue());
							}
							catch (Exception e) {
								System.out.println(e.getClass().getName() + " while updating document '" + updateDocId + "' in cache '" + cache.name + "': " + e.getMessage());
								e.printStackTrace(System.out);
							}
						}
					});
				}
			}
		}
	}
	
	void updateCacheDocument(Cache cache, String docId, long documentTimestamp) throws Exception {
		
		//	get writer
		BufferedWriter out = this.getCacheFileWriter(cache.cacheFolder, docId, documentTimestamp);
		
		//	document up to date, or generation in progress
		if (out == null) return;
		
		//	get data source
		BufferedReader in = new BufferedReader(this.getCacheFileReader(this.masterCache.cacheFolder, docId));
		
		//	create transformed cache file
		cache.xsltTransformer.transform(new StreamSource(in), new StreamResult(out));
		
		//	clean up
		out.flush();
		out.close();
		in.close();
		
		//	remember update
		cache.lastUpdateTimestamp = documentTimestamp;
	}
	
	private void enqueueCacheUpdate(Cache cache, Update update) {
		synchronized(this.updateQueue) {
			this.updateQueue.addLast(update);
			this.updateQueue.notify();
		}
	}
	
	private class CacheUpdaterThread extends Thread {
		private boolean doUpdates = true;
		public void run() {
			
			//	give the rest a little time
			try {
				Thread.sleep(1000 * 65);
			} catch (InterruptedException ie) {}
			
			//	run in infinity
			while (this.doUpdates) {
				
				Update update = null;
				synchronized (updateQueue) {
					try {
						updateQueue.wait(updateQueue.isEmpty() ? 0 : 100);
					} catch (InterruptedException ie) {}
					
					if (this.doUpdates && (updateQueue.size() != 0))
						update = ((Update) updateQueue.removeFirst());
				}
				
				if (update != null) update.update();
			}
		}
		void shutdown() {
			synchronized(updateQueue) {
				this.doUpdates = false;
				updateQueue.notify();
			}
		}
	}
	
	private Reader getCacheFileReader(File cacheRoot, String docId) throws IOException {
		File firstLevel = new File(cacheRoot, docId.substring(0, 2));
		File secondLevel = new File(firstLevel, docId.substring(2, 4));
		File cacheFile = new File(secondLevel, (docId + ".xml"));
		return new InputStreamReader(new FileInputStream(cacheFile), "UTF-8");
	}
	
	private BufferedWriter getCacheFileWriter(File cacheRoot, final String docId, final long documentTimestamp) throws IOException {
		
		final File firstLevel = new File(cacheRoot, docId.substring(0, 2));
		if (!firstLevel.exists())
			firstLevel.mkdirs();
		
		final File secondLevel = new File(firstLevel, docId.substring(2, 4));
		if (!secondLevel.exists())
			secondLevel.mkdirs();
		
		final File cachingFile = new File(secondLevel, (docId + ".caching.xml"));
		
		//	generation in progress
		if (cachingFile.exists()) return null;
		
		final File cacheFile = new File(secondLevel, (docId + ".xml"));
		
		//	file up to date
		if (cacheFile.lastModified() == documentTimestamp) return null;
		
		//	return generation file writer
		return new BufferedWriter(new FilterWriter(new OutputStreamWriter(new FileOutputStream(cachingFile), "UTF-8")) {
			public void close() throws IOException {
				super.flush();
				super.close();
				
				if (cacheFile.exists())
					cacheFile.delete();
				cachingFile.renameTo(cacheFile);
				
				cachingFile.setLastModified(documentTimestamp);
				firstLevel.setLastModified(documentTimestamp);
				secondLevel.setLastModified(documentTimestamp);
			}
		});
	}
	
	void createCacheZipFile(Cache cache, boolean force) throws IOException {
		
		//	zip being created, nothing to do
		if (cache.zipping) {
			if (force)
				cache.regularZipping = false;
			return;
		}
		
		//	zip up to date
		if (cache.lastUpdateTimestamp == cache.lastZipUpdateTimestamp) return;
		
		//	lock cache
		cache.zipping = true;
		cache.regularZipping = !force;
		//	TODOne: check if extending zip file is possible, if so, use here ==> not possible
		
		//	initialize generation file
		File cacheZipGenFile = new File(cache.cacheFolder.getAbsolutePath() + ".generate");
		if (cacheZipGenFile.exists())
			cacheZipGenFile.delete();
		cacheZipGenFile.createNewFile();
		long cacheZipFileTimestamp = 0;
		
		//	create appropriate writer
		ZipOutputStream cacheZipGenOut = new ZipOutputStream(new FileOutputStream(cacheZipGenFile));
		
		//	compute delta to master cache
		File[] firstLevel = cache.cacheFolder.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.isDirectory();
			}
		});
		for (int fl = 0; fl < firstLevel.length; fl++) {
			File[] secondLevel = firstLevel[fl].listFiles(new FileFilter() {
				public boolean accept(File file) {
					return file.isDirectory();
				}
			});
			for (int sl = 0; sl < secondLevel.length; sl++) {
				File[] cacheFiles = secondLevel[sl].listFiles(new FileFilter() {
					public boolean accept(File file) {
						return (file.isFile() && file.getName().endsWith(".xml"));
					}
				});
				for (int cf = 0; cf < cacheFiles.length; cf++) {
					
					//	update overall timestamp
					cacheZipFileTimestamp = Math.max(cacheZipFileTimestamp, cacheFiles[cf].lastModified());
					
					//	initialize entry
					ZipEntry ze = new ZipEntry(cacheFiles[cf].getName());
					ze.setTime(cacheFiles[cf].lastModified());
					cacheZipGenOut.putNextEntry(ze);
					
					//	write selected files
					byte[] buffer = new byte[1024];
					int bufferLevel;
					InputStream docIn = new FileInputStream(cacheFiles[cf]);
					
					while ((bufferLevel = docIn.read(buffer)) != -1)
						cacheZipGenOut.write(buffer, 0, bufferLevel);
					
					cacheZipGenOut.closeEntry();
					docIn.close();
					
					//	give some time to others if not in forced update
					if (cache.regularZipping)
						try {
							Thread.sleep(50);
						} catch (InterruptedException ie) {}
				}
			}
		}
		
		//	finish writing
		cacheZipGenOut.flush();
		cacheZipGenOut.close();
		
		//	set modification timestamp
		cacheZipGenFile.setLastModified(cacheZipFileTimestamp);
		
		//	put new file live
		File cacheZipFile = new File(cache.cacheFolder.getAbsolutePath() + ".zip");
		if (cacheZipFile.exists())
			cacheZipFile.delete();
		cacheZipGenFile.renameTo(cacheZipFile);
		
		//	remember zipped
		cache.lastZipUpdateTimestamp = cacheZipFileTimestamp;
		cache.zipping = false;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.WebServlet#exit()
	 */
	protected void exit() {
		
		//	stop updater thread
		this.updateFetcher.shutdown();
		this.cacheUpdater.shutdown();
	}
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		//	get pth infor and truncate leading slash
		String pathInfo = request.getPathInfo();
		if ((pathInfo != null) && pathInfo.startsWith("/"))
			pathInfo = pathInfo.substring(1);
		
		//	extract cache name and document ID
		String cacheName = null;
		String docId = null;
		if (pathInfo.indexOf('/') != -1) {
			cacheName = pathInfo.substring(0, pathInfo.indexOf('/'));
			docId = pathInfo.substring(pathInfo.indexOf('/') + 1);
		}
		else {
			cacheName = pathInfo;
			docId = request.getParameter(ID_QUERY_FIELD_NAME);
		}
		
		//	zipped dump of cache required?
		if (cacheName.endsWith(".zip")) {
			cacheName = cacheName.substring(0, (cacheName.length() - 4));
			docId = "zip";
		}
		
		//	get cache
		Cache cache = ("xml".equals(cacheName) ? this.masterCache : ((Cache) this.cachesByName.get(cacheName)));
		if (cache == null) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND);
			return;
		}
		
		//	general request
		if (docId == null) {
			
			//	extract timestamp query
			String lastModifiedSince = request.getParameter(LAST_MODIFIED_SINCE);
			long minLastModified;
			
			//	unrestricted (full dump) query
			if (lastModifiedSince == null)
				minLastModified = 0;
			
			//	delta query
			else minLastModified = Long.parseLong(lastModifiedSince);
			
			//	return cached data
			response.setContentType("text/xml");
			response.setCharacterEncoding(request.getCharacterEncoding());
			response.setHeader("Cache-Control", "no-cache");
			this.serveCacheContent(cache.cacheFolder, new OutputStreamWriter(response.getOutputStream(), request.getCharacterEncoding()), minLastModified);
		}
		
		//	refresh command
		else if ("refresh".equals(docId)) {
			
			//	refresh master cache from SRS
			if (cache == this.masterCache) {
				response.setContentType("text/html");
				response.setHeader("Cache-Control", "no-cache");
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), request.getCharacterEncoding()));
				
				//	write response
				bw.write("<html>");
				bw.newLine();
				bw.write("<body>");
				bw.newLine();
				bw.write("<tt>updating master cache ... this might take some time ...</tt>");
				bw.newLine();
				bw.write("</body>");
				bw.newLine();
				bw.write("</html>");
				bw.newLine();
				bw.flush();
				
				//	do the update
				this.updateMasterCache(true);
			}
			
			//	refresh custom cache from master cache
			else {
				response.setContentType("text/html");
				response.setHeader("Cache-Control", "no-cache");
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), request.getCharacterEncoding()));
				
				//	write response
				bw.write("<html>");
				bw.newLine();
				bw.write("<body>");
				bw.newLine();
				bw.write("<tt>updating '" + cacheName + "' ... this might take some time ...</tt>");
				bw.newLine();
				bw.write("</body>");
				bw.newLine();
				bw.write("</html>");
				bw.newLine();
				bw.flush();
				
				//	do the update
				this.updateCache(cache, true);
			}
		}
		
		//	refresh zip file of specified cache
		else if ("rezip".equals(docId)) {
			response.setContentType("text/html");
			response.setHeader("Cache-Control", "no-cache");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), request.getCharacterEncoding()));
			
			//	write response
			bw.write("<html>");
			bw.newLine();
			bw.write("<body>");
			bw.newLine();
			
			if (cache.zipping)
				bw.write("<tt>re-creating zip-compressed dump of '" + cacheName + "' ... this might take some time ...</tt>");
			else if (cache.lastUpdateTimestamp == cache.lastZipUpdateTimestamp)
				bw.write("<tt>zip-compressed dump of '" + cacheName + "' is up to date</tt>");
			else bw.write("<tt>zip-compressed dump of '" + cacheName + "' is already in the process of being created</tt>");
			bw.newLine();
			
			bw.write("</body>");
			bw.newLine();
			bw.write("</html>");
			bw.newLine();
			bw.flush();
			
			//	do update
			this.createCacheZipFile(cache, true);
		}
		
		//	request for zipped collection dump
		else if ("zip".equals(docId)) {
			InputStream zipIn;
			try {
				zipIn = new FileInputStream(new File(this.cacheRoot, (cacheName + ".zip")));
			}
			catch (FileNotFoundException fnfe) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			
			response.setContentType("application/zip");
			response.setCharacterEncoding(request.getCharacterEncoding());
			response.setHeader("Cache-Control", "no-cache");
			OutputStream zipOut = response.getOutputStream();
			
			byte[] buffer = new byte[1024];
			int bufferLevel;
			while ((bufferLevel = zipIn.read(buffer)) != -1)
				zipOut.write(buffer, 0, bufferLevel);
			
			zipOut.flush();
			zipIn.close();
		}
		
		//	request for specific document
		else {
			Reader docReader;
			try {
				docReader = this.getCacheFileReader(cache.cacheFolder, docId);
			}
			catch (FileNotFoundException fnfe) {
				response.sendError(HttpServletResponse.SC_NOT_FOUND);
				return;
			}
			
			response.setContentType("text/xml");
			response.setCharacterEncoding(request.getCharacterEncoding());
			response.setHeader("Cache-Control", "no-cache");
			Writer docWriter = new OutputStreamWriter(response.getOutputStream(), request.getCharacterEncoding());
			
			char[] buffer = new char[1024];
			int bufferLevel;
			while ((bufferLevel = docReader.read(buffer)) != -1)
				docWriter.write(buffer, 0, bufferLevel);
			
			docWriter.flush();
			docReader.close();
		}
	}
	
	private void serveCacheContent(File cacheRoot, Writer docWriter, final long minLastModified) throws IOException {
		
		//	initialize registers
		ArrayList outputDocFiles = new ArrayList();
		
		//	compute delta to master cache
		File[] masterFirstLevel = cacheRoot.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return (file.isDirectory() && (minLastModified <= file.lastModified()));
			}
		});
		for (int fl = 0; fl < masterFirstLevel.length; fl++) {
			File[] masterSecondLevel = masterFirstLevel[fl].listFiles(new FileFilter() {
				public boolean accept(File file) {
					return (file.isDirectory() && (minLastModified <= file.lastModified()));
				}
			});
			for (int sl = 0; sl < masterSecondLevel.length; sl++) {
				File[] masterCacheFiles = masterSecondLevel[sl].listFiles(new FileFilter() {
					public boolean accept(File file) {
						return (file.isFile() && file.getName().endsWith(".xml") && (minLastModified <= file.lastModified()));
					}
				});
				for (int cf = 0; cf < masterCacheFiles.length; cf++)
					outputDocFiles.add(masterCacheFiles[cf]);
			}
		}
		
		//	order document files by timestamp
		Collections.sort(outputDocFiles, new Comparator() {
			public int compare(Object o1, Object o2) {
				return (new Long(((File) o1).lastModified())).compareTo(new Long(((File) o2).lastModified()));
			}
		});
		
		//	write selected files
		char[] buffer = new char[1024];
		int bufferLevel;
		for (int d = 0; d < outputDocFiles.size(); d++) {
			Reader docReader = new InputStreamReader(new FileInputStream((File) outputDocFiles.get(d)), "UTF-8");
			
			while ((bufferLevel = docReader.read(buffer)) != -1)
				docWriter.write(buffer, 0, bufferLevel);
			
			docWriter.flush();
			docReader.close();
		}
	}
}
