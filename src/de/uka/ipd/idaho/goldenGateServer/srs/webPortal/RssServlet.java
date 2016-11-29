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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentListElement;

/**
 * This servlet creates RSS feeds for the documents hosted in the backing
 * GoldenGATE SRS. You can customize the produced RSS feeds through a series of
 * parameters in the config file of the servlet:
 * <ul>
 * <li><b>rssGenerationInterval</b>: the number of seconds between two
 * generations of the feed files. A smaller interval propagates updates faster,
 * whereas a larger interval keeps generation costs lower. The default value of
 * this parameter is 86,400 seconds, so the feed files are generated once a day.</li>
 * <li><b>feedName</b>: this parameter is actually a prefix identifying the
 * group of parameters that represent one RSS feed to generate. There can be any
 * number of feeds, but they have to be distinct names. The names have to
 * consist of alphanumeric characters only. For the individual parameters of
 * each RSS feed, see below the documentation of the nested RSS class.</li>
 * <li><b>docsLink</b>: the servlet-wide default value of the URL of the RSS
 * documentation, shared among feed definitions that do not specify this
 * parameter themselves. Currently defaults to
 * 'http://www.rssboard.org/rss-specification'</li>
 * <li><b>webMasterMailAddress</b>: the servlet-wide default value of the mail
 * address of the web master responsible for the RSS feed, shared among feed
 * definitions that do not specify this parameter themselves</li>
 * <li><b>editorMailAddress</b>: the servlet-wide default value of the mail
 * address of the responsible editor, shared among feed definitions that do not
 * specify this parameter themselves</li>
 * <li><b>descriptionPattern</b>: the servlet-wide default value of the string
 * representation of the AttributePattern for producing the description part of
 * the feed items from the document's attributes, shared among feed definitions
 * that do not specify this parameter themselves</li>
 * <li><b>sitemapFile</b>: the file for writing a sitemap-style index of the
 * backing SRS's document collection to (optional parameter, if not specified,
 * no sitemap will be created)</li>
 * <li><b>sitemapUrlPrefix</b>: the URL to put before the document IDs in the
 * sitemap of the backing SRS's document collection to (optional parameter, if
 * not specified, no sitemap will be created)</li>
 * </ul>
 * 
 * @author sautter
 */
public class RssServlet extends AbstractSrsWebPortalServlet implements SearchPortalConstants {
	
	/**
	 * Descriptor of an individual RSS feed. This class bears a series of
	 * parameters for controlling the feed produced:
	 * <ul>
	 * <li><b>name</b>: the name of the feed, must be alphanumeric. This name is
	 * the prefix identifying the remaining configuration data in the
	 * surrounding servlet's config file. It also serves as the prefix for the
	 * feed cache file. The invocation path '&lt;name&gt;.rss' has to be mapped
	 * to the surrounding servlet in order for the feed to work, where
	 * &lt;name&gt; is the actual value of the name parameter. In the
	 * surrounding servlet's config file, the remaining parameters have to be
	 * specified as '&lt;name&gt;.&lt;paramName&gt;', where &lt;paramName&gt; is
	 * a placeholder for any one of the parameters below.</li>
	 * <li><b>title</b>: the title to appear in the RSS feed</li>
	 * <li><b>maxItemAge</b>: the maximum age for an item to be included in the
	 * RSS feed, in seconds; 0 or less means no limitation</li>
	 * <li><b>maxItemCount</b>: the maximum number of items to include in the
	 * RSS feed, counting from the newest backward; 0 or less means no limitation
	 * </li>
	 * <li><b>maxRestrictAnd</b>: if both item age and item count filters are
	 * active, take the more restrictive or the less restrictive one?</li>
	 * <li><b>description</b>: the textual description of the RSS feed</li>
	 * <li><b>docsLink</b>: the URL of the RSS documentation. If this parameter
	 * is not specified, the servlet-wide default is used.</li>
	 * <li><b>webMasterMailAddress</b>: the mail address of the web master
	 * responsible for the RSS feed. If this parameter is not specified, the
	 * servlet-wide default is used.</li>
	 * <li><b>editorMailAddress</b>: the mail address of the responsible editor.
	 * If this parameter is not specified, the servlet-wide default is used.</li>
	 * <li><b>descriptionPattern</b>: the string representation of the
	 * AttributePattern for producing the description part of the feed items
	 * from the document's attributes. If this parameter is not specified, the
	 * servlet-wide default is used.</li>
	 * <li><b>docLinkUrlPrefix</b>: the URL to append the document IDs to for
	 * retrieving the complete documents described by the items in the RSS feed.
	 * If retrieving the documents for the feed involves an XSLT transformation,
	 * it is a good idea to use XsltServlet and using the feed name as a
	 * configured XSLT name over there, mapped to the URL of the desired XSLT
	 * stylesheet.</li>
	 * </ul>
	 * 
	 * @author sautter
	 */ // this class has to be public in order for the JavaDoc to be available ...
	public static class RssFeed {
		String name = null; // the name of the feed
		
		String title = null; // the title of the feed
		
		int maxItemAge = 0; // the maximum age for an item to be included in the feed
		int maxItemCount = 0; // the maximum number of items to include
		boolean maxRestrictAnd = false; // when filtering both number and age of items, apply the narrower or the wider restriction?
		
		String description = null; // description of the feed as a whole
		String webMasterMailAddress = null; // admin's main address
		String editorMailAddress = null; // editor's main address
		String docsLink = "http://www.rssboard.org/rss-specification"; // this one's fixed for now ...
		
		String docLinkUrlPrefix; // the link to append the document ID to 
		AttributePattern descriptionPattern; // the pattern for creating the descriptions of the individual feed items
		
		String feedFileName; // where to cache the feed?
		
		RssFeed(String name) {
			this.name = name;
			this.feedFileName = (this.name + FEED_FILE_SUFFIX);
		}
		
		boolean isValid() {
			return ((this.name != null)
					&& (this.title != null)
					
					&& (this.description != null)
					&& (this.webMasterMailAddress != null)
					&& (this.editorMailAddress != null)
					&& (this.docsLink != null)
					
					&& (this.docLinkUrlPrefix != null)
					&& (this.descriptionPattern != null)
					
					&& (this.feedFileName != null)
			);
		}
	}
	private static final String FEED_FILE_SUFFIX = ".rss.xml";
	
	private RssFeed[] rssFeeds = new RssFeed[0];
	private File rssFeedCacheFolder;
	
	private File sitemapFile = null;
	private String sitemapUrlPrefix = null;
	
	private long generationTimestamp = 0; 
	private int generationInterval = 86400; // generate once a day by default 
	
	private FeedGenerator rssFeedGenerator;
	
	private static final String DEFAULT_RSS_FEED_TIMESTAMP_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'-02:00'";
	private static final DateFormat RSS_FEED_TIMESTAMP_DATE_FORMAT = new SimpleDateFormat(DEFAULT_RSS_FEED_TIMESTAMP_DATE_FORMAT);
	
	private static final String DEFAULT_RSS_DOCS_LINK = "http://www.rssboard.org/rss-specification";
	
	private static final String DEFAULT_SITEMAP_TIMESTAMP_DATE_FORMAT = "yyyy-MM-dd";
	private static final DateFormat SITEMAP_TIMESTAMP_DATE_FORMAT = new SimpleDateFormat(DEFAULT_SITEMAP_TIMESTAMP_DATE_FORMAT);
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.webPortal.AbstractSrsWebPortalServlet#doInit()
	 */
	protected void doInit() throws ServletException {
		super.doInit();
		
		//	get where to cache the feeds
		this.rssFeedCacheFolder = new File(new File(this.webInfFolder, "caches"), "srsRssFeeds");
		if (!this.rssFeedCacheFolder.exists())
			this.rssFeedCacheFolder.mkdir();
		
		//	start feed generator
		this.rssFeedGenerator = new FeedGenerator();
		this.rssFeedGenerator.start();
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.HtmlServlet#reInit()
	 */
	protected void reInit() throws ServletException {
		super.reInit();
		
		//	read generation interval
		try {
			this.generationInterval = Integer.parseInt(this.getSetting("rssGenerationInterval", ("" + this.generationInterval)));
		} catch (NumberFormatException nfe) {}
		
		//	load defaults
		String docsLink = this.getSetting("docsLink", DEFAULT_RSS_DOCS_LINK);
		String webMasterMailAddress = this.getSetting("webMasterMailAddress");
		String editorMailAddress = this.getSetting("editorMailAddress");
		String descriptionPattern = this.getSetting("descriptionPattern", "");
		
		//	load feed definitions
		String[] feedNames = this.config.getSubsetPrefixes();
		ArrayList feedList = new ArrayList();
		for (int f = 0; f < feedNames.length; f++) {
			RssFeed feed = new RssFeed(feedNames[f]);
			Settings feedSet = this.config.getSubset(feedNames[f]);
			
			feed.title = feedSet.getSetting("title");
			
			feed.maxItemAge = Integer.parseInt(feedSet.getSetting("maxItemAge", ("" + feed.maxItemAge)));
			feed.maxItemCount = Integer.parseInt(feedSet.getSetting("maxItemCount", ("" + feed.maxItemCount)));
			feed.maxRestrictAnd = "true".equalsIgnoreCase(feedSet.getSetting("maxRestrictAnd", "false"));
			
			feed.description = feedSet.getSetting("description");
			feed.docsLink = feedSet.getSetting("docsLink", docsLink);
			feed.webMasterMailAddress = feedSet.getSetting("webMasterMailAddress", webMasterMailAddress);
			feed.editorMailAddress = feedSet.getSetting("editorMailAddress", editorMailAddress);
			
			feed.docLinkUrlPrefix = feedSet.getSetting("docLinkUrlPrefix");
			feed.descriptionPattern = AttributePattern.buildPattern(this.getSetting("descriptionPattern", descriptionPattern));
			
			if (feed.isValid())
				feedList.add(feed);
		}
		this.rssFeeds = ((RssFeed[]) feedList.toArray(new RssFeed[feedList.size()]));
		
		//	initialize sitemap (has to be in root folder for search engines to find it)
		String sitemapFile = this.getSetting("sitemapFile");
		this.sitemapUrlPrefix = this.getSetting("sitemapUrlPrefix");
		if ((sitemapFile != null) && (this.sitemapUrlPrefix != null))
			this.sitemapFile = new File(this.rootFolder, sitemapFile);
		else this.sitemapFile = null;
		
		//	extract generation timestamp
		this.generationTimestamp = 0; // reset to facilitate re-reading timestamp from files
		for (int f = 0; f < this.rssFeeds.length; f++) {
			File feedCacheFile = new File(this.rssFeedCacheFolder, this.rssFeeds[f].feedFileName);
			if (feedCacheFile.exists())
				this.generationTimestamp = Math.max(feedCacheFile.lastModified(), this.generationTimestamp);
		}
	}
	
	private class FeedGenerator extends Thread {
		private boolean run = true;
		FeedGenerator() {
			super("SrsFeedGenerator");
		}
		public void run() {
			while (this.run) {
				
				//	generate feeds if it's the time
				if ((generationTimestamp + (1000 * ((long) generationInterval))) < System.currentTimeMillis()) {
					try {
						generateRssFeeds(this);
					}
					catch (IOException ioe) {
						System.out.println(ioe.getClass().getName() + " while generating feeds: " + ioe.getMessage());
						ioe.printStackTrace(System.out);
					}
					catch (Exception e) {
						System.out.println(e.getClass().getName() + " while generating feeds: " + e.getMessage());
						e.printStackTrace(System.out);
					}
					catch (Throwable t) {
						System.out.println(t.getClass().getName() + " while generating feeds: " + t.getMessage());
						t.printStackTrace(System.out);
					}
				}
				
				//	wait otherwise
				else {
					synchronized(this) {
						try {
							this.wait(generationInterval * 10);
						} catch (InterruptedException ie) {}
					}
				}
			}
		}
		void shutdown() {
			synchronized(this) {
				this.run = false;
				this.notify();
			}
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.easyIO.web.WebServlet#exit()
	 */
	protected void exit() {
		this.rssFeedGenerator.shutdown();
	}
	
	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		
		String invocationPath = request.getServletPath();
		if (invocationPath.indexOf('.') == -1) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, ("Feed '" + invocationPath + "' not found, available feeds are " + Arrays.toString(this.config.getSubsetPrefixes())));
			return;
		}
		else invocationPath = invocationPath.substring(0, invocationPath.indexOf('.'));
		
		File feedFile = new File(this.rssFeedCacheFolder, (invocationPath + FEED_FILE_SUFFIX));
		if (!feedFile.exists()) {
			response.sendError(HttpServletResponse.SC_NOT_FOUND, ("Feed '" + invocationPath + "' not available right now, last generation was at " + this.generationTimestamp));
			return;
		}
		
		Reader r = new BufferedReader(new InputStreamReader(new FileInputStream(feedFile), ENCODING));
		response.setContentType("text/xml");
		response.setContentLength((int) feedFile.length());
		response.setHeader("Expires", expiresFormat.format(new Date(System.currentTimeMillis() + (1000 * ((long) this.generationInterval)))));
		Writer w = new BufferedWriter(new OutputStreamWriter(response.getOutputStream(), ENCODING));
		
		char[] buf = new char[1024];
		int len = 0;
		while ((len = r.read(buf)) != -1)
			w.write(buf, 0, len);
		
		w.flush();
		r.close();
	}
	
	private static final SimpleDateFormat expiresFormat = new SimpleDateFormat("EE, dd MMM yyyy HH:mm:ss 'GMT'Z", Locale.US);
	private static final Comparator checkinTimeOrder = new Comparator() {
		public int compare(Object o1, Object o2) {
			DocumentListElement dle1 = ((DocumentListElement) o1);
			DocumentListElement dle2 = ((DocumentListElement) o2);
			
			String s1 = dle1.getAttribute(CHECKIN_TIME_ATTRIBUTE, "").toString();
			String s2 = dle2.getAttribute(CHECKIN_TIME_ATTRIBUTE, "").toString();
			
			//	try number comparison
			try {
				long l1 = Long.parseLong(s1);
				long l2 = Long.parseLong(s2);
//				return ((int) (l1 - l2)); // ascending order is a bad idea, as this gives the least interesting items first
//				return ((int) (l2 - l1)); // this is just as bad an idea because of overflows
				if (l1 < l2)
					return -1;
				else if (l2 < l1)
					return 1;
				else return 0;
			}
			
			//	do string comparison
			catch (NumberFormatException nfe) {
//				return s1.compareTo(s2);
				return s2.compareTo(s1);
			}
		}
	};
	
	private void generateRssFeeds(FeedGenerator feedGen) throws IOException {
		this.generationTimestamp = System.currentTimeMillis(); // set this immediately in order to block further invocations
		long lastMasterDocUpdate = 0;
		
		//	get master list
		DocumentList masterDocList = this.srsClient.getDocumentList(null);
		ArrayList masterDocEntryList = new ArrayList();
		while (masterDocList.hasNextElement()) {
			DocumentListElement masterDle = masterDocList.getNextDocumentListElement();
			masterDocEntryList.add(masterDle);
			try {
				lastMasterDocUpdate = Math.max(lastMasterDocUpdate, Long.parseLong((String) masterDle.getAttribute(UPDATE_TIME_ATTRIBUTE, "0")));
			} catch (NumberFormatException nfe) {}
		}
		System.out.println("Feed generation starting with " + masterDocEntryList.size() + " master documents");
		
		//	check if update necessary at all
		boolean needToGenerate = false;
		for (int f = 0; f < this.rssFeeds.length; f++) {
			File feedCacheFile = new File(this.rssFeedCacheFolder, this.rssFeeds[f].feedFileName);
			if (feedCacheFile.lastModified() < lastMasterDocUpdate) {
				needToGenerate = true;
				f = this.rssFeeds.length;
			}
		}
		if (!needToGenerate) {
			System.out.println("Feed generation abandoned, all feeds up to date");
			return;
		}
		
		//	sort master list
		Collections.sort(masterDocEntryList, checkinTimeOrder);
		
		//	create writer files for RSS feeds ...
		RssFeedWriter rssFeedWriter = new RssFeedWriter(this.rssFeedCacheFolder, this.rssFeeds, this.generationInterval, this.generationTimestamp);
		System.out.println("Feed generation output writers created");
		
		//	... and for sitemap
		SitemapWriter sitemapWriter = ((this.sitemapFile == null) ? null : new SitemapWriter(this.sitemapFile, this.sitemapUrlPrefix, this.generationTimestamp));
		System.out.println("Sitemap output writer created");
		
		//	write header
		rssFeedWriter.writeHeader();
		if (sitemapWriter != null)
			sitemapWriter.writeHeader();
		System.out.println("Feed headers written");
		
		/*
<?xml version='1.0' encoding='UTF-8'?>
<urlset
  xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"
   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
   xsi:schemaLocation="http://www.sitemaps.org/schemas/sitemap/0.9 http://www.sitemaps.org/schemas/sitemap/0.9/sitemap.xsd"
   >
        <url>
                <loc>http://w3c-at.de</loc>
                <lastmod>2006-11-18</lastmod>
                <changefreq>daily</changefreq>
                <priority>0.8</priority>
        </url>
</urlset>
		 */
		
		//	write channel info
		rssFeedWriter.writeChannelInfo();
		System.out.println("Feed channel info written");
		
		//	write data
		for (int m = 0; m < masterDocEntryList.size(); m++) {
			
			//	are we being shut down?
			if (!feedGen.run) {
				System.out.println("Feed generation interrupted by shutdown");
				return;
			}
			
			//	get next master document
			DocumentListElement masterDle = ((DocumentListElement) masterDocEntryList.get(m));
			String masterDocId = ((String) masterDle.getAttribute(MASTER_DOCUMENT_ID_ATTRIBUTE));
			System.out.println("Creating feed entries from master document " + masterDocId);
			
			//	get individual documents
			ArrayList docEntryList = new ArrayList();
			
			//	try using cached document list
			long masterDocUpdateTime = Long.parseLong((String) masterDle.getAttribute(UPDATE_TIME_ATTRIBUTE));
			File masterDocCacheFile = new File(this.rssFeedCacheFolder, masterDocId + ".cached");
			if (masterDocUpdateTime <= masterDocCacheFile.lastModified()) {
				DocumentList docList = DocumentList.readDocumentList(new InputStreamReader(new FileInputStream(masterDocCacheFile), ENCODING));
				while (docList.hasNextElement())
					docEntryList.add(docList.getNextDocumentListElement());
				System.out.println(" ==> cache hit for " + docEntryList.size() + " document IDs");
			}
			
			//	cache miss, load document list from backing SRS and cache it
			else {
				
				//	delete outdated cache file
				if (masterDocCacheFile.exists())
					masterDocCacheFile.delete();
				
				//	get document list
				DocumentList docList = this.srsClient.getDocumentList(masterDocId);
				
				//	and cache it
				masterDocCacheFile.createNewFile();
				BufferedWriter masterDocCacheWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(masterDocCacheFile), ENCODING));
				masterDocCacheWriter.write(docList.produceStartTag());
				masterDocCacheWriter.newLine();
				while (docList.hasNextElement()) {
					DocumentListElement dle = docList.getNextDocumentListElement();
					docList.writeElement(masterDocCacheWriter, dle);
					docEntryList.add(dle);
				}
				masterDocCacheWriter.write(docList.produceEndTag());
				masterDocCacheWriter.newLine();
				masterDocCacheWriter.flush();
				masterDocCacheWriter.close();
				masterDocCacheFile.setLastModified(masterDocUpdateTime);
				System.out.println(" ==> cache miss, cached " + docEntryList.size() + " document IDs");
//				
//				//	give the others some time TODO find out if this solves the breakdowns
//				try {
//					Thread.sleep(250);
//				} catch (InterruptedException ie) {}
			}
			
			//	sort documents
			Collections.sort(docEntryList, new Comparator() {
				public int compare(Object o1, Object o2) {
					DocumentListElement dle1 = ((DocumentListElement) o1);
					DocumentListElement dle2 = ((DocumentListElement) o2);
					
					String s1 = dle1.getAttribute(CHECKIN_TIME_ATTRIBUTE, "").toString();
					String s2 = dle2.getAttribute(CHECKIN_TIME_ATTRIBUTE, "").toString();
					
					//	try number comparison
					try {
						long i1 = Long.parseLong(s1);
						long i2 = Long.parseLong(s2);
						return ((int) (i1 - i2));
					}
					
					//	do string comparison
					catch (NumberFormatException nfe) {
						return s1.compareTo(s2);
					}
				}
			});
			System.out.println(" - documents sorted");
			
			//	write items
			for (int d = 0; d < docEntryList.size(); d++) {
				DocumentListElement document = ((DocumentListElement) docEntryList.get(d));
				rssFeedWriter.writeItem(document);
				if (sitemapWriter != null)
					sitemapWriter.writeItem(document);
			}
			System.out.println(" - feed items written");
			
			//	give the others some time TODO find out if this solves the breakdowns
			try {
				Thread.sleep(250);
			} catch (InterruptedException ie) {}
		}
		
		//	close files
		rssFeedWriter.finish();
		if (sitemapWriter != null)
			sitemapWriter.finish();
		System.out.println("Feed generation completed");
	}
	
	private static class RssFeedWriter {
		private final File rssFeedFolder;
		private final RssFeed[] rssFeeds;
		
		private final String generationTimestamp;
		private final int generationInterval;
		
		private final File[] feedGenFiles;
		private final BufferedWriter[] feedGenWriters;
		private final int[] feedItemCounts;
		private final long[] feedLastItemTimestamps;
		
		RssFeedWriter(File rssFeedFolder, RssFeed[] rssFeeds, int generationInterval, long generationTimestamp) throws IOException {
			this.rssFeedFolder = rssFeedFolder;
			this.rssFeeds = rssFeeds;
			
			this.feedGenFiles = new File[this.rssFeeds.length];
			this.feedGenWriters = new BufferedWriter[this.rssFeeds.length];
			
			this.feedItemCounts = new int[this.rssFeeds.length];
			this.feedLastItemTimestamps = new long[this.rssFeeds.length];
			
			for (int f = 0; f < this.rssFeeds.length; f++) {
				this.feedGenFiles[f] = new File(this.rssFeedFolder, (this.rssFeeds[f].feedFileName + ".generate"));
				
				//	make way, can only be result of a failed generation
				if (this.feedGenFiles[f].exists())
					this.feedGenFiles[f].delete();
				
				//	create generation file
				this.feedGenFiles[f].createNewFile();
				
				//	create writer (default buffer size of 8KB is sufficient)
				this.feedGenWriters[f] = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.feedGenFiles[f]), ENCODING));
				
				//	fill history arrays
				this.feedItemCounts[f] = 0;
				this.feedLastItemTimestamps[f] = 0;
			}
			this.generationInterval = generationInterval;
			this.generationTimestamp = RSS_FEED_TIMESTAMP_DATE_FORMAT.format(new Date(generationTimestamp));
		}
		
		void writeHeader() throws IOException {
			/*
			<?xml version="1.0"?>
			<rss version="2.0">
			  <channel>
			*/
			this.writeLine("<?xml version=\"1.0\"?>");
			this.writeLine("<rss version=\"2.0\">");
			this.writeLine("<channel>");
		}
		
		void writeChannelInfo() throws IOException {
			/*
		    <title>Lift Off News</title>
		    <link>http://liftoff.msfc.nasa.gov/</link>
		    <description>Liftoff to Space Exploration.</description>
		    <language>en-us</language>
		    <pubDate>Tue, 10 Jun 2003 04:00:00 GMT</pubDate>
		    <lastBuildDate>Tue, 10 Jun 2003 09:41:01 GMT</lastBuildDate>
		    <docs>http://blogs.law.harvard.edu/tech/rss</docs>
		    <generator>Weblog Editor 2.0</generator>
		    <managingEditor>editor@example.com</managingEditor>
		    <webMaster>webmaster@example.com</webMaster>
		    <ttl>5</ttl>		 
			*/
			for (int f = 0; f < this.feedGenWriters.length; f++)
				this.feedGenWriters[f].write("<title>" + AnnotationUtils.escapeForXml(this.rssFeeds[f].title) + "</title>");
			this.newLine();
			
			for (int f = 0; f < this.feedGenWriters.length; f++)
				this.feedGenWriters[f].write("<description>" + AnnotationUtils.escapeForXml(this.rssFeeds[f].description) + "</description>");
			this.newLine();
			
			this.writeLine("<pubDate>" + this.generationTimestamp + "</pubDate>");
			
			this.writeLine("<lastBuildDate>" + this.generationTimestamp + "</lastBuildDate>");
			
			for (int f = 0; f < this.feedGenWriters.length; f++)
				this.feedGenWriters[f].write("<docs>" + AnnotationUtils.escapeForXml(this.rssFeeds[f].docsLink) + "</docs>");
			this.newLine();
			
			this.writeLine("<generator>GoldenGATE SRS Search Portal RSS Extension</generator>");
			
			for (int f = 0; f < this.feedGenWriters.length; f++)
				this.feedGenWriters[f].write("<managingEditor>" + AnnotationUtils.escapeForXml(this.rssFeeds[f].editorMailAddress) + "</managingEditor>");
			this.newLine();
			
			for (int f = 0; f < this.feedGenWriters.length; f++)
				this.feedGenWriters[f].write("<webMaster>" + AnnotationUtils.escapeForXml(this.rssFeeds[f].webMasterMailAddress) + "</webMaster>");
			this.newLine();
			
			this.writeLine("<ttl>" + (this.generationInterval / 60) + "</ttl>");
		}
		
		/*
	    <item>
	      <title>Astronauts' Dirty Laundry</title>
	      <link>http://liftoff.msfc.nasa.gov/news/2003/news-laundry.asp</link>
	      <description>Compared to earlier spacecraft, the International Space
	        Station has many luxuries, but laundry facilities are not one of them.
	        Instead, astronauts have other options.</description>
	      <pubDate>Tue, 20 May 2003 08:56:02 GMT</pubDate>
	      <guid>http://liftoff.msfc.nasa.gov/2003/05/20.html#item570</guid>
	    </item>			
		*/
		
		void writeItem(final DocumentListElement item) throws IOException {
			String docId = ((String) item.getAttribute(DOCUMENT_ID_ATTRIBUTE));
			Properties itemProps = new Properties() {
				public String getProperty(String key, String defaultValue) {
					return ((String) item.getAttribute(key, defaultValue));
				}
				public String getProperty(String key) {
					return ((String) item.getAttribute(key));
				}
			};
			
			String title = ((String) item.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, ""));
			long checkinTime = -1;
			String timestamp = null;
			try {
				checkinTime = Long.parseLong((String) item.getAttribute(CHECKIN_TIME_ATTRIBUTE, "0"));
				timestamp = RSS_FEED_TIMESTAMP_DATE_FORMAT.format(new Date(checkinTime));
			} catch (NumberFormatException e) {}
			
			for (int f = 0; f < this.feedGenWriters.length; f++) {
				
				//	test if feed wants this item
				boolean dropItemForCount = false;
				boolean dropItemForAge = false;
				if (this.rssFeeds[f].maxItemCount > 0)
					dropItemForCount = ((this.feedItemCounts[f] >= this.rssFeeds[f].maxItemCount) && (checkinTime < this.feedLastItemTimestamps[f]));
				if (this.rssFeeds[f].maxItemAge > 0)
					dropItemForAge = (checkinTime < this.rssFeeds[f].maxItemAge);
				if (this.rssFeeds[f].maxRestrictAnd ? (dropItemForCount || dropItemForAge) : (dropItemForCount && dropItemForAge))
					continue;
				
				//	write item to feed
				this.feedGenWriters[f].write("<item>");
				this.feedGenWriters[f].newLine();
				
				this.feedGenWriters[f].write("<title>" + AnnotationUtils.escapeForXml(title) + "</title>");
				this.feedGenWriters[f].newLine();
				
				String description = this.rssFeeds[f].descriptionPattern.createDisplayString(itemProps);
				this.feedGenWriters[f].write("<description>" + AnnotationUtils.escapeForXml(description) + "</description>");
				this.feedGenWriters[f].newLine();
				
				String link = this.rssFeeds[f].docLinkUrlPrefix + (this.rssFeeds[f].docLinkUrlPrefix.endsWith("/") ? "" : "/") + this.rssFeeds[f].name + "/" + docId;
				this.feedGenWriters[f].write("<link>" + AnnotationUtils.escapeForXml(link) + "</link>");
				this.feedGenWriters[f].newLine();
				
				if (timestamp != null) {
					this.feedGenWriters[f].write("<pubDate>" + timestamp + "</pubDate>");
					this.feedGenWriters[f].newLine();
					
					this.feedLastItemTimestamps[f] = Math.min(checkinTime, this.feedLastItemTimestamps[f]);
				}
				
				this.feedGenWriters[f].write("<guid isPermaLink=\"false\">" + docId + "." + this.rssFeeds[f].name + "</guid>");
				this.feedGenWriters[f].newLine();
				
				this.feedGenWriters[f].write("</item>");
				this.feedGenWriters[f].newLine();
				
				this.feedItemCounts[f]++;
			}
		}
		
		private void writeLine(String str) throws IOException {
			this.write(str);
			this.newLine();
		}
		private void write(String str) throws IOException {
			for (int f = 0; f < this.feedGenWriters.length; f++)
				this.feedGenWriters[f].write(str);
		}
		private void newLine() throws IOException {
			for (int f = 0; f < this.feedGenWriters.length; f++)
				this.feedGenWriters[f].newLine();
		}
		
		void finish() throws IOException {
			this.writeLine("</channel>");
			this.writeLine("</rss>");
			for (int f = 0; f < this.feedGenWriters.length; f++) {
				this.feedGenWriters[f].flush();
				this.feedGenWriters[f].close();
				
				File feedFile = new File(this.rssFeedFolder, this.rssFeeds[f].feedFileName);
				if (feedFile.exists())
					feedFile.delete();
				this.feedGenFiles[f].renameTo(feedFile);
			}
		}
	}
	
	private static class SitemapWriter {
		private final String sitemapUrlPrefix;
		private final String generationDateString;
		
		private final File sitemapFile;
		private final File sitemapGenFile;
		private final BufferedWriter sitemapGenWriter;
		
		private final File itemFileFolder;
		
		private int itemFileNumber = 0;
		private File itemFile = null;
		
		private int genFileItemCount = 0;
		private File itemGenFile = null;
		private BufferedWriter itemGenWriter = null;
		
		SitemapWriter(File sitemapFile, String sitemapUrlPrefix, long generationTimestamp) throws IOException {
			
			this.sitemapFile = sitemapFile;
			this.sitemapGenFile = new File(this.sitemapFile.getAbsolutePath() + ".generate");
			
			//	make way, can only be result of a failed generation
			if (this.sitemapGenFile.exists())
				this.sitemapGenFile.delete();
			
			//	create folder if not existing
			else if (!this.sitemapGenFile.getParentFile().exists())
				this.sitemapGenFile.getParentFile().mkdirs();
			
			//	create generation file
			this.sitemapGenFile.createNewFile();
			
			//	create writer (default buffer size of 8KB is sufficient)
			this.sitemapGenWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.sitemapGenFile), ENCODING));
			
			this.itemFileFolder = this.sitemapFile.getParentFile();
			
			this.sitemapUrlPrefix = sitemapUrlPrefix;
			this.generationDateString = SITEMAP_TIMESTAMP_DATE_FORMAT.format(new Date(generationTimestamp));
			
		}
		
		void writeHeader() throws IOException {
			/*
			 <?xml version='1.0' encoding='UTF-8'?>
			 <sitemapindex xmlns='http://www.sitemaps.org/schemas/sitemap/0.9'>
			 */
			this.writeLine("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
			this.writeLine("<sitemapindex xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">");
		}
		
		private void closeItemFile() throws IOException {
			
			//	close item file
			this.itemGenWriter.flush();
			this.itemGenWriter.close();
			this.itemGenWriter = null;
			
			//	put file live
			if (this.itemFile.exists())
				this.itemFile.delete();
			this.itemGenFile.renameTo(this.itemFile);
			
			/*
			<sitemap>
				<url>http://www.gstatic.com/s2/sitemaps/sitemap-023.txt</url>
				<lastmod>2008-10-15</lastmod>
			</sitemap>
			 */
			
			//	write entry to main file
			this.writeLine("<sitemap>");
			this.writeLine("<url>./" + itemFile.getName() + "</url>");
			this.writeLine("<lastmod>" + generationDateString + "</lastmod>");
			this.writeLine("</sitemap>");
		}
		
		private void newItemFile() throws IOException {
			String itemFileNumber = ("" + this.itemFileNumber++);
			while (itemFileNumber.length() < 3)
				itemFileNumber = ("0" + itemFileNumber);
			this.itemFile = new File(this.itemFileFolder, ("sitemap-" + itemFileNumber + ".txt"));
			this.itemGenFile = new File(this.itemFileFolder, ("sitemap-" + itemFileNumber + ".generate"));
			if (this.itemGenFile.exists())
				this.itemGenFile.delete();
			this.itemGenFile.createNewFile();
			this.genFileItemCount = 0;
			this.itemGenWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.itemGenFile), "UTF-8"));
		}
		
		private void writeItem(final DocumentListElement item) throws IOException {
			if (this.itemGenWriter == null)
				this.newItemFile();
			
			if (this.itemGenWriter != null) {
				this.itemGenWriter.write(this.sitemapUrlPrefix + item.getAttribute(DOCUMENT_ID_ATTRIBUTE));
				this.itemGenWriter.newLine();
				if (10000 <= ++this.genFileItemCount)
					this.closeItemFile();
			}
		}
		
		private void writeLine(String str) throws IOException {
			this.write(str);
			this.newLine();
		}
		private void write(String str) throws IOException {
			this.sitemapGenWriter.write(str);
		}
		private void newLine() throws IOException {
			this.sitemapGenWriter.newLine();
		}
		
		void finish() throws IOException {
			this.closeItemFile();
			this.writeLine("</sitemapindex>");
			this.sitemapGenWriter.flush();
			this.sitemapGenWriter.close();
			
			if (this.sitemapFile.exists())
				this.sitemapFile.delete();
			this.sitemapGenFile.renameTo(this.sitemapFile);
		}
	}
}
