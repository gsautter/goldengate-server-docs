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
package de.uka.ipd.idaho.goldenGateServer.srs.indexers;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.TokenSequence;
import de.uka.ipd.idaho.gamta.util.CountingSet;
import de.uka.ipd.idaho.goldenGateServer.AsynchronousWorkQueue;
import de.uka.ipd.idaho.goldenGateServer.srs.AbstractIndexer;
import de.uka.ipd.idaho.goldenGateServer.srs.Query;
import de.uka.ipd.idaho.goldenGateServer.srs.QueryResult;
import de.uka.ipd.idaho.goldenGateServer.srs.QueryResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;
import de.uka.ipd.idaho.stringUtils.StringUtils;

/**
 * Indexer for full text search, using file based inverted index lists
 * 
 * @author sautter
 */
public class FullTextIndexer extends AbstractIndexer {
	
	/*
	 * Append document number to index lists for all terms present in the
	 * document. Keep document numbers sorted in index lists. Leave every
	 * fourth index entry empty for future insertions.
	 * 
	 * Index entry = 6 bytes:
	 * - four bytes document number
	 * - one byte term frequency
	 * - one byte 2-log of document length
	 */
	
//	private static final boolean DEBUG = false;
	
	private static final String FULL_TEXT_QUREY = "ftQuery";
	private static final String MATCH_MODE = "matchMode";
	private static final String EXACT_MATCH_MODE = "exact";
	private static final String PREFIX_MATCH_MODE = "prefix";
	private static final String INFIX_MATCH_MODE = "infix";
	
	private static final int INDEX_TERM_MIN_LENGHTH = 3; // anything less than three chars does not work with trigrams, and hardly bears any meaning
	private static final int INDEX_TERM_MAX_LENGHTH = 128; // this prevents file name length problems, and, seriously, who will search for terms of this length ...
	
	private TreeSet stopWords = new TreeSet(String.CASE_INSENSITIVE_ORDER);
	
	private TreeMap trigramsToIndexTerms = new TreeMap();
	
	private HashMap termIndexCache = new LinkedHashMap();
	private int termIndexCacheLimit = 512;
	
	private HashSet dirtyTermIndexQueue = new LinkedHashSet();
	private HashMap pendingTermIndexEntryCache = new LinkedHashMap();
	
	private HashSet invalidDocumentNumbers = new HashSet();
	
	private File indexRootPath;
	private IndexUpdater indexUpdater;
	private AsynchronousWorkQueue indexUpdaterMonitor;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#getIndexName()
	 */
	public String getIndexName() {
		return "fullText";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractIndexer#init()
	 */
	public void init() {
		System.out.println("FullTextIndexer: initializing ...");
		
		//	load configuration
		File configFile = new File(this.dataPath, "config.cnfg");
		if (configFile.exists()) {
			Settings config = Settings.loadSettings(configFile);
			String indexRootPath = config.getSetting("indexRootPath", "IndexData");
			while (indexRootPath.startsWith("./"))
				indexRootPath = indexRootPath.substring("./".length());
			this.indexRootPath = (((indexRootPath.indexOf(":\\") == -1) && (indexRootPath.indexOf(":/") == -1) && !indexRootPath.startsWith("/")) ? new File(this.dataPath, indexRootPath) : new File(indexRootPath));
			try {
				this.termIndexCacheLimit = Integer.parseInt(config.getSetting("termIndexCacheLimit", ("" + this.termIndexCacheLimit)).trim());
			} catch (NumberFormatException nfe) {}
		}
		
		//	make sure we have an index root path
		if (this.indexRootPath == null)
			this.indexRootPath = this.dataPath;
		if (!this.indexRootPath.exists())
			this.indexRootPath.mkdirs();
		
		//	load stop words
		System.out.println("  - loading stop words ...");
		try {
			BufferedReader swBr = new BufferedReader(new InputStreamReader(new FileInputStream(new File(this.dataPath, "stopWords.txt")), "UTF-8"));
			for (String swLine; (swLine = swBr.readLine()) != null;) {
				swLine = swLine.trim();
				if (swLine.startsWith("//"))
					continue;
				this.stopWords.add(this.normalizeTerm(swLine));
			}
			swBr.close();
			System.out.println("  - got " + this.stopWords.size() + " stop words");
		}
		catch (IOException ioe) {
			System.out.println("  - " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading stop word list");
			ioe.printStackTrace(System.out);
		}
		
		//	load invalid document numbers
		System.out.println("  - loading invalidated document numbers ...");
		File invalidDocNrFile = new File(this.dataPath, "invalid");
		if (invalidDocNrFile.length() > 0) { // replace existing file
			try {
				DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(invalidDocNrFile)));
				synchronized (this.invalidDocumentNumbers) {
					while (dis.available() >= 4)
						this.invalidDocumentNumbers.add(new Long(dis.readLong()));
				}
				dis.close();
				System.out.println("  - got " + this.invalidDocumentNumbers.size() + " invalid document numbers");
			}
			catch (FileNotFoundException fnfe) {
				//	ignore this exception, it simply means there are no invalid document numbers so far
				System.out.println("  - got no invalid document numbers");
			}
			catch (IOException ioe) {
				System.out.println("  - " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading invalid docNr file.");
				ioe.printStackTrace(System.out);
			}
		}
		
		//	start index updater thread
		System.out.println("  - starting index updater ...");
		this.indexUpdater = new IndexUpdater();
		this.indexUpdater.start();
		this.indexUpdaterMonitor = new AsynchronousWorkQueue("FullTextIndexUpdater") {
			public String getStatus() {
				return (this.name + ": " + dirtyTermIndexQueue.size() + " index files to sort, " + pendingTermIndexEntryCache.size() + " ones to update");
			}
		};
		System.out.println("  - index updater started");
		
		System.out.println("  - FullTextIndexer initialized");
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractIndexer#exit()
	 */
	public void exit() {
		
		//	shut down index updater
		this.indexUpdaterMonitor.dispose();
		this.indexUpdater.shutdown();
		try {
			this.indexUpdater.join();
		} catch (InterruptedException ie) {}
		
		//	store invalidated document numbers
		try {
//			File invalidDocNrFile = new File(this.dataPath, "invalid");
			File invalidDocNrFile = new File(this.indexRootPath, "invalid");
			if (invalidDocNrFile.length() > 0) { // replace existing file
				File oldIdnf = new File(invalidDocNrFile.getAbsolutePath() + ".old");
				if (oldIdnf.exists()) oldIdnf.delete();
				invalidDocNrFile.renameTo(new File(invalidDocNrFile.getAbsolutePath() + ".old"));
//				invalidDocNrFile = new File(this.dataPath, "invalid");
				invalidDocNrFile = new File(this.indexRootPath, "invalid");
			}
			invalidDocNrFile.createNewFile();
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(invalidDocNrFile, true)));
			synchronized (this.invalidDocumentNumbers) {
				for (Iterator it = this.invalidDocumentNumbers.iterator(); it.hasNext();)
					dos.writeLong(((Long) it.next()).longValue());
			}
			dos.flush();
			dos.close();
		}
		catch (IOException ioe) {
			System.out.println("FullTextIndexer: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while storing invalid docNr file.");
			ioe.printStackTrace(System.out);
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractIndexer#getFieldGroup()
	 */
	protected SearchFieldGroup getFieldGroup() {
		SearchFieldRow fullTextSfr = new SearchFieldRow();
		fullTextSfr.addField(new SearchField(FULL_TEXT_QUREY, "Search Terms", null, 3));
		SearchField matchMode = new SearchField(MATCH_MODE, "Search Mode", null, PREFIX_MATCH_MODE, SearchField.SELECT_TYPE);
		matchMode.addOption(EXACT_MATCH_MODE, "Exact Match");
		matchMode.addOption(PREFIX_MATCH_MODE, "Prefix Match");
		matchMode.addOption(INFIX_MATCH_MODE, "Infix Match");
		fullTextSfr.addField(matchMode);
		
		SearchFieldGroup sfg = new SearchFieldGroup(this.getIndexName(), "Full Text Index", "Use these fields to search the document text (terms shorter than 3 letters will be ignored)", null);
		sfg.addFieldRow(fullTextSfr);
		
		return sfg;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#processQuery(de.uka.ipd.idaho.goldenGateServer.srs.Query)
	 */
	public QueryResult processQuery(Query query) {
		this.host.logActivity("FullTextIndexer processing query '" + query.toString() + "'");
		
		//	get raw query
		String termString = query.getValue(FULL_TEXT_QUREY);
		this.host.logActivity("  - processing query '" + termString + "'");
		
		//	void raw query
		if ((termString == null) || (termString.trim().length() == 0))
			return null;
		
		//	extract query terms
		TokenSequence termTs = Gamta.newTokenSequence(termString, Gamta.NO_INNER_PUNCTUATION_TOKENIZER);
		CountingSet terms = new CountingSet(new LinkedHashMap());
		for (int t = 0; t < termTs.size(); t++) {
			String term = termTs.valueAt(t);
			if (Gamta.isWord(term) && (term.length() >= INDEX_TERM_MIN_LENGHTH) && !this.stopWords.contains(term))
				terms.add(this.normalizeTerm(term));
			else if (Gamta.isNumber(term) && (term.length() >= INDEX_TERM_MIN_LENGHTH))
				terms.add(this.normalizeTerm(term));
		}
		this.host.logActivity("  - got terms '" + terms.toString() + "'");
		
		//	no terms given
		if (terms.isEmpty())
			return null;
		
		//	read match mode
		String matchMode = query.getValue(MATCH_MODE, PREFIX_MATCH_MODE);
		
		//	process query
		QueryResult result = null;
		for (Iterator tit = terms.iterator(); tit.hasNext();) {
			String term = ((String) tit.next());
			this.host.logActivity("  - doing query term '" + term + "'");
			
			//	assemble result for current term/infix
			QueryResult termOrInfixResult = null;
			
			//	do exact match
			if (EXACT_MATCH_MODE.equals(matchMode)) {
				
				//	get index for term
				TermIndex termIndex = this.getTermIndex(term);
				
				//	produce query result
				termOrInfixResult = new QueryResult();
				if (termIndex != null) {
					for (int e = 0; e < termIndex.size(); e++)
						termOrInfixResult.addResultElement(new QueryResultElement(termIndex.entries[e].docNr, 1.0));
				}
			}
			
			//	do wildcard lookup
			else {
				String infix = term;
				HashSet infixBearingTerms = null;
				
				//	find index terms bearing wildcard match term as infix
				for (int i = 0; i <= (infix.length() - 3); i++) {
					HashSet trigramTermSet = ((HashSet) this.trigramsToIndexTerms.get(infix.substring(i, (i+3))));
					
					//	we have no index terms including this trigram
					if (trigramTermSet == null) {
						if (infixBearingTerms != null)
							infixBearingTerms.clear();
						break; // could return empty result right here in boolean AND mode, but not in VSR or other advanced modes
					}
					
					//	first trigram, initialize intersecting
					if (infixBearingTerms == null)
						infixBearingTerms = new HashSet(trigramTermSet);
					
					//	do intersection with all further sets
					else infixBearingTerms.retainAll(trigramTermSet);
				}
				
				//	iterate over terms bearing current infix
				if (infixBearingTerms == null)
					continue;
				for (Iterator it = infixBearingTerms.iterator(); it.hasNext();) {
					String infixBearingTerm = ((String) it.next());
					
					//	check if actual suffix matches (trigrams are indicators, but no secure evidence)
					if (INFIX_MATCH_MODE.equals(matchMode) ? (infixBearingTerm.indexOf(infix) != -1) : infixBearingTerm.startsWith(infix)) {
						this.host.logActivity("    - got " + matchMode + " match term '" + infixBearingTerm + "'");
						
						//	get index for wildcard matched term
						TermIndex infixBearingTermIndex = this.getTermIndex(infixBearingTerm);
						
						//	produce query result
						QueryResult infixBearingTermResult = new QueryResult(false);
						if (infixBearingTermIndex != null)
							for (int e = 0; e < infixBearingTermIndex.size(); e++)
								infixBearingTermResult.addResultElement(new QueryResultElement(infixBearingTermIndex.entries[e].docNr, 1.0));
						
						//	union with results for other terms bearing current infix
						if (termOrInfixResult == null)
							termOrInfixResult = infixBearingTermResult;
						else termOrInfixResult = QueryResult.merge(termOrInfixResult, infixBearingTermResult, QueryResult.USE_MAX, 0);
					}
				}
			}
			
			this.host.logActivity("  - got " + ((termOrInfixResult == null) ? "no" : ("" + termOrInfixResult.size())) + " results for '" + term + "'");
			
			//	combine result for current term/infix with results for other terms/infixes (boolean AND, vector space measure, etc)
			if (result == null)
				result = ((termOrInfixResult == null) ? new QueryResult() : termOrInfixResult); // empty result will cause merge to be fast and final result to be empty in boolean mode
			else result = QueryResult.join(result, termOrInfixResult, QueryResult.USE_MIN, 0);
		}
		
		//	return final result
		return result;
	}
	
	private TermIndex getTermIndex(String term) {
		TermIndex termIndex;
		
		//	have to synchronize cache lookup in order to avoid problems with LRU ordering in the face of concurrency
		synchronized (this.termIndexCache) {
			
			//	do cache lookup
			if (this.termIndexCache.containsKey(term)) {
				
				//	have to remove and re-add index to maintain LRU ordering in linked hash map
				termIndex = ((TermIndex) this.termIndexCache.remove(term));
				this.termIndexCache.put(term, termIndex);
				return termIndex;
			}
		}
		
		//	cache miss, load index from disc
		termIndex = new TermIndex(term);
		try {
			//	get file
			File indexFile = this.getIndexFile(term, false);
			if (indexFile == null)
				return termIndex;
			
			//	read file
			DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile)));
			
			//	read index data
			while (dis.available() >= 10) {
				long docNr = dis.readLong();
				byte tf = dis.readByte();
				byte lenLog = dis.readByte();
				
				//	check if index contains invalid entries
				synchronized (this.invalidDocumentNumbers) {
					
					//	if so, add index to dirty list so file is rewritten
					if (this.invalidDocumentNumbers.contains(new Long(docNr)))
						synchronized (this.dirtyTermIndexQueue) {
							this.dirtyTermIndexQueue.add(termIndex);
						}
					
					//	check if index entries in file are sorted, if not, add index to dirty list so file is rewritten (may happen if updates are appended)  
					else {
						int size = termIndex.size();
						if (
								termIndex.addEntry(new TermIndexEntry(docNr, tf, lenLog))// ==> sort order violated
								||
								(size == termIndex.size())// ==> duplicate entry (adding did not increase index size)
							) 
							synchronized (this.dirtyTermIndexQueue) {
								this.dirtyTermIndexQueue.add(termIndex);
							}
					}
				}
			}
			dis.close();
			
			//	add pending entries
			synchronized (this.pendingTermIndexEntryCache) {
				if (this.pendingTermIndexEntryCache.containsKey(term)) {
					
					//	add pending entries to index
					LinkedList pendingEntries = ((LinkedList) this.pendingTermIndexEntryCache.get(term));
					boolean modified = false;
					int size = termIndex.size();
					for (Iterator peit = pendingEntries.iterator(); peit.hasNext();) {
						if (termIndex.addEntry((TermIndexEntry) peit.next())) {
							modified = true;
							size = termIndex.size();
						}
						else if (size == termIndex.size()) // remove existing entries so they are not written to file
							peit.remove();
					}
					
					//	if new entries contradict sorting order of index file, add index to dirty list and remove pending entries
					if (modified)
						synchronized (this.dirtyTermIndexQueue) {
							this.dirtyTermIndexQueue.add(termIndex);
							this.pendingTermIndexEntryCache.remove(term);
						}
				}
			}
		}
		catch (IOException ioe) {
			this.host.logError("FullTextIndexer: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading index file for '" + term + "'");
			this.host.logError(ioe);
		}
		
		//	have to synchronize cache modification in order to avoid problems in the face of concurrency
		synchronized (this.termIndexCache) {
			
			//	add index to cache
			this.termIndexCache.put(term, termIndex);
			
			//	test cache size limit
			if (this.termIndexCache.size() > this.termIndexCacheLimit) {
				Iterator it = this.termIndexCache.keySet().iterator();
				while ((this.termIndexCache.size() > this.termIndexCacheLimit) && it.hasNext()) {
					it.next();
					it.remove();
				}
			}
		}
		
		//	return the index
		return termIndex;
	}
	
	private File getIndexFile(String term, boolean create) throws IOException {
		File indexFile = this.indexRootPath;
		for (int l = 0; l < Math.min(term.length(), 3); l++)
			indexFile = new File(indexFile, term.substring(l, (l+1)));
		
		if (!indexFile.exists()) {
			if (create)
				indexFile.mkdirs();
			else return null;
		}
		
		indexFile = new File(indexFile, ("idx-" + term)); // prevent using forbidden file names like 'con'
		if (!indexFile.exists()) {
			if (create) try {
				indexFile.createNewFile();
			}
			catch (IOException ioe) {
				if ("FileNameTooLong".equalsIgnoreCase(ioe.getMessage().replaceAll("\\s+", ""))) // simply cut overly long file names
					return this.getIndexFile(term.substring(0, (term.length()-1)), create);
				else throw ioe;
			}
			else return null;
		}
		
		return indexFile;
	}
	
	/*
TODO Keep SRS full text index entries as byte[] ...
... and decode data akin to taxonomic epithet index
	 */
	private static class TermIndex {
		private static final int CAPACITY_INCREMENT = 16;
		final String term;
		TermIndexEntry[] entries = new TermIndexEntry[CAPACITY_INCREMENT];
		int size = 0;
		TermIndex(String term) {
			this.term = term;
		}
		synchronized int size() {
			return this.size;
		}
		/*
		 * returns true is the new entry was inserted in the middle instead of
		 * appended ==> sort order in file invalid
		 */
		synchronized boolean addEntry(TermIndexEntry entry) {
			
			//	check if append operation in order to save binary search
			if ((this.size != 0) && (this.entries[this.size - 1].docNr >= entry.docNr)) {
				
				//	check if entry already present
				int index = this.find(entry.docNr);
				
				//	entry is present ==> update
				if (index != -1) {
					boolean modified = ((this.entries[index].tf != entry.tf) || (this.entries[index].docLenLog != entry.docLenLog));
					if (modified)
						this.entries[index] = entry;
					return modified;
				}
			}
			
			//	have to extend content array (linear increment, for updates are not as frequent)
			if (this.size == this.entries.length) {
				TermIndexEntry[] newEntries = new TermIndexEntry[this.entries.length + CAPACITY_INCREMENT];
				System.arraycopy(this.entries, 0, newEntries, 0, this.entries.length);
				this.entries = newEntries;
			}
			
			//	append new entry
			this.entries[this.size++] = entry;
			
			/*
			 * have to re-sort ==> one backward pass of bubble sort will do, for
			 * all but the last two elements are guaranteed to be sorted (would
			 * have to shift array anyway for insertion)
			 */
			//	TODO use binary search and System.arraycopy instead (swapping gets a bit more finicky with plain arrays, and arraycopy is native)
			boolean modified = false;
			for (int i = (this.size - 1); i > 0; i--) {
				if (this.entries[i-1].docNr < this.entries[i].docNr)
					break; // break, we're done
				else { // swap current elements
					entry = this.entries[i-1]; // use entry as temp
					this.entries[i-1] = this.entries[i];
					this.entries[i] = entry;
					modified = true;
				}
			}
			return modified;
		}
		private int find(long docNr) {
			int low = 0;
			int high = this.size - 1;
			long midDocNr;
			while (low <= high) {
				int middle = ((low + high) / 2);
				midDocNr = this.entries[middle].docNr;
				if (midDocNr < docNr)
					low = middle + 1;
				else if (midDocNr > docNr)
					high = middle - 1;
				else return middle;
			}
			return -1;
		}
	}
	
	private static class TermIndexEntry {
		final long docNr;
		final byte tf;
		final byte docLenLog;
		TermIndexEntry(long docNr, byte tf, byte lenLog) {
			this.docNr = docNr;
			this.tf = tf;
			this.docLenLog = lenLog;
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#addSearchAttributes(de.uka.ipd.idaho.gamta.Annotation)
	 */
	public void addSearchAttributes(Annotation annotation) {
		//	no search links for full text
	}
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#getIndexEntries(de.uka.ipd.idaho.goldenGateServer.srs.Query, long[], boolean)
//	 */
//	public IndexResult getIndexEntries(Query query, long[] docNumbers, boolean sort) {
//		return null; //	no support for index search
//	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#getIndexEntries(de.uka.ipd.idaho.goldenGateServer.srs.Query, long[], boolean)
	 */
	public IndexResult getIndexEntries(Query query, long docNumber, boolean sort) {
		return null; //	no support for index search
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#doThesaurusLookup(de.uka.ipd.idaho.goldenGateServer.srs.Query)
	 */
	public ThesaurusResult doThesaurusLookup(Query query) {
		return null; // no support for thesaurus search
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#filterIndexEntries(de.uka.ipd.idaho.goldenGateServer.srs.Query, de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult, boolean)
	 */
	public IndexResult filterIndexEntries(Query query, IndexResult allIndexEntries, boolean sort) {
		return null; //	no support for index search
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#index(de.uka.ipd.idaho.gamta.QueriableAnnotation, long)
	 */
//	public void index(QueriableAnnotation doc, long docNr) {
	public IndexResult index(QueriableAnnotation doc, long docNr) {
		
		//	mark document number as valid
		synchronized (this.invalidDocumentNumbers) {
			this.invalidDocumentNumbers.remove(new Long(docNr));
		}
		
		//	catch empty document
		if (doc.size() == 0)
			return null;
		
		//	extract index terms
		TokenSequence termTs = Gamta.newTokenSequence(doc, Gamta.NO_INNER_PUNCTUATION_TOKENIZER);
		CountingSet terms = new CountingSet(new TreeMap(String.CASE_INSENSITIVE_ORDER));
		for (int t = 0; t < termTs.size(); t++) {
			String term = termTs.valueAt(t);
			if (Gamta.isWord(term) && (term.length() >= INDEX_TERM_MIN_LENGHTH) && (term.length() <= INDEX_TERM_MAX_LENGHTH) && !this.stopWords.contains(term))
				terms.add(this.normalizeTerm(term));
			else if (Gamta.isNumber(term) && (term.length() >= INDEX_TERM_MIN_LENGHTH))
				terms.add(this.normalizeTerm(term));
		}
		
		//	compute doc size
		byte docLenLog = ((byte) Math.min(Byte.MAX_VALUE, ((int) Math.round(Math.log(doc.size()) / Math.log(2)))));
		
		//	index terms
		for (Iterator tit = terms.iterator(); tit.hasNext();) {
			String term = ((String) tit.next());
			if (term.trim().length() == 0)
				continue;
			
			//	map trigrams to terms
			for (int t = 0; t <= (term.length() - 3); t++) {
				String trigram = term.substring(t, (t+3));
				HashSet trigramTermSet = ((HashSet) this.trigramsToIndexTerms.get(trigram));
				if (trigramTermSet == null) {
					trigramTermSet = new HashSet();
					this.trigramsToIndexTerms.put(trigram, trigramTermSet);
				}
				trigramTermSet.add(term);
			}
			
			//	get term frequency
			byte tf = ((byte) Math.min(Byte.MAX_VALUE, terms.getCount(term)));
			
			//	add entries to cached indices (update from pending entries happens only on loading from disc)
			synchronized (this.termIndexCache) {
				if (this.termIndexCache.containsKey(term)) {
					TermIndex termIndex = ((TermIndex) this.termIndexCache.get(term));
					int size = termIndex.size;
					
					//	new entry violates sort order
					if (termIndex.addEntry(new TermIndexEntry(docNr, tf, docLenLog)))
						synchronized (this.dirtyTermIndexQueue) {
							this.dirtyTermIndexQueue.add(termIndex);
							tf = 0; // indicate index is dirty, no need for pending entry list
						}
					
					//	entry re-added without changing anything, dont't write entry to index file
					else if (size == termIndex.size)
						tf = 0;
				}
			}
			
			//	enqueue new entries for persistent storage (if not whole index is scheduled for rewrite)
			if (tf == 0)
				continue;
			synchronized (this.pendingTermIndexEntryCache) {
				LinkedList pendingEntries = ((LinkedList) this.pendingTermIndexEntryCache.get(term));
				if (pendingEntries == null) {
					pendingEntries = new LinkedList();
					this.pendingTermIndexEntryCache.put(term, pendingEntries);
				}
				pendingEntries.add(new TermIndexEntry(docNr, tf, docLenLog));
			}
		}
		
		return null; // still no support for index search
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.Indexer#deleteDocument(long)
	 */
	public void deleteDocument(long docNr) {
		/*
		 * Going through all inverted lists in order to remove document number
		 * would be too costly. Add document number to list of deleted
		 * documents, and ignore document number in future searches. Only in
		 * next maintenance, reorganize index lists and remove the entries for
		 * all documents marked as deleted.
		 */
		synchronized (this.invalidDocumentNumbers) {
			this.invalidDocumentNumbers.add(new Long(docNr));
		}
	}
	
	private class IndexUpdater extends Thread {
		private boolean keepRunning = true;
		private Object lock = new Object();
		public void run() {
			
			//	initialize wildcard matching helper
			host.logInfo("  - indexing index terms by trigrams ...");
			int indexTermCount = 0;
			File[] idxFolders1 = indexRootPath.listFiles(new FileFilter() {
				public boolean accept(File file) {
					return file.isDirectory();
				}
			});
			for (int f1 = 0; f1 < idxFolders1.length; f1++) {
				File[] idxFolders2 = idxFolders1[f1].listFiles(new FileFilter() {
					public boolean accept(File file) {
						return file.isDirectory();
					}
				});
				for (int f2 = 0; f2 < idxFolders2.length; f2++) {
					File[] idxFolders3 = idxFolders2[f2].listFiles(new FileFilter() {
						public boolean accept(File file) {
							return file.isDirectory();
						}
					});
					for (int f3 = 0; f3 < idxFolders3.length; f3++) {
						File[] idxFiles = idxFolders3[f3].listFiles(new FileFilter() {
							public boolean accept(File file) {
								return (file.isFile() && file.getName().startsWith("idx-") && !file.getName().endsWith(".old"));
							}
						});
						for (int f = 0; f < idxFiles.length; f++) {
							String term = idxFiles[f].getName().substring("idx-".length());
							indexTermCount ++;
							for (int t = 0; t <= (term.length() - 3); t++) {
								String trigram = term.substring(t, (t+3));
								HashSet trigramTermSet = ((HashSet) trigramsToIndexTerms.get(trigram));
								if (trigramTermSet == null) {
									trigramTermSet = new HashSet();
									trigramsToIndexTerms.put(trigram, trigramTermSet);
								}
								trigramTermSet.add(term);
							}
						}
					}
				}
			}
			host.logInfo("  - got " + trigramsToIndexTerms.size() + " trigrams from " + indexTermCount + " index terms");
			
			//	get ready to work
			while (this.keepRunning) {
				
				//	sleep a little
				try {
					synchronized (this.lock) {
						this.lock.wait(1000);
					}
				} catch (InterruptedException ie) {}
				
				//	do the pending work
				while (this.keepRunning && this.doWork())
					try {
						Thread.sleep(100);
					} catch (InterruptedException ie) {}
			}
			
			//	work off the rest before shutdown
			host.logInfo("IndexUpdater shutting down, working off remaining queues ...");
			while (this.doWork()) {}
		}
		
		private boolean doWork() {
			
			//	write dirty term index
			synchronized (dirtyTermIndexQueue) {
				if (dirtyTermIndexQueue.size() != 0) {
					Iterator it = dirtyTermIndexQueue.iterator();
					TermIndex termIndex = ((TermIndex) it.next());
					synchronized (termIndex) {
						try {
							host.logDebug("  - storing index file for '" + termIndex.term + "'");
							File termIndexFile = getIndexFile(termIndex.term, true);
							if (termIndexFile.length() > 0) { // replace existing file
								File oldTif = new File(termIndexFile.getAbsolutePath() + ".old");
								if (oldTif.exists())
									oldTif.delete();
								termIndexFile.renameTo(new File(termIndexFile.getAbsolutePath() + ".old"));
								termIndexFile = getIndexFile(termIndex.term, true);
							}
							host.logDebug("    - writing " + termIndex.size + " entries");
							DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(termIndexFile, false)));
							for (int e = 0; e < termIndex.size; e++) {
								TermIndexEntry tie = termIndex.entries[e];
								dos.writeLong(tie.docNr);
								dos.writeByte(tie.tf);
								dos.writeByte(tie.docLenLog);
							}
							dos.flush();
							dos.close();
							it.remove();
							host.logDebug("    - done");
							return true;
						}
						catch (IOException ioe) {
							host.logError("FullTextIndexer: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while storing index file for '" + termIndex.term + "'");
							host.logError(ioe);
						}
					}
				}
			}
			
			//	add pending entries to term index
			synchronized (pendingTermIndexEntryCache) {
				if (pendingTermIndexEntryCache.size() != 0) {
					Iterator it = pendingTermIndexEntryCache.keySet().iterator();
					String term = it.next().toString();
					LinkedList pendingEntries = ((LinkedList) pendingTermIndexEntryCache.get(term));
					try {
						host.logDebug("  - extending index file for '" + term + "'");
						File termIndexFile = getIndexFile(term, true);
						host.logDebug("    - writing " + pendingEntries.size() + " entries");
						DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(termIndexFile, true)));
						while (pendingEntries.size() != 0) {
							TermIndexEntry tie = ((TermIndexEntry) pendingEntries.removeFirst());
							dos.writeLong(tie.docNr);
							dos.writeByte(tie.tf);
							dos.writeByte(tie.docLenLog);
						}
						dos.flush();
						dos.close();
						it.remove();
						host.logDebug("    - done");
						return true;
					}
					catch (IOException ioe) {
						host.logError("FullTextIndexer: " + ioe.getClass().getName() + " (" + ioe.getMessage() + ") while extending index file for '" + term + "'");
						host.logError(ioe);
					}
				}
			}
			
			//	nothing to do
			return false;
		}
		private void shutdown() {
			this.keepRunning = false;
			synchronized (this.lock) {
				this.lock.notify();
			}
		}
	}
	
	private String normalizeTerm(String term) {
		/*
		 * TODO: maybe add stemming or even iterative stemming, though results
		 * for non-English terms might be a problem, as may be typos
		 */
		StringBuffer sb = new StringBuffer();
		for (int c = 0; c < term.length(); c++)
			sb.append(Character.toLowerCase(StringUtils.getBaseChar(term.charAt(c))));
		term = sb.toString();
		int len = term.length();
		while (len > (term = StringUtils.porterStem(term)).length())
			len = term.length();
		return term;
	}
}