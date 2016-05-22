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
 *     * Neither the name of the Universität Karlsruhe (TH) / KIT nor the
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

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.srs.data.DocumentResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.IndexResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult;
import de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResultElement;
import de.uka.ipd.idaho.goldenGateServer.srs.data.ThesaurusResult;

/**
 * Client for searching documents in the collection indexed by a GoldenGATE SRS
 * server component, extended to allow paged data delivery. The latter is
 * emulated on top of the cache; namely, the whole result is fetched and
 * cached, and then delivered page by page based on the cached data.
 * 
 * @author sautter
 */
public class GoldenGateSrsClientPaged extends GoldenGateSrsClient {
	
	/**
	 * This interface is implemented by the paged result classes.
	 * 
	 * @author sautter
	 */
	public static interface PagedResult {
		
		/**
		 * Check the index of the first result element in this page. This number
		 * is equal to the sum of the page sizes used so far, no matter if the
		 * respective pages have been read to their end or not.
		 * @return the index of the first result element in this page
		 */
		public abstract int getStart();
		
		/**
		 * Check the number of result elements read so far from this page. This
		 * number is at most the limit specified when the page was created.
		 * @return the number of result elements read so far from this page
		 */
		public abstract int getElementsRead();
		
		/**
		 * Check the upper bound for the number of result elements on this page.
		 * This number is equal to the limit specified when the page was
		 * created.
		 * @return the upper bound for the number of result elements on this
		 *         page
		 */
		public abstract int getSize();
		
		/**
		 * Check the total number of result elements read so far. This number is
		 * equal to the page start plus the elements read so far from this page.
		 * @return the total number of result elements read so far
		 */
		public abstract int getElementsReadTotal();
		
		/**
		 * Check the upper bound for the index of the last result element in
		 * this page. This number is equal to the sum of the page start and the
		 * page size.
		 * @return the upper bound for the index of the last result element in
		 *         this page
		 */
		public abstract int getLimit();
		
		/**
		 * Retrieve the next part of the paged result. Invoking this method
		 * while this page has elements left will proceed to the end of the
		 * page, skipping any remaining elements. The returned page has at most
		 * the &lt;limit&gt; result elements. If no elements remain for a next
		 * page, this method returns null. If not, the result actually returned
		 * is an instance of the same class as the one implementing the method.
		 * @param pageSize the number of elements for the next page
		 * @return a paged result holding (at most) the &lt;limit&gt; next
		 *         elements of the result
		 */
		public abstract PagedResult getNextPage(int pageSize) throws IOException;
	}
	
	private static abstract class PagedResultData {
		
		private SrsSearchResult data;
		
		private int start = 0;
		private int elementsRead = 0;
		private int pageSize;
		
		private CacheAccessData cad;
		
		PagedResultData(SrsSearchResult data, int start, int pageSize, CacheAccessData cad) {
			this.data = data;
			this.start = start;
			this.pageSize = pageSize;
			this.cad = cad;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#hasNextElement()
		 */
		boolean hasNextElement() {
			return (this.data.hasNextElement() && (this.elementsRead < this.pageSize));
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#getNextElement()
		 */
		SrsSearchResultElement getNextElement() {
			if (this.hasNextElement()) {
				SrsSearchResultElement re = this.data.getNextElement();
				this.elementsRead++;
				
				if ((this.elementsRead == this.pageSize) && (this.cad != null) && this.data.hasNextElement())
					this.switchToCache();
				
				return re;
			}
			else return null;
		}
		
		private void switchToCache() {
			try {
				
				//	read all remaining elements of backing result so they are cached
				while (this.data.hasNextElement())
					this.data.getNextElement();
				
				//	switch to cache reader
				this.data = this.getCacheBasedResult(this.cad.getCacheReader());
				
				//	consume cached data already seen
				int skip = 0;
				while (this.data.hasNextElement() && (skip++ < this.pageSize))
					this.data.getNextElement();
			}
			catch (IOException ioe) {
				System.out.println("GoldenGateSrsClient.PagedResultData: file cache transition error " + ioe.getClass().getName() + " (" + ioe.getMessage() + ")");
				ioe.printStackTrace(System.out);
			}
		}
		
		/**
		 * Method to allow result classes wrapping their own implementation
		 * around the cache reader
		 */
		protected abstract SrsSearchResult getCacheBasedResult(Reader reader) throws IOException;
		
		int getStart() {
			return this.start;
		}
		
		int getElementsRead() {
			return this.elementsRead;
		}
		
		int getSize() {
			return this.pageSize;
		}
		
		int getElementsReadTotal() {
			return (this.start + this.elementsRead);
		}
		
		int getLimit() {
			return (this.start + this.pageSize);
		}
		
		PagedResultData getNextPage(int pageSize) throws IOException {
			while (this.hasNextElement())
				this.getNextElement();
			
			return (this.data.hasNextElement() ? new PagedResultData(this.data, (this.start + this.pageSize), pageSize, null) {
				protected SrsSearchResult getCacheBasedResult(Reader reader) throws IOException {
					// returning null is OK here, since file cache transition is finished at this point
					return null;
				}
			} : null);
		}
	}
	
	/**
	 * Constructor
	 * @param serverConnection the ServerConnection to use for communication
	 *            with the backing SRS
	 */
	public GoldenGateSrsClientPaged(ServerConnection serverConnection) {
		super(serverConnection);
	}
	
	/**
	 * Search for complete documents, getting a paged result.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the documents and their
	 *            annotations?
	 * @param pageSize the number of elements for the first page of the result
	 * @return the result of the search
	 * @throws IOException
	 */
	public PagedDocumentResult searchDocumentsPaged(Properties parameters, boolean markSearchables, int pageSize) throws IOException {
		return this.searchDocumentsPaged(parameters, markSearchables, pageSize, true);
	}
	
	/**
	 * Search for complete documents, getting a paged result.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the documents and their
	 *            annotations?
	 * @param pageSize the number of elements for the first page of the result
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search
	 * @throws IOException
	 */
	public PagedDocumentResult searchDocumentsPaged(Properties parameters, boolean markSearchables, int pageSize, boolean allowCache) throws IOException {
		
		//	prepare passing cache access data reference
		CacheAccessData[] cad = new CacheAccessData[1];
		
		//	get backing result
		DocumentResult dr = this.searchDocuments(parameters, markSearchables, allowCache, cad);
		
		//	return first page
		return new PagedDocumentResult(dr, new PagedResultData(dr, 0, pageSize, cad[0]) {
			protected SrsSearchResult getCacheBasedResult(Reader reader) throws IOException {
				return DocumentResult.readDocumentResult(reader);
			}
		});
	}
	
	/**
	 * Search for document details that are especially annotated, getting a
	 * paged result. You can specify the names of the indices whose details you
	 * are interested in the INDEX_NAME parameter as a comma separated list, not
	 * specifying an index name will result in all details being returned.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the documents and their
	 *            annotations?
	 * @param pageSize the number of elements for the first page of the result
	 * @return the result of the search
	 * @throws IOException
	 */
	public PagedDocumentResult searchDocumentDetailsPaged(Properties parameters, boolean markSearchables, int pageSize) throws IOException {
		return this.searchDocumentDetailsPaged(parameters, markSearchables, pageSize, true);
	}
	
	/**
	 * Search for document details that are especially annotated, getting a
	 * paged result. You can specify the names of the indices whose details you
	 * are interested in the INDEX_NAME parameter as a comma separated list, not
	 * specifying an index name will result in all details being returned.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the documents and their
	 *            annotations?
	 * @param pageSize the number of elements for the first page of the result
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search
	 * @throws IOException
	 */
	public PagedDocumentResult searchDocumentDetailsPaged(Properties parameters, boolean markSearchables, int pageSize, boolean allowCache) throws IOException {
		
		//	prepare passing cache access data reference
		CacheAccessData[] cad = new CacheAccessData[1];
		
		//	get backing result
		DocumentResult dr = this.searchDocumentDetails(parameters, markSearchables, allowCache, cad);
		
		//	return first page
		return new PagedDocumentResult(dr, new PagedResultData(dr, 0, pageSize, cad[0]) {
			protected SrsSearchResult getCacheBasedResult(Reader reader) throws IOException {
				return DocumentResult.readDocumentResult(reader);
			}
		});
	}
	
	/**
	 * Search for basic document meta data, like title and author, getting a
	 * paged result.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to eventual sub result
	 *            annotations?
	 * @param pageSize the number of elements for the first page of the result
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search (the document of the
	 *         DocumentResultElement objects will be null, only attributes are
	 *         used in this search mode)
	 * @throws IOException
	 */
	public PagedDocumentResult searchDocumentDataPaged(Properties parameters, boolean markSearchables, int pageSize) throws IOException {
		return this.searchDocumentDataPaged(parameters, markSearchables, pageSize, true);
	}
	
	/**
	 * Search for basic document meta data, like title and author, getting a
	 * paged result.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to eventual sub result
	 *            annotations?
	 * @param pageSize the number of elements for the first page of the result
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search (the document of the
	 *         DocumentResultElement objects will be null, only attributes are
	 *         used in this search mode)
	 * @throws IOException
	 */
	public PagedDocumentResult searchDocumentDataPaged(Properties parameters, boolean markSearchables, int pageSize, boolean allowCache) throws IOException {
		
		//	prepare passing cache access data reference
		CacheAccessData[] cad = new CacheAccessData[1];
		
		//	get backing result
		DocumentResult dr = this.searchDocumentData(parameters, markSearchables, allowCache, cad);
		
		//	return first page
		return new PagedDocumentResult(dr, new PagedResultData(dr, 0, pageSize, cad[0]) {
			protected SrsSearchResult getCacheBasedResult(Reader reader) throws IOException {
				return DocumentResult.readDocumentDataResult(reader);
			}
		});
	}
	
	/**
	 * Search for plain document IDs. The elements of the returned document
	 * result will be plain pairs of document ID and relevance. This type of
	 * query is useful in situations where an application intends to retrieve
	 * the complete documents after the search, e.g. via the getXmlDocument()
	 * method, use a local cache, etc.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param pageSize the number of elements for the first page of the result
	 * @return the result of the search (the document of the
	 *         DocumentResultElement objects will be null, only document ID and
	 *         relevance are used in this search mode)
	 * @throws IOException
	 */
	public PagedDocumentResult searchDocumentIDsPaged(Properties parameters, int pageSize) throws IOException {
		return this.searchDocumentIDsPaged(parameters, pageSize, true);
	}
	
	/**
	 * Search for plain document IDs. The elements of the returned document
	 * result will be plain pairs of document ID and relevance. This type of
	 * query is useful in situations where an application intends to retrieve
	 * the complete documents after the search, e.g. via the getXmlDocument()
	 * method, use a local cache, etc.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param pageSize the number of elements for the first page of the result
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search (the document of the
	 *         DocumentResultElement objects will be null, only document ID and
	 *         relevance are used in this search mode)
	 * @throws IOException
	 */
	public PagedDocumentResult searchDocumentIDsPaged(Properties parameters, int pageSize, boolean allowCache) throws IOException {
		
		//	prepare passing cache access data reference
		CacheAccessData[] cad = new CacheAccessData[1];
		
		//	get backing result
		DocumentResult dr = this.searchDocumentIDs(parameters, allowCache, cad);
		
		//	return first page
		return new PagedDocumentResult(dr, new PagedResultData(dr, 0, pageSize, cad[0]) {
			protected SrsSearchResult getCacheBasedResult(Reader reader) throws IOException {
				return DocumentResult.readDocumentDataResult(reader);
			}
		});
	}
	
	/**
	 * Document result for paged reading.
	 * 
	 * @author sautter
	 */
	public static class PagedDocumentResult extends DocumentResult implements PagedResult {
		private PagedResultData pageData;
		
		PagedDocumentResult(DocumentResult data, PagedResultData pageData) {
			super(data.resultAttributes);
			this.pageData = pageData;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#hasNextElement()
		 */
		public boolean hasNextElement() {
			return this.pageData.hasNextElement();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#getNextElement()
		 */
		public SrsSearchResultElement getNextElement() {
			return this.pageData.getNextElement();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getStart()
		 */
		public int getStart() {
			return this.pageData.getStart();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getElementsRead()
		 */
		public int getElementsRead() {
			return this.pageData.getElementsRead();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getSize()
		 */
		public int getSize() {
			return this.pageData.getSize();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getElementsReadTotal()
		 */
		public int getElementsReadTotal() {
			return this.pageData.getElementsReadTotal();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getLimit()
		 */
		public int getLimit() {
			return this.pageData.getLimit();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getNextPage(int)
		 */
		public PagedResult getNextPage(int pageSize) throws IOException {
			PagedResultData pageData = this.pageData.getNextPage(pageSize);
			return ((pageData == null) ? null : new PagedDocumentResult(this, pageData));
		}
		
		/**
		 * Retrieve the next part of the paged index result. This method
		 * exists for convenience, it loops through to the getNextPage() method.
		 * @param pageSize the number of elements for the next page
		 * @return a paged index result holding (at most) the &lt;limit&gt;
		 *         next elements of the result
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getNextPage(int)
		 */
		public PagedDocumentResult getNextDocumentResultPage(int pageSize) throws IOException {
			return ((PagedDocumentResult) this.getNextPage(pageSize));
		}
	}
	
	/**
	 * Search entries of an index table belonging to documents matching a query,
	 * getting a paged result.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the annotations?
	 * @return the index entries of the index specified with the indexId
	 *         parameter, which belong to documents matching the rest of the
	 *         query
	 * @param pageSize the number of elements for the first page of the result
	 * @return the result of the search
	 * @throws IOException
	 */
	public PagedIndexResult searchIndexPaged(Properties parameters, boolean markSearchables, int pageSize) throws IOException {
		return this.searchIndexPaged(parameters, markSearchables, pageSize, true);
	}
	
	/**
	 * Search entries of an index table belonging to documents matching a query,
	 * getting a paged result.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param markSearchables add search links to the annotations?
	 * @return the index entries of the index specified with the indexId
	 *         parameter, which belong to documents matching the rest of the
	 *         query
	 * @param pageSize the number of elements for the first page of the result
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search
	 * @throws IOException
	 */
	public PagedIndexResult searchIndexPaged(Properties parameters, boolean markSearchables, int pageSize, boolean allowCache) throws IOException {
		
		//	prepare passing cache access data reference
		CacheAccessData[] cad = new CacheAccessData[1];
		
		//	get backing result
		IndexResult ir = this.searchIndex(parameters, markSearchables, allowCache, cad);
		
		//	return first page
		return new PagedIndexResult(ir, 0, pageSize, cad[0]);
	}
	
	/**
	 * Index result for paged reading.
	 * 
	 * @author sautter
	 */
	public static class PagedIndexResult extends IndexResult implements PagedResult {
		private PagedResultData pageData;
		
		PagedIndexResult(IndexResult data, int start, int pageSize, CacheAccessData cad) {
			this(data, new PagedResultData(data, start, pageSize, cad) {
				protected SrsSearchResult getCacheBasedResult(Reader reader) throws IOException {
					return IndexResult.readIndexResult(reader);
				}
			});
		}
		
		private PagedIndexResult(IndexResult data, PagedResultData pageData) {
			super(data.resultAttributes, data.indexName, data.indexLabel);
			this.pageData = pageData;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#hasNextElement()
		 */
		public boolean hasNextElement() {
			return this.pageData.hasNextElement();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#getNextElement()
		 */
		public SrsSearchResultElement getNextElement() {
			return this.pageData.getNextElement();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getStart()
		 */
		public int getStart() {
			return this.pageData.getStart();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getElementsRead()
		 */
		public int getElementsRead() {
			return this.pageData.getElementsRead();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getSize()
		 */
		public int getSize() {
			return this.pageData.getSize();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getElementsReadTotal()
		 */
		public int getElementsReadTotal() {
			return this.pageData.getElementsReadTotal();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getLimit()
		 */
		public int getLimit() {
			return this.pageData.getLimit();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getNextPage(int)
		 */
		public PagedResult getNextPage(int pageSize) throws IOException {
			PagedResultData pageData = this.pageData.getNextPage(pageSize);
			return ((pageData == null) ? null : new PagedIndexResult(this, pageData));
		}
		
		/**
		 * Retrieve the next part of the paged index result. This method
		 * exists for convenience, it loops through to the getNextPage() method.
		 * @param pageSize the number of elements for the next page
		 * @return a paged index result holding (at most) the &lt;limit&gt;
		 *         next elements of the result
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getNextPage(int)
		 */
		public PagedIndexResult getNextIndexResultPage(int pageSize) throws IOException {
			return ((PagedIndexResult) this.getNextPage(pageSize));
		}
	}
	
	/**
	 * Search an index table in thesaurus style, getting a paged result.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param pageSize the number of elements for the first page of the result
	 * @return the result of the search
	 * @throws IOException
	 */
	public PagedThesaurusResult searchThesaurusPaged(Properties parameters, int pageSize) throws IOException {
		return this.searchThesaurusPaged(parameters, pageSize, true);
	}
	
	/**
	 * Search an index table in thesaurus style, getting a paged result.
	 * @param parameters the search parameters, multi-values for a parameter
	 *            separated with line breaks
	 * @param pageSize the number of elements for the first page of the result
	 * @param allowCache allow returning cached result if available?
	 * @return the result of the search
	 * @throws IOException
	 */
	public PagedThesaurusResult searchThesaurusPaged(Properties parameters, int pageSize, boolean allowCache) throws IOException {
		
		//	prepare passing cache access data reference
		CacheAccessData[] cad = new CacheAccessData[1];
		
		//	get backing result
		ThesaurusResult tr = this.searchThesaurus(parameters, allowCache, cad);
		
		//	return first page
		return new PagedThesaurusResult(tr, 0, pageSize, cad[0]);
	}
	
	/**
	 * Thesaurus result for paged reading.
	 * 
	 * @author sautter
	 */
	public static class PagedThesaurusResult extends ThesaurusResult implements PagedResult {
		private PagedResultData pageData;
		
		PagedThesaurusResult(ThesaurusResult data, int start, int pageSize, CacheAccessData cad) {
			this(data, new PagedResultData(data, start, pageSize, cad) {
				protected SrsSearchResult getCacheBasedResult(Reader reader) throws IOException {
					return ThesaurusResult.readThesaurusResult(reader);
				}
			});
		}
		
		private PagedThesaurusResult(ThesaurusResult data, PagedResultData pageData) {
			super(data.resultAttributes, data.thesaurusEntryType, data.thesaurusName);
			this.pageData = pageData;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#hasNextElement()
		 */
		public boolean hasNextElement() {
			return this.pageData.hasNextElement();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.SrsSearchResult#getNextElement()
		 */
		public SrsSearchResultElement getNextElement() {
			return this.pageData.getNextElement();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getStart()
		 */
		public int getStart() {
			return this.pageData.getStart();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getElementsRead()
		 */
		public int getElementsRead() {
			return this.pageData.getElementsRead();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getSize()
		 */
		public int getSize() {
			return this.pageData.getSize();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getElementsReadTotal()
		 */
		public int getElementsReadTotal() {
			return this.pageData.getElementsReadTotal();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getLimit()
		 */
		public int getLimit() {
			return this.pageData.getLimit();
		}

		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getNextPage(int)
		 */
		public PagedResult getNextPage(int pageSize) throws IOException {
			PagedResultData pageData = this.pageData.getNextPage(pageSize);
			return ((pageData == null) ? null : new PagedThesaurusResult(this, pageData));
		}
		
		/**
		 * Retrieve the next part of the paged thesaurus result. This method
		 * exists for convenience, it loops through to the getNextPage() method.
		 * @param pageSize the number of elements for the next page
		 * @return a paged thesaurus result holding (at most) the &lt;limit&gt;
		 *         next elements of the result
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.client.GoldenGateSrsClient.PagedResult#getNextPage(int)
		 */
		public PagedThesaurusResult getNextThesaurusResultPage(int pageSize) throws IOException {
			return ((PagedThesaurusResult) this.getNextPage(pageSize));
		}
	}
}