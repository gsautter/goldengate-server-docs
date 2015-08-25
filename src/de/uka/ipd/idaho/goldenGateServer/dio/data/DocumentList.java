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
package de.uka.ipd.idaho.goldenGateServer.dio.data;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FilterReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * List of documents in a GoldenGATE DIO, implemented iterator-style for
 * efficiency.
 * 
 * @author sautter
 */
public abstract class DocumentList implements GoldenGateDioConstants {
	static final String DOCUMENT_LIST_NODE_NAME = "docList";
	static final String DOCUMENT_NODE_NAME = "doc";
	static final String DOCUMENT_LIST_FIELDS_ATTRIBUTE = "listFields";
	
	/**
	 * A hybrid of set and map, this class contains the distinct values of
	 * document attributes in the database, plus for each attribute value pair
	 * the number of documents having that particular value for the attribute.
	 * This is to help filtering document lists.
	 * 
	 * @author sautter
	 */
	public static class DocumentAttributeSummary {
		private static class Count {
			int count = 0;
			Count(int count) {
				this.count = count;
			}
			void increment(int i) {
				this.count += i;
			}
			void decrement(int d) {
				this.count -= d;
			}
			int getCount() {
				return this.count;
			}
		}
		
		private TreeMap content = new TreeMap();
		private int size = 0;
		private int charSize = 0;
		
		/**
		 * @return the number of times the specified string has been added to
		 *         this index
		 */
		public int getCount(String string) {
			Count c = ((Count) this.content.get(string));
			return ((c == null) ? 0 : c.getCount());
		}
		
		/**
		 * Add a string to this index, using count 1
		 * @return true if the specified string was added for the first time,
		 *         false otherwise
		 */
		public boolean add(String string) {
			return this.add(string, 1);
		}
		
		/**
		 * Add a string to this index, using a custom count (same as count times
		 * adding string, but faster)
		 * @return true if the specified string was added for the first time,
		 *         false otherwise
		 */
		public boolean add(String string, int count) {
			this.size += count;
			Count c = ((Count) this.content.get(string));
			if (c == null) {
				this.content.put(string, new Count(count));
				this.charSize += string.length();
				if (count > 1)
					this.charSize += 2;
				return true;
			}
			else {
				if (c.getCount() == 1)
					this.charSize += 2;
				c.increment(count);
				return false;
			}
		}
		
		/**
		 * Remove a string from this index once, decreasing it's count by 1
		 * @return true if the specified string was totally removed, false
		 *         otherwise
		 */
		public boolean remove(String string) {
			return this.remove(string, 1);
		}
		
		/**
		 * Remove a string from this index, using a custom count (same as count
		 * times removing string, but faster)
		 * @return true if the specified string was totally removed, false
		 *         otherwise
		 */
		public boolean remove(String string, int count) {
			Count c = ((Count) this.content.get(string));
			if (c == null)
				return false;
			int decrement = Math.min(c.getCount(), count);
			this.size -= decrement;
			if (c.getCount() <= count) {
				this.content.remove(string);
				this.charSize -= string.length();
				return true;
			}
			else {
				c.decrement(count);
				if (c.getCount() == 1)
					this.charSize -= 2;
				return false;
			}
		}
		
		/**
		 * Remove a string from this index totally, setting it's count to 0
		 */
		public void removeAll(String string) {
			int count = this.getCount(string);
			this.size -= count;
			this.content.remove(string);
			this.charSize -= string.length();
			if (count > 1)
				this.charSize -= 2;
		}
		
		/**
		 * @return the number of strings that have been added to this index so
		 *         far
		 */
		public int size() {
			return this.size;
		}
		
		/**
		 * @return the (approximate) number of characters this index takes to
		 *         transfer
		 */
		int getCharSize() {
			return this.charSize;
		}
		
		/**
		 * @return the number of distinct strings that have been added to this
		 *         index so far
		 */
		public int valueCount() {
			return this.content.size();
		}
		
		public Set keySet() {
			final Set keySet = this.content.keySet();
			return new Set() {
				public boolean add(Object obj) {
					return !keySet.contains(obj);
				}
				public boolean addAll(Collection coll) {
					return !keySet.containsAll(coll);
				}
				public void clear() {}
				public boolean contains(Object o) {
					return keySet.contains(o);
				}
				public boolean containsAll(Collection coll) {
					return keySet.containsAll(coll);
				}
				public boolean isEmpty() {
					return keySet.isEmpty();
				}
				public Iterator iterator() {
					final Iterator it = keySet.iterator();
					return new Iterator() {
						public boolean hasNext() {
							return it.hasNext();
						}
						public Object next() {
							return it.next();
						}
						public void remove() {}
					};
				}
				public boolean remove(Object o) {
					return keySet.contains(o);
				}
				public boolean removeAll(Collection coll) {
					Set set = new HashSet(keySet);
					return set.removeAll(coll);
				}
				public boolean retainAll(Collection coll) {
					Set set = new HashSet(keySet);
					return set.retainAll(coll);
				}
				public int size() {
					return keySet.size();
				}
				public Object[] toArray() {
					return keySet.toArray();
				}
				public Object[] toArray(Object[] objs) {
					return keySet.toArray(objs);
				}
			};
		}
	}
	
	/**
	 * the field names for the document list, in the order they should be
	 * displayed
	 */
	public final String[] listFieldNames;

	/**
	 * Constructor for general use
	 * @param listFieldNames the field names for the document list, in the order
	 *            they should be displayed
	 */
	public DocumentList(String[] listFieldNames) {
		this.listFieldNames = listFieldNames;
	}
	
	/**
	 * Constructor for creating wrappers
	 * @param model the document list to wrap
	 */
	public DocumentList(DocumentList model) {
		this(model, null);
	}
	
	/**
	 * Constructor for creating wrappers that add fields
	 * @param model the document list to wrap
	 * @param extensionListFieldNames an array holding additional field names
	 */
	public DocumentList(DocumentList model, String[] extensionListFieldNames) {
		if (extensionListFieldNames == null) {
			this.listFieldNames = new String[model.listFieldNames.length];
			System.arraycopy(model.listFieldNames, 0, this.listFieldNames, 0, model.listFieldNames.length);
		}
		else {
			String[] listFieldNames = new String[model.listFieldNames.length + extensionListFieldNames.length];
			System.arraycopy(model.listFieldNames, 0, listFieldNames, 0, model.listFieldNames.length);
			System.arraycopy(extensionListFieldNames, 0, listFieldNames, model.listFieldNames.length, extensionListFieldNames.length);
			this.listFieldNames = listFieldNames;
		}
		for (int f = 0; f < this.listFieldNames.length; f++)
			this.addListFieldValues(this.listFieldNames[f], model.getListFieldValues(this.listFieldNames[f]));
	}
	
	/**
	 * Retrieve a summary of the values in a list field. The sets returned by
	 * this method are immutable. If there is no summary, this method returns
	 * null, but never an empty set. The set does not contain nulls and is
	 * sorted lexicographically. There are no summaries for document ID,
	 * external identifiers, document titles, or time related attributes, as
	 * these would likely have the same size as the document list itself.
	 * @param listFieldName the name of the field
	 * @return a set containing the summary values
	 */
	public DocumentAttributeSummary getListFieldValues(String listFieldName) {
		return this.getListFieldValues(listFieldName, false);
	}
	DocumentAttributeSummary getListFieldValues(String listFieldName, boolean create) {
		DocumentAttributeSummary das = ((DocumentAttributeSummary) this.listFieldValues.get(listFieldName));
		if ((das == null) && create) {
			das = new DocumentAttributeSummary();
			this.listFieldValues.put(listFieldName, das);
		}
		return das;
	}
	
	/**
	 * Add a set of values to the value summary of a list field. 
	 * @param listFieldName the name of the field
	 * @param listFieldValues the values to add
	 */
	protected final void addListFieldValues(String listFieldName, DocumentAttributeSummary listFieldValues) {
		if (summarylessAttributes.contains(listFieldName))
			return;
		if ((listFieldValues == null) || (listFieldValues.size() == 0))
			return;
		DocumentAttributeSummary das = this.getListFieldValues(listFieldName, true);
		for (Iterator vit = listFieldValues.keySet().iterator(); vit.hasNext();) {
			String listFieldValue = ((String) vit.next());
			das.add(listFieldValue, listFieldValues.getCount(listFieldValue));
		}
	}
	
	/**
	 * Add a set of values to the value summary of a list field. 
	 * @param listFieldName the name of the field
	 * @param listFieldValues the values to add
	 */
	protected final void addListFieldValues(DocumentListElement dle) {
		if (dle == null)
			return;
		for (int f = 0; f < this.listFieldNames.length; f++) {
			if (summarylessAttributes.contains(this.listFieldNames[f]))
				continue;
			String listFieldValue = ((String) dle.getAttribute(this.listFieldNames[f]));
			if (listFieldValue == null)
				continue;
			DocumentAttributeSummary das = this.getListFieldValues(this.listFieldNames[f], true);
			das.add(listFieldValue);
		}
	}
	
	private Map listFieldValues = new HashMap();
	
	/**
	 * Check if there is another document in the list.
	 * @return true if there is another document, false otherwise
	 */
	public abstract boolean hasNextDocument();

	/**
	 * Retrieve the next document from the list. If there is no next document, this
	 * method returns null.
	 * @return the next document in the list
	 */
	public abstract DocumentListElement getNextDocument();
	
	/**
	 * Check the total number of documents in the list. If the count is not
	 * available, this method returns -1. Otherwise, the returned value can
	 * either be the exact number of documents remaining, or a conservative
	 * estimate, if the exact number is not available. This default
	 * implementation returns -1 if getRetrievedDocumentCount() returns -1, and
	 * the sum of getRetrievedDocumentCount() and getRemainingDocumentCount()
	 * otherwise. Sub classes are welcome to overwrite it and provide a more
	 * exact estimate. They need to make sure not to use this implementation in
	 * their implementation of getRetrievedDocumentCout() or
	 * getReminingDocumentCount(), however, to prevent delegating back and forth
	 * and causing stack overflows.
	 * @return the number of documents remaining
	 */
	public int getDocumentCount() {
		int retrieved = this.getRetrievedDocumentCount();
		return ((retrieved == -1) ? -1 : (retrieved + this.getRemainingDocumentCount()));
	}
	
	/**
	 * Check the number of documents retrieved so far from the getNextDocument()
	 * method. If the count is not available, this method returns -1. This
	 * default implementation does return -1, sub classes are welcome to
	 * overwrite it and provide a count.
	 * @return the number of documents retrieved so far
	 */
	public int getRetrievedDocumentCount() {
		return -1;
	}
	
	/**
	 * Check the number of documents remaining in the list. If the count is not
	 * available, this method returns -1. Otherwise, the returned value can
	 * either be the exact number of documents remaining, or a conservative
	 * estimate, if the exact number is not available. This default
	 * implementation returns 1 if hasNextDocument() returns true, and 0
	 * otherwise. Sub classes are welcome to overwrite it and provide a more
	 * accurate estimate.
	 * @return the number of documents remaining
	 */
	public int getRemainingDocumentCount() {
		return (this.hasNextDocument() ? 1 : 0);
	}
	
	/**
	 * Write this document list to some writer as XML data.
	 * @param out the Writer to write to
	 * @throws IOException
	 */
	public void writeXml(Writer out) throws IOException {
		
		//	produce writer
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	write empty data
		if (!this.hasNextDocument()) {
			
			//	write results
			StringVector listFields = new StringVector();
			listFields.addContent(this.listFieldNames);
			buf.write("<" + DOCUMENT_LIST_NODE_NAME + 
					" " + DOCUMENT_LIST_FIELDS_ATTRIBUTE + "=\"" + listFields.concatStrings(" ") + "\"" +
			"/>");
			buf.newLine();
		}
		
		//	write data
		else {
			
			//	get result field names
			StringVector listFields = new StringVector();
			listFields.addContent(this.listFieldNames);
			buf.write("<" + DOCUMENT_LIST_NODE_NAME + 
					" " + DOCUMENT_LIST_FIELDS_ATTRIBUTE + "=\"" + listFields.concatStrings(" ") + "\"" +
			">");
			
			while (this.hasNextDocument()) {
				DocumentListElement dle = this.getNextDocument();
				
				//	write element
				buf.write("  <" + DOCUMENT_NODE_NAME);
				for (int a = 0; a < this.listFieldNames.length; a++) {
					String fieldValue = ((String) dle.getAttribute(this.listFieldNames[a]));
					if ((fieldValue != null) && (fieldValue.length() != 0))
						buf.write(" " + this.listFieldNames[a] + "=\"" + AnnotationUtils.escapeForXml(fieldValue, true) + "\"");
				}
				buf.write("/>");
				buf.newLine();
			}
			
			buf.write("</" + DOCUMENT_LIST_NODE_NAME + ">");
			buf.newLine();
		}
		
		//	flush Writer if it was wrapped
		if (buf != out)
			buf.flush();
	}
	
	/**
	 * Write the documents in this list to a given writer. This method consumes
	 * the list, i.e., it iterates through to the last document list element it
	 * contains.
	 * @param out the writer to write to
	 * @throws IOException
	 */
	public void writeData(Writer out) throws IOException {
		BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		int size = this.getDocumentCount();
		if (size != -1) {
			bw.write("" + size);
			bw.newLine();
		}
		for (int f = 0; f < this.listFieldNames.length; f++) {
			if (f != 0) bw.write(",");
			bw.write('"' + this.listFieldNames[f] + '"');
		}
		bw.newLine();
		while (this.hasNextDocument()) {
			bw.write(this.getNextDocument().toCsvString('"', this.listFieldNames));
			bw.newLine();
		}
		for (int f = 0; f < this.listFieldNames.length; f++) {
			DocumentAttributeSummary fieldValues = this.getListFieldValues(this.listFieldNames[f]);
			if (fieldValues == null)
				continue;
			bw.write(this.listFieldNames[f]);
			for (Iterator vit = fieldValues.keySet().iterator(); vit.hasNext();) {
				String fieldValue = ((String) vit.next());
				bw.write(" " + URLEncoder.encode(fieldValue, "UTF-8") + "=" + fieldValues.getCount(fieldValue));
			}
			bw.newLine();
		}
		if (bw != out)
			bw.flush();
	}
	
	/**
	 * Wrap a document list around a reader, which provides the list's data in
	 * form of a character stream. Do not close the specified reader after this
	 * method returns. The reader is closed by the returned list after the last
	 * document list element is read.
	 * @param in the Reader to read from
	 * @return a document list that makes the data from the specified reader
	 *         available as document list elements
	 * @throws IOException
	 */
	public static DocumentList readDocumentList(Reader in) throws IOException {
		final int[] charsRead = {0};
		final BufferedReader finalBr = new BufferedReader(new FilterReader(in) {
			public int read() throws IOException {
				int r = super.read();
				if (r != -1)
					charsRead[0]++;
				return r;
			}
			public int read(char[] cbuf, int off, int len) throws IOException {
				int r = super.read(cbuf, off, len);
				if (r != -1)
					charsRead[0] += r;
				return r;
			}
		}, 65536);
		
		final int[] docCount = {-1};
		String fieldString = finalBr.readLine();
		if ((fieldString.charAt(0) != '"') && (fieldString.charAt(0) > 32)) {
			try {
				docCount[0] = Integer.parseInt(fieldString);
			} catch (NumberFormatException nfe) {}
			fieldString = finalBr.readLine();
		}
		String[] fields = parseCsvLine(fieldString, '"');
		
		//	create document list
		DocumentList dl = new DocumentList(fields) {
			private BufferedReader br = finalBr;
			private String next = null;
			private int docsRetrieved = 0;
			private int charsRetrieved = 0;
			public int getRetrievedDocumentCount() {
				return this.docsRetrieved;
			}
			public int getRemainingDocumentCount() {
				if (docCount[0] == -1) {
					if (this.charsRetrieved == 0)
						return (this.hasNextDocument() ? 1 : 0);
					int docSize = (this.charsRetrieved / ((this.docsRetrieved == 0) ? 1 : this.docsRetrieved));
					int charsRemaining = (charsRead[0] - this.charsRetrieved);
					return Math.round(((float) charsRemaining) / docSize);
				}
				else return (docCount[0] - this.docsRetrieved);
			}
			public boolean hasNextDocument() {
				if (this.next != null)
					return true;
				else if (this.br == null)
					return false;
				
				try {
					this.next = this.br.readLine();
					if (this.next != null) {
						if (this.next.trim().length() == 0)
							this.next = null;
						else if (this.next.startsWith("\""))
							this.charsRetrieved += this.next.length();
						else {
							int split = this.next.indexOf(' ');
							if (split != -1) {
								String fieldName = this.next.substring(0, split);
								String[] fieldValues = this.next.substring(split + 1).split("\\s++");
								DocumentAttributeSummary das = this.getListFieldValues(fieldName, true);
								for (int v = 0; v < fieldValues.length; v++) {
									String fieldValue = fieldValues[v];
									int countStart = fieldValue.indexOf('=');
									if (countStart == -1)
										das.add(URLDecoder.decode(fieldValue, "UTF-8"));
									else das.add(URLDecoder.decode(fieldValue.substring(0, countStart), "UTF-8"), Integer.parseInt(fieldValue.substring(countStart+1)));
								}
							}
							this.next = null;
							return this.hasNextDocument();
						}
					}
				}
				catch (IOException ioe) {
					ioe.printStackTrace(System.out);
				}
				
				if (this.next == null) {
					try {
						this.br.close();
					}
					catch (IOException ioe) {
						ioe.printStackTrace(System.out);
					}
					this.br = null;
					return false;
				}
				else return true;
			}
			public DocumentListElement getNextDocument() {
				if (!this.hasNextDocument())
					return null;
				String[] next = parseCsvLine(this.next, '"');
				this.next = null;
				DocumentListElement dle = new DocumentListElement();
				for (int f = 0; f < this.listFieldNames.length; f++)
					dle.setAttribute(this.listFieldNames[f], ((f < next.length) ? next[f] : ""));
				this.docsRetrieved++;
				return dle;
			}
			protected void finalize() throws Throwable {
				if (this.br != null) {
					this.br.close();
					this.br = null;
				}
			}
		};
		
		/*
		 * This call to hasNextDocument() is necessary to make sure attribute
		 * summaries are loaded even if client does not call hasNextDocument(),
		 * e.g. knowing that it's a list head request only.
		 */
		dl.hasNextDocument();
		
		//	finally ...
		return dl;
	}
	
	private static String[] parseCsvLine(String line, char delimiter) {
		char currentChar;
		char nextChar;
		
		boolean quoted = false;
		boolean escaped = false;
		
		StringVector lineParts = new StringVector();
		StringBuffer linePartAssembler = new StringBuffer();
		
		for (int c = 0; c < line.length(); c++) {
			currentChar = line.charAt(c);
			nextChar = (((c + 1) == line.length()) ? '\u0000' : line.charAt(c+1));
			
			//	escaped character
			if (escaped) {
				escaped = false;
				linePartAssembler.append(currentChar);
			}
			
			//	start or end of quoted value
			else if (currentChar == delimiter) {
				if (quoted) {
					if (nextChar == delimiter) escaped = true;
					else {
						quoted = false;
						lineParts.addElement(linePartAssembler.toString());
						linePartAssembler = new StringBuffer();
					}
				}
				else quoted = true;
			}
			
			//	in quoted value
			else if (quoted) linePartAssembler.append(currentChar);
			
			//	end of value
			else if ((currentChar == ',')) {
				if (linePartAssembler.length() != 0) {
					lineParts.addElement(linePartAssembler.toString());
					linePartAssembler = new StringBuffer();
				}
			}
			
			//	other char
			else linePartAssembler.append(currentChar);
		}
		
		//	return result
		return lineParts.toStringArray();
	}
}