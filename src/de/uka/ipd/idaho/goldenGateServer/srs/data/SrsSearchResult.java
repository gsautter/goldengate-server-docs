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
package de.uka.ipd.idaho.goldenGateServer.srs.data;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants;
import de.uka.ipd.idaho.htmlXmlUtil.Parser;
import de.uka.ipd.idaho.htmlXmlUtil.Parser.ParserInstance;
import de.uka.ipd.idaho.htmlXmlUtil.TokenReceiver;
import de.uka.ipd.idaho.htmlXmlUtil.TreeNodeAttributeSet;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.Grammar;
import de.uka.ipd.idaho.htmlXmlUtil.grammars.StandardGrammar;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * Common root class for all results of SRS search queries.
 * 
 * @author sautter
 */
public abstract class SrsSearchResult implements GoldenGateSrsConstants {
	
	/**
	 * the attribute names for the result, in the order the attribute values
	 * should be displayed
	 */
	public final String[] resultAttributes;

	/**
	 * Constructor
	 * @param resultAttributes the attribute names for the result, in the order
	 *            the attribute values should be displayed
	 */
	protected SrsSearchResult(String[] resultAttributes) {
		this.resultAttributes = resultAttributes;
	}
	
	/**
	 * Check whether this result has more element.
	 * @return true if the result has further elements, false otherwise
	 */
	public abstract boolean hasNextElement();
	
	/**
	 * Retrieve the next element of the result. In contrast to
	 * java.util.Iterator's next() method, this method does not throw a
	 * NoSuchElementException is there are no further elements, but returns
	 * null. The sub classes of Result (i.e. the different concrete result
	 * types) return the respective specific types of result elements. To avoid
	 * casts, the concrete sub classes furthermore provide similarly named
	 * methods that return the specific result element type directly.
	 * @return the next element of the result, or null, if there are no further
	 *         elements
	 */
	public abstract SrsSearchResultElement getNextElement();
	
	/**
	 * Retrieve a Comparator for ordering the elements of the result. Sub
	 * classes implementing this method may cast the argument objects of the
	 * compare() method to their specific result element type. Therefore, a
	 * Comparator retrieved from a given Result should only be used for
	 * comparing elements of that very result.
	 * @return a Comparator for ordering the elements of the result
	 */
	public abstract Comparator getSortOrder();
	
	/**
	 * Produce a start tag for the XML representation of the result. This method
	 * returns a starting tag of type RESULTS_NODE_NAME. Its attributes are (a)
	 * the RESULT_INDEX_FIELDS_ATTRIBUTE holding the content of this classes
	 * resultAttributes field, and (b) the attributes specified by sub classes
	 * through the getStartTagAttributes() method.
	 * @return a start tag for the XML representation of the result
	 */
	public String produceStartTag() {
		return this.produceHeadTag(false);
	}
	
	/**
	 * Produce an empty tag for the XML representation of the result. This method
	 * returns a starting tag of type RESULTS_NODE_NAME. Its attributes are (a)
	 * the RESULT_INDEX_FIELDS_ATTRIBUTE holding the content of this classes
	 * resultAttributes field, and (b) the attributes specified by sub classes
	 * through the getStartTagAttributes() method.
	 * @return an empty tag for the XML representation of the result
	 */
	public String produceEmptyTag() {
		return this.produceHeadTag(true);
	}
	
	/*
	 * Produce a starting or empty tag for the XML representation of the result.
	 * This method returns a starting tag of type RESULTS_NODE_NAME. Its
	 * attributes are (a) the RESULT_INDEX_FIELDS_ATTRIBUTE holding the content
	 * of this classes resultAttributes field, and (b) the attributes specified
	 * by sub classes through the getStartTagAttributes() method.
	 * @return an empty tag for the XML representation of the result
	 */
	private String produceHeadTag(boolean isEmpty) {
		StringBuffer tag = new StringBuffer("<" + RESULTS_NODE_NAME);
		
		tag.append(" " + RESULT_INDEX_FIELDS_ATTRIBUTE + "=\"");
		for (int a = 0; a < this.resultAttributes.length; a++)
			tag.append(((a == 0) ? "" : " ") + this.resultAttributes[a]);
		tag.append("\"");
		
		Properties attributes = this.getStartTagAttributes();
		for (Iterator ait = attributes.keySet().iterator(); ait.hasNext();) {
			String attribute = ait.next().toString();
			if (!RESULT_INDEX_FIELDS_ATTRIBUTE.equals(attribute))
				tag.append(" " + attribute + "=\"" + AnnotationUtils.escapeForXml(attributes.getProperty(attribute), true) + "\"");
		}
		
		if (isEmpty)
			tag.append("/");
		tag.append(">");
		return tag.toString();
	}
	
	/**
	 * Write the start tag of the XML representation of this result to some
	 * Writer
	 * @param out the Writer to write to
	 * @throws IOException
	 */
	public void writeStartTag(Writer out) throws IOException {
		out.write(this.produceStartTag());
	}
	
	/**
	 * Obtain the attributes for the start tag of the XML representation of the
	 * result. The returned Properties Object is only read, never written, so
	 * instances of sub classes can use one single Properties object that they
	 * return on all invocations of this method, rather than produce a new
	 * object for each individual invocation. The returned Properties need not
	 * contain the RESULT_INDEX_FIELDS_ATTRIBUTE, since this attribute is
	 * produced from the resultAttributes field of the Result class.
	 * @return a Properties holding the attributes for the start tag of the XML
	 *         representation of the result
	 */
	public abstract Properties getStartTagAttributes();
	
	/**
	 * Write the XML representation of an element of the result to some Writer.
	 * Implementing sub classes may assume that the specified result element is
	 * of the sub class specific result element type.
	 * @param out the Writer to write to
	 * @param element the result element to write
	 * @throws IOException
	 */
	public abstract void writeElement(Writer out, SrsSearchResultElement element) throws IOException;
	
	/**
	 * Produce an end tag for the XML representation of the result. This method
	 * returns a closing tag of type RESULTS_NODE_NAME by default.
	 * @return an end tag for the XML representation of the result
	 */
	public String produceEndTag() {
		return ("</" + RESULTS_NODE_NAME + ">");
	}
	
	/**
	 * Write the end tag of the XML representation of this result to some
	 * Writer
	 * @param out the Writer to write to
	 * @throws IOException
	 */
	public void writeEndTag(Writer out) throws IOException {
		out.write(this.produceEndTag());
	}
	
	/**
	 * Write the XML representation of this result to some Writer. Invoking this
	 * method consumes the result, i.e., it fetches all (remaining) elements
	 * that are available, and it does not buffer them in any way. If this
	 * effect is undesired, use the writeXml() method of the
	 * SrsSearchResultBuffer class instead.
	 * @param out the Writer to write to
	 * @throws IOException
	 */
	public void writeXml(Writer out) throws IOException {
		
		//	produce writer
		BufferedWriter buf = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
		
		//	result with elements
		if (this.hasNextElement()) {
			buf.write(this.produceStartTag());
			buf.newLine();
			
			while (this.hasNextElement())
				this.writeElement(buf, this.getNextElement());
			
			buf.write(this.produceEndTag());
			buf.newLine();
		}
		
		//	empty result
		else {
			buf.write(this.produceEmptyTag());
			buf.newLine();
		}
		
		//	flush Writer if it was wrapped
		if (buf != out)
			buf.flush();
	}
	
	/**
	 * Create a single result element from given XML data. The data is provided
	 * as an array of tokens.
	 * @param tokens the XML tokens representing the result element to create
	 * @return a result element created from the specified XML data
	 * @throws IOException
	 */
	public abstract SrsSearchResultElement readElement(String[] tokens) throws IOException;
	
	/**
	 * Factory for specific result objects. Instances of this class are
	 * intended to be specified as arguments to the getResult() method, serving
	 * as a proxy for the constructors of specific result sub classes.
	 * 
	 * @author sautter
	 */
	protected static abstract class ResultBuilder extends TokenReceiver {
		
		private Reader in;
//		private char[] buffer = new char[1024];
//		private PipedWriter toParser = new PipedWriter();
//		private Thread parserThread;
		private ParserInstance pi;
		
		private IOException exception = null;
		
		private SrsSearchResult result;
		private LinkedList elementBuffer = new LinkedList();
		private StringVector elementDataBuffer = new StringVector();
		
		private LinkedList elementDataStack = new LinkedList();
		private boolean inResult = false;
		
		/**
		 * Constructor
		 * @param in the Reader to read from
		 * @throws IOException
		 */
		public ResultBuilder(Reader in) throws IOException {
			this.in = in;
			this.pi = parser.getInstance(in, this);
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#finalize()
		 */
		protected void finalize() throws Throwable {
			
			/*
			 * if data has not been read completely before result object is
			 * abandoned (i.e., becomes eligible for garbage collection), source
			 * reader is still open, occupying system resources ==> close it.
			 */
			if (this.in != null)
				this.in.close();
		}
		
		/**
		 * Wrap a result of a specific type around the stream backing the result
		 * builder. The specific result created by this method should loop
		 * through its hasNextElement() and getNextElement() methods to the
		 * respective methods of the result builder. The latter will, in turn,
		 * produce its elements using the created result's readElement() method,
		 * so the generic result elements returned by the getNextElement()
		 * method can be cast to whatever specific elements are created in the
		 * created result's readElement() method.
		 * @param resultAttributes the result attributes array
		 * @param attributes a Properties holding further attributes of the
		 *            result start tag
		 * @return a specific result wrapping the specified generic result
		 */
		protected abstract SrsSearchResult buildResult(String[] resultAttributes, Properties attributes);
		
		/**
		 * @return the result created from the XML data
		 */
		protected SrsSearchResult getResult() throws IOException {
			if (this.exception != null)
				throw this.exception;
			
			while (this.result == null) {
				if (this.pi.hasMoreTokens())
					this.pi.consumeToken();
				else {
					this.in.close();
					this.in = null;
					return this.result;
				}
			}
			
			return this.result;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#hasNextElement()
		 */
		public boolean hasNextElement() {
			if (this.exception != null)
				throw new RuntimeException("Exception retrieving next element.", this.exception);
			
			this.fillElementBuffer();
			return !this.isElementBufferEmpty();
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGateServer.srs.data.Result#getNextElement()
		 */
		public final SrsSearchResultElement getNextElement() {
			if (this.exception != null)
				throw new RuntimeException("Exception retrieving next element.", this.exception);
			
			this.fillElementBuffer();
			return this.getFromElementBuffer();
		}
		
		private void fillElementBuffer() {
			while (this.isElementBufferEmpty()) try {
				if (!this.pi.consumeToken()) {
					if (this.in != null)
						this.in.close();
					this.in = null;
					break;
				}
			}
			catch (IOException ioe) {
				this.exception = ioe;
				throw new RuntimeException("Exception retrieving next element.", ioe);
			}
		}
		
		private boolean isElementBufferEmpty() {
			synchronized(this.elementBuffer) {
				return this.elementBuffer.isEmpty();
			}
		}
		private void addToElementBuffer(SrsSearchResultElement re) {
			synchronized(this.elementBuffer) {
				if (re != null)
					this.elementBuffer.addLast(re);
			}
		}
		private SrsSearchResultElement getFromElementBuffer() {
			synchronized(this.elementBuffer) {
				return (this.elementBuffer.isEmpty() ? null : ((SrsSearchResultElement) this.elementBuffer.removeFirst()));
			}
		}
		
		/* (non-Javadoc)
		 * @see de.htmlXmlUtil.TokenReceiver#close()
		 */
		public void close() throws IOException {
			//	nothing to close, shutdown occurs automatically at end of backing data stream
		}
		
		//	TODO use stack (as in Open String Pool) to catch 'results' nodes in data
		
		/* (non-Javadoc)
		 * @see de.htmlXmlUtil.TokenReceiver#storeToken(java.lang.String, int)
		 */
		public void storeToken(String token, int treeDepth) throws IOException {
//			System.out.println("ResultBuilder: received token '" + token + "'");
//			System.out.print(token);
			
			//	markup token
			if (grammar.isTag(token)) {
				String type = grammar.getType(token);
				
				//	main result
				if (RESULTS_NODE_NAME.equals(type)) {
					
					//	end of main result
					if (grammar.isEndTag(token))
						this.inResult = false;
					
					//	start of main result
					else {
						TreeNodeAttributeSet tnas = TreeNodeAttributeSet.getTagAttributes(token, grammar);
						
						StringVector resultAttributes = new StringVector();
						resultAttributes.parseAndAddElements(tnas.getAttribute(RESULT_INDEX_FIELDS_ATTRIBUTE, ""), " ");
						resultAttributes.removeAll("");
						
						String[] attributeNames = tnas.getAttributeNames();
						Properties attributes = new Properties();
						for (int a = 0; a < attributeNames.length; a++) {
							if (!RESULT_INDEX_FIELDS_ATTRIBUTE.equals(attributeNames[a]))
								attributes.setProperty(attributeNames[a], tnas.getAttribute(attributeNames[a]));
						}
						this.result = this.buildResult(resultAttributes.toStringArray(), attributes);
						this.inResult = true;
					}
				}
				
				//	we're inside the main root tag
				else if (this.inResult) {
					
					//	empty element
					if (grammar.isSingularTag(token)) {
						
						//	store data
						this.elementDataBuffer.addElement(token);
						
						//	stack is empty, no result element open ==> empty result element data, produce element
						if (this.elementDataStack.isEmpty()) {
							this.addToElementBuffer(this.result.readElement(this.elementDataBuffer.toStringArray()));
							this.elementDataBuffer.clear();
//							System.out.println("  ==> stored next element");
						}
					}
					
					//	end tag
					else if (grammar.isEndTag(token)) {
						
						//	store data
						this.elementDataBuffer.addElement(token);
						
						//	pop one open element from stack (if any)
						if (!this.elementDataStack.isEmpty())
							this.elementDataStack.removeLast();
						
						//	stack is empty, result element closed ==> end of result element data, produce element
						if (this.elementDataStack.isEmpty()) {
							this.addToElementBuffer(this.result.readElement(this.elementDataBuffer.toStringArray()));
							this.elementDataBuffer.clear();
//							System.out.println("  ==> stored next element");
						}
					}
					
					//	start tag
					else {
						
						//	store data
						this.elementDataBuffer.addElement(token);
						
						//	put on stack in order to allow for tracking end of result elements
						this.elementDataStack.addLast(type);
					}
				}
			}
			
			//	text data, store if some result element open
			else if (this.elementDataStack.size() != 0) {
				String value = token.replaceAll("[\\r\\n]", "");
				if (value.length() != 0)
					this.elementDataBuffer.addElement(value);
			}
		}
	}
	
	//	parser and grammar for data
	protected static final Grammar grammar = new StandardGrammar();
	protected static final Parser parser = new Parser(grammar);
}
