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
package de.uka.ipd.idaho.goldenGateServer.srs.webPortal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.Annotation;
import de.uka.ipd.idaho.gamta.AnnotationUtils;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.goldenGateServer.srs.GoldenGateSrsConstants;
import de.uka.ipd.idaho.htmlXmlUtil.accessories.HtmlPageBuilder;
import de.uka.ipd.idaho.stringUtils.StringVector;

/**
 * Interface holding additional constants for configuring the GoldenGATE SRS Search
 * Portal web application, like formatting search lists, the way documents are
 * displayed, etc.
 * 
 * @author sautter
 */
public interface SearchPortalConstants extends GoldenGateSrsConstants {
	
	/** the attribute holding the size of a result (in buffered results only) */
	public static final String RESULT_SIZE_ATTRIBUTE = "resultSize";
	
	/** the attribute holding the link to a search with higher cutoff (document search only) */
	public static final String MORE_RESULTS_LINK_ATTRIBUTE = "moreResultsLink";
	
	/** the attribute holding the link to the given search result, but with as few fields as possible */
	public static final String SHORT_LINK_ATTRIBUTE = "shortLink";
	
	/** the attribute holding the label of a search result, i.e., what the search was for */
	public static final String RESULT_LABEL_ATTRIBUTE = "resultLabel";
	
	
	/** the parameter indicating how to layout the list of result documents of a search */
	public static final String RESULT_LIST_MODE_PARAMETER = "resultListMode";
	
	/** value for RESULT_LIST_MODE_PARAMETER indicating to display results as a plain list of document metadata */
	public static final String LIST_RESULT_LIST_MODE = "list";
	
	/** value for RESULT_LIST_MODE_PARAMETER indicating to display results as a list of document metadata plus boxed details */
	public static final String CONCISE_RESULT_LIST_MODE = "concise";
	
	/** value for RESULT_LIST_MODE_PARAMETER indicating to display results as a list of document metadata plus indexed details */
	public static final String DETAIL_RESULT_LIST_MODE = "details";
	
	/** value for RESULT_LIST_MODE_PARAMETER indicating to display results as a list of fully lain out documents */
	public static final String FULL_RESULT_LIST_MODE = "full";
	
	
	/** the parameter indicating how to layout the result documents of a search */
	public static final String RESULT_DISPLAY_MODE_PARAMETER = "resultDisplayMode";
	
	/** value for RESULT_DISPLAY_MODE_PARAMETER indicating XML layout */
	public static final String XML_RESULT_DISPLAY_MODE = "XML";
	
	/** value for RESULT_DISPLAY_MODE_PARAMETER indicating plain text layout */
	public static final String PLAIN_RESULT_DISPLAY_MODE = "plain";
	
	/** value for RESULT_DISPLAY_MODE_PARAMETER indicating boxed layout */
	public static final String BOXED_RESULT_DISPLAY_MODE = "boxed";
	
	
	/** the parameter indicating the search mode*/
	public static final String SEARCH_MODE_PARAMETER = "searchMode";
	
	/** value for SEARCH_MODE_PARAMETER triggering a default search */
	public static final String DEFAULT_SEARCH_MODE = "search";
	
	/** value for SEARCH_MODE_PARAMETER triggering a search for documents */
	public static final String DOCUMENT_SEARCH_MODE = "documents";
	
	/** value for SEARCH_MODE_PARAMETER triggering a document summary for one master document */
	public static final String DOCUMENT_SUMMARY_MODE = "summary";
	
	/** value for SEARCH_MODE_PARAMETER triggering an index search */
	public static final String INDEX_SEARCH_MODE = "index";
	
	/** value for SEARCH_MODE_PARAMETER triggering displaying a single document */
	public static final String HTML_DOCUMENT_SEARCH_MODE = "htmlDoc";
	
	/** value for SEARCH_MODE_PARAMETER triggering the search for a specific XML document, which is identified by its ID */
	public static final String XML_DOCUMENT_SEARCH_MODE = "xmlDoc";
	
	/** value for SEARCH_MODE_PARAMETER triggering a thesaurus-style search through index tables*/
	public static final String THESAURUS_SEARCH_MODE = "thesaurus";
	
	/** value for SEARCH_MODE_PARAMETER triggering collection statistics to be displayed */
	public static final String STATISTICS_MODE = "statistics";
	
	
	/** the parameter indicating the desired response format*/
	public static final String RESULT_FORMAT_PARAMETER = "resultFormat";
	
	/** value for RESULT_FORMAT_PARAMETER causing the response to be an HTML page, with search forms and result index */
	public static final String HTML_RESULT_FORMAT = "html";
	
	/** value for RESULT_FORMAT_PARAMETER causing an XML response containing only the search result*/
	public static final String XML_RESULT_FORMAT = "xml";
	
	/** value for RESULT_FORMAT_PARAMETER causing the response to contain the search result in CSV format (useful only if SEARCH_MODE_PARAMETER is set to THESAURUS_SEARCH_MODE)*/
	public static final String CSV_RESULT_FORMAT = "csv";
	
	
	/** the parameter indicating whether or not reuse of cached results is allowed */
	public static final String CACHE_CONTROL_PARAMETER = "cacheControl";
	
	/** value for CACHE_CONTROL_PARAMETER causing caches to be bypassed */
	public static final String FORCE_CACHE = "force";
	
	
	/** the the name of the SRS search document search form */
	public static final String SRS_SEARCH_FORM_NAME = "srsSearchForm";
	
	
	/** the the XML node type for search result links */
	public static final String LINK_NODE_TYPE = "externalLink";
	
	/** the name of the attribute holding a the class name of the linker that produced a link */
	public static final String LINKER_CLASS_NAME_ATTRIBUTE = "linkerClassName";
	
	/** the name of the attribute holding a link's label */
	public static final String LINK_LABEL_ATTRIBUTE = "label";
	
	/** the name of the attribute holding a the URL of a link's icon */
	public static final String LINK_ICON_URL_ATTRIBUTE = "iconUrl";
	
	/** the name of the attribute holding a link's title, i.e. tooltip text */
	public static final String LINK_TITLE_ATTRIBUTE = "title";
	
	/** the name of the attribute holding a the URL a link points to */
	public static final String LINK_HREF_ATTRIBUTE = "href";
	
	/** the name of the attribute holding a the onclick command for a link */
	public static final String LINK_ONCLICK_ATTRIBUTE = "onclick";
	
	/** the name of the attribute holding a link's category */
	public static final String LINK_CATEGORY_ATTRIBUTE = "category";
	
	
	/** the category for links leading to an XML representation of a search result document (should be used only in links returned by getDocumentLink()) */
	public static final String XML_DOCUMENT = "xmlDocument";
	
	/** the category for links leading to some resource providing visualization of a search result, like images or maps */
	public static final String VISUALIZATION = "visualization";
	
	/** the category for links leading to some external resource providing extra information on a document or a detail in it */
	public static final String EXTERNAL_INFORMATION = "externalInformation";
	
	/** the category for links leading to some functionality relating to a document or some detail in it */
	public static final String FUNCTIONALITY = "functionality";
	
	/**
	 * A pattern for creating display strings from individual XML attributes.
	 * The structure of a pattern string is relatively simple: an '@' followed
	 * by a qName string is interpreted as a reference to an XML attribute, the
	 * name being terminated at the next non-letter. Everything else counts as
	 * and is reproduced as a literal. Use '\' to escape '@' and literal '\'
	 * characters. A '\' can also be used for ending an attribute name by
	 * escaping the first character following the attribute. This is necessary
	 * if the first character after the valiable's value is to be a letter,
	 * without a space in between. Example: <code>'@title (@author)'</code>
	 * will (given the respective attributes) produce the title and author of a
	 * document, with the author's name in brackets.
	 * 
	 * @author sautter
	 */
	public static class AttributePattern {
		
		private static class AttributePatternElement {
			
			/**
			 * the literal value of this element, or the name of the attribute whose
			 * value to insert in its position
			 */
			public final String patternElement;
			
			/** is this element a reference to an attribute or a literal? */
			public final boolean isAttribute;
			
			AttributePatternElement(String patternElement, boolean isAttribute) {
				this.patternElement = patternElement;
				this.isAttribute = isAttribute;
			}
		}
		private final AttributePatternElement[] elements;
		
		/**
		 * Constructor
		 * @param pattern the string representation of the pattern
		 */
		public AttributePattern(String pattern) {
			this(parsePattern(pattern));
		}
		
		private AttributePattern(AttributePatternElement[] elements) {
			this.elements = elements;
		}
		
		/**
		 * @return an array holding the names of the attributes used in the
		 *         pattern
		 */
		public String[] getUsedAttributes() {
			StringVector displayTitleAttributes = new StringVector();
			for (int e = 0; e < this.elements.length; e++)
				if (this.elements[e].isAttribute)
					displayTitleAttributes.addElement(this.elements[e].patternElement);
			return displayTitleAttributes.toStringArray();
		}
		
		/**
		 * Create a custom display string from a set of attributes
		 * @param data the Properties holding the attribute values to create the
		 *            string from
		 * @return a display string assembled from the specified attribute
		 *         values according this pattern
		 */
		public String createDisplayString(Properties data) {
			
			//	no pattern
			if (this.elements.length == 0)
				return data.getProperty(GoldenGateSrsConstants.DOCUMENT_TITLE_ATTRIBUTE, "Unknown Document").toString();
			
			//	assemble string according to pattern
			StringBuffer string = new StringBuffer();
			for (int e = 0; e < this.elements.length; e++) {
				
				//	reference to attribute
				if (this.elements[e].isAttribute) {
					Object attribute = data.getProperty(this.elements[e].patternElement);
					if (attribute != null)
						string.append(attribute);
				}
				
				//	literal
				else string.append(this.elements[e].patternElement);
			}
			
			//	return result
			return string.toString();
		}
		
		/**
		 * Create an AttributePattern from its string representation
		 * @param pattern the pattern to parse
		 * @return an AttributePattern created from the specified string representation
		 */
		public static AttributePattern buildPattern(String pattern) {
			return new AttributePattern(parsePattern(pattern));
		}
		
		private static AttributePatternElement[] parsePattern(String pattern) {
			ArrayList apeList = new ArrayList();
			boolean inAttribute = false;
			int escaped = -1;
			StringBuffer sb = new StringBuffer();
			for (int c = 0; c < pattern.length(); c++) {
				char ch = pattern.charAt(c);
				
				//	escaped character
				if (escaped == c)
					sb.append(ch);
				
				//	escape next character
				else if (ch == '\\') {
					escaped = (c+1);
					
					//	end attribute at escaper
					if (inAttribute) {
						apeList.add(new AttributePatternElement(sb.toString(), true));
						sb = new StringBuffer();
						inAttribute = false;
					}
				}
				
				//	start of attribute
				else if (ch == '@') {
					
					//	store previous attribute or literal
					if (sb.length() != 0) {
						apeList.add(new AttributePatternElement(sb.toString(), inAttribute));
						sb = new StringBuffer();
					}
					inAttribute = true;
				}
				
				//	letter (can be both attribute or literal)
				else if ((('a' <= ch) && (ch <= 'z')) || (('A' <= ch) && (ch <= 'Z')))
					sb.append(ch);
				
				//	other character
				else {
					
					//	end attribute at non-letter
					if (inAttribute) {
						apeList.add(new AttributePatternElement(sb.toString(), true));
						sb = new StringBuffer();
						inAttribute = false;
					}
					
					//	append character
					sb.append(ch);
				}
			}
			
			//	anything left in buffer?
			if (sb.length() != 0)
				apeList.add(new AttributePatternElement(sb.toString(), inAttribute));
			
			return ((AttributePatternElement[]) apeList.toArray(new AttributePatternElement[apeList.size()]));
		}
	}
	
	/**
	 * Class representing a link to include in the navigation part of the search
	 * portal page. In order for the link to be displayed, at least one of label
	 * and icon have to be non-null, and at least one of link and onclick has to
	 * be non-null.
	 * 
	 * @author sautter
	 */
	public static class NavigationLink {
		
		/** the text for the link */
		public final String label;
		
		/**
		 * the path and name of an icon image for the link; must either be
		 * absolute, indicated by specifying a protocol (like 'http://'), or
		 * relative to the surrounding web-app's context path
		 */
		public final String icon;
		
		/** the URL the link points to */
		public final String link;
		
		/** the target to open the link in */
		public final String target;
		
		/** the JavaScript command to execute for a click on the link */
		public final String onclick;
		
		/**
		 * the JavaScript files to include for the link to work; must either be
		 * absolute, indicated by specifying a protocol (like 'http://'), or
		 * relative to the surrounding web-app's context path
		 */
		public final String[] scripts;
		
		/**
		 * Constructor linking to a URL (link & target)
		 * @param label the label for the link (the String to display, null
		 *            results in no text being displayed)
		 * @param icon the URL of the icon image for the link (relative to the
		 *            search portal's context path, null results in no image
		 *            being displayed)
		 * @param link the link URL (absolute, or relative to the search
		 *            portal's context path)
		 * @param target the name of the link target (null is equivalent to
		 *            _self)
		 */
		public NavigationLink(String label, String icon, String link, String target) {
			this.label = label;
			this.icon = icon;
			this.link = link;
			this.target = target;
			this.onclick = null;
			this.scripts = new String[0];
		}

		/**
		 * Constructor using JavaScript (onclick)
		 * @param label the label for the link (the String to display, null
		 *            results in no text being displayed)
		 * @param icon the URL of the icon image for the link (relative to the
		 *            search portal's context path, null results in no image
		 *            being displayed)
		 * @param onclick the JavaScript command to execute for a click on the
		 *            link
		 */
		public NavigationLink(String label, String icon, String onclick) {
			this(label, icon, onclick, new String[0]);
		}

		/**
		 * Constructor using JavaScript (onclick and scripts)
		 * @param label the label for the link (the String to display, null
		 *            results in no text being displayed)
		 * @param icon the URL of the icon image for the link (relative to the
		 *            search portal's context path, null results in no image
		 *            being displayed)
		 * @param onclick the JavaScript command to execute for a click on the
		 *            link
		 * @param scripts an array holding the addresses of JavaScript files to
		 *            include (relative to the search portal's context path, or
		 *            absolute)
		 */
		public NavigationLink(String label, String icon, String onclick, String[] scripts) {
			this.label = label;
			this.icon = icon;
			this.link = null;
			this.target = null;
			this.onclick = onclick;
			this.scripts = ((scripts == null) ? new String[0] : scripts);
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		public boolean equals(Object obj) {
			return ((obj != null) && this.toString().equals(obj.toString()));
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		public int hashCode() {
			return this.toString().hashCode();
		}
		
		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		public String toString() {
			return (this.label + "|" + this.icon + "|" + this.link + "|" + this.target + "|" + this.onclick);
		}
	}
	
//	/**
//	 * Search result linkers are plug-in components to the GoldenGATE SRS web
//	 * portal, which link the result documents of a search - or parts of them -
//	 * to external sources. This can, for instance, be linking locations to an
//	 * online mapping service, linking persons to a who-is-who, linking
//	 * organization names to the organization's homepage, or linking arbitrary
//	 * parts of the document text to a web search engine.
//	 * 
//	 * @author sautter
//	 */
//	public static interface SearchResultLinker extends SearchPortalConstants {
//		
//		/**
//		 * the name of the child folder to the servlet's root folder where the
//		 * search result linkers and their data are located
//		 */
//		public static final String RESULT_LINKER_FOLDER_NAME = "Linkers";
//
//		/**
//		 * Make the SearchResultLinker know the folder where its data is located
//		 * @param dataPath the folder containing the data for this
//		 *            SearchResultLinker
//		 */
//		public abstract void setDataPath(File dataPath);
//		
//		/**
//		 * Make the SearchResultLinker know the servlet it works in
//		 * @param parent the SearchPortalServlet this SearchResultLinker works
//		 *            in
//		 */
//		public abstract void setParent(SearchPortalServlet parent);
//		
//		/**
//		 * @return the name of the SearchResultLinker
//		 */
//		public abstract String getName();
//		
//		/**
//		 * Get the JavaScripts used by this linker. Paths are expected to be
//		 * either absolute (indicate by specifying a protocol, like 'http://'),
//		 * or relative to the linker's data path.
//		 * @return the names of the JavaScript files to load with the search
//		 *         portal page
//		 */
//		public abstract String[] getJavaScriptNames();
//		
//		/**
//		 * @return the JavaScript commands to execute when the portal main page
//		 *         is loaded ('onload' attribute of the html body tag)
//		 */
//		public abstract String[] getLoadCalls();
//		
//		/**
//		 * @return the JavaScript commands to execute when the portal main page
//		 *         is unloaded ('onunload' attribute of the html body tag)
//		 */
//		public abstract String[] getUnloadCalls();
//		
//		/**
//		 * Obtain links for an individual Annotation. This method is used by
//		 * search portal layouts when writing document lists, result indexes,
//		 * and the body of individual documents.
//		 * @param annotation the Annotation to link
//		 * @return a Link object representing the content of the link to add for
//		 *         the specified Annotation
//		 */
//		public abstract SearchResultLink[] getAnnotationLinks(Annotation annotation);
//		
//		/**
//		 * Obtain combined links for a set of annotations. This method is used by
//		 * search portal layouts when writing document lists or result indexes.
//		 * @param annotations the Annotations to link
//		 * @return a Link object representing the content of the combined link
//		 *         to add for the specified Annotations
//		 */
//		public abstract SearchResultLink[] getAnnotationSetLinks(Annotation[] annotations);
//		
//		/**
//		 * Obtain links for a search result document as a whole. This method is
//		 * used by search portal layouts when writing individual documents.
//		 * @param doc the search result document to link
//		 * @return a Link object representing the content of the link to add for
//		 *         the specified document
//		 */
//		public abstract SearchResultLink[] getDocumentLinks(MutableAnnotation doc);
//		
//		/**
//		 * Obtain links for a search result as a whole. This method is used by
//		 * search portal layouts when writing document lists.
//		 * @param docs the search result documents to link
//		 * @return a Link object representing the content of the link to add for
//		 *         the specified documents
//		 */
//		public abstract SearchResultLink[] getSearchResultLinks(MutableAnnotation[] docs);
//	}
	/**
	 * Search result linkers are plug-in components to the GoldenGATE SRS web
	 * portal, which link the result documents of a search - or parts of them -
	 * to external sources. This can, for instance, be linking locations to an
	 * online mapping service, linking persons to a who-is-who, linking
	 * organization names to the organization's homepage, or linking arbitrary
	 * parts of the document text to a web search engine.
	 * 
	 * @author sautter
	 */
	public static abstract class SearchResultLinker implements SearchPortalConstants {
		
		/** the folder where the linker's data is located */
		protected File dataPath;
		
		/** the search portal servlet the linker works in */
		protected SearchPortalServlet parent;
		
		/**
		 * the name of the child folder to the servlet's root folder where the
		 * search result linkers and their data are located
		 */
		public static final String RESULT_LINKER_FOLDER_NAME = "Linkers";

		/**
		 * Make the SearchResultLinker know the servlet it works in
		 * @param parent the SearchPortalServlet this SearchResultLinker works
		 *            in
		 */
		public void setParent(SearchPortalServlet parent) {
			this.parent = parent;
			if (this.dataPath != null)
				this.init();
		}
		
		/**
		 * Make the search result linker know the folder where its data is
		 * located.
		 * @param dataPath the folder containing the data for this linker
		 */
		public final void setDataPath(File dataPath) {
			this.dataPath = dataPath;
			if (this.parent != null)
				this.init();
		}
		
		/**
		 * Initilaize the linker. This method is called after the parent and
		 * data path are set. This default implementation does nothing, sub
		 * classes are welcome to overwrite it as needed.
		 */
		protected void init() {}
		
		/**
		 * @return the name of the linker
		 */
		public abstract String getName();
		
		/**
		 * Get the JavaScripts used by this linker. Paths are expected to be
		 * either absolute (indicate by specifying a protocol, like 'http://'),
		 * or relative to the linker's data path. This default implementation
		 * returns an empty array, sub classes are welcome to overwrite it as
		 * needed.
		 * @return the names of the JavaScript files to load with the search
		 *         portal page
		 */
		public String[] getJavaScriptNames() {
			return new String[0];
		}
		
		/**
		 * Get the JavaScript commands to execute when the portal main page is
		 * loaded ('onload' listener of the HTML body tag). This default
		 * implementation returns an empty array, sub classes are welcome to
		 * overwrite it as needed.
		 * @return the JavaScript commands to execute when the portal main page
		 *         is loaded
		 */
		public String[] getLoadCalls() {
			return new String[0];
		}
		
		/**
		 * Get the JavaScript commands to execute when the portal main page is
		 * unloaded ('onunload' listener of the HTML body tag). This default
		 * implementation returns an empty array, sub classes are welcome to
		 * overwrite it as needed.
		 * @return the JavaScript commands to execute when the portal main page
		 *         is unloaded
		 */
		public String[] getUnloadCalls() {
			return new String[0];
		}
		
		/**
		 * Obtain links for an individual Annotation. This method is used by
		 * search portal layouts when writing document lists, result indexes,
		 * and the body of individual documents. In case of the former two, the
		 * argument will always be an index result element, and only in the
		 * latter case a plain annotation. This default implementation returns
		 * an empty array, sub classes are welcome to overwrite it as needed.
		 * @param annotation the Annotation to link
		 * @return a Link object representing the content of the link to add for
		 *         the specified Annotation
		 */
		public SearchResultLink[] getAnnotationLinks(Annotation annotation) {
			return new SearchResultLink[0];
		}
		
		/**
		 * Obtain combined links for a set of annotations. This method is used
		 * by search portal layouts when writing document lists or result
		 * indexes. The runtime class for the argument array's elements thus
		 * always is <code>IndexResultElement</code>. This default
		 * implementation returns an empty array, sub classes are welcome to
		 * overwrite it as needed.
		 * @param annotations the annotations to link
		 * @return an array holding the links for the argument annotations
		 */
		public SearchResultLink[] getAnnotationSetLinks(Annotation[] annotations) {
			return new SearchResultLink[0];
		}
		
		/**
		 * Obtain links for a search result document as a whole. This method is
		 * used by search portal layouts when writing individual documents. This
		 * default implementation returns an empty array, sub classes are
		 * welcome to overwrite it as needed.
		 * @param doc the search result document to link
		 * @return an array holding the links for the argument document
		 */
		public SearchResultLink[] getDocumentLinks(MutableAnnotation doc) {
			return new SearchResultLink[0];
		}
		
		/**
		 * Obtain links for a multi document search result as a whole. This
		 * method is used by search portal layouts when writing document lists.
		 * This default implementation returns an empty array, sub classes are
		 * welcome to overwrite it as needed.
		 * @param docs the search result documents to link
		 * @return an array holding the links for the argument documents
		 */
		public SearchResultLink[] getSearchResultLinks(MutableAnnotation[] docs) {
			return new SearchResultLink[0];
		}
		
		/**
		 * Write content to the head of an HTML page, like JavaScript or CSS
		 * styles. This method is used by search portal layouts for all result
		 * pages. This default implementation does nothing, sub classes are
		 * welcome to overwrite it as needed.
		 * @param hpb the HTML page builder to write to
		 */
		public void writePageHeadExtensions(HtmlPageBuilder hpb) throws IOException {}
		
		/**
		 * Write content to the head of an HTML page, like JavaScript or CSS
		 * styles. This method is used by search portal layouts when writing
		 * individual documents. This default implementation does nothing, sub
		 * classes are welcome to overwrite it as needed.
		 * @param doc the search result document to extend the page head for
		 * @param hpb the HTML page builder to write to
		 */
		public void writePageHeadExtensions(MutableAnnotation doc, HtmlPageBuilder hpb) throws IOException {}
		
		/**
		 * Write content to the head of an HTML page, like JavaScript or CSS
		 * styles. This method is used by search portal layouts when writing
		 * document lists. This default implementation does nothing, sub classes
		 * are welcome to overwrite it as needed.
		 * @param docs the search result documents to extend the page head for
		 * @param hpb the HTML page builder to write to
		 */
		public void writePageHeadExtensions(MutableAnnotation[] docs, HtmlPageBuilder hpb) throws IOException {}
	}
	
	/**
	 * Representation of the content of a link. The actual layout and placement
	 * of the link is up to the web portal servlet and it's configuration.
	 * 
	 * @author sautter
	 */
	public static class SearchResultLink {
		
		/**
		 * the category of the link (one of XML_DOCUMENT, VISUALIZATION, or
		 * EXTERNAL_INFORMATION)
		 */
		public final String category;
		
		/** the class name of the SearchResultLinker who created the link */
		public final String linkerClassName;
		
		/**
		 * the label to use for representing the link (may be null if iconUrl is
		 * used)
		 */
		public final String label;
		
		/**
		 * the URL of the image to use for representing the link (may be null if
		 * label is used)
		 */
		public final String iconUrl;
		
		/** the titel (tooltip) to use for representing the link, may be null */
		public final String title;
		
		/** the URL the link leads to, may be null if onclick is used */
		public final String href;
		
		/**
		 * the JavaScript function call to execute when the link is clicked, may
		 * be null if href is used
		 */
		public final String onclick;
		
		/**
		 * Constructor
		 * @param category the category of the link (one of XML_DOCUMENT,
		 *            VISUALIZATION, EXTERNAL_INFORMATION, or FUNCTIONALITY)
		 * @param linkerClassName the class name of the SearchResultLinker who
		 *            created the link
		 * @param label the label to use for representing the link (may be null
		 *            if iconUrl is used)
		 * @param iconUrl the URL of the image to use for representing the link
		 *            (may be null if label is used)
		 * @param title the titel (tooltip) to use for representing the link,
		 *            may be null
		 * @param href the URL the link leads to, may be null if onclick is used
		 * @param onclick the JavaScript function call to execute when the link
		 *            is clicked, may be null if href is used
		 */
		public SearchResultLink(String category, String linkerClassName, String label, String iconUrl, String title, String href, String onclick) {
			this.category = ((category == null) ? EXTERNAL_INFORMATION : category);
			this.linkerClassName = linkerClassName;
			this.label = label;
			this.iconUrl = iconUrl;
			this.title = title;
			this.href = href;
			this.onclick = onclick;
		}
		
		/**
		 * @return an XML representation of this link
		 */
		public String toXml() {
			return ("<" + LINK_NODE_TYPE + 
				" " + LINK_CATEGORY_ATTRIBUTE + "=\"" + this.category + "\"" +
				(((this.linkerClassName == null) || (this.linkerClassName.length() == 0)) ? "" : (" " + LINKER_CLASS_NAME_ATTRIBUTE + "=\"" + this.linkerClassName + "\"")) +
				(((this.label == null) || (this.label.length() == 0)) ? "" : (" " + LINK_LABEL_ATTRIBUTE + "=\"" + AnnotationUtils.escapeForXml(this.label, true) + "\"")) +
				(((this.iconUrl == null) || (this.iconUrl.length() == 0)) ? "" : (" " + LINK_ICON_URL_ATTRIBUTE + "=\"" + AnnotationUtils.escapeForXml(this.iconUrl, true) + "\"")) +
				(((this.title == null) || (this.title.length() == 0)) ? "" : (" " + LINK_TITLE_ATTRIBUTE + "=\"" + AnnotationUtils.escapeForXml(this.title, true) + "\"")) +
				(((this.href == null) || (this.href.length() == 0)) ? "" : (" " + LINK_HREF_ATTRIBUTE + "=\"" + AnnotationUtils.escapeForXml(this.href, true) + "\"")) +
				(((this.onclick == null) || (this.onclick.length() == 0)) ? "" : (" " + LINK_ONCLICK_ATTRIBUTE + "=\"" + AnnotationUtils.escapeForXml(this.onclick, true) + "\"")) +
				"/>");
		}
	}
}
