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
package de.uka.ipd.idaho.goldenGateServer.dio;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.ReadOnlyDocument;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DioDocumentList;
import de.uka.ipd.idaho.goldenGateServer.dst.DocumentStoreConstants;

/**
 * Interface holding constants for communicating with the GoldenGateDIO
 * 
 * @author sautter
 */
public interface GoldenGateDioConstants extends DocumentStoreConstants {
	
	/** the dummy session ID for the document servlet to retrieve documents from a backing DIO */
	public static final String DOCUMENT_SERVLET_SESSION_ID = "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD";
	
	
	/** the command for loading a document */
	public static final String GET_DOCUMENT = "DIO_GET_DOCUMENT";
	
	/** the command for loading a document */
	public static final String GET_DOCUMENT_AS_STREAM = "DIO_GET_DOCUMENT_AS_STREAM";
	
	/** the command for uploading a document, i.e. adding a new document to the collection */
	public static final String UPLOAD_DOCUMENT = "DIO_UPLOAD_DOCUMENT";
	
	/** the command for checking out a document from a storage */
	public static final String CHECKOUT_DOCUMENT = "DIO_CHECKOUT_DOCUMENT";
	
	/** the command for checking out a document from a storage as a stream */
	public static final String CHECKOUT_DOCUMENT_AS_STREAM = "DIO_CHECKOUT_DOCUMENT_AS_STREAM";
	
	/** the command for updating a document, i.e. uploading a new version */
	public static final String UPDATE_DOCUMENT = "DIO_UPDATE_DOCUMENT";
	
	/** the command for deleting a document */
	public static final String DELETE_DOCUMENT = "DIO_DELETE_DOCUMENT";
	
	/** the command for releasing a document that was previously checked out */
	public static final String RELEASE_DOCUMENT = "DIO_RELEASE_DOCUMENT";
	
	/** the command for loading a list of all documents in the DIO */
	public static final String GET_DOCUMENT_LIST = "DIO_GET_DOCUMENT_LIST";
	
	/** the command for loading a list of all documents in the DIO */
	public static final String GET_DOCUMENT_LIST_SHARED = "DIO_GET_DOCUMENT_LIST_SHARED";
	
	/** the command for retrieving the update protocol of document, i.e. messages that describe which other modifications the new version incurred throughout the server (only modifications that happen synchronously on update notification, though) */
	public static final String GET_UPDATE_PROTOCOL = "DIO_GET_UPDATE_PROTOCOL";
	
	/** the last entry in a document update protocol, indicating that the update is fully propagated through the server */
	public static final String UPDATE_COMPLETE = "Document update complete";
	
	/** the last entry in a document update protocol belonging to a deletion, indicating that the update is fully propagated through the server */
	public static final String DELETION_COMPLETE = "Document deletion complete";
	
	
	/** the permission for uploading new documents to the DIO */
	public static final String UPLOAD_DOCUMENT_PERMISSION = "DIO.UploadDocument";
	
	/** the permission for updating existing documents in the DIO */
	public static final String UPDATE_DOCUMENT_PERMISSION = "DIO.UpdateDocument";
	
	/** the permission for deleting documents from the DIO */
	public static final String DELETE_DOCUMENT_PERMISSION = "DIO.DeleteDocument";
	
	
	/** the attribute holding keywords of a document */
	public static final String DOCUMENT_KEYWORDS_ATTRIBUTE = "docKeywords";
//	
//	/** the attribute holding the name of the user who has currently checked out a document for working */
//	public static final String CHECKOUT_USER_ATTRIBUTE = "checkoutUser";
//	
//	/** the attribute holding the time when a document was checked out */
//	public static final String CHECKOUT_TIME_ATTRIBUTE = "checkoutTime";
	
	
	/** the attribute holding external document identifiers, e.g. from some third-party meta data */
	public static final String EXTERNAL_IDENTIFIER_ATTRIBUTE = "externalIdentifier";
	
	/** the handling mode for external identifiers that throws a DuplicateExternalIdentifierException */
	public static final String EXTERNAL_IDENTIFIER_MODE_CHECK = "check";
	
	/** the handling mode for external identifiers that makes DIO ignore external identifier duplication */
	public static final String EXTERNAL_IDENTIFIER_MODE_IGNORE = "ignore";
	
	/** the indicator for duplicate external identifier exception data coming */
	public static final String DUPLICATE_EXTERNAL_IDENTIFIER = "DUPLICATE_EXTERNAL_IDENTIFIER";
	
	/**
	 * Exception that indicates an external identifier conflict on document upload
	 * 
	 * @author sautter
	 */
	public static class DuplicateExternalIdentifierException extends IOException {
		public final String externalIdentifierAttributeName;
		public final String conflictingExternalIdentifier;
		private String[] listFieldNames;
		private DocumentListElement[] conflictingDocuments;
		
		/**
		 * @param externalIdentifierAttributeName
		 * @param conflictingExternalIdentifier
		 * @param conflictingDocuments
		 */
		public DuplicateExternalIdentifierException(String externalIdentifierAttributeName, String conflictingExternalIdentifier, DioDocumentList conflictingDocuments) {
			super("The " + externalIdentifierAttributeName + " '" + conflictingExternalIdentifier + "' is already assigned to another document.");
			this.externalIdentifierAttributeName = externalIdentifierAttributeName;
			this.conflictingExternalIdentifier = conflictingExternalIdentifier;
			this.listFieldNames = conflictingDocuments.listFieldNames;
			ArrayList conflictingDocumentList = new ArrayList();
			while (conflictingDocuments.hasNextDocument())
				conflictingDocumentList.add(conflictingDocuments.getNextDocument());
			this.conflictingDocuments = ((DocumentListElement[]) conflictingDocumentList.toArray(new DocumentListElement[conflictingDocumentList.size()]));
		}
		
		/**
		 * @return an array loading the internal IDs of the documents with
		 *         conflicting external identifiers
		 */
		public DocumentListElement[] getConflictingDocuments() {
			DocumentListElement[] conflictingDocumentsData = new DocumentListElement[this.conflictingDocuments.length];
			System.arraycopy(this.conflictingDocuments, 0, conflictingDocumentsData, 0, conflictingDocumentsData.length);
			return conflictingDocumentsData;
		}
		
		/**
		 * Write the list of conflicting documents to a given writer.
		 * @param out the writer to write to
		 * @throws IOException
		 */
		public void writeConflictingDocuments(Writer out) throws IOException {
			BufferedWriter bw = ((out instanceof BufferedWriter) ? ((BufferedWriter) out) : new BufferedWriter(out));
			for (int f = 0; f < this.listFieldNames.length; f++) {
				if (f != 0) bw.write(",");
				bw.write('"' + this.listFieldNames[f] + '"');
			}
			bw.newLine();
			for (int c = 0; c < this.conflictingDocuments.length; c++) {
				bw.write(this.conflictingDocuments[c].toCsvString('"', this.listFieldNames));
				bw.newLine();
			}
			if (bw != out)
				bw.flush();
		}
	}
//	
//	/**
//	 * Extension for IO operations on GoldenGATE DIO. By means of registering
//	 * extensions with GoldenGATE DIO, other components can add additional
//	 * functionality and control to DIO's IO operations. For convenience, all
//	 * methods are implemented to do nothing, so sub classes only have to
//	 * implement the methods required for their specific functionality. Release
//	 * operations cannot be prevented, as it does not make any sense to filter
//	 * release operations after a prior checkout or update operation has been
//	 * permitted.
//	 * 
//	 * @author sautter
//	 */
//	public static abstract class DocumentIoExtension implements Comparable {
//		
//		/**
//		 * The extension's priority influences order of application if multiple
//		 * extensions are present, on a scale from 0 to 100, with higher
//		 * priority extensions being applied earlier. In favor of performance,
//		 * extensions that filter out data should be applied before ones that
//		 * add data, as the reverse order could cause data to be added and later
//		 * filtered out again. This especially applies to filtering document
//		 * lists.
//		 */
//		public final int priority;
//		
//		public static final int MIN_PRIORITY = 0;
//		public static final int MAX_PRIORITY = 100;
//		
//		/**
//		 * Constructor
//		 * @param priority the extensions's priority, on a scale from 0 to 100
//		 */
//		public DocumentIoExtension(int priority) {
//			this.priority = Math.max(MIN_PRIORITY, Math.min(MAX_PRIORITY, priority));
//		}
//		
//		/**
//		 * Compares two extensions based on their priority, enforcing decreasing
//		 * priority order.
//		 * @see java.lang.Comparable#compareTo(java.lang.Object)
//		 */
//		public final int compareTo(Object obj) {
//			return ((obj instanceof DocumentIoExtension) ? (((DocumentIoExtension) obj).priority - this.priority) : -1);
//		}
//		
//		/**
//		 * Get the size of a document list for a set of filter predicates. This
//		 * is to decide whether to return list elements or to require additional
//		 * filtering. This default implementation simply returns the argument
//		 * selectivity. Sub classes that overwrite the extendList() method to
//		 * apply some filter should overwrite this method as well.
//		 * @param selectivity the selectivity estimated so far
//		 * @param filter the filter to estimate the selectivity for
//		 * @param user the user submitting the filter
//		 * @return a selectivity estimate for the specified filter
//		 */
//		public int getSelectivity(int selectivity, Properties filter, String user) {
//			return selectivity;
//		}
//		
//		/**
//		 * Wrap an extension around a document list. An extension can add or
//		 * remove attributes to/from document list elements, or filter out
//		 * document list elements altogether. This default implementations
//		 * simply returns the argument document list, sub classes are welcome to
//		 * overwrite it as needed.<br>
//		 * This method should not return null. If it does, the extension is
//		 * ignored.
//		 * @param dl the document list to wrap
//		 * @param user the user retrieving the document list
//		 * @param headOnly if set to true, this method must return an empty
//		 *            list, only containing the header data (field names and
//		 *            optionally attribute value summaries)
//		 * @param filter a properties object containing filter predicates for
//		 *            the document list
//		 * @return a document list adjusted according to the specific extension
//		 */
//		public DocumentList extendList(DocumentList dl, String user, boolean headOnly, Properties filter) {
//			return dl;
//		}
//		
//		/**
//		 * Pre-check or extend an upload operation. DIO invokes this method with
//		 * all registered extensions before actually storing the argument
//		 * document. This method may modify the document by adding or removing
//		 * attributes, and it may throw an IOException to prevent the upload
//		 * operation altogether. This default implementation does nothing, sub
//		 * classes are welcome to overwrite it as needed.<br>
//		 * Note: an upload differs from an update in that it never modifies an
//		 * existing document, but always creates a new one. This implies that an
//		 * upload is never preceded by a checkout. In addition, the uploading
//		 * user does not get the lock on the document, so no release is
//		 * necessary.
//		 * @param document the document the user is trying to store in DIO
//		 * @param user the user trying to upload the document
//		 * @see de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DocumentIoExtension#extendUpdate(String, QueriableAnnotation, String)
//		 */
//		public void extendUpload(QueriableAnnotation document, String user) throws IOException {}
//		
//		/**
//		 * Pre-check or extend a get operation. DIO invokes this method with all
//		 * registered extensions before actually getting the document. This
//		 * method may modify the document by adding or removing attributes, and
//		 * it may throw an IOException to prevent the checkout operation
//		 * altogether. Before invoking this method, DIO sets all necessary
//		 * attributes with the argument document reader, but does not read from
//		 * it yet. This means that this method can still modify the reader's
//		 * attributes. Implementations returning the argument reader should not
//		 * read from it, as this might cause problems in subsequent extensions.
//		 * However, implementations are allowed to wrap the argument reader in a
//		 * new document reader and return the latter in an unread-from state.
//		 * This default implementation simply returns the argument document
//		 * reader, sub classes are welcome to overwrite it as needed.<br>
//		 * This method should not return null. If it does, the extension is
//		 * ignored.
//		 * @param documentId the ID of the document to be retrieved
//		 * @param dr the document reader the document will be read from
//		 */
//		public DocumentReader extendGet(String documentId, DocumentReader dr) throws IOException {
//			return dr;
//		}
//		
//		/**
//		 * Pre-check or extend a checkout operation. DIO invokes this method
//		 * with all registered extensions before actually checking out the
//		 * document. This method may modify the document by adding or removing
//		 * attributes, and it may throw an IOException to prevent the checkout
//		 * operation altogether. Before invoking this method, DIO sets all
//		 * necessary attributes with the argument document reader, but does not
//		 * read from it yet. This means that this method can still modify the
//		 * reader's attributes. Implementations returning the argument reader
//		 * should not read from it, as this might cause problems in subsequent
//		 * extensions. However, implementations are allowed to wrap the argument
//		 * reader in a new document reader and return the latter in an
//		 * unread-from state. This default implementation simply returns the
//		 * argument document reader, sub classes are welcome to overwrite it as
//		 * needed.<br>
//		 * This method should not return null. If it does, the extension is
//		 * ignored.
//		 * @param documentId the ID of the document to be checked out
//		 * @param dr the document reader the document will be read from
//		 * @param user the user trying to check out the document
//		 */
//		public DocumentReader extendCheckout(String documentId, DocumentReader dr, String user) throws IOException {
//			return dr;
//		}
//		
//		/**
//		 * Pre-check or extend an update operation. DIO invokes this method with
//		 * all registered extensions before actually storing the argument
//		 * document. This method may modify the document by adding or removing
//		 * attributes, and it may throw an IOException to prevent the update
//		 * operation altogether. This default implementation does nothing, sub
//		 * classes are welcome to overwrite it as needed.<br>
//		 * Note: an update differs from an upload in that it can modify an
//		 * existing document. A new document is created only if no document with
//		 * the specified ID exists so far. To update an existing document, the
//		 * updating user has to perform a checkout first. When creating a new
//		 * document, the updating user gets the lock on the document. He has to
//		 * release it before other users can work on it.
//		 * @param documentId the ID of the document to be updated
//		 * @param document the version of the document the user is trying to
//		 *            store in DIO
//		 * @param user the user trying to update the document
//		 * @see de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DocumentIoExtension#extendUpload(QueriableAnnotation, String)
//		 */
//		public void extendUpdate(String documentId, QueriableAnnotation document, String user) throws IOException {}
//		
//		/**
//		 * Pre-check a delete operation. DIO invokes this method with all
//		 * registered extensions before actually deleting the argument document.
//		 * This method may throw an IOException to prevent the delete operation.
//		 * This default implementation does nothing, sub classes are welcome to
//		 * overwrite it as needed.
//		 * @param documentId the ID of the document to be deleted
//		 * @param user the user trying to delete the document
//		 */
//		public void extendDelete(String documentId, String user) throws IOException {}
//		
//		/**
//		 * Pre-check a release operation. DIO invokes this method with all
//		 * registered extensions before actually releasing the argument
//		 * document. This method may throw an IOException to prevent the release
//		 * operation. This default implementation does nothing, sub classes are
//		 * welcome to overwrite it as needed.
//		 * @param documentId the ID of the document to be released
//		 * @param user the user trying to release the document
//		 */
//		public void extendRelease(String documentId, String user) throws IOException {}
//	}
	
	/**
	 * Constant set containing the names of document attributes for which
	 * document lists do not contain value summaries. This set is immutable, any
	 * modification methods are implemented to simply return false.
	 */
	public static final Set summarylessAttributes = new LinkedHashSet() {
		{
			String[] summarylessAttributeNames = {
				DOCUMENT_ID_ATTRIBUTE,
				DOCUMENT_NAME_ATTRIBUTE,
				EXTERNAL_IDENTIFIER_ATTRIBUTE,
				DOCUMENT_TITLE_ATTRIBUTE,
				DOCUMENT_KEYWORDS_ATTRIBUTE,
				CHECKIN_TIME_ATTRIBUTE,
				UPDATE_TIME_ATTRIBUTE,
				CHECKOUT_TIME_ATTRIBUTE,
				DOCUMENT_VERSION_ATTRIBUTE,
			};
			Arrays.sort(summarylessAttributeNames);
			for (int a = 0; a < summarylessAttributeNames.length; a++)
				super.add(summarylessAttributeNames[a]);
		}
		public boolean add(Object o) {
			return false;
		}
		public boolean remove(Object o) {
			return false;
		}
		public void clear() {}
		public Iterator iterator() {
			final Iterator it = super.iterator();
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
		public boolean addAll(Collection coll) {
			return false;
		}
		public boolean removeAll(Collection coll) {
			return false;
		}
		public boolean retainAll(Collection coll) {
			return false;
		}
	};
	
	
	/**
	 * Constant set containing the names of document attributes which can be
	 * used for document list filters. This set is immutable, any modification
	 * methods are implemented to simply return false.
	 */
	public static final Set filterableAttributes = new LinkedHashSet() {
		{
			//	TODO keep this in sync with table definition
			String[] docTableFields = {
					
					//	- identifier data
//					DOCUMENT_ID_ATTRIBUTE, // nobody will filter by a document ID
					EXTERNAL_IDENTIFIER_ATTRIBUTE,
					DOCUMENT_NAME_ATTRIBUTE,
					
					//	- meta data
					DOCUMENT_AUTHOR_ATTRIBUTE,
					DOCUMENT_DATE_ATTRIBUTE,
					DOCUMENT_TITLE_ATTRIBUTE,
					DOCUMENT_KEYWORDS_ATTRIBUTE,
					
					//	- management data
					CHECKIN_USER_ATTRIBUTE,
					CHECKIN_TIME_ATTRIBUTE,
					CHECKOUT_USER_ATTRIBUTE,
					CHECKOUT_TIME_ATTRIBUTE,
					UPDATE_USER_ATTRIBUTE,
					UPDATE_TIME_ATTRIBUTE,
					DOCUMENT_VERSION_ATTRIBUTE,
			};
			Arrays.sort(docTableFields);
			for (int a = 0; a < docTableFields.length; a++)
				super.add(docTableFields[a]);
		}
		public boolean add(Object o) {
			return false;
		}
		public boolean remove(Object o) {
			return false;
		}
		public void clear() {}
		public Iterator iterator() {
			final Iterator it = super.iterator();
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
		public boolean addAll(Collection coll) {
			return false;
		}
		public boolean removeAll(Collection coll) {
			return false;
		}
		public boolean retainAll(Collection coll) {
			return false;
		}
	};
	
	
	/**
	 * Constant set containing the names of numeric document attributes, for
	 * which specific comparison operators can be used for document list
	 * filters. This set is immutable, any modification methods are implemented
	 * to simply return false.
	 */
	public static final Set numericAttributes = new LinkedHashSet() {
		{
			//	TODO keep this in sync with table definition
			String[] numericFieldNames = {
					DOCUMENT_DATE_ATTRIBUTE,
					CHECKIN_TIME_ATTRIBUTE,
					UPDATE_TIME_ATTRIBUTE,
					CHECKOUT_TIME_ATTRIBUTE,
					DOCUMENT_VERSION_ATTRIBUTE,
			};
			Arrays.sort(numericFieldNames);
			for (int a = 0; a < numericFieldNames.length; a++)
				super.add(numericFieldNames[a]);
		}
		public boolean add(Object o) {
			return false;
		}
		public boolean remove(Object o) {
			return false;
		}
		public void clear() {}
		public Iterator iterator() {
			final Iterator it = super.iterator();
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
		public boolean addAll(Collection coll) {
			return false;
		}
		public boolean removeAll(Collection coll) {
			return false;
		}
		public boolean retainAll(Collection coll) {
			return false;
		}
	};
	
	
	/**
	 * Constant set containing the comparison operators that can be used for
	 * numeric document attributes in document list filters. This set is
	 * immutable, any modification methods are implemented to simply return
	 * false.
	 */
	public static final Set numericOperators = new LinkedHashSet() {
		{
			String[] numericOperators = {
					">",
					">=",
					"=",
					"<=",
					"<",
			};
			Arrays.sort(numericOperators);
			for (int a = 0; a < numericOperators.length; a++)
				super.add(numericOperators[a]);
		}
		public boolean add(Object o) {
			return false;
		}
		public boolean remove(Object o) {
			return false;
		}
		public void clear() {}
		public Iterator iterator() {
			final Iterator it = super.iterator();
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
		public boolean addAll(Collection coll) {
			return false;
		}
		public boolean removeAll(Collection coll) {
			return false;
		}
		public boolean retainAll(Collection coll) {
			return false;
		}
	};
	
	/**
	 * GoldenGATE DIO specific document storage event, adding types for
	 * checkout and release.
	 * 
	 * @author sautter
	 */
	public static class DioDocumentEvent extends DataObjectEvent {
		
		/**
		 * Specialized storage listener for GoldenGATE DIO, receiving notifications of
		 * document checkout and release operations, besides update and delete
		 * operations.
		 * 
		 * @author sautter
		 */
		public static abstract class DioDocumentEventListener extends GoldenGateServerEventListener {
			
			/* (non-Javadoc)
			 * @see de.uka.ipd.idaho.goldenGateServer.events.GoldenGateServerEventListener#notify(de.uka.ipd.idaho.goldenGateServer.events.GoldenGateServerEvent)
			 */
			public void notify(GoldenGateServerEvent gse) {
				if (gse instanceof DioDocumentEvent) {
					DioDocumentEvent dse = ((DioDocumentEvent) gse);
					if (dse.type == CHECKOUT_TYPE)
						this.documentCheckedOut(dse);
					else if (dse.type == RELEASE_TYPE)
						this.documentReleased(dse);
					else if (dse.type == UPDATE_TYPE)
						this.documentUpdated(dse);
					else if (dse.type == DELETE_TYPE)
						this.documentDeleted(dse);
				}
			}
			
			/**
			 * Receive notification that a document was checked out by a user. The
			 * actual document will be null in this type of notification, and the
			 * version number will be -1.
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            checkout
			 */
			public abstract void documentCheckedOut(DioDocumentEvent dse);
			
			/**
			 * Receive notification that a document was updated (can be both a new
			 * document or an updated version of an existing document)
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            update
			 */
			public abstract void documentUpdated(DioDocumentEvent dse);
			
			/**
			 * Receive notification that a document was deleted
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            deletion
			 */
			public abstract void documentDeleted(DioDocumentEvent dse);
			
			/**
			 * Receive notification that a document was released by a user. The actual
			 * document will be null in this type of notification, and the version
			 * number will be -1.
			 * @param dse the DocumentStorageEvent providing detail information on the
			 *            release
			 */
			public abstract void documentReleased(DioDocumentEvent dse);
		}
		
		/**
		 * The document affected by the event, null for deletion events. This
		 * document is strictly read-only, any attempt of modification will
		 * result in a RuntimException being thrown.
		 */
		public final QueriableAnnotation document;
		
		/**
		 * Constructor for update events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was updated
		 * @param document the actual document that was updated
		 * @param version the current version number of the document (after the
		 *            update)
		 * @param sourceClassName the class name of the component issuing the event
		 * @param eventTime the timstamp of the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 */
		public DioDocumentEvent(String user, String documentId, QueriableAnnotation document, int version, String sourceClassName, long eventTime, EventLogger logger) {
			this(user, documentId, document, version, UPDATE_TYPE, sourceClassName, eventTime, logger);
		}
		
		/**
		 * Constructor for deletion events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was deleted
		 * @param sourceClassName the class name of the component issuing the event
		 * @param eventTime the timstamp of the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 */
		public DioDocumentEvent(String user, String documentId, String sourceClassName, long eventTime, EventLogger logger) {
			this(user, documentId, null, -1, DELETE_TYPE, sourceClassName, eventTime, logger);
		}
		
		/**
		 * Constructor for custom-type events
		 * @param user the name of the user who caused the event
		 * @param documentId the ID of the document that was updated
		 * @param document the actual document that was updated
		 * @param version the current version number of the document (after the
		 *            update)
		 * @param type the event type (used for dispatching)
		 * @param sourceClassName the class name of the component issuing the event
		 * @param eventTime the timstamp of the event
		 * @param logger a DocumentStorageLogger to collect log messages while the
		 *            event is being processed in listeners
		 */
		public DioDocumentEvent(String user, String documentId, QueriableAnnotation document, int version, int type, String sourceClassName, long eventTime, EventLogger logger) {
			super(user, documentId, version, type, sourceClassName, eventTime, logger);
			this.document = ((document == null) ? null : new ReadOnlyDocument(document, "The document contained in a DioDocumentEvent cannot be modified."));
		}
		
		/**
		 * Parse a document event from its string representation returned by the
		 * getParameterString() method.
		 * @param data the string to parse
		 * @return a document event created from the specified data
		 */
		public static DioDocumentEvent parseEvent(String data) {
			String[] dataItems = data.split("\\s");
			return new DioDocumentEvent(dataItems[4], dataItems[5], null, Integer.parseInt(dataItems[6]), Integer.parseInt(dataItems[0]), dataItems[1], Long.parseLong(dataItems[2]), null);
		}
	}
}
