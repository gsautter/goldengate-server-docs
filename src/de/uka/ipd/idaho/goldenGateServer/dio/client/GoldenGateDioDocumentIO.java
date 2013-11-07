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
package de.uka.ipd.idaho.goldenGateServer.dio.client;


import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.SpinnerNumberModel;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.DefaultTableColumnModel;
import javax.swing.table.JTableHeader;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.ControllingProgressMonitor;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.ComponentInitializer;
import de.uka.ipd.idaho.gamta.util.GamtaClassLoader.InputStreamProvider;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML.DocumentReader;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.gamta.util.swing.ProgressMonitorDialog;
import de.uka.ipd.idaho.gamta.util.swing.ProgressMonitorWindow;
import de.uka.ipd.idaho.goldenGate.DocumentEditor;
import de.uka.ipd.idaho.goldenGate.plugins.AbstractDocumentIO;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentSaveOperation;
import de.uka.ipd.idaho.goldenGate.plugins.GoldenGatePluginDataProvider;
import de.uka.ipd.idaho.goldenGate.plugins.MonitorableDocumentIO;
import de.uka.ipd.idaho.goldenGate.plugins.MonitorableDocumentSaveOperation;
import de.uka.ipd.idaho.goldenGate.plugins.PluginDataProviderPrefixBased;
import de.uka.ipd.idaho.goldenGate.util.DialogPanel;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentList.DocumentAttributeSummary;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentListBuffer;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentListElement;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManagerPlugin;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringRelation;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * DocumentIO plugin for the GoldenGATE document editor supporting document IO
 * with GoldenGATE Document IO Server. If the backing DIO is unreachable, this
 * client will save documents in a local cache and upload then the next time the
 * the user successfully logs in to the backing DIO. However, if the DIO login
 * fails due to invalid authentication data rather than network problems,
 * documents will not be cached.
 * 
 * @author sautter
 */
public class GoldenGateDioDocumentIO extends AbstractDocumentIO implements GoldenGateDioConstants, MonitorableDocumentIO {
	
	/**
	 * A document list extension provides additional functionality for the
	 * context menu of the 'Open Document' dialog.
	 * 
	 * @author sautter
	 */
	public static abstract class DocumentListExtension {
		
		/** The plugin's data provider */
		protected GoldenGatePluginDataProvider dataProvider;
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.GoldenGatePlugin#setDataProvider(de.uka.ipd.idaho.goldenGate.plugins.GoldenGatePluginDataProvider)
		 */
		public void setDataProvider(GoldenGatePluginDataProvider dataProvider) {
			this.dataProvider = dataProvider;
		}
		
		/**
		 * Provide a set of additional items for the context menu of the 'Open
		 * Document' dialog, specifically for the argument string tupel, which
		 * holds the data of the row currently selected. Server interaction
		 * should be done through the argument authenticated client, which is
		 * exactly the one through which the document list was retrieved. If an
		 * extension does not have any functionality to add for a specific
		 * document, this method may return either of null or an empty array.
		 * Any functionality implemented in a menu item returned by this method
		 * should stick to providing additional information on a document; it
		 * must not modify the argument document data.
		 * @param docData the data of the document to provide menu items for
		 * @param authClient the authenticated client through which the document
		 *            list was retrieved
		 * @return an array holding additional menu items for the argument
		 *         document
		 */
		public abstract JMenuItem[] getMenuItems(StringTupel docData, AuthenticatedClient authClient);
	}
	
	private ArrayList listExtensionList = new ArrayList();
	
	private AuthenticationManagerPlugin authManager = null;
	private AuthenticatedClient authClient = null;
	
	private GoldenGateDioClient dioClient = null;
	private DioClientDocumentCache cache = null;
	
	private static final String[] listSortKeys = {DOCUMENT_NAME_ATTRIBUTE};
	
	private DisplayTitlePatternElement[] listTitlePattern = new DisplayTitlePatternElement[0];
	private StringVector listTitlePatternAttributes = new StringVector();
	private StringVector listFieldOrder = new StringVector();
	private StringVector listFields = new StringVector();
	
	/**
	 * Element of a pattern for creating the display title of a document from
	 * individual attributes
	 * 
	 * @author sautter
	 */
	private static class DisplayTitlePatternElement {
		
		/** the literal value of this element, or the name of the attribute whose value insert in its position */
		public final String patternElement;
		
		/** is this element a literal value or a variable? */
		public final boolean isLiteral;
		
		DisplayTitlePatternElement(String patternElement, boolean isLiteral) {
			this.patternElement = patternElement;
			this.isLiteral = isLiteral;
		}
	}
	
	/**
	 * Parse a display title pattern from a pattern string
	 * @param pattern the pattern to parse
	 * @return an array of pattern elements
	 */
	private static DisplayTitlePatternElement[] parseDisplayTitlePattern(String pattern) {
		ArrayList dtpeList = new ArrayList();
		if (pattern != null) {
			boolean quoted = false;
			int escaped = -1;
			StringBuffer sb = new StringBuffer();
			for (int c = 0; c < pattern.length(); c++) {
				char ch = pattern.charAt(c);
				
				//	escaped character
				if (escaped == c)
					sb.append(ch);
				
				//	escape next character
				else if (ch == '\\')
					escaped = (c+1);
				
				//	quoter
				else if (ch == '\'') {
					
					//	store literal
					if (sb.length() != 0) {
						dtpeList.add(new DisplayTitlePatternElement((quoted ? sb.toString() : sb.toString().trim()), quoted));
						sb = new StringBuffer();
					}
					
					//	switch quote mode
					quoted = !quoted;
				}
				
				//	quoted part
				else if (quoted)
					sb.append(ch);
				
				//	start of attribute
				else if (ch == '@') {
					
					//	store previous attribute (if any)
					if (sb.length() != 0) {
						dtpeList.add(new DisplayTitlePatternElement(sb.toString().trim(), false));
						sb = new StringBuffer();
					}
				}
				
				//	other character
				else sb.append(ch);
			}
			
			//	anything left in buffer?
			if (sb.length() != 0)
				dtpeList.add(new DisplayTitlePatternElement((quoted ? sb.toString() : sb.toString().trim()), quoted));
		}
		
		return ((DisplayTitlePatternElement[]) dtpeList.toArray(new DisplayTitlePatternElement[dtpeList.size()]));
	}
	
	/**
	 * Create a custom document title from a set of attributes
	 * @param data the Properties holding the attribute values to create a title
	 *            from
	 * @return a title assembled from the specified attribute values according
	 *         to the specified pattern
	 */
	private String createDisplayTitle(Properties data) {
		
		//	no pattern
		if (this.listTitlePattern.length == 0)
			return data.getProperty(DOCUMENT_TITLE_ATTRIBUTE, "Unknown Document");
		
		//	assemble title according to pattern
		StringBuffer title = new StringBuffer();
		for (int e = 0; e < this.listTitlePattern.length; e++) {
			
			//	literal
			if (this.listTitlePattern[e].isLiteral)
				title.append(this.listTitlePattern[e].patternElement);
			
			//	variable
			else {
				Object attribute = data.getProperty(this.listTitlePattern[e].patternElement);
				if (attribute != null)
					title.append(attribute);
			}
		}
		
		//	return title
		return title.toString();
	}
	
	/**
	 * Extract which attributes will be used in a title created according to a
	 * pattern
	 * @param the pattern to inspect
	 * @return an array holding the names of the attributes used in the
	 *         createDisplayTitle() method
	 */
	private static String[] getDisplayTitleAttributes(DisplayTitlePatternElement[] pattern) {
		StringVector displayTitleAttributes = new StringVector();
		for (int e = 0; e < pattern.length; e++)
			if (!pattern[e].isLiteral)
				displayTitleAttributes.addElement(pattern[e].patternElement);
		return displayTitleAttributes.toStringArray();
	}
	
	/**	Constructor
	 */
	public GoldenGateDioDocumentIO() {}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.GoldenGatePlugin#getPluginName()
	 */
	public String getPluginName() {
		return "DIO Document IO";
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#init()
	 */
	public void init() {
		
		//	get authentication manager
		this.authManager = ((AuthenticationManagerPlugin) this.parent.getPlugin(AuthenticationManagerPlugin.class.getName()));
		
		//	read display configuration
		try {
			InputStream is = this.dataProvider.getInputStream("config.cnfg");
			Settings set = Settings.loadSettings(is);
			is.close();
			
			this.listTitlePattern = parseDisplayTitlePattern(set.getSetting("listTitlePattern"));
			this.listTitlePatternAttributes.addContent(getDisplayTitleAttributes(this.listTitlePattern));
			this.listFieldOrder.parseAndAddElements(set.getSetting("listFieldOrder"), " ");
			this.listFields.parseAndAddElements(set.getSetting("listFields"), " ");
			
			Settings listFieldLabels = set.getSubset("listFieldLabel");
			String[] listFieldNames = listFieldLabels.getKeys();
			for (int f = 0; f < listFieldNames.length; f++)
				this.listFieldLabels.setProperty(listFieldNames[f], listFieldLabels.getSetting(listFieldNames[f], listFieldNames[f]));
		}
		catch (IOException ioe) {
			System.out.println(ioe.getClass() + " (" + ioe.getMessage() + ") while initializing DioDocumentIO.");
			ioe.printStackTrace(System.out);
		}
		
		//	load extensions
		this.loadListExtensions();
	}
	
	private void loadListExtensions() {
		System.out.println("GeoReferencer: initializing data providers");
		
		//	load list extensions
		Object[] listExtensionObjects = GamtaClassLoader.loadComponents(
				this.dataProvider.getDataNames(),
				null,
				new InputStreamProvider() {
					public InputStream getInputStream(String dataName) throws IOException {
						return dataProvider.getInputStream(dataName);
					}
				},
				DocumentListExtension.class, 
				new ComponentInitializer() {
					public void initialize(Object component, String componentJarName) throws Throwable {
						if (componentJarName == null)
							throw new RuntimeException("Cannot determine data path for " + component.getClass().getName());
						DocumentListExtension dle = ((DocumentListExtension) component);
						componentJarName = componentJarName.substring(0, componentJarName.lastIndexOf('.')) + "Data";
						dle.setDataProvider(new PluginDataProviderPrefixBased(dataProvider, componentJarName));
					}
				});
		
		//	store & return list extensions
		for (int c = 0; c < listExtensionObjects.length; c++)
			this.listExtensionList.add(listExtensionObjects[c]);
	}
	
	/**
	 * Add a document list extension to DIO DocumentIO. This method is meant as
	 * an access point for other plugins to enhance the functionality of the
	 * document list without having to deploy a list extension in a separate jar
	 * file.
	 * @param dle the document list extension to add
	 */
	public void addDocumentListExtension(DocumentListExtension dle) {
		if (dle != null)
			this.listExtensionList.add(dle);
	}
	
	private DocumentListExtension[] getDocumentListExtensions() {
		return ((DocumentListExtension[]) this.listExtensionList.toArray(new DocumentListExtension[this.listExtensionList.size()]));
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#exit()
	 */
	public void exit() {
		this.logout();
	}
	
	private boolean ensureLoggedIn() {
		
		//	test if connection alive
		if (this.authClient != null) {
			try {
				//	test if connection alive
				if (this.authClient.ensureLoggedIn())
					return true;
				
				//	connection dead (eg a session timeout), make way for re-getting from auth manager
				else {
					this.dioClient = null;
					this.authClient = null;
				}
			}
			
			//	server temporarily unreachable, re-login will be done by auth manager
			catch (IOException ioe) {
				this.dioClient = null;
				this.authClient = null;
				return false;
			}
		}
		
		
		//	check if existing cache valid
		if (this.cache != null) {
			String host = this.authManager.getHost();
			String user = this.authManager.getUser();
			if ((host == null) || (user == null) || !this.cache.belongsTo(host, user) || !this.authManager.isAuthenticated()) {
				this.cache.close();
				this.cache = null;
			}
		}
		
		
		//	got no valid connection at the moment
		if (this.authClient == null)
			this.authClient = this.authManager.getAuthenticatedClient();
		
		
		//	create cache if none exists
		if ((this.cache == null) && this.dataProvider.isDataEditable() && this.authManager.isAuthenticated()) {
			String host = this.authManager.getHost();
			String user = this.authManager.getUser();
			if ((host != null) && (user != null))
				this.cache = new DioClientDocumentCache(this.dataProvider, host, user);
		}
		
		
		//	authentication failed
		if (this.authClient == null)
			return false;
		
		//	got valid connection
		else {
			this.dioClient = new GoldenGateDioClient(this.authClient);
			if (this.cache != null)
				this.cache.flush(this.dioClient);
			return true;
		}
	}
	
	private void logout() {
		try {
			if (this.cache != null) {
				this.cache.close();
				this.cache = null;
			}
			
			this.dioClient = null;
			
			if ((this.authClient != null) && this.authClient.isLoggedIn()) // might have been logged out from elsewhere
				this.authClient.logout();
			this.authClient = null;
		}
		catch (IOException ioe) {
			JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("An error occurred while logging out from GoldenGATE Server\n" + ioe.getMessage()), ("Error on Logout"), JOptionPane.ERROR_MESSAGE);
		}
	}
	
	private boolean uploadDocument(String docName, QueriableAnnotation doc, ProgressMonitor pm) {
		this.ensureLoggedIn();
		Window promptParent;
		if ((pm != null) && (pm instanceof ProgressMonitorWindow))
			promptParent = ((ProgressMonitorWindow) pm).getWindow();
		else promptParent = DialogPanel.getTopWindow();
		
		//	check connection
		if ((this.dioClient == null) && (this.cache == null)) {
			JOptionPane.showMessageDialog(promptParent, ("Cannot save a document to GoldenGATE Server without authentication."), ("Cannot Save Document"), JOptionPane.ERROR_MESSAGE);
			return false;
		}
		
		//	check document ID
		String docId = (doc.hasAttribute(DOCUMENT_ID_ATTRIBUTE) ? ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE)) : doc.getAnnotationID());
		
		//	cache document if possible
		boolean docCached;
		if (this.cache != null) {
			if (pm != null)
				pm.setInfo("Caching document ...");
			docCached = this.cache.storeDocument(doc, docName);
		}
		else docCached = false;
		
		//	server unreachable, we're done here
		if (this.dioClient == null) {
			JOptionPane.showMessageDialog(promptParent, ("Could not upload '" + docName + "' to GoldenGATE Server at " + authManager.getHost() + "\nbecause this server is unreachable at the moment." + (docCached ? "\n\nThe document has been stored to the local cache\nand will be uploaded when you log in next time." : "")), ("Server Unreachable" + (docCached ? " - Document Cached" : "")), JOptionPane.INFORMATION_MESSAGE);
			return docCached;
		}
		
		try {
			String[] uploadProtocol;
			try {
				if (pm instanceof ControllingProgressMonitor) {
					((ControllingProgressMonitor) pm).setPauseResumeEnabled(true);
					((ControllingProgressMonitor) pm).setAbortEnabled(true);
				}
				uploadProtocol = this.dioClient.updateDocument(doc, docName, pm);
				if (this.cache != null) {
					this.cache.markNotDirty(docId);
					if (pm != null)
						pm.setInfo("Cache updated.");
				}
			}
			catch (DuplicateExternalIdentifierException deie) {
				StringVector conflictDocuments = new StringVector();
				DocumentListElement[] dles = deie.getConflictingDocuments();
				for (int d = 0; d < dles.length; d++)
					conflictDocuments.addElement(" - " + dles[d].getAttribute(DOCUMENT_NAME_ATTRIBUTE) + ", last edited by " + dles[d].getAttribute(UPDATE_USER_ATTRIBUTE));
				if (JOptionPane.showConfirmDialog(promptParent, ("An error occurred while uploading document '" + docName + "' to the GoldenGATE Server at\n" + authManager.getHost() + "\n" + deie.getMessage() + "\n" + conflictDocuments.concatStrings("\n") + "\nStore the document anyway?"), "Duplicate External Identifier", JOptionPane.YES_NO_OPTION, JOptionPane.ERROR_MESSAGE) == JOptionPane.YES_OPTION) {
					if (pm instanceof ControllingProgressMonitor) {
						((ControllingProgressMonitor) pm).setPauseResumeEnabled(true);
						((ControllingProgressMonitor) pm).setAbortEnabled(true);
					}
					uploadProtocol = this.dioClient.updateDocument(doc, docName, EXTERNAL_IDENTIFIER_MODE_IGNORE, pm);
					if (this.cache != null) {
						this.cache.markNotDirty(docId);
						if (pm != null)
							pm.setInfo("Cache updated.");
					}
				}
				else return false;
			}
			UploadProtocolDialog uploadProtocolDialog = new UploadProtocolDialog("Document Upload Protocol", ("Document '" + docName + "' successfully uploaded to GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\nDetails:"), uploadProtocol);
			uploadProtocolDialog.setVisible(true);
			return true;
		}
		catch (IOException ioe) {
			JOptionPane.showMessageDialog(promptParent, ("An error occurred while uploading document '" + docName + "' to the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage() + (docCached ? "" : "\n\nThe document has been stored to the local cache\nand will be uploaded when you log in next time.")), ("Error Uploading Document"), JOptionPane.ERROR_MESSAGE);
			return false;
		}
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentLoader#getLoadDocumentMenuItem()
	 */
	public JMenuItem getLoadDocumentMenuItem() {
		JMenuItem mi = new JMenuItem("Load Document from GG Server");
		return mi;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.DocumentLoader#loadDocument()
	 */
	public DocumentData loadDocument() throws Exception {
		
		//	create progress monitor
		final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Loading Document List from GoldenGATE DIO ...");
		pmd.setAbortExceptionMessage("ABORTED BY USER");
		pmd.setInfoLineLimit(1);
		pmd.getWindow().setSize(400, 100);
		pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
		
		//	load document in separate thread
		final DocumentData[] dd = {null};
		final Exception[] e = {null};
		Thread dl = new Thread() {
			public void run() {
				try {
					dd[0] = loadDocument(pmd);
				}
				catch (RuntimeException re) {
					if (!"ABORTED BY USER".equals(re.getMessage()))
						e[0] = re;
				}
				catch (Exception ex) {
					e[0] = ex;
				}
				finally {
					pmd.close();
				}
			}
		};
		
		//	start loading
		dl.start();
		pmd.popUp(true);
		
		//	finally ...
		if (e[0] != null)
			throw e[0];
		else return dd[0];
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.MonitorableDocumentLoader#loadDocument(de.uka.ipd.idaho.gamta.util.ProgressMonitor)
	 */
	public DocumentData loadDocument(ProgressMonitor pm) throws Exception {
		System.out.println("DioDocumentIO (" + this.getClass().getName() + "): loading document");
		pm.setInfo("Checking authentication at GoldenGATE Server ...");
		this.ensureLoggedIn();
		
		//	get list of documents
		DocumentListBuffer documentList;
		boolean documentListEmpty = true;
		
		//	got server connection, load document list
		if (this.dioClient != null) {
			pm.setInfo("Connected, getting document list from GoldenGATE DIO ...");
			
			//	load document list
			try {
				DocumentList dl = this.dioClient.getDocumentList();
				for (int f = 0; f < dl.listFieldNames.length; f++) {
					DocumentAttributeSummary das = dl.getListFieldValues(dl.listFieldNames[f]);
					if ((das != null) && (das.size() != 0)) {
						documentListEmpty = false;
						f = dl.listFieldNames.length;
					}
				}
				pm.setInfo("Got document list, caching content ...");
				documentList = new DocumentListBuffer(dl, pm);
				
				if (pm instanceof ControllingProgressMonitor) {
					((ControllingProgressMonitor) pm).setPauseResumeEnabled(false);
					((ControllingProgressMonitor) pm).setAbortEnabled(false);
				}
				
				if (documentList.isEmpty() && documentListEmpty) {
					JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("Currently, there are no documents available from the GoldenGATE Server at\n" + this.authManager.getHost() + ":" + this.authManager.getPort()), "No Documents To Load", JOptionPane.INFORMATION_MESSAGE);
					return null;
				}
			}
			catch (IOException ioe) {
				ioe.printStackTrace(System.out);
				throw new Exception(("An error occurred while loading the document list from the GoldenGATE Server at\n" + this.authManager.getHost() + ":" + this.authManager.getPort() + "\n" + ioe.getMessage()), ioe);
			}
		}
		
		//	server unreachable, list cached documents
		else if (this.cache != null) {
			pm.setInfo("Server unreachable, getting document list from local cache ...");
			documentList = this.cache.getDocumentList();
		}
		
		//	not connected to server and no cache, indicate error
		else documentList = null;
		
		
		//	check success
		if (documentList == null) 
			throw new Exception("Cannot open a document from GoldenGATE Server without authentication.");
		
		//	check if documents to open
		else if (documentList.isEmpty() && documentListEmpty) {
			JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("Currently, there are no documents available from the GoldenGATE Server at\n" + this.authManager.getHost()), "No Documents To Load", JOptionPane.INFORMATION_MESSAGE);
			return null;
		}
		
		pm.setInfo("Document list loaded, opening selector dialog ...");
		try {
			//	display document list
			DocumentListDialog dld = new DocumentListDialog("Select Document", documentList, this.authManager.getUser(), ((this.authClient != null) && this.authClient.isAdmin()));
			dld.setLocationRelativeTo(DialogPanel.getTopWindow());
			dld.setVisible(true);
			
			//	return selected document
			return dld.getDocumentData();
		}
		catch (Throwable t) {
			t.printStackTrace(System.out);
			return null;
		}
	}
	
	private DocumentRoot checkoutDocumentFromServer(String docId, int version, String docName, int readTimeout, ProgressMonitor pm) throws IOException, TimeoutException {
		pm.setInfo("Loading document from GoldenGATE DIO ...");
		Window promptParent;
		if ((pm != null) && (pm instanceof ProgressMonitorWindow))
			promptParent = ((ProgressMonitorWindow) pm).getWindow();
		else promptParent = DialogPanel.getTopWindow();
		
		try {
			DocumentReader dr = this.dioClient.checkoutDocumentAsStream(docId, version);
			BufferedReader br = new BufferedReader(new TimeoutReader(dr, pm, readTimeout));
			DocumentRoot doc = GenericGamtaXML.readDocument(br);
			br.close();
			
			if (pm instanceof ControllingProgressMonitor) {
				((ControllingProgressMonitor) pm).setPauseResumeEnabled(false);
				((ControllingProgressMonitor) pm).setAbortEnabled(false);
			}
			
			if (this.cache != null) {
				pm.setInfo("Document loaded, caching it ...");
				this.cache.storeDocument(doc, docName);
				this.cache.markNotDirty(docId);
				pm.setInfo("Document cached.");
			}
			
			return doc;
		}
		catch (RuntimeException re) {
			if ("ABORTED BY USER".equals(re.getMessage())) try {
				this.dioClient.releaseDocument(docId);
			}
			catch (Exception ioe) {
				ioe.printStackTrace(System.out);
			}
			throw re;
		}
		catch (TimeoutException te) {
			te.printStackTrace(System.out);
			try {
				this.dioClient.releaseDocument(docId);
			}
			catch (Exception ioe) {
				ioe.printStackTrace(System.out);
			}
			JOptionPane.showMessageDialog(promptParent, ("Loading document '" + docName + "' from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + " timed out after " + readTimeout + " seconds.\nUse the 'Read Timeout' field below to increase the timeout."), ("Error Loading Document"), JOptionPane.ERROR_MESSAGE);
			throw te;
		}
		catch (IOException ioe) {
			ioe.printStackTrace(System.out);
			JOptionPane.showMessageDialog(promptParent, ("An error occurred while loading document '" + docName + "' from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage()), ("Error Loading Document"), JOptionPane.ERROR_MESSAGE);
			throw ioe;
		}
	}
	
	private class TimeoutReader extends Reader {
		DocumentReader dr;
		ProgressMonitor pm;
		int readTimeout;
		int readTotal = 0;
		TimeoutReader(DocumentReader dr, ProgressMonitor pm, int readTimeout) {
			this.dr = dr;
			this.pm = pm;
			this.readTimeout = readTimeout;
		}
		public void close() throws IOException {
			this.pm.setProgress(100);
			this.dr.close();
		}
		public int read(final char[] cbuf, final int off, final int len) throws IOException {
			final int[] read = {-2};
			final IOException ioe[] = {null};
			final Object readLock = new Object();
			
			//	create reader
			Thread reader = new Thread() {
				public void run() {
					synchronized (readLock) {
						readLock.notify();
					}
					
					try {
						System.out.println("Start timed reading");
						read[0] = dr.read(cbuf, off, len);
						System.out.println(" --> read " + read[0] + " chars");
					}
					catch (IOException ioex) {
						ioe[0] = ioex;
						System.out.println(" --> exception: " + ioex.getMessage());
					}
					finally {
						synchronized (readLock) {
							readLock.notify();
						}
					}
				}
			};
			
			//	start reader and wait for it
			synchronized (readLock) {
				reader.start();
				try {
					readLock.wait();
				} catch (InterruptedException ie) {}
			}
			
			//	wait for reading to succeed, be cancelled, or time out
			long readDeadline = ((readTimeout < 1) ? Long.MAX_VALUE : (System.currentTimeMillis() + (readTimeout * 1000)));
			do {
				
				//	wait a bit
				synchronized (readLock) {
					try {
						readLock.wait(500);
					} catch (InterruptedException ie) {}
				}
				
				//	reading exception
				if (ioe[0] != null) {
					System.out.println("Reader threw exception: " + ioe[0].getMessage());
					throw ioe[0];
				}
				
				//	read successful
				if (read[0] != -2) {
					System.out.println("Reader read " + read[0] + " chars");
					this.pm.setProgress((100 * this.readTotal) / this.dr.docLength()); // throws runtime exception if checkout aborted
					this.readTotal += read[0];
					return read[0];
				}
			}
			
			//	check for timeout
			while (System.currentTimeMillis() < readDeadline);
			
			//	read timeout, close asynchronously and throw exception
			Thread closer = new Thread() {
				public void run() {
					try {
						System.out.println("Start timed closing");
						dr.close();
						System.out.println(" --> closed");
					}
					catch (IOException ioe) {
						System.out.println(" --> exception on closing: " + ioe.getMessage());
					}
				}
			};
			closer.start();
			
			throw new TimeoutException("Read timeout");
		}
	}
	
	private class TimeoutException extends IOException {
		TimeoutException(String message) {
			super(message);
		}
	}
	
	private Properties listFieldLabels = new Properties();
	
	private final String produceFieldLabel(String fieldName) {
		String listFieldLabel = listFieldLabels.getProperty(fieldName);
		if (listFieldLabel != null)
			return listFieldLabel;
		
		if (fieldName.length() < 2)
			return fieldName;
		
		StringVector parts = new StringVector();
		fieldName = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
		int c = 1;
		while (c < fieldName.length()) {
			if (Character.isUpperCase(fieldName.charAt(c))) {
				parts.addElement(fieldName.substring(0, c));
				fieldName = fieldName.substring(c);
				c = 1;
			} else c++;
		}
		if (fieldName.length() != 0)
			parts.addElement(fieldName);
		
		for (int p = 0; p < (parts.size() - 1);) {
			String part1 = parts.get(p);
			String part2 = parts.get(p + 1);
			if ((part2.length() == 1) && Character.isUpperCase(part1.charAt(part1.length() - 1))) {
				part1 += part2;
				parts.setElementAt(part1, p);
				parts.remove(p+1);
			}
			else p++;
		}
		
		return parts.concatStrings(" ");
	}
	
	private class DocumentFilterPanel extends JPanel {
		
		private abstract class Filter {
			final String fieldName;
			Filter(String fieldName) {
				this.fieldName = fieldName;
			}
			abstract JComponent getOperatorSelector();
			abstract String getOperator();
			abstract JComponent getValueInputField();
			abstract String[] getFilterValues() throws RuntimeException;
		}
		
		private class StringFilter extends Filter {
			private String[] suggestionLabels;
			private Properties suggestionMappings;
			private boolean editable;
			private JTextField valueInput;
			private JComboBox valueSelector;
			StringFilter(String fieldName, DocumentAttributeSummary suggestions, boolean editable) {
				super(fieldName);
				if (suggestions == null) 
					this.editable = true;
				else {
					this.editable = editable;
					this.suggestionLabels = new String[suggestions.valueCount()];
					this.suggestionMappings = new Properties();
					for (Iterator sit = suggestions.keySet().iterator(); sit.hasNext();) {
						String suggestion = ((String) sit.next());
						if (this.editable) {
							suggestion = suggestion.replaceAll("\\s", "+");
							this.suggestionLabels[this.suggestionMappings.size()] = suggestion;
							this.suggestionMappings.setProperty(suggestion, suggestion);
						}
						else {
							String suggestionLabel = (suggestion + " (" + suggestions.getCount(suggestion) + ")");
							this.suggestionLabels[this.suggestionMappings.size()] = suggestionLabel;
							this.suggestionMappings.setProperty(suggestionLabel, suggestion);
						}
					}
				}
			}
			JComponent getOperatorSelector() {
				return new JLabel("contains (use '+' for spaces)", JLabel.CENTER);
			}
			String getOperator() {
				return null;
			}
			JComponent getValueInputField() {
				if (this.suggestionLabels == null) {
					this.valueInput = new JTextField();
					this.valueInput.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							parent.filterDocumentList();
						}
					});
					return this.valueInput;
				}
				else {
					this.valueSelector = new JComboBox(this.suggestionLabels);
					this.valueSelector.insertItemAt("<do not filter>", 0);
					this.valueSelector.setSelectedItem("<do not filter>");
					this.valueSelector.setEditable(this.editable);
					this.valueSelector.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							parent.filterDocumentList();
						}
					});
					return this.valueSelector;
				}
			}
			String[] getFilterValues() throws RuntimeException {
				String filterValue;
				if (this.suggestionLabels == null)
					filterValue = this.valueInput.getText().trim();
				else {
					filterValue = ((String) this.valueSelector.getSelectedItem()).trim();
					filterValue = this.suggestionMappings.getProperty(filterValue, filterValue);
				}
				
				if ((filterValue.length() == 0) || "<do not filter>".equals(filterValue))
					return null;
				
				if (this.editable) {
					String[] filterValues = filterValue.split("\\s++");
					for (int v = 0; v < filterValues.length; v++)
						filterValues[v] = filterValues[v].replaceAll("\\+", " ").trim();
					return filterValues;
				}
				else {
					String[] filterValues = {filterValue};
					return filterValues;
				}
			}
		}
		
		private class NumberFilter extends Filter {
			private String[] operatorLabels;
			private Properties operatorMappings;
			private JComboBox operatorSelector;
			private String[] suggestionLabels;
			private Properties suggestionMappings;
			private boolean editable;
			private JTextField valueInput;
			private JComboBox valueSelector;
			NumberFilter(String fieldName, DocumentAttributeSummary suggestions, boolean editable, boolean isTime) {
				super(fieldName);
				
				this.operatorLabels = new String[numericOperators.size()];
				this.operatorMappings = new Properties();
				for (Iterator oit = numericOperators.iterator(); oit.hasNext();) {
					String operator = ((String) oit.next());
					String operatorLabel;
					if (">".equals(operator))
						operatorLabel = (isTime ? "after" : "more than");
					else if (">=".equals(operator))
						operatorLabel = (isTime ? "the earliest in" : "at least");
					else if ("=".equals(operator))
						operatorLabel = "exactly in";
					else if ("<=".equals(operator))
						operatorLabel = (isTime ? "the latest in" : "at most");
					else if ("<".equals(operator))
						operatorLabel = (isTime ? "before" : "less than");
					else continue;
					this.operatorLabels[this.operatorMappings.size()] = operatorLabel;
					this.operatorMappings.setProperty(operatorLabel, operator);
				}
				
				if (suggestions == null)
					this.editable = true;
				else {
					this.editable = editable;
					this.suggestionLabels = new String[suggestions.valueCount()];
					this.suggestionMappings = new Properties();
					for (Iterator sit = suggestions.keySet().iterator(); sit.hasNext();) {
						String suggestion = ((String) sit.next());
						if (this.editable) {
							this.suggestionLabels[this.suggestionMappings.size()] = suggestion;
							this.suggestionMappings.setProperty(suggestion, suggestion);
						}
						else {
							String suggestionLabel = (suggestion + " (" + suggestions.getCount(suggestion) + ")");
							this.suggestionLabels[this.suggestionMappings.size()] = suggestionLabel;
							this.suggestionMappings.setProperty(suggestionLabel, suggestion);
						}
					}
				}
			}
			JComponent getOperatorSelector() {
				this.operatorSelector = new JComboBox(this.operatorLabels);
				this.operatorSelector.setEditable(false);
				return this.operatorSelector;
			}
			String getOperator() {
				String operator = ((String) this.operatorSelector.getSelectedItem()).trim();
				return this.operatorMappings.getProperty(operator, operator);
			}
			JComponent getValueInputField() {
				if (this.suggestionLabels == null) {
					this.valueInput = new JTextField();
					this.valueInput.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							parent.filterDocumentList();
						}
					});
					return this.valueInput;
				}
				else {
					this.valueSelector = new JComboBox(this.suggestionLabels);
					this.valueSelector.insertItemAt("<do not filter>", 0);
					this.valueSelector.setSelectedItem("<do not filter>");
					this.valueSelector.setEditable(this.editable);
					this.valueSelector.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							parent.filterDocumentList();
						}
					});
					return this.valueSelector;
				}
			}
			String[] getFilterValues() throws RuntimeException {
				String filterValue;
				if (this.suggestionLabels == null)
					filterValue = this.valueInput.getText().trim();
				else {
					filterValue = ((String) this.valueSelector.getSelectedItem()).trim();
					filterValue = this.suggestionMappings.getProperty(filterValue, filterValue);
				}
				
				if ((filterValue.length() == 0) || "<do not filter>".equals(filterValue))
					return null;
				
				try {
					Long.parseLong(filterValue);
				}
				catch (NumberFormatException nfe) {
					throw new RuntimeException("'" + filterValue + "' is not a valid value for " + produceFieldLabel(this.fieldName) + ".");
				}
				
				String[] filterValues = {filterValue};
				return filterValues;
			}
		}
		
		private class TimeFilter extends Filter {
			private String[] operatorLabels;
			private Properties operatorMappings;
			private JComboBox operatorSelector;
			private JComboBox valueSelector;
			TimeFilter(String fieldName) {
				super(fieldName);
				
				this.operatorLabels = new String[numericOperators.size()];
				this.operatorMappings = new Properties();
				for (Iterator oit = numericOperators.iterator(); oit.hasNext();) {
					String operator = ((String) oit.next());
					String operatorLabel;
					if (">".equals(operator))
						operatorLabel = "less than";
					else if (">=".equals(operator))
						operatorLabel = "at most";
					else if ("=".equals(operator))
						operatorLabel = "exactly";
					else if ("<=".equals(operator))
						operatorLabel = "at least";
					else if ("<".equals(operator))
						operatorLabel = "more than";
					else continue;
					this.operatorLabels[this.operatorMappings.size()] = operatorLabel;
					this.operatorMappings.setProperty(operatorLabel, operator);
				}
			}
			JComponent getOperatorSelector() {
				this.operatorSelector = new JComboBox(this.operatorLabels);
				this.operatorSelector.setEditable(false);
				return this.operatorSelector;
			}
			String getOperator() {
				String operator = ((String) this.operatorSelector.getSelectedItem()).trim();
				return this.operatorMappings.getProperty(operator, operator);
			}
			JComponent getValueInputField() {
				this.valueSelector = new JComboBox();
				this.valueSelector.addItem("<do not filter>");
				this.valueSelector.addItem("one hour ago");
				this.valueSelector.addItem("one day ago");
				this.valueSelector.addItem("one week ago");
				this.valueSelector.addItem("one month ago");
				this.valueSelector.addItem("three months ago");
				this.valueSelector.addItem("one year ago");
				this.valueSelector.setEditable(false);
				return this.valueSelector;
			}
			String[] getFilterValues() throws RuntimeException {
				String filterValue = ((String) this.valueSelector.getSelectedItem()).trim();
				if ("one hour ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (1 * 1 * 60 * 60) * 1000)));
				else if ("one day ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (1 * 24 * 60 * 60) * 1000)));
				else if ("one week ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (7 * 24 * 60 * 60) * 1000)));
				else if ("one month ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (30 * 24 * 60 * 60) * 1000)));
				else if ("three months ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (90 * 24 * 60 * 60) * 1000)));
				else if ("one year ago".equals(filterValue))
					filterValue = ("" + (System.currentTimeMillis() - ((long) (365 * 24 * 60 * 60) * 1000)));
				else return null;
				String[] filterValues = {filterValue};
				return filterValues;
			}
		}
		
		private DocumentListDialog parent;
		private Filter[] filters;
		
		DocumentFilterPanel(DocumentListBuffer docList, DocumentListDialog parent) {
			super(new GridBagLayout(), true);
			this.parent = parent;
			
			ArrayList filterList = new ArrayList();
			for (int f = 0; f < docList.listFieldNames.length; f++) {
				DocumentAttributeSummary das = docList.getListFieldValues(docList.listFieldNames[f]);
				if (!filterableAttributes.contains(docList.listFieldNames[f]) && (das == null))
					continue;
				
				Filter filter;
				if (numericAttributes.contains(docList.listFieldNames[f])) {
					if (docList.listFieldNames[f].endsWith("Time"))
						filter = new TimeFilter(docList.listFieldNames[f]);
					else filter = new NumberFilter(docList.listFieldNames[f], das, true, DOCUMENT_DATE_ATTRIBUTE.equals(docList.listFieldNames[f]));
				}
				else filter = new StringFilter(docList.listFieldNames[f], das, !docList.listFieldNames[f].endsWith("User"));
				filterList.add(filter);
			}
			this.filters = ((Filter[]) filterList.toArray(new Filter[filterList.size()]));
			
			GridBagConstraints gbc = new GridBagConstraints();
			gbc.insets.top = 3;
			gbc.insets.bottom = 3;
			gbc.insets.left = 3;
			gbc.insets.right = 3;
			gbc.weighty = 0;
			gbc.gridheight = 1;
			gbc.gridwidth = 1;
			gbc.fill = GridBagConstraints.BOTH;
			gbc.gridy = 0;
			for (int f = 0; f < this.filters.length; f++) {
				gbc.gridx = 0;
				gbc.weightx = 0;
				this.add(new JLabel(produceFieldLabel(this.filters[f].fieldName), JLabel.LEFT), gbc.clone());
				gbc.gridx = 1;
				gbc.weightx = 0;
				this.add(this.filters[f].getOperatorSelector(), gbc.clone());
				gbc.gridx = 2;
				gbc.weightx = 1;
				this.add(this.filters[f].getValueInputField(), gbc.clone());
				gbc.gridy++;
			}
		}
		
		DocumentFilter getFilter() {
			final LinkedList filterList = new LinkedList();
			for (int f = 0; f < this.filters.length; f++) {
				String[] filterValues = this.filters[f].getFilterValues();
				if ((filterValues == null) || (filterValues.length == 0))
					continue;
				
				System.out.println(this.filters[f].fieldName + " filter value is " + this.flattenArray(filterValues));
				
				if (numericAttributes.contains(this.filters[f].fieldName)) {
					final long filterValue = Long.parseLong(filterValues[0]);
					final String operator = this.filters[f].getOperator();
					if ((operator != null) && numericOperators.contains(operator))
						filterList.addFirst(new DocumentFilter(this.filters[f].fieldName) {
							boolean passesFilter(StringTupel docData) {
								String dataValueString = docData.getValue(this.fieldName);
								if (dataValueString == null)
									return false;
								long dataValue;
								try {
									dataValue = Long.parseLong(dataValueString);
								}
								catch (NumberFormatException nfe) {
									return false;
								}
								if (">".equals(operator))
									return (dataValue > filterValue);
								else if (">=".equals(operator))
									return (dataValue >= filterValue);
								else if ("=".equals(operator))
									return (dataValue == filterValue);
								else if ("<=".equals(operator))
									return (dataValue <= filterValue);
								else if ("<".equals(operator))
									return (dataValue < filterValue);
								else return true;
							}
						});
				}
				else {
					final String[] filterStrings = new String[filterValues.length];
					for (int v = 0; v < filterValues.length; v++)
						filterStrings[v] = filterValues[v].replaceAll("\\s++", " ").toLowerCase();
					
					if (DOCUMENT_KEYWORDS_ATTRIBUTE.equals(this.filters[f].fieldName)) {
						for (int s = 0; s < filterStrings.length; s++) {
							while (filterStrings[s].startsWith("%%"))
								filterStrings[s] = filterStrings[s].substring(1);
							if (filterStrings[s].startsWith("%"))
								filterStrings[s] = filterStrings[s].substring(1);
							else filterStrings[s] = ("|" + filterStrings[s]);
							while (filterStrings[s].endsWith("%"))
								filterStrings[s] = filterStrings[s].substring(0, (filterStrings[s].length() - 1));
						}
						filterList.addLast(new DocumentFilter(this.filters[f].fieldName) {
							boolean passesFilter(StringTupel docData) {
								String dataValueString = docData.getValue(this.fieldName);
								if (dataValueString == null)
									return false;
								dataValueString = dataValueString.replaceAll("\\s++", " ").toLowerCase();
								for (int f = 0; f < filterStrings.length; f++) {
									if (dataValueString.indexOf(filterStrings[f]) != -1)
										return true;
								}
								return false;
							}
						});
					}
					else {
						for (int s = 0; s < filterStrings.length; s++) {
							while (filterStrings[s].startsWith("%"))
								filterStrings[s] = filterStrings[s].substring(1);
							while (filterStrings[s].endsWith("%"))
								filterStrings[s] = filterStrings[s].substring(0, (filterStrings[s].length() - 1));
						}
						filterList.addLast(new DocumentFilter(this.filters[f].fieldName) {
							boolean passesFilter(StringTupel docData) {
								String dataValueString = docData.getValue(this.fieldName);
								if (dataValueString == null)
									return false;
								dataValueString = dataValueString.replaceAll("\\s++", " ").toLowerCase();
								for (int f = 0; f < filterStrings.length; f++) {
									if (dataValueString.indexOf(filterStrings[f]) != -1)
										return true;
								}
								return false;
							}
						});
					}
				}
			}
			
			return (filterList.isEmpty() ? null : new DocumentFilter(null) {
				boolean passesFilter(StringTupel docData) {
					for (Iterator fit = filterList.iterator(); fit.hasNext();) {
						if (!((DocumentFilter) fit.next()).passesFilter(docData))
							return false;
					}
					return true;
				}
			});
		}
		
		Properties getFilterParameters() {
			Properties filter = new Properties();
			for (int f = 0; f < this.filters.length; f++) {
				String filterValue = this.flattenArray(this.filters[f].getFilterValues());
				if (filterValue == null)
					continue;
				filter.setProperty(this.filters[f].fieldName, filterValue);
				if (numericAttributes.contains(this.filters[f].fieldName)) {
					String operator = this.filters[f].getOperator();
					if ((operator != null) && numericOperators.contains(operator))
						filter.setProperty((this.filters[f].fieldName + "Operator"), operator);
				}
			}
			return filter;
		}
		private String flattenArray(String[] filterValues) {
			if ((filterValues == null) || (filterValues.length == 0))
				return null;
			if (filterValues.length == 1)
				return filterValues[0];
			StringBuffer filterValue = new StringBuffer(filterValues[0]);
			for (int v = 1; v < filterValues.length; v++)
				filterValue.append("\n" + filterValues[v]);
			return filterValue.toString();
		}
	}
	
	private abstract class DocumentFilter {
		String fieldName;
		DocumentFilter(String fieldName) {
			this.fieldName = fieldName;
		}
		abstract boolean passesFilter(StringTupel docData);
	}
	
	private static final String[] cacheDocumentListAttributes = {
		DOCUMENT_ID_ATTRIBUTE,
		DOCUMENT_NAME_ATTRIBUTE,
		DOCUMENT_TITLE_ATTRIBUTE,
		CHECKIN_USER_ATTRIBUTE,
		CHECKIN_TIME_ATTRIBUTE,
		UPDATE_USER_ATTRIBUTE,
		UPDATE_TIME_ATTRIBUTE,
		DOCUMENT_VERSION_ATTRIBUTE,
	};
	
	private static final String DEFAULT_TIMESTAMP_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final DateFormat TIMESTAMP_DATE_FORMAT = new SimpleDateFormat(DEFAULT_TIMESTAMP_DATE_FORMAT);
	
	/* TODO
reduce document list loading effort:
- for non-admin users, only transfer document list head with input suggestions for filters
- embed list size in header
  - facilitates displaying list loading progress
  - facilitates detailed message for selectivity of filter
- display message label in place of document list table if list too large
- in DioDocumentIO, reload list from server when filter button clicked
 */
	private class StringTupelTray {
		final StringTupel data;
		Object[] sortKey = new Object[0];
		StringTupelTray(StringTupel data) {
			this.data = data;
		}
		void updateSortKey(StringVector sortFields) {
			this.sortKey = new Object[sortFields.size()];
			for (int f = 0; f < sortFields.size(); f++)
				this.sortKey[f] = this.data.getValue(sortFields.get(f), "");
		}
	}
	
	private class DocumentListDialog extends DialogPanel {
		
		private static final String CACHE_STATUS_ATTRIBUTE = "Cache";
		private DocumentListBuffer docList;
		private StringTupelTray[] listData;
		
		private JTable docTable = new JTable();
		private DocumentTableModel docTableModel;
		
		private DocumentFilterPanel filterPanel;
		
		private DocumentData loadData = null;
		
		private String userName;
		private boolean isAdmin;
		
		private String title;
		
		private SpinnerNumberModel readTimeout = new SpinnerNumberModel(5, 0, 60, 5);
		
		DocumentListDialog(String title, DocumentListBuffer docList, String userName, boolean isAdmin) {
			super(title, true);
			this.title = title;
			this.userName = userName;
			this.isAdmin = isAdmin;
			this.docList = docList;
			
			this.filterPanel = new DocumentFilterPanel(docList, this);
			
			final JTableHeader header = this.docTable.getTableHeader();
			if (header != null)
				header.addMouseListener(new MouseAdapter() {
					public void mouseClicked(MouseEvent me) {
						if (docTableModel == null)
							return;
		                int column = header.columnAtPoint(me.getPoint());
		                if (column != -1)
		                	sortList(docTableModel.getFieldName(column));
					}
				});
			
			this.docTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
			this.docTable.addMouseListener(new MouseAdapter() {
				public void mouseClicked(MouseEvent me) {
					int row = docTable.getSelectedRow();
					if (row == -1) return;
					if (me.getClickCount() > 1)
						open(row, 0);
					else if (me.getButton() != MouseEvent.BUTTON1)
						showContextMenu(row, me);
				}
			});
			
			JScrollPane docTableBox = new JScrollPane(this.docTable);
			docTableBox.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_AS_NEEDED);
			
			JButton filterButton = new JButton("Filter");
			filterButton.setBorder(BorderFactory.createRaisedBevelBorder());
			filterButton.setPreferredSize(new Dimension(100, 21));
			filterButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					filterDocumentList();
				}
			});
			
			JButton okButton = new JButton("OK");
			okButton.setBorder(BorderFactory.createRaisedBevelBorder());
			okButton.setPreferredSize(new Dimension(100, 21));
			okButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					int row = docTable.getSelectedRow();
					if (row == -1) return;
					open(row, 0);
				}
			});
			
			JButton cancelButton = new JButton("Cancel");
			cancelButton.setBorder(BorderFactory.createRaisedBevelBorder());
			cancelButton.setPreferredSize(new Dimension(100, 21));
			cancelButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					if ((cache != null) && (dioClient != null))
						cache.cleanup(dioClient);
//					loadException = null;
					dispose();
				}
			});
			
			JSpinner readTimeoutSelector = new JSpinner(this.readTimeout);
			JLabel readTimeoutLabel = new JLabel("Read Timeout (in seconds, 0 means no timeout)", JLabel.RIGHT);
			JPanel readTimeoutPanel = new JPanel(new FlowLayout());
			readTimeoutPanel.add(readTimeoutLabel);
			readTimeoutPanel.add(readTimeoutSelector);
			readTimeoutPanel.setBorder(BorderFactory.createEtchedBorder());
			
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
			buttonPanel.add(filterButton);
			buttonPanel.add(readTimeoutPanel);
			buttonPanel.add(okButton);
			buttonPanel.add(cancelButton);
			
			this.add(this.filterPanel, BorderLayout.NORTH);
			this.add(docTableBox, BorderLayout.CENTER);
			this.add(buttonPanel, BorderLayout.SOUTH);
			
			this.setSize(new Dimension(800, 800));
			
			this.updateListData(null);
		}
		
		private void setDocumentList(DocumentListBuffer docList) {
			this.docList = docList;
			this.updateListData(null);
		}
		
		private void updateListData(DocumentFilter filter) {
			if (filter == null) {
				this.listData = new StringTupelTray[this.docList.size()];
				for (int d = 0; d < this.docList.size(); d++)
					this.listData[d] = new StringTupelTray(this.docList.get(d));
				this.setTitle(this.title + " (" + this.docList.size() + " documents)");
			}
			else {
				ArrayList listDataList = new ArrayList();
				for (int d = 0; d < this.docList.size(); d++) {
					StringTupel docData = this.docList.get(d);
					if (filter.passesFilter(docData))
						listDataList.add(docData);
				}
				this.listData = new StringTupelTray[listDataList.size()];
				for (int d = 0; d < listDataList.size(); d++)
					this.listData[d] = new StringTupelTray((StringTupel) listDataList.get(d));
				this.setTitle(this.title + " (" + this.listData.length + " of " + this.docList.size() + " documents passing filter)");
			}
			
			StringVector fieldNames = new StringVector();
			if (cache != null) {
				fieldNames.addElement(CACHE_STATUS_ATTRIBUTE);
				for (int d = 0; d < this.listData.length; d++) {
					String docId = this.listData[d].data.getValue(DOCUMENT_ID_ATTRIBUTE);
					if (docId == null)
						continue;
					else if (cache.isExplicitCheckout(docId))
						this.listData[d].data.setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
					else if (cache.containsDocument(docId))
						this.listData[d].data.setValue(CACHE_STATUS_ATTRIBUTE, "Cached");
					else this.listData[d].data.setValue(CACHE_STATUS_ATTRIBUTE, "");
				}
			}
			fieldNames.addContent(listFieldOrder);
			for (int f = 0; f < docList.listFieldNames.length; f++) {
				String fieldName = docList.listFieldNames[f];
				if (!DOCUMENT_ID_ATTRIBUTE.equals(fieldName) && !DOCUMENT_KEYWORDS_ATTRIBUTE.equals(fieldName) && (DOCUMENT_TITLE_ATTRIBUTE.equals(fieldName) || !listTitlePatternAttributes.contains(fieldName)))
					fieldNames.addElementIgnoreDuplicates(fieldName);
			}
			
			for (int f = 0; f < fieldNames.size(); f++) {
				String fieldName = fieldNames.get(f);
				if (CACHE_STATUS_ATTRIBUTE.equals(fieldName))
					continue;
				
				if (!listFields.contains(fieldName)) {
					fieldNames.remove(f--);
					continue;
				}
				
				boolean fieldEmpty = true;
				if (DOCUMENT_TITLE_ATTRIBUTE.equals(fieldName))
					for (int t = 0; t < listTitlePatternAttributes.size(); t++) {
						fieldName = listTitlePatternAttributes.get(t);
						for (int d = 0; d < this.listData.length; d++) {
							if (!"".equals(this.listData[d].data.getValue(fieldName, ""))) {
								fieldEmpty = false;
								d = this.listData.length;
								t = listTitlePatternAttributes.size();
							}
						}
					}
				
				else for (int d = 0; d < this.listData.length; d++) {
					if (!"".equals(this.listData[d].data.getValue(fieldName, ""))) {
						fieldEmpty = false;
						d = this.listData.length;
					}
				}
				if (fieldEmpty)
					fieldNames.remove(f--);
			}
			
			this.docTableModel = new DocumentTableModel(fieldNames.toStringArray(), this.listData);
			this.docTable.setColumnModel(new DefaultTableColumnModel() {
				public TableColumn getColumn(int columnIndex) {
					TableColumn tc = super.getColumn(columnIndex);
					String fieldName = docTableModel.getColumnName(columnIndex);
					if (DOCUMENT_TITLE_ATTRIBUTE.equals(fieldName))
						return tc;
					
					if (docTableModel.listData.length == 0) {
						tc.setPreferredWidth(70);
						tc.setMinWidth(70);
					}
					else if (false
							|| CHECKIN_TIME_ATTRIBUTE.equals(fieldName)
							|| UPDATE_TIME_ATTRIBUTE.equals(fieldName)
							|| CHECKOUT_TIME_ATTRIBUTE.equals(fieldName)
							) {
						tc.setPreferredWidth(120);
						tc.setMinWidth(120);
					}
					else if (CACHE_STATUS_ATTRIBUTE.equals(fieldName)) {
						tc.setPreferredWidth(70);
						tc.setMinWidth(70);
					}
					else {
						String test = docTableModel.getValueAt(0, columnIndex).toString().replaceAll("\\<[A-Z\\/]++\\>", "");
						tc.setPreferredWidth(test.matches("[0-9]++") ? 50 : 100);
						tc.setMinWidth(test.matches("[0-9]++") ? 50 : 100);
					}
					
					tc.setResizable(true);
					
					return tc;
				}
			});
			this.docTable.setModel(this.docTableModel);
			
			this.sortList(null);
			
			this.docTable.validate();
			this.docTable.repaint();
		}
		
		private void filterDocumentList() {
			
			//	received list head only so far
			if (this.docList.isEmpty()) {
				DocumentListBuffer documentList;
				boolean documentListEmpty = true;
				Properties filter = this.filterPanel.getFilterParameters();
				try {
					DocumentList dl = dioClient.getDocumentList(filter);
					for (int f = 0; f < dl.listFieldNames.length; f++) {
						DocumentAttributeSummary das = dl.getListFieldValues(dl.listFieldNames[f]);
						if ((das != null) && (das.size() != 0)) {
							documentListEmpty = false;
							f = dl.listFieldNames.length;
						}
					}
					documentList = new DocumentListBuffer(dl);
					if (documentList.isEmpty() && documentListEmpty)
						JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("Currently, there are no documents available from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + ",\nor your filter is too restrictive."), "No Documents To Load", JOptionPane.INFORMATION_MESSAGE);
					this.setDocumentList(documentList);
				}
				catch (IOException ioe) {
					ioe.printStackTrace(System.out);
					JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("An error occurred while loading the document list from the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage()), "Error Getting Filtered List", JOptionPane.ERROR_MESSAGE);
				}
			}
			
			//	filter local list
			else this.updateListData(this.filterPanel.getFilter());
		}
		
		private void sortList(String sortField) {
			final StringVector sortFields = new StringVector();
			if (sortField != null) {
				if (DOCUMENT_TITLE_ATTRIBUTE.equals(sortField) && (listTitlePatternAttributes.size() != 0))
					sortFields.addContentIgnoreDuplicates(listTitlePatternAttributes);
				else sortFields.addElement(sortField);
			}
			sortFields.addContent(listSortKeys);
//			Arrays.sort(this.listData, new Comparator() {
//				public int compare(Object o1, Object o2) {
//					StringTupel st1 = ((StringTupel) o1);
//					StringTupel st2 = ((StringTupel) o2);
//					
//					for (int f = 0; f < sortFields.size(); f++) {
//						String sortField = sortFields.get(f);
//						String s1 = st1.getValue(sortField, "");
//						String s2 = st2.getValue(sortField, "");
//						
//						//	try number comparison
//						try {
//							int i1 = Integer.parseInt(s1);
//							int i2 = Integer.parseInt(s2);
//							if (i1 != i2) return (i1 - i2);
//						}
//						
//						//	do string comparison
//						catch (NumberFormatException nfe) {
//							int c = s1.compareTo(s2);
//							if (c != 0) return c;
//						}
//					}
//					return 0;
//				}
//			});
			
			//	update sort keys, and check which fields are numeric
			boolean[] isFieldNumeric = new boolean[sortFields.size()];
			Arrays.fill(isFieldNumeric, true);
			for (int d = 0; d < this.listData.length; d++) {
				this.listData[d].updateSortKey(sortFields);
				for (int f = 0; f < isFieldNumeric.length; f++) {
					if (isFieldNumeric[f]) try {
						Integer.parseInt((String) this.listData[d].sortKey[f]);
					}
					catch (NumberFormatException nfe) {
						isFieldNumeric[f] = false;
					}
				}
			}
			
			//	make field values numeric only if they are numeric throughout the list
			for (int d = 0; d < this.listData.length; d++) {
				for (int f = 0; f < isFieldNumeric.length; f++) {
					if (isFieldNumeric[f])
						this.listData[d].sortKey[f] = new Integer((String) this.listData[d].sortKey[f]);
				}
			}
			
			//	sort list (catching comparison errors that can occur in Java 7)
			Arrays.sort(this.listData, new Comparator() {
				public int compare(Object o1, Object o2) {
					StringTupelTray st1 = ((StringTupelTray) o1);
					StringTupelTray st2 = ((StringTupelTray) o2);
					int c = 0;
					for (int f = 0; f < st1.sortKey.length; f++) {
						if (st1.sortKey[f] instanceof Integer)
							c = (((Integer) st1.sortKey[f]).intValue() - ((Integer) st2.sortKey[f]).intValue());
						else c = ((String) st1.sortKey[f]).compareToIgnoreCase((String) st2.sortKey[f]);
						if (c != 0)
							return c;
					}
					return 0;
				}
			});
			
			//	update display
			this.docTableModel.update();
			this.docTable.validate();
			this.docTable.repaint();
		}
		
		private void delete(int row) {
			if ((dioClient == null) || (row == -1)) return;
			
			//	get document data
//			String docId = this.listData[row].getValue(DOCUMENT_ID_ATTRIBUTE);
			String docId = this.listData[row].data.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null) return;
			
			//	checkout and delete document
			try {
				
				//	do it
				if (cache != null)
					cache.unstoreDocument(docId);
				dioClient.deleteDocument(docId);
				
				//	inform user
				JOptionPane.showMessageDialog(this, "The document has been deleted.", "Document Deleted", JOptionPane.INFORMATION_MESSAGE);
				
				//	update data
//				StringTupel[] newListData = new StringTupel[this.listData.length - 1];
				StringTupelTray[] newListData = new StringTupelTray[this.listData.length - 1];
				System.arraycopy(this.listData, 0, newListData, 0, row);
				System.arraycopy(this.listData, (row+1), newListData, row, (newListData.length - row));
				this.listData = newListData;
				
				//	update display
				this.docTableModel.setListData(this.listData);
				this.docTable.validate();
				this.docTable.repaint();
			}
			catch (IOException ioe) {
				JOptionPane.showMessageDialog(this, ("Document could not be deleted:\n" + ioe.getMessage()), "Error Deleting Document", JOptionPane.ERROR_MESSAGE);
			}
		}
		
		private void open(int row, final int version) {
			if (row == -1) return;
			
			//	get document data
//			final String docId = this.listData[row].getValue(DOCUMENT_ID_ATTRIBUTE);
			final String docId = this.listData[row].data.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null)
				return;
//			final String docName = this.listData[row].getValue(DOCUMENT_NAME_ATTRIBUTE);
			final String docName = this.listData[row].data.getValue(DOCUMENT_NAME_ATTRIBUTE);
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Loading Document from GoldenGATE DIO ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dl = new Thread() {
				public void run() {
					
					//	open document
					try {
						MutableAnnotation doc = null;
						
						if ((cache != null) && (cache.containsDocument(docId))) {
							pmd.setInfo("Loading document from cache ...");
							doc = cache.loadDocument(docId, (dioClient == null));
							pmd.setInfo("Document loaded from cache.");
							pmd.setProgress(100);
						}
						
						if ((doc == null) && (dioClient != null))
							doc = checkoutDocumentFromServer(docId, version, docName, readTimeout.getNumber().intValue(), pmd);
						
						if (doc == null)
							throw new IOException("Cannot open a document from GoldenGATE Server without authentication.");
						
						ServerDocumentSaveOperation dso = new ServerDocumentSaveOperation(docId, docName);
						loadData = new DocumentData(doc, docName, dso.format, dso);
						
//						loadException = null; // clean up exception from earlier loading attempts
						
						if (cache != null) {
							cache.markOpen(docId);
							if (dioClient != null)
								cache.cleanup(dioClient);
						}
						
						dispose();
					}
					catch (RuntimeException re) {
						if (!"ABORTED BY USER".equals(re.getMessage()))
							throw re;
					}
					catch (IOException ioe) {
//						loadException = new Exception(("An error occurred while loading document '" + docName + "' from the DIO at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage()), ioe);
					}
					finally {
						pmd.close();
					}
				}
			};
			
			dl.start();
			pmd.popUp(true);
		}
		
		private void showContextMenu(final int row, MouseEvent me) {
			if (row == -1) return;
			
//			final String docId = this.listData[row].getValue(DOCUMENT_ID_ATTRIBUTE);
			final String docId = this.listData[row].data.getValue(DOCUMENT_ID_ATTRIBUTE);
			if (docId == null) return;
			
			int preVersion = 0;
			try {
//				preVersion = Integer.parseInt(this.listData[row].getValue(DOCUMENT_VERSION_ATTRIBUTE, "0"));
				preVersion = Integer.parseInt(this.listData[row].data.getValue(DOCUMENT_VERSION_ATTRIBUTE, "0"));
			} catch (NumberFormatException e) {}
			final int version = preVersion;
			
//			String preCheckoutUser = this.listData[row].getValue(CHECKOUT_USER_ATTRIBUTE);
			String preCheckoutUser = this.listData[row].data.getValue(CHECKOUT_USER_ATTRIBUTE);
			if ((preCheckoutUser != null) && (preCheckoutUser.trim().length() == 0))
				preCheckoutUser = null;
			final String checkoutUser = preCheckoutUser;
			
			JPopupMenu menu = new JPopupMenu();
			JMenuItem mi = null;
			
			//	load document (have to exclude checked-out ones for admins, who can see them)
			if ((checkoutUser == null) || checkoutUser.equals(this.userName)) {
				mi = new JMenuItem("Load Document");
				mi.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						open(row, 0);
					}
				});
				menu.add(mi);
				
				//	load previous version (if available)
				if ((version > 1) && (dioClient != null)) {
					JMenu versionMenu = new JMenu("Load Document Version ...");
					
					for (int v = version; v > Math.max(0, (version - 20)); v--) {
						final int openVersion = v;
						mi = new JMenuItem("Version " + openVersion + ((openVersion == version) ? " (most recent)" : ""));
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								open(row, ((openVersion == version) ? 0 : openVersion));
							}
						});
						versionMenu.add(mi);
					}
					
					menu.add(versionMenu);
				}
				
				//	delete document
				if (dioClient != null) {
					mi = new JMenuItem("Delete Document");
					mi.addActionListener(new ActionListener() {
						public void actionPerformed(ActionEvent ae) {
							delete(row);
						}
					});
					menu.add(mi);
				}
				
				//	cache operations
				if ((dioClient != null) && (cache != null)) {
					menu.addSeparator();
					
					//	document cached explicitly, offer release
					if (cache.isExplicitCheckout(docId)) {
						mi = new JMenuItem("Release Document");
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
//								releaseDocumentFromCache(docId, listData[row]);
								releaseDocumentFromCache(docId, listData[row].data);
							}
						});
						menu.add(mi);
					}
					
					//	document cached, offer making checkout explicit
					else if (cache.containsDocument(docId)) {
						mi = new JMenuItem("Cache Document");
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								cache.markExplicitCheckout(docId);
//								listData[row].setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
								listData[row].data.setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
								
								JOptionPane.showMessageDialog(DocumentListDialog.this, "Document cached successfully.", "Document Cached", JOptionPane.INFORMATION_MESSAGE);
								
								docTableModel.update();
								docTable.validate();
								docTable.repaint();
							}
						});
						menu.add(mi);
					}
					
					//	document not in cache, offer cache checkout
					else {
						mi = new JMenuItem("Cache Document");
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
//								checkoutDocumentToCache(docId, listData[row]);
								checkoutDocumentToCache(docId, listData[row].data);
							}
						});
						menu.add(mi);
					}
				}
			}
			
			//	release document (admin only)
			if (this.isAdmin && (dioClient != null) && (checkoutUser != null)) {
				if (mi != null) menu.addSeparator();
				
				mi = new JMenuItem("Unlock Document");
				mi.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						if ((checkoutUser != null) && (JOptionPane.showConfirmDialog(DocumentListDialog.this, ("This document is currently locked by " + checkoutUser + ",\nunlocking it may incur that all work done by " + checkoutUser + " is lost.\nDo you really want to unlock this document?"), "Confirm Unlock Document", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE) == JOptionPane.YES_OPTION)) try {
							if (cache != null)
								cache.unstoreDocument(docId);
							dioClient.releaseDocument(docId);
//							listData[row].removeValue(CHECKOUT_USER_ATTRIBUTE);
//							listData[row].removeValue(CHECKOUT_TIME_ATTRIBUTE);
//							listData[row].removeValue(CACHE_STATUS_ATTRIBUTE);
							listData[row].data.removeValue(CHECKOUT_USER_ATTRIBUTE);
							listData[row].data.removeValue(CHECKOUT_TIME_ATTRIBUTE);
							listData[row].data.removeValue(CACHE_STATUS_ATTRIBUTE);
							
							JOptionPane.showMessageDialog(DocumentListDialog.this, "Document unlocked successfully.", "Document Unlocked", JOptionPane.INFORMATION_MESSAGE);
							docTableModel.update();
							docTable.validate();
							docTable.repaint();
						}
						catch (IOException ioe) {
							JOptionPane.showMessageDialog(DocumentListDialog.this, ("An error occurred while unlocking the document.\n" + ioe.getClass().getName() + ": " + ioe.getMessage()), "Error Unlocking Document", JOptionPane.ERROR_MESSAGE);
							
							System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while unlocking document '" + docId + "' at " + authManager.getHost());
							ioe.printStackTrace(System.out);
						}
					}
				});
				menu.add(mi);
			}
			
			//	get extension menu items
			DocumentListExtension[] listExtensions = getDocumentListExtensions();
			for (int e = 0; e < listExtensions.length; e++) {
//				JMenuItem[] lems = listExtensions[e].getMenuItems(listData[row], authClient);
				JMenuItem[] lems = listExtensions[e].getMenuItems(listData[row].data, authClient);
				if ((lems == null) || (lems.length == 0))
					continue;
				menu.addSeparator();
				for (int m = 0; m < lems.length; m++)
					menu.add(lems[m]);
			}
			
			menu.show(this.docTable, me.getX(), me.getY());
		}
		
		private void checkoutDocumentToCache(final String docId, final StringTupel docData) {
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Checking Out Document from GoldenGATE DIO ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dc = new Thread() {
				public void run() {
					try {
						String docName = docData.getValue(DOCUMENT_NAME_ATTRIBUTE);
						checkoutDocumentFromServer(docId, 0, docName, readTimeout.getNumber().intValue(), pmd);
						cache.markExplicitCheckout(docId);
						docData.setValue(CACHE_STATUS_ATTRIBUTE, "Localized");
						
						JOptionPane.showMessageDialog(pmd.getWindow(), "Document cached successfully.", "Document Cached", JOptionPane.INFORMATION_MESSAGE);
						
						docTableModel.update();
						docTable.validate();
						docTable.repaint();
					}
					catch (RuntimeException re) {
						if (!"ABORTED BY USER".equals(re.getMessage()))
							throw re;
					}
					catch (IOException ioe) {} // we can swallow it here, as user is notified in checkout method
					finally {
						pmd.close();
					}
				}
			};
			
			dc.start();
			pmd.popUp(true);
		}
		
		private void releaseDocumentFromCache(final String docId, final StringTupel docData) {
			
			//	create progress monitor
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Uploading Document to GoldenGATE DIO ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			//	load in separate thread
			Thread dr = new Thread() {
				public void run() {
					try {
						
						//	we have uncommitted changes, upload then to server
						if (cache.isDirty(docId)) {
							pmd.setInfo("Loading document from cache ...");
							MutableAnnotation doc = cache.loadDocument(docId, true);
							String docName = docData.getValue(DOCUMENT_NAME_ATTRIBUTE);
							try {
								pmd.setInfo("Uploading document from cache ...");
								String[] uploadProtocol = dioClient.updateDocument(doc, docName, pmd);
								UploadProtocolDialog uploadProtocolDialog = new UploadProtocolDialog("Document Upload Protocol", ("Document '" + docName + "' successfully uploaded to GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\nDetails:"), uploadProtocol);
								uploadProtocolDialog.setVisible(true);
							}
							catch (IOException ioe) {
								if (JOptionPane.showConfirmDialog(pmd.getWindow(), ("An error occurred while uploading document '" + docName + "' to the GoldenGATE Server at\n" + authManager.getHost() + ":" + authManager.getPort() + "\n" + ioe.getMessage() + "\nRelease document anyway, discarting all cached changes?"), "Error Uploading Cached Document", JOptionPane.YES_NO_OPTION, JOptionPane.ERROR_MESSAGE) != JOptionPane.YES_OPTION)
									return;
							}
						}
						
						//	clean up
						pmd.setInfo("Cleaning up cache ...");
						cache.unstoreDocument(docId);
						dioClient.releaseDocument(docId);
						docData.removeValue(CACHE_STATUS_ATTRIBUTE);
						
						JOptionPane.showMessageDialog(DocumentListDialog.this, "Document released successfully.", "Document Released", JOptionPane.INFORMATION_MESSAGE);
						
						docTableModel.update();
						docTable.validate();
						docTable.repaint();
					}
					catch (IOException ioe) {
						JOptionPane.showMessageDialog(DocumentListDialog.this, ("An error occurred while releasing the document from the local cache.\n" + ioe.getClass().getName() + ": " + ioe.getMessage()), "Error Releasing Document", JOptionPane.ERROR_MESSAGE);
						
						System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while releasing cached document '" + docId + "' from GoldenGATE Server at " + authManager.getHost());
						ioe.printStackTrace(System.out);
					}
					catch (RuntimeException re) {
						if (!"ABORTED BY USER".equals(re.getMessage()))
							throw re;
					}
					finally {
						pmd.close();
					}
				}
			};
			
			dr.start();
			pmd.popUp(true);
		}
		
		DocumentData getDocumentData() throws Exception {
//			if (this.loadException != null)
//				throw this.loadException;
//			else 
				return this.loadData;
		}
	}
	
	private class DocumentTableModel implements TableModel {
		
		/** the filed names of the table (raw column names) */
		public final String[] fieldNames;
		
		/** an array holding the string tupels that contain the document data to display */
//		protected StringTupel[] listData;
		protected StringTupelTray[] listData;
		
		/**
		 * @param fieldNames
		 */
//		protected DocumentTableModel(String[] fieldNames, StringTupel[] listData) {
		protected DocumentTableModel(String[] fieldNames, StringTupelTray[] listData) {
			this.fieldNames = fieldNames;
			this.listData = listData;
		}
		
//		void setListData(StringTupel[] listData) {
		void setListData(StringTupelTray[] listData) {
			this.listData = listData;
			this.update();
		}
		
		private ArrayList listeners = new ArrayList();
		public void addTableModelListener(TableModelListener tml) {
			this.listeners.add(tml);
		}
		public void removeTableModelListener(TableModelListener tml) {
			this.listeners.remove(tml);
		}
		
		/**
		 * Update the table, refreshing the display.
		 */
		public void update() {
			for (int l = 0; l < this.listeners.size(); l++)
				((TableModelListener) this.listeners.get(l)).tableChanged(new TableModelEvent(this));
		}
		
		/**
		 * Retrieve the field name at some index.
		 * @param columnIndex the index of the desired field name
		 * @return the field name at the specified index
		 */
		public String getFieldName(int columnIndex) {
			return this.fieldNames[columnIndex];
		}
		
		public String getColumnName(int columnIndex) {
			return produceFieldLabel(this.fieldNames[columnIndex]);
		}
		public Class getColumnClass(int columnIndex) {
			return String.class;
		}
		public int getColumnCount() {
			return this.fieldNames.length;
		}
		public int getRowCount() {
			return listData.length;
		}
		public boolean isCellEditable(int rowIndex, int columnIndex) {
			return false;
		}
		public void setValueAt(Object aValue, int rowIndex, int columnIndex) {}
		
		public Object getValueAt(int rowIndex, int columnIndex) {
			String fieldName = this.fieldNames[columnIndex];
			String fieldValue = "";
//			final StringTupel rowData = listData[rowIndex];
			final StringTupel rowData = listData[rowIndex].data;
			
			//	format title
			if (DOCUMENT_TITLE_ATTRIBUTE.equals(fieldName)) {
				fieldValue = createDisplayTitle(new Properties() {
					public String getProperty(String key, String defaultValue) {
						return rowData.getValue(key, defaultValue);
					}
					public String getProperty(String key) {
						return rowData.getValue(key);
					}
				});
			}
			else fieldValue = rowData.getValue(this.fieldNames[columnIndex], "");
			
			//	format timestamp
			if (false
					|| CHECKIN_TIME_ATTRIBUTE.equals(fieldName)
					|| UPDATE_TIME_ATTRIBUTE.equals(fieldName)
					|| CHECKOUT_TIME_ATTRIBUTE.equals(fieldName)
					) {
				if (fieldValue.matches("[0-9]++")) try {
					fieldValue = TIMESTAMP_DATE_FORMAT.format(new Date(Long.parseLong(fieldValue)));
				} catch (NumberFormatException e) {}
			}
			
			return (fieldValue);
		}
	}
	
	private class UploadProtocolDialog extends DialogPanel {
		UploadProtocolDialog(String title, String label, final String[] uplaodProtocol) {
			super(title, true);
			this.getContentPane().setLayout(new BorderLayout());
			
			if (label != null)
				this.getContentPane().add(new JLabel(label, JLabel.LEFT), BorderLayout.NORTH);
			
			JTable resultList = new JTable(new TableModel() {
				public int getColumnCount() {
					return 1;
				}
				public int getRowCount() {
					return uplaodProtocol.length;
				}
				public String getColumnName(int columnIndex) {
					if (columnIndex == 0) return "";
					return null;
				}
				public Class getColumnClass(int columnIndex) {
					return String.class;
				}
				public boolean isCellEditable(int rowIndex, int columnIndex) {
					return false;
				}
				public Object getValueAt(int rowIndex, int columnIndex) {
					if (columnIndex == 0) return uplaodProtocol[rowIndex];
					return null;
				}
				public void setValueAt(Object aValue, int rowIndex, int columnIndex) {}
				
				public void addTableModelListener(TableModelListener l) {}
				public void removeTableModelListener(TableModelListener l) {}
			});
			resultList.setShowHorizontalLines(true);
			resultList.setShowVerticalLines(false);
			resultList.setTableHeader(null);
			
			JScrollPane resultListBox = new JScrollPane(resultList);
			resultListBox.setViewportBorder(BorderFactory.createLoweredBevelBorder());
			
			this.getContentPane().add(resultListBox, BorderLayout.CENTER);
			
			JButton okButton = new JButton("OK");
			okButton.setBorder(BorderFactory.createRaisedBevelBorder());
			okButton.setPreferredSize(new Dimension(100, 21));
			okButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					dispose();
				}
			});
			this.getContentPane().add(okButton, BorderLayout.SOUTH);
			
			//	show dialog
			this.setSize(500, 600);
			this.setLocationRelativeTo(this.getOwner());
		}
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaver#getSaveDocumentMenuItem()
	 */
	public JMenuItem getSaveDocumentMenuItem() {
		JMenuItem mi = new JMenuItem("Upload Document to GG Server");
		return mi;
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaver#getSaveDocumentPartsMenuItem()
	 */
	public JMenuItem getSaveDocumentPartsMenuItem() {
		return null; // we're uploading documents only as a whole
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaver#getSaveOperation(de.uka.ipd.idaho.goldenGate.plugins.DocumentSaveOperation)
	 */
	public DocumentSaveOperation getSaveOperation(DocumentSaveOperation model) {
		if (model instanceof ServerDocumentSaveOperation) return model;
		else return this.getSaveOperation(model.getDocumentName(), model.getDocumentFormat());
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaver#getSaveOperation(java.lang.String, de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat)
	 */
	public DocumentSaveOperation getSaveOperation(String documentName, DocumentFormat format) {
		if (format instanceof GamtaDocumentFormat)
			return ((GamtaDocumentFormat) format).parent;
		else return new ServerDocumentSaveOperation(documentName);
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaver#saveDocumentParts(de.uka.ipd.idaho.goldenGate.DocumentEditor, de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat, java.lang.String)
	 */
	public String saveDocumentParts(DocumentEditor data, DocumentFormat modelFormat, String modelType) {
		return null; // we're uploading documents only as a whole
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.MonitorableDocumentSaver#saveDocumentParts(de.uka.ipd.idaho.goldenGate.DocumentEditor, de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat, java.lang.String, de.uka.ipd.idaho.gamta.util.ProgressMonitor)
	 */
	public String saveDocumentParts(DocumentEditor data, DocumentFormat modelFormat, String modelType, ProgressMonitor pm) {
		return null; // we're uploading documents only as a whole
	}
	
	private class ServerDocumentSaveOperation implements MonitorableDocumentSaveOperation {
		private String documentId;
		private String documentName;
		private DocumentFormat format;
		
		ServerDocumentSaveOperation(String documentName) {
			this(null, documentName);
		}
		
		ServerDocumentSaveOperation(String documentId, String documentName) {
			this.documentId = documentId;
			this.documentName = documentName;
			this.format = new GamtaDocumentFormat(this);
			System.out.println("DIODocumentSaveOperation: created");
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaveOperation#keepAsDefault()
		 */
		public boolean keepAsDefault() {
			return true;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaveOperation#getDocumentFormat()
		 */
		public DocumentFormat getDocumentFormat() {
			return this.format;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaveOperation#getDocumentName()
		 */
		public String getDocumentName() {
			return this.documentName;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaveOperation#saveDocument(de.uka.ipd.idaho.gamta.QueriableAnnotation)
		 */
		public String saveDocument(final QueriableAnnotation data) {
			final ProgressMonitorDialog pmd = new ProgressMonitorDialog(false, true, DialogPanel.getTopWindow(), "Uploading Document to GoldenGATE DIO ...");
			pmd.setAbortExceptionMessage("ABORTED BY USER");
			pmd.setInfoLineLimit(1);
			pmd.getWindow().setSize(400, 100);
			pmd.getWindow().setLocationRelativeTo(pmd.getWindow().getOwner());
			
			final String[] dn = {null};
			final RuntimeException[] e = {null};
			Thread ds = new Thread() {
				public void run() {
					try {
						dn[0] = saveDocument(data, pmd);
					}
					catch (RuntimeException re) {
						if (!"ABORTED BY USER".equals(re.getMessage()))
							e[0] = re;
					}
					finally {
						pmd.close();
					}
				}
			};
			
			ds.start();
			pmd.popUp(true);
			
			if (e[0] != null)
				throw e[0];
			else return dn[0];
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.MonitorableDocumentSaveOperation#saveDocument(de.uka.ipd.idaho.gamta.QueriableAnnotation, de.uka.ipd.idaho.gamta.util.ProgressMonitor)
		 */
		public String saveDocument(QueriableAnnotation data, ProgressMonitor pm) {
			
			//	set document ID
			if (this.documentId == null) {
				this.documentId = ((String) data.getAttribute(DOCUMENT_ID_ATTRIBUTE));
				if (this.documentId == null)
					this.documentId = data.getAnnotationID();
			}
			
			//	get name in case of modification
			String docName = ((String) data.getAttribute(DOCUMENT_NAME_ATTRIBUTE));
			if ((docName != null) && (docName.trim().length() != 0))
				this.documentName = docName;
			
			//	do upload
			return (uploadDocument(this.documentName, data, pm) ? this.documentName : null);
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaveOperation#saveDocument(de.uka.ipd.idaho.goldenGate.DocumentEditor)
		 */
		public String saveDocument(DocumentEditor data) {
			return this.saveDocument(data.getContent());
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.MonitorableDocumentSaveOperation#saveDocument(de.uka.ipd.idaho.goldenGate.DocumentEditor, de.uka.ipd.idaho.gamta.util.ProgressMonitor)
		 */
		public String saveDocument(DocumentEditor data, ProgressMonitor pm) {
			return this.saveDocument(data.getContent(), pm);
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.DocumentSaveOperation#documentClosed()
		 */
		public void documentClosed() {
			try {
				if (this.documentId != null) {// opened from DIO, or saved before
					if ((cache != null) && cache.isExplicitCheckout(this.documentId))
						System.out.println("GgDIO DocIO: not releasing document '" + this.documentId + "', explicitly checked out.");
					
					else {
						System.out.println("GgDIO DocIO: releasing document '" + this.documentId + "'");
						if (cache != null)
							cache.unstoreDocument(this.documentId);
						if (dioClient != null)
							dioClient.releaseDocument(this.documentId);
						System.out.println("GgDIO DocIO: document '" + this.documentId + "' released");
					}
				}
				else System.out.println("GgDIO DocIO: Cannot release document, ID is null");
			}
			catch (IOException ioe) {
				System.out.println("DIODocumentSaveOperation: error releasing document '" + this.documentId + "': " + ioe.getMessage());
				ioe.printStackTrace(System.out);
			}
		}
	}
	
	private class GamtaDocumentFormat extends DocumentFormat {
		private ServerDocumentSaveOperation parent;
		
		GamtaDocumentFormat(ServerDocumentSaveOperation parent) {
			this.parent = parent;
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#isExportFormat()
		 */
		public boolean isExportFormat() {
			return false;
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#getDefaultSaveFileExtension()
		 */
		public String getDefaultSaveFileExtension() {
			return "gamta";
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#getDefaultEncodingName()
		 */
		public String getFormatDefaultEncodingName() {
			return ENCODING;
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#loadDocument(java.io.Reader)
		 */
		public MutableAnnotation loadDocument(InputStream source) throws IOException {
			throw new IOException("Cannot load document from GoldenGATE DIO, upload only.");
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#loadDocument(java.io.Reader)
		 */
		public MutableAnnotation loadDocument(Reader source) throws IOException {
			throw new IOException("Cannot load document from GoldenGATE DIO, upload only.");
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#saveDocument(de.uka.ipd.idaho.gamta.MutableAnnotation, java.io.OutputStream)
		 */
		public boolean saveDocument(QueriableAnnotation data, OutputStream out) throws IOException {
			return this.saveDocument(null, data, out);
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#saveDocument(de.uka.ipd.idaho.goldenGate.DocumentEditor, java.io.OutputStream)
		 */
		public boolean saveDocument(DocumentEditor data, OutputStream out) throws IOException {
			return this.saveDocument(data, data.getContent(), out);
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#saveDocument(de.uka.ipd.idaho.goldenGate.DocumentEditor, de.uka.ipd.idaho.gamta.MutableAnnotation, java.io.OutputStream)
		 */
		public boolean saveDocument(DocumentEditor data, QueriableAnnotation doc, OutputStream out) throws IOException {
			OutputStreamWriter osw = new OutputStreamWriter(out, ENCODING);
			GenericGamtaXML.storeDocument(doc, osw);
			osw.flush();
			return true;
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#saveDocument(de.uka.ipd.idaho.gamta.MutableAnnotation, java.io.Writer)
		 */
		public boolean saveDocument(QueriableAnnotation data, Writer out) throws IOException {
			return this.saveDocument(null, data, out);
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#saveDocument(de.uka.ipd.idaho.goldenGate.DocumentEditor, java.io.Writer)
		 */
		public boolean saveDocument(DocumentEditor data, Writer out) throws IOException {
			return this.saveDocument(data, data.getContent(), out);
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#saveDocument(de.uka.ipd.idaho.goldenGate.DocumentEditor, de.uka.ipd.idaho.gamta.MutableAnnotation, java.io.Writer)
		 */
		public boolean saveDocument(DocumentEditor data, QueriableAnnotation doc, Writer out) throws IOException {
			GenericGamtaXML.storeDocument(doc, out);
			return true;
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#accept(java.lang.String)
		 */
		public boolean accept(String fileName) {
			return fileName.toLowerCase().endsWith(".gamta");
		}
		
		/** @see javax.swing.filechooser.FileFilter#getDescription()
		 */
		public String getDescription() {
			return "GAMTA files";
		}
		
		/** @see de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat#equals(de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat)
		 */
		public boolean equals(DocumentFormat format) {
			return ((format != null) && (format instanceof GamtaDocumentFormat));
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.Resource#getName()
		 */
		public String getName() {
			return "<GAMTA Document Format>";
		}
		
		/* (non-Javadoc)
		 * @see de.uka.ipd.idaho.goldenGate.plugins.Resource#getProviderClassName()
		 */
		public String getProviderClassName() {
			return ""; // we don't have an actual provider
		}
	}
	
	private class DioClientDocumentCache {
		private static final String EXPLICIT_CHECKOUT = "EC";
		private static final String DIRTY = "D";
		private StringVector metaDataStorageKeys = new StringVector();
		
		private GoldenGatePluginDataProvider dataProvider;
		private String host;
		private String user;
		
		private String cachePrefix;
		private TreeMap cacheMetaData = new TreeMap();
		private TreeSet openDocuments = new TreeSet();
		
		DioClientDocumentCache(GoldenGatePluginDataProvider dataProvider, String host, String user) {
			this.dataProvider = dataProvider;
			this.host = host;
			this.user = user;
			
			//	compute data prefix
			this.cachePrefix = ("cache/" + (this.host + "." + this.user).hashCode() + "/");
			
			//	load meta data
			try {
				Reader cmdIn = new InputStreamReader(this.dataProvider.getInputStream(this.cachePrefix + "MetaData.cnfg"));
				StringRelation cmd = StringRelation.readCsvData(cmdIn, '"');
				cmdIn.close();
				for (int d = 0; d < cmd.size(); d++) {
					StringTupel dmd = cmd.get(d);
					String cDocId = dmd.getValue(DOCUMENT_ID_ATTRIBUTE);
					if (cDocId != null)
						this.cacheMetaData.put(cDocId, dmd);
					
					//	learn keys
					this.metaDataStorageKeys.addContentIgnoreDuplicates(dmd.getKeys());
				}
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document cache meta data for document cache '" + this.cachePrefix + "'");
				ioe.printStackTrace(System.out);
				this.metaDataStorageKeys.addContentIgnoreDuplicates(cacheDocumentListAttributes);
			}
			
			//	make sure local attributes are stored
			this.metaDataStorageKeys.addElementIgnoreDuplicates(EXPLICIT_CHECKOUT);
			this.metaDataStorageKeys.addElementIgnoreDuplicates(DIRTY);
		}
		
		private void storeMetaData() throws IOException {
			StringRelation cmd = new StringRelation();
			for (Iterator cdit = this.cacheMetaData.values().iterator(); cdit.hasNext();)
				cmd.addElement((StringTupel) cdit.next());
			OutputStreamWriter out = new OutputStreamWriter(this.dataProvider.getOutputStream(this.cachePrefix + "MetaData.cnfg"), ENCODING);
			StringRelation.writeCsvData(out, cmd, '"', this.metaDataStorageKeys);
			out.flush();
			out.close();
		}
		
		synchronized void close() {
//			StringRelation cmd = new StringRelation();
//			for (Iterator cdit = this.cacheMetaData.values().iterator(); cdit.hasNext();)
//				cmd.addElement((StringTupel) cdit.next());
			
			try {
//				OutputStreamWriter out = new OutputStreamWriter(this.dataProvider.getOutputStream(this.cachePrefix + "MetaData.cnfg"), ENCODING);
//				StringRelation.writeCsvData(out, cmd, '"', this.metaDataStorageKeys);
//				out.flush();
//				out.close();
				this.storeMetaData();
				this.cacheMetaData.clear();
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while storing document cache meta data for document cache '" + this.cachePrefix + "'");
				ioe.printStackTrace(System.out);
			}
		}
		
		synchronized boolean belongsTo(String host, String user) {
			return (this.host.equals(host) && this.user.equals(user));
		}
		
		synchronized boolean storeDocument(QueriableAnnotation doc, String documentName) {
			
			//	collect meta data
			String time = ("" + System.currentTimeMillis());
			String docId = ((String) doc.getAttribute(DOCUMENT_ID_ATTRIBUTE));
			if (docId == null)
				docId = doc.getAnnotationID();
			
			String docTitle = ((String) doc.getAttribute(DOCUMENT_TITLE_ATTRIBUTE, documentName));
			
			String checkinUser = ((String) doc.getAttribute(CHECKIN_USER_ATTRIBUTE, this.user));
			String checkinTime = ((String) doc.getAttribute(CHECKIN_TIME_ATTRIBUTE, time));
			
			String docVersion = ((String) doc.getAttribute(DOCUMENT_VERSION_ATTRIBUTE, "-1"));
			
			//	organize meta data
			StringTupel docMetaData = ((StringTupel) this.cacheMetaData.get(docId));
			if (docMetaData == null)
				docMetaData = new StringTupel();
			
			docMetaData.setValue(DOCUMENT_ID_ATTRIBUTE, docId);
			
			docMetaData.setValue(DOCUMENT_NAME_ATTRIBUTE, documentName);
			docMetaData.setValue(DOCUMENT_TITLE_ATTRIBUTE, docTitle);
			
			docMetaData.setValue(CHECKIN_USER_ATTRIBUTE, checkinUser);
			docMetaData.setValue(CHECKIN_TIME_ATTRIBUTE, checkinTime);
			
			docMetaData.setValue(UPDATE_USER_ATTRIBUTE, this.user);
			docMetaData.setValue(UPDATE_TIME_ATTRIBUTE, time);
			
			docMetaData.setValue(DOCUMENT_VERSION_ATTRIBUTE, docVersion);
			
			//	store document
			try {
				OutputStreamWriter out = new OutputStreamWriter(this.dataProvider.getOutputStream(this.cachePrefix + docId + ".gamta.xml"), ENCODING);
				GenericGamtaXML.storeDocument(doc, out);
				out.flush();
				out.close();
				
				docMetaData.setValue(DIRTY, DIRTY);
				this.cacheMetaData.put(docId, docMetaData);
				this.metaDataStorageKeys.addContentIgnoreDuplicates(docMetaData.getKeys());
				
				this.storeMetaData();
				
				return true;
			}
			catch (IOException ioe) {
				JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("An error occurred while storing document '" + documentName + "' to local cache for GoldenGATE Server at " + this.host + "\n" + ioe.getMessage()), ("Error Caching Document"), JOptionPane.ERROR_MESSAGE);
				return false;
			}
		}
		
		synchronized DocumentRoot loadDocument(String docId, boolean showError) throws IOException {
			try {
				//	load document
				InputStreamReader in = new InputStreamReader(this.dataProvider.getInputStream(this.cachePrefix + docId + ".gamta.xml"), ENCODING);
				DocumentRoot doc = GenericGamtaXML.readDocument(in);
				in.close();
				
				//	set document ID
				if (!doc.hasAttribute(DOCUMENT_ID_ATTRIBUTE))
					doc.setAttribute(DOCUMENT_ID_ATTRIBUTE, docId);
				
				//	return document
				return doc;
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while loading document '" + docId + "' from cache");
				ioe.printStackTrace(System.out);
				if (showError) {
					JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("An error occurred while loading document '" + docId + "' from cache of GoldenGATE Server at '" + this.host + "'\n" + ioe.getMessage()), ("Error Loading Document From Cache"), JOptionPane.ERROR_MESSAGE);
					throw ioe;
				}
				else return null;
			}
		}
		
		synchronized void unstoreDocument(String docId) throws IOException {
			this.openDocuments.remove(docId);
			this.cacheMetaData.remove(docId);
			this.dataProvider.deleteData(this.cachePrefix + docId + ".gamta.xml");
			this.storeMetaData();
		}
		
		synchronized DocumentListBuffer getDocumentList() throws IOException {
			DocumentListBuffer documentList = new DocumentListBuffer(cacheDocumentListAttributes);
			for (Iterator cdit = this.cacheMetaData.values().iterator(); cdit.hasNext();) {
				StringTupel dmd = ((StringTupel) cdit.next());
				StringTupel listDmd = new StringTupel();
				for (int f = 0; f < documentList.listFieldNames.length; f++) {
					String listFieldValue = dmd.getValue(documentList.listFieldNames[f]);
					if (listFieldValue != null)
						listDmd.setValue(documentList.listFieldNames[f], listFieldValue);
				}
				if (listDmd.size() != 0)
					documentList.addElement(listDmd);
			}
			return documentList;
		}
		
		synchronized boolean containsDocument(String docId) {
			return this.cacheMetaData.containsKey(docId);
		}
		
		synchronized void markExplicitCheckout(String docId) {
			StringTupel dmd = ((StringTupel) this.cacheMetaData.get(docId));
			if (dmd != null) try {
				dmd.setValue(EXPLICIT_CHECKOUT, EXPLICIT_CHECKOUT);
				this.storeMetaData();
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while storing document cache meta data for document cache '" + this.cachePrefix + "'");
				ioe.printStackTrace(System.out);
			}
		}
		
		synchronized boolean isExplicitCheckout(String docId) {
			StringTupel dmd = ((StringTupel) this.cacheMetaData.get(docId));
			return ((dmd != null) && EXPLICIT_CHECKOUT.equals(dmd.getValue(EXPLICIT_CHECKOUT)));
		}
		
		synchronized boolean isDirty(String docId) {
			StringTupel dmd = ((StringTupel) this.cacheMetaData.get(docId));
			return ((dmd == null) ? false : (dmd.getValue(DIRTY) != null));
		}
		
		synchronized void markNotDirty(String docId) {
			StringTupel dmd = ((StringTupel) this.cacheMetaData.get(docId));
			if (dmd != null) try {
				dmd.removeValue(DIRTY);
				this.storeMetaData();
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while storing document cache meta data for document cache '" + this.cachePrefix + "'");
				ioe.printStackTrace(System.out);
			}
		}
		
		synchronized void markOpen(String docId) {
			this.openDocuments.add(docId);
		}
		
		synchronized boolean isOpen(String docId) {
			return this.openDocuments.contains(docId);
		}
		
		synchronized void flush(GoldenGateDioClient dioClient) {
			
			//	upload current version of all dirty documents
			for (Iterator cdit = (new ArrayList(this.cacheMetaData.values())).iterator(); cdit.hasNext();) {
				StringTupel dmd = ((StringTupel) cdit.next());
				String docId = dmd.getValue(DOCUMENT_ID_ATTRIBUTE);
				String docName = dmd.getValue(DOCUMENT_NAME_ATTRIBUTE);
				
				//	got required meta data
				if ((docId != null) && this.isDirty(docId) && (docName != null)) try {
					
					//	get document
					DocumentRoot doc = this.loadDocument(docId, false);
					if (doc != null) {
						String[] uploadLog = dioClient.updateDocument(doc, docName);
						System.out.println("Cached document '" + docId + "' uploaded to GoldenGATE Server at " + this.host + ":");
						for (int l = 0; l < uploadLog.length; l++)
							System.out.println(uploadLog[l]);
						this.markNotDirty(docId);
					}
				}
				catch (IOException ioe) {
					System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while uploading cached document '" + docId + "' to GoldenGATE Server at " + this.host);
					ioe.printStackTrace(System.out);
				}
			}
		}
		
		synchronized void cleanup(GoldenGateDioClient dioClient) {
			
			//	upload current version of all dirty documents
			for (Iterator cdit = (new ArrayList(this.cacheMetaData.values())).iterator(); cdit.hasNext();) {
				StringTupel dmd = ((StringTupel) cdit.next());
				String docId = dmd.getValue(DOCUMENT_ID_ATTRIBUTE);
				
				//	got required meta data
				if ((docId != null) && !this.isExplicitCheckout(docId) && !this.isOpen(docId)) {
					try {
						this.unstoreDocument(docId);
						dioClient.releaseDocument(docId);
					}
					catch (IOException ioe) {
						System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while releasing cached document '" + docId + "' at GoldenGATE Server at " + this.host);
						ioe.printStackTrace(System.out);
					}
				}
			}
		}
	}
}
