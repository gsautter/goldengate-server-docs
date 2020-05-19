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
package de.uka.ipd.idaho.goldenGateServer.dio.client.documentGateway;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.datatransfer.DataFlavor;
import java.awt.datatransfer.Transferable;
import java.awt.datatransfer.UnsupportedFlavorException;
import java.awt.dnd.DropTarget;
import java.awt.dnd.DropTargetAdapter;
import java.awt.dnd.DropTargetDropEvent;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JProgressBar;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.Token;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.GenericQueriableAnnotationWrapper;
import de.uka.ipd.idaho.gamta.util.transfer.DocumentListElement;
import de.uka.ipd.idaho.goldenGate.CustomFunction;
import de.uka.ipd.idaho.goldenGate.CustomShortcut;
import de.uka.ipd.idaho.goldenGate.DocumentEditor;
import de.uka.ipd.idaho.goldenGate.GoldenGATE;
import de.uka.ipd.idaho.goldenGate.GoldenGateConfiguration;
import de.uka.ipd.idaho.goldenGate.GoldenGateConstants;
import de.uka.ipd.idaho.goldenGate.configuration.FileConfiguration;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentFormat;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentLoader;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentLoader.DocumentData;
import de.uka.ipd.idaho.goldenGate.plugins.GoldenGatePlugin;
import de.uka.ipd.idaho.goldenGate.util.DialogPanel;
import de.uka.ipd.idaho.goldenGateServer.GoldenGateServerActivityLogger;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants.DuplicateExternalIdentifierException;
import de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient;
import de.uka.ipd.idaho.goldenGateServer.dst.DocumentStore;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManager;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManagerPlugin;
import de.uka.ipd.idaho.stringUtils.StringVector;
//import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentListElement;

/**
 * Utility for uploading batches of documents to GoldenGATE Server, namely to
 * DIO. Sub classes that want to do specific things before uploading a document
 * can do so by overwriting the checkDoument() method. Sub classes further have
 * to implement their own main() method and simply construct an instance of
 * themselves there. This class's constructor takes care of the rest.
 * 
 * @author sautter
 */
public class GgServerDocumentGateway implements GoldenGateConstants {
	
	/**
	 * The main method to run GoldenGATE Document Gateway as a standalone
	 * application
	 * @param args the arguments
	 */
	public static void main(String[] args) throws Exception {
		new GgServerDocumentGateway(args);
	}
	
	protected File basePath = null;
	
	/**
	 * Constructor
	 * @param args the arguments from the main method
	 */
	protected GgServerDocumentGateway(String[] args) throws Exception {
		
		//	set platform L&F
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {}
		
		String basePath = "./";
		boolean log = false;
		
		//	parse remaining args
		for (int a = 0; a < args.length; a++) {
			String arg = args[a];
			if (arg != null) {
				if (arg.startsWith(BASE_PATH_PARAMETER + "=")) basePath = arg.substring((BASE_PATH_PARAMETER + "=").length());
				else if (LOG_PARAMETER.equals(arg)) log = true;
			}
		}
		
		//	remember program base path
		this.basePath = new File(basePath);
		
		//	create log files
		long logTimestamp = System.currentTimeMillis();
		File sysOutFile = new File(basePath, ("GgServerDgSystemOut." + logTimestamp + ".log"));
		sysOutFile.createNewFile();
		System.setOut(new PrintStream(new FileOutputStream(sysOutFile)));
		File errorFile = new File(basePath, ("GgServerDgSystemOut." + logTimestamp + ".log"));
		errorFile.createNewFile();
		System.setErr(new PrintStream(new FileOutputStream(errorFile)));
		
		//	load parameters
		Settings parameters = new Settings();
		try {
			StringVector parameterLoader = StringVector.loadList(new File(this.basePath, PARAMETER_FILE_NAME));
			for (int p = 0; p < parameterLoader.size(); p++) try {
				String param = parameterLoader.get(p);
				int split = param.indexOf('=');
				if (split != -1) {
					String key = param.substring(0, split).trim();
					String value = param.substring(split + 1).trim();
					if ((key.length() != 0) && (value.length() != 0))
						parameters.setSetting(key, value);
				}
			} catch (Exception e) {}
		} catch (Exception e) {}
		
		//	configure web access
		if (parameters.containsKey(PROXY_NAME)) {
			System.getProperties().put("proxySet", "true");
			System.getProperties().put("proxyHost", parameters.getSetting(PROXY_NAME));
			if (parameters.containsKey(PROXY_PORT))
				System.getProperties().put("proxyPort", parameters.getSetting(PROXY_PORT));
			
			if (parameters.containsKey(PROXY_USER) && parameters.containsKey(PROXY_PWD)) {
				//	initialize proxy authentication
			}
		}
		
		//	get configuration
		GoldenGateConfiguration ggConfiguration = new FileConfiguration(MASTER_CONFIG_NAME, new File(basePath), true, true, (log ? new File(basePath, ("GgServerDocumentGateway." + System.currentTimeMillis() + ".log")) : null)) {
			//	filtering out CFM & CSM prevents pre-loading CF and CS accessible resources
			private GoldenGatePlugin[] plugins;
			public GoldenGatePlugin[] getPlugins() {
				if (this.plugins == null) {
					GoldenGatePlugin[] plugins = super.getPlugins();
					ArrayList pluginList = new ArrayList();
					for (int p = 0; p < plugins.length; p++) {
						if ((plugins[p] instanceof CustomFunction.Manager) || (plugins[p] instanceof CustomShortcut.Manager))
							continue;
						pluginList.add(plugins[p]);
					}
					this.plugins = ((GoldenGatePlugin[]) pluginList.toArray(new GoldenGatePlugin[pluginList.size()]));
				}
				return this.plugins;
			}
		};
		
		//	make sure icons are OK
		JFrame iconFrame = new JFrame();
		iconFrame.setIconImage(ggConfiguration.getIconImage());
		
		try {
			
			//	load GoldenGATE core
			final GoldenGATE goldenGate = GoldenGATE.openGoldenGATE(ggConfiguration, false, true);
			System.out.println(" - GoldenGATE core loaded");
			
			//	create gateway window
			final GgServerDocumentGatewayGUI ggsdg = new GgServerDocumentGatewayGUI(this, ggConfiguration, goldenGate);
			
			//	listen for exit
			ggsdg.addWindowListener(new WindowAdapter() {
				boolean closed = false;
				public void windowClosed(WindowEvent we) {
					if (this.closed) return;
					this.closed = true;
					goldenGate.exitShutdown();
					System.exit(0);
				}
			});
			
			//	open GUI
			ggsdg.setVisible(true);
		}
		catch (Exception e) {
			System.out.println("Exception starting GoldenGATE Document Gateway from configuration '" + ggConfiguration.getName() + "':\n   " + e.getClass().getName() + " (" + e.getMessage() + ")");
			e.printStackTrace(System.out);
			JOptionPane.showMessageDialog(iconFrame, ("Error starting GoldenGATE Document Gateway from configuration '" + ggConfiguration.getName() + "':\n   " + e.getClass().getName() + " (" + e.getMessage() + ")"), "Error Starting GoldenGATE Markup Wizard", JOptionPane.ERROR_MESSAGE);
			System.exit(0);
		}
	}
	
	/**
	 * Investigate and (optionally) adjust a document after being loaded. This
	 * default implementation simply returns true, sub classes are welcome to
	 * overwrite it as needed.
	 * @param docData the loading container providing the document and its data
	 * @return true if the document is ready for upload, false otherwise
	 */
	protected boolean checkDocument(DocumentData docData) {
		return true;
	}
	
	private static final String MASTER_CONFIG_NAME = "Local Master Configuration";
	private static final String WINDOW_BASE_TITLE = "GoldenGATE Server Document Gateway";
	
	private static class GgServerDocumentGatewayGUI extends JFrame {
		GgServerDocumentGateway parent;
		
		GoldenGateConfiguration ggConfiguration;
		GoldenGATE goldenGate;
		GoldenGateDioClient dioClient;
		
		ArrayList documents = new ArrayList();
		
		DocumentTableModel documentTableModel = new DocumentTableModel();
		JTable documentTable = new JTable(this.documentTableModel);
		
		JMenuItem uploadItem;
		JButton uploadButton;
		JButton cleanButton;
		
		GgServerDocumentGatewayGUI(GgServerDocumentGateway parent, GoldenGateConfiguration ggConfig, GoldenGATE goldenGate) {
			super(WINDOW_BASE_TITLE);
			
			this.parent = parent;
			this.ggConfiguration = ggConfig;
			this.goldenGate = goldenGate;
			
			this.setIconImage(ggConfiguration.getIconImage());
			this.setDefaultCloseOperation(DISPOSE_ON_CLOSE);
			
			
			//	build load menu
			JMenu loadMenu = new JMenu("Load Document");
			DocumentLoader[] dls = this.goldenGate.getDocumentLoaders();
			for (int l = 0; l < dls.length; l++) {
				if (dls[l].getClass().getName().indexOf(".goldenGateServer.") == -1) {
					JMenuItem loadItem = dls[l].getLoadDocumentMenuItem();
					if (loadItem != null) {
						final DocumentLoader loader = dls[l];
						loadItem.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								try {
									DocumentData dd = loader.loadDocument();
									if (dd != null)
										GgServerDocumentGatewayGUI.this.openDocument(dd);
								}
								catch (Exception e) {
									JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), e.getMessage(), "Exception Loading Document", JOptionPane.ERROR_MESSAGE);
								}
							}
						});
						loadMenu.add(loadItem);
					}
				}
			}
			
			//	add option to upload local DST
			try {
				
				//	check if DST on class path
				DocumentStore.class.getName();
				
				//	add menu item
				JMenuItem dstMenuItem = new JMenuItem("Open Local Collection");
				dstMenuItem.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						JFileChooser fc = this.getFileChooser();
						if (fc.showOpenDialog(GgServerDocumentGatewayGUI.this) == JFileChooser.APPROVE_OPTION)
							this.openDst(fc.getSelectedFile());
					}
					
					private JFileChooser fileChooser;
					private JFileChooser getFileChooser() {
						if (this.fileChooser == null) {
							this.fileChooser = new JFileChooser();
							this.fileChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
							this.fileChooser.setMultiSelectionEnabled(false);
							this.fileChooser.setAcceptAllFileFilterUsed(true);
						}
						return this.fileChooser;
					}
					
					private void openDst(File dstRoot) {
						if ((dstRoot == null) || !dstRoot.isDirectory()) {
							JOptionPane.showMessageDialog(GgServerDocumentGatewayGUI.this, ("The local document collection could not be opened." + ((dstRoot == null) ? "" : ("\n" + dstRoot.getAbsolutePath() + " is not a valid collection root."))), "Cannot Open Local Collection", JOptionPane.ERROR_MESSAGE);
							return;
						}
						
//						DocumentStore dst = new DocumentStore(dstRoot);
						DocumentStore dst = new DocumentStore("UploadTemp", dstRoot, GoldenGateServerActivityLogger.silent);
						String[] docIds = dst.getDocumentIDs();
						int openedDocCount = 0;
						StringVector errors = new StringVector(); 
						for (int d = 0; d < docIds.length; d++) try {
							DocumentRoot doc = dst.loadDocument(docIds[d]);
							String docName = ((String) doc.getAttribute(GoldenGateDioConstants.DOCUMENT_NAME_ATTRIBUTE));
							GgServerDocumentGatewayGUI.this.openDocument(new DocumentData(doc, ((docName == null) ? ("Unnamed Document '" + docIds[d] + "'") : docName), new GamtaDocumentFormat()));
							openedDocCount++;
						}
						catch (IOException ioe) {
							errors.addElement("Could not open document '" + docIds[d] + "': " + ioe.getMessage());
						}
						JOptionPane.showMessageDialog(GgServerDocumentGatewayGUI.this, (((openedDocCount == 0) ? "Could not open any documents" : ("Loaded " + openedDocCount + " documents")) + " from the local collection at " + dstRoot.getAbsolutePath() + (errors.isEmpty() ? "" : ("\n" + errors.size() + " errors occurred:\n- " + errors.concatStrings("\n- ")))), "Local Collection Opened", JOptionPane.INFORMATION_MESSAGE);
					}
					
					class GamtaDocumentFormat extends DocumentFormat {
						GamtaDocumentFormat() {}
						public boolean isExportFormat() {
							return false;
						}
						public String getDefaultSaveFileExtension() {
							return "gamta";
						}
						public String getFormatDefaultEncodingName() {
							return GoldenGateDioConstants.ENCODING;
						}
						public MutableAnnotation loadDocument(InputStream source) throws IOException {
							throw new IOException("Cannot load document from GoldenGATE DIO, upload only.");
						}
						public MutableAnnotation loadDocument(Reader source) throws IOException {
							throw new IOException("Cannot load document from GoldenGATE DIO, upload only.");
						}
						public boolean saveDocument(QueriableAnnotation data, OutputStream out) throws IOException {
							return this.saveDocument(null, data, out);
						}
						public boolean saveDocument(DocumentEditor data, OutputStream out) throws IOException {
							return this.saveDocument(data, data.getContent(), out);
						}
						public boolean saveDocument(DocumentEditor data, QueriableAnnotation doc, OutputStream out) throws IOException {
							OutputStreamWriter osw = new OutputStreamWriter(out, GoldenGateDioConstants.ENCODING);
							GenericGamtaXML.storeDocument(doc, osw);
							osw.flush();
							return true;
						}
						public boolean saveDocument(QueriableAnnotation data, Writer out) throws IOException {
							return this.saveDocument(null, data, out);
						}
						public boolean saveDocument(DocumentEditor data, Writer out) throws IOException {
							return this.saveDocument(data, data.getContent(), out);
						}
						public boolean saveDocument(DocumentEditor data, QueriableAnnotation doc, Writer out) throws IOException {
							GenericGamtaXML.storeDocument(doc, out);
							return true;
						}
						public boolean accept(String fileName) {
							return fileName.toLowerCase().endsWith(".gamta");
						}
						public String getDescription() {
							return "GAMTA files";
						}
						public boolean equals(DocumentFormat format) {
							return ((format != null) && (format instanceof GamtaDocumentFormat));
						}
						public String getName() {
							return "<GAMTA Document Format>";
						}
						public String getProviderClassName() {
							return ""; // we don't have an actual provider
						}
					}
				});
				loadMenu.addSeparator();
				loadMenu.add(dstMenuItem);
			}
			catch (Throwable t) {
				System.out.println("DST not available.");
			}
			
			//	build config menu
			JMenu dgMenu = new JMenu("Document Gateway");
			
			//	add upload option
			this.uploadItem = new JMenuItem("Upload Documents");
			this.uploadItem.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					GgServerDocumentGatewayGUI.this.uploadDocuments();
				}
			});
			dgMenu.add(this.uploadItem);
			dgMenu.addSeparator();
			
			//	add logout option
			JMenuItem logoutItem = new JMenuItem("Log Out From Server");
			logoutItem.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					AuthenticationManager.logout();
				}
			});
			dgMenu.add(logoutItem);
			
			//	add exit option
			JMenuItem exitItem = new JMenuItem("Exit Document Gateway");
			exitItem.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					GgServerDocumentGatewayGUI.this.dispose();
				}
			});
			dgMenu.add(exitItem);
			
			//	assemble menu bar
			JMenuBar menu = new JMenuBar();
			menu.add(loadMenu);
			menu.add(dgMenu);
			menu.add(this.goldenGate.getHelpMenu());
			
			
			//	create upload button
			this.uploadButton = new JButton("Upload Documents");
			this.uploadButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.uploadButton.setPreferredSize(new Dimension(100, 21));
			this.uploadButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					GgServerDocumentGatewayGUI.this.uploadDocuments();
				}
			});
			
			//	create edit button
			this.cleanButton = new JButton("Clear Document List");
			this.cleanButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.cleanButton.setPreferredSize(new Dimension(100, 21));
			this.cleanButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					GgServerDocumentGatewayGUI.this.documents.clear();
					GgServerDocumentGatewayGUI.this.refreshGui();
				}
			});
			
			//	line up buttons
			JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.CENTER));
			buttonPanel.add(this.uploadButton);
			buttonPanel.add(this.cleanButton);
			
			
			//	create document table
			this.documentTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
			this.documentTable.getColumnModel().getColumn(1).setMaxWidth(60);
			
			//	add table context menu
			this.documentTable.addMouseListener(new MouseAdapter() {
				public void mouseClicked(MouseEvent me) {
					if (me.getButton() != MouseEvent.BUTTON1) {
						final int row = documentTable.getSelectedRow();
						if (row == -1) return;
						
						JPopupMenu pm = new JPopupMenu();
						JMenuItem mi = new JMenuItem("Remove from List");
						mi.addActionListener(new ActionListener() {
							public void actionPerformed(ActionEvent ae) {
								documents.remove(row);
								refreshGui();
							}
						});
						pm.add(mi);
						pm.show(documentTable, me.getX(), me.getY());
					}
				}
			});
			
			//	make table scrollable
			JScrollPane documentTableBox = new JScrollPane(this.documentTable);
			documentTableBox.getVerticalScrollBar().setUnitIncrement(20);
			documentTableBox.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED);
			documentTableBox.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
			
			//	initialize GUI
			this.refreshGui();
			
			//	create main panel
			JSplitPane mainPanel = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT);
			mainPanel.setResizeWeight(.5);
			mainPanel.setDividerLocation(.5);
			mainPanel.setLeftComponent(new DocumentDropPanel(this));
			mainPanel.setRightComponent(documentTableBox);
			
			this.getContentPane().setLayout(new BorderLayout());
			this.getContentPane().add(menu, BorderLayout.NORTH);
			this.getContentPane().add(mainPanel, BorderLayout.CENTER);
			this.getContentPane().add(buttonPanel, BorderLayout.SOUTH);
			
			this.setSize(600, 400);
			this.setResizable(true);
			this.setLocationRelativeTo(null);
		}
		
		void openDocument(DocumentData dd) {
			System.out.println("GgServerDocumentGateway: opening document '" + dd.name + "'");
			if (this.parent.checkDocument(dd)) {
				if (dd.name != null)
					dd.docData.setAttribute(GoldenGateDioConstants.DOCUMENT_NAME_ATTRIBUTE, dd.name);
				this.documents.add(dd);
				this.refreshGui();
			}
		}
		
		void uploadDocuments() {
			final int docCount = this.documents.size();
			if (docCount == 0)
				return;
			
			if (this.dioClient == null) {
				AuthenticationManagerPlugin authManager = ((AuthenticationManagerPlugin) this.goldenGate.getPlugin(AuthenticationManagerPlugin.class.getName()));
				if (authManager == null) {
					JOptionPane.showMessageDialog(this, "Authentication Manager missing, cannot connect to GoldenGATE Server", "Cannot Connect to Server", JOptionPane.ERROR_MESSAGE);
					return;
				}
				
				AuthenticatedClient authClient = authManager.getAuthenticatedClient();
				if (authClient == null) {
					AuthenticationManager.logout();
					return;
				}
				
				this.dioClient = new GoldenGateDioClient(authClient);
			}
			
			final UploadDialog uploadDialog = new UploadDialog();
			Thread uploadThread = new Thread() {
				public void run() {
					while (!uploadDialog.isVisible()) try {
						Thread.sleep(100);
					} catch (InterruptedException ie) {}
					
					try {
						for (int d = 0; d < documents.size(); d++) {
							DocumentData dd = ((DocumentData) documents.get(d));
							QueriableAnnotation observingDoc = new WriteProgressObserver(dd.docData, dd.name, uploadDialog);
							try {
								String[] uploadProtocol = dioClient.uploadDocument(observingDoc, dd.name);
								uploadDialog.addLog(uploadProtocol);
								documents.remove(d--);
								if (uploadDialog.isInterrupted())
									d = documents.size();
							}
							catch (DuplicateExternalIdentifierException deie) {
								StringVector conflictDocuments = new StringVector();
								DocumentListElement[] dles = deie.getConflictingDocuments();
								for (int c = 0; c < dles.length; c++)
									conflictDocuments.addElement(" - " + dles[c].getAttribute(GoldenGateDioConstants.DOCUMENT_NAME_ATTRIBUTE) + ", last edited by " + dles[c].getAttribute(GoldenGateDioConstants.UPDATE_USER_ATTRIBUTE));
								if (JOptionPane.showConfirmDialog(uploadDialog, ("Error uploading document '" + dd.name + "': " + deie.getMessage() + "\n" + conflictDocuments.concatStrings("\n") + "\nStore the document anyway?"), ("Duplicate External Identifier"), JOptionPane.YES_NO_OPTION, JOptionPane.ERROR_MESSAGE) == JOptionPane.YES_OPTION) {
									String[] uploadProtocol = dioClient.uploadDocument(observingDoc, dd.name, GoldenGateDioConstants.EXTERNAL_IDENTIFIER_MODE_IGNORE);
									uploadDialog.addLog(uploadProtocol);
									documents.remove(d--);
									if (uploadDialog.isInterrupted())
										d = documents.size();
								}
								else if (JOptionPane.showConfirmDialog(uploadDialog, ("Could not upload document '" + dd.name + "' due to a duplicate " + deie.externalIdentifierAttributeName + "\nContinue uploading with next document?"), "Error Uploading Document", JOptionPane.YES_NO_OPTION, JOptionPane.ERROR_MESSAGE) != JOptionPane.YES_OPTION)
									d = documents.size();
							}
							catch (IOException ioe) {
								System.out.println("Error uploading document '" + dd.name + "': " + ioe.getMessage());
								ioe.printStackTrace(System.out);
								if (JOptionPane.showConfirmDialog(uploadDialog, ("Error uploading document '" + dd.name + "': " + ioe.getMessage() + "\nContinue uploading?"), "Error Uploading Document", JOptionPane.YES_NO_OPTION, JOptionPane.ERROR_MESSAGE) != JOptionPane.YES_OPTION)
									d = documents.size();
							}
						}
					}
					
					catch (Throwable t) {
						System.out.println("Error uploading document '" + uploadDialog.docName + "': " + t.getMessage());
						t.printStackTrace(System.out);
						JOptionPane.showMessageDialog(uploadDialog, ("An error occurred while a uploading document '" + uploadDialog.docName + "':\n" + t.getMessage()), ("Error Uploading Document"), JOptionPane.ERROR_MESSAGE);
					}
					
					finally {
						JOptionPane.showMessageDialog(uploadDialog, ((docCount - documents.size()) + " document(s) sucessfully uploaded to GoldenGATE Server"), "Document Upload Successful", JOptionPane.INFORMATION_MESSAGE);
						uploadDialog.finished();
					}
				}
			};
			uploadThread.start();
			uploadDialog.setVisible(true);
			
			this.refreshGui();
		}
		
		private static class UploadDialog extends DialogPanel {
			private static class LogTableModel extends AbstractTableModel {
				StringVector log = new StringVector();
				public int getColumnCount() {
					return 1;
				}
				public int getRowCount() {
					return this.log.size();
				}
				public Object getValueAt(int rowIndex, int columnIndex) {
					return this.log.get(rowIndex);
				}
				void addLog(String[] log) {
					this.log.addContent(log);
					this.fireTableRowsInserted(0, (this.log.size() - 1));
				}
				void addLog(String log) {
					this.log.addElement(log);
					this.fireTableRowsInserted(0, (this.log.size() - 1));
				}
			}
			
			String docName = "";
			private JLabel docLabel = new JLabel("", JLabel.LEFT);
			
			private LogTableModel logTableModel = new LogTableModel();
			private JTable logTable = new JTable(this.logTableModel);
			private JScrollPane logTableBox = new JScrollPane(this.logTable);
			
			JProgressBar progress = new JProgressBar();
			
			private JButton button = new JButton("");
			private boolean interrupted = false;
			
			UploadDialog() {
				super("Uploading Documents to GoldenGATE Server", true);
				this.getContentPane().setLayout(new BorderLayout());
				
				JPanel dataPanel = new JPanel(new BorderLayout());
				dataPanel.setBorder(BorderFactory.createEtchedBorder());
				
				dataPanel.add(this.docLabel, BorderLayout.NORTH);
				
				this.logTable.setShowHorizontalLines(true);
				this.logTable.setShowVerticalLines(false);
				this.logTable.setTableHeader(null);
				
				this.logTableBox.setViewportBorder(BorderFactory.createLoweredBevelBorder());
				this.logTableBox.getVerticalScrollBar().setUnitIncrement(50);
				
				dataPanel.add(this.logTableBox, BorderLayout.CENTER);
				
				this.progress.setStringPainted(true);
				dataPanel.add(this.progress, BorderLayout.SOUTH);
				
				this.button.setText("Stop After Document");
				this.button.setBorder(BorderFactory.createRaisedBevelBorder());
				this.button.setPreferredSize(new Dimension(100, 21));
				this.button.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						button.setText("Stopping After Document ...");
						button.setEnabled(false);
						interrupted = true;
					}
				});
				
				this.getContentPane().add(dataPanel, BorderLayout.CENTER);
				this.getContentPane().add(this.button, BorderLayout.SOUTH);
				
				//	show dialog
				this.setSize(500, 600);
				this.setLocationRelativeTo(this.getOwner());
			}
			void newDocument(String docName) {
				this.docName = docName;
				this.docLabel.setText("<HTML>Uploading document <B>" + this.docName + "</B></HTML>");
				this.logTableModel.addLog("===== Uploading document '" + this.docName + "' =====");
				this.progress.setValue(0);
			}
			void docFinished() {
				this.docLabel.setText("<HTML>Upload of document <B>" + this.docName + "</B> finished,<BR>please wait while GoldenGATE Server processes your document.</HTML>");
			}
			void addLog(String[] log) {
				this.logTableModel.addLog(log);
				final JScrollBar sb = this.logTableBox.getVerticalScrollBar();
				SwingUtilities.invokeLater(new Runnable() {
					public void run() {
						sb.setValue(sb.getMaximum());
						validate();
						repaint();
					}
				});
			}
			void finished() {
				this.button.setText("OK");
				this.button.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						dispose();
					}
				});
			}
			boolean isInterrupted() {
				return this.interrupted;
			}
		}
		
		private static class WriteProgressObserver extends GenericQueriableAnnotationWrapper {
			private int dataSize = 1;
			private UploadDialog ud;
			WriteProgressObserver(QueriableAnnotation data, String docName, UploadDialog ud) {
				super(data);
				this.dataSize = data.size();
				this.ud = ud;
				this.ud.newDocument(docName);
			}
			public Token tokenAt(int index) {
				this.ud.progress.setValue(((index+1) * 100) / this.dataSize);
				this.checkFinished(index);
				return super.tokenAt(index);
			}
			public String valueAt(int index) {
				this.ud.progress.setValue(((index+1) * 100) / this.dataSize);
				this.checkFinished(index);
				return super.valueAt(index);
			}
			private void checkFinished(int index) {
				if ((index+1) == this.dataSize)
					this.ud.docFinished();
			}
		}
		
		void refreshGui() {
			this.uploadItem.setEnabled(this.documents.size() != 0);
			this.uploadButton.setEnabled(this.documents.size() != 0);
			this.cleanButton.setEnabled(this.documents.size() != 0);
			
			this.documentTableModel.update();
			this.documentTable.validate();
			this.documentTable.repaint();
		}
		
		private class DocumentTableModel implements TableModel {
			ArrayList listeners = new ArrayList();
			public int getColumnCount() {
				return 2;
			}
			public String getColumnName(int columnIndex) {
				if (columnIndex == 0) return "Document Name";
				else if (columnIndex == 1) return "Size";
				else return null;
			}
			public Class getColumnClass(int columnIndex) {
				return String.class;
			}
			
			public int getRowCount() {
				return documents.size();
			}
			public Object getValueAt(int rowIndex, int columnIndex) {
				DocumentData dd = ((DocumentData) documents.get(rowIndex));
				if (columnIndex == 0) return dd.name;
				else if (columnIndex == 1) return ("" + dd.docData.size());
				else return null;
			}
			
			public boolean isCellEditable(int rowIndex, int columnIndex) {
				return false;
			}
			public void setValueAt(Object aValue, int rowIndex, int columnIndex) {}
			
			public void addTableModelListener(TableModelListener tml) {
				this.listeners.add(tml);
			}
			public void removeTableModelListener(TableModelListener tml) {
				this.listeners.remove(tml);
			}
			void update() {
				for (int l = 0; l < this.listeners.size(); l++)
					((TableModelListener) this.listeners.get(l)).tableChanged(new TableModelEvent(this));
			}
		}
	}
	
	private static class DocumentDropPanel extends JPanel {
		GgServerDocumentGatewayGUI parent;
		
		DocumentDropPanel(GgServerDocumentGatewayGUI parent) {
			super(new BorderLayout(), true);
			this.parent = parent;
			
			JPanel dropPanel = new JPanel();
			dropPanel.setBorder(BorderFactory.createLineBorder(Color.RED, 3));
			DropTarget dropTarget = new DropTarget(dropPanel, new DropTargetAdapter() {
				public void drop(DropTargetDropEvent dtde) {
					dtde.acceptDrop(dtde.getDropAction());
					Transferable transfer = dtde.getTransferable();
					DataFlavor[] dataFlavors = transfer.getTransferDataFlavors();
					for (int d = 0; d < dataFlavors.length; d++) {
						System.out.println(dataFlavors[d].toString());
						System.out.println(dataFlavors[d].getRepresentationClass());
						try {
							Object transferData = transfer.getTransferData(dataFlavors[d]);
							System.out.println(transferData.getClass().getName());
							
							List transferList = ((List) transferData);
							if (transferList.isEmpty()) return;
							
							for (int f = 0; f < transferList.size(); f++) {
								File droppedFile = ((File) transferList.get(f));
								try {
									String fileName = droppedFile.getName();
									String dataType;
									if ((fileName.indexOf('.') == -1) || fileName.endsWith(".")) dataType = "xml";
									else dataType = fileName.substring(fileName.lastIndexOf('.') + 1);
									
									DocumentFormat format = DocumentDropPanel.this.parent.goldenGate.getDocumentFormatForFileExtension(dataType);
									if (format == null) {
										String formatName = DocumentDropPanel.this.parent.goldenGate.selectLoadFormat();
										if (formatName != null)
											format = DocumentDropPanel.this.parent.goldenGate.getDocumentFormatForName(formatName);
									}
									
									if (format == null) JOptionPane.showMessageDialog(DocumentDropPanel.this, ("GoldenGATE Markup Wizzard cannot open the dropped file, sorry,\nthe data format in '" + droppedFile.getAbsolutePath() + "' is unknown."), "Unknown Document Format", JOptionPane.INFORMATION_MESSAGE);
									else {
										InputStream source = new FileInputStream(droppedFile);
										MutableAnnotation doc = format.loadDocument(source);
										source.close();
										if (doc != null)
											DocumentDropPanel.this.parent.openDocument(new DocumentData(doc, fileName, format));
									}
								}
								catch (IOException ioe) {
									System.out.println("Error opening document '" + droppedFile.getAbsolutePath() + "':\n   " + ioe.getClass().getName() + " (" + ioe.getMessage() + ")");
									ioe.printStackTrace(System.out);
								}
							}
						}
						catch (UnsupportedFlavorException ufe) {
							ufe.printStackTrace(System.out);
						}
						catch (IOException ioe) {
							ioe.printStackTrace(System.out);
						}
						catch (Exception e) {
							e.printStackTrace(System.out);
						}
					}
				}
			});
			dropTarget.setActive(true);
			
			this.add(dropPanel, BorderLayout.CENTER);
			this.add(new JLabel("<HTML><B>Drop documents<BR>in the red frame<BR>to open them.</B></HTML>", JLabel.CENTER), BorderLayout.NORTH);
		}
	}
}
