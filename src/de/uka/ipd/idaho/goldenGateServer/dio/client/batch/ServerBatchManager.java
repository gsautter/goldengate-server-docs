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
package de.uka.ipd.idaho.goldenGateServer.dio.client.batch;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.PatternSyntaxException;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.WindowConstants;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import de.uka.ipd.idaho.easyIO.util.RandomByteSource;
import de.uka.ipd.idaho.gamta.Gamta;
import de.uka.ipd.idaho.gamta.MutableAnnotation;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.AnnotationInputStream;
import de.uka.ipd.idaho.gamta.util.swing.ProgressMonitorDialog;
import de.uka.ipd.idaho.goldenGate.DocumentEditorDialog;
import de.uka.ipd.idaho.goldenGate.GoldenGATE;
import de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentProcessor;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentProcessorManager;
import de.uka.ipd.idaho.goldenGate.plugins.Resource;
import de.uka.ipd.idaho.goldenGate.util.DialogPanel;
import de.uka.ipd.idaho.goldenGate.util.ResourceDialog;
import de.uka.ipd.idaho.goldenGateServer.dio.GoldenGateDioConstants;
import de.uka.ipd.idaho.goldenGateServer.dio.client.GoldenGateDioClient;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentList;
import de.uka.ipd.idaho.goldenGateServer.dio.data.DocumentListBuffer;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticatedClient;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManager;
import de.uka.ipd.idaho.goldenGateServer.uaa.client.AuthenticationManagerPlugin;
import de.uka.ipd.idaho.stringUtils.StringVector;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * GoldenGATE Editor plugin for running document processors on entire
 * collections stored in a GoldenGATE DIO. Running a server batch requires a
 * user to log in to the targeted DIO with administrative proviledges.
 * 
 * @author sautter
 */
public class ServerBatchManager extends AbstractGoldenGatePlugin {
	
	private AuthenticationManagerPlugin authManager = null;
	
	/*
	 * @see de.uka.ipd.idaho.goldenGate.plugins.ResourceManager#getResourceTypeLabel()
	 */
	public String getResourceTypeLabel() {
		return "Server Batch";
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#getPluginName()
	 */
	public String getPluginName() {
		return "Server Batch Manager";
	}
	
	/** @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#init()
	 */
	public void init() {
		this.authManager = ((AuthenticationManagerPlugin) this.parent.getPlugin(AuthenticationManagerPlugin.class.getName()));
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#exit()
	 */
	public void exit() {
		if (this.batchDialog != null) {
			final BatchRunDialog batchDialog = this.batchDialog;
			this.batchDialog = null;
			
			//	we have to do this in an extra thread so we don't block invoker (yet)
			Thread batchDialogShower = new Thread() {
				public void run() {
					batchDialog.setTitle("Please Wait for Running Server Batch to Terminate");
					batchDialog.setVisible(true);
				}
			};
			batchDialogShower.start();
			
			//	shut down batch and wait
			batchDialog.stop();
			while (!batchDialog.closeButton.isEnabled()) try {
				Thread.sleep(100);
			} catch (InterruptedException ie) {}
			batchDialog.dispose();
			try {
				batchDialogShower.join();
			} catch (InterruptedException ie) {}
			
			//	reset remaining stuff
			this.openBatchDialogMain.setText("Run");
			this.openBatchDialogTools.setText("Run Server Batch");
		}
	}
	
	/*
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractDocumentProcessorManager#getMainMenuItems()
	 */
	public JMenuItem[] getMainMenuItems() {
		if (this.openBatchDialogMain == null) {
			this.openBatchDialogMain = new JMenuItem("Run");
			this.openBatchDialogMain.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					openServerBatch();
				}
			});
		}
		JMenuItem[] mis = {this.openBatchDialogMain};
		return mis;
	}
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractGoldenGatePlugin#getToolsMenuFunctionItems(de.uka.ipd.idaho.goldenGate.GoldenGateConstants.InvokationTargetProvider)
	 */
	public JMenuItem[] getToolsMenuFunctionItems(InvokationTargetProvider targetProvider) {
		if (this.openBatchDialogTools == null) {
			this.openBatchDialogTools = new ToolsMenuItem("Run Server Batch");
			this.openBatchDialogTools.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					openServerBatch();
				}
			});
		}
		JMenuItem[] mis = {this.openBatchDialogTools.getClone()};
		return mis;
	}
	
	/*
	 * @see de.uka.ipd.idaho.goldenGate.plugins.AbstractDocumentProcessorManager#getMainMenuTitle()
	 */
	public String getMainMenuTitle() {
		return "Server Batches";
	}
	
	private JMenuItem openBatchDialogMain = null;
	private ToolsMenuItem openBatchDialogTools = null;
	private BatchRunDialog batchDialog = null;
	private void openServerBatch() {
		if (this.batchDialog == null) {
			this.batchDialog = new BatchRunDialog("Run Server Batch");
			this.batchDialog.setSize(new Dimension(600, 400));
			this.batchDialog.setLocationRelativeTo(batchDialog.getOwner());
		}
		this.batchDialog.setVisible(true);
	}
	
	private class BatchRunDialog extends DialogPanel {
		
		private BatchDataPanel dataPanel;
		
		private JLabel statusLabel = new JLabel("", JLabel.CENTER) {
			public void setText(String text) {
				super.setText("<HTML><B>" + text + "</B></HTML>");
			}
		};
		
		private JCheckBox interactive = new JCheckBox("Interactive", true);
		private JCheckBox inspectDocument = new JCheckBox("Inspect Document", false);
		
		private JButton startButton = new JButton("Start");
		private JButton pauseButton = new JButton("Pause");
		private JButton stopButton = new JButton("Stop");
		private JButton resetButton = new JButton("Reset");
		private JButton backgroundButton = new JButton("Background");
		private JButton closeButton = new JButton("Close");
		
		JProgressBar progressBar = new JProgressBar(0, 100);
		private StringVector logLines = new StringVector();
		private JTextArea logDisplay = new JTextArea();
		
		private Batch batch = null;
		
		BatchRunDialog(String title) {
			super(title, true);
			this.setDefaultCloseOperation(WindowConstants.DO_NOTHING_ON_CLOSE);
			
			this.dataPanel = new BatchDataPanel();
			this.dataPanel.addChangeListener(new ChangeListener() {
				public void stateChanged(ChangeEvent ce) {
					Thread ubdThread = new Thread() {
						public void run() {
							updateBatchData();
						}
					};
					ubdThread.start();
				}
			});
			
			
			this.interactive.setBorder(BorderFactory.createEtchedBorder());
			this.inspectDocument.setBorder(BorderFactory.createEtchedBorder());
			
			this.statusLabel.setBorder(BorderFactory.createLineBorder(Color.RED));
			
			this.startButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.startButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					start();
				}
			});
			
			this.pauseButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.pauseButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					pause();
				}
			});
			
			this.stopButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.stopButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					stop();
				}
			});
			
			this.resetButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.resetButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					reset();
				}
			});
			
			this.backgroundButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.backgroundButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					openBatchDialogMain.setText("Re-Open");
					openBatchDialogTools.setText("Re-Open Running Server Batch");
					dispose();
				}
			});
			
			this.closeButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.closeButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					batchDialog = null;
					openBatchDialogMain.setText("Run");
					openBatchDialogTools.setText("Run Server Batch");
					dispose();
				}
			});
			
			this.progressBar.setStringPainted(true);
			
			
			JPanel detailPanel = new JPanel(new GridBagLayout(), true);
			GridBagConstraints gbc = new GridBagConstraints();
			gbc.gridwidth = 1;
			gbc.gridheight = 1;
			gbc.weightx = 1;
			gbc.weighty = 1;
			gbc.fill = GridBagConstraints.BOTH;
			gbc.insets.left = 5;
			gbc.insets.right = 5;
			gbc.insets.top = 3;
			gbc.insets.bottom = 3;
			
			gbc.gridy = 0;
			gbc.gridx = 0;
			gbc.gridwidth = 3;
			detailPanel.add(this.progressBar, gbc.clone());
			gbc.gridx += 3;
			gbc.gridwidth = 1;
			detailPanel.add(this.statusLabel, gbc.clone());
			
			gbc.gridy ++;
			gbc.gridx = 0;
			detailPanel.add(this.interactive, gbc.clone());
			gbc.gridx++;
			detailPanel.add(this.inspectDocument, gbc.clone());
			gbc.gridx++;
			detailPanel.add(this.backgroundButton, gbc.clone());
			gbc.gridx++;
			detailPanel.add(this.closeButton, gbc.clone());
			
			gbc.gridy ++;
			gbc.gridx = 0;
			detailPanel.add(this.startButton, gbc.clone());
			gbc.gridx++;
			detailPanel.add(this.pauseButton, gbc.clone());
			gbc.gridx++;
			detailPanel.add(this.stopButton, gbc.clone());
			gbc.gridx++;
			detailPanel.add(this.resetButton, gbc.clone());
			
			
			this.add(this.dataPanel, BorderLayout.NORTH);
			this.add(new JScrollPane(this.logDisplay), BorderLayout.CENTER);
			this.add(detailPanel, BorderLayout.SOUTH);
			
			this.updateBatchData();
			this.updateStatus();
		}
		
		private GoldenGateDioClient dioClient;
		private DocumentListBuffer docList;
		private String docListKey = "";
		private DocumentProcessor processor;
		
		private TreeMap allDocData = new TreeMap();
		private TreeSet todoDocIDs = new TreeSet();
		private TreeSet skippedDocIDs = new TreeSet();
		private TreeSet doneDocIDs = new TreeSet();
		
		private String doneLogFileName;
		
		private void updateBatchData() {
			
			//	got all parameters, load data objects
			if (this.dataPanel.isComplete()) {
				this.dioClient = this.dataPanel.dioClient;
				this.processor = parent.getDocumentProcessorForName(this.dataPanel.processorSelector.getProcessorName());
			}
			
			//	check if all objects valid
			if (this.isBatchRunnable()) {
				
				//	list documents to process
				ProgressMonitorDialog pmd = null;
				try {
					
					//	(re)load document list from server if necessary
					String docListKey = (authManager.getUser() + "@" + authManager.getHost());
					if ((this.docList == null) || !this.docListKey.equals(docListKey)) {
						pmd = new ProgressMonitorDialog(DialogPanel.getTopWindow(), "Loading Document List");
						pmd.setSize(400, 100);
						pmd.setLocationRelativeTo(this);
						pmd.popUp(false);
						pmd.setInfo("Getting document list from GoldenGATE DIO ...");
						DocumentList dl = this.dioClient.getDocumentList();
						this.docList = new DocumentListBuffer(dl, pmd);
						this.docListKey = docListKey;
						pmd.close();
						pmd = null;
					}
					
					//	filter documents
					this.allDocData.clear();
					for (int d = 0; d < this.docList.size(); d++) {
						StringTupel docData = this.docList.get(d);
						
						//	check if document checked out
						String checkoutUser = docData.getValue(GoldenGateDioConstants.CHECKOUT_USER_ATTRIBUTE);
						if (((checkoutUser == null) || checkoutUser.equals("") || checkoutUser.equals(authManager.getUser())) && this.dataPanel.matchesFilter(docData))
							this.allDocData.put(docData.getValue(GoldenGateDioConstants.DOCUMENT_ID_ATTRIBUTE), docData);
					}
				}
				catch (IOException ioe) {
					JOptionPane.showMessageDialog(this, ("Could not load document list from GoldenGATE Server at " + authManager.getHost() + ":\n" + ioe.getMessage()), "Error Loading Document List", JOptionPane.ERROR_MESSAGE);
					ioe.printStackTrace(System.out);
					this.dioClient = null;
					return;
				}
				finally {
					if (pmd != null)
						pmd.close();
					pmd = null;
				}
				
				//	load IDs already processed
				this.doneLogFileName = ((authManager.getHost() + "-" + this.processor.getName()).replaceAll("[^a-zA-Z0-9\\-\\_\\.]", "_") + ".done");
				this.skippedDocIDs.clear();
				this.doneDocIDs.clear();
				try {
					BufferedReader br = new BufferedReader(new InputStreamReader(dataProvider.getInputStream(this.doneLogFileName)));
					String doneDocId;
					while ((doneDocId = br.readLine()) != null) {
						if (this.allDocData.containsKey(doneDocId))
							this.doneDocIDs.add(doneDocId);
					}
					br.close();
				}
				catch (IOException ioe) {}
				
				//	diff doc ID lists
				this.todoDocIDs.clear();
				for (Iterator dit = this.allDocData.keySet().iterator(); dit.hasNext();) {
					String docId = ((String) dit.next());
					if (!this.doneDocIDs.contains(docId))
						this.todoDocIDs.add(docId);
				}
				
				//	show progress
				this.progressBar.setMaximum(this.allDocData.size());
				this.updateProgress();
			}
			
			//	something's missing, clean up
			else {
				this.dioClient = null;
				this.processor = null;
				this.allDocData.clear();
				this.todoDocIDs.clear();
				this.skippedDocIDs.clear();
				this.doneDocIDs.clear();
				this.doneLogFileName = null;
				
				this.progressBar.setMaximum(100);
				this.progressBar.setValue(0);
				this.progressBar.setString("Progress will be indicated when all data is present");
			}
			
			//	reflect state in buttons
			this.updateStatus();
		}
		
		private boolean isBatchRunnable() {
			return (true
					&& (this.dioClient != null)
					&& (this.processor != null)
					);
		}
		
		private StringVector getErrorReport() {
			StringVector errorMessages = new StringVector();
			
			if (this.dioClient == null)
				errorMessages.addElement("No GoldenGATE Server is specified, or login failed, or the document list could not be loaded.");
			
			if (this.processor == null)
				errorMessages.addElement("The selected document processor does not exist or could not be loaded.");
			
			return errorMessages;
		}
		
		void documentSkipped(String docId) {
			this.todoDocIDs.remove(docId);
			this.skippedDocIDs.add(docId);
			this.updateProgress();
		}
		
		void documentFinished(String docId) {
			this.todoDocIDs.remove(docId);
			this.doneDocIDs.add(docId);
			this.updateProgress();
		}
		
		private void updateProgress() {
			this.progressBar.setValue(this.doneDocIDs.size() + this.skippedDocIDs.size());
			this.progressBar.setString(this.doneDocIDs.size() + " of " + this.allDocData.size() + " documents processed" + (this.skippedDocIDs.isEmpty() ? "" : (", " + this.skippedDocIDs.size() + " skipped")));
		}
		
		StringTupel getNextDocument() {
			return (this.todoDocIDs.isEmpty() ? null : ((StringTupel) this.allDocData.get(this.todoDocIDs.first())));
		}
		
		private void start() {
			
			//	start after shutdown
			if (this.batch == null) {
				
				//	check parameters
				if (!this.dataPanel.isComplete()) {
					JOptionPane.showMessageDialog(this, "The batch cannot be executed in its current state.\nMake sure you specified all the parameters required:\n - An input folder and input format\n - An output folder and output format\n - The document processor to run on the documents", "Cannot Execute Batch", JOptionPane.ERROR_MESSAGE);
					return;
				}
				
				//	gather data and check if complete
				if (this.isBatchRunnable()) {
					
					//	initialize batch
					this.batch = new Batch(parent, this);
					
					//	start batch and wait until it's running
					synchronized (this.batch) {
						this.batch.start();
						try {
							this.batch.wait();
						} catch (InterruptedException ie) {}
					}
				}
				
				//	something's missing
				else {
					StringVector errorReport = this.getErrorReport();
					JOptionPane.showMessageDialog(this, ("The batch cannot be executed in its current state:\n - " + errorReport.concatStrings("\n - ")), "Cannot Execute Batch", JOptionPane.ERROR_MESSAGE);
				}
			}
			
			//	start after pause
			else if (this.batch.paused)
				synchronized (this.batch.pause) {
					this.batch.paused = false;
					this.batch.pause.notify();
				}
			
			//	make sure buttons reflect current state
			this.updateStatus();
		}
		
		private void pause() {
			
			//	check state
			if ((this.batch == null) || !this.batch.isActive())
				return;
			
			//	prepare handshake
			final Object handshake = new Object();
			
			//	use new thread to pause batch in order to keep EDT available for processor dialogs
			Thread pauser = new Thread() {
				private Batch pBatch = batch;
				public void run() {
					synchronized (handshake) {
						handshake.notify();
					}
					synchronized (this.pBatch.pause) {
						this.pBatch.paused = true;
						try {
							this.pBatch.pause.wait();
						} catch (InterruptedException ie) {}
					}
					
					//	make sure buttons reflect current state
					updateStatus();
				}
			};
			
			//	make sure buttons reflect current state
			this.statusLabel.setText("Pausing After Document");
			this.interactive.setEnabled(false);
			this.inspectDocument.setEnabled(false);
			this.startButton.setEnabled(false);
			this.pauseButton.setEnabled(false);
			this.stopButton.setEnabled(false);
			this.resetButton.setEnabled(false);
			this.backgroundButton.setEnabled(false);
			this.closeButton.setEnabled(false);
			this.dataPanel.setEnabled(false);
			
			//	trigger pausing
			synchronized (handshake) {
				pauser.start();
				try {
					handshake.wait();
				} catch (InterruptedException ie) {}
			}
		}
		
		private void stop() {
			
			//	check state
			if (this.batch == null)
				return;
			
			//	prepare handshake
			final Object handshake = new Object();
			
			//	use new thread in order to keep EDT available for processor dialogs
			Thread stopper = new Thread() {
				private Batch sBatch = batch;
				public void run() {
					this.sBatch.stop = true;
					synchronized (handshake) {
						handshake.notify();
					}
					if (this.sBatch.paused)
						synchronized (this.sBatch.pause) {
							this.sBatch.paused = false;
							this.sBatch.pause.notify();
						}
					try {
						this.sBatch.join();
					} catch (InterruptedException ie) {}
					
					//	make sure buttons reflect current state
					updateStatus();
					
					//	reset progress bar
					updateProgress();
				}
			};
			
			//	make sure buttons reflect current state
			this.statusLabel.setText("Stopping After Document");
			this.interactive.setEnabled(false);
			this.inspectDocument.setEnabled(false);
			this.startButton.setEnabled(false);
			this.pauseButton.setEnabled(false);
			this.stopButton.setEnabled(false);
			this.resetButton.setEnabled(false);
			this.backgroundButton.setEnabled(false);
			this.closeButton.setEnabled(false);
			this.dataPanel.setEnabled(false);
			
			//	trigger stopping
			synchronized (handshake) {
				stopper.start();
				try {
					handshake.wait();
				} catch (InterruptedException ie) {}
			}
		}
		
		private void reset() {
			
			//	check state
			if (this.batch != null)
				return;
			
			//	delete log file
			if (dataProvider.isDataAvailable(this.doneLogFileName))
				dataProvider.deleteData(this.doneLogFileName);
			
			//	clear local logs
			this.skippedDocIDs.clear();
			this.doneDocIDs.clear();
			this.todoDocIDs.addAll(this.allDocData.keySet());
			
			//	show new status
			this.updateProgress();
		}
		
		private void updateStatus() {
			boolean runnable = this.isBatchRunnable();
			this.statusLabel.setText(runnable ? ((this.batch == null) ? "Stopped" : (this.batch.paused ? "Paused" : "Running")) : "Not Runnable");
			this.interactive.setEnabled((this.batch == null) || this.batch.paused);
			this.inspectDocument.setEnabled((this.batch == null) || this.batch.paused);
			this.startButton.setEnabled(runnable && ((this.batch == null) || this.batch.paused));
			this.pauseButton.setEnabled((this.batch != null) && !this.batch.paused);
			this.stopButton.setEnabled(this.batch != null);
			this.resetButton.setEnabled(runnable && (this.batch == null));
			this.backgroundButton.setEnabled(runnable && (this.batch != null));
			this.closeButton.setEnabled(this.batch == null);
			this.dataPanel.setEnabled(this.batch == null);
		}
		
		void batchRunTerminated() {
			this.batch = null;
			try {
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dataProvider.getOutputStream(this.doneLogFileName)));
				for (Iterator dfit = this.doneDocIDs.iterator(); dfit.hasNext();) {
					String doneFileName = ((String) dfit.next());
					bw.write(doneFileName);
					bw.newLine();
				}
				bw.flush();
				bw.close();
			}
			catch (IOException ioe) {}
			this.updateStatus();
		}
		
		void log(String entry) {
			System.out.println(entry);
			this.logLines.addElement(entry);
			while (this.logLines.size() > 100)
				this.logLines.remove(0);
			this.logDisplay.setText(this.logLines.concatStrings("\n"));
			this.logDisplay.validate();
		}
	}
	
	private static class Batch extends Thread {
		
		private GoldenGATE host;
		private BatchRunDialog parent;
		
		boolean stop = false;
		boolean paused = false;
		Object pause = new Object();
		
		private Batch(GoldenGATE host, BatchRunDialog parent) {
			this.host = host;
			this.parent = parent;
		}
		
		public void run() {
			
			//	remember session we started with
			String batchSessionId = this.getCurrentSession();
			
			//	notify any objects that might be waiting for the start to complete
			synchronized (this) {
				this.notify();
			}
			
			//	run until told to stop
			StringTupel docData;
			while ((batchSessionId != null) && !this.stop && ((docData = this.parent.getNextDocument()) != null)) {
				
				//	gather data of next document
				String docId = docData.getValue(GoldenGateDioConstants.DOCUMENT_ID_ATTRIBUTE);
				String docName = docData.getValue(GoldenGateDioConstants.DOCUMENT_NAME_ATTRIBUTE);
				this.log("Processing " + docName);
				
				//	ckeck out document
				MutableAnnotation doc = null;
				if (this.checkCurrentSession(batchSessionId)) try {
					doc = this.parent.dioClient.checkoutDocument(docId);
					this.log(" - " + docName + " checked out, size is " + doc.size());
				}
				catch (Exception e) {
					this.log(e, docName);
				}
				
				//	checkout failed
				if (doc == null) {
					
					//	document unavailable for some reason other than logout, e.g. checked out by someone else
					if (this.checkCurrentSession(batchSessionId))
						this.parent.documentSkipped(docId);
					
					//	we've been logged out ==> stop
					else this.parent.stop();
					
					//	skip over rest of loop
					continue;
				}
				
				//	prepare tracking changes
				String beforeCheckSum = getChecksum(doc);
				
				//	process document
				boolean saveDoc = false;
				try {
					Properties parameters = new Properties();
					if (this.parent.interactive.isSelected())
						parameters.setProperty(Resource.INTERACTIVE_PARAMETER, Resource.INTERACTIVE_PARAMETER);
					this.parent.processor.process(doc, parameters);
					this.log(" - " + docName + " processed, size is " + doc.size());
					
					//	display document if selected
					if (this.parent.inspectDocument.isSelected()) {
						DocumentEditDialog ded = new DocumentEditDialog(this.host, docName, doc);
						ded.setLocationRelativeTo(ded.getOwner());
						ded.setVisible(true);
						saveDoc = ded.save();
						if (ded.pause.isSelected())
							this.parent.pause();
						else if (ded.stop.isSelected())
							this.parent.stop();
					}
					else saveDoc = true;
				}
				catch (Throwable t) {
					this.log(t, docName);
				}
				
				//	save updates (if any)
				if (saveDoc) {
					
					//	document unchanged
					if (beforeCheckSum.equals(getChecksum(doc)))
						this.log(" - " + docName + " unchanged");
					
					//	actual change
					else try {
						this.log(" - " + docName + " changed, uploading");
						String[] log = this.parent.dioClient.updateDocument(doc, docName);
						this.log(" - " + docName + " saved to server:");
						for (int l = 0; l < log.length; l++)
							this.log("   - " + log[l]);
					}
					catch (Exception e) {
						this.log(e, docName);
						
						//	update failed for some reason other than logout, e.g. someone stole the lock
						if (this.checkCurrentSession(batchSessionId))
							this.parent.documentSkipped(docId);
						
						//	we've been logged out ==> stop
						else {
							JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("Document '" + docName + "' cannot be saved and released due to a logout.\nPlease remember to release the document as soon as possible.\nLogging in again and running the batch again will do so as well."), "Cannot Save Document", JOptionPane.ERROR_MESSAGE);
							this.parent.stop();
						}
						
						//	skip over release attempt (won't work, and if someone stole the lock, we'd be stealing it back due to admin priviledges, which would be no good)
						continue;
					}
				}
				
				//	release document
				try {
					this.parent.dioClient.releaseDocument(docId);
				}
				catch (Exception e) {
					this.log(e, docName);
					
					//	release failed for some reason other than logout, e.g. someone stole the lock
					if (this.checkCurrentSession(batchSessionId))
						this.parent.documentSkipped(docId);
					
					//	we've been logged out ==> stop
					else {
						JOptionPane.showMessageDialog(DialogPanel.getTopWindow(), ("Document '" + docName + "' cannot be released due to a logout.\nPlease remember to release the document as soon as possible."), "Cannot Release Document", JOptionPane.ERROR_MESSAGE);
						this.parent.stop();
					}
					
					//	skip over finish report
					continue;
				}
				
				//	make progress visible
				this.parent.documentFinished(docId);
				
				//	clean up and give the others a little time ...
				doc = null;
				System.gc();
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {}
				Thread.yield();
				
				//	pause if sheduled to pause
				if (this.paused)
					synchronized (this.pause) {
						this.pause.notify();
						try {
							this.pause.wait();
						} catch (InterruptedException ie) {}
					}
			}
			
			//	notify parent
			this.parent.batchRunTerminated();
		}
		
		private String getCurrentSession() {
			return ((this.parent.dataPanel.authClient == null) ? null : this.parent.dataPanel.authClient.getSessionID());
		}
		
		private boolean checkCurrentSession(String refSessionId) {
			String currentSessionId = this.getCurrentSession();
			boolean ok = ((refSessionId == null) ? (currentSessionId == null) : refSessionId.equals(currentSessionId));
			if (!ok) {
				this.parent.dataPanel.dioClient = null;
				this.parent.dataPanel.authClient = null;
				this.parent.dataPanel.loginButton.setText("Login");
				this.parent.dataPanel.stateChanged();
			}
			return ok;
		}
		
		private static MessageDigest checksumDigester = null;
		private static final String getChecksum(QueriableAnnotation document) {
			if (checksumDigester == null) {
				try {
					checksumDigester = MessageDigest.getInstance("MD5");
				}
				catch (NoSuchAlgorithmException nsae) {
					System.out.println(nsae.getClass().getName() + " (" + nsae.getMessage() + ") while creating checksum digester.");
					nsae.printStackTrace(System.out); // should not happe, but Java don't know ...
					return Gamta.getAnnotationID(); // use random value so a document is regarded as changed
				}
			}
			long start = System.currentTimeMillis();
			checksumDigester.reset();
			AnnotationInputStream ais = new AnnotationInputStream(document, "UTF-8", null, new HashSet());
			try {
				byte[] buffer = new byte[1024];
				int read;
				while ((read = ais.read(buffer)) != -1)
					checksumDigester.update(buffer, 0, read);
			}
			catch (IOException ioe) {
				System.out.println(ioe.getClass().getName() + " (" + ioe.getMessage() + ") while computing document checksum.");
				ioe.printStackTrace(System.out); // should not happen, but Java don't know ...
				return Gamta.getAnnotationID(); // use random value so a document is regarded as new
			}
			byte[] checksumBytes = checksumDigester.digest();
			String checksum = new String(RandomByteSource.getHexCode(checksumBytes));
			System.out.println("Checksum computed in " + (System.currentTimeMillis() - start) + " ms: " + checksum);
			return checksum;
		}
		
		boolean isActive() {
			return (!this.stop && !this.paused && this.isAlive());
		}
		
		private void log(String entry) {
			this.parent.log(entry);
		}
		
		private void log(Throwable tr, String docFileName) {
			this.log(tr.getClass().getName() + " (" + tr.getMessage() + ") while processing " + docFileName);
			Throwable cTr = tr;
			do {
				this.log(((cTr == tr) ? "" : "Caused by: ") + cTr.toString());
	            StackTraceElement[] trace = cTr.getStackTrace();
				for (int t = 0; t < trace.length; t++)
					this.log("\tat " + trace[t]);
			}
			while ((cTr = cTr.getCause()) != null);
		}
		
		private class DocumentEditDialog extends DocumentEditorDialog {
			
			boolean save = true;
			
			JCheckBox pause = new JCheckBox("Pause After Document", false);
			JCheckBox stop = new JCheckBox("Stop After Document", false);
			
			DocumentEditDialog(GoldenGATE host, String title, MutableAnnotation doc) {
				super(host, null, title, doc);
				
				//	initialize main buttons
				JButton commitButton = new JButton("Store Document");
				commitButton.setBorder(BorderFactory.createRaisedBevelBorder());
				commitButton.setPreferredSize(new Dimension(150, 21));
				commitButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						documentEditor.writeChanges();
						dispose();
					}
				});
				JButton abortButton = new JButton("Discart Document");
				abortButton.setBorder(BorderFactory.createRaisedBevelBorder());
				abortButton.setPreferredSize(new Dimension(150, 21));
				abortButton.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent ae) {
						if (JOptionPane.showConfirmDialog(DocumentEditDialog.this, "Really discart Document?", "Confirm Discarting Document", JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
							save = false;
							dispose();
						}
					}
				});
				
				this.mainButtonPanel.add(abortButton);
				this.mainButtonPanel.add(commitButton);
				this.mainButtonPanel.add(this.pause);
				this.mainButtonPanel.add(this.stop);
				
				this.setResizable(true);
				this.setSize(new Dimension(800, 600));
				this.setLocationRelativeTo(DialogPanel.getTopWindow());
			}
			
			boolean save() {
				return this.save;
			}
		}
	}
	
	private static final String doNotFilterFieldName = "<Do Not Filter>";
	private static final String[] filterFields = {
		doNotFilterFieldName,
		GoldenGateDioConstants.EXTERNAL_IDENTIFIER_ATTRIBUTE,
		GoldenGateDioConstants.DOCUMENT_NAME_ATTRIBUTE,
		GoldenGateDioConstants.DOCUMENT_AUTHOR_ATTRIBUTE,
		GoldenGateDioConstants.DOCUMENT_DATE_ATTRIBUTE,
		GoldenGateDioConstants.DOCUMENT_TITLE_ATTRIBUTE,
		GoldenGateDioConstants.CHECKIN_USER_ATTRIBUTE,
		GoldenGateDioConstants.UPDATE_USER_ATTRIBUTE,
	};
	
	private class BatchDataPanel extends JPanel {
		
		private JButton loginButton = new JButton("Login");
		private AuthenticatedClient authClient = null;
		GoldenGateDioClient dioClient = null;
		
		private JComboBox filterFieldSelector = new JComboBox(filterFields);
		private String filterField = doNotFilterFieldName;
		private JTextField filterPatternField = new JTextField();
		private String filterPattern = "";
		
		ProcessorSelectorPanel processorSelector;
		
		BatchDataPanel() {
			super(new GridLayout(3, 1, 0, 3), true);
			
			this.loginButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.loginButton.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent ae) {
					logInOrOut();
				}
			});
			this.add(this.loginButton);
			
			this.filterFieldSelector.setSelectedItem(doNotFilterFieldName);
			this.filterFieldSelector.addItemListener(new ItemListener() {
				public void itemStateChanged(ItemEvent ie) {
					String ff = ((String) filterFieldSelector.getSelectedItem());
					if (!filterField.equals(ff)) {
						filterField = ff;
						filterPatternField.setEnabled(!doNotFilterFieldName.equals(filterField));
						stateChanged();
					}
				}
			});
			this.filterPatternField.setBorder(BorderFactory.createLoweredBevelBorder());
			this.filterPatternField.setEnabled(false);
			this.filterPatternField.addActionListener(new ActionListener() {
				public void actionPerformed(ActionEvent e) {
					String fp = filterPatternField.getText().trim();
					if (!filterPattern.equals(fp)) {
						filterPattern = fp;
						stateChanged();
					}
				}
			});
			this.filterPatternField.addFocusListener(new FocusAdapter() {
				public void focusLost(FocusEvent fe) {
					String fp = filterPatternField.getText().trim();
					if (!filterPattern.equals(fp)) {
						filterPattern = fp;
						stateChanged();
					}
				}
			});
			
			JPanel filterFieldPanel = new JPanel(new BorderLayout());
			filterFieldPanel.add(new JLabel("Only Process Documents Whose ", JLabel.RIGHT), BorderLayout.WEST);
			filterFieldPanel.add(this.filterFieldSelector, BorderLayout.CENTER);
			filterFieldPanel.add(new JLabel(" Attribute Matches ", JLabel.CENTER), BorderLayout.EAST);
			JPanel filterPanel = new JPanel(new BorderLayout());
			filterPanel.add(filterFieldPanel, BorderLayout.WEST);
			filterPanel.add(this.filterPatternField, BorderLayout.CENTER);
			this.add(filterPanel);
			
			this.processorSelector = new ProcessorSelectorPanel();
			this.add(this.processorSelector);
		}
		
		public void setEnabled(boolean enabled) {
			super.setEnabled(enabled);
			this.processorSelector.setEnabled(enabled);
			this.loginButton.setEnabled(enabled);
			this.filterFieldSelector.setEnabled(enabled);
			this.filterPatternField.setEnabled(enabled && !doNotFilterFieldName.equals(this.filterFieldSelector.getSelectedItem()));
		}
		
		boolean isComplete() {
			return (this.processorSelector.isComplete() && (this.dioClient != null));
		}
		
		private void logInOrOut() {
			if (this.authClient == null) {
				this.ensureLoggedIn();
				this.stateChanged();
			}
			else {
				this.dioClient = null;
				this.authClient = null;
				AuthenticationManager.logout();
				this.loginButton.setText("Login");
				this.stateChanged();
			}
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
			
			//	got no valid connection at the moment
			if (this.authClient == null)
				this.authClient = authManager.getAuthenticatedClient();
			
			//	authentication failed
			if (this.authClient == null)
				return false;
			
			//	not an admin
			else if (!this.authClient.isAdmin()) {
				JOptionPane.showMessageDialog(this, ("The specified login data does not authorize you to run a server batch on " + authManager.getHost() + "\nRunning server batches requires administrative priviledges."), "Admin Priviledges Required", JOptionPane.ERROR_MESSAGE);
				this.authClient = null;
				AuthenticationManager.logout();
				return false;
			}
			//	got valid connection
			else {
				this.dioClient = new GoldenGateDioClient(this.authClient);
				this.loginButton.setText("Logged in to " + authManager.getHost() + " (click to log out)");
				return true;
			}
		}
		
		boolean matchesFilter(StringTupel st) {
			if (doNotFilterFieldName.equals(this.filterField))
				return true;
			if ("".equals(this.filterPattern))
				return true;
			try {
				return st.getValue(this.filterField, "").matches(this.filterPattern);
			}
			catch (PatternSyntaxException pse) {
				return true;
			}
		}
		
		private ArrayList cls = null;
		void addChangeListener(ChangeListener cl) {
			if (cl == null)
				return;
			if (this.cls == null) {
				this.cls = new ArrayList();
				this.processorSelector.addChangeListener(new ChangeListener() {
					public void stateChanged(ChangeEvent ce) {
						BatchDataPanel.this.stateChanged();
					}
				});
			}
			this.cls.add(cl);
		}
		private void stateChanged() {
			if (this.cls == null)
				return;
			ChangeEvent ce = new ChangeEvent(this);
			for (Iterator lit = this.cls.iterator(); lit.hasNext();)
				((ChangeListener) lit.next()).stateChanged(ce);
		}
	}
	
	private class ProcessorSelectorPanel extends JPanel {
		
		private String processorName = null;
		private String processorProviderClassName = null;
		
		private JButton useDpButton;
		private JButton createDpButton;
		private JLabel processorLabel = new JLabel("<No Document Processor Selected>", JLabel.LEFT);
		
		ProcessorSelectorPanel() {
			super(new BorderLayout(), true);
			
			this.processorLabel.addMouseListener(new MouseAdapter() {
				public void mouseClicked(MouseEvent me) {
					if (ProcessorSelectorPanel.this.isEnabled() && (me.getClickCount() > 1)) {
						DocumentProcessorManager dpm = parent.getDocumentProcessorProvider(processorProviderClassName);
						if (dpm != null)
							dpm.editDocumentProcessor(processorName);
					}
				}
			});
			
			final JPopupMenu useDpMenu = new JPopupMenu();
			final JPopupMenu createDpMenu = new JPopupMenu();
			
			DocumentProcessorManager[] dpms = parent.getDocumentProcessorProviders();
			for (int p = 0; p < dpms.length; p++) {
				final String className = dpms[p].getClass().getName();
				
				JMenuItem useDpMi = new JMenuItem("Use " + dpms[p].getResourceTypeLabel());
				useDpMi.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						selectProcessor(className);
					}
				});
				useDpMenu.add(useDpMi);
				
				JMenuItem createDpMi = new JMenuItem("Create " + dpms[p].getResourceTypeLabel());
				createDpMi.addActionListener(new ActionListener() {
					public void actionPerformed(ActionEvent e) {
						createProcessor(className);
					}
				});
				createDpMenu.add(createDpMi);
			}
			
			this.useDpButton = new JButton("Use ...");
			this.useDpButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.useDpButton.setSize(new Dimension(120, 21));
			this.useDpButton.addMouseListener(new MouseAdapter() {
				public void mouseClicked(MouseEvent me) {
					if (ProcessorSelectorPanel.this.isEnabled())
						useDpMenu.show(useDpButton, me.getX(), me.getY());
				}
			});
			
			this.createDpButton = new JButton("Create ...");
			this.createDpButton.setBorder(BorderFactory.createRaisedBevelBorder());
			this.createDpButton.setSize(new Dimension(120, 21));
			this.createDpButton.addMouseListener(new MouseAdapter() {
				public void mouseClicked(MouseEvent me) {
					if (ProcessorSelectorPanel.this.isEnabled())
						createDpMenu.show(createDpButton, me.getX(), me.getY());
				}
			});
			
			JPanel setDpPanel = new JPanel(new GridLayout(1, 2, 3, 0), true);
			setDpPanel.add(this.createDpButton);
			setDpPanel.add(this.useDpButton);
			
			this.add(setDpPanel, BorderLayout.WEST);
			this.add(this.processorLabel, BorderLayout.CENTER);
			this.setBorder(BorderFactory.createEtchedBorder());
		}
		
		void selectProcessor(String providerClassName) {
			DocumentProcessorManager dpm = parent.getDocumentProcessorProvider(providerClassName);
			if (dpm != null) {
				ResourceDialog rd = ResourceDialog.getResourceDialog(dpm, ("Select " + dpm.getResourceTypeLabel()), "Select");
				rd.setLocationRelativeTo(DialogPanel.getTopWindow());
				rd.setVisible(true);
				DocumentProcessor dp = dpm.getDocumentProcessor(rd.getSelectedResourceName());
				if (dp != null) {
					this.processorName = dp.getName();
					this.processorProviderClassName = dp.getProviderClassName();
					this.processorLabel.setText(" " + dp.getTypeLabel() + ": " + dp.getName() + " (double click to edit)");
					this.stateChanged();
				}
			}
		}
		
		void createProcessor(String providerClassName) {
			DocumentProcessorManager dpm = parent.getDocumentProcessorProvider(providerClassName);
			if (dpm != null) {
				String dpName = dpm.createDocumentProcessor();
				DocumentProcessor dp = dpm.getDocumentProcessor(dpName);
				if (dp != null) {
					this.processorName = dp.getName();
					this.processorProviderClassName = dp.getProviderClassName();
					this.processorLabel.setText(" " + dp.getTypeLabel() + ": " + dp.getName() + " (double click to edit)");
					this.stateChanged();
				}
			}
		}
		
		public void setEnabled(boolean enabled) {
			super.setEnabled(enabled);
			this.createDpButton.setEnabled(enabled);
			this.useDpButton.setEnabled(enabled);
		}
		
		boolean isComplete() {
			return ((this.processorName != null) && (this.processorProviderClassName != null));
		}
		
		String getProcessorName() {
			return (this.isComplete() ? (this.processorName + "@" + this.processorProviderClassName) : null);
		}
		
		private ArrayList cls = null;
		void addChangeListener(ChangeListener cl) {
			if (cl == null)
				return;
			if (this.cls == null)
				this.cls = new ArrayList();
			this.cls.add(cl);
		}
		private void stateChanged() {
			if (this.cls == null)
				return;
			ChangeEvent ce = new ChangeEvent(this);
			for (Iterator lit = this.cls.iterator(); lit.hasNext();)
				((ChangeListener) lit.next()).stateChanged(ce);
		}
	}
}
