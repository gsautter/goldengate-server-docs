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
 *     * Neither the name of the Universitaet Karlsruhe (TH) / KIT nor the
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
package de.uka.ipd.idaho.goldenGateServer.dpr.slave;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.util.Analyzer;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGate.GoldenGATE;
import de.uka.ipd.idaho.goldenGate.GoldenGateConfiguration;
import de.uka.ipd.idaho.goldenGate.GoldenGateConstants;
import de.uka.ipd.idaho.goldenGate.configuration.ConfigurationUtils;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentProcessor;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentProcessorManager;
import de.uka.ipd.idaho.goldenGate.plugins.MonitorableDocumentProcessor;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.MasterProcessInterface;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveConstants;
import de.uka.ipd.idaho.goldenGateServer.util.masterSlave.SlaveRuntimeUtils;

/**
 * Stripped-down copy of GoldenGATE Imagine's batch runner utility, loading
 * documents from a folder rather than directly from PDFs, with options
 * restricted to specific application needs.
 * 
 * @author sautter
 */
public class GoldenGateDprSlave implements GoldenGateConstants, SlaveConstants {
	private static final String CONFIG_HOST_PARAMETER = "CONFHOST";
	private static final String CONFIG_NAME_PARAMETER = "CONFNAME";
	private static final String DOCUMENT_PROCESSOR_NAME_PARAMETER = "DPNAME";
	private static final String LIST_DOCUMENT_PROCESSORS_NAME = "LISTDPS";
	
	/**	the main method to run GoldenGATE Imagine as a batch application
	 */
	public static void main(String[] args) throws Exception {
//		
//		//	adjust basic parameters
//		String basePathStr = ".";
//		String logPath = null;
//		String docPath = null;
//		String ggConfigHost = null;
//		String ggConfigName = null;
//		String dpNameString = null;
//		boolean useSingleCpuCore = false;
//		
//		//	parse remaining args
//		for (int a = 0; a < args.length; a++) {
//			if (args[a] == null)
//				continue;
//			if (args[a].startsWith(DATA_PATH_PARAMETER + "="))
//				docPath = args[a].substring((DATA_PATH_PARAMETER + "=").length());
//			else if (args[a].startsWith(LOG_PATH_PARAMETER + "="))
//				logPath = args[a].substring((LOG_PATH_PARAMETER + "=").length());
//			else if (args[a].startsWith(CONFIG_HOST_PARAMETER + "="))
//				ggConfigHost = args[a].substring((CONFIG_HOST_PARAMETER + "=").length());
//			else if (args[a].startsWith(CONFIG_NAME_PARAMETER + "="))
//				ggConfigName = args[a].substring((CONFIG_NAME_PARAMETER + "=").length());
//			else if (args[a].startsWith(DOCUMENT_PROCESSOR_NAME_PARAMETER + "="))
//				dpNameString = args[a].substring((DOCUMENT_PROCESSOR_NAME_PARAMETER + "=").length());
//			else if (USE_SINGLE_CORE_PARAMETER.equals(args[a]))
//				useSingleCpuCore = true;
//		}
//		
//		//	remember program base path
//		File basePath = new File(basePathStr);
		
		//	adjust basic parameters
		Properties argsMap = SlaveRuntimeUtils.parseArguments(args);
		
		//	set up communication with master (before logging tampers with output streams)
		MasterProcessInterface mpi = new MasterProcessInterface();
//		
//		//	set up logging (if we have a folder)
//		if (logPath != null) {
//			File logFolder = new File(logPath);
//			
//			//	clean up log files older than 24 hours
//			SlaveRuntimeUtils.cleanUpLogFiles(logFolder, "DprSlaveBatch");
//			
//			//	set up logging (keep error stream going to master process, though)
//			SlaveRuntimeUtils.setUpLogFiles(logFolder, "DprSlaveBatch", false, true);
//		}
//		
//		//	silence standard output stream otherwise (keep error stream going to master process, though)
//		else System.setOut(new PrintStream(new OutputStream() {
//			public void write(int b) throws IOException {}
//		}));
		
		//	get list of document processors to apply
		String dpNameString = argsMap.getProperty(DOCUMENT_PROCESSOR_NAME_PARAMETER);
		if (dpNameString == null) {
			mpi.sendError("No Dcoument Processor configured to run, check parameter " + DOCUMENT_PROCESSOR_NAME_PARAMETER);
//			System.exit(0);
			return;
		}
		String[] dpNames = dpNameString.split("\\+");
		
		//	set up logging (if we have a folder)
		SlaveRuntimeUtils.setUpLogFiles(argsMap, "DprSlaveBatch");
		
		//	start receiving control commands from master process
		mpi.start();
		
		//	remember program base path
		File basePath = new File(".");
		
		//	get GoldenGATE Editor configuration
		String ggConfigHost = argsMap.getProperty(CONFIG_HOST_PARAMETER);
		String ggConfigName = argsMap.getProperty(CONFIG_NAME_PARAMETER);
		GoldenGateConfiguration ggConfig = ConfigurationUtils.getConfiguration(ggConfigName, null, ggConfigHost, basePath);
		
		//	check if configuration found
		if (ggConfig == null) {
			mpi.sendError("Configuration '" + ggConfigName + "' not found, check parameter " + CONFIG_NAME_PARAMETER);
//			System.exit(0);
			return;
		}
		
		//	instantiate GoldenGATE
		GoldenGATE goldenGate = GoldenGATE.openGoldenGATE(ggConfig, false, false);
		mpi.sendResult("GoldenGATE core created, configuration is " + ggConfigName);
		
		//	list document processors
		if (LIST_DOCUMENT_PROCESSORS_NAME.equals(dpNameString)) {
			DocumentProcessorManager[] dpms = goldenGate.getDocumentProcessorProviders();
			for (int m = 0; m < dpms.length; m++) {
				if (dpms[m].getMainMenuTitle() == null)
					continue;
				String[] mDpNames = dpms[m].getResourceNames();
				if(mDpNames.length == 0)
					continue;
				mpi.sendOutput("DPM:" + dpms[m].getMainMenuTitle() + " (" + mDpNames.length + "):");
				for (int p = 0; p < mDpNames.length; p++)
					mpi.sendOutput("DP:" + mDpNames[p]);
			}
//			System.exit(0);
			return;
		}
		
		//	get individual image markup tools
		DocumentProcessor[] dps = new DocumentProcessor[dpNames.length];
		for (int t = 0; t < dpNames.length; t++) {
			dps[t] = goldenGate.getDocumentProcessorForName(dpNames[t]);
			if (dps[t] == null) {
				mpi.sendError("Document Processor '" + dpNames[t] + "' not found, check parameter " + DOCUMENT_PROCESSOR_NAME_PARAMETER);
//				System.exit(0);
				return;
			}
			else mpi.sendResult("Document Processor '" + dpNames[t] + "' loaded");
		}
//		
//		//	switch parallel jobs to linear execution if requested to
//		if (useSingleCpuCore)
//			ParallelJobRunner.setLinear(true);
		
		//	impose parallel processing limitations
		SlaveRuntimeUtils.setUpMaxCores(argsMap);
		
		//	process document
		String docPath = argsMap.getProperty(DATA_PATH_PARAMETER);
		try {
			
			//	load document
			BufferedReader docIn = new BufferedReader(new InputStreamReader(new FileInputStream(new File(docPath)), "UTF-8"));
			DocumentRoot doc = GenericGamtaXML.readDocument(docIn);
			docIn.close();
			
			//	process document
			Properties params = new Properties();
			params.setProperty(Analyzer.ONLINE_PARAMETER, Analyzer.ONLINE_PARAMETER);
			ProgressMonitor pm = mpi.createProgressMonitor();
			for (int t = 0; t < dps.length; t++) {
				mpi.sendOutput("PR:" + dps[t].getName());
				if (dps[t] instanceof MonitorableDocumentProcessor)
					((MonitorableDocumentProcessor) dps[t]).process(doc, params, pm);
				else dps[t].process(doc, params);
			}
			
			//	store updates
			BufferedWriter docOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(docPath)), "UTF-8"));
			GenericGamtaXML.storeDocument(doc, docOut);
			docOut.flush();
			docOut.close();
			
			//	TODO maybe compute document hash on saving and return as result (saves loading in master if document unchanged)
		}
		
		//	catch and log whatever might go wrong
		catch (Throwable t) {
			mpi.sendError("Error processing document: " + t.getMessage());
			mpi.sendError(t);
		}
//		
//		//	shut down whatever threads are left
//		System.exit(0);
	}
}