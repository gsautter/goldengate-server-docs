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
package de.uka.ipd.idaho.goldenGateServer.dpr.slave;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Properties;

import de.uka.ipd.idaho.gamta.DocumentRoot;
import de.uka.ipd.idaho.gamta.util.Analyzer;
import de.uka.ipd.idaho.gamta.util.GenericGamtaXML;
import de.uka.ipd.idaho.gamta.util.ParallelJobRunner;
import de.uka.ipd.idaho.gamta.util.ProgressMonitor;
import de.uka.ipd.idaho.goldenGate.GoldenGATE;
import de.uka.ipd.idaho.goldenGate.GoldenGateConfiguration;
import de.uka.ipd.idaho.goldenGate.GoldenGateConfiguration.ConfigurationDescriptor;
import de.uka.ipd.idaho.goldenGate.GoldenGateConstants;
import de.uka.ipd.idaho.goldenGate.configuration.ConfigurationUtils;
import de.uka.ipd.idaho.goldenGate.configuration.FileConfiguration;
import de.uka.ipd.idaho.goldenGate.configuration.UrlConfiguration;
import de.uka.ipd.idaho.goldenGate.plugins.DocumentProcessor;
import de.uka.ipd.idaho.goldenGate.plugins.MonitorableDocumentProcessor;

/**
 * Stripped-down copy of GoldenGATE Imagine's batch runner utility, loading
 * documents from a folder rather than directly from PDFs, with options
 * restricted to specific application needs.
 * 
 * @author sautter
 */
public class GoldenGateDprSlave implements GoldenGateConstants {
	private static final String DATA_PATH_PARAMETER = "DATA";
	private static final String CONFIG_HOST_PARAMETER = "CONFHOST";
	private static final String CONFIG_NAME_PARAMETER = "CONFNAME";
	private static final String DOCUMENT_PROCESSOR_NAME_PARAMETER = "DPNAME";
	private static final String USE_SINGLE_CORE_PARAMETER = "SINGLECORE";
	
	/**	the main method to run GoldenGATE Imagine as a batch application
	 */
	public static void main(String[] args) throws Exception {
		
		//	adjust basic parameters
		String basePathStr = ".";
		String docPath = null;
		String ggConfigHost = null;
		String ggConfigName = null;
		String dpNameString = null;
		boolean useSingleCpuCore = false;
		
		//	parse remaining args
		for (int a = 0; a < args.length; a++) {
			if (args[a] == null)
				continue;
			if (args[a].startsWith(DATA_PATH_PARAMETER + "="))
				docPath = args[a].substring((DATA_PATH_PARAMETER + "=").length());
			else if (args[a].startsWith(CONFIG_HOST_PARAMETER + "="))
				ggConfigHost = args[a].substring((CONFIG_HOST_PARAMETER + "=").length());
			else if (args[a].startsWith(CONFIG_NAME_PARAMETER + "="))
				ggConfigName = args[a].substring((CONFIG_NAME_PARAMETER + "=").length());
			else if (args[a].startsWith(DOCUMENT_PROCESSOR_NAME_PARAMETER + "="))
				dpNameString = args[a].substring((DOCUMENT_PROCESSOR_NAME_PARAMETER + "=").length());
			else if (USE_SINGLE_CORE_PARAMETER.equals(args[a]))
				useSingleCpuCore = true;
		}
		
		//	remember program base path
		File basePath = new File(basePathStr);
		
		//	preserve System.out and System.err
		final PrintStream sysOut = System.out;
		final PrintStream sysErr = System.err;
		
		//	set up logging
		ProgressMonitor pm = new ProgressMonitor() {
			public void setStep(String step) {
				sysOut.println("S:" + step);
			}
			public void setInfo(String info) {
				sysOut.println("I:" + info);
			}
			public void setBaseProgress(int baseProgress) {
				sysOut.println("BP:" + baseProgress);
			}
			public void setMaxProgress(int maxProgress) {
				sysOut.println("MP:" + maxProgress);
			}
			public void setProgress(int progress) {
				sysOut.println("P:" + progress);
			}
		};
		
		//	silence System.out
		System.setOut(new PrintStream(new OutputStream() {
			public void write(int b) throws IOException {}
		}));
		
		//	get list of image markup tools to run
		if (dpNameString == null) {
			sysOut.println("No Dcoument Processor configured to run, check parameter " + DOCUMENT_PROCESSOR_NAME_PARAMETER);
			System.exit(0);
		}
		String[] dpNames = dpNameString.split("\\+");
		
		//	select new configuration
		ConfigurationDescriptor configuration = getConfiguration(basePath, ggConfigHost, ggConfigName);
		
		//	check if cancelled
		if (configuration == null) {
			sysOut.println("Configuration '" + ggConfigName + "' not found, check parameter " + CONFIG_NAME_PARAMETER);
			System.exit(0);
			return;
		}
		
		//	open GoldenGATE Imagine window
		GoldenGateConfiguration ggConfig = null;
		
		//	local configuration selected
		if (configuration.host == null)
			ggConfig = new FileConfiguration(configuration.name, new File(new File(basePath, CONFIG_FOLDER_NAME), configuration.name), false, true, null);
		
		//	remote configuration selected
		else ggConfig = new UrlConfiguration((configuration.host + (configuration.host.endsWith("/") ? "" : "/") + configuration.name), configuration.name);
		
		//	instantiate GoldenGATE
		GoldenGATE goldenGate = GoldenGATE.openGoldenGATE(ggConfig, false, false);
		sysOut.println("GoldenGATE core created, configuration is " + ggConfigName);
		
		//	get individual image markup tools
		DocumentProcessor[] dps = new DocumentProcessor[dpNames.length];
		for (int t = 0; t < dpNames.length; t++) {
			dps[t] = goldenGate.getDocumentProcessorForName(dpNames[t]);
			if (dps[t] == null) {
				sysOut.println("Document Processor '" + dpNames[t] + "' not found, check parameter " + DOCUMENT_PROCESSOR_NAME_PARAMETER);
				System.exit(0);
			}
			else sysOut.println("Document Processor '" + dpNames[t] + "' loaded");
		}
		
		//	switch parallel jobs to linear execution if requested to
		if (useSingleCpuCore)
			ParallelJobRunner.setLinear(true);
		
		//	process document
		try {
			
			//	load document
			BufferedReader docIn = new BufferedReader(new InputStreamReader(new FileInputStream(new File(docPath)), "UTF-8"));
			DocumentRoot doc = GenericGamtaXML.readDocument(docIn);
			docIn.close();
			
			//	process document
			Properties params = new Properties();
			params.setProperty(Analyzer.ONLINE_PARAMETER, Analyzer.ONLINE_PARAMETER);
			for (int t = 0; t < dps.length; t++) {
				sysOut.println("Running Document Processor '" + dps[t].getName() + "'");
				if (dps[t] instanceof MonitorableDocumentProcessor)
					((MonitorableDocumentProcessor) dps[t]).process(doc, params, pm);
				else dps[t].process(doc, params);
			}
			
			//	store updates
			BufferedWriter docOut = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(docPath)), "UTF-8"));
			GenericGamtaXML.storeDocument(doc, docOut);
			docOut.flush();
			docOut.close();
		}
		
		//	catch and log whatever might go wrong
		catch (Throwable t) {
			sysOut.println("Error processing document: " + t.getMessage());
			t.printStackTrace(sysOut);
		}
		
		//	shut down whatever threads are left
		System.exit(0);
	}
	
	private static ConfigurationDescriptor getConfiguration(File dataBasePath, String configHost, String configName) {
		
		//	get available configurations
		ConfigurationDescriptor[] configurations = getConfigurations(dataBasePath, configHost);
		
		//	get selected configuration, doing update if required
		return ConfigurationUtils.getConfiguration(configurations, configName, dataBasePath, false, false);
	}
	
	private static ConfigurationDescriptor[] getConfigurations(File dataBasePath, String configHost) {
		
		//	collect configurations
		final ArrayList configList = new ArrayList();
		
		//	load local non-default configurations
		ConfigurationDescriptor[] configs = ConfigurationUtils.getLocalConfigurations(dataBasePath);
		for (int c = 0; c < configs.length; c++)
			configList.add(configs[c]);
		
		//	get downloaded zip files
		configs = ConfigurationUtils.getZipConfigurations(dataBasePath);
		for (int c = 0; c < configs.length; c++)
			configList.add(configs[c]);
		
		//	get remote configurations
		if (configHost != null) {
			configs = ConfigurationUtils.getRemoteConfigurations(configHost);
			for (int c = 0; c < configs.length; c++)
				configList.add(configs[c]);
		}
		
		//	finally ...
		return ((ConfigurationDescriptor[]) configList.toArray(new ConfigurationDescriptor[configList.size()]));
	}
}