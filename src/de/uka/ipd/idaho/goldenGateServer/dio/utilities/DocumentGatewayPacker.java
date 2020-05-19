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
package de.uka.ipd.idaho.goldenGateServer.dio.utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.ZipOutputStream;

import javax.swing.JOptionPane;
import javax.swing.UIManager;

import de.uka.ipd.idaho.goldenGate.utilities.PackerUtils;

/**
 * @author sautter
 *
 */
public class DocumentGatewayPacker {
	public static void main(String[] args) {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (Exception e) {}
		
		try {
			buildVersion();
		}
		catch (Exception e) {
			System.out.println("An error occurred creating the document gateway zip file:\n" + e.getMessage());
			e.printStackTrace(System.out);
			JOptionPane.showMessageDialog(null, ("An error occurred creating the document gateway zip file:\n" + e.getMessage()), "Document Gateway Creation Error", JOptionPane.ERROR_MESSAGE);
		}
	}
	
	private static final String[] specialFiles = {
		"Parameters.cnfg",
		"README.txt",
	};
	
	private static void buildVersion() throws Exception {
		File rootFolder = new File(PackerUtils.normalizePath(new File(".").getAbsolutePath()));
		System.out.println("Root folder is '" + rootFolder.getAbsolutePath() + "'");
		
		String configName = PackerUtils.selectConfigurationName(rootFolder, "Please select the configuration to include in the document gateway zip file.", false, null);
		if (configName == null)
			return;
		System.out.println(("Local Master Configuration".equals(configName) ? configName : ("Configuration '" + configName + "'")) + " selected for zipping.");
		
		String versionZipName = "DocumentGateway.zip";
		System.out.println("Building document gateway '" + versionZipName + "'");
		
		String[] coreFileNames = getCoreFileNames(rootFolder);
		String[] configFileNames = PackerUtils.getConfigFileNames(rootFolder, configName);
		
		File versionZipFile = new File(rootFolder, ("_Zips/" + versionZipName));
		if (versionZipFile.exists()) {
			versionZipFile.renameTo(new File(rootFolder, ("_Zips/" + versionZipName + "." + System.currentTimeMillis() + ".old")));
			versionZipFile = new File(rootFolder, ("_Zips/" + versionZipName));
		}
		System.out.println("Creating document gateway zip file '" + versionZipFile.getAbsolutePath() + "'");
		
		versionZipFile.getParentFile().mkdirs();
		versionZipFile.createNewFile();
		ZipOutputStream versionZipper = new ZipOutputStream(new FileOutputStream(versionZipFile));
		
		for (int s = 0; s < specialFiles.length; s++) {
			File specialFile = new File(rootFolder, ("_DocumentGatewayPacker." + specialFiles[s]));
			PackerUtils.writeZipFileEntry(specialFile, specialFiles[s], versionZipper);
		}
		
		PackerUtils.writeZipFileEntries(rootFolder, versionZipper, coreFileNames);
		for (int f = 0; f < configFileNames.length; f++) {
			if ("files.txt".equals(configFileNames[f]))
				continue;
			else if ("configuration.xml".equals(configFileNames[f]))
				continue;
			else if ("timestamp.txt".equals(configFileNames[f]))
				continue;
			else if ("README.txt".equals(configFileNames[f]))
				continue;
			
			File sourceFile;
			if ("Local Master Configuration".equals(configName))
				sourceFile = new File(rootFolder, configFileNames[f]);
			else sourceFile = new File(rootFolder, ("Configurations/" + configName + "/" + configFileNames[f]));
			PackerUtils.writeZipFileEntry(sourceFile, configFileNames[f], versionZipper);
		}
		
		versionZipper.flush();
		versionZipper.close();
		
		System.out.println("Document gateway zip file '" + versionZipFile.getAbsolutePath() + "' created successfully.");
		JOptionPane.showMessageDialog(null, ("Document gateway '" + versionZipName + "' zip file created successfully."), "Document Gateway Created Successfully", JOptionPane.INFORMATION_MESSAGE);
	}
	
	private static String[] getCoreFileNames(File rootFolder) throws IOException {
		Set coreFiles = new TreeSet();
		
		File coreFileList = new File(rootFolder, "_DocumentGatewayPacker.cnfg");
		BufferedReader br = new BufferedReader(new FileReader(coreFileList));
		String coreFile;
		while ((coreFile = br.readLine()) != null) {
			coreFile = coreFile.trim();
			if ((coreFile.length() != 0) && !coreFile.startsWith("//"))
				coreFiles.add(coreFile);
		}
		br.close();
		
		for (int s = 0; s < specialFiles.length; s++)
			coreFiles.remove(specialFiles[s]);
		
		return ((String[]) coreFiles.toArray(new String[coreFiles.size()]));
	}
}
