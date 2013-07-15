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
package de.uka.ipd.idaho.goldenGateServer.srs.filters;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import de.uka.ipd.idaho.easyIO.settings.Settings;
import de.uka.ipd.idaho.gamta.QueriableAnnotation;
import de.uka.ipd.idaho.gamta.util.AnnotationInputStream;
import de.uka.ipd.idaho.goldenGateServer.srs.AbstractStorageFilter;

/**
 * An XmlSchemaFilter uses XML schema to determine whether or not to store a
 * document in SRS. This class takes four parameters in the 'config.cnfg' file
 * in its data path, one of which is required: - schemaName: A nice name
 * for the schema in use - schemaFileName: The name of the XML schema file
 * to use, relative to the data path - filterDocuments: If set to any
 * non-null value, the filter will validate documents as a whole using the
 * filter(QueriableAnnotation) method. This is recommended when no
 * DocumentSplitter is in use. - filterDocumentParts: If set to any
 * non-null value, the filter will validate document parts after splitting using
 * the filter(QueriableAnnotation[], QueriableAnnotation) method. This is
 * recommended when one or more DocumentSplitters are in use.
 * 
 * @author sautter
 */
public class XmlSchemaFilter extends AbstractStorageFilter {

	private Schema schema = null;
	
	private String schemaName = null;
	private String schemaFileName = null;
	
	private boolean filterDocuments = false;
	private boolean filterDocumentParts = false;
	
	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.AbstractStorageFilter#init()
	 */
	public void init() {
		Settings set = Settings.loadSettings(new File(this.dataPath, "config.cnfg"));
		
		//	get behavioral configuration
		this.filterDocuments = set.containsKey("filterDocuments");
		this.filterDocumentParts = set.containsKey("filterDocumentParts");
		
		//	get schema data
		this.schemaName = set.getSetting("schemaName");
		this.schemaFileName = set.getSetting("schemaFile");
		
		//	load schema
		if (this.schemaFileName != null) try {
			File schemaFile = new File(this.dataPath, schemaFileName);
			if (this.schemaName == null)
				this.schemaName = schemaFile.getName();
			
			SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			InputStream is = new FileInputStream(schemaFile);
		    Source source = new StreamSource(is);
			try {
				this.schema = schemaFactory.newSchema(source);
			} catch (SAXException e) {}
			is.close();
		} catch (IOException ioe) {}
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.StorageFilter#filter(de.uka.ipd.idaho.gamta.QueriableAnnotation)
	 */
	public String filter(QueriableAnnotation doc) {
		return (this.filterDocuments ? this.validate(doc) : null);
	}

	/* (non-Javadoc)
	 * @see de.uka.ipd.idaho.goldenGateServer.srs.StorageFilter#filter(de.uka.ipd.idaho.gamta.QueriableAnnotation[], de.uka.ipd.idaho.gamta.QueriableAnnotation)
	 */
	public QueriableAnnotation[] filter(QueriableAnnotation[] parts, QueriableAnnotation doc) {
		if (this.filterDocumentParts) {
			ArrayList partList = new ArrayList();
			for (int p = 0; p < parts.length; p++) {
				String error = this.validate(parts[p]);
				if (error == null)
					partList.add(parts[p]);
			}
			return ((QueriableAnnotation[]) partList.toArray(new QueriableAnnotation[partList.size()]));
		}
		else return parts;
	}
	
	private String validate(QueriableAnnotation doc) {
		
		//	set up collecting messages
		final HashSet errorCollector = new HashSet();
		
		//	get validator
	    final Validator validator = this.schema.newValidator();
		validator.setErrorHandler(new ErrorHandler() {
			public void error(SAXParseException exception) throws SAXException {
				errorCollector.add(exception);
				throw exception; // stop immediately, no use going on
			}
			public void fatalError(SAXParseException exception) throws SAXException {
				errorCollector.add(exception);
				throw exception; // stop immediately, no use going on
			}
			public void warning(SAXParseException exception) throws SAXException {
				//	ignore warnings, they are not validity errors
			}
		});
		
		//	do validation
	    try {
			SAXResult sRes = new SAXResult();
	        validator.validate(new SAXSource(new InputSource(new AnnotationInputStream(doc, "  ", "utf-8"))), sRes);
	    }
	    catch (SAXException se) {
			errorCollector.add(se);
	    }
	    catch (IOException ioe) {
			errorCollector.add(ioe);
		}
		
	    //	report result
	    return (errorCollector.isEmpty() ? null : ("The specified document does not match the '" + this.schemaName + "' XML schema"));
	}
}
