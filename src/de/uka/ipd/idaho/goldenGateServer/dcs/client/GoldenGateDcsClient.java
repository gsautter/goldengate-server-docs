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
package de.uka.ipd.idaho.goldenGateServer.dcs.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Properties;

import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants;

/**
 * Client for accessing the document collection statistics data stored in a
 * backing GoldenGATE DCS instance.
 * 
 * @author sautter
 */
public class GoldenGateDcsClient implements GoldenGateDcsConstants {
	
	private ServerConnection serverConnection;
	private String dcsLetterCode;
	
	/**
	 * Constructor
	 * @param serverConnection the ServerConnection to use for communication
	 *            with the backing DCS
	 * @param dcsLetterCode the letter code of the concrete DCS instance to
	 *            communicate with
	 */
	public GoldenGateDcsClient(ServerConnection serverConnection, String dcsLetterCode) {
		this.serverConnection = serverConnection;
		this.dcsLetterCode = dcsLetterCode;
	}
	
	/**
	 * Retrieve the definitions of the statistics fields available in the
	 * backing DCS instance.
	 * @return the field sets
	 * @throws IOException
	 */
	public StatFieldSet getFieldSet() throws IOException {
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(this.dcsLetterCode + GET_FIELDS_COMMAND_SUFFIX);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if ((this.dcsLetterCode + GET_FIELDS_COMMAND_SUFFIX).equals(error))
				return StatFieldSet.readFieldSet(br);
			
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Compile and retrieve a statistics from the data stored in the backing
	 * DCS instance. Ordering is ascending for strings, but descending for
	 * numbers. The <code>DocCount</code> aggregate field is contained in every
	 * statistics.
	 * @param outputFields the fields to include in the statistics
	 * @param groupingFields the fields to use for grouping
	 * @param orderingFields the fields to use for ordering
	 * @param fieldPredicates filter predicates against individual fields
	 * @param fieldAggregates custom aggregation functions for fields not used for grouping
	 * @param aggregatePredicates filter predicates against aggregate data
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates) throws IOException {
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(this.dcsLetterCode + GET_STATISTICS_COMMAND_SUFFIX);
			bw.newLine();
			for (int f = 0; f < outputFields.length; f++) {
				if (f != 0) bw.write(" ");
				bw.write(outputFields[f]);
			}
			bw.newLine();
			for (int f = 0; f < groupingFields.length; f++) {
				if (f != 0) bw.write(" ");
				bw.write(groupingFields[f]);
			}
			bw.newLine();
			for (int f = 0; f < orderingFields.length; f++) {
				if (f != 0) bw.write(" ");
				bw.write(orderingFields[f]);
			}
			bw.newLine();
			if (fieldPredicates != null)
				for (Iterator ffit = fieldPredicates.keySet().iterator(); ffit.hasNext();) {
					String field = ((String) ffit.next());
					String predicate = fieldPredicates.getProperty(field);
					bw.write("FP:" + field + "=" + URLEncoder.encode(predicate, ENCODING));
					bw.newLine();
				}
			if (fieldAggregates != null)
				for (Iterator ffit = fieldAggregates.keySet().iterator(); ffit.hasNext();) {
					String field = ((String) ffit.next());
					String aggregate = fieldAggregates.getProperty(field);
					bw.write("FA:" + field + "=" + URLEncoder.encode(aggregate, ENCODING));
					bw.newLine();
				}
			if (aggregatePredicates != null)
				for (Iterator ffit = aggregatePredicates.keySet().iterator(); ffit.hasNext();) {
					String field = ((String) ffit.next());
					String predicate = aggregatePredicates.getProperty(field);
					bw.write("AP:" + field + "=" + URLEncoder.encode(predicate, ENCODING));
					bw.newLine();
				}
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if ((this.dcsLetterCode + GET_STATISTICS_COMMAND_SUFFIX).equals(error))
				return DcStatistics.readStatistics(br);
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
}