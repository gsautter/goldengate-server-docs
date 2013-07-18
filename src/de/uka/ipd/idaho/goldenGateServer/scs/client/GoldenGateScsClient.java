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
package de.uka.ipd.idaho.goldenGateServer.scs.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection;
import de.uka.ipd.idaho.goldenGateServer.client.ServerConnection.Connection;
import de.uka.ipd.idaho.goldenGateServer.scs.GoldenGateScsConstants;

/**
 * Client for accessing the statistics data on an SRS document collection stored
 * by a backing GoldenGATE SCS.
 * 
 * @author sautter
 */
public class GoldenGateScsClient implements GoldenGateScsConstants {
	
	private ServerConnection serverConnection;
	
	/**
	 * Constructor
	 * @param serverConnection the ServerConnection to use for communication
	 *            with the backing SCS
	 */
	public GoldenGateScsClient(ServerConnection serverConnection) {
		this.serverConnection = serverConnection;
	}
	
	/**
	 * Retrieve the definitions of the statistics field available in the backing
	 * SCS.
	 * @return and array holding the field sets, which in turn hold the fields
	 * @throws IOException
	 */
	public FieldSet[] getFields() throws IOException {
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_FIELDS);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_FIELDS.equals(error))
				return FieldSet.readFieldSets(br);
			
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * Compile and retrieve a statistics from the data stored in the backing
	 * SCS. TODO explain query mechanism
	 * @param fields the query field objects representing the query / defining
	 *            the statistics to compile
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public Statistics getStatistics(QueryField[] fields) throws IOException {
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(GET_STATISTICS);
			bw.newLine();
			for (int f = 0; f < fields.length; f++)
				fields[f].writeXml(bw);
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if (GET_STATISTICS.equals(error))
				return Statistics.readStatistics(br);
			
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
}