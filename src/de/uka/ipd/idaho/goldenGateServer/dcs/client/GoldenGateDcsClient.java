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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
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
public abstract class GoldenGateDcsClient implements GoldenGateDcsConstants {
	private ServerConnection serverConnection;
	private String dcsLetterCode;
	
	/**
	 * Constructor
	 * @param serverConnection the ServerConnection to use for communication
	 *            with the backing DCS
	 * @param dcsLetterCode the letter code of the concrete DCS instance to
	 *            communicate with
	 */
	protected GoldenGateDcsClient(ServerConnection serverConnection, String dcsLetterCode) {
		this.serverConnection = serverConnection;
		this.dcsLetterCode = dcsLetterCode;
		this.cache = getCache(this.dcsLetterCode);
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
	 * @param customFilters custom filters to apply to the statistics fields
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String[] customFilters) throws IOException {
		return this.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, customFilters, -1, true);
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
	 * @param customFilters custom filters to apply to the statistics fields
	 * @param allowCache allow returning cached results?
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String[] customFilters, boolean allowCache) throws IOException {
		return this.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, customFilters, -1, allowCache);
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
	 * @param customFilters custom filters to apply to the statistics fields
	 * @param limit the maximum number of output rows (-1 returns all rows)
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String[] customFilters, int limit) throws IOException {
		return this.getStatistics(outputFields, groupingFields, orderingFields, fieldPredicates, fieldAggregates, aggregatePredicates, customFilters, limit, true);
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
	 * @param customFilters custom filters to apply to the statistics fields
	 * @param limit the maximum number of output rows (-1 returns all rows)
	 * @param allowCache allow returning cached results?
	 * @return the requested statistics, packed in a string relation
	 * @throws IOException
	 */
	public DcStatistics getStatistics(String[] outputFields, String[] groupingFields, String[] orderingFields, Properties fieldPredicates, Properties fieldAggregates, Properties aggregatePredicates, String[] customFilters, int limit, boolean allowCache) throws IOException {
		
		//	normalize limit
		if (limit < 1)
			limit = -1;
		
		//	generate cache key
		String cacheKey = "" + limit +
				"-" + Integer.toString(Arrays.hashCode(outputFields), 16) +
				"-" + Integer.toString(Arrays.hashCode(groupingFields), 16) +
				"-" + Integer.toString(Arrays.hashCode(orderingFields), 16) +
				"-" + ((fieldPredicates == null) ? "0" : Integer.toString(fieldPredicates.hashCode(), 16)) +
				"-" + ((fieldAggregates == null) ? "0" : Integer.toString(fieldAggregates.hashCode(), 16)) +
				"-" + ((aggregatePredicates == null) ? "0" : Integer.toString(aggregatePredicates.hashCode(), 16)) +
				"-" + ((customFilters == null) ? "0" : Integer.toString(Arrays.hashCode(customFilters), 16)) +
				"";
		
		//	do cache lookup if allowed to
		if (allowCache) {
			DcStatistics stats = ((DcStatistics) this.cache.get(cacheKey));
			if (stats != null)
				return stats;
		}
		
		//	get statistics from back-end
		Connection con = null;
		try {
			con = this.serverConnection.getConnection();
			BufferedWriter bw = con.getWriter();
			
			bw.write(this.dcsLetterCode + GET_STATISTICS_COMMAND_SUFFIX);
			bw.newLine();
			bw.write("" + limit);
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
			if (customFilters != null)
				for (int f = 0; f < customFilters.length; f++) {
					bw.write("CF:" + URLEncoder.encode(customFilters[f], ENCODING));
					bw.newLine();
				}
			bw.newLine();
			bw.flush();
			
			BufferedReader br = con.getReader();
			String error = br.readLine();
			if ((this.dcsLetterCode + GET_STATISTICS_COMMAND_SUFFIX).equals(error)) {
				DcStatistics stats = DcStatistics.readStatistics(br);
				this.cache.put(cacheKey, stats);
				return stats;
			}
			
			else throw new IOException(error);
		}
		finally {
			if (con != null)
				con.close();
		}
	}
	
	private Map cache;
	
	private static final int initCacheSize = 128;
	private static final int maxCacheSize = 256;
	private static Map cachesByDcsLetterCode = Collections.synchronizedMap(new HashMap(2));
	private static synchronized Map getCache(String dcsLetterCode) {
		Map cache = ((Map) cachesByDcsLetterCode.get(dcsLetterCode));
		if (cache == null) {
			cache = Collections.synchronizedMap(new LinkedHashMap(initCacheSize, 0.9f, true) {
				private long statsLastUpdated = 0;
				public synchronized Object put(Object key, Object value) {
					if (this.statsLastUpdated < ((DcStatistics) value).lastUpdated) {
						this.clear();
						this.statsLastUpdated = ((DcStatistics) value).lastUpdated;
					}
					return super.put(key, value);
				}
				protected boolean removeEldestEntry(Entry eldest) {
					return (this.size() > maxCacheSize);
				}
			});
			cachesByDcsLetterCode.put(dcsLetterCode, cache);
		}
		return cache;
	}
}