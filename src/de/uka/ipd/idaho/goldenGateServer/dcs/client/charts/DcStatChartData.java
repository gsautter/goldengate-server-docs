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
package de.uka.ipd.idaho.goldenGateServer.dcs.client.charts;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.TreeSet;

import de.uka.ipd.idaho.gamta.util.CountingSet;
import de.uka.ipd.idaho.goldenGateServer.dcs.GoldenGateDcsConstants.DcStatistics;
import de.uka.ipd.idaho.goldenGateServer.dcs.client.charts.DcStatChartEngine.DcStatChartRequest;
import de.uka.ipd.idaho.stringUtils.csvHandler.StringTupel;

/**
 * Wrapper for data retrieved from GoldenGATE Document Collection Statistics,
 * adding grouping, cutoff, and output functionality. 
 * 
 * @author sautter
 */
public class DcStatChartData {
	final DcStatistics sourceData;
	
	public final String seriesField;
	public final String groupField;
	
	public final boolean translateMonthNumbers;
	
	private LinkedHashMap seriesByName = new LinkedHashMap();
	private LinkedHashMap groupsByName = new LinkedHashMap();
	
	public final String valueField;
	
	private int value = 0;
	
	/** @return the sum of all values in the charts data */
	public int getValue() {
		return this.value;
	}
	/** @return the number of data series in the charts data */
	public int getSeriesCount() {
		return this.seriesByName.size();
	}
	/** @return the number of data groups in the charts data */
	public int getGroupCount() {
		return this.groupsByName.size();
	}
	
	DcStatChartData(DcStatistics stats, String seriesField, String groupField, String valueField, boolean tmn) {
		this.sourceData = stats;
		this.seriesField = seriesField;
		this.groupField = groupField;
		this.valueField = valueField;
		this.translateMonthNumbers = tmn;
	}
	void addSeries(String name, String label) {
		DcStatSeriesHead dssh = this.getSeriesHead(name);
		if (label != null)
			dssh.label = label;
	}
	DcStatSeriesHead getSeriesHead(String name) {
		DcStatSeriesHead dssh = ((DcStatSeriesHead) this.seriesByName.get(name));
		if (dssh == null) {
			dssh = new DcStatSeriesHead(name);
			this.seriesByName.put(name, dssh);
		}
		return dssh;
	}
	void addGroup(String name, String label, boolean isMultiSeries) {
		DcStatGroup dsg = this.getGroup(name, isMultiSeries);
		if (label != null)
			dsg.label = label;
	}
	DcStatGroup getGroup(String name, boolean isMultiSeries) {
		DcStatGroup dsg = ((DcStatGroup) this.groupsByName.get(name));
		if (dsg == null) {
			dsg = new DcStatGroup(name, isMultiSeries);
			this.groupsByName.put(name, dsg);
		}
		return dsg;
	}
	void addData(StringTupel st, String seriesName, String groupName, String valueField) {
		try {
			int value = Integer.parseInt(st.getValue(valueField, "0"));
			this.value += value;
			if (seriesName != null)
				this.getSeriesHead(seriesName).add(value);
			if (groupName != null)
				this.getGroup(groupName, (seriesName != null)).add(value, seriesName);
		} catch (NumberFormatException nfe) {}
	}
	void removeEmptyGroups() {
		for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
			DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
			if (dsg.value == 0)
				git.remove();
		}
	}
	void pruneGroups(int cutoff) {
		if (this.groupsByName.size() <= cutoff)
			return;
		TreeSet groups = new TreeSet(new Comparator() {
			public int compare(Object obj1, Object obj2) {
				DcStatGroup dsg1 = ((DcStatGroup) obj1);
				DcStatGroup dsg2 = ((DcStatGroup) obj2);
				return ((dsg1.value == dsg2.value) ? dsg1.label.compareTo(dsg2.label) : (dsg2.value - dsg1.value));
			}
		});
		for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();)
			groups.add(this.getGroup(((String) git.next()), (this.seriesByName.size() != 0)));
		DcStatGroup oDsg = new DcStatGroup("other", (this.seriesByName.size() != 0));
		oDsg.label = ("Other (" + (this.groupsByName.size() - cutoff) + ")");
		while (groups.size() > cutoff) {
			DcStatGroup dsg = ((DcStatGroup) groups.last());
			this.groupsByName.remove(dsg.name);
			groups.remove(dsg);
			oDsg.addAll(dsg);
		}
		this.groupsByName.put(oDsg.name, oDsg);
	}
	void removeEmptySeries() {
		for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
			DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
			if (dssh.value != 0)
				continue;
			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
				DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
				dsg.renameSeries(dssh.name, "");
			}
			sit.remove();
		}
	}
	void pruneSeries(int cutoff) {
		if (this.seriesByName.size() <= cutoff)
			return;
		TreeSet series = new TreeSet(new Comparator() {
			public int compare(Object obj1, Object obj2) {
				DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
				DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
				return ((dssh1.value == dssh2.value) ? dssh1.label.compareTo(dssh2.label) : (dssh2.value - dssh1.value));
			}
		});
		for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();)
			series.add(this.getSeriesHead((String) sit.next()));
		DcStatSeriesHead oDssh = new DcStatSeriesHead("other");
		oDssh.label = ("Other (" + (this.seriesByName.size() - cutoff) + ")");
		while (series.size() > cutoff) {
			DcStatSeriesHead dssh = ((DcStatSeriesHead) series.last());
			this.seriesByName.remove(dssh.name);
			series.remove(dssh);
			oDssh.value += dssh.value;
			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
				DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
				dsg.renameSeries(dssh.name, oDssh.name);
			}
		}
		this.seriesByName.put(oDssh.name, oDssh);
	}
	void bucketizeGroups(String bucketBoundaries, boolean truncate, boolean numeric, boolean monthNames) {
		
		//	parse, de-duplicate, and validate bucket boundaries
		String[] bucketBoundaryStrings = bucketBoundaries.trim().split("\\s*\\;\\s*");
		ArrayList bucketBoundaryList = new ArrayList();
		for (int b = 0; b < bucketBoundaryStrings.length; b++) {
			if (bucketBoundaryList.contains(bucketBoundaryStrings[b]))
				continue;
			if (numeric) try {
				Double.parseDouble(bucketBoundaryStrings[b].replaceAll("\\,", "."));
			} catch (NumberFormatException nfe) {continue;}
			if (bucketBoundaryStrings[b].length() != 0)
				bucketBoundaryList.add(bucketBoundaryStrings[b]);
		}
		if (bucketBoundaryList.size() < 2)
			return;
		
		//	get bucket order
		Comparator bucketOrder = this.getLabelOrder(numeric);
		
		//	assess whether to sort ascending or descending
		int upStepCount = 0;
		int downStepCount = 0;
		for (int b = 1; b < bucketBoundaryList.size(); b++) {
			int c = bucketOrder.compare(bucketBoundaryList.get(b-1), bucketBoundaryList.get(b));
			if (c < 0)
				upStepCount++;
			else if (0 < c)
				downStepCount++;
		}
		boolean bucketBoundariesDescending = (downStepCount > upStepCount);
		
		//	order bucket boundaries
		Collections.sort(bucketBoundaryList, bucketOrder);
		if (bucketBoundariesDescending)
			Collections.reverse(bucketBoundaryList);
		
		//	create buckets
		DcStatGroup[] buckets = new DcStatGroup[bucketBoundaryList.size() + 1];
		for (int b = 0; b < bucketBoundaryList.size(); b++)
			buckets[b] = new DcStatGroup(((String) bucketBoundaryList.get(b)), (this.seriesByName.size() != 0));
		if (monthNames)
			buckets[bucketBoundaryList.size()] = new DcStatGroup((bucketBoundariesDescending ? "1" : "12"), (this.seriesByName.size() != 0));
		else if (numeric)
			buckets[bucketBoundaryList.size()] = new DcStatGroup(("" + (bucketBoundariesDescending ? Integer.MIN_VALUE : Integer.MAX_VALUE)), (this.seriesByName.size() != 0));
		else buckets[bucketBoundaryList.size()] = new DcStatGroup((bucketBoundariesDescending ? "" : "\uFFFF"), (this.seriesByName.size() != 0));
		
		//	initialize numeric left boundaries in labels
		if (numeric)
			for (int b = 1; b < buckets.length; b++) {
				buckets[b].label = ("" + (Double.parseDouble(buckets[b-1].name) + (bucketBoundariesDescending ? -1 : 1)));
				buckets[b].label = buckets[b].label.replaceAll("\\.[0]+\\z", "");
			}
		
		//	keep track if lowmost bucket has value less than boundary
		boolean lowmostBucketIsLessEqual = false;
		
		//	distribute data into buckets, collecting left boundary in label
		for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
			DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
			for (int b = 0; b < buckets.length; b++) {
				int c = bucketOrder.compare(dsg.name, buckets[b].name);
				if (bucketBoundariesDescending && (0 <= c)) {
					buckets[b].addAll(dsg);
					if (0 < bucketOrder.compare(dsg.name, buckets[b].label)) {
						buckets[b].label = dsg.name;
						if ((b + 1) == buckets.length)
							lowmostBucketIsLessEqual = true;
					}
					break;
				}
				else if (!bucketBoundariesDescending && (c <= 0)) {
					buckets[b].addAll(dsg);
					if (bucketOrder.compare(dsg.name, buckets[b].label) < 0)
						buckets[b].label = dsg.name;
					if ((b == 0) && (c < 0))
						lowmostBucketIsLessEqual = true;
					break;
				}
			}
		}
		
		//	create labels
		if (monthNames) {
			if (Integer.parseInt(buckets[0].label) < 1)
				buckets[0].label = "1";
			else if (Integer.parseInt(buckets[0].label) > 12)
				buckets[0].label = "12";
			for (int b = 0; b < buckets.length; b++) {
				if (buckets[b].label.equals(buckets[b].name))
					buckets[b].label = numbersToMonthNames.getProperty(buckets[b].name, buckets[b].name);
				else buckets[b].label = (numbersToMonthNames.getProperty(buckets[b].label, buckets[b].label) + "-" + numbersToMonthNames.getProperty(buckets[b].name, buckets[b].name));
			}
		}
		else {
			buckets[0].label = ((bucketBoundariesDescending ? "\u2265" : (lowmostBucketIsLessEqual ? "\u2264" : "")) + buckets[0].name);
			for (int b = 1; b < (buckets.length - 1); b++) {
				if (!buckets[b].label.equals(buckets[b].name))
					buckets[b].label = (buckets[b].label + "-" + buckets[b].name);
			}
			buckets[buckets.length-1].label = ((bucketBoundariesDescending ? (lowmostBucketIsLessEqual ? "\u2264" : "") : "\u2265") + buckets[buckets.length-1].label);
		}
		
		//	leading and tailing set empty buckets to null
		if (truncate) {
			for (int b = 0; b < buckets.length; b++) {
				if (buckets[b] == null)
					continue;
				else if (buckets[b].value == 0)
					buckets[b] = null;
				else break;
			}
			for (int b = (buckets.length-1); b >= 0; b--) {
				if (buckets[b] == null)
					continue;
				else if (buckets[b].value == 0)
					buckets[b] = null;
				else break;
			}
		}
		
		//	replace plain groups with bucktized ones
		this.groupsByName.clear();
		for (int b = 0; b < buckets.length; b++) {
			if (buckets[b] != null)
				this.groupsByName.put(buckets[b].name, buckets[b]);
		}
	}
	void bucketizeSeries(String bucketBoundaries, boolean truncate, boolean numeric, boolean monthNames) {
		
		//	parse, de-duplicate, and validate bucket boundaries
		String[] bucketBoundaryStrings = bucketBoundaries.trim().split("\\s*\\;\\s*");
		ArrayList bucketBoundaryList = new ArrayList();
		for (int b = 0; b < bucketBoundaryStrings.length; b++) {
			if (bucketBoundaryList.contains(bucketBoundaryStrings[b]))
				continue;
			if (numeric) try {
				Double.parseDouble(bucketBoundaryStrings[b].replaceAll("\\,", "."));
			} catch (NumberFormatException nfe) {continue;}
			if (bucketBoundaryStrings[b].length() != 0)
				bucketBoundaryList.add(bucketBoundaryStrings[b]);
		}
		if (bucketBoundaryList.size() < 2)
			return;
		
		//	get bucket order
		Comparator bucketOrder = this.getLabelOrder(numeric);
		
		//	assess whether to sort ascending or descending
		int upStepCount = 0;
		int downStepCount = 0;
		for (int b = 1; b < bucketBoundaryList.size(); b++) {
			int c = bucketOrder.compare(bucketBoundaryList.get(b-1), bucketBoundaryList.get(b));
			if (c < 0)
				upStepCount++;
			else if (0 < c)
				downStepCount++;
		}
		boolean bucketBoundariesDescending = (downStepCount > upStepCount);
		
		//	order bucket boundaries
		Collections.sort(bucketBoundaryList, bucketOrder);
		if (bucketBoundariesDescending)
			Collections.reverse(bucketBoundaryList);
		
		//	create buckets
		DcStatSeriesHead[] buckets = new DcStatSeriesHead[bucketBoundaryList.size() + 1];
		for (int b = 0; b < bucketBoundaryList.size(); b++)
			buckets[b] = new DcStatSeriesHead((String) bucketBoundaryList.get(b));
		if (monthNames)
			buckets[bucketBoundaryList.size()] = new DcStatSeriesHead(bucketBoundariesDescending ? "1" : "12");
		else if (numeric)
			buckets[bucketBoundaryList.size()] = new DcStatSeriesHead("" + (bucketBoundariesDescending ? Integer.MIN_VALUE : Integer.MAX_VALUE));
		else buckets[bucketBoundaryList.size()] = new DcStatSeriesHead(bucketBoundariesDescending ? "" : "\uFFFF");
		
		//	initialize numeric left boundaries labels
		if (numeric)
			for (int b = 1; b < buckets.length; b++) {
				buckets[b].label = ("" + (Double.parseDouble(buckets[b-1].name) + (bucketBoundariesDescending ? -1 : 1)));
				buckets[b].label = buckets[b].label.replaceAll("\\.[0]+\\z", "");
			}
		
		//	keep track if lowmost bucket has value less than boundary
		boolean lowmostBucketIsLessEqual = false;
		
		//	distribute data into buckets, collecting left boundary in label
		for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
			DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
			for (int b = 0; b < buckets.length; b++) {
				int c = bucketOrder.compare(dssh.name, buckets[b].name);
				if (bucketBoundariesDescending && (0 <= c)) {
					buckets[b].value += dssh.value;
					for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
						DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
						dsg.renameSeries(dssh.name, buckets[b].name);
						if ((b + 1) == buckets.length)
							lowmostBucketIsLessEqual = true;
					}
					if (0 < bucketOrder.compare(dssh.name, buckets[b].label))
						buckets[b].label = dssh.name;
					break;
				}
				else if (!bucketBoundariesDescending && (c <= 0)) {
					buckets[b].value += dssh.value;
					for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
						DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
						dsg.renameSeries(dssh.name, buckets[b].name);
					}
					if (bucketOrder.compare(dssh.name, buckets[b].label) < 0)
						buckets[b].label = dssh.name;
					if ((b == 0) && (c < 0))
						lowmostBucketIsLessEqual = true;
					break;
				}
			}
		}
		
		//	create labels
		if (monthNames) {
			if (Integer.parseInt(buckets[0].label) < 1)
				buckets[0].label = "1";
			else if (Integer.parseInt(buckets[0].label) > 12)
				buckets[0].label = "12";
			for (int b = 0; b < buckets.length; b++) {
				if (buckets[b].label.equals(buckets[b].name))
					buckets[b].label = numbersToMonthNames.getProperty(buckets[b].name, buckets[b].name);
				else buckets[b].label = (numbersToMonthNames.getProperty(buckets[b].label, buckets[b].label) + "-" + numbersToMonthNames.getProperty(buckets[b].name, buckets[b].name));
			}
		}
		else {
			buckets[0].label = ((bucketBoundariesDescending ? "\u2265" : (lowmostBucketIsLessEqual ? "\u2264" : "")) + buckets[0].name);
			for (int b = 1; b < (buckets.length - 1); b++) {
				if (!buckets[b].label.equals(buckets[b].name))
					buckets[b].label = (buckets[b].label + "-" + buckets[b].name);
			}
			buckets[buckets.length-1].label = ((bucketBoundariesDescending ? (lowmostBucketIsLessEqual ? "\u2264" : "") : "\u2265") + buckets[buckets.length-1].label);
		}
		
		//	leading and tailing set empty buckets to null
		if (truncate) {
			for (int b = 0; b < buckets.length; b++) {
				if (buckets[b] == null)
					continue;
				else if (buckets[b].value == 0)
					buckets[b] = null;
				else break;
			}
			for (int b = (buckets.length-1); b >= 0; b--) {
				if (buckets[b] == null)
					continue;
				else if (buckets[b].value == 0)
					buckets[b] = null;
				else break;
			}
		}
		
		//	replace plain series with bucktized ones
		this.seriesByName.clear();
		for (int b = 0; b < buckets.length; b++) {
			if (buckets[b] != null)
				this.seriesByName.put(buckets[b].name, buckets[b]);
		}
	}
	void translateMonthNumbers() {
		if ((seriesField != null) && seriesField.toLowerCase().endsWith("month"))
			for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
				DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
				dssh.label = numbersToMonthNames.getProperty(dssh.name, dssh.label);
			}
		if ((groupField != null) && groupField.toLowerCase().endsWith("month"))
			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
				DcStatGroup dsg = this.getGroup(((String) git.next()), (seriesField != null));
				dsg.label = numbersToMonthNames.getProperty(dsg.name, dsg.label);
			}
	}
	void orderGroups(String groupOrder, boolean namesNumeric) {
		final Comparator labelOrder = this.getLabelOrder(namesNumeric);
		Comparator order;
		if ("label".equals(groupOrder))
			order = new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatGroup dsg1 = ((DcStatGroup) obj1);
					DcStatGroup dsg2 = ((DcStatGroup) obj2);
					return labelOrder.compare(dsg1.label, dsg2.label);
				}
			};
		else if ("-label".equals(groupOrder))
			order = new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatGroup dsg1 = ((DcStatGroup) obj1);
					DcStatGroup dsg2 = ((DcStatGroup) obj2);
					return labelOrder.compare(dsg2.label, dsg1.label);
				}
			};
		else if ("value".equals(groupOrder))
			order = new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatGroup dsg1 = ((DcStatGroup) obj1);
					DcStatGroup dsg2 = ((DcStatGroup) obj2);
					return ((dsg1.value == dsg2.value) ? labelOrder.compare(dsg1.label, dsg2.label) : (dsg1.value - dsg2.value));
				}
			};
		else if ("-value".equals(groupOrder))
			order = new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatGroup dsg1 = ((DcStatGroup) obj1);
					DcStatGroup dsg2 = ((DcStatGroup) obj2);
					return ((dsg1.value == dsg2.value) ? labelOrder.compare(dsg1.label, dsg2.label) : (dsg2.value - dsg1.value));
				}
			};
		else return;
		TreeSet groups = new TreeSet(order);
		groups.addAll(this.groupsByName.values());
		this.groupsByName.clear();
		for (Iterator git = groups.iterator(); git.hasNext();) {
			DcStatGroup dsg = ((DcStatGroup) git.next());
			this.groupsByName.put(dsg.name, dsg);
		}
	}
	void orderSeries(String seriesOrder, boolean namesNumeric) {
		final Comparator labelOrder = this.getLabelOrder(namesNumeric);
		Comparator order;
		if ("label".equals(seriesOrder))
			order = new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
					DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
					return labelOrder.compare(dssh1.label, dssh2.label);
				}
			};
		else if ("-label".equals(seriesOrder))
			order = new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
					DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
					return labelOrder.compare(dssh2.label, dssh1.label);
				}
			};
		else if ("value".equals(seriesOrder))
			order = new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
					DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
					return ((dssh1.value == dssh2.value) ? labelOrder.compare(dssh1.label, dssh2.label) : (dssh1.value - dssh2.value));
				}
			};
		else if ("-value".equals(seriesOrder))
			order = new Comparator() {
				public int compare(Object obj1, Object obj2) {
					DcStatSeriesHead dssh1 = ((DcStatSeriesHead) obj1);
					DcStatSeriesHead dssh2 = ((DcStatSeriesHead) obj2);
					return ((dssh1.value == dssh2.value) ? labelOrder.compare(dssh1.label, dssh2.label) : (dssh2.value - dssh1.value));
				}
			};
		else return;
		TreeSet series = new TreeSet(order);
		series.addAll(this.seriesByName.values());
		this.seriesByName.clear();
		for (Iterator sit = series.iterator(); sit.hasNext();) {
			DcStatSeriesHead dssh = ((DcStatSeriesHead) sit.next());
			this.seriesByName.put(dssh.name, dssh);
		}
	}
	private Comparator getLabelOrder(boolean namesNumeric) {
		if (namesNumeric) return new Comparator() {
			public int compare(Object obj1, Object obj2) {
				String l1 = ((String) obj1).replaceAll("\\,", ".");
				String l2 = ((String) obj2).replaceAll("\\,", ".");
				return Double.compare(Double.parseDouble(l1), Double.parseDouble(l2));
			}
		};
		else return new Comparator() {
			public int compare(Object obj1, Object obj2) {
				return ((String) obj1).compareTo((String) obj2);
			}
		};
	}
	void writeJsonArrayContent(BufferedWriter bw, boolean isGroupFieldNumeric, boolean forMultiSeriesChart, DcStatChartRequest request, DcStatChartEngine engine, DcStatChartRenderer renderer) throws IOException {
		boolean areGroupLabelsNumeric = (
				isGroupFieldNumeric
				&&
				engine.isNumericField(this.groupField)
				&&
				!"B".equals(request.getParameter("groupCutoff"))
				&&
				(
					!this.groupField.toLowerCase().endsWith("month")
					||
					!this.translateMonthNumbers
				));
		
		if (this.seriesByName.isEmpty() && this.groupsByName.isEmpty()) {
			bw.write("    ['" + engine.getFieldLabel("Data") + "', '" + engine.getFieldLabel(this.valueField) + "'" + ((renderer == null) ? "" : renderer.getTitleRowExtension(this, request)) + "],"); bw.newLine();
			bw.write("    ['" + engine.getFieldLabel(this.valueField) + "', " + this.value + "" + ((renderer == null) ? "" : renderer.getDataRowExtension(this, request, null)) + "]"); bw.newLine();
		}
		
		else if (this.seriesByName.isEmpty()) {
			bw.write("    ['" + engine.getFieldLabel("Group") + "', '" + engine.getFieldLabel((this.valueField == null) ? "Value" : this.valueField) + "'" + ((renderer == null) ? "" : renderer.getTitleRowExtension(this, request)) + "],"); bw.newLine();
			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
				DcStatGroup dsg = this.getGroup(((String) git.next()), false);
				if (areGroupLabelsNumeric) try {
					Double.parseDouble(dsg.label);
					bw.write("    [" + dsg.label + "");
				} catch (NumberFormatException nfe) {continue;}
				else bw.write("    ['" + DcStatChartEngine.escapeForJavaScript(dsg.label) + "'");
				bw.write(", " + dsg.value + "" + ((renderer == null) ? "" : renderer.getDataRowExtension(this, request, dsg.name)) + "]" + (git.hasNext() ? "," : "")); bw.newLine();
			}
		}
		
		else if (this.groupsByName.isEmpty()) {
			if (forMultiSeriesChart) {
				bw.write("    ['" + DcStatChartEngine.escapeForJavaScript(engine.getFieldLabel("Series")) + "'");
				for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
					DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
					bw.write(", '" + DcStatChartEngine.escapeForJavaScript(dssh.label) + "'");
				}
				bw.write((renderer == null) ? "" : renderer.getTitleRowExtension(this, request));
				bw.write("],");
				bw.newLine();
				bw.write("    [''");
				for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
					DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
					bw.write(", " + dssh.value + "");
				}
				bw.write((renderer == null) ? "" : renderer.getDataRowExtension(this, request, null));
				bw.write("]");
				bw.newLine();
			}
			else {
				bw.write("    ['" + engine.getFieldLabel("Series") + "', '" + engine.getFieldLabel((this.valueField == null) ? "Value" : this.valueField) + "'" + ((renderer == null) ? "" : renderer.getTitleRowExtension(this, request)) + "],"); bw.newLine();
				for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
					DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
					bw.write("    ['" + DcStatChartEngine.escapeForJavaScript(dssh.label) + "', " + dssh.value + "" + ((renderer == null) ? "" : renderer.getDataRowExtension(this, request, dssh.name)) + "]" + (sit.hasNext() ? "," : "")); bw.newLine();
				}
			}
		}
		
		else {
			bw.write("    ['" + DcStatChartEngine.escapeForJavaScript(engine.getFieldLabel("Data")) + "'");
			for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
				DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
				bw.write(", '" + DcStatChartEngine.escapeForJavaScript(dssh.label) + "'");
			}
			bw.write((renderer == null) ? "" : renderer.getTitleRowExtension(this, request));
			bw.write("],");
			bw.newLine();
			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
				DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
				if (areGroupLabelsNumeric) try {
					Double.parseDouble(dsg.label);
					bw.write("    [" + dsg.label + "");
				} catch (NumberFormatException nfe) {continue;}
				else bw.write("    ['" + DcStatChartEngine.escapeForJavaScript(dsg.label) + "'");
				for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
					DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
					bw.write(", " + dsg.seriesValues.getCount(dssh.name) + "");
				}
				bw.write((renderer == null) ? "" : renderer.getDataRowExtension(this, request, dsg.name));
				bw.write("]" + (git.hasNext() ? "," : ""));
				bw.newLine();
			}
		}
	}
//	void printData() {
//		if (this.seriesByName.isEmpty() && this.groupsByName.isEmpty()) {
//			System.out.println("Series\tValue");
//			System.out.println(this.valueField + "\t" + this.value);
//		}
//		else if (this.seriesByName.isEmpty()) {
//			System.out.println("Group\tValue");
//			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
//				DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
//				System.out.println(dsg.label + "\t" + dsg.value);
//			}
//		}
//		else if (this.groupsByName.isEmpty()) {
//			System.out.println("Series\tValue");
//			for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
//				DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
//				System.out.println(dssh.label + "\t" + dssh.value);
//			}
//		}
//		else {
//			System.out.print("Series");
//			for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
//				DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
//				System.out.print("\t" + dssh.label);
//			}
//			System.out.println();
//			for (Iterator git = this.groupsByName.keySet().iterator(); git.hasNext();) {
//				DcStatGroup dsg = this.getGroup(((String) git.next()), (this.seriesByName.size() != 0));
//				System.out.print(dsg.label);
//				for (Iterator sit = this.seriesByName.keySet().iterator(); sit.hasNext();) {
//					DcStatSeriesHead dssh = this.getSeriesHead((String) sit.next());
//					System.out.print("\t" + dsg.seriesValues.getCount(dssh.name));
//				}
//				System.out.println();
//			}
//		}
//	}
	
	private static class DcStatSeriesHead {
		final String name;
		String label;
		int value = 0;
		DcStatSeriesHead(String name) {
			this.name = name;
			this.label = name;
		}
		void add(int value) {
			this.value += value;
		}
	}
	
	private static class DcStatGroup {
		final String name;
		String label;
		int value = 0;
		CountingSet seriesValues = null;
		DcStatGroup(String name, boolean isMultiSeries) {
			this.name = name;
			this.label = name;
			if (isMultiSeries)
				this.seriesValues = new CountingSet();
		}
		void add(int value, String seriesName) {
			this.value += value;
			if ((this.seriesValues != null) && (seriesName != null))
				this.seriesValues.add(seriesName, value);
		}
		void addAll(DcStatGroup dsg) {
			this.value += dsg.value;
			if ((this.seriesValues != null) && (dsg.seriesValues != null))
				this.seriesValues.addAll(dsg.seriesValues);
		}
		void renameSeries(String osn, String nsn) {
			if (this.seriesValues == null)
				return;
			int sv = this.seriesValues.removeAll(osn);
			if (sv != 0)
				this.seriesValues.add(nsn, sv);
		}
	}
	
	private static final Properties numbersToMonthNames = new Properties();
	static {
		numbersToMonthNames.setProperty("1", "Jan");
		numbersToMonthNames.setProperty("01", "Jan");
		numbersToMonthNames.setProperty("2", "Feb");
		numbersToMonthNames.setProperty("02", "Feb");
		numbersToMonthNames.setProperty("3", "Mar");
		numbersToMonthNames.setProperty("03", "Mar");
		numbersToMonthNames.setProperty("4", "Apr");
		numbersToMonthNames.setProperty("04", "Apr");
		numbersToMonthNames.setProperty("5", "May");
		numbersToMonthNames.setProperty("05", "May");
		numbersToMonthNames.setProperty("6", "Jun");
		numbersToMonthNames.setProperty("06", "Jun");
		numbersToMonthNames.setProperty("7", "Jul");
		numbersToMonthNames.setProperty("07", "Jul");
		numbersToMonthNames.setProperty("8", "Aug");
		numbersToMonthNames.setProperty("08", "Aug");
		numbersToMonthNames.setProperty("9", "Sep");
		numbersToMonthNames.setProperty("09", "Sep");
		numbersToMonthNames.setProperty("10", "Oct");
		numbersToMonthNames.setProperty("11", "Nov");
		numbersToMonthNames.setProperty("12", "Dec");
	}
}