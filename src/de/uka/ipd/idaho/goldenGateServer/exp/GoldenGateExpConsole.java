///*
// * Copyright (c) 2006-, IPD Boehm, Universitaet Karlsruhe (TH) / KIT, by Guido Sautter
// * All rights reserved.
// *
// * Redistribution and use in source and binary forms, with or without
// * modification, are permitted provided that the following conditions are met:
// *
// *     * Redistributions of source code must retain the above copyright
// *       notice, this list of conditions and the following disclaimer.
// *     * Redistributions in binary form must reproduce the above copyright
// *       notice, this list of conditions and the following disclaimer in the
// *       documentation and/or other materials provided with the distribution.
// *     * Neither the name of the Universität Karlsruhe (TH) / KIT nor the
// *       names of its contributors may be used to endorse or promote products
// *       derived from this software without specific prior written permission.
// *
// * THIS SOFTWARE IS PROVIDED BY UNIVERSITÄT KARLSRUHE (TH) / KIT AND CONTRIBUTORS 
// * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
// * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR ANY
// * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// */
//package de.uka.ipd.idaho.goldenGateServer.exp;
//
//import java.util.ArrayList;
//
//import de.uka.ipd.idaho.goldenGateServer.AbstractGoldenGateServerComponent;
//
///**
// * Central controller component for GoldenGATE EXP sub classes.
// * 
// * @author sautter
// */
//public class GoldenGateExpConsole extends AbstractGoldenGateServerComponent {
//	
//	/**
//	 * Constructor passing 'EXP' as the letter code to the super constructor.
//	 * @param letterCode
//	 */
//	public GoldenGateExpConsole() {
//		super("EXP");
//	}
//	
//	private static final String PAUSE_EXPORTERS_COMMAND = "pause";
//	private static final String UNPAUSE_EXPORTERS_COMMAND = "unpause";
//	private static final String LIST_EXPORTERS_COMMAND = "list";
//	
//	/* (non-Javadoc)
//	 * @see de.uka.ipd.idaho.goldenGateServer.GoldenGateServerComponent#getActions()
//	 */
//	public ComponentAction[] getActions() {
//		ArrayList cal = new ArrayList();
//		ComponentAction ca;
//		
//		//	pause all exporters
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return PAUSE_EXPORTERS_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = {
//						PAUSE_EXPORTERS_COMMAND,
//						"Pause all installed exporters."
//					};
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length == 0) {
//					if (GoldenGateEXP.setExpPause(true))
//						System.out.println("Exporter instances set to pause.");
//					else System.out.println("Exporter instances already pausing.");
//				}
//				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
//			}
//		};
//		cal.add(ca);
//		
//		//	un-pause all exporters
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return UNPAUSE_EXPORTERS_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = {
//						UNPAUSE_EXPORTERS_COMMAND,
//						"Un-pause all installed exporters."
//					};
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length == 0) {
//					if (GoldenGateEXP.setExpPause(false))
//						System.out.println("Exporter instances un-paused.");
//					else System.out.println("Exporter instances not pausing.");
//				}
//				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
//			}
//		};
//		cal.add(ca);
//		
//		//	un-pause all exporters
//		ca = new ComponentActionConsole() {
//			public String getActionCommand() {
//				return LIST_EXPORTERS_COMMAND;
//			}
//			public String[] getExplanation() {
//				String[] explanation = {
//						LIST_EXPORTERS_COMMAND,
//						"List all installed exporters."
//					};
//				return explanation;
//			}
//			public void performActionConsole(String[] arguments) {
//				if (arguments.length == 0) {
//					System.out.println("These are the exporter instances currently installed:");
//					GoldenGateEXP.listInstances(" - ");
//				}
//				else System.out.println(" Invalid arguments for '" + this.getActionCommand() + "', specify no arguments.");
//			}
//		};
//		cal.add(ca);
//		
//		return ((ComponentAction[]) cal.toArray(new ComponentAction[cal.size()]));
//	}
//}