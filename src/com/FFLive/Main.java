/*
 * FFLive - Java program to scrape information from http://fantasy.premierleague.com/ 
 * and display it with real time updating leagues.
 * 
 * Copyright (C) 2014  Matt Croydon
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.FFLive;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Main {


	public static void main(String[] args) {

		//TODO Via MINS Played, move people off the bench correctly
		//TODO Transfer deductions
		//TODO Add Ability to collect data from previous gameweeks for new league additions
		//TODO WEBSITE: AllTeams, Stats, Show Previous Gameweeks?, Captain, Hover Breakdown, Injury Issues
		//TODO Clean up error handling
		//TODO Time Bug everything in BST
		//TODO Stats and Graphs	

		//Config File Args
		String ipaddr = "localhost:3306";
		String username = "";
		String passwd = "";
		String database = "FFLive";
		//String[] leagueIDs = null;
		List<String> leagueIDs = new ArrayList<String>();
		//String gameweek = null;

		//Initial open
		Calendar currentDate = Calendar.getInstance();
		System.out.println("");
		System.out.println("FFLive v3.1.1 - Started " + currentDate.getTime());
		System.out.print("Loading Config...  ");

		//Load Config File
		try {
			BufferedReader config = new BufferedReader(new FileReader(new File("config.cfg")));
			String line = config.readLine();
			int lineNumber = 1;
			//Map to load config into
			HashMap<String, String> arguments = new HashMap<String,String>();
			while (line != null) {
				//Ignore # for comment
				line = line.split("#",2)[0].replaceAll(" ", "");

				if (line.equals("")) {
					line = config.readLine();
					lineNumber ++;
				}
				else {
					String[] splitArgs = line.split("=");
					if (splitArgs.length == 2) {
						arguments.put(splitArgs[0].trim(), splitArgs[1].trim());
					}
					else {
						System.out.println("Error in Config File on line: " + lineNumber + " Skipping...");
					}
					line = config.readLine();
					lineNumber ++;
				}
			}
			if (arguments.containsKey("mysqlip")) {
				ipaddr = arguments.get("mysqlip");
			}
			if (arguments.containsKey("mysqluser")) {
				username = arguments.get("mysqluser");
			}
			if (arguments.containsKey("mysqlpw")) {
				passwd = arguments.get("mysqlpw");
			}
			if (arguments.containsKey("database")) {
				database = arguments.get("database");
			}
			if (arguments.containsKey("leagueid")) {
				for (String ID: arguments.get("leagueid").split(",")) {
					leagueIDs.add(ID);
				}
			}
			else {
				System.err.println("No Leagues in config file");
				System.exit(7);
			}
			config.close();
			System.out.println("Config Loaded!");
		}
		catch (FileNotFoundException e) {
			System.out.println("Config File Not Found! Creating Sample Config File...");
			//Creates Sample Config File
			try {
				FileWriter configWrite = new FileWriter(new File("config.cfg"));
				configWrite.write("#FFLive Config File! \r\n"
						+ "# is treated as a comment\r\n"
						+ "\r\n"
						+ "#Configure MySQL Server Address:Port and Login Details (Can be left blank), and the name of the database you want to make/use.\r\n"
						+ "#Make sure your defined user has permissions to at least USE,SELECT,UPDATE,INSERT.\r\n"
						+ "#Default Server is localhost:3306 with no username or password\r\n"
						+ "mysqlip=\r\n"
						+ "mysqluser=\r\n"
						+ "mysqlpw=\r\n"
						+ "database=\r\n"
						+ "\r\n"
						+ "#League IDs that will be stored and updated, separate IDs with commas\r\n"
						+ "leagueid=");
				configWrite.close();
			}
			catch (IOException io) {
				System.err.println("Cannot create config file, check you have the required privilages");
				System.exit(1);
			}

			System.out.println("See Config File for Further Details...");
			System.exit(2);
		}
		catch (IOException io) {
			System.err.println("Cannot open config.cfg, check that you have the required privilages");
			System.exit(3);
		}

		//Open - Connect to and Check DB if data stored, check in current GW and update or end current GW and prep for next GW. Else setup for first run
		MySQLConnection dbAccess = new MySQLConnection(ipaddr, username, passwd, database);

		//Set Args
		//List<String> startCmd = new ArrayList<String>();
		//startCmd.add("generboolean gen = false;

		//Read Program  Args
		if (args.length!= 0) {
			for (String test: args) {
								
				String parts[] = test.replaceAll("-", "").trim().split("=");

				if (parts[0].equals("generate")) {
					if (parts.length!=2) {
						System.out.println("Invalid arguments given for " + test);
					}
					else {
						System.out.println("Will generate Player List and Update for Gameweek " + parts[1]);
						dbAccess.generatePlayerList(parts[1]);
						dbAccess.updatePlayers(parts[1]);
						dbAccess.posDifferential(parts[1]);
					}
				}
				if (parts[0].equals("post")) {
					if (parts.length!=2) {
						System.out.println("Invalid arguments given for " + test);
					}
					else {
						System.out.println("Marking all leagues for Post Update for Gameweek " + parts[1]);
						dbAccess.clearPostUpdate(parts[1].trim());
						System.out.println("Restart without arguments...");
					}
				}
				else {
					System.out.println("Invalid Argument: " + test);
					System.out.println("Valid Arguments: --generate");
				}
			}
		}

		else {
			//Add Config Passed LeagueIDs to status DB
			dbAccess.addStatus(leagueIDs);
			boolean repeat = true;

			//Leagues live = new Leagues();
			Leagues post = new Leagues();

			boolean goLive = false;
			String gameweek = "0";
			boolean wait = false;

			while(repeat) {
				repeat = false;
				//Check Status DB for problems and where we are in the gameweek
				Map<String, List<String>> incomplete = dbAccess.statusCheck(); 

				if(!incomplete.isEmpty()) {
					dbAccess.setWebFront("index", "Checking for Updates");

					if (incomplete.containsKey("wait")) {
						for (String temp: incomplete.get("wait")) {
							System.out.println("Will wait for " + temp);
							repeat = true;
							wait = true;
						}
					}

					if (incomplete.containsKey("post")) {

						int postNum = 0;
						int lateNum = 0;
						for (String temp :incomplete.get("post")) {
							String gw = temp.split(",")[1];
							String leagueID = temp.split(",")[0];
							
							if (dbAccess.nextGWStarted(gw)) {
								//TODO Add Debug text, leagueID and GW
								//System.out.println("The next gameweek has already started for " + leagueID + " and GW:" + gw + ", Post-Gameweek Data will be out of date!");
								
								dbAccess.missedUpdateStatus(leagueID, gw);
								lateNum++;
							}
							else {
								//TODO
								//System.out.print("Processing Post GW Updates for " + leagueID + "...  ");
								post.addLeague(leagueID);
								postNum++;
							}
						}
						
						if (postNum == 1) {
							System.out.print("Queueing " + postNum + " League for Post GW Update. ");
						}
						else {
							System.out.print("Queueing " + postNum + " Leagues for Post GW Update. ");
						}
						
						if(lateNum == 1) {
							System.out.println(lateNum + " League was too late for Post Update");
						}
						else if (lateNum != 0) {
							System.out.println(lateNum + " Leagues were too late for Post Update");
						}
						else {
							System.out.println("");
						}
						dbAccess.postUpdate(post);
						System.out.println("Finished Post-GW Updates!");
						repeat = true;
						//If a wait is issued, will cancel the wait to repeat first.
						wait=false;

					}
					if (incomplete.containsKey("teams")) {
						for (String temp: incomplete.get("teams")) {
							String gw = temp.split(",")[1];
							String leagueID = temp.split(",")[0];

							if (leagueIDs.contains(leagueID)) {
								//TODO Debug Text
								System.out.println("Loading Teams for " + leagueID + " for GW:" + gw + "...");
								Leagues teams = new Leagues();
								teams.addLeague(leagueID);
								dbAccess.teamUpdate(gw, teams);
							}
							else {
								//TODO Debug Text
								System.out.println("Removing " + leagueID + " from the Update List, as it is not in the config...  ");
								dbAccess.removeLeague(leagueID, gw);
							}

						}
						repeat = true;

					}
					if (incomplete.containsKey("live")) {
						int removed = 0;
						int addedLive = 0;
						
						for (String temp: incomplete.get("live")) {
							String gw = temp.split(",")[1];
							String leagueID = temp.split(",")[0];
							
							if (leagueIDs.contains(leagueID)) {
								//TODO This should debug log only
								if(!goLive) {
									System.out.println("Preparing to live update...   ");
								}
								//TODO Debug Text
								//System.out.println("Adding League " + leagueID + " to Live Update Queue!");
								//live.addLeague(leagueID);
								addedLive++;
								goLive = true;
								gameweek = gw;
							}
							else {
								//TODO Debug Text
								//System.out.println("Removing " + leagueID + " from the Update List, as it is not in the config...  ");
								dbAccess.removeLeague(leagueID, gw);
							}
						}
						
						if (addedLive == 1) {
							System.out.print(addedLive + " League to be Live Updated. ");
						}
						else {
							System.out.print(addedLive + " Leagues to be Live Updated. ");
						}
						
						if(removed == 1) {
							System.out.println(removed + " League removed, as it is not in the config... ");
						}
						else if (removed != 0) {
							System.out.println(removed + " Leagues removed, as they were not in the config...");
						}
						else {
							System.out.println("");
						}
					}

				}
				if (wait) {
					try {
						System.out.print("Waiting for Gameweek to start, Create stop.txt to end program\r");
						dbAccess.setWebFront("index", "Waiting for GW to Start");
						Thread.sleep(120000);
					} catch (InterruptedException e) {
						System.err.println(e);
						break;
					}
					if (new File("stop.txt").exists()) {
						System.out.println("Trigger File found, exiting...  ");
						break;
					}
				}

			}





			if (goLive) {
				Calendar endOfDay = Calendar.getInstance();
				Calendar early = Calendar.getInstance();
				Calendar now = Calendar.getInstance();
				endOfDay.set(Calendar.HOUR_OF_DAY, 22);
				endOfDay.set(Calendar.MINUTE, 0);
				early.set(Calendar.HOUR_OF_DAY, 8);
				early.set(Calendar.MINUTE, 0);

				dbAccess.setWebFrontGW("index", Integer.parseInt(gameweek));

				if (new File("stop.txt").exists()) {
					System.out.println("Trigger File found before starting, exiting...  ");
				}
				else if (now.after(endOfDay)) {
					//Don't go live, started the program late
					System.out.print("FFLive started after end of today's matches, please set to set to start after 8am...  ");
				}
				else if (now.before(early)) {
					System.out.println("FFLive starting too early, set to start after 8am..  ");
				}
				else {


					dbAccess.setWebFront("index", "Live Updating");
					System.out.println("Starting Live Update for all Selected Leagues...  ");
					dbAccess.generatePlayerList(gameweek);
					
					System.out.println("Live Running Until " + endOfDay.getTime() + ", or End program by creating stop.txt in this directory... ");
					
					while (Calendar.getInstance().before(endOfDay)) {
						dbAccess.updatePlayers(gameweek);
						System.out.println("");
						dbAccess.updateScores(gameweek);
						if (new File("stop.txt").exists()) {
							System.out.println("Trigger File found, exiting...  ");
							break;
						}
						try {
							Thread.sleep(120000);
						} 
						catch (InterruptedException e) {
							System.err.println(e);
							break;	
						}
						if (new File("stop.txt").exists()) {
							System.out.println("Trigger File found, exiting...  ");
							break;
						}
					}
				}
			}
			else {
				System.out.println("All Updates Complete and currently not time to go live!");
			}

			dbAccess.setWebFront("index", "Up To Date");
		}
		/*ClassicLeague test = new ClassicLeague(27611);
		H2HLeague test2 = new H2HLeague(28716);
		test.loadLeague();
		test2.loadH2HLeague();
		dbAccess.createGraphs(test);
		dbAccess.createGraphs(test2);*/
		
		dbAccess.closeConnections();
		System.out.println("Finished running FFLive!");

	}
}

