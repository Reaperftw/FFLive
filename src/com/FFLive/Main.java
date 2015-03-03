/*
 * FFLive - Java program to scrape information from http://fantasy.premierleague.com/ 
 * and display it with real time updating leagues.
 * 
 * Copyright (C) 2015  Matt Croydon
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

	//Level 5 Normal Logging
	public static ConsoleLog log = new ConsoleLog(5);

	public static void main(String[] args) {


		//TODO Via MINS Played, move people off the bench correctly


		//TODO WEBSITE: Show Previous Gameweeks?
		//TODO Clean up error handling
		//TODO Time Bug everything in BST
		//TODO Head to Head more than 1 page


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
		log.ln(4);
		log.log(4, "FFLive v3.5 - Started " + currentDate.getTime() + "\n", 0);
		log.log(4, "Loading Config...  ",0);

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
						log.log(4,"Error in Config File on line: " + lineNumber + " Skipping...  \n");
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
				log.log(1,"No Leagues in config file!\n",0);
				System.exit(7);
			}
			config.close();
			log.log(4,"Config Loaded!\n",0);
		}
		catch (FileNotFoundException e) {
			log.log(3,"Config File Not Found! Creating Sample Config File...\n",0);
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
						+ "#League IDs that will be stored and updated, separate IDs with commas 1234,4321 \r\n"
						+ "leagueid=");
				configWrite.close();
			}
			catch (IOException io) {
				log.log(1,"Cannot create config file, check you have the required privilages\n");
				System.exit(1);
			}

			log.log(4,"See Config File for Further Details...\n");
			System.exit(2);
		}
		catch (IOException io) {
			log.log(1,"Cannot open config.cfg, check that you have the required privilages\n");
			System.exit(3);
		}

		//Open - Connect to and Check DB if data stored, check in current GW and update or end current GW and prep for next GW. Else setup for first run
		MySQLConnection dbAccess = new MySQLConnection(ipaddr, username, passwd, database);

		//Set Args
		//List<String> startCmd = new ArrayList<String>();
		//startCmd.add("generboolean gen = false;
		
		boolean normal = false;
		//Read Program  Args
		if (args.length!= 0) {
			for (String test: args) {

				String parts[] = test.replaceAll("-", "").trim().split("=");
				if (parts[0].equals("logLevel")) {
					if (parts.length!=2) {
						log.log(2,"Invalid arguments given for " + test + "\n");
					}
					else {
						try {
							log.logLevel(Integer.parseInt(parts[1]));
							log.log(0,"Logging Level set to " + log.logLevel + "\n");
							log.log(3,"Note always set log level as first argument...");
							//If This is the only argument it carries on with the main program, otherwise it will cycle for the other commands
							if (args.length==1) {
								normal = true;
							}
						}
						catch (NumberFormatException n) {
							log.log(2,"Invalid number for Logging Level " + n + "\n");
						} 
					}
				}
				if (parts[0].equals("generate")) {
					if (parts.length!=2) {
						log.log(2,"Invalid arguments given for " + test + "\n");
					}
					else {

						try {
							int gw = Integer.parseInt(parts[1]);
							log.log(4,"Will generate Player List and Update for Gameweek " + parts[1] + " (Does not update old scores)\n");
							dbAccess.generatePlayerList(gw);
							dbAccess.updatePlayers(gw, true);
							dbAccess.makePlayerGraph(gw);
							dbAccess.posDifferential(gw);
						}
						catch(NumberFormatException n) {
							log.log(2,"Gameweek " + parts[1] + " is not a valid number...\n" + n + "\n");
						}
					}
				}
				else if (parts[0].equals("generateCurr")) {

					try {

						log.log(4,"Will generate Player List and Update for Current Gameweek\n");
						Fixtures now = new Fixtures();
						now.loadFixtures();
						int gw = Integer.parseInt(now.gameweek);
						if(!now.gameweekStarted()) {
							gw--;
						}
						dbAccess.generatePlayerList(gw);
						dbAccess.updatePlayers(gw, false);
						dbAccess.makePlayerGraph(gw);
						dbAccess.updateScores(gw);
						dbAccess.posDifferential(gw);
					}
					catch(NumberFormatException n) {
						log.log(2,"Gameweek is not a valid number...\n" + n + "\n");
					}

				}
				else if (parts[0].equals("post")) {
					if (parts.length!=2) {
						log.log(2,"Invalid arguments given for " + test + "\n");
					}
					else {
						log.log(4,"Marking all leagues for Post Update for Gameweek " + parts[1] + "\n");
						dbAccess.clearPostUpdate(parts[1].trim());
						normal=true;
					}
				}
				else if (parts[0].equals("teams")) {
					if (parts.length!=2) {
						log.log(2,"Invalid arguments given for " + test + "\n");
					}
					else {
						log.log(4,"Marking all leagues for Team Update for Gameweek " + parts[1] + "\n");
						dbAccess.clearTeamStatus(parts[1].trim());
						normal=true;
					}
				}
				else if (parts[0].equals("buildTeams")) {
					log.log(4,"Fetching all missing team data for all teams currently being updated\n");
					log.log(4,"If you would like to fetch all team data run --rebuildAllTeams\n");
					dbAccess.fetchMissingGWs(false, false);
					dbAccess.rebuildTransfers();
					dbAccess.rebuildGWScores();
				}
				else if (parts[0].equals("rebuildAllTeams")) {
					log.log(4,"Fetching ALL team data for all teams currently being updated\n");
					dbAccess.fetchMissingGWs(false, true);
					dbAccess.rebuildTransfers();
					dbAccess.rebuildGWScores();
				}
				else if (parts[0].equals("buildLeague")) {
					if (parts.length!=2) {
						log.log(2,"Invalid arguments given for " + test + "\n");
					}
					else {
						log.log(4,"Starting rebuild of given leagues...\n");
						log.log(4,"If rebuild process fails or has errors, run this program normally with your required league in the config, then run --buildTeams then try again.\n");
						dbAccess.contructLeagueData(parts[1].trim());

					}
				}
				else if (parts[0].equals("buildMissingLeague")) {
					if (parts.length!=2) {
						log.log(2,"Invalid arguments given for " + test + "\n");
					}
					else {
						log.log(4,"Starting rebuild of given leagues and missing teams...\n");
						dbAccess.fetchMissingGWs(false, false);
						dbAccess.rebuildTransfers();
						dbAccess.rebuildGWScores();
						dbAccess.contructLeagueData(parts[1].trim());
					}
				}
				else if(parts[0].equals("help")) {
					log.log(4,"FFLive Version 3.5 - Matt Croydon 2015, See Source on github for Licence github.com/Reaperftw/FFLive\n"
							+ "Valid Arguments"
							+ "--logLevel \tSet Logging Level(between 0-10), Must be first Arugment for effect\n"
							+ "--generate=# \tRegenerate and update player List and update scores for GW:#\n"
							+ "--generateCurr \tAs Above for Current GW\n"
							+ "--post=# \tMark GW:# for Post GW Update\n"
							+ "--teams=# \tMark GW:# for Team Update\n"
							+ "--buildTeams \tFetches All Missing Teams Compared to currently stored Teams\n"
							+ "--rebuildAllTeams \tFetches ALL TEAMS compared to currently stored Teams\n"
							+ "--buildLeague=# \tRebuilds League Scores for # - Format LeagueID1/StartingGW,LeagueID2/StartingGW\n"
							+ "--buildMissingLeague=# \tAs Above but also fetches missing teams\n"
							+ "--help \t This Text\n");
				}
				//else if (parts[0].equals("test")) {					
				//}
				else {
					log.log(2,"Invalid Argument: " + test + "... Individual Arguments should not contain spaces...\n",0);
					log.log(4, "Use --help for summary");
				}
			}
		}
		else {
			normal = true;
		}

		if(normal) {
			//Add Config Passed LeagueIDs to status DB
			dbAccess.addStatus(leagueIDs);
			boolean repeat = true;

			//Leagues live = new Leagues();
			Leagues post = new Leagues();

			boolean goLive = false;
			int gameweek = 0;
			boolean wait = false;

			while(repeat) {
				repeat = false;
				//Check Status DB for problems and where we are in the gameweek
				Map<String, List<String>> incomplete = dbAccess.statusCheck(); 

				if(!incomplete.isEmpty()) {
					dbAccess.setWebFront("index", "Checking for Updates");

					if (incomplete.containsKey("wait")) {
						for (String temp: incomplete.get("wait")) {
							if (!wait) {
								log.log(4, "Will wait for " + temp + "\n");
							}
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
								Main.log.log(6,"The next gameweek has already started for " + leagueID + " and GW:" + gw + ", Post-Gameweek Data will be out of date!\n");

								dbAccess.missedUpdateStatus(leagueID, gw);
								lateNum++;
							}
							else {
								Main.log.log(6,"Processing Post GW Updates for " + leagueID + "...  \n");
								post.addLeague(leagueID);
								postNum++;
							}
						}

						if (postNum == 1) {
							log.log(4,"Queueing " + postNum + " League for Post GW Update. \n",0);
						}
						else {
							log.log(4,"Queueing " + postNum + " Leagues for Post GW Update. \n",0);
						}

						if(lateNum == 1) {
							log.log(4, lateNum + " League was too late for Post Update\n",0);
						}
						else if (lateNum != 0) {
							log.log(4,lateNum + " Leagues were too late for Post Update\n",0);
						}
						else {
							System.out.println("");
						}
						dbAccess.postUpdate(post);
						log.log(4,"Finished Post-GW Updates!\n");
						repeat = true;
						//If a wait is issued, will cancel the wait to repeat first.
						wait=false;

					}
					if (incomplete.containsKey("teams")) {
						for (String temp: incomplete.get("teams")) {
							int gw = Integer.parseInt(temp.split(",")[1]);
							String leagueID = temp.split(",")[0];

							if (leagueIDs.contains(leagueID)) {
								log.log(6,"Loading Teams for " + leagueID + " for GW:" + gw + "...\n");
								Leagues teams = new Leagues();
								teams.addLeague(leagueID);
								dbAccess.teamUpdate(gw, teams);
							}
							else {
								log.log(4,"Removing " + leagueID + " from the Update List, as it is not in the config...  \n");
								dbAccess.removeLeague(leagueID, gw);
							}

						}
						repeat = true;

					}
					if (incomplete.containsKey("prePost")) {
						log.log(4,"Gameweek ended but waiting for post data, performing player and scores update...\n");
						int gw = Integer.parseInt(incomplete.get("prePost").get(0));
						dbAccess.generatePlayerList(gw);
						dbAccess.updatePlayers(gw, false);
						dbAccess.updateScores(gw);
					}
					if (incomplete.containsKey("live")) {
						int removed = 0;
						int addedLive = 0;

						for (String temp: incomplete.get("live")) {
							int gw = Integer.parseInt(temp.split(",")[1]);
							String leagueID = temp.split(",")[0];

							if (leagueIDs.contains(leagueID)) {
								if(!goLive) {
									log.log(4,"Preparing to live update...   \n");
								}

								Main.log.log(6,"Adding League " + leagueID + " to Live Update Queue!\n");
								//live.addLeague(leagueID);
								addedLive++;
								goLive = true;
								gameweek = gw;
							}
							else {
								Main.log.log(6,"Removing " + leagueID + " from the Update List, as it is not in the config...  \n");
								dbAccess.removeLeague(leagueID, gw);
							}
						}

						if (addedLive == 1) {
							log.log(5,addedLive + " League to be Live Updated. ",0);
						}
						else {
							log.log(5,addedLive + " Leagues to be Live Updated. ",0);
						}

						if(removed == 1) {
							log.log(5,removed + " League removed, as it is not in the config... \n",0);
						}
						else if (removed != 0) {
							log.log(5,removed + " Leagues removed, as they were not in the config...\n",0);
						}
						else {
							log.ln(5);
						}
					}

				}
				if (wait) {
					try {
						log.log(4,"Waiting for Gameweek to start, Create stop.txt to end program\r");
						dbAccess.setWebFront("index", "Waiting for GW to Start");
						Thread.sleep(120000);
					} catch (InterruptedException e) {
						System.err.println(e);
						break;
					}
					if (new File("stop.txt").exists()) {
						log.log(4,"Trigger File found, exiting...  \n");
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

				dbAccess.setWebFrontGW("index", gameweek);

				if (new File("stop.txt").exists()) {
					log.log(4,"Trigger File found before starting, exiting...  \n");
				}
				else if (now.after(endOfDay)) {
					//Don't go live, started the program late
					log.log(4,"FFLive started after end of today's matches, please set to set to start after 8am...  \n");
				}
				else if (now.before(early)) {
					log.log(4,"FFLive starting too early, set to start after 8am..  \n");
				}
				else {


					dbAccess.setWebFront("index", "Live Updating");
					log.log(4,"Starting Live Update for all Selected Leagues...  \n");
					dbAccess.generatePlayerList(gameweek);
					dbAccess.updatePlayers(gameweek, false);
					dbAccess.makePlayerGraph(gameweek);

					log.log(4,"Live Running Until " + endOfDay.getTime() + ", or End program by creating stop.txt in this directory... \n");

					while (Calendar.getInstance().before(endOfDay)) {
						dbAccess.updatePlayers(gameweek, false);
						log.ln(5);
						dbAccess.updateScores(gameweek);
						if (new File("stop.txt").exists()) {
							log.log(4,"Trigger File found, exiting...  \n");
							break;
						}
						try {
							Thread.sleep(90000);
						} 
						catch (InterruptedException e) {
							log.log(2,e.toString());
							break;	
						}
						if (new File("stop.txt").exists()) {
							log.log(4,"Trigger File found, exiting...  \n");
							break;
						}
					}
				}
			}
			else {
				log.log(4,"All Updates Complete and currently not time to go live!\n");
			}

			dbAccess.setWebFront("index", "Up To Date");
		}

		dbAccess.closeConnections();
		log.log(4,"Finished running FFLive!\n");

	}
}

