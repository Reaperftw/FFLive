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
import java.util.Map.Entry;


public class Main {


	public static void main(String[] args) {
		
		//TODO Via MINS Played, move peope off the bench correctly
		//TODO Clean up error handling
		
		//Program Args
		String ipaddr = "localhost:3306";
		String username = "";
		String passwd = "";
		//String[] leagueIDs = null;
		ArrayList<String> leagueIDs = new ArrayList<String>();
		//String gameweek = null;

		//Initial open
		Calendar currentDate = Calendar.getInstance();
		System.out.println("");
		System.out.println("FFLive v3.0 - Started " + currentDate.getTime());
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
						+ "#Configure MySQL Server Address:Port and Login Details (Can be left blank)\r\n"
						+ "mysqlip=\r\n"
						+ "mysqluser=\r\n"
						+ "mysqlpw=\r\n"
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
		MySQLConnection dbAccess = new MySQLConnection(ipaddr, username, passwd);

		//Add Config Passed LeagueIDs to status DB
		//dbAccess.addStatus(leagueIDs);
		boolean repeat = true;

		Leagues live = new Leagues();
		Leagues post = new Leagues();
		//boolean needToPost = false;
		boolean goLive = false;
		String gameweek = "0";


		while(repeat) {
			repeat = false;
			boolean wait = false;
			//Check Status DB for problems and where we are in the gameweek
			HashMap<String, String> incomplete = dbAccess.statusCheck(); 



			if(!incomplete.isEmpty()) {
				for(Entry<String,String> entry : incomplete.entrySet()) {
					String type = entry.getValue();
					

					if (type.equals("post")) {
						
						String gw = entry.getKey().split(",")[1];
						String leagueID = entry.getKey().split(",")[0];
						
						//Check if next game week has started otherwise post data will be useless...
						if (dbAccess.nextGWStarted(gw)) {
							System.out.println("The next gameweek has alraedy started, Post-Gameweek Data will be out of date!");
							dbAccess.missedUpdateStatus(leagueID, gw);
						}
						else {
							System.out.print("Processing Post GW Updates for " + leagueID + "...  ");
							post.addLeague(leagueID);
							post.postUpdate(gw, dbAccess);
							System.out.println("Done!");
							
						}
					}
					else if (type.equals("teams")) {
						
						String gw = entry.getKey().split(",")[1];
						String leagueID = entry.getKey().split(",")[0];
						
						//Check this team is in the config file, otherwise it could be discarded
						if (leagueIDs.contains(leagueID)) {
							System.out.println("Loading Teams for " + leagueID + " for GW:" + gw + "...");
							Leagues teams = new Leagues();
							teams.addLeague(leagueID);
							teams.teamUpdate(gw, dbAccess);
						}
						else {
							System.out.println("Removing " + leagueID + " from the Update List, as it is not in the config...  ");
							dbAccess.removeLeague(leagueID, gw);
						}
					}
					else if (type.equals("live")) {
						
						String gw = entry.getKey().split(",")[1];
						String leagueID = entry.getKey().split(",")[0];
						
						if (leagueIDs.contains(leagueID)) {
							System.out.println("Adding League " + leagueID + " to Live Update Queue!");
							live.addLeague(leagueID);
							goLive = true;
							gameweek = gw;
						}
						else {
							System.out.println("Removing " + leagueID + " from the Update List, as it is not in the config...  ");
							dbAccess.removeLeague(leagueID, gw);
						}


					}
					else if (type.equals("wait")) {
						System.out.print("Will wait for " + entry.getKey() + ", End program by creating stop.txt in this directory...  ");
						repeat = true;
						wait = true;
					}
					else {						
						String leagueID = entry.getKey().split(",")[0];
						
						System.err.println("Something has gone wrong processing League ID " + leagueID + ", skipping...   ");
					}
				}
			}
			
			if (wait) {
				try {
					Thread.sleep(120000);
				} catch (InterruptedException e) {
					System.err.println(e);
					break;
				}
			}
		}


		if (goLive) {
			if (new File("stop.txt").exists()) {
				System.out.print("Trigger File found, exiting...  ");
			}
			else {
				System.out.print("Starting Live Update for all Selected Leagues...  ");
				Calendar endOfDay = Calendar.getInstance();
				endOfDay.set(Calendar.HOUR_OF_DAY, 22);
				endOfDay.set(Calendar.MINUTE, 0);
				System.out.println("Live Running Until" + endOfDay.getTime() + ", or End program by creating stop.txt in this directory... ");
				while (Calendar.getInstance().before(endOfDay)) {
					dbAccess.generatePlayerList(gameweek);
					dbAccess.updatePlayers(gameweek);
					dbAccess.updateScores(gameweek);
					try {
						Thread.sleep(120000);
					} 
					catch (InterruptedException e) {
						System.err.println(e);
						break;	
					}
					if (new File("stop.txt").exists()) {
						System.out.print("Trigger File found, exiting...  ");
						break;
					}
				}
			}
		}

		/*
		ClassicLeague league = new ClassicLeague(27611);
		league.loadLeague();
		league.gameWeek = "1";
		league.addManager("1234", "1", "1000", "2");
		league.addManager("4321", "1", "2000", "2");
		dbAccess.storeLeague(league);
		Team test = new Team("1234", "1", "1000", "2");
		test.goalkeeper[0] = "130";
		test.defenders[0] = "80";
		test.defenders[1] = "341"; 
		test.defenders[2] = "262";
		test.defenders[3] = "401";
		test.midfield[0] = "246";
		test.midfield[1] = "226";
		test.midfield[2] = "119";
		test.midfield[3] = "331";
		test.forwards[0] = "25";
		test.forwards[1] = "229";
		test.bench[0] = "422";
		test.bench[1] = "162";
		test.bench[2] = "247";
		test.bench[3] = "227";
		test.captains[0] = "229";
		test.captains[1] = "119";
		Team test2 = new Team("4321", "1", "2000", "1");
		test2.goalkeeper[0] = "130";
		test2.defenders[0] = "134";
		test2.defenders[1] = "341"; 
		test2.defenders[2] = "6";
		test2.defenders[3] = "401";
		test2.midfield[0] = "246";
		test2.midfield[1] = "244";
		test2.midfield[2] = "119";
		test2.midfield[3] = "21";
		test2.forwards[0] = "254";
		test2.forwards[1] = "227";
		test2.bench[0] = "422";
		test2.bench[1] = "162";
		test2.bench[2] = "247";
		test2.bench[3] = "227";
		test2.captains[0] = "134";
		test2.captains[1] = "246";
		//dbAccess.storeLeagueData(league);
		dbAccess.storeTeamData(test);
		dbAccess.storeTeamData(test2);
		dbAccess.generatePlayerList("1");
		dbAccess.updatePlayers(league.gameWeek);
		 */


		dbAccess.closeConnections();
		System.out.println("Finished running Live Leagues!");

		/*System.out.print("Arguments Provided:");
		for (String temp: args) {
			System.out.print(" " + temp);
		}
		System.out.println();*/
		/*
		MySQLConnection dbAccess = new MySQLConnection();


		//dbAccess.printLeague(league);
		//league.loadTeams();

		 */

		//Player player = new Player("130");
		//player.getPlayer();

		/*if(args.length == 1 | args.length == 2) {
			try {
				int leagueID = Integer.parseInt(args[0]);
				if (args.length == 1) {
					ClassicLeague league = new ClassicLeague(leagueID);
					league.loadLeague();
					league.loadTeams();
					league.updatePreviousWeek();
					league.writeLeague();
					league.liveScores();
				}
				else {
					if (args[1].equals("pre")) {
						ClassicLeague league = new ClassicLeague(leagueID);
						league.loadLeague();
						league.loadTeams();
						league.previousWeek();
					}
					else if (args[1].equals("simp")) {
						ClassicLeague league = new ClassicLeague(leagueID);
						league.loadLeague();
						league.loadTeams();
						league.writeLeague();
						league.liveScores();
					}
					else if (args[1].equals("simphtml")) {
						ClassicLeague league = new ClassicLeague(leagueID);
						league.loadLeague();
						league.loadTeams();
						league.writeLeaguehtml();
						league.liveScoreshtml();
					}
					else if (args[1].equals("html")) {
						ClassicLeague league = new ClassicLeague(leagueID);
						league.loadLeague();
						league.loadTeams();
						league.updatePreviousWeek();
						league.liveScoreshtml();
						league.writeLeaguehtml();
					}
					else if (args[1].equals("stats")) {
						ClassicLeague league = new ClassicLeague(leagueID);
						league.loadLeague();
						GWStat gwStats = league.collectGameWeekHistory();
						gwStats.makeCumulativeGWTable();
						gwStats.makeGWTable();
						gwStats.makeGameWeekPositionTable();
						gwStats.makePositionTable();
					}
					else if (args[1].equals("statshtml")) {
						ClassicLeague league = new ClassicLeague(leagueID);
						league.loadLeague();
						GWStat gwStats = league.collectGameWeekHistory();
						gwStats.makeCumulativeGWTablehtml();
						gwStats.makeGWTablehtml();
						gwStats.makeGameWeekPositionTablehtml();
						gwStats.makePositionTablehtml();

					}
					else if (args[1].equals("LiveH2H")){
						H2HLeague league = new H2HLeague(leagueID);
						league.loadH2HLeague();
						league.loadTeams();
						league.makeLiveH2HTable();	
						league.writeAllH2HScores();
					}
					else if (args[1].equals("LiveH2Hhtml")){
						H2HLeague league = new H2HLeague(leagueID);
						league.loadH2HLeague();
						league.loadTeams();
						league.makeLiveH2HTable();	
						league.writeAllH2HScoreshtml();
					}
					else if (args[1].equals("H2H")){
						H2HLeague league = new H2HLeague(leagueID);
						league.loadH2HLeague();
						league.loadTeams();
						league.makeCurrentH2HTable();	
						league.writeAllH2HScores();
					}
					else if (args[1].equals("H2Hhtml")){
						H2HLeague league = new H2HLeague(leagueID);
						league.loadH2HLeague();
						league.loadTeams();
						league.makeCurrentH2HTable();	
						league.writeAllH2HScoreshtml();
					}
					else if (args[1].equals("playerStats")) {
						ClassicLeague league = new ClassicLeague(leagueID);
						league.loadLeague();
						league.loadTeams();
						PlayerStat allPlayers = new PlayerStat(league);
						allPlayers.writeList();
						allPlayers.makeCountSortedList();
						allPlayers.writeSortedList();
					}
					else if (args[1].equals("playerStatshtml")) {
						ClassicLeague league = new ClassicLeague(leagueID);
						league.loadLeague();
						league.loadTeams();
						PlayerStat allPlayers = new PlayerStat(league);
						allPlayers.makeCountSortedList();
						allPlayers.writeSortedList();
						allPlayers.writeSortedListhtml();
					}
					else if (args[1].equals("preH2H")) {
						H2HLeague league = new H2HLeague(leagueID);
						league.saveCurrentH2HTable();
					}
					else {
						System.err.println("Invalid Second Argument Given, please use simp, pre, html, simphtml, stats, statshtml, H2H, H2Hhtml, LiveH2Hhtml, LiveH2H, playerStats, playerStatshtml or preH2H");
						System.exit(8);
					}
				}
				System.out.println("FFLive Completed!");
				System.out.println("");
			}
			catch (NumberFormatException e) {
				e.printStackTrace();
				System.err.println("Error: The First Argument is not a League Number");
				System.out.println("First Argument Must be League ID Number in an Integer Format eg. 123456");
				System.exit(7);
			}
		}
		else {
			System.out.println("Please Pass Arguments Separated by Spaces");
			System.out.println("First Argument is Always a League Number");
			System.out.println("Second Argument Optional: No Argument Adds Current Scores to Previous Week");
			System.out.println("simp = Just Takes This Weeks Scores, pre = Stores Current Scores");
			System.out.println("html = Updates all scores and makes html files, simphtml = Processes simp command and makes html files");
			System.out.println("stats = Produces a secection of Gameweek Stats, statshtml = Makes html files of stats");
			System.out.println("H2H = For a given H2H League, Produces Current Fixtures and Table, H2Hhtml = Same as H2H with html output, preH2H = Saves Current H2H Scores");
			System.out.println("LiveH2H = For a given H2H League, Produces Live Fixtures and Table, LiveH2Hhtml = Same as H2H with html output");
			System.out.println("playerStats = For a given H2H League, Produces Live Fixtures and Table, playerStatshtml = Same as playerStats with html output");
		}*/
	}
}

