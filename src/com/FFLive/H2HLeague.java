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

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class H2HLeague {

	public Map<String, String> fixtureMap = null;
	public Map<String, Team> managerMap = null;
	//public TreeMap<Integer,String[][]> liveH2HTable = null;
	public Map<Integer,Team[]> liveH2HTeamTable = null;
	public Map<String,String[]> allScoreMap = null;
	public String[][] liveH2HFixtures = null;
	private int timeout = 0;
	private static final int maxH2HManagers = 50;
	public int leagueID;
	public String leagueName;
	public String gameweek;
	//private int retryPoint = 0;

	public H2HLeague(int league) {
		leagueID = league;
		leagueName = null;
		managerMap = new HashMap<String, Team> (maxH2HManagers);
		fixtureMap = new TreeMap<String,String> ();
		//liveH2HTable = new TreeMap<Integer,String[][]>();
		allScoreMap = new TreeMap<String,String[]>();
		liveH2HTeamTable = new TreeMap<Integer,Team[]>();
		gameweek = "0";
	}


	public void loadH2HLeague() {
		System.out.print("Loading Head-to-Head League: " + leagueID + "...   ");
		try {
			String URL = ("http://fantasy.premierleague.com/my-leagues/" + leagueID + "/standings/");
			Document leaguePage = Jsoup.connect(URL).get();


			//Gets the League Table of the HTML to be manipulated
			Elements leagueTable = leaguePage.select("table.ismTable.ismH2HStandingsTable");
			//Checks for H2H League
			if (leagueTable.isEmpty()) {
				System.err.println("");
				System.err.println("This League either contains no teams or the season has not yet begun!");

			}
			else {
				//Pulls the links for the team names out of the table
				Elements managerID = leagueTable.select("a[href]");
				//Takes the Table Sections for Scores
				Elements tableRows = leagueTable.select("tr");
				//Gets League Name Section
				Elements lName = leaguePage.getElementsByClass("ismTabHeading");

				leagueName = lName.text();
				//TODO Debug Text
				//System.out.println("Loaded League: " + lName.text());

				//Works Out Gameweek
				gameweek = managerID.attr("href").split("/")[4];

				//TODO Debug Text
				//System.out.println("The Current Gameweek is " + gameweek);
				//System.out.print("Loading Managers...  ");

				//To Save Reloading the Page later, passes forward the league score and ManagerIDs


				String managerIDTemp = null;

				for (Element tableRow : tableRows) {
					Elements tableSections = tableRow.select("td");
					String[] tempScores = new String [6];
					int counter = 0;

					for (Element el : tableSections) {
						counter++;
						if (counter%8 == 2) {
							//Position
							tempScores[0] = el.text().replaceAll("\\D+","");
						}
						if (counter%8 == 3) {
							//Team ID
							if (el.text().equals("Average")) {
								managerIDTemp="0";
							}
							else {
								//ManagerIds in the link in the 3rd Column (of 6)
								String[] linkSegments = el.select("a").attr("href").split("/");
								managerIDTemp = linkSegments[2];
							}
						}
						if (counter%8 == 4) {
							//Wins
							tempScores[1] = el.text().replaceAll("\\D+","");
						}
						if (counter%8 == 5) {
							//Draws
							tempScores[2] = el.text().replaceAll("\\D+","");
						}
						if (counter%8 == 6) {
							//Losses
							tempScores[3] = el.text().replaceAll("\\D+","");
						}
						if (counter%8 == 7) {
							//LP
							tempScores[4] = el.text().replaceAll("\\D+","");
						}
						if (counter%8 == 0) {
							//Score and add to Map
							tempScores[5] = el.text().replaceAll("\\D+","");
							allScoreMap.put(managerIDTemp, tempScores);
						}
					}
				}

				for (Entry<String,String[]> entry: allScoreMap.entrySet()) {
					addH2HManager(entry.getKey(), gameweek, entry.getValue());
				}

				//TODO Debug Text?
				System.out.print("Loading Fixtures...  ");

				Element fixtureTable = leaguePage.select("table.ismTable.ismH2HFixTable").first();
				Elements fixtures = fixtureTable.select("tr");
				if(fixtures.size() > 25) {
					System.out.println("");
					System.err.print("This Program Does Not Currently Support Leagues Longer Than 1 Page...");
					System.exit(51);
				}
				for (Element el: fixtures) {
					String homeID = el.select("td.ismHome").select("a").attr("href");
					String awayID = el.select("td.ismAway").select("a").attr("href");

					if(homeID.equals("")) {
						homeID="0";
					}
					else {
						homeID = homeID.split("/")[2];
					}

					if(awayID.equals("")) {
						awayID="0";
					}
					else {
						awayID = awayID.split("/")[2];
					}
					fixtureMap.put(homeID, awayID);
				}
				//TODO Debug Text
				//System.out.println("Done!");

			}
		}
		catch (ConnectException c) {
			if (timeoutCheck() > 10) {
				System.err.println("Too Many Timeouts... Skipping");
			}
			System.out.println("Timeout Connecting. Retrying...");
			loadH2HLeague();
		}
		catch (SocketTimeoutException e) {
			if (timeoutCheck() > 10) {
				System.err.println("Too Many Timeouts... Skipping");
			}
			System.out.println("Timeout Connecting. Retrying...");
			loadH2HLeague();
		}
		catch (NoRouteToHostException h) {
			System.err.println("No Connection... Skipping");
		}
		catch (UnknownHostException g) {
			System.err.println("No Connection... Skipping");
		}
		catch (IOException f) {
			System.err.println("--In loadH2HLeague: " + f);
		}

	}

	public void addH2HManager(String manID, String GW, String[] scores) {
		//Adds a map of managerIDs to teams (which store the managerID anyway, but ID needed later for sorting)Max teams maybe expanded
		//Adds the managers using addManager Method, passing on the League Score Also
		//Passes all league details within the Team constructor
			managerMap.put(manID, new Team(manID, GW, scores[4], scores[1], scores[2], scores[3], scores[5], scores[0]));
		
	}

	public void loadTeams() {
		//Runs through all the entries to load the Teams
		int n = 1;
		for (Entry<String,Team> entry: managerMap.entrySet()) {
			System.out.print("Loading Teams for '" + leagueName + "'... Team " + n++ + "/" + managerMap.size() + "\r");
			entry.getValue().getTeam();
		}
		System.out.println("");
	}

	/*
	public void makeCurrentH2HTable() {
		//Just Reads All The Data Off the Page and Saves it to be printed
		int fixtureLength = fixtureMap.size();
		if(fixtureLength > 25) {
			System.out.println("Fixtures will not have complete data, therefore table will not be up to date");
			System.out.println("This Program Does Not Currently Support Leagues Longer Than 1 Page (50 Teams)");
			System.exit(203);
		}
		liveH2HFixtures = new String[fixtureLength][5];
		//Fill Centre of table with "-" and fills the scores with L for sake of argument..
		for(int i = 0; i < fixtureLength; i++){
			liveH2HFixtures[i][2] = "-";
		}
		int counter = 0;
		for(Entry<String,String> Entry :fixtureMap.entrySet()) {
			String homeTeam = Entry.getKey();
			String awayTeam = Entry.getValue();
			liveH2HFixtures[counter][0] = managerMap.get(homeTeam).teamName;
			liveH2HFixtures[counter][1] = Integer.toString(managerMap.get(homeTeam).gameWeekScore);
			liveH2HFixtures[counter][3] = Integer.toString(managerMap.get(awayTeam).gameWeekScore);
			liveH2HFixtures[counter][4] = managerMap.get(awayTeam).teamName;
			counter++;
		}



		for(Entry<String,Team> manager : managerMap.entrySet()) {
			Integer newScore = manager.getValue().h2hScore;			
			if (liveH2HTeamTable.containsKey(newScore)) {
				int oldLength = liveH2HTeamTable.get(newScore).length;
				Team[] addedData = Arrays.copyOf(liveH2HTeamTable.get(newScore), oldLength+1);
				addedData[oldLength] = manager.getValue();
				liveH2HTeamTable.put(newScore, addedData);
			}
			else {
				Team[] singleData = new Team[1];
				singleData[0] = manager.getValue();
				liveH2HTeamTable.put(newScore, singleData);
			}
		}
	}*/

	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}
}
