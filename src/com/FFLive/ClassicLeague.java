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

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ClassicLeague {

	public Map<String, Team> managerMap = null;
	private int timeout = 0;
	public int leagueID;
	public String leagueName = null;
	public int gameweek = 0;
	public int startingGameweek = 41;

	public ClassicLeague(int league) {
		leagueID = league;
		managerMap = new TreeMap<String, Team> ();
	}

	public ClassicLeague(int league, int startingGW) {
		leagueID = league;
		startingGameweek = startingGW;
		managerMap = new TreeMap<String, Team> ();
	}

	public void loadLeague() {
		Main.log.log(6,"Loading Classic League: " + leagueID + "...   \n");
		try {

			boolean nextPage = true;
			String page = "";

			while(nextPage) {
				nextPage = false;

				String URL = ("http://fantasy.premierleague.com/my-leagues/" + leagueID + "/standings/" + page);
				Document leaguePage = Jsoup.connect(URL).get();

				//Checks if there is a next page
				Elements nextButton = leaguePage.select("a.ismButton");
				for(Element temp: nextButton) {
					if(temp.text().equals("Next")) {
						page = temp.attr("href");
						nextPage = true;
						break;
					}
				}

				//Gets the League Table of the HTML to be manipulated
				Elements leagueTable = leaguePage.select("table.ismTable.ismStandingsTable");
				//Checks for Classic League
				if (leagueTable.isEmpty()) {
					Main.log.ln(3);
					Main.log.log(3,"League " + leagueID + "is empty so either contains no teams or the season has not yet begun!\n");
				}
				else {

					//Gets League Name
					leagueName = leaguePage.getElementsByClass("ismTabHeading").text();
					
					Main.log.log(6,"Loaded League: " + leagueName + "\n");


					//Works Out Gameweek
					gameweek = Integer.parseInt(leagueTable.select("a[href]").first().attr("href").split("/")[4]);

					
					Main.log.log(6,"The Current Gameweek is " + gameweek +"\n");
					Main.log.log(6,"Loading Managers...  ");

					int counter = 0;

					//Loads A Table Row and Runs through the sections of that row to extract TeamID, Current Position and Current LP Score
					for (Element tableRow : leagueTable.select("tr")) {

						String managerIDTemp = null;
						String[] mapString = new String [2];
						//For the box Elements
						for (Element el :  tableRow.select("td")) {

							counter++;
							if (counter == 2) {
								//Saves Position
								mapString[0] = el.text().replaceAll("\\D+","");
							}
							else if (counter == 3) {
								//ManagerIds in the link in the 3rd Column (of 6)
								managerIDTemp = el.select("a").attr("href").split("/")[2];
							}
							else if (counter == 6) {
								//League Score in the 6th Column
								mapString[1] = el.text().replaceAll("\\D+","");
								//Adds the manager using addManager Method
								addManager(managerIDTemp, gameweek, mapString[1], mapString[0]);
								counter = 0;
								break;
							}
						} 
					}
					Main.log.log(6,"Managers Loaded\n");
				}
			}
		}
		catch (ConnectException c) {
			if (timeoutCheck() > 10) {
				Main.log.log(2,"Too Many Timeouts... Skipping");
			}
			Main.log.log(6,"Timeout Connecting. Retrying...");
			loadLeague();
		}
		catch (SocketTimeoutException e) {
			if (timeoutCheck() > 10) {
				Main.log.log(2,"Too Many Timeouts... Skipping");
			}
			Main.log.log(6,"Timeout Connecting. Retrying...");
			loadLeague();
		}
		catch (UnknownHostException g) {
			Main.log.log(2,"No Connection... Skipping");
		}
		catch (NoRouteToHostException h) {
			Main.log.log(2,"No Connection... Skipping");
		}
		catch(NumberFormatException n ) {
			Main.log.log(2,"Error in Parsing GW In loadLeague: " + n);
		}
		catch (IOException f) {
			Main.log.log(2,"In loadLeague: " + f);
		}
		catch (NullPointerException i) {
			Main.log.log(2,"This League either contains no teams or the season has not yet begun!");
		}

	}


	public void addManager(String manID, int GW, String LPScore, String lPosition) {
		//Adds a map of managerIDs to teams (which store the managerID anyway, but ID needed later for sorting)Max teams maybe expanded
		//Passes the League Score to the Team
		managerMap.put(manID, new Team(manID, GW, LPScore, lPosition));

	}

	public void loadTeams() {
		//Runs through all the entries to load the Teams
		int n = 1;
		for (Entry<String,Team> entry: managerMap.entrySet()) {

			Main.log.log(4,"Loading Teams for '" + leagueName + "'... Team " + n++ + "/" + managerMap.size() + "\r");
			entry.getValue().getTeam();
		}
		Main.log.ln(4);
	}

	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}
}
