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
	public String gameweek = null;

	public ClassicLeague(int league) {
		leagueID = league;
		managerMap = new TreeMap<String, Team> ();
	}

	public void loadLeague() {
		System.out.println("Loading Classic League: " + leagueID + "...");
		try {
			String URL = ("http://fantasy.premierleague.com/my-leagues/" + leagueID + "/standings/");
			Document leaguePage = Jsoup.connect(URL).get();


			//Gets the League Table of the HTML to be manipulated
			Elements leagueTable = leaguePage.select("table.ismTable.ismStandingsTable");
			//Checks for Classic League
			if (leagueTable.isEmpty()) {
				System.err.println("This League is empty so either contains no teams or the season has not yet begun!");
			}
			else {

				//Gets League Name
				leagueName = leaguePage.getElementsByClass("ismTabHeading").text();
				System.out.println("Loaded League: " + leagueName);

				//Works Out Gameweek
				gameweek = leagueTable.select("a[href]").first().attr("href").split("/")[4];

				System.out.println("The Current Gameweek is " + gameweek);
				System.out.print("Loading Managers...  ");

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

				System.out.println("Managers Loaded");
			}
		}
		catch (ConnectException c) {
			if (timeoutCheck() > 10) {
				System.err.println("Too Many Timeouts... Quitting");
				System.exit(102);
			}
			System.out.println("Timeout Connecting. Retrying...");
			loadLeague();
		}
		catch (SocketTimeoutException e) {
			if (timeoutCheck() > 10) {
				System.err.println("Too Many Timeouts... Quitting");
				System.exit(102);
			}
			System.out.println("Timeout Connecting. Retrying...");
			loadLeague();
		}
		catch (UnknownHostException g) {
			System.err.println("No Connection... Quitting");
			System.exit(100);
		}
		catch (NoRouteToHostException h) {
			System.err.println("No Connection... Quitting");
			System.exit(101);
		}
		catch (IOException f) {
			System.err.println("--In loadLeague: " + f);
			System.exit(104);
		}
		catch (NullPointerException i) {
			System.err.println("This League either contains no teams or the season has not yet begun!");
			//System.exit(3);
		}

	}


	public void addManager(String manID, String GW, String LPScore, String lPosition) {
		//Adds a map of managerIDs to teams (which store the managerID anyway, but ID needed later for sorting)Max teams maybe expanded
		//Passes the League Score to the Team
		managerMap.put(manID, new Team(manID, GW, LPScore, lPosition));

	}

	public void loadTeams() {
		//Runs through all the entries to load the Teams
		for (Entry<String,Team> entry: managerMap.entrySet()) {
			entry.getValue().getTeam();
		}
	}

	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}
}
