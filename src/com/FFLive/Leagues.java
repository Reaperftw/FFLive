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
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class Leagues {

	public List<ClassicLeague> classicLeague = new ArrayList<ClassicLeague>();
	public List<H2HLeague> h2hLeague = new ArrayList<H2HLeague>();
	
	private int timeout = 0;
	
	
	public Leagues() {
		
	}
	
	public void addLeague(String leagueIDString) {
		try {
			addLeague(Integer.parseInt(leagueIDString), 41);
		}
		catch(NumberFormatException n) {
			Main.log.log(2,"Invalid Number for LeagueID or Starting GW.. See Error Below for More Details...\n");
			Main.log.log(2,n.toString());
		}
	}
	
	public void addLeague(int leagueID) {
		addLeague(leagueID, 41);
	}
	
	public void addLeague(String leagueIDString, String startingGWString) {
		try {
			addLeague(Integer.parseInt(leagueIDString), Integer.parseInt(startingGWString));
		}
		catch(NumberFormatException n) {
			Main.log.log(2,"Invalid Number for LeagueID or Starting GW.. See Error Below for More Details...\n");
			Main.log.log(2,n.toString());
		}
	}
	
	public void addLeague(int leagueID, int startingGW) {
		//Takes a league ID, loads the page and checks if it is a H2H League or a Classic League
		try {
			
			Main.log.log(6,"Adding League " + leagueID + "...  \n");
			
			String URL = ("http://fantasy.premierleague.com/my-leagues/" + leagueID + "/standings/");
			Document leaguePage = Jsoup.connect(URL).get();


			//Gets the League Table of the HTML to be manipulated
			Elements leagueTable = leaguePage.select("table.ismTable.ismStandingsTable");
			Elements H2HleagueTable = leaguePage.select("table.ismTable.ismH2HStandingsTable");
			
			boolean H2H = false;
			boolean classic = false;
			
			//Checks both to check there has not been a page load error/or an invalid league ID.
			//Checks for Classic League
			if (!leagueTable.isEmpty()) {
				classic = true;
			}
			
			//Checks for H2H League
			if (!H2HleagueTable.isEmpty()) {
				H2H = true;
			}

			if (H2H && classic) {
				//Error both cannot be true
				Main.log.log(3,"There has been an error loading " + leagueID + "... Aborting\n");		
			}
			else if (H2H && !classic) {
				//System.out.println("H2H League detected!");
				h2hLeague.add(new H2HLeague(leagueID, startingGW));
			}
			else if (!H2H && classic) {
				//System.out.println("Classic League detected!");
				classicLeague.add(new ClassicLeague(leagueID, startingGW));
			}
			else if (!H2H && !classic) {
				Main.log.log(3,"This League either contains no teams or the season has not yet begun!\n");
			}
		}
		catch (ConnectException c) {
			if (timeoutCheck() > 10) {
				Main.log.log(2,"Too Many Timeouts... Skipping\n");
			}
			Main.log.log(6,"Timeout Connecting. Retrying...\n");
			addLeague(leagueID, startingGW);
		}
		catch (SocketTimeoutException e) {
			if (timeoutCheck() > 10) {
				Main.log.log(2,"Too Many Timeouts... Skipping\n");
			}
			Main.log.log(6,"Timeout Connecting. Retrying...\n");
			addLeague(leagueID, startingGW);
		}
		catch (UnknownHostException g) {
			Main.log.log(2,"No Connection... Skipping\n");
		}
		catch (NoRouteToHostException h) {
			Main.log.log(2,"No Connection... Skipping\n");
		}
		catch (NumberFormatException n) {
			Main.log.log(2,"Invalid Number for LeagueID or Starting GW.. See Error Below for More Details...\n");
			Main.log.log(2,n.toString());
		}
		catch (IOException f) {
			Main.log.log(2,"--In loadLeague: " + f + "\n");
		}
		catch (NullPointerException i) {
			Main.log.log(3,"This League either contains no teams or the season has not yet begun!\n");
			//System.exit(3);
		}
	}
	/*
	public void postUpdate (String gameweek, MySQLConnection sql) {
		for (ClassicLeague CL: classicLeague) {
			CL.loadLeague();
			CL.loadTeams();
			sql.storeLeague(CL);
			sql.postUpdateStatus(CL.leagueID, gameweek);
		}
		for (H2HLeague H2H: h2hLeague) {
			H2H.loadH2HLeague();
			H2H.loadTeams();
			sql.storeLeague(H2H);
			sql.postUpdateStatus(H2H.leagueID, gameweek);
		}
	}
	
	public void teamUpdate (String Gameweek, MySQLConnection sql) {
		for (ClassicLeague CL: classicLeague) {
			CL.loadLeague();
			CL.loadTeams();
			sql.storeLeagueData(CL);
			sql.teamsUpdateStatus(CL.leagueID, Gameweek);
		}
		for (H2HLeague H2H: h2hLeague) {
			H2H.loadH2HLeague();
			H2H.loadTeams();
			sql.storeLeagueData(H2H);
			sql.teamsUpdateStatus(H2H.leagueID, Gameweek);
		}
	}
	*/
	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}
}
