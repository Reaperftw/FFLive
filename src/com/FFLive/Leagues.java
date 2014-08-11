package com.FFLive;

import java.io.IOException;
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
	
	
	public void addLeague(String leagueID) {
		//Takes a league ID, loads the page and checks if it is a H2H League or a Classic League
		try {
			System.out.print("Adding League" + leagueID + "...  ");
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
				System.err.println("There has been an error loading " + leagueID + "... Aborting");		
			}
			else if (H2H && !classic) {
				System.out.println("H2H League detected!");
				h2hLeague.add(new H2HLeague(Integer.parseInt(leagueID)));
			}
			else if (!H2H && classic) {
				System.out.println("Classic League detected!");
				classicLeague.add(new ClassicLeague(Integer.parseInt(leagueID)));
			}
			else if (!H2H && !classic) {
				System.out.println("This League either contains no teams or the season has not yet begun!");
			}
		}
		catch (SocketTimeoutException e) {
			if (timeoutCheck() > 10) {
				System.err.println("Too Many Timeouts... Quitting");
				System.exit(102);
			}
			System.out.println("Timeout Connecting. Retrying...");
			addLeague(leagueID);
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
	
	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}
}
