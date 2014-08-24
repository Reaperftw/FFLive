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

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class Fixtures {

	private int timeout = 0;
	public String loadGameweek = "";
	
	public String gameweek = null;
	public String startTime = null;
	public String kickOff = null;
	public String endTime = null;

	public Fixtures() {
		
	}
	
	public Fixtures(String gw) {
		loadGameweek = gw;
	}
	public Fixtures(int gw) {
		loadGameweek = Integer.toString(gw);
	}
	
	public void loadFixtures() {
		//Assuming this page shows the current gameweek
		try {
			//Loading Fixtures Page
			System.out.print("Checking Fixtures and Start Times...  ");
			String URL = ("http://fantasy.premierleague.com/fixtures/" + loadGameweek);
			Document fixtures = Jsoup.connect(URL).get();
			Elements fixtureTable = fixtures.select("table.ismFixtureTable");
			//Loads the Next for the Gameweek and the Start and end fixture times
			Elements gameweekText = fixtureTable.select("caption.ismStrongCaption");
			Elements individualFixtures = fixtureTable.select("tr.ismFixture");
			
			
			gameweek = gameweekText.text().split("-")[0].replaceAll("\\D+", "").trim();

			startTime = gameweekText.text().split("-")[1].trim();
			kickOff = individualFixtures.first().select("td").first().text().trim();
			endTime = individualFixtures.last().select("td").first().text().trim();
			//Add Two Hours After final kickoff to make sure match has finished.
			//endTime.add(Calendar.HOUR_OF_DAY, 2);
			
			
			
		}
		catch (ConnectException c) {
			if (timeoutCheck() > 10) {
				System.err.println("Too Many Timeouts... Skipping");
			}
			System.out.println("Timeout Connecting. Retrying...");
			loadFixtures();
		}
		catch (SocketTimeoutException e) {
			if (timeoutCheck() > 10) {
				System.err.println("Too Many Timeouts... Skipping");
			}
			System.out.println("Timeout Connecting. Retrying...");
			loadFixtures();
		}
		catch (UnknownHostException g) {
			System.err.println("No Connection... Skipping");
		}
		catch (NoRouteToHostException h) {
			System.err.println("No Connection... Skipping");
		}
		catch (IOException f) {
			System.err.println("In Fixtures: " + f);
		}
	}

	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}
}
