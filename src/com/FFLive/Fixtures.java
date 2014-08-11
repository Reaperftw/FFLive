package com.FFLive;

import java.io.IOException;
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

		catch (SocketTimeoutException e) {
			if (timeoutCheck() > 10) {
				System.err.println("Too Many Timeouts... Quitting");
				System.exit(1102);
			}
			System.out.println("Timeout Connecting. Retrying...");
			loadFixtures();
		}
		catch (UnknownHostException g) {
			System.err.println("No Connection... Quitting");
			System.exit(1100);
		}
		catch (NoRouteToHostException h) {
			System.err.println("No Connection... Quitting");
			System.exit(1101);
		}
		catch (IOException f) {
			System.err.println("In Fixtures: " + f);
			System.exit(1103);
		}
	}

	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}
}
