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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
//import org.jsoup.Jsoup;
//import org.jsoup.nodes.Document;
//import org.jsoup.select.Elements;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

public class Player {

	public String playerID;
	public String firstName = null;
	public String lastName = null;
	public String playerName = null;
	public String playerTeam = null;
	public String position = null;
	public int playerScore = 0;
	public int teamNumber = 0;
	//public String scoreBreakdown = null;
	public String gameweekBreakdown = null;
	public String currentFixture = null;
	public String nextFixture = null;
	public String status = null;
	public String news = null;
	public String photo = null;
	public int GW = 0;

	private int timeout = 0;
	//Status a = 100%, d = 75-25%, n=0%

	public Player (String playerid) {
		playerID= playerid;
	}
	
	public Player (String playerid, int gameweek) {
		playerID= playerid;
		GW = gameweek;
	}

	public void getPlayer() {
		try {
			if(playerID.equals("-1")) {
				//Average Team Player...
				Document doc = Jsoup.connect("http://fantasy.premierleague.com/entry/1/event-history/" + GW + "/").get();
				Elements averageScore = doc.select("div.ismUnit.ismSize2of5.ismLastUnit").select("div.ismSBSecondaryVal");
				if (averageScore.isEmpty()) {
					playerScore = 0;
				}
				else {
					try {
						playerScore = Integer.parseInt(averageScore.text().replaceAll("\\D+", ""));
					}
					catch (NumberFormatException n) {
						Main.log.log(2,"Issue saving Average Team..." + n + "\n");
					}
				}
				playerName = "Average";
			}
			else {
				Main.log.log(7, "Fetching Player " + playerID + "\n");
				//Connects to the players info page
				InputStream playerJson = new URL("http://fantasy.premierleague.com/web/api/elements/" + playerID + "/").openStream();
				//Reads the data into a JSON object (via casting into a regular object)
				Reader reader = new InputStreamReader(playerJson, "UTF-8");
				JSONObject playerValues =  (JSONObject)JSONValue.parse(reader);

				//TODO Check if there are values overlength
				//Max Length Ref playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(40), lastName VARCHAR(40),
				//webName VARCHAR(50), score INT, gameweekBreakdown VARCHAR(250), breakdown VARCHAR(250), 
				//teamName VARCHAR(40), currentFixture VARCHAR(40), nextFixture VARCHAR(40), status VARCHAR(10), 
				//news VARCHAR(250), photo VARCHAR(30))
				
				//Adds Required Data
				firstName = playerValues.get("first_name").toString();
				lastName = playerValues.get("second_name").toString();
				playerName = playerValues.get("web_name").toString();
				playerTeam = playerValues.get("team_name").toString();
				teamNumber = Integer.parseInt(playerValues.get("team_id").toString());
				position = playerValues.get("type_name").toString();
				/*
			JSONObject test = (JSONObject)JSONValue.parse(playerValues.get("fixture_history").toString());
			String summary = test.get("summary").toString();
			String all = test.get("all").toString();
				 */
				playerScore = Integer.parseInt(playerValues.get("event_total").toString());
				gameweekBreakdown = playerValues.get("event_explain").toString();
				//scoreBreakdown = playerValues.get("fixture_history").toString();
				currentFixture = playerValues.get("current_fixture").toString();
				nextFixture = playerValues.get("next_fixture").toString();
				status = playerValues.get("status").toString();
				news = playerValues.get("news").toString();
				photo = playerValues.get("photo").toString();

				/*
			System.out.println(firstName);
			System.out.println(lastName);
			System.out.println(playerName);
			System.out.println(playerTeam);
			System.out.println(position);
			System.out.println(summary);
			System.out.println(all);
			System.out.println(playerScore);
			System.out.println(scoreBreakdown);
			System.out.println(currentFixture);
			System.out.println(nextFixture);
			System.out.println(status);
			System.out.println(news);
			System.out.println(photo);*/
			}
		}
		catch(ConnectException c) {
			if(timeoutCheck() > 3) {
				Main.log.log(2,"Too Many Timeouts.. Skipping\n");
			}
			Main.log.log(6,"Timeout Connecting, Retrying...\n");
			getPlayer();
		}
		catch(SocketTimeoutException e) {
			if(timeoutCheck() > 3) {
				Main.log.log(2,"Too Many Timeouts.. Skipping\n");
			}
			Main.log.log(6,"Timeout Connecting, Retrying...\n");
			getPlayer();
		}
		catch (UnknownHostException g) {
			Main.log.log(6,"No Connection... Skipping\n");
		}
		catch (NoRouteToHostException h) {
			Main.log.log(6,"No Connection... Skipping\n");
		}
		catch (IOException f) {
			Main.log.log(6,"In getPlayer: " + f + "\n");
		}
		catch (NullPointerException n) {
			Main.log.log(2, "Missing Player Field with ID:" + playerID + " " + n + "\n");
			Main.log.log(9,n);
		}
	}

	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}
}


/*if (playerID.equals("Average")) {
//Gets the Average Score and puts in the dummy player Average
Document doc = Jsoup.connect("http://fantasy.premierleague.com/entry/1/event-history/" + GW + "/").get();
Elements avScoreBox = doc.select("div.ismUnit.ismSize2of5.ismLastUnit").select("div.ismSBSecondaryVal");
playerScore = Integer.parseInt(avScoreBox.text().replaceAll("\\D+",""));
}*/
