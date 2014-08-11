package com.FFLive;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
//import org.jsoup.Jsoup;
//import org.jsoup.nodes.Document;
//import org.jsoup.select.Elements;

public class Player {

	public String playerID;
	public String firstName = null;
	public String lastName = null;
	public String playerName = null;
	public String playerTeam = null;
	public String position = null;
	public int playerScore = 0;
	public String scoreBreakdown = null;
	public String currentFixture = null;
	public String nextFixture = null;
	public String status = null;
	public String news = null;
	public String photo = null;
	
	private int timeout = 0;
	//Status a = 100%, d = 75-25%, n=0%

	public Player (String playerid) {
		playerID= playerid;
	}

	public void getPlayer() {
		try {
			//Connects to the players info page
			InputStream playerJson = new URL("http://fantasy.premierleague.com/web/api/elements/" + playerID + "/").openStream();
			//Reads the data into a JSON object (via casting into a regular object)
			Reader reader = new InputStreamReader(playerJson, "UTF-8");
			JSONObject playerValues =  (JSONObject)JSONValue.parse(reader);
			
			//Adds Required Data
			firstName = playerValues.get("first_name").toString();
			lastName = playerValues.get("second_name").toString();
			playerName = playerValues.get("web_name").toString();
			playerTeam = playerValues.get("team_name").toString();
			position = playerValues.get("type_name").toString();
			/*
			JSONObject test = (JSONObject)JSONValue.parse(playerValues.get("fixture_history").toString());
			String summary = test.get("summary").toString();
			String all = test.get("all").toString();
			*/
			playerScore = Integer.parseInt(playerValues.get("event_total").toString());
			scoreBreakdown = playerValues.get("fixture_history").toString();
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
		catch(SocketTimeoutException e) {
			if(timeoutCheck() > 3) {
				System.err.println("Too Many Timeouts.. Quitting");
				System.exit(402);
			}
			System.out.println("Timeout Connecting, Retrying...");
			getPlayer();
		}
		catch (UnknownHostException g) {
			System.err.println("No Connection... Quitting");
			System.exit(400);
		}
		catch (NoRouteToHostException h) {
			System.err.println("No Connection... Quitting");
			System.exit(401);
		}
		catch (IOException f) {
			System.out.println("In getPlayer: " + f);
			System.exit(404);
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
