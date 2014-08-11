package com.FFLive;

import java.io.IOException;
import java.net.SocketTimeoutException;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class Team {

	//Creates a Map for the team, filling it with playerID and a captain tag
	/*public Map <String, String> goalkeeper = new HashMap<String, String>(1);
	public Map <String, String> defenders = new HashMap<String, String>(5);
	public Map <String, String> midfield = new HashMap<String, String>(5);
	public Map <String, String> forwards = new HashMap<String, String>(3);
	public Map <String, String> bench = new HashMap<String, String>(4);*/
	//Values initiated as null for a sanity check later on in the program that can be handled if there was a failure or mistake retrieving team data
	public String[] goalkeeper = {null};
	public String[] defenders = {null, null, null, "0", "0"};
	public String[] midfield = {null, null, "0", "0", "0",};
	public String[] forwards = {null, "0", "0"};
	public String[] bench = {null, null, null, null};
	public String[] captains = {null, null};
	public String managerID;
	public String GW;
	public String teamName;
	public String managerName = null;
	public int position = 0;
	public int opScore = 0;
	public int lpScore;
	public int h2hScore = 0;
	public int win = 0;
	public int loss = 0;
	public int draw = 0;
	public int gameWeekScore;
	public boolean H2H = false;
	private int timeout = 0;

	public Team(String manID, String gameWeek, String leaguePoints, String lPosition) {
		managerID = manID;
		GW = gameWeek;
		lpScore = Integer.parseInt(leaguePoints);
		position = Integer.parseInt(lPosition);
	}

	public Team(String manID, String gameWeek, String leaguePoints, String wins, String draws, String losses, String H2HScore, String lPosition) {
		managerID = manID;
		GW = gameWeek;
		lpScore = Integer.parseInt(leaguePoints);
		win = Integer.parseInt(wins);
		loss = Integer.parseInt(losses);
		draw = Integer.parseInt(draws);
		h2hScore = Integer.parseInt(H2HScore);
		position = Integer.parseInt(lPosition);
		H2H = true;
	}

	public void getTeam() {

		try {
			//Checks if there is an Average Team in this League
			if (managerID.equals("Average")) {
				//Average Team Score will be dealt with when it builds the league.
				//teamName = "Average";	
				//goalkeeper.put("Average", null);
				
				System.out.println("Average Team Added...");
			}
			else {
				System.out.println("Connecting to Team " + managerID);
				//The FF Team in question's webpage
				Document doc = Jsoup.connect("http://fantasy.premierleague.com/entry/" + managerID + "/event-history/" + GW + "/").get();

				teamName = doc.getElementsByClass("ismSection3").text();

				managerName = doc.getElementsByClass("ismSection2").text();

				System.out.print("Getting Team Data For " + teamName + "...  ");

				//Overall Points Score and Gameweek Score, Saved for Previous Week Needs
				opScore = Integer.parseInt(doc.getElementsByClass("ismRHSDefList").text().split(" ")[2].replaceAll("\\D+",""));
				gameWeekScore = Integer.parseInt(doc.select("div.ismSBValue.ismSBPrimary").text().replaceAll("\\D+",""));

				//Adding Players
				//For each loop it makes the string for the individual rows then iterates over all 5
				for (int rowNumber = 1; rowNumber < 6; rowNumber++) {
					String rowClass = "ismPitchRow" + rowNumber;
					//Selects the individual row and then selects all the pitch boxes on that row
					Elements pitchRowPlayerBoxes = doc.getElementsByClass(rowClass).select("div.ismPlayerContainer");
					
					int c = 0;
					
					//Cycles through the pitch boxes and pulls the name, score, captain, vice captain and status elements
					for (Element el:pitchRowPlayerBoxes) {
						//Selects the required sections within the pitch box

						//Gets the Website Player ID
						String playerID = el.select("span.JS_ISM_INFO.a").attr("href").replaceAll("\\D+","");

						Elements playerBoxCaptain = el.select("img.ismCaptain.ismCaptainOn");
						Elements playerBoxVice = el.select("img.ismViceCaptain.ismViceCaptainOn");

						//String captain = new String();

						//Checks for the Captain or Vice Captain Tags and adds it to the captain array
						if (playerBoxCaptain.hasClass("ismCaptainOn")) {
							captains[0] = playerID;
							//captain = "C";
						}
						else if (playerBoxVice.hasClass("ismViceCaptainOn")) {
							captains[1] = playerID;
							//captain = "V";
						}
						//else {
						//	captain = null;
						//}

						//Adds to the Map of the correct type, found by the row number.
						if (rowNumber == 1) {
							goalkeeper[c++] = playerID;
							//goalkeeper.put(playerID, captain);
						}
						else if (rowNumber == 2) {
							defenders[c++] = playerID;
							//defenders.put(playerID, captain);
						}
						else if (rowNumber == 3) {
							midfield[c++] = playerID;
							//midfield.put(playerID, captain);
						}
						else if (rowNumber == 4) {
							forwards[c++] = playerID;
							//forwards.put(playerID, captain);
						}
						else if (rowNumber == 5) {
							bench[c++] = playerID;
							//bench.put(playerID, captain);
						}
					}
				}

				System.out.println("Done!");
			}
		}
		catch(SocketTimeoutException e) {
			if(timeoutCheck() > 4) {
				System.err.println("Too Many Timeouts.. Quitting");
				System.exit(30);
			}
			System.out.println("-- Timeout Connecting, Retrying...");
			getTeam();
		}
		catch(IOException f) {
			System.err.println("-- In addPlayers: " + f);
			System.exit(31);
		}
	}

	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}

}
