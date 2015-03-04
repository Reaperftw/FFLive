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

import org.jsoup.HttpStatusException;
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
	public String[] goalkeeper = {"0"};
	public String[] defenders = {"0", "0", "0", "0", "0"};
	public String[] midfield = {"0", "0", "0", "0", "0",};
	public String[] forwards = {"0", "0", "0"};
	public String[] bench = {"0", "0", "0", "0"};
	public String[] captains = {"0", "0"};
	public String managerID;
	public int GW;
	public String teamName;
	public String managerName = null;
	public int position = 0;
	public int opScore = 0;
	public int lpScore;
	public int h2hScore = 0;
	public int transfers = 0;
	public int deductions = 0;
	public int win = 0;
	public int loss = 0;
	public int draw = 0;
	public int gameWeekScore;
	private int timeout = 0;

	public Team(String manID, int gameWeek) {
		managerID = manID;
		GW = gameWeek;
	}
	
	public Team(String manID, int gameWeek, String leaguePoints, String lPosition) {
		managerID = manID;
		GW = gameWeek;
		lpScore = Integer.parseInt(leaguePoints);
		position = Integer.parseInt(lPosition);
	}

	public Team(String manID, int gameWeek, String leaguePoints, String wins, String draws, String losses, String H2HScore, String lPosition) {
		managerID = manID;
		GW = gameWeek;
		lpScore = Integer.parseInt(leaguePoints);
		win = Integer.parseInt(wins);
		loss = Integer.parseInt(losses);
		draw = Integer.parseInt(draws);
		h2hScore = Integer.parseInt(H2HScore);
		position = Integer.parseInt(lPosition);
	}

	public void getTeam() {

		try {
			//Checks if there is an Average Team in this League
			if (managerID.equals("0")) {
				//Average Team Score will be dealt with when it builds the league.
				teamName = "Average";
				managerName = "Average";
				
				//Needed for initial setup and post GW updates
				Player average = new Player("-1");
				average.getPlayer();
				gameWeekScore = average.playerScore;
				
				Main.log.log(7,"Average Team Added...\n");
			}
			else {
				Main.log.log(7,"Connecting to Team " + managerID);
				//The FF Team in question's webpage
				Document doc = Jsoup.connect("http://fantasy.premierleague.com/entry/" + managerID + "/event-history/" + GW + "/").get();

				teamName = doc.getElementsByClass("ismSection3").text();

				managerName = doc.getElementsByClass("ismSection2").text();
				
				//Checks for transfers and deductions
				String[] transfer = doc.select("dl.ismDefList.ismSBDefList").text().split("Transfers")[1].split("\\(");
				if(transfer.length == 1) {
					transfers = Integer.parseInt(transfer[0].trim());
				}
				else if (transfer.length == 2) {
					transfers = Integer.parseInt(transfer[0].trim());
					deductions = Integer.parseInt(transfer[1].replace("pts)", "").trim());
				}
				else {
					Main.log.log(3,"Error in Deductions and Transfers for TeamID:" + managerID + "\n");
				}
				Main.log.log(7,"Getting Team Data For " + teamName + "...  \n");

				//Overall Points Score and Gameweek Score, Saved for Previous Week Needs
				opScore = Integer.parseInt(doc.getElementsByClass("ismRHSDefList").text().split(" ")[2].replaceAll("\\D+",""));
				gameWeekScore = Integer.parseInt(doc.select("div.ismSBValue.ismSBPrimary").text().replaceAll("\\D+",""));

				//Adding Players
				//For each loop it makes the string for the individual rows then iterates over all 5
				for (int rowNumber = 1; rowNumber < 6; rowNumber++) {
					String rowClass = "ismPitchRow" + rowNumber;
					//Selects the individual row and then selects all the pitch boxes on that row
					Elements pitchRowPlayerBoxes = doc.select("div." + rowClass).select("div.ismPitchElement");
					
					
					int c = 0;
					
					//Cycles through the pitch boxes and pulls the name, score, captain, vice captain and status elements
					for (Element el:pitchRowPlayerBoxes) {
						//Selects the required sections within the pitch box

						//Gets the Website Player ID
						String playerID = el.select("a.JS_ISM_INFO").attr("href").replaceAll("\\D+","");

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
				Main.log.log(8,"Done!\n");
			}
		}
		catch (ConnectException c) {
			if(timeoutCheck() > 4) {
				Main.log.ln(2);
				Main.log.log(2,"Too Many Timeouts Connecting to ID: " + managerID + " For GW:" + GW + ".. Skipping\n");
			}
			else {
				Main.log.ln(6);
				Main.log.log(6,"Timeout Connecting, Retrying...\n");
				getTeam();
			}
		}
		catch(SocketTimeoutException e) {
			if(timeoutCheck() > 4) {
				Main.log.ln(2);
				Main.log.log(2,"Too Many Timeouts Connecting to ID: " + managerID + " For GW:" + GW + ".. Skipping\n");
			}
			else {
				Main.log.ln(6);
				Main.log.log(6,"Timeout Connecting, Retrying...\n");
				getTeam();
			}
		}
		catch (HttpStatusException i) {
			if(timeoutCheck() > 4) {
				Main.log.ln(2);
				Main.log.log(2,"Too Many Timeouts Connecting to ID: " + managerID + " For GW:" + GW + ".. Skipping\n");
			}
			else {
				Main.log.ln(6);
				Main.log.log(6,"Timeout Connecting, Retrying...\n");
				getTeam();
			}
		}
		catch (UnknownHostException g) {
			Main.log.ln(2);
			Main.log.log(2,"No Connection Connecting to ID: " + managerID + " For GW:" + GW + ".. Skipping\n");
		}
		catch (NoRouteToHostException h) {
			Main.log.ln(2);
			Main.log.log(2,"No Connection Connecting to ID: " + managerID + " For GW:" + GW + ".. Skipping\n");
		}
		catch(IOException f) {
			Main.log.ln(2);
			Main.log.log(2,"-- In addPlayers: " + f);
			if(timeoutCheck() > 2) {
				Main.log.ln(2);
				Main.log.log(2,"  Problem Connecting to ID: " + managerID + " For GW:" + GW + ".. Skipping\n");
			}
			else {
				Main.log.ln(2);
				Main.log.log(2,"-- Retrying...\n");
				getTeam();
			}
		}
		
	}

	public int timeoutCheck() {
		this.timeout++;
		return this.timeout;
	}

}
