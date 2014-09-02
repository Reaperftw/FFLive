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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;

public class MySQLConnection {

	public Connection conn = null;

	public MySQLConnection(String ipAddress, String userName, String password, String database) {
		try {
			// this will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// setup the connection with the DB.
			System.out.print("Connecting to your MySQL Database...  ");
			conn = DriverManager.getConnection("jdbc:mysql://" + ipAddress + "/?user=" + userName + "&password=" + password + "&useUnicode=true&characterEncoding=utf8");
			Statement statement = conn.createStatement();
			statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + database);
			statement.executeUpdate("USE " + database);
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS leagues (ID INT NOT NULL UNIQUE, name VARCHAR(30) NOT NULL, type VARCHAR(10) NOT NULL)");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS status (LeagueID INT NOT NULL, Gameweek INT NOT NULL, starts VARCHAR(20), kickOff VARCHAR(20), started VARCHAR(2) DEFAULT 'N', ends VARCHAR(20) , ended VARCHAR(2) DEFAULT 'N', teamsStored VARCHAR(2) DEFAULT 'N', postGwUpdate VARCHAR(2) DEFAULT 'N', UNIQUE (leagueID, Gameweek))");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS webFront (page VARCHAR(10) UNIQUE, status VARCHAR(30), currGameweek INT)");
			statement.executeUpdate("INSERT INTO webFront (page, status) values ('index', 'Loading...')  ON DUPLICATE KEY UPDATE status = 'loading'");
			System.out.println("Database loaded!");
			statement.close();
		}
		catch (ClassNotFoundException c) {
			System.err.println("Critical Program Failure!");
			System.err.println(c);
			System.exit(1001);
		}
		catch (SQLException sql) {
			System.err.println("MySQL Error Encountered, Check that your details are correct and that your user has permissions to edit DB " + database);
			System.err.println(sql);
			sql.printStackTrace();
			System.exit(1002);
		}
	}

	public void setWebFront(String page, String status) {
		try {
			PreparedStatement UWeb = conn.prepareStatement("UPDATE webFront set status = ? where page = ?");
			UWeb.setString(1, status);
			UWeb.setString(2, page);
			UWeb.executeUpdate();
			UWeb.close();
		}
		catch (SQLException sql) {
			sql.printStackTrace();
		}
	}

	public void setWebFrontGW(String page, int gw) {
		try {
			PreparedStatement web = conn.prepareStatement("UPDATE webFront set currGameweek = ? where page = ?");	
			web.setInt(1, gw);
			web.setString(2, page);
			web.executeUpdate();
			web.close();
		}
		catch (SQLException sql){
			System.err.println("");
			System.err.println("Error setting Web Front Gameweek -- " + sql);
		}
	}

	public void addStatus(ArrayList<String> leagueIDs) {

		System.out.print("Adding/Updating leagues...  ");
		try {

			Fixtures fixture = new Fixtures();
			fixture.loadFixtures();
			int gw = Integer.parseInt(fixture.gameweek);
			
			//Set the correct gameweek on the front of the webpage
			Calendar now = Calendar.getInstance();
			if(now.before(new DateParser(fixture.kickOff).convertDate())) {
				setWebFrontGW("index",gw-1);
			}
			else {
				setWebFrontGW("index",gw);
			}
				


			//CREATE all the required Gameweek Leagues...

			PreparedStatement CTleagueTeamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS leagues_teamsGW? (leagueID INT NOT NULL, managerID INT NOT NULL, lp INT DEFAULT 0, position INT, wins INT DEFAULT 0, loss INT DEFAULT 0, draw INT DEFAULT 0, points INT DEFAULT 0, fixture VARCHAR(2), livePoints INT, liveLP INT DEFAULT 0, liveWin INT DEFAULT 0, liveLoss INT DEFAULT 0, liveDraw INT DEFAULT 0, livePosition INT, posDifferential VARCHAR(10) DEFAULT '-', UNIQUE (leagueID, managerID))");
			CTleagueTeamsGW.setInt(1, gw);
			CTleagueTeamsGW.executeUpdate();

			PreparedStatement CTteamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS teamsGW? (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), op INT DEFAULT 0, gw INT DEFAULT 0, liveOP INT DEFAULT 0, GkID INT DEFAULT 0, DefID1 INT DEFAULT 0, DefID2 INT DEFAULT 0, DefID3 INT DEFAULT 0, DefID4 INT DEFAULT 0, DefID5 INT DEFAULT 0, MidID1 INT DEFAULT 0, MidID2 INT DEFAULT 0, MidID3 INT DEFAULT 0, MidID4 INT DEFAULT 0, MidID5 INT DEFAULT 0, ForID1 INT DEFAULT 0, ForID2 INT DEFAULT 0, ForID3 INT DEFAULT 0, BenchID1 INT DEFAULT 0, BenchID2 INT DEFAULT 0, BenchID3 INT DEFAULT 0, BenchID4 INT DEFAULT 0, captainID INT DEFAULT 0, viceCaptainID INT DEFAULT 0)");
			CTteamsGW.setInt(1, gw);
			CTteamsGW.executeUpdate();

			PreparedStatement CTPlayersGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS playersGW? (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), webName VARCHAR(40), score INT, gameweekBreakdown VARCHAR(150), breakdown VARCHAR(250), teamName VARCHAR(30), currentFixture VARCHAR(30), nextFixture VARCHAR(30), status VARCHAR(10), news VARCHAR(100), photo VARCHAR(30))");
			CTPlayersGW.setInt(1,gw);
			CTPlayersGW.executeUpdate();

			PreparedStatement CTH2HFixture = conn.prepareStatement("CREATE TABLE IF NOT EXISTS H2HGW? (leagueID INT NOT NULL, home VARCHAR(30), away VARCHAR(30), fixtureNo INT, UNIQUE(leagueID, home, away))");
			CTH2HFixture.setInt(1,gw);
			CTH2HFixture.executeUpdate();

			for (String leagueID : leagueIDs) {
				PreparedStatement preparedStmt = conn.prepareStatement("INSERT INTO status (LeagueID, Gameweek, starts, kickOff, ends) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE starts = ?, kickOff = ?, ends = ?");
				preparedStmt.setInt(1 , Integer.parseInt(leagueID));
				preparedStmt.setInt(2 , gw);
				preparedStmt.setString(3 , fixture.startTime);
				preparedStmt.setString(4 , fixture.kickOff);
				preparedStmt.setString(5 , fixture.endTime);
				preparedStmt.setString(6 , fixture.startTime);
				preparedStmt.setString(7 , fixture.kickOff);
				preparedStmt.setString(8 , fixture.endTime);

				preparedStmt.executeUpdate();
				preparedStmt.close();
			}


			if (gw != 1) {
				int prevGW = gw -1;
				Fixtures prevFixture = new Fixtures(prevGW);
				prevFixture.loadFixtures();
				//PreparedStatement CTleagueTeamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS leagues_teamsGW? (leagueID INT NOT NULL, managerID INT NOT NULL, lp INT DEFAULT 0, position INT, wins INT DEFAULT 0, loss INT DEFAULT 0, draw INT DEFAULT 0, points INT DEFAULT 0, fixture VARCHAR(2), livePoints INT, liveLP INT DEFAULT 0, liveWin INT DEFAULT 0, liveLoss INT DEFAULT 0, liveDraw INT DEFAULT 0, livePosition INT, posDifferential VARCHAR(10) DEFAULT '-', UNIQUE (leagueID, managerID))");
				CTleagueTeamsGW.setInt(1, prevGW);
				CTleagueTeamsGW.executeUpdate();

				//PreparedStatement CTteamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS teamsGW? (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), op INT DEFAULT 0, gw INT DEFAULT 0, liveOP INT DEFAULT 0, GkID INT DEFAULT 0, DefID1 INT DEFAULT 0, DefID2 INT DEFAULT 0, DefID3 INT DEFAULT 0, DefID4 INT DEFAULT 0, DefID5 INT DEFAULT 0, MidID1 INT DEFAULT 0, MidID2 INT DEFAULT 0, MidID3 INT DEFAULT 0, MidID4 INT DEFAULT 0, MidID5 INT DEFAULT 0, ForID1 INT DEFAULT 0, ForID2 INT DEFAULT 0, ForID3 INT DEFAULT 0, BenchID1 INT DEFAULT 0, BenchID2 INT DEFAULT 0, BenchID3 INT DEFAULT 0, BenchID4 INT DEFAULT 0, captainID INT DEFAULT 0, viceCaptainID INT DEFAULT 0)");
				CTteamsGW.setInt(1, prevGW);
				CTteamsGW.executeUpdate();

				//PreparedStatement CTPlayersGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS playersGW? (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), webName VARCHAR(40), score INT, gameweekBreakdown VARCHAR(150), breakdown VARCHAR(250), teamName VARCHAR(30), currentFixture VARCHAR(30), nextFixture VARCHAR(30), status VARCHAR(10), news VARCHAR(100), photo VARCHAR(30))");
				CTPlayersGW.setInt(1,prevGW);
				CTPlayersGW.executeUpdate();

				//PreparedStatement CTH2HFixture = conn.prepareStatement("CREATE TABLE IF NOT EXISTS H2HGW? (leagueID INT NOT NULL, home VARCHAR(30), away VARCHAR(30), fixtureNo INT, UNIQUE(leagueID, home, away))");
				CTH2HFixture.setInt(1,prevGW);
				CTH2HFixture.executeUpdate();

				for (String leagueID : leagueIDs) {
					PreparedStatement preparedStmt = conn.prepareStatement("INSERT INTO status (LeagueID, Gameweek, starts, kickOff, ends) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE starts = ?, kickOff = ?, ends = ?");
					preparedStmt.setInt(1 , Integer.parseInt(leagueID));
					preparedStmt.setInt(2 , prevGW);
					preparedStmt.setString(3 , prevFixture.startTime);
					preparedStmt.setString(4 , prevFixture.kickOff);
					preparedStmt.setString(5 , prevFixture.endTime);
					preparedStmt.setString(6 , prevFixture.startTime);
					preparedStmt.setString(7 , prevFixture.kickOff);
					preparedStmt.setString(8 , prevFixture.endTime);

					preparedStmt.executeUpdate();
					preparedStmt.close();
				}


			}
			CTleagueTeamsGW.close();
			CTteamsGW.close();
			CTPlayersGW.close();
			CTH2HFixture.close();

			System.out.println("Ready!");

		} catch (SQLException e) {
			//TODO Error Handle
			e.printStackTrace();
			System.exit(1006);
		}
	}

	public void removeLeague(String leagueID, String gameweek) {
		try {
			PreparedStatement delStatus = conn.prepareStatement("DELETE FROM status WHERE LeagueID = ? AND Gameweek = ?");
			delStatus.setString(1, leagueID);
			delStatus.setString(2, gameweek);
			PreparedStatement delTeams = conn.prepareStatement("DELETE FROM ? WHERE leagueID = ?");
			delTeams.setString(1, "leagues_teamsGW" +gameweek);
			delTeams.setString(2, leagueID);
			delStatus.close();
			delTeams.close();
		}
		catch (SQLException sql) {
			//TODO Error Handle
			sql.printStackTrace();
		}
	}

	public Map<String, List<String>> statusCheck() {
		System.out.print("Checking Status of Stored Leagues...  ");
		Map<String,List<String>> incomplete = new HashMap<String,List<String>>();


		try {
			//Check the status table
			Statement statement = conn.createStatement();
			ResultSet status = statement.executeQuery("SELECT * from status");

			Calendar now = Calendar.getInstance();

			//Check Status Table for integrity
			while (status.next()) {
				int leagueID = status.getInt("LeagueID");
				int gw = status.getInt("Gameweek");

				Calendar startDate = (new DateParser(status.getString("starts"))).convertDate();
				Calendar kickOff = (new DateParser(status.getString("kickOff"))).convertDate();
				Calendar endDate = (new DateParser(status.getString("ends"))).convertDate();
				Calendar postDate = (new DateParser(status.getString("ends"))).convertDate();
				//Add 2 Hours on to make sure match has finished
				endDate.add(Calendar.HOUR_OF_DAY, 2);
				startDate.add(Calendar.MINUTE, 30);
				postDate.add(Calendar.DAY_OF_MONTH, 1);

				//Check the Times and update the status table


				if(status.getString("started").equals("Y")) {
					//GW Has Started, are teams store
					if(status.getString("teamsStored").equals("Y")) {
						//Teams Are Stored, has the gameweek now ended
						if(status.getString("ended").equals("Y")) {
							//Gameweek has now ended, has the post GW update been done
							if(status.getString("postGwUpdate").equals("Y")) {
								//All Done Nothing to Do.
							}
							else if(status.getString("postGwUpdate").equals("F")) {
								//Past the Post GW Update Time so Ignore
							}
							//Ended but is after post date?
							else if (now.after(postDate)){
								//After Post Date, queue for post update
								if(!incomplete.containsKey("post")) {
									List<String> post = new ArrayList<String>();
									post.add(leagueID + "," + gw);
									incomplete.put("post",post);
								}
								else {
									incomplete.get("post").add(leagueID + "," + gw);
								}
							}
							else {
								//Not after post date, nothing to do.
							}
						}
						//Gameweek not marked as ended, check if now ended
						else if (now.after(endDate)){
							//Gameweek has now ended, check if after post date
							if(now.after(postDate)) {
								//Now is after post date, update status table and queue for post update

								generalStatusUpdate(leagueID,gw,"Y", "Y");
								if(!incomplete.containsKey("post")) {
									List<String> post = new ArrayList<String>();
									post.add(leagueID + "," + gw);
									incomplete.put("post",post);
								}
								else {
									incomplete.get("post").add(leagueID + "," + gw);
								}
							}
							else {
								//Not after post date, update status table and mark to do nothing
								generalStatusUpdate(leagueID,gw,"Y", "Y");

							}
						}
						else {
							//Gameweek has stated but not ended, queue to go live

							if(!incomplete.containsKey("live")) {
								List<String> live = new ArrayList<String>();
								live.add(leagueID + "," + gw);
								incomplete.put("live",live);
							}
							else {
								incomplete.get("live").add(leagueID + "," + gw);
							}
						}
					}
					else {
						//Gameweek Started but teams not stored, Queue for team update...
						//Has Gameweek now ended?
						if (now.after(endDate)) {
							//Gameweek has now ended, time for post?
							if (now.after(postDate)) {
								//After Post, Queue for Post and Update status table
								generalStatusUpdate(leagueID,gw,"Y", "Y");

								if(!incomplete.containsKey("post")) {
									List<String> post = new ArrayList<String>();
									post.add(leagueID + "," + gw);
									incomplete.put("post",post);
								}
								else {
									incomplete.get("post").add(leagueID + "," + gw);
								}
							}
							else {
								//Ended but not after post, queue for teams and update status
								generalStatusUpdate(leagueID,gw,"Y", "Y");

								if(!incomplete.containsKey("teams")) {
									List<String> teams = new ArrayList<String>();
									teams.add(leagueID + "," + gw);
									incomplete.put("teams",teams);
								}
								else {
									incomplete.get("teams").add(leagueID + "," + gw);
								}
							}
						}
						else {
							//Not Ended, Update Status and get teams and go live

							generalStatusUpdate(leagueID,gw,"Y", "N");
							if(!incomplete.containsKey("teams")) {
								List<String> teams = new ArrayList<String>();
								teams.add(leagueID + "," + gw);
								incomplete.put("teams",teams);
							}
							else {
								incomplete.get("teams").add(leagueID + "," + gw);
							}

							if(!incomplete.containsKey("live")) {
								List<String> live = new ArrayList<String>();
								live.add(leagueID + "," + gw);
								incomplete.put("live",live);
							}
							else {
								incomplete.get("live").add(leagueID + "," + gw);
							}
						}
					}
				}
				//Started != Y
				else if (status.getString("teamsStored").equals("Y")) {
					//Gameweek not marked as Started, but teams are stored, check if gameweek has now started
					if (now.after(kickOff)) {
						//Games Have Kicked off and teams are stored, check if gameweek has ended.
						if(now.after(endDate)) {
							//Gameweek has now ended, check if it is time for post update
							if(now.after(postDate)) {
								//Now After Post Date, Queue for Post and Update Status Table
								generalStatusUpdate(leagueID,gw,"Y", "Y");

								if(!incomplete.containsKey("post")) {
									List<String> post = new ArrayList<String>();
									post.add(leagueID + "," + gw);
									incomplete.put("post",post);
								}
								else {
									incomplete.get("post").add(leagueID + "," + gw);
								}
							}
							else {
								//After End Date, not time for Post, Update Status Table and Do Nothing

								generalStatusUpdate(leagueID,gw,"Y", "Y");
							}
						}
						else {
							//Not After End Date, Update Status Table to Started and Queue for Live


							generalStatusUpdate(leagueID,gw,"Y", "N");

							if(!incomplete.containsKey("live")) {
								List<String> live = new ArrayList<String>();
								live.add(leagueID + "," + gw);
								incomplete.put("live",live);
							}
							else {
								incomplete.get("live").add(leagueID + "," + gw);
							}
						}
					}
					//Gameweek Not Started, but teams stored, does gameweek start today?
					else if (startDate.get(Calendar.DAY_OF_MONTH) == now.get(Calendar.DAY_OF_MONTH) && startDate.get(Calendar.MONTH) == now.get(Calendar.MONTH)) {
						//Gameweek does start today, queue for wait, nothing to update the status with

						if(!incomplete.containsKey("wait")) {
							List<String> wait = new ArrayList<String>();
							wait.add("Matches to Kick-Off..");
							incomplete.put("wait",wait);
						}
						else {
							incomplete.get("wait").add("Matches to Kick-Off..");
						}
					}
					else {
						//Else Gameweek does not start today, nothing to do
					}
				}
				//Started != Y and teamStored != Y
				//Check Dates
				//Check is after Start Date, if it isnt, then it is pre gameweek and nothing to do
				else if(now.after(startDate)) {
					//After StartDate, is after kick off
					if(now.after(kickOff)) {
						//After Kick Off, is after End Date?
						if(now.after(endDate)) {
							//After End date, is after postUpdate?
							if(now.after(postDate)) {
								//After Post Date, Update Status table and queue for post

								generalStatusUpdate(leagueID,gw,"Y", "Y");
								if(!incomplete.containsKey("post")) {
									List<String> post = new ArrayList<String>();
									post.add(leagueID + "," + gw);
									incomplete.put("post",post);
								}
								else {
									incomplete.get("post").add(leagueID + "," + gw);
								}
							}
							else {
								//After End Date, update Status
								generalStatusUpdate(leagueID,gw,"Y", "Y");
							}
						}
						else {
							//After kick off but before end date, update status table queue for live and teams
							generalStatusUpdate(leagueID,gw,"Y", "N");
							if(!incomplete.containsKey("teams")) {
								List<String> teams = new ArrayList<String>();
								teams.add(leagueID + "," + gw);
								incomplete.put("teams",teams);
							}
							else {
								incomplete.get("teams").add(leagueID + "," + gw);
							}

							if(!incomplete.containsKey("live")) {
								List<String> live = new ArrayList<String>();
								live.add(leagueID + "," + gw);
								incomplete.put("live",live);
							}
							else {
								incomplete.get("live").add(leagueID + "," + gw);
							}
						}
					}
					else {
						//Not after kick off but after started, queue for teams and wait

						if(!incomplete.containsKey("teams")) {
							List<String> teams = new ArrayList<String>();
							teams.add(leagueID + "," + gw);
							incomplete.put("teams",teams);
						}
						else {
							incomplete.get("teams").add(leagueID + "," + gw);
						}
						if(!incomplete.containsKey("wait")) {
							List<String> wait = new ArrayList<String>();
							wait.add("Matches to Kick-Off..");
							incomplete.put("wait",wait);
						}
						else {
							incomplete.get("wait").add("Matches to Kick-Off..");
						}
					}
				}
				else if (startDate.get(Calendar.DAY_OF_MONTH) == now.get(Calendar.DAY_OF_MONTH) && startDate.get(Calendar.MONTH) == now.get(Calendar.MONTH)) {
					//Gameweek does start today, queue for wait, nothing to update the status with
					if(!incomplete.containsKey("wait")) {
						List<String> wait = new ArrayList<String>();
						wait.add("Teams to be finalised");
						incomplete.put("wait",wait);
					}
					else {
						incomplete.get("wait").add("Teams to be finalised");
					}
				}
				else {
					//Pre Gameweek and nothing to do.
				}

			}
			statement.close();

		}		
		catch (SQLException sql){
			//TODO Error Handle
			sql.printStackTrace();
			System.exit(1003);
		}
		catch (NullPointerException n) {
			System.out.println("Data is Missing from the DB");
			System.err.println(n);
			System.exit(1005);
		}
		System.out.println("Done!");
		return incomplete;
	}

	public void generalStatusUpdate (int leagueID, int gw, String started, String ended) {
		try {
			PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set started=?, ended=? where LeagueID = ? AND Gameweek = ?");
			preparedStmt.setString(1, started);
			preparedStmt.setString(2, ended);		
			preparedStmt.setInt(3 , leagueID);
			preparedStmt.setInt(4 ,gw);
			preparedStmt.executeUpdate();
			preparedStmt.close();
		}
		catch (SQLException sql) {
			System.err.println("General Status Table Update Failed");
			sql.printStackTrace();
		}
	}

	public void missedUpdateStatus (String leagueID, String gw) {
		try {
			PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set postGwUpdate='F' where LeagueID = ? AND Gameweek = ?");
			preparedStmt.setInt(1 , Integer.parseInt(leagueID));
			preparedStmt.setInt(2 , Integer.parseInt(gw));
			preparedStmt.executeUpdate();
			preparedStmt.close();
		}
		catch (SQLException sql) {
			System.err.println("Status Table Update Failed");
			System.err.println(sql);
		}
	}

	public void postUpdateStatus (int leagueID, String gw) {
		try {
			PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set postGwUpdate='Y' where LeagueID = ? AND Gameweek = ?");
			preparedStmt.setInt(1 , leagueID);
			preparedStmt.setInt(2 , Integer.parseInt(gw));
			preparedStmt.executeUpdate();
			preparedStmt.close();
		}
		catch (SQLException sql) {
			System.err.println("Status Table Update Failed");
			System.err.println(sql);
		}
	}

	public void teamsUpdateStatus (int leagueID, String gw) {
		try {
			PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set teamsStored='Y' where LeagueID = ? AND Gameweek = ?");
			preparedStmt.setInt(1 , leagueID);
			preparedStmt.setInt(2 , Integer.parseInt(gw));
			preparedStmt.executeUpdate();
			preparedStmt.close();
		}
		catch (SQLException sql) {
			System.err.println("Status Table Update Failed");
			System.err.println(sql);
		}
	}

	public boolean nextGWStarted (String gw) {

		boolean started = false;
		try {
			int GW = Integer.parseInt(gw.trim()) + 1;
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery("SELECT starts FROM status WHERE Gameweek=" + GW);

			Calendar now = Calendar.getInstance();
			while (rs.next()) {
				Calendar yes = (new DateParser (rs.getString("starts"))).convertDate();
				if (now.after(yes)) {
					started = true;
					break;
				}
				else {
					//Gameweek has not started
				}
			}
			statement.close();

		}
		catch (SQLException sql) {
			//TODO Error Handle
			System.err.println(sql);
		}
		return started;
	}

	public void storeLeague(ClassicLeague league) {
		try {
			System.out.print("Storing League '" + league.leagueName + "' to DB...  ");
			int gw = Integer.parseInt(league.gameweek);

			PreparedStatement leaguesInsert = conn.prepareStatement("INSERT INTO leagues values (?, ?, 'Classic') ON DUPLICATE KEY UPDATE name = ?, type = 'Classic'");
			leaguesInsert.setInt(1, league.leagueID);
			leaguesInsert.setString(2, league.leagueName);
			leaguesInsert.setString(3, league.leagueName);
			leaguesInsert.executeUpdate();


			for (Entry<String,Team> entry: league.managerMap.entrySet()) {
				PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, liveLp, livePosition)  values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE liveLp = ?, livePosition = ?");
				ILeaguesTeamsGW.setInt(1, gw);
				ILeaguesTeamsGW.setInt(2, league.leagueID);
				ILeaguesTeamsGW.setInt(3, Integer.parseInt(entry.getKey()));
				ILeaguesTeamsGW.setInt(4, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(5, entry.getValue().position);
				ILeaguesTeamsGW.setInt(6, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(7, entry.getValue().position);
				ILeaguesTeamsGW.executeUpdate();

				//statement.executeUpdate("INSERT INTO " + teamsGW + " (managerID, teamName, managerName) values (" + entry.getValue().managerID + ", '" + entry.getValue().teamName + "' ,'" + entry.getValue().managerName + "') ON DUPLICATE KEY UPDATE teamName = '" + entry.getValue().teamName + "', managerName = '" + entry.getValue().managerName + "'");
				PreparedStatement IteamsGW = conn.prepareStatement("INSERT INTO teamsGW? (managerID, teamName, managerName, liveOp, gw) values (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE teamName = ?, managerName = ?, liveOp = ?, gw = ?");
				IteamsGW.setInt(1, gw);
				IteamsGW.setString(2, entry.getValue().managerID);
				IteamsGW.setString(3, entry.getValue().teamName);
				IteamsGW.setString(4, entry.getValue().managerName);
				IteamsGW.setInt(5, entry.getValue().opScore);
				IteamsGW.setInt(6, entry.getValue().gameWeekScore);
				IteamsGW.setString(7, entry.getValue().teamName);
				IteamsGW.setString(8, entry.getValue().managerName);
				IteamsGW.setInt(9, entry.getValue().opScore);
				IteamsGW.setInt(10, entry.getValue().gameWeekScore);

				IteamsGW.executeUpdate();

				ILeaguesTeamsGW.close();
				IteamsGW.close();
			}
			leaguesInsert.close();
			System.out.println("Done!");

		}
		catch (Exception e) {
			//TODO Error Handle
			e.printStackTrace();
			System.exit(1000);
		}
	}

	public void storeLeague(H2HLeague league) {
		try {
			System.out.print("Storing League '" + league.leagueName + "' to DB...  ");
			int gw = Integer.parseInt(league.gameweek);

			PreparedStatement leaguesInsertH2H = conn.prepareStatement("INSERT INTO leagues values (?, ?, 'H2H') ON DUPLICATE KEY UPDATE name = ?, type = 'H2H'");
			leaguesInsertH2H.setInt(1, league.leagueID);
			leaguesInsertH2H.setString(2, league.leagueName);
			leaguesInsertH2H.setString(3, league.leagueName);
			leaguesInsertH2H.executeUpdate();

			//Insert Fixtures
			int id = 1;
			for (Entry<String, String> entry : league.fixtureMap.entrySet()) {
				PreparedStatement ITH2HFixtures = conn.prepareStatement("INSERT INTO H2HGW? (leagueID, home, away, fixtureNo) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE fixtureNo = ?");
				ITH2HFixtures.setInt(1, gw);
				ITH2HFixtures.setInt(2, league.leagueID);
				ITH2HFixtures.setInt(3, Integer.parseInt(entry.getKey()));
				ITH2HFixtures.setInt(4, Integer.parseInt(entry.getValue()));
				ITH2HFixtures.setInt(5, id);
				ITH2HFixtures.setInt(6, id);
				ITH2HFixtures.executeUpdate();
				id++;
				ITH2HFixtures.close();

			}

			for (Entry<String,Team> entry: league.managerMap.entrySet()) {

				PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, liveLp, livePosition, liveWin, liveLoss, liveDraw, livePoints)  values (?, ?, ?, ?, ? ,? ,? ,?) ON DUPLICATE KEY UPDATE liveLp = ?, livePosition = ?, liveWin = ?, liveLoss = ?, liveDraw = ?, livePoints = ?");
				ILeaguesTeamsGW.setInt(1, gw);
				ILeaguesTeamsGW.setInt(2, league.leagueID);
				ILeaguesTeamsGW.setInt(3, Integer.parseInt(entry.getKey()));
				ILeaguesTeamsGW.setInt(4, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(5, entry.getValue().position);
				ILeaguesTeamsGW.setInt(6, entry.getValue().win);
				ILeaguesTeamsGW.setInt(7, entry.getValue().loss);
				ILeaguesTeamsGW.setInt(8, entry.getValue().draw);
				ILeaguesTeamsGW.setInt(9, entry.getValue().h2hScore);
				ILeaguesTeamsGW.setInt(10, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(11, entry.getValue().position);
				ILeaguesTeamsGW.setInt(12, entry.getValue().win);
				ILeaguesTeamsGW.setInt(13, entry.getValue().loss);
				ILeaguesTeamsGW.setInt(14, entry.getValue().draw);
				ILeaguesTeamsGW.setInt(15, entry.getValue().h2hScore);
				ILeaguesTeamsGW.executeUpdate();

				PreparedStatement IteamsGW = conn.prepareStatement("INSERT INTO teamsGW? (managerID, teamName, managerName, liveOp, gw) values (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE teamName = ?, managerName = ?, liveOp = ?, gw = ?");
				IteamsGW.setInt(1, gw);
				IteamsGW.setString(2, entry.getValue().managerID);
				IteamsGW.setString(3, entry.getValue().teamName);
				IteamsGW.setString(4, entry.getValue().managerName);
				IteamsGW.setInt(5, entry.getValue().opScore);
				IteamsGW.setInt(6, entry.getValue().gameWeekScore);
				IteamsGW.setString(7, entry.getValue().teamName);
				IteamsGW.setString(8, entry.getValue().managerName);
				IteamsGW.setInt(9, entry.getValue().opScore);
				IteamsGW.setInt(10, entry.getValue().gameWeekScore);

				IteamsGW.executeUpdate();

				ILeaguesTeamsGW.close();
				IteamsGW.close();
			}
			leaguesInsertH2H.close();
			System.out.println("Done!");

		}
		catch (SQLException sql) {
			sql.printStackTrace();
		}
		catch (Exception e) {
			//TODO Error Handle
			e.printStackTrace();
			System.exit(1010);
		}
	}

	public void preStore (ClassicLeague league) {
		try {
			System.out.print("Storing Pre-Gameweek Data for League '" + league.leagueName + "' to DB...  ");
			int gw = Integer.parseInt(league.gameweek) + 1;

			PreparedStatement CTleagueTeamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS leagues_teamsGW? (leagueID INT NOT NULL, managerID INT NOT NULL, lp INT DEFAULT 0, position INT, wins INT DEFAULT 0, loss INT DEFAULT 0, draw INT DEFAULT 0, points INT DEFAULT 0, fixture VARCHAR(2), livePoints INT, liveLP INT DEFAULT 0, liveWin INT DEFAULT 0, liveLoss INT DEFAULT 0, liveDraw INT DEFAULT 0, livePosition INT, posDifferential VARCHAR(10) DEFAULT '-', UNIQUE (leagueID, managerID))");
			CTleagueTeamsGW.setInt(1, gw);
			CTleagueTeamsGW.executeUpdate();

			PreparedStatement CTteamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS teamsGW? (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), op INT DEFAULT 0, gw INT DEFAULT 0, liveOP INT DEFAULT 0, GkID INT DEFAULT 0, DefID1 INT DEFAULT 0, DefID2 INT DEFAULT 0, DefID3 INT DEFAULT 0, DefID4 INT DEFAULT 0, DefID5 INT DEFAULT 0, MidID1 INT DEFAULT 0, MidID2 INT DEFAULT 0, MidID3 INT DEFAULT 0, MidID4 INT DEFAULT 0, MidID5 INT DEFAULT 0, ForID1 INT DEFAULT 0, ForID2 INT DEFAULT 0, ForID3 INT DEFAULT 0, BenchID1 INT DEFAULT 0, BenchID2 INT DEFAULT 0, BenchID3 INT DEFAULT 0, BenchID4 INT DEFAULT 0, captainID INT DEFAULT 0, viceCaptainID INT DEFAULT 0)");
			CTteamsGW.setInt(1, gw);
			CTteamsGW.executeUpdate();

			PreparedStatement CTPlayersGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS playersGW? (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), webName VARCHAR(40), score INT, gameweekBreakdown VARCHAR(150), breakdown VARCHAR(250), teamName VARCHAR(30), currentFixture VARCHAR(30), nextFixture VARCHAR(30), status VARCHAR(10), news VARCHAR(100), photo VARCHAR(30))");
			CTPlayersGW.setInt(1,gw);
			CTPlayersGW.executeUpdate();

			PreparedStatement CTH2HFixture = conn.prepareStatement("CREATE TABLE IF NOT EXISTS H2HGW? (leagueID INT NOT NULL, home VARCHAR(30), away VARCHAR(30), fixtureNo INT, UNIQUE(leagueID, home, away))");
			CTH2HFixture.setInt(1,gw);
			CTH2HFixture.executeUpdate();

			PreparedStatement leaguesInsert = conn.prepareStatement("INSERT INTO leagues values (?, ?, 'Classic') ON DUPLICATE KEY UPDATE name = ?, type = 'Classic'");
			leaguesInsert.setInt(1, league.leagueID);
			leaguesInsert.setString(2, league.leagueName);
			leaguesInsert.setString(3, league.leagueName);
			leaguesInsert.executeUpdate();


			for (Entry<String,Team> entry: league.managerMap.entrySet()) {
				PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, lp, position)  values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE lp = ?, position = ?");
				ILeaguesTeamsGW.setInt(1, gw);
				ILeaguesTeamsGW.setInt(2, league.leagueID);
				ILeaguesTeamsGW.setInt(3, Integer.parseInt(entry.getKey()));
				ILeaguesTeamsGW.setInt(4, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(5, entry.getValue().position);
				ILeaguesTeamsGW.setInt(6, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(7, entry.getValue().position);
				ILeaguesTeamsGW.executeUpdate();

				//statement.executeUpdate("INSERT INTO " + teamsGW + " (managerID, teamName, managerName) values (" + entry.getValue().managerID + ", '" + entry.getValue().teamName + "' ,'" + entry.getValue().managerName + "') ON DUPLICATE KEY UPDATE teamName = '" + entry.getValue().teamName + "', managerName = '" + entry.getValue().managerName + "'");
				PreparedStatement IteamsGW = conn.prepareStatement("INSERT INTO teamsGW? (managerID, teamName, managerName, op) values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE teamName = ?, managerName = ?, op = ?");
				IteamsGW.setInt(1, gw);
				IteamsGW.setString(2, entry.getValue().managerID);
				IteamsGW.setString(3, entry.getValue().teamName);
				IteamsGW.setString(4, entry.getValue().managerName);
				IteamsGW.setInt(5, entry.getValue().opScore);
				//IteamsGW.setInt(6, entry.getValue().gameWeekScore);
				IteamsGW.setString(6, entry.getValue().teamName);
				IteamsGW.setString(7, entry.getValue().managerName);
				IteamsGW.setInt(8, entry.getValue().opScore);
				//IteamsGW.setInt(10, entry.getValue().gameWeekScore);

				IteamsGW.executeUpdate();

				ILeaguesTeamsGW.close();
				IteamsGW.close();
			}
			leaguesInsert.close();
			System.out.println("Done!");

		}
		catch (Exception e) {
			//TODO Error Handle
			e.printStackTrace();
			System.exit(1030);
		}
	}

	public void preStore(H2HLeague league) {
		try {
			System.out.print("Storing Pre-Gameweek Data for League '" + league.leagueName + "' to DB...  ");
			int gw = Integer.parseInt(league.gameweek) + 1;

			PreparedStatement CTleagueTeamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS leagues_teamsGW? (leagueID INT NOT NULL, managerID INT NOT NULL, lp INT DEFAULT 0, position INT, wins INT DEFAULT 0, loss INT DEFAULT 0, draw INT DEFAULT 0, points INT DEFAULT 0, fixture VARCHAR(2), livePoints INT, liveLP INT DEFAULT 0, liveWin INT DEFAULT 0, liveLoss INT DEFAULT 0, liveDraw INT DEFAULT 0, livePosition INT, posDifferential VARCHAR(10) DEFAULT '-', UNIQUE (leagueID, managerID))");
			CTleagueTeamsGW.setInt(1, gw);
			CTleagueTeamsGW.executeUpdate();

			PreparedStatement CTteamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS teamsGW? (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), op INT DEFAULT 0, gw INT DEFAULT 0, liveOP INT DEFAULT 0, GkID INT DEFAULT 0, DefID1 INT DEFAULT 0, DefID2 INT DEFAULT 0, DefID3 INT DEFAULT 0, DefID4 INT DEFAULT 0, DefID5 INT DEFAULT 0, MidID1 INT DEFAULT 0, MidID2 INT DEFAULT 0, MidID3 INT DEFAULT 0, MidID4 INT DEFAULT 0, MidID5 INT DEFAULT 0, ForID1 INT DEFAULT 0, ForID2 INT DEFAULT 0, ForID3 INT DEFAULT 0, BenchID1 INT DEFAULT 0, BenchID2 INT DEFAULT 0, BenchID3 INT DEFAULT 0, BenchID4 INT DEFAULT 0, captainID INT DEFAULT 0, viceCaptainID INT DEFAULT 0)");
			CTteamsGW.setInt(1, gw);
			CTteamsGW.executeUpdate();

			PreparedStatement CTPlayersGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS playersGW? (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), webName VARCHAR(40), score INT, gameweekBreakdown VARCHAR(150), breakdown VARCHAR(250), teamName VARCHAR(30), currentFixture VARCHAR(30), nextFixture VARCHAR(30), status VARCHAR(10), news VARCHAR(100), photo VARCHAR(30))");
			CTPlayersGW.setInt(1,gw);
			CTPlayersGW.executeUpdate();

			PreparedStatement CTH2HFixture = conn.prepareStatement("CREATE TABLE IF NOT EXISTS H2HGW? (leagueID INT NOT NULL, home VARCHAR(30), away VARCHAR(30), fixtureNo INT, UNIQUE(leagueID, home, away))");
			CTH2HFixture.setInt(1,gw);
			CTH2HFixture.executeUpdate();


			PreparedStatement leaguesInsertH2H = conn.prepareStatement("INSERT INTO leagues values (?, ?, 'H2H') ON DUPLICATE KEY UPDATE name = ?, type = 'H2H'");
			leaguesInsertH2H.setInt(1, league.leagueID);
			leaguesInsertH2H.setString(2, league.leagueName);
			leaguesInsertH2H.setString(3, league.leagueName);
			leaguesInsertH2H.executeUpdate();


			for (Entry<String,Team> entry: league.managerMap.entrySet()) {

				PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, lp, position, wins, loss, draw, points)  values (?, ?, ?, ?, ? ,? ,? ,?) ON DUPLICATE KEY UPDATE lp = ?, position = ?, wins = ?, loss = ?, draw = ?, points = ?");
				ILeaguesTeamsGW.setInt(1, gw);
				ILeaguesTeamsGW.setInt(2, league.leagueID);
				ILeaguesTeamsGW.setInt(3, Integer.parseInt(entry.getKey()));
				ILeaguesTeamsGW.setInt(4, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(5, entry.getValue().position);
				ILeaguesTeamsGW.setInt(6, entry.getValue().win);
				ILeaguesTeamsGW.setInt(7, entry.getValue().loss);
				ILeaguesTeamsGW.setInt(8, entry.getValue().draw);
				ILeaguesTeamsGW.setInt(9, entry.getValue().h2hScore);
				ILeaguesTeamsGW.setInt(10, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(11, entry.getValue().position);
				ILeaguesTeamsGW.setInt(12, entry.getValue().win);
				ILeaguesTeamsGW.setInt(13, entry.getValue().loss);
				ILeaguesTeamsGW.setInt(14, entry.getValue().draw);
				ILeaguesTeamsGW.setInt(15, entry.getValue().h2hScore);
				ILeaguesTeamsGW.executeUpdate();

				PreparedStatement IteamsGW = conn.prepareStatement("INSERT INTO teamsGW? (managerID, teamName, managerName,op) values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE teamName = ?, managerName = ?, op = ?");
				IteamsGW.setInt(1, gw);
				IteamsGW.setString(2, entry.getValue().managerID);
				IteamsGW.setString(3, entry.getValue().teamName);
				IteamsGW.setString(4, entry.getValue().managerName);
				IteamsGW.setInt(5, entry.getValue().opScore);
				//IteamsGW.setInt(6, entry.getValue().gameWeekScore);
				IteamsGW.setString(6, entry.getValue().teamName);
				IteamsGW.setString(7, entry.getValue().managerName);
				IteamsGW.setInt(8, entry.getValue().opScore);
				//IteamsGW.setInt(10, entry.getValue().gameWeekScore);

				IteamsGW.executeUpdate();

				ILeaguesTeamsGW.close();
				IteamsGW.close();
			}
			leaguesInsertH2H.close();
			System.out.println("Done!");

		}
		catch (SQLException sql) {
			sql.printStackTrace();
		}
		catch (Exception e) {
			//TODO Error Handle
			e.printStackTrace();
			System.exit(1040);
		}
	}

	public void storeLeagueData (ClassicLeague league) {
		System.out.print("Storing Team Data for League '" + league.leagueName + "'...  ");
		for (Entry <String, Team> entry : league.managerMap.entrySet()) {
			storeTeamData(entry.getValue());
		}
		System.out.println("Done!");
	}

	public void storeLeagueData (H2HLeague league) {
		System.out.print("Storing Team Data for League '" + league.leagueName + "'...  ");
		for (Entry <String, Team> entry : league.managerMap.entrySet()) {
			storeTeamData(entry.getValue());
		}
		System.out.println("Done!");
	}

	//TODO FIX THIS WITH PREP STATEMENTS
	public void storeTeamData (Team team) {
		Statement statement = null;
		try {
			//String teamsGW = "teamsGW" + team.GW;
			statement = conn.createStatement();
			//Make a general update string to bundle DB changes into one command. Note, adding leading space on subsequent commands.
			String updateString = "UPDATE teamsGW" + team.GW + " set ";
			/*for (Entry<String, String> entry: team.goalkeeper.entrySet()) {
				if (entry.getValue().equals("C")
				statement.executeUpdate("INSERT INTO ");
			}*/
			//Only 1 GK, add to update string
			if (!team.goalkeeper[0].equals("0")) {
				updateString += "GkID = " + team.goalkeeper[0];
			}
			//statement.executeUpdate("INSERT INTO playersGW" + team.GW + " (playerID) values (" + team.goalkeeper[0] + ") ON DUPLICATE KEY UPDATE playerCount = playerCount + 1");
			int n = 1;
			for (String playerID : team.defenders) {
				if (!playerID.equals("0")) {
					updateString += ", DefID" + n++ + " = " + playerID;
					//statement.executeUpdate("INSERT INTO playersGW" + team.GW + " (playerID) values (" + playerID + ")  ON DUPLICATE KEY UPDATE playerCount = playerCount + 1");
				}
			}
			n = 1;
			for (String playerID : team.midfield) {
				if (!playerID.equals("0")) {
					updateString += ", MidID" + n++ + " = " + playerID;
					//statement.executeUpdate("INSERT INTO playersGW" + team.GW + " (playerID) values (" + playerID + ") ON DUPLICATE KEY UPDATE playerCount = playerCount + 1");
				}
			}
			n = 1;
			for (String playerID : team.forwards) {
				if (!playerID.equals("0")) {
					updateString += ", ForID" + n++ + " = " + playerID;
					//statement.executeUpdate("INSERT INTO playersGW" + team.GW + " (playerID) values (" + playerID + ")  ON DUPLICATE KEY UPDATE playerCount = playerCount + 1");
				}
			}
			n = 1;
			for (String playerID : team.bench) {
				updateString += ", BenchID" + n++ + " = " + playerID;
				//statement.executeUpdate("INSERT INTO playersGW" + team.GW + " (playerID) values (" + playerID + ")  ON DUPLICATE KEY UPDATE playerCount = playerCount + 1");
			}
			updateString += ", captainID = " + team.captains[0];
			updateString += ", viceCaptainID = " + team.captains[1];
			updateString += " where managerID = " + team.managerID;
			statement.executeUpdate(updateString);

			statement.close();
		}
		catch (Exception e) {
			//TODO Error Handle
			e.printStackTrace();
		}

	}

	public void updatePlayers(String gw) {
		System.out.print("Updating All Player Info...  ");
		try {

			PreparedStatement SelPID = conn.prepareStatement("SELECT playerID FROM playersGW?");
			SelPID.setInt(1, Integer.parseInt(gw));
			ResultSet playerList = SelPID.executeQuery();
			//statement.executeQuery("SELECT playerID FROM playersGW" + gw);

			while (playerList.next()) {
				Player player = new Player(Integer.toString(playerList.getInt("playerID")));
				player.getPlayer();
				PreparedStatement UpPGw = conn.prepareStatement("UPDATE playersGW? set "
						+ "firstName =?, "
						+ "lastName =?, "
						+ "webName = ?, "
						+ "score = ?, "
						+ "gameweekBreakdown = ?,"
						//+ "breakdown = ?, "
						+ "teamName = ?, "
						+ "currentFixture = ?, "
						+ "nextFixture = ?, "
						+ "status = ?, "
						+ "news = ?, "
						+ "photo = ? "
						+ "WHERE playerID = ?");
				UpPGw.setInt(1, Integer.parseInt(gw));
				UpPGw.setString(2, player.firstName);
				UpPGw.setString(3, player.lastName);
				UpPGw.setString(4, player.playerName);
				UpPGw.setInt(5, player.playerScore);
				UpPGw.setString(6, player.gameweekBreakdown);
				//UpPGw.setString(7, player.scoreBreakdown);
				UpPGw.setString(7, player.playerTeam);
				UpPGw.setString(8, player.currentFixture);
				UpPGw.setString(9, player.nextFixture);
				UpPGw.setString(10, player.status);
				UpPGw.setString(11, player.news);
				UpPGw.setString(12, player.photo);
				UpPGw.setInt(13, Integer.parseInt(player.playerID));
				UpPGw.executeUpdate();
				/*updateSt.executeUpdate("UPDATE playersGW" + gw + " set "
						+ "firstName = '" + player.firstName + "', "
						+ "lastName = '" + player.lastName  + "', "
						+ "webName = '" + player.playerName  + "', "
						+ "score = '" + player.playerScore  + "', "
						+ "breakdown = '" + player.scoreBreakdown  + "', "
						+ "teamName = '" + player.playerTeam  + "', "
						+ "currentFixture = '" + player.currentFixture  + "', "
						+ "nextFixture = '" + player.nextFixture  + "', "
						+ "status = '" + player.status  + "', "
						+ "news = '" + player.news  + "', "
						+ "photo = '" + player.photo   + "' "
						+ "where playerID = " + player.playerID);*/
				UpPGw.close();
			}

		}

		catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Done!");
	}

	public void generatePlayerList(String gameWeek) {
		System.out.print("Generating Player List..  ");
		try {
			int gw = Integer.parseInt(gameWeek);
			String[] positions = {"GkID","DefID1","DefID2","DefID3","DefID4","DefID5","MidID1","MidID2","MidID3","MidID4","MidID5","ForID1","ForID2","ForID3","BenchID1","BenchID2","BenchID3","BenchID4"};
			//for(String temp: positions) {

			PreparedStatement selectPS = conn.prepareStatement("SELECT * from teamsGW?");
			//selectPS.setString(1, temp);
			selectPS.setInt(1, gw);

			ResultSet rs = selectPS.executeQuery();


			while (rs.next()) {
				for(String temp: positions) {
					int player = rs.getInt(temp);
					if (player != 0) {
						PreparedStatement InsertPS = conn.prepareStatement("INSERT INTO playersGW? (playerID) values (?) ON DUPLICATE KEY UPDATE playerCount = playerCount + 1");
						InsertPS.setInt(1, gw);
						InsertPS.setInt(2, rs.getInt(temp));
						InsertPS.executeUpdate();
						InsertPS.close();
					}
				}
			}
			selectPS.close();
		}
		//}
		catch (SQLException sql) {
			sql.printStackTrace();
		}
	}

	public void updateScores(String gw) {
		System.out.print("Updating Gameweek Scores...  ");
		int gameweek = Integer.parseInt(gw);
		try {

			PreparedStatement SManId = conn.prepareStatement("SELECT managerID from teamsGW?");
			SManId.setInt(1, gameweek);
			ResultSet teams = SManId.executeQuery();

			String[] positions = {"GkID","DefID1","DefID2","DefID3","DefID4","DefID5","MidID1","MidID2","MidID3","MidID4","MidID5","ForID1","ForID2","ForID3"};
			while(teams.next()) {
				int manId = teams.getInt("managerID");
				int gwScore = 0;
				for (String position: positions) {
					PreparedStatement Splayers = conn.prepareStatement("SELECT score from playersGW? JOIN teamsGW? on playersGW?.playerid = teamsGW?." + position + " WHERE managerID = ?");
					Splayers.setInt(1, gameweek);
					Splayers.setInt(2, gameweek);
					Splayers.setInt(3, gameweek);
					Splayers.setInt(4, gameweek);
					Splayers.setInt(5, manId);

					ResultSet score = Splayers.executeQuery();
					while(score.next()){
						gwScore += score.getInt("score");	
					}
					Splayers.close();
				}
				//Adds on the captain score again, unless 0 then it adds on the vice captain score (assuming that 0 means he has not played, will make this based on mins played at a later date.
				PreparedStatement captain = conn.prepareStatement("SELECT score from playersGW? JOIN teamsGW? on playersGW?.playerid = teamsGW?.captainID WHERE managerID = ?");
				captain.setInt(1, gameweek);
				captain.setInt(2, gameweek);
				captain.setInt(3, gameweek);
				captain.setInt(4, gameweek);
				captain.setInt(5, manId);
				ResultSet RSScore = captain.executeQuery();
				while(RSScore.next()) {
					int capScore = RSScore.getInt("score");
					if (capScore == 0) {
						PreparedStatement viceCap = conn.prepareStatement("SELECT score from playersGW? JOIN teamsGW? on playersGW?.playerid = teamsGW?.viceCaptainID WHERE managerID = ?");
						viceCap.setInt(1, gameweek);
						viceCap.setInt(2, gameweek);
						viceCap.setInt(3, gameweek);
						viceCap.setInt(4, gameweek);
						viceCap.setInt(5, manId);
						ResultSet RSViceScore = viceCap.executeQuery();
						while(RSViceScore.next()){
							gwScore += RSScore.getInt("score");
						}
						viceCap.close();
					}
					else {
						gwScore += capScore;
					}
				}
				RSScore.close();

				PreparedStatement UScore = conn.prepareStatement("UPDATE teamsGW? set gw = ?, liveOP = OP + ? WHERE managerID = ?");
				UScore.setInt(1, gameweek);
				UScore.setInt(2, gwScore);
				UScore.setInt(3, gwScore);
				UScore.setInt(4, manId);
				UScore.executeUpdate();
				UScore.close();
				PreparedStatement ULeagueScore = conn.prepareStatement("UPDATE leagues_teamsGW? set liveLP = lp + ? WHERE managerID = ?");
				ULeagueScore.setInt(1, gameweek);
				ULeagueScore.setInt(2, gwScore);
				ULeagueScore.setInt(3, manId);
				ULeagueScore.executeUpdate();
				ULeagueScore.close();
			}
		}
		catch (SQLException sql) {
			sql.printStackTrace();
		}
		System.out.print("Updating Fixture Scores...  ");

		try {
			PreparedStatement SFix = conn.prepareStatement("SELECT * FROM H2HGW?");
			SFix.setInt(1, gameweek);
			ResultSet fixtures = SFix.executeQuery();
			while (fixtures.next()) {
				PreparedStatement STeam = conn.prepareStatement("SELECT managerID, gw FROM teamsGW? WHERE managerID IN (? ,?)");
				STeam.setInt(1, gameweek);
				STeam.setInt(2, fixtures.getInt("home"));
				STeam.setInt(3, fixtures.getInt("away"));
				ResultSet match = STeam.executeQuery();
				int[][] scores = new int[2][2];
				int x = 0;
				while (match.next()) {
					scores[x][0] = match.getInt("managerID");
					scores[x][1] = match.getInt("gw");
					x++;
				}
				PreparedStatement home = conn.prepareStatement("UPDATE leagues_teamsGW? set fixture = ?, livePoints = points + ?, liveWin = wins + ?, liveDraw = draw + ?, liveLoss = loss + ? WHERE managerID = ? AND leagueID = ?");
				PreparedStatement away = conn.prepareStatement("UPDATE leagues_teamsGW? set fixture = ?, livePoints = points + ?, liveWin = wins + ?, liveDraw = draw + ?, liveLoss = loss + ? WHERE managerID = ? AND leagueID = ?");
				//1.gw, 2.fixture, 3.points, 4.wins, 5.draws, 6.loss, 7.manID, 8.leagueID
				home.setInt(1, gameweek);
				away.setInt(1, gameweek);

				home.setInt(7, scores[0][0]);
				away.setInt(7, scores[1][0]);

				home.setInt(8, fixtures.getInt("leagueID"));
				away.setInt(8, fixtures.getInt("leagueID"));

				if (scores[0][1] == scores[1][1]) {
					//Draw
					home.setString(2, "D");
					away.setString(2, "D");
					home.setInt(3, 1);
					away.setInt(3, 1);
					home.setInt(4, 0);
					away.setInt(4, 0);
					home.setInt(5, 1);
					away.setInt(5, 1);
					home.setInt(6, 0);
					away.setInt(6, 0);
				}
				else if (scores[0][1] > scores[1][1]) {
					//Team 0 Wins
					home.setString(2, "W");
					away.setString(2, "L");
					home.setInt(3, 3);
					away.setInt(3, 0);
					home.setInt(4, 1);
					away.setInt(4, 0);
					home.setInt(5, 0);
					away.setInt(5, 0);
					home.setInt(6, 0);
					away.setInt(6, 1);
				}
				else if (scores[0][1] < scores[1][1]) {
					home.setString(2, "L");
					away.setString(2, "W");
					home.setInt(3, 0);
					away.setInt(3, 3);
					home.setInt(4, 0);
					away.setInt(4, 1);
					home.setInt(5, 0);
					away.setInt(5, 0);
					home.setInt(6, 1);
					away.setInt(6, 0);
				}
				else {
					System.out.println("There has been a problem with the Gameweek Scores...   ");
					break;
				}
				home.executeUpdate();
				away.executeUpdate();
				home.close();
				away.close();
				STeam.close();
			}
			SFix.close();




		}
		catch (ArrayIndexOutOfBoundsException a) {
			System.out.println("There is a problem with the fixtures or the team list...  ");
		}
		catch (MySQLSyntaxErrorException p) {
			p.printStackTrace();
			System.out.println("No H2H Leagues to Update...  ");
		}
		catch (SQLException sql) {
			sql.printStackTrace();
		}

		//UPDATE positions
		try {
			PreparedStatement SLeagues = conn.prepareStatement("SELECT ID, type FROM leagues");
			ResultSet allLeagues = SLeagues.executeQuery();
			while(allLeagues.next()) {
				int leagueID = allLeagues.getInt("ID");
				String type = allLeagues.getString("type");
				if(type.equals("Classic")) {
					PreparedStatement STeams = conn.prepareStatement("SELECT liveLP, position, managerID FROM leagues_teamsGW? WHERE leagueID = ? ORDER BY liveLP DESC");
					STeams.setInt(1, gameweek);
					STeams.setInt(2, leagueID);
					ResultSet allTeams = STeams.executeQuery();
					int position = 0;
					int prevLP = -1;
					int skip = 1;
					while(allTeams.next()) {

						if(prevLP == allTeams.getInt("liveLP")) {
							skip ++;
						}
						else {
							position += skip;
							skip = 1;
						}
						String posDiff = "-";
						if (position > allTeams.getInt("position")) {
							posDiff = "Down";
						}
						else if (position < allTeams.getInt("position")) {
							posDiff = "Up";
						}
						PreparedStatement UTeam = conn.prepareStatement("UPDATE leagues_teamsGW? set livePosition = ?, posDifferential = ? WHERE managerID = ? AND leagueID = ?");
						UTeam.setInt(1, gameweek);
						UTeam.setInt(2, position);
						UTeam.setString(3, posDiff);
						UTeam.setInt(4, allTeams.getInt("managerID"));
						UTeam.setInt(5, leagueID);
						UTeam.executeUpdate();
						UTeam.close();
						prevLP = allTeams.getInt("liveLP");
					}
					STeams.close();
				}
				else if(type.equals("H2H")) {
					PreparedStatement STeams = conn.prepareStatement("SELECT managerID, liveLP, livePoints, position FROM leagues_teamsGW? WHERE leagueID = ? ORDER BY livePoints DESC, liveLP DESC");
					STeams.setInt(1, gameweek);
					STeams.setInt(2, leagueID);
					ResultSet allTeams = STeams.executeQuery();
					int prevLP = -1;
					int prevPoints = -1;
					int position = 0;
					int skip = 1;
					while(allTeams.next()) {
						if(prevPoints == allTeams.getInt("livePoints") && prevLP == allTeams.getInt("liveLP")) {
							skip ++;
						}
						else {
							position += skip;
							skip = 1;
						}
						String posDiff = "-";
						if (position > allTeams.getInt("position")) {
							posDiff = "Down";
						}
						else if (position < allTeams.getInt("position")) {
							posDiff = "Up";
						}
						PreparedStatement UTeam = conn.prepareStatement("UPDATE leagues_teamsGW? set livePosition = ?, posDifferential = ? WHERE managerID = ? AND leagueID = ?");
						UTeam.setInt(1, gameweek);
						UTeam.setInt(2, position);
						UTeam.setString(3, posDiff);
						UTeam.setInt(4, allTeams.getInt("managerID"));
						UTeam.setInt(5, leagueID);
						UTeam.executeUpdate();
						UTeam.close();
						prevLP = allTeams.getInt("liveLP");
						prevPoints = allTeams.getInt("livePoints");
					}
					STeams.close();
				}
				else {
					System.out.println("Something has gone wrong with " + leagueID + "of type " + type);
				}
			}
			SLeagues.close();
		}
		catch (SQLException sql) {
			sql.printStackTrace();
		}

		System.out.println("Done!");
	}

	public void postUpdate (Leagues leagues) {
		Set<String> gameweeks = new HashSet<String>();
		for (ClassicLeague CL: leagues.classicLeague) {
			CL.loadLeague();
			CL.loadTeams();
			storeLeague(CL);
			preStore(CL);
			storeLeagueData(CL);
			teamsUpdateStatus(CL.leagueID, CL.gameweek);
			postUpdateStatus(CL.leagueID, CL.gameweek);
			gameweeks.add(CL.gameweek);

		}
		for (H2HLeague H2H: leagues.h2hLeague) {
			H2H.loadH2HLeague();
			H2H.loadTeams();
			storeLeague(H2H);
			preStore(H2H);
			storeLeagueData(H2H);
			teamsUpdateStatus(H2H.leagueID, H2H.gameweek);
			postUpdateStatus(H2H.leagueID, H2H.gameweek);
			gameweeks.add(H2H.gameweek);
		}
		for(String temp: gameweeks) {
			generatePlayerList(temp);
			updatePlayers(temp);
		}
	}


	public void teamUpdate (String Gameweek, Leagues leagues) {
		for (ClassicLeague CL: leagues.classicLeague) {
			CL.loadLeague();
			CL.loadTeams();
			storeLeague(CL);
			storeLeagueData(CL);
			teamsUpdateStatus(CL.leagueID, Gameweek);
		}
		for (H2HLeague H2H: leagues.h2hLeague) {
			H2H.loadH2HLeague();
			H2H.loadTeams();
			storeLeague(H2H);
			storeLeagueData(H2H);
			teamsUpdateStatus(H2H.leagueID, Gameweek);
		}
	}

	public void closeConnections() {
		try {
			conn.close();
		}
		catch (Exception e) {
			System.err.println(e);
		}
	}
}

