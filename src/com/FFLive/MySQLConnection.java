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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;









import org.json.simple.JSONArray;
import org.json.simple.JSONValue;

import com.mysql.jdbc.MysqlDataTruncation;
import com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException;

public class MySQLConnection {

	public Connection conn = null;

	public MySQLConnection(String ipAddress, String userName, String password, String database) {
		try {
			// this will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// setup the connection with the DB.
			Main.log.log(4, "Connecting to your MySQL Database...  ");
			conn = DriverManager.getConnection("jdbc:mysql://" + ipAddress + "/?user=" + userName + "&password=" + password + "&useUnicode=true&characterEncoding=utf8");
			Statement statement = conn.createStatement();
			statement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + database);
			statement.executeUpdate("USE " + database);
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS leagues (ID INT NOT NULL UNIQUE, name VARCHAR(30) NOT NULL, type VARCHAR(10) NOT NULL)");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS status (LeagueID INT NOT NULL, Gameweek INT NOT NULL, starts VARCHAR(20), kickOff VARCHAR(20), started VARCHAR(2) DEFAULT 'N', ends VARCHAR(20) , ended VARCHAR(2) DEFAULT 'N', teamsStored VARCHAR(2) DEFAULT 'N', postGwUpdate VARCHAR(2) DEFAULT 'N', UNIQUE (leagueID, Gameweek))");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS webFront (page VARCHAR(50) UNIQUE, status VARCHAR(300), currGameweek INT)");
			statement.executeUpdate("INSERT INTO webFront (page, status) values ('index', 'Loading...')  ON DUPLICATE KEY UPDATE status = 'loading'");
			Main.log.log(4, "Connected!\n", 0);
			statement.close();
		}
		catch (ClassNotFoundException c) {
			Main.log.ln(1);
			Main.log.log(1, "Critical Program Failure!\n" + c + "\n");
			Main.log.log(9, c); 
			System.exit(1001);
		}
		catch (SQLException sql) {
			Main.log.ln(1);
			Main.log.log(1, "MySQL Error Encountered, Check that your details are correct and that your user has permissions to edit DB " + database + "\n");
			Main.log.log(1, sql.toString());
			Main.log.log(9, sql);
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
			Main.log.log(2, "Error Updating Web Status Table.. " + sql + "\n");
			Main.log.log(9,sql);
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
			Main.log.ln(1);
			Main.log.log(1,"Error setting Web Front Gameweek -- " + sql + "\n");
		}
	}

	public void addStatus(List<String> leagueIDs) {

		Main.log.log(4,"Adding/Updating leagues...  ");
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
			createGWTables(gw);


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


			//Make previous week tables
			if (gw != 1) {
				int prevGW = gw -1;
				Fixtures prevFixture = new Fixtures(prevGW);
				prevFixture.loadFixtures();

				createGWTables(prevGW);

				for (String leagueID: leagueIDs) {
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


			Main.log.log(4,"Ready!",0);

		} catch (SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Updating Leagues and Making Tables.. " + e + "\n");
			Main.log.log(9,e);
			System.exit(1006);
		}
	}

	public void removeLeague(String leagueID, int gameweek) {
		try {
			PreparedStatement delStatus = conn.prepareStatement("DELETE FROM status WHERE LeagueID = ? AND Gameweek = ?");
			delStatus.setString(1, leagueID);
			delStatus.setInt(2, gameweek);
			PreparedStatement delTeams = conn.prepareStatement("DELETE FROM ? WHERE leagueID = ?");
			delTeams.setString(1, "leagues_teamsGW" +gameweek);
			delTeams.setString(2, leagueID);
			delStatus.close();
			delTeams.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2,"Error Removing Leagues.. " + sql + "\n");
			Main.log.log(9,sql);
		}
	}

	public Map<String, List<String>> statusCheck() {
		Main.log.log(4,"Checking Status of Stored Leagues...  ");
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
								//Not after post date, updated players and scores
								List<String> prePost = new ArrayList<String>();
								prePost.add(Integer.toString(gw));
								incomplete.put("prePost", prePost);
							}
						}
						//Gameweek not marked as ended, check if now ended
						else if (now.after(endDate)){
							//Gameweek has now ended, check if after post date
							if(now.after(postDate)) {
								//Started and ended, update status table
								generalStatusUpdate(leagueID,gw,"Y", "Y");


								//Now is after post date, update status table and queue for post update

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

								List<String> prePost = new ArrayList<String>();
								prePost.add(Integer.toString(gw));
								incomplete.put("prePost", prePost);

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

								generalStatusUpdate(leagueID,gw,"Y", "Y");

								//Has it already failed a status update
								if(status.getString("postGwUpdate").equals("F")) {
									//Therefore no teams but no status update, get teams for that gw
									if(!incomplete.containsKey("teams")) {
										List<String> teams = new ArrayList<String>();
										teams.add(leagueID + "," + gw);
										incomplete.put("teams",teams);
									}
									else {
										incomplete.get("teams").add(leagueID + "," + gw);
									}
								}
								else {
									//After Post, Queue for Post and Update status table
									if(!incomplete.containsKey("post")) {
										List<String> post = new ArrayList<String>();
										post.add(leagueID + "," + gw);
										incomplete.put("post",post);
									}
									else {
										incomplete.get("post").add(leagueID + "," + gw);
									}
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
								//After End Date, not time for Post, Update Status Table and Update Scores

								generalStatusUpdate(leagueID,gw,"Y", "Y");
								List<String> prePost = new ArrayList<String>();
								prePost.add(Integer.toString(gw));
								incomplete.put("prePost", prePost);
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
								//After End Date, update Status and update scores
								generalStatusUpdate(leagueID,gw,"Y", "Y");
								List<String> prePost = new ArrayList<String>();
								prePost.add(Integer.toString(gw));
								incomplete.put("prePost", prePost);
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
			Main.log.ln(1);
			Main.log.log(1, "Critical Error Updating Status.. " + sql + "\n");
			Main.log.log(9, sql);
			System.exit(1003);
		}
		catch (NullPointerException n) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Data is Missing from the DB, Try Again or Backup and Delete this Table\n");
			Main.log.log(1, n.toString());
			System.exit(1005);
		}
		Main.log.log(4,"Done!\n",0);
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
			Main.log.ln(2);
			Main.log.log(2,"General Status Table Update Failed\n");
			Main.log.log(2, sql);
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
			Main.log.ln(2);
			Main.log.log(2,"Missed Status Table Update Failed\n");
			Main.log.log(2, sql);
		}
	}

	public void postUpdateStatus (int leagueID, int gw) {
		try {
			PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set postGwUpdate='Y' where LeagueID = ? AND Gameweek = ?");
			preparedStmt.setInt(1 , leagueID);
			preparedStmt.setInt(2 , gw);
			preparedStmt.executeUpdate();
			preparedStmt.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2,"Post Status Table Update Failed\n");
			Main.log.log(2, sql);
		}
	}

	public void clearPostUpdate (String gw) {
		try {
			PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set postGwUpdate='N' where Gameweek = ?");
			preparedStmt.setInt(1 , Integer.parseInt(gw));
			preparedStmt.executeUpdate();
			preparedStmt.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2,"Clear Status Table Update Failed\n");
			Main.log.log(2, sql);
		}
	}

	public void teamsUpdateStatus (int leagueID, int gw) {
		try {
			PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set teamsStored='Y' where LeagueID = ? AND Gameweek = ?");
			preparedStmt.setInt(1 , leagueID);
			preparedStmt.setInt(2 , gw);
			preparedStmt.executeUpdate();
			preparedStmt.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2,"Teams Status Table Update Failed\n");
			Main.log.log(2, sql);
		}
	}

	public void clearTeamStatus (String gw) {
		try {
			PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set teamsStored='N' where Gameweek = ?");
			preparedStmt.setInt(1 , Integer.parseInt(gw));
			preparedStmt.executeUpdate();
			preparedStmt.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2,"Clear Status Table Update Failed\n");
			Main.log.log(2, sql);
		}
	}

	public boolean nextGWStarted (String gw) {

		boolean started = true;
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
					started = false;
				}
			}
			statement.close();

		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2,"Error Checking Gameweek Status\n");
			Main.log.log(2, sql);
		}
		return started;
	}

	public void storeTeamGW(Team team, int gw) {
		try {
			PreparedStatement IteamsGW = conn.prepareStatement("INSERT INTO teamsGW? (managerID, teamName, managerName, liveOP, gw, transfers, deductions) values (?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE teamName = ?, managerName = ?, liveOP = ?, gw = ?, transfers = ?, deductions = ?");
			IteamsGW.setInt(1, gw);
			IteamsGW.setString(2, team.managerID);
			IteamsGW.setString(3, team.teamName);
			IteamsGW.setString(4, team.managerName);
			IteamsGW.setInt(5, team.opScore);
			IteamsGW.setInt(6, team.gameWeekScore + team.deductions);
			IteamsGW.setInt(7, team.transfers);
			IteamsGW.setInt(8, team.deductions);
			IteamsGW.setString(9, team.teamName);
			IteamsGW.setString(10, team.managerName);
			IteamsGW.setInt(11, team.opScore);
			IteamsGW.setInt(12, team.gameWeekScore + team.deductions);			
			IteamsGW.setInt(13, team.transfers);
			IteamsGW.setInt(14, team.deductions);

			IteamsGW.executeUpdate();
			IteamsGW.close();

		}
		catch (SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing Teams\n");
			Main.log.log(1, e);
			System.exit(1090);
		}
	}

	public void preStoreTeamGW(Team team, int gw) {
		try {
			PreparedStatement IteamsGW = conn.prepareStatement("INSERT INTO teamsGW? (managerID, teamName, managerName, op) values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE teamName = ?, managerName = ?, op = ?");
			IteamsGW.setInt(1, gw);
			IteamsGW.setString(2, team.managerID);
			IteamsGW.setString(3, team.teamName);
			IteamsGW.setString(4, team.managerName);
			IteamsGW.setInt(5, team.opScore);
			IteamsGW.setString(6, team.teamName);
			IteamsGW.setString(7, team.managerName);
			IteamsGW.setInt(8, team.opScore);

			IteamsGW.executeUpdate();
			IteamsGW.close();

		}
		catch (SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing Teams\n");
			Main.log.log(1, e);
			System.exit(1090);
		}
	}

	public void storeLeagueTeamGWClas (Team team, int leagueID, int gw) {
		try {
			PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, liveLp, livePosition)  values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE liveLp = ?, livePosition = ?");
			ILeaguesTeamsGW.setInt(1, gw);
			ILeaguesTeamsGW.setInt(2, leagueID);
			ILeaguesTeamsGW.setInt(3, Integer.parseInt(team.managerID));
			ILeaguesTeamsGW.setInt(4, team.lpScore);
			ILeaguesTeamsGW.setInt(5, team.position);
			ILeaguesTeamsGW.setInt(6, team.lpScore);
			ILeaguesTeamsGW.setInt(7, team.position);
			ILeaguesTeamsGW.executeUpdate();

			ILeaguesTeamsGW.close();
		}
		catch(SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing Teams\n");
			Main.log.log(1, e);
			System.exit(1091);
		}
	}

	public void storeLeagueTeamGWH2H (Team team, int leagueID, int gw) {
		try {
			PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, liveLp, livePosition, liveWin, liveLoss, liveDraw, livePoints)  values (?, ?, ?, ?, ? ,? ,? ,?) ON DUPLICATE KEY UPDATE liveLp = ?, livePosition = ?, liveWin = ?, liveLoss = ?, liveDraw = ?, livePoints = ?");
			ILeaguesTeamsGW.setInt(1, gw);
			ILeaguesTeamsGW.setInt(2, leagueID);
			ILeaguesTeamsGW.setInt(3, Integer.parseInt(team.managerID));
			ILeaguesTeamsGW.setInt(4, team.lpScore);
			ILeaguesTeamsGW.setInt(5, team.position);
			ILeaguesTeamsGW.setInt(6, team.win);
			ILeaguesTeamsGW.setInt(7, team.loss);
			ILeaguesTeamsGW.setInt(8, team.draw);
			ILeaguesTeamsGW.setInt(9, team.h2hScore);
			ILeaguesTeamsGW.setInt(10, team.lpScore);
			ILeaguesTeamsGW.setInt(11, team.position);
			ILeaguesTeamsGW.setInt(12, team.win);
			ILeaguesTeamsGW.setInt(13, team.loss);
			ILeaguesTeamsGW.setInt(14, team.draw);
			ILeaguesTeamsGW.setInt(15, team.h2hScore);
			ILeaguesTeamsGW.executeUpdate();

			ILeaguesTeamsGW.close();

		}
		catch (SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing Teams\n");
			Main.log.log(1, e);
			System.exit(1092);
		}
	}

	public void preStoreLeagueTeamGWClas (Team team, int leagueID, int gw) {
		try {
			PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, lp, position)  values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE lp = ?, position = ?");
			ILeaguesTeamsGW.setInt(1, gw);
			ILeaguesTeamsGW.setInt(2, leagueID);
			ILeaguesTeamsGW.setInt(3, Integer.parseInt(team.managerID));
			ILeaguesTeamsGW.setInt(4, team.lpScore);
			ILeaguesTeamsGW.setInt(5, team.position);
			ILeaguesTeamsGW.setInt(6, team.lpScore);
			ILeaguesTeamsGW.setInt(7, team.position);
			ILeaguesTeamsGW.executeUpdate();

			ILeaguesTeamsGW.close();
		}
		catch(SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing Teams\n");
			Main.log.log(1, e);
			System.exit(1091);
		}
	}

	public void preStoreLeagueTeamGWH2H (Team team, int leagueID, int gw) {
		try {
			PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, lp, position, wins, loss, draw, points)  values (?, ?, ?, ?, ? ,? ,? ,?) ON DUPLICATE KEY UPDATE lp = ?, position = ?, wins = ?,loss = ?, draw = ?, points = ?");
			ILeaguesTeamsGW.setInt(1, gw);
			ILeaguesTeamsGW.setInt(2, leagueID);
			ILeaguesTeamsGW.setInt(3, Integer.parseInt(team.managerID));
			ILeaguesTeamsGW.setInt(4, team.lpScore);
			ILeaguesTeamsGW.setInt(5, team.position);
			ILeaguesTeamsGW.setInt(6, team.win);
			ILeaguesTeamsGW.setInt(7, team.loss);
			ILeaguesTeamsGW.setInt(8, team.draw);
			ILeaguesTeamsGW.setInt(9, team.h2hScore);
			ILeaguesTeamsGW.setInt(10, team.lpScore);
			ILeaguesTeamsGW.setInt(11, team.position);
			ILeaguesTeamsGW.setInt(12, team.win);
			ILeaguesTeamsGW.setInt(13, team.loss);
			ILeaguesTeamsGW.setInt(14, team.draw);
			ILeaguesTeamsGW.setInt(15, team.h2hScore);
			ILeaguesTeamsGW.executeUpdate();

			ILeaguesTeamsGW.close();

		}
		catch (SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing Teams\n");
			Main.log.log(1, e);
			System.exit(1092);
		}
	}

	public void storeLeaguesData (ClassicLeague CL) {
		try {
			String type = "Classic";
			PreparedStatement leaguesInsert = conn.prepareStatement("INSERT INTO leagues values (?, ?, ?) ON DUPLICATE KEY UPDATE name = ?, type = ?");
			leaguesInsert.setInt(1, CL.leagueID);
			leaguesInsert.setString(2, CL.leagueName);
			leaguesInsert.setString(3, type);
			leaguesInsert.setString(4, CL.leagueName);
			leaguesInsert.setString(5, type);
			leaguesInsert.executeUpdate();

			leaguesInsert.close();
		}
		catch (SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing League Data\n");
			Main.log.log(1, e);
			System.exit(1093);
		}
	}

	public void storeLeaguesData (H2HLeague h2h) {
		try {
			String type = "H2H";
			PreparedStatement leaguesInsert = conn.prepareStatement("INSERT INTO leagues values (?, ?, ?) ON DUPLICATE KEY UPDATE name = ?, type = ?");
			leaguesInsert.setInt(1, h2h.leagueID);
			leaguesInsert.setString(2, h2h.leagueName);
			leaguesInsert.setString(3, type);
			leaguesInsert.setString(4, h2h.leagueName);
			leaguesInsert.setString(5, type);
			leaguesInsert.executeUpdate();

			leaguesInsert.close();
		}
		catch (SQLException e) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing League Data\n");
			Main.log.log(1, e);
			System.exit(1093);
		}
	}

	public void storeLeague(ClassicLeague league) {
		Main.log.log(6,"Storing League '" + league.leagueName + "' to DB...  ");

		storeLeaguesData(league);

		for (Entry<String,Team> entry: league.managerMap.entrySet()) {
			storeLeagueTeamGWClas(entry.getValue(), league.leagueID, league.gameweek);
			storeTeamGW(entry.getValue(), league.gameweek);
		}

		Main.log.log(6,"Done!\n",0);
	}

	public void storeLeague(H2HLeague league) {

		Main.log.log(6,"Storing League '" + league.leagueName + "' to DB...  ");

		storeLeaguesData(league);
		storeFixtures(league);

		for (Entry<String,Team> entry: league.managerMap.entrySet()) {

			storeLeagueTeamGWH2H(entry.getValue(), league.leagueID, league.gameweek);
			storeTeamGW(entry.getValue(), league.gameweek);
		}

		Main.log.log(6,"Done!\n",0);
	}

	public void storeFixtures (H2HLeague league) {
		try {
			//Insert Fixtures
			int id = 1;
			for (Entry<String, String> entry : league.fixtureMap.entrySet()) {
				PreparedStatement ITH2HFixtures = conn.prepareStatement("INSERT INTO H2HGW? (leagueID, home, away, fixtureNo) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE fixtureNo = ?");
				ITH2HFixtures.setInt(1, league.gameweek);
				ITH2HFixtures.setInt(2, league.leagueID);
				ITH2HFixtures.setInt(3, Integer.parseInt(entry.getKey()));
				ITH2HFixtures.setInt(4, Integer.parseInt(entry.getValue()));
				ITH2HFixtures.setInt(5, id);
				ITH2HFixtures.setInt(6, id);
				ITH2HFixtures.executeUpdate();
				id++;
				ITH2HFixtures.close();

			}
		}
		catch(SQLException sql) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Storing Fixtures\n");
			Main.log.log(1, sql);
			System.exit(1094);
		}
	}

	public void preStore (ClassicLeague league) {

		Main.log.log(6,"Storing Pre-Gameweek Data for League '" + league.leagueName + "' to DB...  ");
		int gw = league.gameweek + 1;

		createGWTables(gw);
		storeLeaguesData(league);

		for (Entry<String,Team> entry: league.managerMap.entrySet()) {
			preStoreLeagueTeamGWClas(entry.getValue(), league.leagueID, gw);
			preStoreTeamGW(entry.getValue(), gw);
		}

		Main.log.log(6,"Done!\n",0);


	}

	public void preStore(H2HLeague league) {
		Main.log.log(6,"Storing Pre-Gameweek Data for League '" + league.leagueName + "' to DB...  ");
		int gw = league.gameweek + 1;

		createGWTables(gw);
		storeLeaguesData(league);

		for (Entry<String,Team> entry: league.managerMap.entrySet()) {
			preStoreLeagueTeamGWH2H(entry.getValue(), league.leagueID, gw);
			preStoreTeamGW(entry.getValue(), gw);
		}

		Main.log.log(6,"Done!\n",0);


	}

	public void createGWTables (int gw) {
		Main.log.log(7,"Creating GW Tables for GW: " + gw + "\n");
		try {
			PreparedStatement CTleagueTeamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS leagues_teamsGW? (leagueID INT NOT NULL, managerID INT NOT NULL, lp INT DEFAULT 0, position INT DEFAULT 0, wins INT DEFAULT 0, loss INT DEFAULT 0, draw INT DEFAULT 0, points INT DEFAULT 0, fixture VARCHAR(2), livePoints INT, liveLP INT DEFAULT 0, liveWin INT DEFAULT 0, liveLoss INT DEFAULT 0, liveDraw INT DEFAULT 0, livePosition INT, posDifferential VARCHAR(10) DEFAULT '-', UNIQUE (leagueID, managerID))");
			CTleagueTeamsGW.setInt(1, gw);
			CTleagueTeamsGW.executeUpdate();

			PreparedStatement CTteamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS teamsGW? (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), op INT DEFAULT 0, gw INT DEFAULT 0, liveOP INT DEFAULT 0, benchScore INT DEFAULT 0, transfers INT DEFAULT 0, deductions INT DEFAULT 0, GkID INT DEFAULT 0, DefID1 INT DEFAULT 0, DefID2 INT DEFAULT 0, DefID3 INT DEFAULT 0, DefID4 INT DEFAULT 0, DefID5 INT DEFAULT 0, MidID1 INT DEFAULT 0, MidID2 INT DEFAULT 0, MidID3 INT DEFAULT 0, MidID4 INT DEFAULT 0, MidID5 INT DEFAULT 0, ForID1 INT DEFAULT 0, ForID2 INT DEFAULT 0, ForID3 INT DEFAULT 0, BenchID1 INT DEFAULT 0, BenchID2 INT DEFAULT 0, BenchID3 INT DEFAULT 0, BenchID4 INT DEFAULT 0, captainID INT DEFAULT 0, viceCaptainID INT DEFAULT 0, transferIn VARCHAR(100) DEFAULT 'NONE', transferOut VARCHAR(100) DEFAULT 'NONE')");
			CTteamsGW.setInt(1, gw);
			CTteamsGW.executeUpdate();

			PreparedStatement CTPlayersGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS playersGW? (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, transferOut INT DEFAULT 0 NOT NULL, firstName VARCHAR(100), lastName VARCHAR(100), webName VARCHAR(100), score INT, gameweekBreakdown VARCHAR(20000), breakdown VARCHAR(20000), teamName VARCHAR(40), teamID INT, currentFixture VARCHAR(40), nextFixture VARCHAR(40), status VARCHAR(10), news VARCHAR(20000), photo VARCHAR(30))");
			CTPlayersGW.setInt(1,gw);
			CTPlayersGW.executeUpdate();

			PreparedStatement CTH2HFixture = conn.prepareStatement("CREATE TABLE IF NOT EXISTS H2HGW? (leagueID INT NOT NULL, home VARCHAR(30), away VARCHAR(30), fixtureNo INT, UNIQUE(leagueID, home, away))");
			CTH2HFixture.setInt(1,gw);
			CTH2HFixture.executeUpdate();

			CTleagueTeamsGW.close();
			CTteamsGW.close();
			CTPlayersGW.close();
			CTH2HFixture.close();
		}
		catch (SQLException sql) {
			Main.log.ln(1);
			Main.log.log(1,"Critical Error Creating Gameweek Tables\n");
			Main.log.log(1, sql);
			System.exit(1070);
		}
	}

	public void storeLeagueData (ClassicLeague league) {
		Main.log.log(6,"Storing Team Data for League '" + league.leagueName + "'...  ");
		for (Entry <String, Team> entry : league.managerMap.entrySet()) {
			storeTeamData(entry.getValue());
		}
		Main.log.log(6,"Done!\n",0);
	}

	public void storeLeagueData (H2HLeague league) {
		Main.log.log(6,"Storing Team Data for League '" + league.leagueName + "'...  ");
		for (Entry <String, Team> entry : league.managerMap.entrySet()) {
			storeTeamData(entry.getValue());
		}
		Main.log.log(6,"Done!\n",0);
	}


	public void storeTeamData (Team team) {

		try {
			if (!team.managerID.equals("0")) {

				PreparedStatement updateTeam = conn.prepareStatement("UPDATE teamsGW? set "
						+ "GkID = ?, DefID1 = ?, DefID2 = ?, DefID3 = ?, DefID4 = ? ,DefID5 = ?, "
						+ "MidID1 = ?, MidID2 = ?, MidID3 = ?, MidID4 = ?, MidID5 = ?, "
						+ "ForID1 = ?, ForID2 = ?, ForID3 = ?, "
						+ "BenchID1 = ?, BenchID2 = ?, BenchID3 = ?, BenchID4 = ?, "
						+ "captainID = ?, viceCaptainID = ? WHERE managerID = ?");

				int n = 1;
				updateTeam.setInt(n++, team.GW);
				for(String playerID : team.goalkeeper) {
					updateTeam.setInt(n++, Integer.parseInt(playerID));
				}
				for(String playerID : team.defenders) {
					updateTeam.setInt(n++, Integer.parseInt(playerID));
				}
				for(String playerID : team.midfield) {
					updateTeam.setInt(n++, Integer.parseInt(playerID));
				}
				for(String playerID : team.forwards) {
					updateTeam.setInt(n++, Integer.parseInt(playerID));
				}
				for(String playerID : team.bench) {
					updateTeam.setInt(n++, Integer.parseInt(playerID));
				}
				for(String playerID : team.captains) {
					updateTeam.setInt(n++, Integer.parseInt(playerID));
				}
				updateTeam.setInt(n++, Integer.parseInt(team.managerID));

				updateTeam.executeUpdate();
				updateTeam.close();
			}
		}
		catch (SQLException e) {
			Main.log.ln(2);
			Main.log.log(2,"Error Storing Team Data... " + e + "\n");
			Main.log.log(9,e);
		}

	}

	public void updatePlayers(int gw, boolean namesOnly) {
		Main.log.log(5,"Updating All Player Info...                   \r");
		try {

			PreparedStatement SelPID = conn.prepareStatement("SELECT playerID FROM playersGW?");
			SelPID.setInt(1, gw);
			ResultSet playerList = SelPID.executeQuery();
			//statement.executeQuery("SELECT playerID FROM playersGW" + gw);
			int counter = 0;
			while (playerList.next()) {
				if(counter%30 == 0) {
					Main.log.log(5,"Updating All Player Info.                   \r");
				}
				else if (counter%30 == 10) {
					Main.log.log(5,"Updating All Player Info..                  \r");
				}
				else if (counter%30 == 20) {
					Main.log.log(5,"Updating All Player Info...                 \r");
				}
				counter++;
				try {
					Player player = new Player(Integer.toString(playerList.getInt("playerID")),gw);
					player.getPlayer();
					if(namesOnly) {
						PreparedStatement UpPGw = conn.prepareStatement("UPDATE playersGW? set "
								+ "firstName =?, "
								+ "lastName =?, "
								+ "webName = ?, "
								+ "teamID = ?, "
								+ "teamName = ?, "
								+ "photo = ? "
								+ "WHERE playerID = ?");
						UpPGw.setInt(1, gw);
						UpPGw.setString(2, player.firstName);
						UpPGw.setString(3, player.lastName);
						UpPGw.setString(4, player.playerName);
						UpPGw.setInt(5, player.teamNumber);
						UpPGw.setString(6, player.playerTeam);
						UpPGw.setString(7, player.photo);
						UpPGw.setInt(8, Integer.parseInt(player.playerID));
						UpPGw.executeUpdate();

						UpPGw.close();
					}
					else {
						PreparedStatement UpPGw = conn.prepareStatement("UPDATE playersGW? set "
								+ "firstName =?, "
								+ "lastName =?, "
								+ "webName = ?, "
								+ "score = ?, "
								+ "gameweekBreakdown = ?, "
								+ "teamID = ?, "
								+ "teamName = ?, "
								+ "currentFixture = ?, "
								+ "nextFixture = ?, "
								+ "status = ?, "
								+ "news = ?, "
								+ "photo = ? "
								+ "WHERE playerID = ?");
						UpPGw.setInt(1, gw);
						UpPGw.setString(2, player.firstName);
						UpPGw.setString(3, player.lastName);
						UpPGw.setString(4, player.playerName);
						UpPGw.setInt(5, player.playerScore);
						UpPGw.setString(6, player.gameweekBreakdown);
						UpPGw.setInt(7, player.teamNumber);
						UpPGw.setString(8, player.playerTeam);
						UpPGw.setString(9, player.currentFixture);
						UpPGw.setString(10, player.nextFixture);
						UpPGw.setString(11, player.status);
						UpPGw.setString(12, player.news);
						UpPGw.setString(13, player.photo);
						UpPGw.setInt(14, Integer.parseInt(player.playerID));
						UpPGw.executeUpdate();

						UpPGw.close();
					}
				}
				catch (MysqlDataTruncation g) {
					Main.log.ln(4);
					Main.log.log(4, g + " - while updating Players, Skipping Player\n");
				}
				catch (SQLException f) {
					Main.log.ln(2);
					Main.log.log(2, "Error Updating Players, Skipping Player\n");
					Main.log.log(2,f.toString());
					Main.log.log(9,f);
				}
				catch (NumberFormatException n) {
					Main.log.ln(2);
					Main.log.log(2, "Invalid Player ID, Skipping Player\n");
					Main.log.log(2,n.toString());
					Main.log.log(9,n);
				}
			}

		}

		catch (Exception e) {
			Main.log.ln(1);
			Main.log.log(1, "Critical Error Updating Players\n");
			Main.log.log(1,e.toString());
			Main.log.log(9,e);
			System.exit(1097);
		}
		Main.log.log(5,"Updating All Player Info... Done!              \r");
	}

	public void generatePlayerList(int gw) {

		Main.log.log(5,"Generating Player List.                  \r");

		try {

			PreparedStatement resetPlayerCounts = conn.prepareStatement("UPDATE playersGW? SET playerCount = 0, transferOut = 0");
			resetPlayerCounts.setInt(1, gw);
			resetPlayerCounts.executeUpdate();

			String[] positions = {"GkID","DefID1","DefID2","DefID3","DefID4","DefID5","MidID1","MidID2","MidID3","MidID4","MidID5","ForID1","ForID2","ForID3","BenchID1","BenchID2","BenchID3","BenchID4"};
			//for(String temp: positions) {

			PreparedStatement selectPS = conn.prepareStatement("SELECT * from teamsGW?");
			//selectPS.setString(1, temp);
			selectPS.setInt(1, gw);

			ResultSet rs = selectPS.executeQuery();

			int counter = 1;
			while (rs.next()) {
				if(counter%30 == 10) {
					Main.log.log(5,"Generating Player List..                     \r");
				}
				if (counter%30 == 20) {
					Main.log.log(5,"Generating Player List...                    \r");
				}
				if (counter%30 == 0) {
					Main.log.log(5,"Generating Player List.                      \r");
				}
				counter ++;
				if(rs.getInt("managerID")== 0) {
					PreparedStatement InsertPS = conn.prepareStatement("INSERT INTO playersGW? (playerID) values (?) ON DUPLICATE KEY UPDATE playerCount = playerCount + 1");
					InsertPS.setInt(1, gw);
					InsertPS.setInt(2, -1);
					InsertPS.executeUpdate();
					InsertPS.close();

				}
				else {
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
					String[] trans = {"transferIn", "transferOut"};
					for (String temp: trans) {
						String[] transfers = rs.getString(temp).split(",");
						if (!transfers[0].equals("NONE")) {
							for (String inOut: transfers) {
								PreparedStatement InsertPS = conn.prepareStatement("INSERT INTO playersGW? (playerID, playerCount, transferOut) values (?, 0, transferOut + 1) ON DUPLICATE KEY UPDATE transferOut = transferOut + 1");
								InsertPS.setInt(1, gw);
								InsertPS.setInt(2, Integer.parseInt(inOut));
								InsertPS.executeUpdate();
								InsertPS.close();
							} 
						}
					}
				}
			}
			selectPS.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2, "Error Generating Player List! " + sql + "\n");
			Main.log.log(9, sql);
		}
		Main.log.log(5,"Generating Player List... Done!                     \r");
	}

	public void updateScores(int gameweek) {

		Main.log.log(5,"Updating Gameweek Scores...            \r");
		//int gameweek = Integer.parseInt(gw);
		try {

			PreparedStatement SManId = conn.prepareStatement("SELECT managerID from teamsGW?");
			SManId.setInt(1, gameweek);
			ResultSet teams = SManId.executeQuery();

			String[] positions = {"GkID","DefID1","DefID2","DefID3","DefID4","DefID5","MidID1","MidID2","MidID3","MidID4","MidID5","ForID1","ForID2","ForID3"};
			//String[] benchPos = {"BenchID1","BenchID2","BenchID3","BenchID4"};
			while(teams.next()) {
				int manId = teams.getInt("managerID");
				int gwScore = 0;
				if (manId == 0) {
					//The average team so update its gameweek score with the dummy average player
					PreparedStatement SAvPlayer = conn.prepareStatement("SELECT score from playersGW? WHERE playerID = -1");
					SAvPlayer.setInt(1, gameweek);
					ResultSet avPlayer = SAvPlayer.executeQuery();
					while(avPlayer.next()) {
						gwScore = avPlayer.getInt("score");
					}

					SAvPlayer.close();
					PreparedStatement UAvScore = conn.prepareStatement("UPDATE teamsGW? set gw = ? WHERE managerID = 0");
					UAvScore.setInt(1, gameweek);
					UAvScore.setInt(2, gwScore);
					UAvScore.executeUpdate();
					UAvScore.close();

				}
				else {

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

					//Adds on the captain score again, unless Min played = 0 then it adds on the vice captain score

					PreparedStatement captain = conn.prepareStatement("SELECT score, gameweekBreakdown FROM playersGW? JOIN teamsGW? on playersGW?.playerid = teamsGW?.captainID WHERE managerID = ?");
					captain.setInt(1, gameweek);
					captain.setInt(2, gameweek);
					captain.setInt(3, gameweek);
					captain.setInt(4, gameweek);
					captain.setInt(5, manId);
					ResultSet RSScore = captain.executeQuery();
					while(RSScore.next()) {
						int capScore = RSScore.getInt("score");
						//Parse the Breakdown to JSON, find the right part of the array and check if the minutes is 0
						String capBreakdown = RSScore.getString("gameweekBreakdown");
						JSONArray breakdown =  (JSONArray)JSONValue.parse(capBreakdown);

						for(int x = 0; x<breakdown.size(); x++) {
							String parts = breakdown.get(x).toString();
							if(parts.contains("Minutes")) {
								if(parts.split(",")[1].equals("0")) {
									//Mins = 0 so double Vice score and add to GW score
									//System.out.println("True " + parts);
									PreparedStatement viceCap = conn.prepareStatement("SELECT score from playersGW? JOIN teamsGW? on playersGW?.playerid = teamsGW?.viceCaptainID WHERE managerID = ?");
									viceCap.setInt(1, gameweek);
									viceCap.setInt(2, gameweek);
									viceCap.setInt(3, gameweek);
									viceCap.setInt(4, gameweek);
									viceCap.setInt(5, manId);
									ResultSet RSViceScore = viceCap.executeQuery();
									while(RSViceScore.next()){								
										gwScore += RSViceScore.getInt("score");
									}
									viceCap.close();
								}
								else {
									//System.out.println("False " + parts);
									gwScore += capScore;
								}
							}
						}

					}
					RSScore.close();

					//Check for transfer Deductions and apply them.
					PreparedStatement STransfers = conn.prepareStatement("SELECT deductions FROM teamsGW? WHERE managerID = ?");
					STransfers.setInt(1, gameweek);
					STransfers.setInt(2, manId);
					ResultSet transfers = STransfers.executeQuery();

					while(transfers.next()) {
						gwScore += transfers.getInt("deductions");
					}
					STransfers.close();

					//Add bench scores up for stats
					int benchScore = 0;
					PreparedStatement Sbench = conn.prepareStatement("SELECT score from playersGW? JOIN teamsGW? ON playersGW?.playerid = teamsGW?.BenchID1 OR playersGW?.playerid = teamsGW?.BenchID2 "
							+ "OR playersGW?.playerid = teamsGW?.BenchID3 OR playersGW?.playerid = teamsGW?.BenchID4 WHERE managerID = ?");
					Sbench.setInt(1, gameweek);
					Sbench.setInt(2, gameweek);
					Sbench.setInt(3, gameweek);
					Sbench.setInt(4, gameweek);
					Sbench.setInt(5, gameweek);
					Sbench.setInt(6, gameweek);
					Sbench.setInt(7, gameweek);
					Sbench.setInt(8, gameweek);
					Sbench.setInt(9, gameweek);
					Sbench.setInt(10, gameweek);
					Sbench.setInt(11, manId);
					ResultSet benchPlayers = Sbench.executeQuery();

					while(benchPlayers.next()) {
						benchScore += benchPlayers.getInt("score");
					}

					//Update Scores
					PreparedStatement UScore = conn.prepareStatement("UPDATE teamsGW? set gw = ?, liveOP = op + ?, benchScore = ? WHERE managerID = ?");
					UScore.setInt(1, gameweek);
					UScore.setInt(2, gwScore);
					UScore.setInt(3, gwScore);
					UScore.setInt(4, benchScore);
					UScore.setInt(5, manId);
					UScore.executeUpdate();
					UScore.close();
				}
				PreparedStatement ULeagueScore = conn.prepareStatement("UPDATE leagues_teamsGW? set liveLP = lp + ? WHERE managerID = ?");
				ULeagueScore.setInt(1, gameweek);
				ULeagueScore.setInt(2, gwScore);
				ULeagueScore.setInt(3, manId);
				ULeagueScore.executeUpdate();
				ULeagueScore.close();
			}
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2, "Error Updating Scores" + sql + "\n");
			Main.log.log(9, sql);
		}


		//Update Fixture Scores
		fixtureScores(gameweek,0);



		Main.log.log(5,"Scores Updated!                                                  \r");
		updatePositions(gameweek, 0);
		Main.log.log(5,"Scores and Positions Updated!                                    \r");

	}

	public void fixtureScores (int gameweek, int leagueID) {
		//League ID 0 Means all leagues

		Main.log.log(5,"Updating Fixture Scores...                    \r");

		try {
			PreparedStatement SFix;
			if(leagueID==0) {
				SFix = conn.prepareStatement("SELECT * FROM H2HGW?");
				SFix.setInt(1, gameweek);
			}
			else {
				SFix = conn.prepareStatement("SELECT * FROM H2HGW? WHERE leagueID = ?");
				SFix.setInt(1, gameweek);
				SFix.setInt(2, leagueID);
			}

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
					Main.log.ln(2);
					Main.log.log(2,"There has been a problem with the Gameweek Scores...   \n");
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
			Main.log.ln(2);
			Main.log.log(2,"There is a problem with the fixtures or the team list...  \n");
			Main.log.log(9,a);
		}
		catch (MySQLSyntaxErrorException p) {
			Main.log.ln(2);
			Main.log.log(2, "Error Updating Fixtures, No H2H Leagues to Update? " + p + "\n"); 
			Main.log.log(9,p);
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2, "Error Updating Fixture Scores " + sql + "\n");
			Main.log.log(9, sql);
		}
	}

	public void updatePositions (int gameweek, int league) {
		//UPDATE positions
		//Use league = 0 for all Leagues
		try {
			Main.log.log(5,"Updating Positions...                            \r");

			PreparedStatement SLeagues;
			if(league==0) {
				SLeagues = conn.prepareStatement("SELECT ID, type FROM leagues");
			}
			else {
				SLeagues = conn.prepareStatement("SELECT ID, type FROM leagues WHERE ID = ?");
				SLeagues.setInt(1, league);
			}

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
					int prevLP = -10000;
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
						if (allTeams.getInt("position") == 0) {
							posDiff = "-";
						}
						else if (position > allTeams.getInt("position")) {
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
					int prevLP = -10000;
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
					Main.log.ln(2);
					Main.log.log(2,"Error updating " + leagueID + "of type " + type + "\n");
				}
			}
			SLeagues.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2, "Error Updating Positions " + sql + "\n");
			Main.log.log(9,sql);
		}
		Main.log.log(5,"Updated Positions!                                    \r");

	}

	public void postBenchScores(int gw) {

		Main.log.log(5, "Updating Bench Scores...                   \r");
		//int gameweek = Integer.parseInt(gw);
		try {
			PreparedStatement SManId = conn.prepareStatement("SELECT managerID from teamsGW?");
			SManId.setInt(1, gw);
			ResultSet teams = SManId.executeQuery();
			while(teams.next()) {
				//Add bench scores up for stats
				int manId = teams.getInt("managerID");
				int benchScore = 0;
				PreparedStatement Sbench = conn.prepareStatement("SELECT score from playersGW? JOIN teamsGW? ON playersGW?.playerid = teamsGW?.BenchID1 OR playersGW?.playerid = teamsGW?.BenchID2 "
						+ "OR playersGW?.playerid = teamsGW?.BenchID3 OR playersGW?.playerid = teamsGW?.BenchID4 WHERE managerID = ?");
				Sbench.setInt(1, gw);
				Sbench.setInt(2, gw);
				Sbench.setInt(3, gw);
				Sbench.setInt(4, gw);
				Sbench.setInt(5, gw);
				Sbench.setInt(6, gw);
				Sbench.setInt(7, gw);
				Sbench.setInt(8, gw);
				Sbench.setInt(9, gw);
				Sbench.setInt(10, gw);
				Sbench.setInt(11, manId);
				ResultSet benchPlayers = Sbench.executeQuery();

				while(benchPlayers.next()) {
					benchScore += benchPlayers.getInt("score");
				}
				//Update Scores
				PreparedStatement UScore = conn.prepareStatement("UPDATE teamsGW? set benchScore = ? WHERE managerID = ?");
				UScore.setInt(1, gw);
				UScore.setInt(2, benchScore);
				UScore.setInt(3, manId);
				UScore.executeUpdate();
				UScore.close();
				Sbench.close();
			}
			SManId.close();
			teams.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2, "Error Updating Bench Scores " + sql + "\n");
			Main.log.log(9,sql);

		}

	}

	public void fetchMissingGWs (boolean showMissingGW, boolean fullRebuild) {

		//Changing Logging Level to 4 if set as default to make console output clearer
		boolean logChanged = false;
		if(Main.log.logLevel == 5) {
			//Change Level if it is default
			Main.log.logLevel(4);
			logChanged = true;
		}

		Fixtures currentGW = new Fixtures();
		currentGW.loadFixtures();
		//Get all Team Data for all previous GW

		if(fullRebuild) {
			Main.log.log(4,"Rebuilding All GW Data.. This May Take Some Time...\n");
		}
		else {
			Main.log.log(4,"Loading All Missing Gameweeks.. This May Take Some Time...\n");
		}

		int gw = Integer.parseInt(currentGW.gameweek);
		if(!currentGW.gameweekStarted()) {
			gw--;
		}

		try {
			//Get all of the IDs for Teams Currently In For This Week
			PreparedStatement getTeams = conn.prepareStatement("SELECT managerID FROM teamsGW?");
			getTeams.setInt(1, gw);
			ResultSet currentTeams = getTeams.executeQuery();

			Collection<Integer> allTeams = new ArrayList<Integer>();			
			while(currentTeams.next()) {
				allTeams.add(currentTeams.getInt("managerID"));
			}

			if(allTeams.isEmpty()) {
				Main.log.log(3,"Please run again without arguments to fetch this weeks teams...\n");
			}
			else {
				//Start from 1 to current GW
				int lastGW = gw-1;
				for(int x = 1; x < gw; x++) {	
					Main.log.log(4,"Processing Week " + x + "/" + lastGW + "...                                        \r");
					Collection<Integer> copyAllTeams = new ArrayList<Integer>();
					copyAllTeams.addAll(allTeams);

					//Create Tables if not Exist
					createGWTables(x);

					//If you want a full rebuild, do not remove existing teams
					if(!fullRebuild) {
						PreparedStatement thisWeek = conn.prepareStatement("SELECT managerID FROM teamsGW?");
						thisWeek.setInt(1, x);
						ResultSet teams = thisWeek.executeQuery();

						//Add This Week of Teams into a Collection then remove the overlaps
						Collection<Integer> currentGWTeams = new ArrayList<Integer>();
						while(teams.next()) {
							currentGWTeams.add(teams.getInt("managerID"));
						}

						//This leaves the left over of missing teams
						copyAllTeams.removeAll(currentGWTeams);
					}

					//If you just want to see what gameweeks were missing you would launch this as true
					if(showMissingGW) {
						Main.log.log(4,"Missing Teams:" + copyAllTeams + "\n");
					}		
					else {
						Main.log.log(6,"Missing Teams:" + copyAllTeams + "\n");
						for (Integer missingTeam : copyAllTeams) {
							Team team = new Team(Integer.toString(missingTeam), x);
							team.getTeam();
							if (team.managerName != null) {
								storeTeamGW(team, x);
								storeTeamData(team);
							}
							else {
								Main.log.log(6,"Missing Team ID:" + team.managerID + " for GW:" + x + "\n");
							}
						}
						//Generate a Player List for the teams and update the players with names, no scores (will be out of date)
						//TODO Read the JSON for Previous Gameweeks (Will be annoying)
						Main.log.log(4,"Processing Week " + x + "/" + lastGW + "... Generating Player List...            \r");
						generatePlayerList(x);
						Main.log.log(4,"Processing Week " + x + "/" + lastGW + "... Updating Player Scores...            \r");
						updatePlayers(x, true);
						makePlayerGraph(gw);
						Main.log.log(4,"Processing Week " + x + "/" + lastGW + "... Done!                                \n");
					}
				}
			}
		}
		catch (SQLException e) {
			Main.log.ln(2);
			Main.log.log(2, "Error Fetching Missing Teams.. " + e + "\n");
			Main.log.log(9,e);
		}

		if(logChanged) {
			//Change Level back to default
			Main.log.logLevel(5);
		}

	}

	public void rebuildGWScores() {
		Fixtures currentGW = new Fixtures();
		currentGW.loadFixtures();
		int gw = Integer.parseInt(currentGW.gameweek);
		if(!currentGW.gameweekStarted()) {
			gw--;
		}
		rebuildGWScores(gw);
	}

	public void rebuildGWScores (int gw) {
		try {
			//Rebuild OP Scores
			Main.log.log(5,"Updating GW Scores...                                     \r");
			for(int x = 1; x < gw; x++) {
				Main.log.log(5,"Updating GW Scores... GW:" + x + "/" + gw + "...                        \r");				
				PreparedStatement getGWTeams = conn.prepareStatement("SELECT managerID, op, liveOP, gw FROM teamsGW?");
				getGWTeams.setInt(1, x);
				ResultSet currGWTeams = getGWTeams.executeQuery();
				while(currGWTeams.next()) {
					int newScore = currGWTeams.getInt("gw") + currGWTeams.getInt("op");
					PreparedStatement updateThisGW = conn.prepareStatement("UPDATE teamsGW? SET liveOP = ? WHERE managerID = ?");
					updateThisGW.setInt(1,x);
					updateThisGW.setInt(2, newScore);
					updateThisGW.setInt(3, currGWTeams.getInt("managerID"));
					updateThisGW.executeUpdate();
					updateThisGW.close();

					PreparedStatement updateNextGW = conn.prepareStatement("UPDATE teamsGW? SET op = ? WHERE managerID = ?");
					updateNextGW.setInt(1,x+1);
					updateNextGW.setInt(2, newScore);
					updateNextGW.setInt(3, currGWTeams.getInt("managerID"));
					updateNextGW.executeUpdate();
					updateNextGW.close();
				}
				getGWTeams.close();
				currGWTeams.close();
			}
		}
		catch(SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2,"Error Rebuilding GW Scores..." + sql + "\n");
			Main.log.log(9,sql);
		}
	}

	public void contructLeagueData (String leagueIDsGWs) {

		boolean logChanged = false;
		if(Main.log.logLevel == 5) {
			//Changing Logging Level for cleaner console output
			Main.log.logLevel(4);
			logChanged = true;
		}
		//New Leagues Construct
		Leagues leagues = new Leagues ();

		String[] rebuild = leagueIDsGWs.split(",");
		for(String leagueIDGW : rebuild) {
			String[] splitLeagueIDGW = leagueIDGW.split("/");
			if (splitLeagueIDGW.length != 2) {
				Main.log.log(2,"Invalid Format given for buildLeague for " + leagueIDGW + "- Input in format leagueID/Gameweek with leagues split by commas (,), skipping...\n");
			}
			else {
				try {
					leagues.addLeague(Integer.parseInt(splitLeagueIDGW[0].trim()), Integer.parseInt(splitLeagueIDGW[1].trim()));
					Main.log.log(5,"Rebuilding All League Data for League:" + splitLeagueIDGW[0] + " Starting from GW:" + splitLeagueIDGW[1] + "\n");				
				}
				catch (NumberFormatException e) {
					Main.log.log(2,"Please confirm your League ID and Gameweek are valid, in the form leagueID/Gameweek and are numbers\n");
					Main.log.log(2,e.toString());
				}
			}
		}



		for(ClassicLeague classicLeague: leagues.classicLeague) {
			rebuildLeague(classicLeague);
		}
		for(H2HLeague H2HLeague: leagues.h2hLeague) {
			rebuildLeague(H2HLeague);
		}

		if(logChanged) {
			//Returning to Default
			Main.log.logLevel(5);
		}
	}

	public void rebuildLeague (ClassicLeague cl) {
		Main.log.log(5,"Rebuilding League " + cl.leagueID + "\r");

		cl.loadLeague();
		storeLeaguesData(cl);

		//Starting from starting gw

		for (int gw = cl.startingGameweek; gw < cl.gameweek; gw++) {


			Main.log.log(4,"Rebuilding League " + cl.leagueID + " - GW:" + gw + "/" + cl.gameweek + "...                                  \r");

			//Makes Sure the right tables exist
			createGWTables(gw);
			createGWTables(gw+1);
			try {
				for(String manIDs: cl.managerMap.keySet()) {

					PreparedStatement getTeam = conn.prepareStatement("SELECT managerID, gw FROM teamsGW? WHERE managerID = ?");
					getTeam.setInt(1, gw);
					getTeam.setInt(2, Integer.parseInt(manIDs));
					ResultSet teamData = getTeam.executeQuery();
					while(teamData.next()) {
						//Set This Week
						PreparedStatement ltgw = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, liveLP) VALUES (?, ?, lp + ?) ON DUPLICATE KEY UPDATE liveLP = lp + ?");
						ltgw.setInt(1, gw);
						ltgw.setInt(2, cl.leagueID);
						ltgw.setInt(3, teamData.getInt("managerID"));
						ltgw.setInt(4, teamData.getInt("gw"));
						ltgw.setInt(5, teamData.getInt("gw"));
						ltgw.executeUpdate();

						ltgw.close();

					}

					teamData.close();
					getTeam.close();
				}

				//Finished Week, Update Positions and Position Differentials
				Main.log.log(4,"Rebuilding League " + cl.leagueID + " - GW:" + gw + "/" + cl.gameweek + "... Updating Positions               \r");
				updatePositions(gw, cl.leagueID);

				
				//Set Next Week
				PreparedStatement ltgwNextWeek = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, lp, position) SELECT leagueID, managerID, liveLP, livePosition FROM leagues_teamsGW? WHERE leagueID = ? ON DUPLICATE KEY UPDATE lp = leagues_teamsGW?.liveLP, position = leagues_teamsGW?.livePosition");
				ltgwNextWeek.setInt(1, gw+1);
				ltgwNextWeek.setInt(2, gw);
				ltgwNextWeek.setInt(3, cl.leagueID);
				ltgwNextWeek.setInt(4, gw);
				ltgwNextWeek.setInt(5, gw);
				ltgwNextWeek.executeUpdate();

				ltgwNextWeek.close();
			}
			catch (NumberFormatException n) {
				Main.log.ln(2);
				Main.log.log(2,"Error Rebuilding CL " + cl.leagueName + " :" + n + "\n");
			} 
			catch (SQLException e) {
				Main.log.ln(2);
				Main.log.log(2,"Error Rebuilding CL " + cl.leagueName + " :" + e + "\n");
			}
		}
		Main.log.log(4,"Rebuilding League " + cl.leagueID + " - Updated!                                              \n");

	}

	public void rebuildLeague (H2HLeague h2h) {

		Main.log.log(5,"Rebuilding League " + h2h.leagueID + "\r");

		h2h.loadH2HLeague();
		storeLeaguesData(h2h);

		Main.log.log(4,"Rebuilding League " + h2h.leagueID + " - Fetching Fixtures                                       \r");

		//Fill in all the fixtures
		Map<Integer, Map<String,String>> allFixtures = h2h.loadAllFixtures(h2h.gameweek);
		try {
			for (Entry<Integer, Map<String,String>> gwFixtures :allFixtures.entrySet() ) {

				int x = 0;
				for(Entry<String,String> gwFixture : gwFixtures.getValue().entrySet()){
					x++;
					PreparedStatement insertFixtures = conn.prepareStatement("INSERT INTO h2hgw? (leagueID, home, away, fixtureNo) VALUES (?,?,?,?) ON DUPLICATE KEY UPDATE fixtureNo = ?");
					insertFixtures.setInt(1, gwFixtures.getKey());
					insertFixtures.setInt(2, h2h.leagueID);
					insertFixtures.setString(3, gwFixture.getKey());
					insertFixtures.setString(4, gwFixture.getValue());
					insertFixtures.setInt(5, x);
					insertFixtures.setInt(6, x);
					insertFixtures.executeUpdate();

					insertFixtures.close();
				}
			}
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2,"Error Processing Fixtures while Rebuilding League:" + h2h.leagueID + "\n");
			Main.log.log(2,sql.toString());
			Main.log.log(9,sql);
		}

		//Process GW Scores and work out points
		for(int gw= h2h.startingGameweek; gw < h2h.gameweek; gw++) {
			//Create Required Gameweek Tables
			createGWTables(gw);
			createGWTables(gw+1);
			
			//Fetch GW Scores and add them to Live LP
			try {
				for(String manIDs: h2h.managerMap.keySet()) {
					PreparedStatement getTeam = conn.prepareStatement("SELECT managerID, gw FROM teamsGW? WHERE managerID = ?");
					getTeam.setInt(1, gw);
					getTeam.setInt(2, Integer.parseInt(manIDs));
					ResultSet teamData = getTeam.executeQuery();
					while(teamData.next()) {
						//Set This Week
						PreparedStatement ltgw = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, liveLP) VALUES (?, ?, lp + ?) ON DUPLICATE KEY UPDATE liveLP = lp + ?");
						ltgw.setInt(1, gw);
						ltgw.setInt(2, h2h.leagueID);
						ltgw.setInt(3, teamData.getInt("managerID"));
						ltgw.setInt(4, teamData.getInt("gw"));
						ltgw.setInt(5, teamData.getInt("gw"));
						ltgw.executeUpdate();

						ltgw.close();

					}

					teamData.close();
					getTeam.close();
				}

				Main.log.log(4,"Rebuilding League " + h2h.leagueID + " GW:" + gw + "/" + h2h.gameweek + " Updating Scores...                             \r");
				fixtureScores(gw, h2h.leagueID);
				Main.log.log(4,"Rebuilding League " + h2h.leagueID + " GW:" + gw + "/" + h2h.gameweek + " Updating Positions...                          \r");
				updatePositions(gw, h2h.leagueID);
				
				//Set Next Week Including wins, position and LP
				PreparedStatement ltgwNextWeek = conn.prepareStatement("INSERT INTO leagues_teamsGW? (leagueID, managerID, lp, position, wins, loss, draw, points) SELECT leagueID, managerID, liveLP, livePosition, liveWin, liveLoss, LiveDraw, livePoints FROM leagues_teamsGW? WHERE leagueID = ? ON DUPLICATE KEY UPDATE lp = leagues_teamsGW?.liveLP, position = leagues_teamsGW?.livePosition, wins = leagues_teamsGW?.liveWin, loss = leagues_teamsGW?.liveLoss, draw = leagues_teamsGW?.liveDraw, points =leagues_teamsGW?.livePoints");
				ltgwNextWeek.setInt(1, gw+1);
				ltgwNextWeek.setInt(2, gw);
				ltgwNextWeek.setInt(3, h2h.leagueID);
				ltgwNextWeek.setInt(4, gw);
				ltgwNextWeek.setInt(5, gw);
				ltgwNextWeek.setInt(6, gw);
				ltgwNextWeek.setInt(7, gw);
				ltgwNextWeek.setInt(8, gw);
				ltgwNextWeek.setInt(9, gw);
				ltgwNextWeek.executeUpdate();

				ltgwNextWeek.close();
			}
			catch(SQLException sql) {
				Main.log.ln(2);
				Main.log.log(2,"Error Passing Gameweek Scores... " + sql + "\n");
				Main.log.log(9,sql);
			}
		}
		Main.log.log(4,"Rebuilding League " + h2h.leagueID + " - Updated!                                                \n");


	}

	public void rebuildTransfers () {
		// Check what current gameweek is, and update
		Fixtures getGW = new Fixtures();
		getGW.loadFixtures();
		int currGW = Integer.parseInt(getGW.gameweek);
		if(!getGW.gameweekStarted()) {
			currGW--;
		}
		rebuildTransfers(currGW);
	}

	public void rebuildTransfers(int currGW) {	
		Main.log.log(6,"Rebuilding All Transfer Data...\n");
		for (int gw = 2; gw <= currGW; gw++) {
			Main.log.log(5,"Rebuilding Transfer Data for GW:" + gw + "/" + currGW + "...             \r");
			saveTransfers(gw);
		}
		Main.log.ln(5);
	}

	public void saveTransfers (int gw) {
		Main.log.log(6,"Saving GW Transfer Data...\n");
		//int gw = Integer.parseInt(gameweek);
		String[] positions = {"GkID","DefID1","DefID2","DefID3","DefID4","DefID5","MidID1","MidID2","MidID3","MidID4","MidID5","ForID1","ForID2","ForID3","BenchID1","BenchID2","BenchID3","BenchID4"};

		try {
			//Get All Teams and Convert to Map<Int, Collection>
			PreparedStatement getTeam = conn.prepareStatement("SELECT * FROM teamsGW?");
			getTeam.setInt(1, gw);
			ResultSet allTeams = getTeam.executeQuery();
			//Map<Integer,Collection<Integer>> allTeams = makeList(allTeamsResults);


			while (allTeams.next()) {
				if(allTeams.getInt("Transfers") != 0) {
					PreparedStatement getLastGWTeam = conn.prepareStatement("SELECT * FROM teamsGW? WHERE managerID = ?");
					getLastGWTeam.setInt(1, gw-1);
					getLastGWTeam.setInt(2, allTeams.getInt("managerID"));
					ResultSet lastGWTeam = getLastGWTeam.executeQuery();

					while (lastGWTeam.next()) {
						//Make Required Collections to get all overlap data
						Collection<Integer> thisWeek = new ArrayList<Integer>(); 						
						Collection<Integer> transfersOut = new ArrayList<Integer>();

						for(String temp :positions) {
							thisWeek.add(allTeams.getInt(temp));
						}
						for(String temp :positions) {
							transfersOut.add(lastGWTeam.getInt(temp));
						}
						Main.log.ln(8);
						Main.log.log(8,"Team List: " + thisWeek + "\n");
						Main.log.log(8,"Team List Last Week: " + transfersOut + "\n");

						//Copy One to preserve thisWeeks Data
						Collection<Integer> transfersIn = new ArrayList<Integer>();
						transfersIn.addAll(thisWeek);

						//RemoveAll to get overall
						transfersIn.removeAll(transfersOut);
						transfersOut.removeAll(thisWeek);

						Main.log.ln(7);
						Main.log.log(7,"Transfers In: " + transfersIn + " - Transfers Out: " + transfersOut + "\n");

						//Sanity Check, Can have more transfer points than real transfers but can have more real transfers that what the game says.
						if (transfersIn.size() > allTeams.getInt("Transfers")) {
							//TODO Can Transfer Issues Be Error Handled?
							Main.log.ln(3);
							Main.log.log(3,"Too Many Transfers for " + allTeams.getInt("managerID") + " in GW:" + gw + "\n");
							Main.log.log(6,"Measured transfers: " + transfersIn.size() + " Real Value:" + allTeams.getInt("Transfers") + "\n");
							Main.log.log(6,"Transfers In: " + transfersIn + " - Transfers Out: " + transfersOut + "\n");
						}
						else {

							PreparedStatement writeTransfers = conn.prepareStatement("UPDATE teamsGW? SET transferIN = ?, transferOut = ? WHERE managerID = ?");
							writeTransfers.setInt(1, gw);
							writeTransfers.setString(2, listCollectionToString(transfersIn));
							writeTransfers.setString(3, listCollectionToString(transfersOut));
							writeTransfers.setInt(4, allTeams.getInt("managerID"));
							writeTransfers.execute();
							//Clean Up
							writeTransfers.close();
						}

					}
					lastGWTeam.close();
				}
			}
			allTeams.close();
		}
		catch (SQLException sql) {
			Main.log.ln(2);
			Main.log.log(2, "Error Processing Transfers.. " + sql  + "\n");
			Main.log.log(9,sql);
		}
	}

	public String listCollectionToString (Collection<Integer> list) {
		String listString = new String();
		int n =0;
		for(Integer player: list) {
			if (n == 0) {
				listString += Integer.toString(player);
			}
			else {
				listString += "," + Integer.toString(player);
			}
			n++;
		}
		return listString;
	}


	public void posDifferential (int gameweek) {
		Main.log.log(5,"Updating Positions...      ");
		//int gameweek = Integer.parseInt(GameWeek);
		try {
			PreparedStatement SPositions = conn.prepareStatement("SELECT managerID, leagueID, position, livePosition, livePoints, liveLP FROM leagues_teamsGW? ORDER BY leagueID");
			SPositions.setInt(1, gameweek);
			ResultSet RPositions = SPositions.executeQuery();

			while(RPositions.next()) {

				String posDiff = "-";
				if (RPositions.getInt("position") == 0) {
					posDiff = "-";
				}
				else if (RPositions.getInt("livePosition") > RPositions.getInt("position")) {
					posDiff = "Down";
				}
				else if (RPositions.getInt("livePosition") < RPositions.getInt("position")) {
					posDiff = "Up";
				}
				PreparedStatement UTeam = conn.prepareStatement("UPDATE leagues_teamsGW? set posDifferential = ? WHERE managerID = ? AND leagueID = ?");
				UTeam.setInt(1, gameweek);
				UTeam.setString(2, posDiff);
				UTeam.setInt(3, RPositions.getInt("managerID"));
				UTeam.setInt(4, RPositions.getInt("leagueID"));
				UTeam.executeUpdate();
				UTeam.close();
			}
			SPositions.close();
		}
		catch (Exception e) {
			Main.log.ln(2);
			Main.log.log(2,"Error Updating Positions.. " + e + "\n");
			Main.log.log(9,e);
		}
		Main.log.log(5,"Done\n",0);
	}



	public void createGraphs (ClassicLeague league) {
		Map <String, Map<Integer, Integer>> teamsGwScores = new HashMap<String, Map<Integer,Integer>>();
		Map <String, Map<Integer, Integer>> teamsGwPosition = new HashMap<String, Map<Integer,Integer>>();
		int currGW = league.gameweek;

		//TODO Known Bug if two teams in a league have the same name
		for(int n = 1; n <= currGW; n++) {
			try {
				PreparedStatement SScores = conn.prepareStatement("SELECT t.teamName, t.managerID, m.leagueID, m.managerID, m.liveLP, m.livePosition FROM leagues_teamsGW? AS m, teamsGW? AS t WHERE m.managerID = t.managerID AND leagueID = ?");
				SScores.setInt(1, n);
				SScores.setInt(2, n);
				SScores.setInt(3, league.leagueID);
				ResultSet gwScores = SScores.executeQuery();

				while (gwScores.next()) {
					if(teamsGwScores.containsKey(gwScores.getString("teamName"))) {
						teamsGwScores.get(gwScores.getString("teamName")).put(n, gwScores.getInt("liveLP"));
					}
					else {
						Map <Integer, Integer> temp = new TreeMap <Integer, Integer>();
						temp.put(n, gwScores.getInt("liveLP"));
						teamsGwScores.put(gwScores.getString("teamName"), temp);
					}

					if(teamsGwPosition.containsKey(gwScores.getString("teamName"))) {
						teamsGwPosition.get(gwScores.getString("teamName")).put(n, gwScores.getInt("livePosition"));
					}
					else {
						Map <Integer, Integer> temp = new TreeMap <Integer, Integer>();
						temp.put(n, gwScores.getInt("livePosition"));
						teamsGwPosition.put(gwScores.getString("teamName"), temp);
					}
				}
				SScores.close();
			}
			catch(SQLException sql) {
				//Missing GameWeek Table, Skip Over Data
				Main.log.log(6,"Missing GW, Skipping to next GW..\n");
			}
			catch(Exception e) {
				Main.log.log(2,"Error in createGraphs (CL) - Skipping This GW..." + e + "\n");
				Main.log.log(9,e);
			}
		}
		writeJSON(teamsGwScores, Integer.toString(league.leagueID) + "scores");
		writeJSON(teamsGwPosition, Integer.toString(league.leagueID) + "position");
	}

	public void createGraphs (H2HLeague league) {
		Map <String, Map<Integer, Integer>> teamsGwScores = new HashMap<String, Map<Integer,Integer>>();
		Map <String, Map<Integer, Integer>> teamsGwPosition = new HashMap<String, Map<Integer,Integer>>();
		int currGW = league.gameweek;

		//TODO Known Bug if two teams in a league have the same name
		for(int n = 1; n <= currGW; n++) {
			try {
				PreparedStatement SScores = conn.prepareStatement("SELECT t.teamName, t.managerID, m.leagueID, m.managerID, m.livePoints, m.livePosition FROM leagues_teamsGW? AS m, teamsGW? AS t WHERE m.managerID = t.managerID AND leagueID = ?");
				SScores.setInt(1, n);
				SScores.setInt(2, n);
				SScores.setInt(3, league.leagueID);
				ResultSet gwScores = SScores.executeQuery();

				while (gwScores.next()) {
					if(teamsGwScores.containsKey(gwScores.getString("teamName"))) {
						teamsGwScores.get(gwScores.getString("teamName")).put(n, gwScores.getInt("livePoints"));
					}
					else {
						Map <Integer, Integer> temp = new TreeMap <Integer, Integer>();
						temp.put(n, gwScores.getInt("livePoints"));
						teamsGwScores.put(gwScores.getString("teamName"), temp);
					}

					if(teamsGwPosition.containsKey(gwScores.getString("teamName"))) {
						teamsGwPosition.get(gwScores.getString("teamName")).put(n, gwScores.getInt("livePosition"));
					}
					else {
						Map <Integer, Integer> temp = new TreeMap <Integer, Integer>();
						temp.put(n, gwScores.getInt("livePosition"));
						teamsGwPosition.put(gwScores.getString("teamName"), temp);
					}
				}
				SScores.close();
			}
			catch(SQLException sql) {
				//Missing GameWeek Table, Skip Over Data
				Main.log.log(6,"Missing GW, Skipping to next GW..\n");
			}
			catch(Exception e) {
				Main.log.log(2,"Error in createGraphs (H2H) - Skipping This GW..." + e + "\n");
				Main.log.log(9,e);
			}
		}
		writeJSON(teamsGwScores, Integer.toString(league.leagueID) + "scores");
		writeJSON(teamsGwPosition, Integer.toString(league.leagueID) + "position");
	}

	public void makePlayerGraph(int gw) {
		Map <String, Map<Integer, Integer>> playersGWPicks = new HashMap<String, Map<Integer,Integer>>();
		//Map <String, Map<Integer, Integer>> playersForm = new HashMap<String, Map<Integer,Integer>>();
		Main.log.log(5,"Making Player Picks Graph...                \r");
		for(int x = 1; x <= gw; x++){
			try {
				PreparedStatement selPlayers = conn.prepareStatement("SELECT playerID, webName, playerCount FROM playersGW? ORDER BY playerCount DESC");
				selPlayers.setInt(1, x);
				ResultSet players = selPlayers.executeQuery();
				int count = 0;
				while(players.next()) {
					count++;
					if(count > 10) {
						break;
					}

					if(playersGWPicks.containsKey(players.getString("webName"))) {
						playersGWPicks.get(players.getString("webName")).put(x, players.getInt("playerCount"));
					}
					else {
						Map <Integer, Integer> temp = new TreeMap <Integer, Integer>();
						temp.put(x, players.getInt("playerCount"));
						playersGWPicks.put(players.getString("webName"), temp);
					}
				}
				players.close();
			}

			catch(SQLException sql) {
				//Missing GameWeek Table, Skip Over Data
				Main.log.log(6,"Missing GW, Skipping to next GW..\n");
			}
			catch(Exception e) {
				Main.log.log(2,"Error in makePlayerGraph - Skipping This GW..." + e + "\n");
				Main.log.log(9,e);
			}
		}
		writeJSON(playersGWPicks, "playerPicks");
	}

	public void writeJSON(Map <String, Map<Integer, Integer>> map, String fileName) {
		try {
			Main.log.log(6,"Writing JSON..  ");
			File gameWeekFile = new File("stats/" + fileName + ".json");
			FileWriter jsonWrite = null;
			if(!gameWeekFile.exists()) {
				jsonWrite = new FileWriter(gameWeekFile);
			}
			else {
				jsonWrite = new FileWriter(gameWeekFile);
				//To Append
				//fantasyScores = new FileWriter(scores, true);
			}


			//For Every Row
			jsonWrite.write("[");
			jsonWrite.flush();
			String writeString = "";
			for (Entry <String,Map <Integer,Integer>> team :map.entrySet()) {
				//Write the json opening
				writeString += "{"
						+ "\"label\": \"" + team.getKey() + "\","
						+ "\"data\": [";

				//jsonWrite.write("[" + toPrint[0][2] + ", " + toPrint[x][2] + "]");
				for (Entry <Integer,Integer> scores: team.getValue().entrySet()) {
					writeString += "[" + scores.getKey() + ", " + scores.getValue() + "],";
				}
				writeString = writeString.substring(0, writeString.lastIndexOf(","));
				writeString += "]},";
			}
			writeString = writeString.substring(0, writeString.lastIndexOf(","));
			jsonWrite.write(writeString);
			jsonWrite.write("]");
			jsonWrite.flush();
			jsonWrite.close();

			Main.log.log(6,"Done!\n",0);
		}
		catch(IOException e) {
			Main.log.ln(2);
			Main.log.log(2,"Error Writing JSON to File.. " + e + "\n");
			Main.log.log(2,"Manual Creation of a 'stats' folder relative to the run path may be required");
			Main.log.log(9,e);
		}
		catch(Exception f) {
			Main.log.ln(2);
			Main.log.log(2,"Error Writing JSON to File.. " + f + "\n");
			Main.log.log(9,f);
		}
	}

	public void postUpdate (Leagues leagues) {
		Set<Integer> gameweeks = new HashSet<Integer>();
		int leagueCount = leagues.classicLeague.size() + leagues.h2hLeague.size();
		int n = 1;
		for (ClassicLeague CL: leagues.classicLeague) {
			Main.log.log(4,"Starting Post Updates... League: " + n++ + "/" + leagueCount + "\n");

			CL.loadLeague();
			CL.loadTeams();
			storeLeague(CL);
			preStore(CL);
			storeLeagueData(CL);
			createGraphs(CL);
			teamsUpdateStatus(CL.leagueID, CL.gameweek);
			postUpdateStatus(CL.leagueID, CL.gameweek);
			gameweeks.add(CL.gameweek);

		}
		for (H2HLeague H2H: leagues.h2hLeague) {
			Main.log.log(4,"Starting Post Updates... League: " + n++ + "/" + leagueCount + "\n");

			H2H.loadH2HLeague();
			H2H.loadTeams();
			storeLeague(H2H);
			preStore(H2H);
			storeLeagueData(H2H);
			createGraphs(H2H);
			teamsUpdateStatus(H2H.leagueID, H2H.gameweek);
			postUpdateStatus(H2H.leagueID, H2H.gameweek);
			gameweeks.add(H2H.gameweek);
		}
		for(int temp: gameweeks) {
			generatePlayerList(temp);
			updatePlayers(temp, false);
			makePlayerGraph(temp);
			posDifferential(temp);
			postBenchScores(temp);
		}
	}


	public void teamUpdate (int Gameweek, Leagues leagues) {

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
			Main.log.log(1, "Error Closing Connections");
			Main.log.log(1,e);
		}
	}
}

