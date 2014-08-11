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
import java.util.Map.Entry;

public class MySQLConnection {

	public Connection conn = null;

	public MySQLConnection(String ipAddress, String userName, String password) {
		try {
			// this will load the MySQL driver, each DB has its own driver
			Class.forName("com.mysql.jdbc.Driver");
			// setup the connection with the DB.
			System.out.print("Connecting to your MySQL Database...  ");
			conn = DriverManager.getConnection("jdbc:mysql://" + ipAddress + "/?user=" + userName + "&password=" + password + "&useUnicode=true&characterEncoding=utf8");
			Statement statement = conn.createStatement();
			statement.executeUpdate("CREATE DATABASE IF NOT EXISTS FFLive");
			statement.executeUpdate("USE FFLive");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS leagues (ID INT NOT NULL UNIQUE, name VARCHAR(30) NOT NULL, type VARCHAR(10) NOT NULL)");
			statement.executeUpdate("CREATE TABLE IF NOT EXISTS status (LeagueID INT NOT NULL, Gameweek INT NOT NULL, starts VARCHAR(20), kickOff VARCHAR(20), started VARCHAR(2) DEFAULT 'N', ends VARCHAR(20) , ended VARCHAR(2) DEFAULT 'N', teamsStored VARCHAR(2) DEFAULT 'N', postGwUpdate VARCHAR(2) DEFAULT 'N', UNIQUE (leagueID, Gameweek))");
			System.out.println("Database loaded!");
			statement.close();
		}
		catch (ClassNotFoundException c) {
			System.err.println("Critical Program Failure!");
			System.err.println(c);
			System.exit(1001);
		}
		catch (SQLException sql) {
			System.err.println("MySQL Error Encountered, Check that your details are correct and that your user has permissions to edit DB FFLive");
			System.err.println(sql);
			sql.printStackTrace();
			System.exit(1002);
		}
	}

	public void addStatus(ArrayList<String> leagueIDs) {

		System.out.print("Adding/Updating leagues...  ");
		try {

			Fixtures fixture = new Fixtures();
			fixture.loadFixtures();
			for (String leagueID : leagueIDs) {
				PreparedStatement preparedStmt = conn.prepareStatement("INSERT INTO status (LeagueID, Gameweek, starts, kickOff, ends) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE starts = ?, kickOff = ?, ends = ?");
				preparedStmt.setInt(1 , Integer.parseInt(leagueID));
				preparedStmt.setInt(2 , Integer.parseInt(fixture.gameweek));
				preparedStmt.setString(3 , fixture.startTime);
				preparedStmt.setString(4 , fixture.kickOff);
				preparedStmt.setString(5 , fixture.endTime);
				preparedStmt.setString(6 , fixture.startTime);
				preparedStmt.setString(7 , fixture.kickOff);
				preparedStmt.setString(8 , fixture.endTime);

				preparedStmt.executeUpdate();
				preparedStmt.close();
			}
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

	public HashMap<String, String> statusCheck() {
		System.out.print("Checking Status of Stored Leagues...  ");
		HashMap<String,String> incomplete = new HashMap<String,String>();

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
				//Add 2 Hours on to make sure match has finished
				endDate.add(Calendar.HOUR_OF_DAY, 2);

				if (status.getString("started").equals("Y")) {

					//Gameweek has started, check if it has ended
					if(status.getString("ended").equals("Y")) {

						//Gameweek marked as ended, check if post GW Scores as saved
						if(status.getString("postGwUpdate").equals("Y")) {
							//GW Complete, Move onto next line
						}
						else if(status.getString("postGwUpdate").equals("F")) {

							//System.out.println("Post Gameweek Update Missed");
						}
						else {
							//Gameweek complete but mark for post-update.
							incomplete.put(leagueID + "," + gw, "post");
						}
					}
					//Check Time to see if Gameweek has now ended.
					else if(now.after(endDate)) {
						//Gameweek has now ended, mark for post-update and update status table
						PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set ended='Y' where LeagueID = ? AND Gameweek = ?");
						preparedStmt.setInt(1 , leagueID);
						preparedStmt.setInt(2 , gw);

						preparedStmt.executeUpdate();

						incomplete.put(leagueID + "," + gw, "post");
						preparedStmt.close();
					}
					else {
						//Gameweek Still ongoing, mark for live update
						incomplete.put(leagueID + "," + gw, "live");
					}
				}

				//Check if teams are stored (Implies that teams are finalised)
				else if (status.getString("teamsStored").equals("Y")) {
					//Teams Stored, have games kicked off
					if (now.after(kickOff)) {

						//Gameweek Has now kicked off, check if it has ended
						if (now.after(endDate)) {
							//Gameweek has ended, Queue for post-update and add to table
							PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set started='Y', ended='Y' where LeagueID = ? AND Gameweek = ?");

							preparedStmt.setInt(1 , leagueID);
							preparedStmt.setInt(2 , gw);

							preparedStmt.executeUpdate();

							incomplete.put(leagueID + "," + gw, "post");
							preparedStmt.close();
						}
						else {
							//Gameweek in Progress, mark for live update
							PreparedStatement preparedStmt = conn.prepareStatement("UPDATE status set started='Y' where LeagueID = ? AND Gameweek = ?");
							preparedStmt.setInt(1 , leagueID);
							preparedStmt.setInt(2 , gw);

							preparedStmt.executeUpdate();

							incomplete.put(leagueID + "," + gw, "live");
							preparedStmt.close();
						}

					}
					else {
						//Gameweek will start soon so wait and check. Call for 2 min wait to check again
						incomplete.put("Gameweek to go Live", "wait");
						
					}
				}

				//Check If Teams have been finalised
				else if (now.after(startDate)) {
					//Teams Finalised, get teams
					incomplete.put(leagueID + "," + gw, "teams");
				}
				else {
					//Check If Gameweek starts today
					if(startDate.get(Calendar.DAY_OF_MONTH) == now.get(Calendar.DAY_OF_MONTH) && startDate.get(Calendar.MONTH) == now.get(Calendar.MONTH)){
						//Gameweek does start today
						incomplete.put("Teams to be finalised","wait");
					}
					//Else, still before the start of the next gameweek, nothing to do
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
			System.out.println("Data is Missing from the DB, run --fix to repair..");
			System.err.println(n);
			System.exit(1005);
		}
		System.out.println("Done!");
		return incomplete;
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
			int GW = Integer.parseInt(gw.trim());
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery("SELECT starts FROM status WHERE Gameweek=" + GW+1);
			Calendar yes = (new DateParser (rs.getString("starts"))).convertDate();
			Calendar now = Calendar.getInstance();
			while (rs.next()) {
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
			String leagues_teamsGW = "leagues_teamsGW" + league.gameWeek;
			String teamsGW = "teamsGW" + league.gameWeek;
			//String teams_playersGW = "teams_playersGW" + league.gameWeek;
			String playersGW = "playersGW" + league.gameWeek;
			//String allPlayersGW = "allPlayersGW" + league.gameWeek;

			//Statement statement = conn.createStatement();

			//statement.executeUpdate("INSERT INTO leagues values (" + league.leagueID + ", '" + league.leagueName + "', 'classic') ON DUPLICATE KEY UPDATE name = '" + league.leagueName + "', type = 'classic'");
			PreparedStatement leaguesInsert = conn.prepareStatement("INSERT INTO leagues values (?, ?, 'classic') ON DUPLICATE KEY UPDATE name = ?, type = 'classic'");
			leaguesInsert.setInt(1, league.leagueID);
			leaguesInsert.setString(2, league.leagueName);
			leaguesInsert.setString(3, league.leagueName);
			leaguesInsert.executeUpdate();

			//statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + leagues_teamsGW + " (leagueID INT NOT NULL, managerID INT NOT NULL, op INT, lp INT, position INT, wins INT, loss INT, draw INT, points FLOAT, UNIQUE (leagueID, managerID))");
			PreparedStatement CTleagueTeamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + leagues_teamsGW + " (leagueID INT NOT NULL, managerID INT NOT NULL, lp INT, position INT, wins INT, loss INT, draw INT, points INT, fixture VARCHAR(2), UNIQUE (leagueID, managerID))");
			//CTleagueTeamsGW.setsetString(1, leagues_teamsGW);
			CTleagueTeamsGW.executeUpdate();

			//statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + teamsGW + " (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), GkID INT, DefID1 INT, DefID2 INT, DefID3 INT, DefID4 INT, DefID5 INT, MidID1 INT, MidID2 INT, MidID3 INT, MidID4 INT, MidID5 INT, ForID1 INT, ForID2 INT, ForID3 INT, BenchID1 INT, BenchID2 INT, BenchID3 INT, BenchID4 INT, captainID INT, viceCaptainID INT)");
			PreparedStatement CTteamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + teamsGW + " (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), op INT, gw INT, GkID INT, DefID1 INT, DefID2 INT, DefID3 INT, DefID4 INT, DefID5 INT, MidID1 INT, MidID2 INT, MidID3 INT, MidID4 INT, MidID5 INT, ForID1 INT, ForID2 INT, ForID3 INT, BenchID1 INT, BenchID2 INT, BenchID3 INT, BenchID4 INT, captainID INT, viceCaptainID INT)");
			//CTteamsGW.setString(1, teamsGW);
			CTteamsGW.executeUpdate();

			//statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + playersGW + " (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), webName VARCHAR(40), score INT, breakdown VARCHAR(150), teamName VARCHAR(30), currentFixture VARCHAR(30), nextFixture VARCHAR(30), status VARCHAR(10), news VARCHAR(100), photo VARCHAR(30))");
			PreparedStatement CTPlayersGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + playersGW + " (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), webName VARCHAR(40), score INT, breakdown VARCHAR(150), teamName VARCHAR(30), currentFixture VARCHAR(30), nextFixture VARCHAR(30), status VARCHAR(10), news VARCHAR(100), photo VARCHAR(30))");
			//CTPlayersGW.setString(1, playersGW);
			CTPlayersGW.executeUpdate();

			for (Entry<String,Team> entry: league.managerMap.entrySet()) {
				//statement.executeUpdate("INSERT INTO " + leagues_teamsGW + "(leagueID, managerID, lp, position)  values (" + league.leagueID + ", " + entry.getKey() + ", " + entry.getValue().lpScore + ", " + entry.getValue().position + ") ON DUPLICATE KEY UPDATE lp = " + entry.getValue().lpScore + ", position = " + entry.getValue().position);
				PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO " + leagues_teamsGW + " (leagueID, managerID, lp, position)  values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE lp = ?, position = ?");
				//ILeaguesTeamsGW.setString(1, leagues_teamsGW);
				ILeaguesTeamsGW.setInt(1, league.leagueID);
				ILeaguesTeamsGW.setInt(2, Integer.parseInt(entry.getKey()));
				ILeaguesTeamsGW.setInt(3, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(4, entry.getValue().position);
				ILeaguesTeamsGW.setInt(5, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(6, entry.getValue().position);
				ILeaguesTeamsGW.executeUpdate();

				//statement.executeUpdate("INSERT INTO " + teamsGW + " (managerID, teamName, managerName) values (" + entry.getValue().managerID + ", '" + entry.getValue().teamName + "' ,'" + entry.getValue().managerName + "') ON DUPLICATE KEY UPDATE teamName = '" + entry.getValue().teamName + "', managerName = '" + entry.getValue().managerName + "'");
				PreparedStatement IteamsGW = conn.prepareStatement("INSERT INTO " + teamsGW + " (managerID, teamName, managerName, op, gw) values (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE teamName = ?, managerName = ?, op = ?, gw = ?");
				//IteamsGW.setString(1, teamsGW);
				IteamsGW.setString(1, entry.getValue().managerID);
				IteamsGW.setString(2, entry.getValue().teamName);
				IteamsGW.setString(3, entry.getValue().managerName);
				IteamsGW.setInt(4, entry.getValue().opScore);
				IteamsGW.setInt(5, entry.getValue().gameWeekScore);
				IteamsGW.setString(6, entry.getValue().teamName);
				IteamsGW.setString(7, entry.getValue().managerName);
				IteamsGW.setInt(8, entry.getValue().opScore);
				IteamsGW.setInt(9, entry.getValue().gameWeekScore);

				IteamsGW.executeUpdate();

				ILeaguesTeamsGW.close();
				IteamsGW.close();
			}
			leaguesInsert.close();
			CTleagueTeamsGW.close();
			CTteamsGW.close();
			CTPlayersGW.close();
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
			String leagues_teamsGW = "leagues_teamsGW" + league.gameweek;
			String teamsGW = "teamsGW" + league.gameweek;
			//String teams_playersGW = "teams_playersGW" + league.gameWeek;
			String playersGW = "playersGW" + league.gameweek;
			//String allPlayersGW = "allPlayersGW" + league.gameWeek;
			String H2HGW = "H2HFixturesGW" + league.gameweek;

			//Statement statement = conn.createStatement();

			//statement.executeUpdate("INSERT INTO leagues values (" + league.leagueID + ", '" + league.leagueName + "', 'classic') ON DUPLICATE KEY UPDATE name = '" + league.leagueName + "', type = 'classic'");
			PreparedStatement leaguesInsert = conn.prepareStatement("INSERT INTO leagues values (?, ?, 'H2H') ON DUPLICATE KEY UPDATE name = ?, type = 'H2H'");
			leaguesInsert.setInt(1, league.leagueID);
			leaguesInsert.setString(2, league.leagueName);
			leaguesInsert.setString(3, league.leagueName);
			leaguesInsert.executeUpdate();

			//statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + leagues_teamsGW + " (leagueID INT NOT NULL, managerID INT NOT NULL, op INT, lp INT, position INT, wins INT, loss INT, draw INT, points FLOAT, UNIQUE (leagueID, managerID))");
			PreparedStatement CTleagueTeamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + leagues_teamsGW + " (leagueID INT NOT NULL, managerID INT NOT NULL, lp INT, position INT, wins INT, loss INT, draw INT, points INT, fixture VARCHAR(2), UNIQUE (leagueID, managerID))");
			//CTleagueTeamsGW.setsetString(1, leagues_teamsGW);
			CTleagueTeamsGW.executeUpdate();

			//statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + teamsGW + " (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), GkID INT, DefID1 INT, DefID2 INT, DefID3 INT, DefID4 INT, DefID5 INT, MidID1 INT, MidID2 INT, MidID3 INT, MidID4 INT, MidID5 INT, ForID1 INT, ForID2 INT, ForID3 INT, BenchID1 INT, BenchID2 INT, BenchID3 INT, BenchID4 INT, captainID INT, viceCaptainID INT)");
			PreparedStatement CTteamsGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + teamsGW + " (managerID INT NOT NULL UNIQUE, teamName VARCHAR(25), managerName VARCHAR(120), op INT, gw INT, GkID INT, DefID1 INT, DefID2 INT, DefID3 INT, DefID4 INT, DefID5 INT, MidID1 INT, MidID2 INT, MidID3 INT, MidID4 INT, MidID5 INT, ForID1 INT, ForID2 INT, ForID3 INT, BenchID1 INT, BenchID2 INT, BenchID3 INT, BenchID4 INT, captainID INT, viceCaptainID INT)");
			//CTteamsGW.setString(1, teamsGW);
			CTteamsGW.executeUpdate();

			//statement.executeUpdate("CREATE TABLE IF NOT EXISTS " + playersGW + " (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), webName VARCHAR(40), score INT, breakdown VARCHAR(150), teamName VARCHAR(30), currentFixture VARCHAR(30), nextFixture VARCHAR(30), status VARCHAR(10), news VARCHAR(100), photo VARCHAR(30))");
			PreparedStatement CTPlayersGW = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + playersGW + " (playerID INT NOT NULL UNIQUE, playerCount INT DEFAULT 1 NOT NULL, firstName VARCHAR(30), lastName VARCHAR(30), webName VARCHAR(40), score INT, breakdown VARCHAR(150), teamName VARCHAR(30), currentFixture VARCHAR(30), nextFixture VARCHAR(30), status VARCHAR(10), news VARCHAR(100), photo VARCHAR(30))");
			//CTPlayersGW.setString(1, playersGW);
			CTPlayersGW.executeUpdate();

			PreparedStatement CTH2HFixture = conn.prepareStatement("CREATE TABLE IF NOT EXISTS " + H2HGW + " (leagueID INT NOT NULL, home VARCHAR(30), away VARCHAR(30), fixtureNo INT, UNIQUE(leagueID, home, away)");
			CTH2HFixture.executeUpdate();

			//Insert Fixtures
			int id = 1;
			for (Entry<String, String> entry : league.fixtureMap.entrySet()) {
				PreparedStatement ITH2HFixtures = conn.prepareStatement("INSERT INTO TABLE " + H2HGW +  " (leagueID, home, away, fixtureNo) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE fixtureNo = ?");
				ITH2HFixtures.setInt(1, league.leagueID);
				ITH2HFixtures.setInt(2, Integer.parseInt(entry.getKey()));
				ITH2HFixtures.setInt(3, Integer.parseInt(entry.getValue()));
				ITH2HFixtures.setInt(4, id);
				ITH2HFixtures.setInt(5, id);
				id++;
				ITH2HFixtures.close();

			}

			for (Entry<String,Team> entry: league.managerMap.entrySet()) {
				//statement.executeUpdate("INSERT INTO " + leagues_teamsGW + "(leagueID, managerID, lp, position)  values (" + league.leagueID + ", " + entry.getKey() + ", " + entry.getValue().lpScore + ", " + entry.getValue().position + ") ON DUPLICATE KEY UPDATE lp = " + entry.getValue().lpScore + ", position = " + entry.getValue().position);
				PreparedStatement ILeaguesTeamsGW = conn.prepareStatement("INSERT INTO " + leagues_teamsGW + " (leagueID, managerID, lp, position, wins, loss, draw, points)  values (?, ?, ?, ?, ? ,? ,? ,?) ON DUPLICATE KEY UPDATE lp = ?, position = ?, wins = ?, loss = ?, draw = ?, points = ?");
				//ILeaguesTeamsGW.setString(1, leagues_teamsGW);
				ILeaguesTeamsGW.setInt(1, league.leagueID);
				ILeaguesTeamsGW.setInt(2, Integer.parseInt(entry.getKey()));
				ILeaguesTeamsGW.setInt(3, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(4, entry.getValue().position);
				ILeaguesTeamsGW.setInt(5, entry.getValue().win);
				ILeaguesTeamsGW.setInt(6, entry.getValue().loss);
				ILeaguesTeamsGW.setInt(7, entry.getValue().draw);
				ILeaguesTeamsGW.setInt(8, entry.getValue().h2hScore);
				ILeaguesTeamsGW.setInt(9, entry.getValue().lpScore);
				ILeaguesTeamsGW.setInt(10, entry.getValue().position);
				ILeaguesTeamsGW.setInt(11, entry.getValue().win);
				ILeaguesTeamsGW.setInt(12, entry.getValue().loss);
				ILeaguesTeamsGW.setInt(13, entry.getValue().draw);
				ILeaguesTeamsGW.setInt(14, entry.getValue().h2hScore);
				ILeaguesTeamsGW.executeUpdate();

				//statement.executeUpdate("INSERT INTO " + teamsGW + " (managerID, teamName, managerName) values (" + entry.getValue().managerID + ", '" + entry.getValue().teamName + "' ,'" + entry.getValue().managerName + "') ON DUPLICATE KEY UPDATE teamName = '" + entry.getValue().teamName + "', managerName = '" + entry.getValue().managerName + "'");
				PreparedStatement IteamsGW = conn.prepareStatement("INSERT INTO " + teamsGW + " (managerID, teamName, managerName, op, gw) values (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE teamName = ?, managerName = ?, op = ?, gw = ?");
				//IteamsGW.setString(1, teamsGW);
				IteamsGW.setString(1, entry.getValue().managerID);
				IteamsGW.setString(2, entry.getValue().teamName);
				IteamsGW.setString(3, entry.getValue().managerName);
				IteamsGW.setInt(4, entry.getValue().opScore);
				IteamsGW.setInt(5, entry.getValue().gameWeekScore);
				IteamsGW.setString(6, entry.getValue().teamName);
				IteamsGW.setString(7, entry.getValue().managerName);
				IteamsGW.setInt(8, entry.getValue().opScore);
				IteamsGW.setInt(9, entry.getValue().gameWeekScore);

				IteamsGW.executeUpdate();

				ILeaguesTeamsGW.close();
				IteamsGW.close();
			}
			leaguesInsert.close();
			CTleagueTeamsGW.close();
			CTteamsGW.close();
			CTPlayersGW.close();
			CTH2HFixture.close();
			System.out.println("Done!");

		}
		catch (Exception e) {
			//TODO Error Handle
			e.printStackTrace();
			System.exit(1010);
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
			updateString += "GkID = " + team.goalkeeper[0];
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
			//Statement statement = conn.createStatement();
			//Statement updateSt = conn.createStatement();
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
						+ "breakdown = ?, "
						+ "teamName = ?, "
						+ "currentFixture = ?, "
						+ "nextFixture = ?, "
						+ "status = ?, "
						+ "news = ?, "
						+ "photo = ? "
						+ "where playerID = ?");
				UpPGw.setInt(1, Integer.parseInt(gw));
				UpPGw.setString(2, player.firstName);
				UpPGw.setString(3, player.lastName);
				UpPGw.setString(4, player.playerName);
				UpPGw.setInt(5, player.playerScore);
				UpPGw.setString(6, player.scoreBreakdown);
				UpPGw.setString(7, player.playerTeam);
				UpPGw.setString(8, player.currentFixture);
				UpPGw.setString(9, player.nextFixture);
				UpPGw.setString(10, player.status);
				UpPGw.setString(11, player.news);
				UpPGw.setString(12, player.photo);
				UpPGw.setInt(13, Integer.parseInt(player.playerID));

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
			//Statement stmt = conn.createStatement();
			ResultSet rs = selectPS.executeQuery();
			//ResultSet rs = stmt.executeQuery("SELECT * from teamsGW" + gameWeek);

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

			String[] positions = {"GkID","DefID1","DefID2","DefID3","DefID4","DefID5","MidID1","MidID2","MidID3","MidID4","MidID5","ForID1","ForID2","ForID3","BenchID1","BenchID2","BenchID3","BenchID4"};
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
				PreparedStatement UScore = conn.prepareStatement("UPDATE teamsGW? set gw = ? where managerID = ?");
				UScore.setInt(1, gameweek);
				UScore.setInt(2, gwScore);
				UScore.setInt(3, manId);
				UScore.executeUpdate();
				UScore.close();
			}
		}
		catch (SQLException sql) {
			sql.printStackTrace();
		}
		System.out.print("Updating Fixture Scores...  ");

		try {
			PreparedStatement SFix = conn.prepareStatement("SELECT * FROM H2HFixturesGW?");
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
				PreparedStatement home = conn.prepareStatement("UPDATE leagues_teamsGW? set fixture = ? WHERE managerID = ? AND leagueID = ?");
				PreparedStatement away = conn.prepareStatement("UPDATE leagues_teamsGW? set fixture = ? WHERE managerID = ? AND leagueID = ?");
				home.setInt(1, gameweek);
				away.setInt(1, gameweek);
				
				home.setInt(3, scores[0][0]);
				away.setInt(3, scores[1][0]);
				
				home.setInt(4, fixtures.getInt("leagueID"));
				away.setInt(4, fixtures.getInt("leagueID"));
				
				if (scores[0][1] == scores[1][1]) {
					//Draw
					home.setString(2, "D");
					away.setString(2, "D");
				}
				else if ((scores[0][1] > scores[1][1])) {
					//Team 0 Wins
					home.setString(2, "W");
					away.setString(2, "L");
				}
				else if ((scores[0][1] > scores[1][1])) {
					home.setString(2, "L");
					away.setString(2, "W");
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
		catch (SQLException sql) {
			sql.printStackTrace();
		}

		System.out.println("Done!");
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

