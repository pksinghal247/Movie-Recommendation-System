package pk.singhal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Scanner;

public class HiveJdbcMain 
{
	public static final String DB_DRIVER = "org.apache.hive.jdbc.HiveDriver";
	public static final String DB_URL = "jdbc:hive2://localhost:10000/MovieRecommendation";
	public static final String DB_USER = "sunbeam";
	public static final String DB_PASSWORD = "";

	static 
	{
		try 
		{
			// 1. load & register driver class
			Class.forName(DB_DRIVER);
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	public static void main(String[] args) 
	{
		Scanner sc = new Scanner(System.in);
		System.out.print("\n\n\n\n\n\t\t\tEnter Movie Id	:	");
		int movieId = sc.nextInt();
//		System.out.print("\n\n\n\n\n\t\t\tEnter Movie Title	:	");
//		String title = sc.nextLine();
		System.out.println("\n\n");
//		 2. create jdbc connection
		try(Connection con = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) 
		{
			// 3. create statement
		      String sql = "select * from movies limit 10";
			//String sql = "SELECT m.movieid, m.title, r.ratingcount, r.correlation FROM ratings r INNER JOIN movies m ON r.movieid2 = m.movieid WHERE r.movieid1 = ? AND r.correlation > 0.5 AND r.ratingcount > 30 LIMIT 7";
//			String sql = "SELECT m.movieid, m.title, r.ratingcount, r.correlation FROM ratings r INNER JOIN movies m ON r.movieid2 = m.movieid WHERE r.movieid1 = (SELECT movieid FROM movies WHERE title = "+"?"+") AND r.correlation > 0.5 AND r.ratingcount > 30 LIMIT 7";
			try(PreparedStatement stmt = con.prepareStatement(sql)) 
			{
				//stmt.setInt(1, movieId);
//				stmt.setString(1, title);
				// 4. execute query and process results
				try(ResultSet rs = stmt.executeQuery()) 
				{
					System.out.println("\n\n\n\n\n\n\t\t\tRecommended Movies are :	");
					while(rs.next()) 
					{
						int id = rs.getInt(1);
						String movie = rs.getString(2);
						System.out.println("\n\t\t\t"+id + ", " + movie);
					}
				} // 5. rs close
			} // 5. stmt close
		} // 5. con close
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
}





