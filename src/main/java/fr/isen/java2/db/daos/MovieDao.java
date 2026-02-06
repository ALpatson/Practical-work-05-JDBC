package fr.isen.java2.db.daos;

import fr.isen.java2.db.entities.Genre;
import fr.isen.java2.db.entities.Movie;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object for Movie entities.
 * Handles all database operations for movies.
 */
public class MovieDao {

	/**
	 * Retrieves all movies from the database with their associated genres.
	 * 
	 * @return a list of all movies
	 * @throws RuntimeException if a database error occurs
	 */
	public List<Movie> listMovies() {
		List<Movie> movies = new ArrayList<>();
		String sql = "SELECT * FROM movie JOIN genre ON movie.genre_id = genre.idgenre";
		try (Connection connection = DataSourceFactory.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql);
				ResultSet resultSet = statement.executeQuery()) {
			while (resultSet.next()) {
				movies.add(mapMovie(resultSet));
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error listing movies from database", e);
		}
		return movies;
	}

	/**
	 * Retrieves all movies from a specific genre.
	 * 
	 * @param genreName the name of the genre to filter by
	 * @return a list of movies in the specified genre
	 * @throws RuntimeException if a database error occurs
	 */
	public List<Movie> listMoviesByGenre(String genreName) {
		List<Movie> movies = new ArrayList<>();
		String sql = "SELECT * FROM movie JOIN genre ON movie.genre_id = genre.idgenre WHERE genre.name = ?";
		try (Connection connection = DataSourceFactory.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql)) {
			statement.setString(1, genreName);
			try (ResultSet resultSet = statement.executeQuery()) {
				while (resultSet.next()) {
					movies.add(mapMovie(resultSet));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error fetching movies by genre: " + genreName, e);
		}
		return movies;
	}

	/**
	 * Adds a new movie to the database and returns it with the generated ID.
	 * 
	 * @param movie the movie to add (id should be 0 or ignored)
	 * @return the same movie object with the database-generated ID set
	 * @throws RuntimeException if a database error occurs
	 */
	public Movie addMovie(Movie movie) {
		String sql = "INSERT INTO movie(title, release_date, genre_id, duration, director, summary) VALUES(?, ?, ?, ?, ?, ?)";
		try (Connection connection = DataSourceFactory.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
			statement.setString(1, movie.getTitle());
			// Convert LocalDate to java.sql.Date for storage
			if (movie.getReleaseDate() != null) {
				statement.setDate(2, java.sql.Date.valueOf(movie.getReleaseDate()));
			} else {
				statement.setNull(2, java.sql.Types.DATE);
			}
			statement.setInt(3, movie.getGenre().getId());
			statement.setInt(4, movie.getDuration());
			statement.setString(5, movie.getDirector());
			statement.setString(6, movie.getSummary());
			statement.executeUpdate();

			try (ResultSet generatedKeys = statement.getGeneratedKeys()) {
				if (generatedKeys.next()) {
					movie.setId(generatedKeys.getInt(1));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error adding movie to database", e);
		}
		return movie;
	}

	/**
	 * Maps a ResultSet row to a Movie object.
	 * The ResultSet must contain columns from both movie and genre tables.
	 * 
	 * @param resultSet the result set positioned at the current row
	 * @return a new Movie object with all fields populated
	 * @throws SQLException if a column is not found or value cannot be extracted
	 */
	private Movie mapMovie(ResultSet resultSet) throws SQLException {
		// FIX: Use getTimestamp() instead of getDate() because SQLite DATETIME
		// columns need timestamp parsing even if they only contain date values
		LocalDate releaseDate = null;
		Timestamp sqlTimestamp = resultSet.getTimestamp("release_date");
		if (sqlTimestamp != null) {
			releaseDate = sqlTimestamp.toLocalDateTime().toLocalDate();
		}

		// Constructor order: id, title, releaseDate, genre, duration, director, summary
		return new Movie(
				resultSet.getInt("idmovie"),
				resultSet.getString("title"),
				releaseDate,
				new Genre(
						resultSet.getInt("idgenre"),
						resultSet.getString("name")
				),
				resultSet.getInt("duration"),
				resultSet.getString("director"),
				resultSet.getString("summary")
		);
	}
}