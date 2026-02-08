package fr.isen.java2.db.daos;

import fr.isen.java2.db.entities.Genre;
import fr.isen.java2.db.entities.Movie;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * Data Access Object for Movie entity.
 * Provides database operations: retrieve all movies, filter by genre, and add new movies.
 * Returns movies with associated genre information via SQL JOIN.
 * Handles conversion between LocalDate and SQL Timestamp automatically.
 * 
 * @author ALpatson Cobbina SIAW
 * @version 1.10
 * @since 2026-02-08
 */
public class MovieDao {

	/**
	 * Retrieves all movies with their genre information.
	 * 
	 * @return List of all movies with genres. Returns empty list if none exist.
	 * @throws RuntimeException if database error occurs.
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
	 * Retrieves all movies of a specific genre.
	 * Search is case-sensitive.
	 * 
	 * @param genreName the genre name to filter by (must not be null)
	 * @return List of movies in the specified genre. Returns empty list if none match.
	 * @throws RuntimeException if database error occurs.
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
	 * Adds a new movie to the database.
	 * The database generates a unique ID and sets it on the returned movie object.
	 * LocalDate is automatically converted to SQL Date format.
	 * Null release dates are handled gracefully.
	 * 
	 * @param movie the movie to add (ID should be 0, will be overwritten with generated ID)
	 * @return the movie object with database-generated ID
	 * @throws RuntimeException if database error occurs (e.g., genre doesn't exist)
	 */
	public Movie addMovie(Movie movie) {
		String sql = "INSERT INTO movie(title, release_date, genre_id, duration, director, summary) VALUES(?, ?, ?, ?, ?, ?)";

		try (Connection connection = DataSourceFactory.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

			statement.setString(1, movie.getTitle());

			if (movie.getReleaseDate() != null) {
				statement.setDate(2, Date.valueOf(movie.getReleaseDate()));
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
	 * Converts a ResultSet row to a Movie object.
	 * Handles conversion of SQL Timestamp to LocalDate.
	 * Expects ResultSet to contain both movie and genre columns from a JOIN.
	 * 
	 * @param resultSet the current row (must be positioned after next())
	 * @return Movie object with all fields populated including genre information
	 * @throws SQLException if column not found or conversion fails
	 */
	private Movie mapMovie(ResultSet resultSet) throws SQLException {
		LocalDate releaseDate = null;
		Timestamp sqlTimestamp = resultSet.getTimestamp("release_date");
		if (sqlTimestamp != null) {
			releaseDate = sqlTimestamp.toLocalDateTime().toLocalDate();
		}

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