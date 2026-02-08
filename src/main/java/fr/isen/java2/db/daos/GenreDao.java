package fr.isen.java2.db.daos;

import fr.isen.java2.db.entities.Genre;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Data Access Object for Genre entity.
 * Provides database operations: retrieve all genres, find by name, and add new genres.
 * Uses prepared statements to prevent SQL injection.
 * 
 * @author ALpatson Cobbina SIAW
 * @version 1.10
 * @since 2026-02-08
 */
public class GenreDao {

	/**
	 * Retrieves all genres from the database.
	 * 
	 * @return List of all genres. Returns empty list if none exist.
	 * @throws RuntimeException if database error occurs.
	 */
	public List<Genre> listGenres() {
		List<Genre> genres = new ArrayList<>();
		String sql = "SELECT * FROM genre";

		try (Connection connection = DataSourceFactory.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql);
				ResultSet resultSet = statement.executeQuery()) {

			while (resultSet.next()) {
				genres.add(new Genre(
						resultSet.getInt("idgenre"),
						resultSet.getString("name")
				));
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error listing genres from database", e);
		}

		return genres;
	}

	/**
	 * Retrieves a genre by name.
	 * Search is case-sensitive.
	 * 
	 * @param name the genre name to search for (must not be null)
	 * @return Optional containing the genre if found, empty otherwise
	 * @throws RuntimeException if database error occurs
	 */
	public Optional<Genre> getGenre(String name) {
		String sql = "SELECT * FROM genre WHERE name = ?";

		try (Connection connection = DataSourceFactory.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql)) {

			statement.setString(1, name);

			try (ResultSet resultSet = statement.executeQuery()) {
				if (resultSet.next()) {
					return Optional.of(new Genre(
							resultSet.getInt("idgenre"),
							resultSet.getString("name")
					));
				}
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error fetching genre: " + name, e);
		}

		return Optional.empty();
	}

	/**
	 * Adds a new genre to the database.
	 * The database generates a unique ID automatically.
	 * 
	 * @param name the genre name (must not be null or empty)
	 * @throws RuntimeException if database error occurs
	 */
	public void addGenre(String name) {
		String sql = "INSERT INTO genre(name) VALUES(?)";

		try (Connection connection = DataSourceFactory.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql)) {

			statement.setString(1, name);
			statement.executeUpdate();

		} catch (SQLException e) {
			throw new RuntimeException("Error adding genre: " + name, e);
		}
	}
}