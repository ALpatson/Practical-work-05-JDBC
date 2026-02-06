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
 * Data Access Object for Genre entities.
 * Handles all database operations for genres.
 */
public class GenreDao {

	/**
	 * Retrieves all genres from the database.
	 * 
	 * @return a list of all genres
	 * @throws RuntimeException if a database error occurs
	 */
	public List<Genre> listGenres() {
		List<Genre> genres = new ArrayList<>();
		String sql = "SELECT * FROM genre";

		try (Connection connection = DataSourceFactory.getDataSource().getConnection();
				PreparedStatement statement = connection.prepareStatement(sql);
				ResultSet resultSet = statement.executeQuery()) {

			while (resultSet.next()) {
				Genre genre = new Genre(
						resultSet.getInt("idgenre"),
						resultSet.getString("name")
				);
				genres.add(genre);
			}
		} catch (SQLException e) {
			throw new RuntimeException("Error listing genres from database", e);
		}

		return genres;
	}

	/**
	 * Retrieves a genre by name from the database.
	 * 
	 * @param name the name of the genre to find
	 * @return an Optional containing the genre if found, or empty if not found
	 * @throws RuntimeException if a database error occurs
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
	 * 
	 * @param name the name of the genre to add
	 * @throws RuntimeException if a database error occurs
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