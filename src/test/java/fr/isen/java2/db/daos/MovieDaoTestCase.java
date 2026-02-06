package fr.isen.java2.db.daos;

import fr.isen.java2.db.entities.Genre;
import fr.isen.java2.db.entities.Movie;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for MovieDao
 */
public class MovieDaoTestCase {
	private final MovieDao movieDao = new MovieDao();

	@BeforeEach
	public void initDb() throws Exception {
		Connection connection = DataSourceFactory.getDataSource().getConnection();
		Statement stmt = connection.createStatement();
		stmt.executeUpdate(
				"CREATE TABLE IF NOT EXISTS genre (idgenre INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT , name VARCHAR(50) NOT NULL);");
		stmt.executeUpdate(
				"CREATE TABLE IF NOT EXISTS movie (\r\n"
				+ "  idmovie INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,\r\n" + "  title VARCHAR(100) NOT NULL,\r\n"
				+ "  release_date DATETIME NULL,\r\n" + "  genre_id INT NOT NULL,\r\n" + "  duration INT NULL,\r\n"
				+ "  director VARCHAR(100) NOT NULL,\r\n" + "  summary MEDIUMTEXT NULL,\r\n"
				+ "  CONSTRAINT genre_fk FOREIGN KEY (genre_id) REFERENCES genre (idgenre));");
		stmt.executeUpdate("DELETE FROM movie");
		stmt.executeUpdate("DELETE FROM genre");
		stmt.executeUpdate("DELETE FROM sqlite_sequence WHERE name='movie'");
		stmt.executeUpdate("DELETE FROM sqlite_sequence WHERE name='genre'");
		stmt.executeUpdate("INSERT INTO genre(idgenre,name) VALUES (1,'Drama')");
		stmt.executeUpdate("INSERT INTO genre(idgenre,name) VALUES (2,'Comedy')");
		// FIX: Insert dates with full timestamp format (YYYY-MM-DD HH:MM:SS.mmm)
		stmt.executeUpdate("INSERT INTO movie(idmovie,title, release_date, genre_id, duration, director, summary) "
				+ "VALUES (1, 'Title 1', '2015-11-26 12:00:00.000', 1, 120, 'director 1', 'summary of the first movie')");
		stmt.executeUpdate("INSERT INTO movie(idmovie,title, release_date, genre_id, duration, director, summary) "
				+ "VALUES (2, 'My Title 2', '2015-11-14 12:00:00.000', 2, 114, 'director 2', 'summary of the second movie')");
		stmt.executeUpdate("INSERT INTO movie(idmovie,title, release_date, genre_id, duration, director, summary) "
				+ "VALUES (3, 'Third title', '2015-12-12 12:00:00.000', 2, 176, 'director 3', 'summary of the third movie')");
		stmt.close();
		connection.close();
	}

	@Test
	public void shouldListMovies() {
		// WHEN
		List<Movie> movies = movieDao.listMovies();

		// THEN
		assertThat(movies).hasSize(3);
		assertThat(movies)
				.extracting("title", "genre.name")
				.containsExactlyInAnyOrder(
						tuple("Title 1", "Drama"),
						tuple("My Title 2", "Comedy"),
						tuple("Third title", "Comedy")
				);
	}

	@Test
	public void shouldListMoviesByGenre() {
		// WHEN
		List<Movie> movies = movieDao.listMoviesByGenre("Comedy");

		// THEN
		assertThat(movies).hasSize(2);
		assertThat(movies)
				.extracting("title")
				.containsExactlyInAnyOrder("My Title 2", "Third title");
	}

	@Test
	public void shouldAddMovie() throws Exception {
		// GIVEN
		// Constructor: Movie(Integer id, String title, LocalDate releaseDate, Genre genre, Integer duration, String director, String summary)
		Genre drama = new Genre(1, "Drama");
		Movie newMovie = new Movie(0, "New Movie", LocalDate.of(2020, 6, 15), drama, 100, "New Director", "A new movie summary");

		// WHEN
		Movie addedMovie = movieDao.addMovie(newMovie);

		// THEN
		assertThat(addedMovie.getId()).isNotZero();
		assertThat(addedMovie.getTitle()).isEqualTo("New Movie");
		assertThat(addedMovie.getDirector()).isEqualTo("New Director");
		assertThat(addedMovie.getReleaseDate()).isEqualTo(LocalDate.of(2020, 6, 15));

		// Verify in database
		Connection connection = DataSourceFactory.getDataSource().getConnection();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("SELECT * FROM movie WHERE idmovie = " + addedMovie.getId());
		assertThat(resultSet.next()).isTrue();
		assertThat(resultSet.getString("title")).isEqualTo("New Movie");
		assertThat(resultSet.getString("director")).isEqualTo("New Director");
		assertThat(resultSet.next()).isFalse();
		resultSet.close();
		statement.close();
		connection.close();
	}
}