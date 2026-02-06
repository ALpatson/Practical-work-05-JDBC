package fr.isen.java2.db.daos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import fr.isen.java2.db.entities.Genre;

/**
 * Test cases for GenreDao
 */
public class GenreDaoTestCase {
	private final GenreDao genreDao = new GenreDao();

	@BeforeEach
	public void initDatabase() throws Exception {
		Connection connection = DataSourceFactory.getDataSource().getConnection();
		Statement stmt = connection.createStatement();
		stmt.executeUpdate(
				"CREATE TABLE IF NOT EXISTS genre (idgenre INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT , name VARCHAR(50) NOT NULL);");
		stmt.executeUpdate("DELETE FROM genre");
		stmt.executeUpdate("DELETE FROM sqlite_sequence WHERE name='genre'");
		stmt.executeUpdate("INSERT INTO genre(idgenre,name) VALUES (1,'Drama')");
		stmt.executeUpdate("INSERT INTO genre(idgenre,name) VALUES (2,'Comedy')");
		stmt.executeUpdate("INSERT INTO genre(idgenre,name) VALUES (3,'Thriller')");
		stmt.close();
		connection.close();
	}

	@Test
	public void shouldListGenres() {
		// WHEN
		List<Genre> genres = genreDao.listGenres();
		// THEN
		assertThat(genres).hasSize(3);
		assertThat(genres).extracting("id", "name").containsOnly(tuple(1, "Drama"), tuple(2, "Comedy"),
				tuple(3, "Thriller"));
	}

	@Test
	public void shouldGetGenreByName() {
		// WHEN
		Optional<Genre> optionalGenre = genreDao.getGenre("Comedy");
		// THEN
		assertThat(optionalGenre).isPresent();
		Genre genre = optionalGenre.get();
		assertThat(genre.getId()).isEqualTo(2);
		assertThat(genre.getName()).isEqualTo("Comedy");
	}
        
        // We agreed that we can change this test case not to take NULL
        //The Lady and I
	@Test
	public void shouldNotGetUnknownGenre() {
		// WHEN
		Optional<Genre> genre = genreDao.getGenre("Unknown");
		// THEN
		assertThat(genre).isEmpty();
	}

	@Test
	public void shouldAddGenre() throws Exception {
		// WHEN 
		genreDao.addGenre("Western");
		// THEN
		Connection connection = DataSourceFactory.getDataSource().getConnection();
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery("SELECT * FROM genre WHERE name='Western'");
		assertThat(resultSet.next()).isTrue();
		assertThat(resultSet.getInt("idgenre")).isNotNull();
		assertThat(resultSet.getString("name")).isEqualTo("Western");
		assertThat(resultSet.next()).isFalse();
		resultSet.close();
		statement.close();
		connection.close();
	}
}