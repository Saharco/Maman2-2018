package techflix;

import techflix.business.Movie;
import techflix.business.MovieRating;
import techflix.business.ReturnValue;
import techflix.business.Viewer;
import techflix.data.DBConnector;
import techflix.data.PostgresSQLErrorCodes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


/**
 * The API provides the following functions:
 *
 *  ************************************************************
 *  CRUD API:
 *
 *  createViewer: {@link #createViewer(Viewer)} ()}
 *  getViewer: {@link #getViewer(Integer)}
 *  deleteViewer: {@link #deleteViewer(Viewer)}
 *  updateViewer: {@link #updateViewer(Viewer)}
 *  createMovie: {@link #createMovie(Movie)}
 *  getMovie: {@link #getMovie(Integer)}
 *  deleteMovie: {@link #deleteMovie(Movie)}}
 *  updateMovie: {@link #updateMovie(Movie)}
 *
 *  ************************************************************
 *  Basic API:
 *
 *  addView: {@link #addView(Integer, Integer)}
 *  removeView: {@link #removeView(Integer, Integer)}
 *  getMovieViewCount: {@link #getMovieViewCount(Integer)}
 *  addMovieRating: {@link #addMovieRating(Integer, Integer, MovieRating)}
 *  removeMovieRating: {@link #removeMovieRating(Integer, Integer)}
 *  getMovieLikesCount: {@link #getMovieLikesCount(int)}
 *  getMovieDislikesCount: {@link #getMovieDislikesCount(int)}
 *
 *  ************************************************************
 *  Advanced API:
 *
 *  getSimilarViewers: {@link #getSimilarViewers(Integer)}
 *  mostInfluencingViewers: {@link #mostInfluencingViewers()}
 *  getMoviesRecommendations: {@link #getMoviesRecommendations(Integer)}
 *  getConditionalRecommendations: {@link #getConditionalRecommendations(Integer, int)}
 *
 *  ************************************************************
 *
 */

public class Solution {

    /**
     * Helper function that processes a query error, and returns the appropriate return value
     * @param e: the exception thrown as a result from executing a query
     * @return:
     *  ALREADY_EXISTS: in case of a key constraint violation
     *  BAD_PARAMS: in case of either NOT NULL or CHECK constraints violations
     *  ERROR: otherwise (unexpected error)
     */
    private static ReturnValue processError(SQLException e) {
        if(Integer.valueOf(e.getSQLState()) ==
                PostgresSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
            //Primary key(s) constraint violation
            return ReturnValue.ALREADY_EXISTS;
        }
        else if(Integer.valueOf(e.getSQLState()) ==
                PostgresSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
            //Foreign key(s) constraint violation
            return ReturnValue.NOT_EXISTS;
        }
        else if(Integer.valueOf(e.getSQLState()) ==
                PostgresSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
            //Not NULL constraint violation
            return ReturnValue.BAD_PARAMS;
        }
        else if(Integer.valueOf(e.getSQLState()) ==
                PostgresSQLErrorCodes.CHECK_VIOLATION.getValue()) {
            //Check constraint violation
            return ReturnValue.BAD_PARAMS;
        }
        else {
            //Default case: unexpected error occurred
            return ReturnValue.ERROR;
        }
    }


    /**
     * Creates the tables in the database, as well as the datatypes (enum).
     * If a table already exists - it won't be affected
     */
    public static void createTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            //Creates a new enum type with two instances
            statement = connection.prepareStatement
                    ("CREATE TYPE movie_rating AS ENUM('LIKE', 'DISLIKE');");
            try {
                statement.execute();
            } catch (SQLException e) {
                //Type already declared!
            }
            statement = connection.prepareStatement("CREATE TABLE Viewers\n" +
                    "(\n" +
                    "    viewer_id integer NOT NULL,\n" +
                    "    viewer_name text NOT NULL,\n" +
                    "    CONSTRAINT \"Viewers_pkey\" PRIMARY KEY (viewer_id),\n" +
                    "    CONSTRAINT check_viewer_id CHECK (viewer_id > 0)\n" +
                    ")");
            try {
                statement.execute();
            } catch (SQLException e) {
                //Table already created!
            }

            statement = connection.prepareStatement("CREATE TABLE Movies\n" +
                    "(\n" +
                    "    movie_id INTEGER NOT NULL,\n" +
                    "    movie_name TEXT NOT NULL,\n" +
                    "    movie_description TEXT NOT NULL,\n" +
                    "    CONSTRAINT \"Movies_pkey\" PRIMARY KEY (movie_id), \n" +
                    "    CONSTRAINT check_movie_id CHECK (movie_id > 0)\n" +
                    ")");
            try {
                statement.execute();
            } catch (SQLException e) {
                //Table already created!
            }

            statement = connection.prepareStatement("CREATE TABLE Ratings\n" +
                    "(\n" +
                    "    viewer_id integer NOT NULL,\n" +
                    "    movie_id integer NOT NULL,\n" +
                    "    rating movie_rating,\n" +    //Can be NULL !
                    "    CONSTRAINT \"Ratings_pkey\" PRIMARY KEY (viewer_id, movie_id),\n" +
                    "    CONSTRAINT movie_id FOREIGN KEY (movie_id)\n" +
                    "        REFERENCES Movies (movie_id) MATCH SIMPLE\n" +
                    "        ON UPDATE NO ACTION\n" +
                    "        ON DELETE CASCADE,\n" +
                    "    CONSTRAINT viewer_id FOREIGN KEY (viewer_id)\n" +
                    "        REFERENCES Viewers (viewer_id) MATCH SIMPLE\n" +
                    "        ON UPDATE NO ACTION\n" +
                    "        ON DELETE CASCADE\n" +
                    ")");
            try {
                statement.execute();
            } catch (SQLException e) {
                //Table already created!
            }

        } catch (SQLException e) {
            //
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    /**
     * Clears all of the data from the database's tables, but keeps the schemas intact
     */
    public static void clearTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            //clear the Ratings table
            statement = connection.prepareStatement
                    ("DELETE FROM Ratings");
            statement.execute();

            //clear the Viewers table
            statement = connection.prepareStatement
                    ("DELETE FROM Viewers");
            statement.execute();

            //clear the Movies table
            statement = connection.prepareStatement
                    ("DELETE FROM Movies");
            statement.execute();

        } catch (SQLException e) {
            //We shouldn't get here
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    /**
     * Removes all of the database's information from the server
     */
    public static void dropTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;
        try {

            //Delete the Ratings table
            statement = connection.prepareStatement
                    ("DROP TABLE Ratings");
            try {
                statement.executeUpdate();
            } catch (SQLException e) {
                //Table doesn't exist
            }

            //Delete the Viewers table
            statement = connection.prepareStatement
                    ("DROP TABLE Viewers");
            try {
                statement.executeUpdate();
            } catch (SQLException e) {
                //Table doesn't exist
            }

            //Delete the Movies table
            statement = connection.prepareStatement
                    ("DROP TABLE Movies");
            try {
                statement.executeUpdate();
            } catch (SQLException e) {
                //Table doesn't exist
            }

        } catch (SQLException e) {
            //
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    /**
     * Adds a viewer to the database.
     * @param viewer: the viewer to be added to the database.
     * @return:
     *  ERROR: if there was a database error
     *  BAD_PARAMS: if the viewer's ID or name are illegal
     *  ALREADY_EXISTS: if the student's ID already exists in the database
     *  OK: otherwise.
     */
    public static ReturnValue createViewer(Viewer viewer) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Attempts to insert to given viewer into the Viewers table
             */
            statement = connection.prepareStatement("INSERT INTO Viewers(\n" +
                    "\tviewer_id, viewer_name)\n" +
                    "\tVALUES (?, ?);");
            statement.setInt(1, viewer.getId());
            statement.setString(2, viewer.getName());
            try {
                statement.execute();
            } catch (SQLException e) {
                //Failed to insert the viewer, return the appropriate error
                return processError(e);
            }

        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Gets a viewer from the database.
     * @param viewerId: the desired viewer's id.
     * @return: the desired viewer, if it exists (and no errors occured), and badViewer otherwise
     */
    public static Viewer getViewer(Integer viewerId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;
        Viewer viewer = null;

        try {
            /*
             * Returns the names of all the viewers in the Viewers table with the given ID.
             * This query cannot return more than one result, because the ID is the primary key
             */
            statement = connection.prepareStatement("SELECT viewer_name FROM Viewers" +
                    " WHERE viewer_id = ?");
            statement.setInt(1, viewerId);
            ResultSet result = statement.executeQuery();
            if (!result.next()) {
                //Didn't find a single viewer: no such viewer exists
                return Viewer.badViewer();
            }
            viewer = new Viewer();
            viewer.setId(viewerId);
            viewer.setName(result.getString(1));

        } catch (SQLException e) {
            return Viewer.badViewer();
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return viewer;
    }

    /**
     * Removes a viewer from the database.
     * @param viewer: the viewer to be removed from the database.
     * @return:
     *  ERROR: if there was a database error
     *  NOT_EXISTS: if the given viewer's ID does not exist in the database
     *  OK: otherwise.
     */
    public static ReturnValue deleteViewer(Viewer viewer) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Attempts to delete the viewer with the given ID from the Viewers table
             */
            statement = connection.prepareStatement("DELETE FROM Viewers WHERE viewer_id = ?");
            statement.setInt(1, viewer.getId());
            int affectedRows = statement.executeUpdate();
            if (affectedRows == 0) {
                //No rows were affected: the viewer does not exist in the Viewers table
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Updates a viewer's name in the database.
     * @param viewer: the viewer to be updated.
     * @return:
     *  ERROR: if there was a database error
     *  NOT_EXISTS: if the viewer does not exist
     *  BAD_PARAMS: if the viewer's name is NULL
     *  OK: otherwise.
     */
    public static ReturnValue updateViewer(Viewer viewer) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * return the names of all the viewers in the Viewers table with the given ID
             */
            statement = connection.prepareStatement("SELECT viewer_name FROM Viewers" +
                    " WHERE viewer_id = ?");
            statement.setInt(1, viewer.getId());
            ResultSet result = statement.executeQuery();
            if (!result.next()) {
                //no viewers were returned: the desired viewer does not exist
                return ReturnValue.NOT_EXISTS;
            }

            if (viewer.getName() == null) {
                return ReturnValue.BAD_PARAMS;
            }

            /*
             * update the viewer in the database with the given id to have the new given name
             */
            statement = connection.prepareStatement("UPDATE Viewers " +
                    "SET viewer_name = ? " + "WHERE viewer_id = ?");
            statement.setString(1, viewer.getName());
            statement.setInt(2, viewer.getId());
            statement.executeUpdate();

        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Adds a movie to the database.
     * @param movie: the movie to be added to the database.
     * @return:
     *  ERROR: if there was a database error
     *  BAD_PARAMS: if either the movie's ID, name or description are illegal
     *  ALREADY_EXISTS: if the movie's ID already exists in the database
     *  OK: otherwise.
     */
    public static ReturnValue createMovie(Movie movie) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Attempts to insert to given movie into the Movies table
             */
            statement = connection.prepareStatement("INSERT INTO Movies" +
                    " VALUES (?, ?, ?)");
            statement.setInt(1, movie.getId());
            statement.setString(2, movie.getName());
            statement.setString(3, movie.getDescription());
            try {
                statement.execute();
            } catch (SQLException e) {
                //Failed to insert the movie: return the appropriate error
                return processError(e);
            }

        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Gets a movie from the database.
     * @param movieId: the desired movie's ID.
     * @return: the desired movie if it exists (and no errors occurred), and badMovie otherwise.
     */
    public static Movie getMovie(Integer movieId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;
        Movie movie = null;

        try {
            /*
             * Returns the movie with the given ID from the Movies table
             */
            statement = connection.prepareStatement("SELECT * FROM Movies" +
                    " WHERE movie_id = ?");
            statement.setInt(1, movieId);
            ResultSet result = statement.executeQuery();
            if (!result.next()) {
                //No movies were returned, so the desired movie does not exist in the table
                return Movie.badMovie();
            }
            movie = new Movie();
            movie.setId(movieId);
            movie.setName(result.getString(2));
            movie.setDescription(result.getString(3));

        } catch (SQLException e) {
            return Movie.badMovie();
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return movie;
    }

    /**
     * Removes a movie from the database.
     * @param movie: the movie to be removed from the database.
     * @return:
     *  ERROR: if there was a database error
     *  NOT_EXISTS: if the given movie's ID does not exist in the database
     *  OK: otherwise.
     */
    public static ReturnValue deleteMovie(Movie movie) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Attempts to delete a movie with a given movie ID from the Movies table
             */
            statement = connection.prepareStatement
                    ("DELETE FROM Movies WHERE movie_id = ?");
            statement.setInt(1, movie.getId());
            int affectedRows = statement.executeUpdate();
            if (affectedRows == 0) {
                //The desired movie was not found
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Updates a movie's description in the database.
     * @param movie: the movie to be updated.
     * @return:
     *  ERROR: if there was a database error
     *  NOT_EXISTS: if the movie does not exist
     *  BAD_PARAMS: if the movie's description is NULL
     *  OK: otherwise.
     */
    public static ReturnValue updateMovie(Movie movie) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Returns the movie from the Movies table with the desired movie_id
             */
            statement = connection.prepareStatement("SELECT movie_description FROM Movies" +
                    " WHERE movie_id = ?");
            statement.setInt(1, movie.getId());
            ResultSet result = statement.executeQuery();
            if (!result.next()) {
                //No movie was found: the movie does not exist in the Movies table
                return ReturnValue.NOT_EXISTS;
            }

            if (movie.getDescription() == null) {
                //Movie description cannot be null
                return ReturnValue.BAD_PARAMS;
            }

            /*
             * Updates the desired movie's description
             */
            statement = connection.prepareStatement("UPDATE Movies " +
                    "SET movie_description = ? " + "WHERE movie_id = ?");
            statement.setString(1, movie.getDescription());
            statement.setInt(2, movie.getId());
            statement.executeUpdate();

        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Marks a movie as watched by a viewer
     * @param viewerId: the desired viewer's ID
     * @param movieId: the desired movie's ID
     * @return:
     *  ERROR: if there was a database error
     *  NOT_EXISTS: if either the viewer or the movie do not exist in the database
     *  ALREADY_EXISTS: if the given viewer already watched the movie
     *  OK: otherwise.
     */
    public static ReturnValue addView(Integer viewerId, Integer movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Attempt to insert the viewer and movie into the Ratings table, with a NULL value
             * for the rating (viewer rating does not exist upon insertion)
             */
            statement = connection.prepareStatement
                    ("INSERT INTO Ratings VALUES (?, ?, NULL)");
            statement.setInt(1, viewerId);
            statement.setInt(2, movieId);
            try {
                statement.execute();
            } catch (SQLException e) {
                return processError(e);
            }

        } catch (SQLException e) {
            //Failed to insert into the Ratings table: return the appropriate error
            return ReturnValue.ERROR;
        }
        finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Marks a movie as unwatched by the viewer
     * @param viewerId: the desired viewer's ID
     * @param movieId: the desired movie's ID
     * @return:
     *  ERROR: if there was a database error
     *  NOT_EXISTS: if either the desired viewer or movie do not exist,
     *      or the viewer hasn't watched the movie yet
     *  OK: otherwise
     */
    public static ReturnValue removeView(Integer viewerId, Integer movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Delete the appropriate viewer rating to a given movie in the Ratings table
             */
            statement = connection.prepareStatement
                    ("DELETE FROM Ratings WHERE viewer_id = ? AND movie_id = ?");
            statement.setInt(1, viewerId);
            statement.setInt(2, movieId);
            int affectedRows = statement.executeUpdate();
            if (affectedRows == 0) {
                //Viewer rating for this movie does not exist in the Ratings table
                return ReturnValue.NOT_EXISTS;
            }
        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Returns the number of viewers who watched a given movie
     * @param movieId: the desired movie's ID
     * @return: the amount of viewers who watched the movie, and 0 in case there's an error
     */
    public static Integer getMovieViewCount(Integer movieId)
    {
        Integer count = 0;
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Return the number of tuples in the table with the given movie's ID:
             * this is the amount of unique viewers' views for this movie
             */
            statement = connection.prepareStatement
                    ("SELECT COUNT(viewer_id) FROM Ratings WHERE movie_id = ?");
            statement.setInt(1, movieId);
            ResultSet res = statement.executeQuery();
            while(res.next()) {
                count = res.getInt(1);
            }

        } catch (SQLException e) {
            return 0;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return count;
    }

    /**
     * Gives a certain movie a rating (either LIKED or DISLIKED) by a viewer
     * @param viewerId: the desired viewer's ID
     * @param movieId: the desired movie's ID
     * @param rating: the desired rating for the movie
     * @return:
     *  ERROR: if there was a database error
     *  NOT_EXISTS: if either the desired viewer or the movie do not exist,
     *      or the viewer hasn't watched the movie
     *  OK: Otherwise
     */
    public static ReturnValue addMovieRating(Integer viewerId, Integer movieId, MovieRating rating)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Add the given viewer's rating to the given movie in the Ratings table.
             * The corresponding tuple should already exist in the table, so we change the
             * movie_rating value to the given rating value
             */
            statement = connection.prepareStatement
                    ("UPDATE Ratings SET rating = CAST(? AS movie_rating) \n"
                            + "WHERE viewer_id = ? AND movie_id = ?");
            statement.setString(1, rating.name());
            statement.setInt(2, viewerId);
            statement.setInt(3, movieId);
            int res = statement.executeUpdate();
            if(res == 0) {
                //Viewer rating for this movie does not exist in the Ratings table
                return ReturnValue.NOT_EXISTS;
            }

        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Removes the rating of a certain movie, given by a certain viewer
     * @param viewerId: the desired viewer in the database
     * @param movieId: the desired movie in the database
     * @return:
     *  ERROR: if there was a database error
     *  NOT_EXISTS: if either the viewer or the movie do not exist in the database,
     *      or the viewer hasn't rated the movie yet
     *  OK: otherwise
     */
    public static ReturnValue removeMovieRating(Integer viewerId, Integer movieId)
    {
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Update the viewer rating for the given movie to NULL (not rated) in the Ratings table
             */
            statement = connection.prepareStatement
                    ("UPDATE Ratings SET rating = NULL \n"
                            + "WHERE viewer_id = ? AND movie_id = ? AND rating IS NOT NULL");
            statement.setInt(1, viewerId);
            statement.setInt(2, movieId);
            int res = statement.executeUpdate();
            if(res == 0) {
                //Viewer rating for this movie does not exist in the Ratings table
                return ReturnValue.NOT_EXISTS;
            }

        } catch (SQLException e) {
            return ReturnValue.ERROR;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ReturnValue.OK;
    }

    /**
     * Returns the number of viewers who liked a given movie
     * @param movieId: the desired movie's ID
     * @return: the amount of viewers who liked the desired movie, or 0 in case of an error
     */
    public static int getMovieLikesCount(int movieId)
    {
        Integer count = 0;
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Return the number of tuples in the Ratings table where the desired movie is rated,
             * and the rating is 'LIKE'; that is: the amount of 'LIKE' ratings for this movie
             */
            statement = connection.prepareStatement
                    ("SELECT COUNT(viewer_id) FROM Ratings WHERE movie_id = ? \n" +
                            "AND rating = 'LIKE'");
            statement.setInt(1, movieId);
            ResultSet res = statement.executeQuery();
            while(res.next()) {
                count = res.getInt(1);
            }

        } catch (SQLException e) {
            return 0;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return count;
    }

    /**
     * Returns the number of viewers who disliked a given movie
     * @param movieId: the desired movie's ID
     * @return: the amount of viewers who disliked the desired movie, or 0 in case of an error
     */
    public static int getMovieDislikesCount(int movieId)
    {
        Integer count = 0;
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Return the number of tuples in the Ratings table where the desired movie is rated,
             * and the rating is 'DISLIKE'; that is: the amount of 'DISLIKE' ratings for this movie
             */
            statement = connection.prepareStatement
                    ("SELECT COUNT(viewer_id) FROM Ratings WHERE movie_id = ? \n" +
                            "AND rating = 'DISLIKE'");
            statement.setInt(1, movieId);
            ResultSet res = statement.executeQuery();
            while(res.next()) {
                count = res.getInt(1);
            }

        } catch (SQLException e) {
            return 0;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return count;
    }

    /**
     * returns the viewers' IDs of all the viewers who watched similar movies to the given viewer
     * @param viewerId: the desired viewer
     * @return: a list of viewers' IDs who watched at least 75% of the movies that the given viewer
     *  watched. returned list is ordered by ID in ascending order
     */
    public static ArrayList<Integer> getSimilarViewers(Integer viewerId)
    {
        ArrayList<Integer> list = new ArrayList<>();
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * This query utilizes several nested sub-queries:
             *  "ViewerMovies" sub-query: returns two-tuples of the given viewer and all the
             *      movies that this viewer watched.
             *  "SimilarViewers" sub-query: returns two-tuples of all the viewers (except the
             *      given viewer), and the number of movies, out of the selected movies that the
             *      given viewer has watched (ViewerMovies), that they have also watched.
             *
             *  The query returns all of the viewers' IDs in the database, where there is at least
             *  75% overlap between the movies that the given viewer watched, and the movies that
             *  these viewers watched.
             *
             */
            statement = connection.prepareStatement("SELECT viewer_id FROM \n" +
                    "(SELECT COUNT(Ratings.movie_id) AS movie_count, \n" +
                    "Ratings.viewer_id AS viewer_id\n FROM Ratings INNER JOIN \n" +
                    "(SELECT viewer_id, movie_id FROM Ratings WHERE viewer_id=?)\n" +
                    "AS ViewersMovies ON Ratings.movie_id = ViewersMovies.movie_id AND \n" +
                    "Ratings.viewer_id <> ViewersMovies.viewer_id GROUP BY \n" +
                    "Ratings.viewer_id) AS SimilarViewers\n WHERE movie_count >= \n" +
                    "((SELECT COUNT(movie_id) FROM Ratings WHERE viewer_id = ?)*3 +3) / 4 \n" +
                    "AND viewer_id <> ? ORDER BY viewer_id");
            statement.setInt(1, viewerId);
            statement.setInt(2, viewerId);
            statement.setInt(3, viewerId);
            ResultSet res = statement.executeQuery();
            while(res.next()) {
                //Add all of the viewers' IDs to the list
                list.add(res.getInt(1));
            }

        } catch (SQLException e) {
            return list;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return list;
    }

    /**
     * Returns the top 10 viewers' IDs with the highest views and rating count across the database
     * views count
     * @return: the top 10 viewers' IDs with the highest views and rating count, ordered primarily
     * by views count, descending order, and then rating count, descending order (as secondary order),
     * and in the case of equality: by id, ascending order (as third order)
     */
    public static ArrayList<Integer> mostInfluencingViewers()
    {
        final int LIMIT = 10; //Return 10 viewer IDs at most
        ArrayList<Integer> list = new ArrayList<>();
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Documented in the attached PDF file
             */
            statement = connection.prepareStatement("SELECT viewList.viewer_id AS viewer FROM \n" +
                    "(SELECT viewer_id, COUNT(rating) AS votes FROM Ratings WHERE rating \n" +
                    "IS NOT NULL GROUP BY viewer_id) AS ratingList RIGHT JOIN (SELECT viewer_id, \n" +
                    " COUNT(movie_id) AS views FROM Ratings GROUP BY viewer_id) AS viewList ON \n" +
                    "ratingList.viewer_id = viewList.viewer_id ORDER BY -views ASC, -votes ASC, viewer ASC");
            ResultSet res = statement.executeQuery();
            while(res.next() && list.size() < LIMIT) {
                //Add the top 10 viewers' IDs to the list (WE DIDN'T STUDY ABOUT "TOP" / "LIMIT" IN SQL)
                list.add(res.getInt(1));
            }
        } catch (SQLException e) {
            return list;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return list;
    }


    public static ArrayList<Integer> getMoviesRecommendations(Integer viewerId)
    {
        final int LIMIT = 10; //Return 10 viewer IDs at most
        ArrayList<Integer> list = new ArrayList<>();
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Documented in the attached PDF file
             */
            statement = connection.prepareStatement("SELECT movieList.movie_id, likesAmount FROM\n" +
                    "(SELECT movie_id, COUNT(rating) AS likesAmount FROM Ratings \n" +
                    "WHERE viewer_id IN (SELECT viewer_id FROM (SELECT COUNT(Ratings.movie_id)\n" +
                    "AS movie_count, Ratings.viewer_id AS viewer_id FROM Ratings INNER JOIN \n" +
                    "(SELECT viewer_id, movie_id FROM Ratings WHERE viewer_id=?)\n" +
                    "AS ViewersMovies ON Ratings.movie_id = ViewersMovies.movie_id \n" +
                    "AND Ratings.viewer_id <> ViewersMovies.viewer_id GROUP BY\n" +
                    "Ratings.viewer_id) AS SimilarViewers WHERE movie_count >=\n" +
                    "((SELECT COUNT(movie_id) FROM Ratings WHERE viewer_id = ?) * 3+3) / 4\n" +
                    "AND viewer_id <> ? ORDER BY viewer_id) AND movie_id NOT IN\n" +
                    "(SELECT movie_id FROM Ratings WHERE viewer_id = ?) AND rating = 'LIKE'\n" +
                    "GROUP BY movie_id ORDER BY likesAmount DESC,movie_id ASC)AS likesList\n" +
                    "RIGHT OUTER JOIN\n" +
                    "(SELECT movie_id FROM Ratings \n" +
                    "WHERE viewer_id IN (SELECT viewer_id FROM (SELECT COUNT(Ratings.movie_id)\n" +
                    "AS movie_count, Ratings.viewer_id AS viewer_id FROM Ratings INNER JOIN \n" +
                    "(SELECT viewer_id, movie_id FROM Ratings WHERE viewer_id=?) AS\n" +
                    "ViewersMovies ON Ratings.movie_id = ViewersMovies.movie_id \n" +
                    "AND Ratings.viewer_id <> ViewersMovies.viewer_id GROUP BY Ratings.viewer_id)\n" +
                    "AS SimilarViewers WHERE movie_count >= ((SELECT COUNT(movie_id) FROM Ratings\n" +
                    "WHERE viewer_id = ?) * 3+3) / 4 AND viewer_id <> ? ORDER BY viewer_id) \n" +
                    "AND movie_id NOT IN (SELECT movie_id FROM Ratings WHERE viewer_id = ?)\n" +
                    "GROUP BY movie_id) AS movieList\n" +
                    "ON movieList.movie_id = likesList.movie_id ORDER BY -likesAmount ASC,movie_id ASC");
            statement.setInt(1, viewerId);
            statement.setInt(2, viewerId);
            statement.setInt(3, viewerId);
            statement.setInt(4, viewerId);
            statement.setInt(5, viewerId);
            statement.setInt(6, viewerId);
            statement.setInt(7, viewerId);
            statement.setInt(8, viewerId);
            ResultSet res = statement.executeQuery();
            while(res.next() && list.size() < LIMIT) {
                //Add the top 10 viewers' IDs to the list (WE DIDN'T STUDY ABOUT "TOP" / "LIMIT" IN SQL)
                list.add(res.getInt(1));
            }
        } catch (SQLException e) {
            return list;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return list;
    }


    public static ArrayList<Integer> getConditionalRecommendations(Integer viewerId, int movieId)
    {
        final int LIMIT = 10; //Return 10 viewer IDs at most
        ArrayList<Integer> list = new ArrayList<>();
        Connection connection = DBConnector.getConnection();
        PreparedStatement statement = null;

        try {
            /*
             * Documented in the attached PDF file
             */
            statement = connection.prepareStatement("SELECT movieList.movie_id, likesAmount FROM \n" +
                    "(SELECT movie_id, COUNT(rating) AS likesAmount FROM Ratings \n" +
                    "WHERE viewer_id IN (SELECT viewer_id FROM Ratings WHERE viewer_id IN \n" +
                    "(SELECT viewer_id FROM (SELECT COUNT(Ratings.movie_id) AS movie_count, \n" +
                    "Ratings.viewer_id AS viewer_id FROM Ratings INNER JOIN (SELECT viewer_id, \n" +
                    "movie_id FROM Ratings WHERE viewer_id=?) AS ViewersMovies ON Ratings.movie_id = \n" +
                    "ViewersMovies.movie_id AND Ratings.viewer_id <> ViewersMovies.viewer_id \n" +
                    "GROUP BY Ratings.viewer_id) AS SimilarViewers WHERE movie_count >= \n" +
                    "((SELECT COUNT(movie_id) FROM Ratings WHERE viewer_id = ?) * 3+3) / 4 AND \n" +
                    "viewer_id <> ? ORDER BY viewer_id) AND movie_id = ? AND rating = \n" +
                    "(SELECT rating FROM Ratings WHERE viewer_id = ? AND movie_id = ?)) \n" +
                    "AND movie_id NOT IN (SELECT movie_id FROM Ratings WHERE viewer_id = ?) AND \n" +
                    "rating = 'LIKE' GROUP BY movie_id ORDER BY likesAmount DESC,movie_id ASC) AS likesList\n" +
                    "RIGHT OUTER JOIN\n" +
                    "(SELECT movie_id FROM Ratings \n" +
                    "WHERE viewer_id IN (SELECT viewer_id FROM Ratings WHERE viewer_id IN \n" +
                    "(SELECT viewer_id FROM (SELECT COUNT(Ratings.movie_id) AS movie_count, Ratings.viewer_id AS viewer_id FROM Ratings INNER JOIN \n" +
                    "(SELECT viewer_id, movie_id FROM Ratings WHERE viewer_id=?) AS ViewersMovies ON Ratings.movie_id = ViewersMovies.movie_id \n" +
                    "AND Ratings.viewer_id <> ViewersMovies.viewer_id GROUP BY Ratings.viewer_id) AS SimilarViewers \n" +
                    "WHERE movie_count >= ((SELECT COUNT(movie_id) FROM Ratings WHERE viewer_id = ?) * 3+3) / 4 AND viewer_id <> ? ORDER BY viewer_id)\n" +
                    "AND movie_id = ? AND rating = (SELECT rating FROM Ratings WHERE viewer_id = ? AND movie_id = ?))\n" +
                    "AND movie_id NOT IN (SELECT movie_id FROM Ratings WHERE viewer_id = ?) GROUP BY movie_id) AS movieList\n" +
                    "ON movieList.movie_id = likesList.movie_id ORDER BY -likesAmount ASC, movie_id ASC");
            statement.setInt(1, viewerId);
            statement.setInt(2, viewerId);
            statement.setInt(3, viewerId);
            statement.setInt(4, movieId);
            statement.setInt(5, viewerId);
            statement.setInt(6, movieId);
            statement.setInt(7, viewerId);
            statement.setInt(8, viewerId);
            statement.setInt(9, viewerId);
            statement.setInt(10, viewerId);
            statement.setInt(11, movieId);
            statement.setInt(12, viewerId);
            statement.setInt(13, movieId);
            statement.setInt(14, viewerId);
            ResultSet res = statement.executeQuery();
            while(res.next() && list.size() < LIMIT) {
                //Add the top 10 viewers' IDs to the list (WE DIDN'T STUDY ABOUT "TOP" / "LIMIT" IN SQL)
                list.add(res.getInt(1));
            }
        } catch (SQLException e) {
            return list;
        } finally {
            try {
                statement.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return list;
    }

}
