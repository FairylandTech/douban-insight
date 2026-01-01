/*****************************************************
 * @software: IntelliJ IDEA
 * @author: Lionel Johnson
 * @contact: https://fairy.host
 * @organization: https://github.com/FairylandFuture
 * @datetime: 2026-01-02 04:47:24 UTC+08:00
 ****************************************************/
package host.fairy.service;

import host.fairy.model.MovieModel;

/**
 * @author Lionel Johnson
 * @version 1.0
 */
public interface MovieService {
    MovieModel getMovieByMovieId(String movieId);
}
