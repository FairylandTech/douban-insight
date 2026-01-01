/*****************************************************
 * @software: IntelliJ IDEA
 * @author: Lionel Johnson
 * @contact: https://fairy.host
 * @organization: https://github.com/FairylandFuture
 * @datetime: 2026-01-02 04:54:50 UTC+08:00
 ****************************************************/
package host.fairy.controller;

import host.fairy.fairylandfuture.common.web.response.Response;
import host.fairy.model.MovieModel;
import host.fairy.service.MovieService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author Lionel Johnson
 * @version 1.0
 */
@Slf4j
@RestController
@RequestMapping("v1/movie")
public class MovieController {
    
    private final MovieService movieService;
    
    public MovieController(MovieService movieService) {
        this.movieService = movieService;
    }
    
    @GetMapping("/{movieId}")
    public Response<MovieModel> getMovieFetchOne(@PathVariable("movieId") String movieId) {
        log.info("Fetching movie with ID: {}", movieId);
        MovieModel movie = this.movieService.getMovieByMovieId(movieId);
        return Response.success(movie);
    }
}
