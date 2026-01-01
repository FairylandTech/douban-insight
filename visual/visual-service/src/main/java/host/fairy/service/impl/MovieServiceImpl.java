/*****************************************************
 * @software: IntelliJ IDEA
 * @author: Lionel Johnson
 * @contact: https://fairy.host
 * @organization: https://github.com/FairylandFuture
 * @datetime: 2026-01-02 04:48:15 UTC+08:00
 ****************************************************/
package host.fairy.service.impl;

import host.fairy.mapper.MovieMapper;
import host.fairy.model.MovieModel;
import host.fairy.service.MovieService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Lionel Johnson
 * @version 1.0
 */
@Slf4j
@Service
public class MovieServiceImpl implements MovieService {
    
    private final MovieMapper movieMapper;
    
    public MovieServiceImpl(MovieMapper movieMapper) {
        this.movieMapper = movieMapper;
    }
    
    @Override
    public MovieModel getMovieByMovieId(String movieId) {
        MovieModel movie = movieMapper.selectMovieByMovieId(movieId);
        log.info("Mapper result movie: {}", movie);
        return movie;
    }
}
