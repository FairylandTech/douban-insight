/*****************************************************
 * @software: IntelliJ IDEA
 * @author: Lionel Johnson
 * @contact: https://fairy.host
 * @organization: https://github.com/FairylandFuture
 * @datetime: 2026-01-02 02:57:52 UTC+08:00
 ****************************************************/
package host.fairy.model;

import host.fairy.fairylandfuture.model.ModelBase;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.time.LocalDate;

/**
 * @author Lionel Johnson
 * @version 1.0
 */
@Data
@Builder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class MovieModel extends ModelBase {
    
    private String movieId;
    
    private String fullName;
    
    private String chineseName;
    
    private String originalName;
    
    private LocalDate releaseDate;
    
    private Double score;
    
    private String summary;
    
    private String icon;
}
