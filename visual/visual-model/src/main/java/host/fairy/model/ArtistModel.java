/*****************************************************
 * @software: IntelliJ IDEA
 * @author: Lionel Johnson
 * @contact: https://fairy.host
 * @organization: https://github.com/FairylandFuture
 * @datetime: 2026-01-02 03:12:07 UTC+08:00
 ****************************************************/
package host.fairy.model;

import host.fairy.fairylandfuture.model.ModelBase;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * @author Lionel Johnson
 * @version 1.0
 */
@Data
@Builder
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ArtistModel extends ModelBase {
    
    private String artistId;
    
    private String name;
}
