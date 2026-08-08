package za.co.fnb.dcre.cpx.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

import java.util.UUID;

/**
 * One reply verdict per original transaction (PBSR response leg).
 * emissionId links the verdict to the emitting CRW batch (SCRUM-55);
 * NULL means the emission registry had no row at ingest time.
 */
@Table("pbsr_resp")
public class PbsrRespEntity extends BaseEntity {

    private String responseFile;
    private String orgnlMsgId;
    private UUID emissionId;
    private String e2e;
    private String status;
    private String reason;

    public String getResponseFile() { return responseFile; }
    public String getOrgnlMsgId() { return orgnlMsgId; }
    public UUID getEmissionId() { return emissionId; }
    public String getE2e() { return e2e; }
    public String getStatus() { return status; }
    public String getReason() { return reason; }
}
