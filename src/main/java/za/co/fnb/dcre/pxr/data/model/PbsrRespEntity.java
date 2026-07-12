package za.co.fnb.dcre.pxr.data.model;

import org.springframework.data.relational.core.mapping.Table;
import za.co.fnb.dcre.platform.persistence.BaseEntity;

/** One reply verdict per original transaction (PBSR response leg). */
@Table("pbsr_resp")
public class PbsrRespEntity extends BaseEntity {

    private String responseFile;
    private String orgnlMsgId;
    private String e2e;
    private String status;
    private String reason;

    public String getResponseFile() { return responseFile; }
    public String getOrgnlMsgId() { return orgnlMsgId; }
    public String getE2e() { return e2e; }
    public String getStatus() { return status; }
    public String getReason() { return reason; }
}
