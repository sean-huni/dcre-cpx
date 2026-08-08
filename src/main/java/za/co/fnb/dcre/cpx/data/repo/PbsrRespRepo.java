package za.co.fnb.dcre.cpx.data.repo;

import org.springframework.data.jdbc.repository.query.Modifying;
import org.springframework.data.jdbc.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import za.co.fnb.dcre.cpx.data.model.PbsrRespEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PbsrRespRepo extends CrudRepository<PbsrRespEntity, UUID> {

    // CRDB: UPSERT resolves on PK only, so replay safety needs
    // INSERT ... ON CONFLICT on the business identity (response_file, e2e).
    // emission_id backfills monotonically on replay (COALESCE): a late
    // registry catch-up upgrades NULL, a known correlation is never lost.
    @Modifying
    @Query("""
            INSERT INTO pbsr_resp (id, response_file, orgnl_msg_id, emission_id, e2e, status, reason)
            VALUES (gen_random_uuid(), :responseFile, :orgnlMsgId, :emissionId, :e2e, :status, :reason)
            ON CONFLICT (response_file, e2e)
            DO UPDATE SET status = excluded.status, reason = excluded.reason,
                          emission_id = COALESCE(excluded.emission_id, pbsr_resp.emission_id),
                          updated_at = now()""")
    void upsert(@Param("responseFile") String responseFile, @Param("orgnlMsgId") String orgnlMsgId,
                @Param("emissionId") UUID emissionId, @Param("e2e") String e2e,
                @Param("status") String status, @Param("reason") String reason);

    /** Resolves a reply's OrgnlMsgId to the emitting CRW batch (SCRUM-55 correlation). */
    @Query("SELECT id FROM crw_emission WHERE outbound_msg_id = :m")
    Optional<UUID> findEmissionIdByOutboundMsgId(@Param("m") String m);

    /** Member e2e ids of one batch; bounded by max-split-size, safe as an in-memory set. */
    @Query("SELECT e2e FROM crw_emission_member WHERE emission_id = :id")
    List<String> findMemberE2e(@Param("id") UUID id);

    long countByResponseFile(String responseFile);
}
