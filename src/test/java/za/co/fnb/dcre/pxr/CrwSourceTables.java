package za.co.fnb.dcre.pxr;

import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Test bootstrap for the CRW-owned source tables the reader correlates
 * against. PXR does NOT own this DDL (crw does, 001-crw.xml + 003-split.xml
 * on the crw repo); tests create the column shapes via plain JDBC, the same
 * pattern PRG uses for its bootstrap sources. Never add crw DDL to the pxr
 * changelog.
 */
public final class CrwSourceTables {

    private CrwSourceTables() {
    }

    public static void ensure(final JdbcTemplate jdbc) {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS crw_emission (
                    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
                    arrival_id UUID NOT NULL,
                    run_date DATE NOT NULL,
                    file_name VARCHAR(128) NOT NULL,
                    state VARCHAR(32) NOT NULL,
                    group_id UUID,
                    batch_ordinal INT NOT NULL DEFAULT 1,
                    outbound_msg_id VARCHAR(64),
                    tx_count BIGINT,
                    control_sum DECIMAL(18,2),
                    visible_at TIMESTAMPTZ,
                    version BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (arrival_id, run_date, batch_ordinal)
                )""");
        jdbc.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_emission_outbound_msg"
                + " ON crw_emission (outbound_msg_id)");
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS crw_emission_member (
                    id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
                    emission_id UUID NOT NULL,
                    sequence INT NOT NULL,
                    e2e VARCHAR(35) NOT NULL,
                    amount DECIMAL(18,2) NOT NULL,
                    version BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    UNIQUE (emission_id, sequence)
                )""");
    }
}
