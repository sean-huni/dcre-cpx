# dcre-pxr

PBSR response-leg reader (SCRUM-27, M4). Ingests synthetic pain.002-family PBSR reply files (token _PBSR) into pbsr_resp: one row per Tx block. Replay-safe via INSERT ... ON CONFLICT (response_file, e2e). 3-tier: ReaderTasklet -> ReaderService -> data/repo. [SYNTHETIC-CONTRACT R-35] reply shape.
