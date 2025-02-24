## modeling out data mart tables w/o dbt

models = {
    "base__agency_section": {
        "query": """
            SELECT 
                a.name AS agency,
                s.id,
                s.chapter,
                s.title,
                s.subtitle,
                s.part,
                s.subpart,
                s.subpart_id,
                s.HEAD AS header_text,
                s.p AS paragraph_text,
                COALESCE(s.HEAD || ' ' || s.p, '') AS full_text
            FROM agency a
            JOIN agency_section_ref r 
                ON a.slug = r.slug
            JOIN section s 
                ON CAST(s.cfr_ref_title AS INTEGER) = r.cfr_ref_title
                AND s.cfr_ref_chapter IS NOT DISTINCT FROM r.cfr_ref_chapter
            WHERE s.is_current = TRUE
            ORDER BY a.name, s.title, s.chapter
        """,
        "type": "table",
        "stmt_type": "create"
    },
    "stg__agg_chapter_part_doc_count": {
        "query": """
            WITH base_filtered AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    subpart_id,
                    id,
                    -- Combine all text for this agency + chapter + part + head
                    STRING_AGG(full_text, '\n\n') AS combined_full_text,
                    COUNT(*) AS section_count
                FROM base__agency_section
                GROUP BY agency, title, chapter, part, subpart_id, id
            ),

            -- 2) Expand tokens for counting
            expanded AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    subpart_id,
                    id,
                    section_count,
                    combined_full_text,
                    SPLIT(REPLACE(combined_full_text, '  ', ' '), ' ') AS all_tokens
                FROM base_filtered
            ),

            -- 3) Unnest into rows
            unnested AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    subpart_id,
                    id,
                    section_count,
                    TRIM(token) AS token
                FROM expanded
                CROSS JOIN UNNEST(all_tokens) AS t(token)
            ),

            -- 4) Exclude stopwords, count frequencies
            counts AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    subpart_id,
                    id,
                    section_count,
                    token,
                    COUNT(*) AS cnt
                FROM unnested
                WHERE 
                    NOT REGEXP_MATCHES(
                        token,
                        '^(§|a|about|above|after|again|against|all|am|an|and|any|are|aren''t|as|at|be|because|been|before|being|below|between|both|but|by|b|can|can''t|cannot|could|could''t|cfr|c|did|did''t|do|does|does''t|doing|don''t|down|during|d|each|e|few|for|from|further|had|had''t|has|has''t|have|haven''t|having|he|he''d|he''ll|he''s|her|here|here''s|hers|herself|him|himself|his|how|how''s|i|i''d|i''ll|i''m|i''ve|if|in|into|is|isn''t|it|it''s|its|itself|let''s|me|may|more|most|must|mustn''t|my|myself|no|nor|not|of|off|on|once|only|or|other|ought|our|ours|ourselves|out|over|own|paragraph|part|required|reserved|same|section|shall|shan''t|she|she''d|she''ll|she''s|should|shouldn''t|so|s|some|such|subpart|''#text'':|than|that|that''s|the|their|theirs|them|themselves|then|there|there''s|these|they|they''d|they''ll|they''re|they''ve|this|those|through|to|too|under|until|up|u|very|was|wasn''t|we|we''d|we''ll|we''re|we''ve|were|weren''t|what|what''s|when|when''s|where|where''s|which|while|who|who''s|whom|why|why''s|with|won''t|would|wouldn''t|you|you''d|you''ll|you''re|you''ve|your|yours|yourself|yourselves)$',
                        'i'
                    )
                    AND NOT REGEXP_MATCHES(token, '^(\\d+|{''[@A-Z]+'':)$')
                GROUP BY agency, title, chapter, part, subpart_id, id, section_count, token
            ),

            -- 5) Rank top tokens
            ranked AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    subpart_id,
                    id,
                    section_count,
                    token,
                    cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY agency, chapter, part
                        ORDER BY cnt DESC
                    ) AS rnk
                FROM counts
            ),

            top15 AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    subpart_id,
                    id,
                    section_count,
                    STRING_AGG(token || ' (' || cnt || ')', ', ') AS top_words
                FROM ranked
                WHERE rnk <= 15
                GROUP BY agency, title, chapter, part, subpart_id, id, section_count
            ),

            -- 6) Count total words
            word_counts AS (
                SELECT
                    agency,
                    title,
                    chapter,
                    part,
                    subpart_id,
                    id,
                    section_count,
                    SUM(array_length(all_tokens, 1)) AS total_word_count
                FROM expanded
                GROUP BY agency, title, chapter, part, subpart_id, id, section_count
            )

            -- 7) Combine them all
            SELECT
                wc.agency,
                wc.title,
                wc.chapter,
                wc.part,
                wc.subpart_id,
                wc.id,
                wc.section_count,
                wc.total_word_count,
                t.top_words,
                bf.combined_full_text
            FROM word_counts wc
            JOIN top15 t
              ON t.agency = wc.agency
              AND t.id = wc.id
             AND t.section_count = wc.section_count
            JOIN base_filtered bf
              ON bf.agency  = wc.agency
              AND bf.id = wc.id
            ORDER BY total_word_count DESC

        """,
        "type": "table",
        "stmt_type": "create"
    },
    "stg__agg_chapter_part_doc_count_unnested": {
        "query": """
            SELECT
                agency,
                title,
                chapter,
                part,
                subpart_id,
                id,
                section_count,
                total_word_count,
                top_words,
                UNNEST(STRING_SPLIT(combined_full_text, '\n\n')) AS full_text
            FROM stg__agg_chapter_part_doc_count
        """,
        "type": "table",
        "stmt_type": "create"
    },
    "stg__agg_section_change_metrics": {
        "query": """
        WITH aggregated AS (
        SELECT 
            COALESCE(a.name, '') AS agency,
            s.title,
            s.chapter,
            s.subtitle,
            s.part,
            s.subpart,
            s.subpart_id,
            s.id,
            s.n AS node,
            s.is_current,
            MAX(s.valid_from) AS max_valid_from,
            COUNT(CASE WHEN p_delta_chars <> 0 OR is_deleted = TRUE THEN 1 END) AS count_p_deltas,
            SUM(p_delta_chars) AS sum_p_delta_chars,
            SUM(CASE WHEN is_deleted = TRUE THEN LENGTH(s.p) ELSE 0 END) AS sum_deleted_chars
        FROM section s
        LEFT JOIN agency_section_ref r 
            ON CAST(s.cfr_ref_title AS INTEGER) = r.cfr_ref_title
            AND s.cfr_ref_chapter IS NOT DISTINCT FROM r.cfr_ref_chapter
        LEFT JOIN agency a
            ON a.slug = r.slug
        GROUP BY ALL
        ),

        rolling_aggregated AS (
        SELECT 
            agency,
            title,
            chapter,
            subtitle,
            part,
            subpart,
            subpart_id,
            id,
            node,
            max_valid_from,
            COALESCE(sum_p_delta_chars, 0) AS sum_p_delta_chars,
            COALESCE(count_p_deltas, 0) AS count_p_deltas,
            COALESCE(ROUND(AVG(sum_p_delta_chars) OVER (
                PARTITION BY id
                ORDER BY max_valid_from
                RANGE BETWEEN INTERVAL '60 months' PRECEDING AND CURRENT ROW
            ), 3), 0) AS rolling_60m_avg_sum_p_delta_chars,
            COALESCE(ROUND(AVG(count_p_deltas) OVER (
                PARTITION BY id
                ORDER BY max_valid_from
                RANGE BETWEEN INTERVAL '60 months' PRECEDING AND CURRENT ROW
            ), 3), 0) AS rolling_60m_avg_count_p_deltas,
            is_current
        FROM aggregated
        )

        SELECT * FROM rolling_aggregated
        WHERE is_current = TRUE
        ORDER BY count_p_deltas DESC

        """,
        "type": "table",
        "stmt_type": "create"
    }
#     # "stg__agency_section_correction": {
#     #     "query": """
#     #         SELECT 
#     #             bas.agency,
#     #             bas.chapter,
#     #             bas.title,
#     #             bas.subtitle,
#     #             bas.part,
#     #             bas.subpart,
#     #             bas.subpart_id,
#     #             bas.header_text,
#     #             bas.paragraph_text,
#     #             bas.full_text,
#     #             corr.corrective_action,
#     #             corr.error_corrected,
#     #             corr.error_occurred,
#     #             corr.fr_citation,
#     #             COALESCE(corr.corrective_action, bas.full_text) AS corrected_full_text
#     #         FROM base__agency_section bas
#     #         LEFT JOIN ecfr_analyzer_local.correction corr
#     #             ON bas.title = json_extract_string(
#     #                             TRY_CAST(REPLACE(corr.cfr_references, '''', '"') AS JSON),
#     #                             '$[0].hierarchy.title'
#     #                         )
#     #             AND bas.chapter = json_extract_string(
#     #                                 TRY_CAST(REPLACE(corr.cfr_references, '''', '"') AS JSON),
#     #                                 '$[0].hierarchy.chapter'
#     #                             )
#     #             AND bas.part = json_extract_string(
#     #                                 TRY_CAST(REPLACE(corr.cfr_references, '''', '"') AS JSON),
#     #                                 '$[0].hierarchy.part'
#     #                             )
#     #         ORDER BY bas.agency, bas.title, bas.chapter
#     #     """,
#     #     "type": "table",
#     #     "stmt_type": "create"
#     # }
    
}

