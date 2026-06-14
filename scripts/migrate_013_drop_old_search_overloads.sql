-- Migration 013: drop the pre-entity search-RPC overloads
--
-- migrate_012 added filter_entity_id to semantic_search/keyword_search. Because
-- the parameter list changed, CREATE OR REPLACE created a SECOND overload rather
-- than replacing the old one, leaving two functions of each name. PostgREST
-- then cannot choose between them (PGRST203) and every search call fails. Drop
-- the old (pre-entity) signatures so only the entity-aware versions remain.
--
-- Idempotent (DROP ... IF EXISTS); matches the exact old signatures only, so the
-- new 7-arg / 6-arg entity-aware functions survive.

DROP FUNCTION IF EXISTS semantic_search(VECTOR(384), FLOAT, INT, DATE, DATE, TEXT);
DROP FUNCTION IF EXISTS keyword_search(TEXT, INT, DATE, DATE, TEXT);
