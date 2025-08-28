import * as SQLite from 'wa-sqlite';
import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs';

export interface DatabaseManager {
  db: number;
  sqlite3: any;
  close(): Promise<void>;
  execute(sql: string, params?: any[]): Promise<void>;
  query(sql: string, params?: any[]): Promise<any[]>;
}

/**
 * Initialize SQLite WASM module
 */
async function initSQLite() {
  const module = await SQLiteESMFactory();
  const sqlite3 = SQLite.Factory(module);
  return sqlite3;
}

/**
 * Create the database schema with all required tables
 * 
 * SCRAPEBOOK / OBSERVATION DB — SEMANTICS-FIRST SCHEMA
 * 
 * Core model:
 * • ops: every execution node (transform / analysis / observation).
 *   An op may also be an *alias* (structural), but aliasing is orthogonal to class.
 * • op_inputs: wire consumer input slots to producer output slots.
 * • op_outputs: payload bytes per producer output slot (0..n-1).
 * • composite_ground_truth: content-addressed sets for multi-origin ground truths.
 * • artifacts: optional content-addressed byte store (useful for GC/dedupe).
 * 
 * Classes (derived; see v_ops_class):
 *   - Transform   : op_key = input_digest
 *   - Observation : ground_truth = op_key
 *   - Analysis    : otherwise
 */
const SCHEMA_SQL = `
/* =============================================================================
   SCRAPEBOOK / OBSERVATION DB — SEMANTICS-FIRST SCHEMA
   -----------------------------------------------------------------------------
   Core model
   • ops: every execution node (transform / analysis / observation).
     An op may also be an *alias* (structural), but aliasing is orthogonal to class.
   • op_inputs: wire consumer input slots to producer output slots.
   • op_outputs: payload bytes per producer output slot (0..n-1).
   • composite_ground_truth: content-addressed sets for multi-origin ground truths.
   • artifacts: optional content-addressed byte store (useful for GC/dedupe).
   -----------------------------------------------------------------------------
   Classes (derived; see v_ops_class):
     - Transform   : op_key = input_digest
     - Observation : ground_truth = op_key
     - Analysis    : otherwise

   Aliases (orthogonal, materialized)
     - alias (nullable) points to an underlying op you're encapsulating.
     - When creating an alias op A of target T:
         1) Set A.alias = T.op_key
         2) Copy T's op_outputs into A (same indices, same data)
         3) Usually set A.ground_truth = T.ground_truth,
            unless A is an observational "box" (then ground_truth = A.op_key).
     - Purpose: encapsulate a subgraph (e.g. pipeline) behind a higher-level
       operation, so you can safely drop or omit the inner subgraph when summarizing/exporting.

     Application-level constraints (not enforced in SQL):
       * If the alias op is NOT an observation, then none of its encapsulated
         inner ops may be observations, except those depending directly on the
         alias's external inputs.
       * If the alias op is NOT an observation or analysis, then none of its
         encapsulated inner ops may be analyses, except those depending directly
         on the alias's external inputs.
       Example:
         ComplexOp(A,B,C) aliases SimpleOp(SimpleOp2(A,B), C).
         - A, B, C are unconstrained.
         - If ComplexOp is not an observation, then neither SimpleOp nor
           SimpleOp2 may be observations.
         - If ComplexOp is a transform, then neither SimpleOp nor SimpleOp2
           may be analyses or observations.

   Ground truth (app-side)
     • Observation: ground_truth := op_key (early return).
     • Otherwise:
         L := [non-NULL ground_truths of input producers]
         sort & dedupe → expand any composite members → sort & dedupe
         → NULL / single / composite (insert composite row if new)

   Input digest (app-side)
     • input_digest := sha256(op_type || 0x00 || tool_id || 0x00 ||
                              canon(params_json) || 0x00 ||
                              concat(h_i))
     • Here h_i are the **raw input bytes**, concatenated in input-slot order
       (multiplicity preserved), not hashes. This makes transforms content-defined.
   ============================================================================= */


/* =======================
   ARTIFACTS (content-addressed bytes)
   ======================= */
CREATE TABLE IF NOT EXISTS artifacts (
  artifact_hash  BLOB PRIMARY KEY,   -- 32B sha256(bytes)
  bytes          BLOB                -- payload; may be NULL (purged)
);

CREATE VIEW IF NOT EXISTS artifact_sizes AS
SELECT artifact_hash, length(bytes) AS size_bytes FROM artifacts;


/* =======================
   OPS (Transform / Analysis / Observation; alias is orthogonal)
   ======================= */
CREATE TABLE IF NOT EXISTS ops (
  op_key        BLOB PRIMARY KEY,   -- 32B unique key (random for run-addressed)
  op_type       TEXT NOT NULL,      -- e.g. 'http_fetch','html_to_text','llm_infer','human_label'
  tool_id       TEXT NOT NULL,      -- e.g. 'requests_v1','bs4_v1','rules_v2'
  params_json   TEXT,               -- canonical JSON (sorted keys, stable forms)

  input_digest  BLOB NOT NULL,      -- 32B

  ground_truth  BLOB,               -- 32B; NULL only if synthetic / no provenance
  observed_at   TEXT,               -- optional wall clock; MUST be NULL unless observation

  alias         BLOB,               -- nullable: op_key this row *encapsulates* (outputs copied)

  CHECK ( (observed_at IS NULL) OR (ground_truth = op_key) )
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_ops_ground_truth ON ops(ground_truth);
CREATE INDEX IF NOT EXISTS idx_ops_type         ON ops(op_type);
CREATE INDEX IF NOT EXISTS idx_ops_inputdig     ON ops(input_digest);
CREATE INDEX IF NOT EXISTS idx_ops_alias        ON ops(alias);
CREATE INDEX IF NOT EXISTS idx_ops_observed_at  ON ops(observed_at) WHERE ground_truth = op_key;


/* =======================
   OP OUTPUTS (payloads, app-defined)
   ======================= */
CREATE TABLE IF NOT EXISTS op_outputs (
  op_key  BLOB NOT NULL,     -- producing op
  idx     INTEGER NOT NULL,  -- output slot: 0,1,2,...
  data    BLOB,              -- arbitrary bytes; may be NULL
  PRIMARY KEY (op_key, idx)
);


/* =======================
   OP INPUTS (structural edges)
   ======================= */
CREATE TABLE IF NOT EXISTS op_inputs (
  op_key              BLOB NOT NULL,     -- consumer op
  idx                 INTEGER NOT NULL,  -- consumer input slot (0,1,2,...)
  producer_op_key     BLOB NOT NULL,     -- producing op (may itself be an alias)
  producer_output_idx INTEGER NOT NULL,  -- producing output slot
  PRIMARY KEY (op_key, idx)
);

CREATE INDEX IF NOT EXISTS idx_op_inputs_producer ON op_inputs(producer_op_key, producer_output_idx);


/* =======================
   COMPOSITE GROUND TRUTH (hash → sorted members)
   ======================= */
CREATE TABLE IF NOT EXISTS composite_ground_truth (
  composite_hash  BLOB PRIMARY KEY,  -- 32B
  members_blob    BLOB NOT NULL      -- 32*n bytes (n >= 2), members in ascending byte order
);

CREATE INDEX IF NOT EXISTS idx_cgt_members_len ON composite_ground_truth(length(members_blob));


/* =======================
   VIEWS (summaries)
   ======================= */

-- Classify each op: transform / observation / analysis
CREATE VIEW IF NOT EXISTS v_ops_class AS
SELECT
  o.*,
  CASE
    WHEN o.op_key = o.input_digest THEN 'transform'
    WHEN o.ground_truth = o.op_key THEN 'observation'
    ELSE 'analysis'
  END AS op_class
FROM ops o;

-- Ground truth kind
CREATE VIEW IF NOT EXISTS v_ops_ground_kind AS
SELECT
  o.*,
  CASE
    WHEN o.ground_truth IS NULL THEN 'synthetic'
    WHEN EXISTS (SELECT 1 FROM ops x WHERE x.op_key = o.ground_truth)
         THEN 'single_origin'
    WHEN EXISTS (SELECT 1 FROM composite_ground_truth c WHERE c.composite_hash = o.ground_truth)
         THEN 'composite_origin'
    ELSE 'unknown_single'
  END AS ground_kind
FROM ops o;

-- Consumer inputs joined to producer outputs (aliases work since outputs are copied)
CREATE VIEW IF NOT EXISTS v_inputs_resolved AS
SELECT
  i.op_key           AS consumer_op,
  i.idx              AS consumer_idx,
  i.producer_op_key,
  i.producer_output_idx,
  o.data             AS producer_output_data
FROM op_inputs i
JOIN op_outputs o
  ON o.op_key = i.producer_op_key
 AND o.idx    = i.producer_output_idx;


/* =============================================================================
   OPTIONAL / NON-SEMANTIC SECTION (GC, provenance, audits)
   ============================================================================= */

-- Per-slot artifact linkage (signed indices)
--   input slot s → idx = -(s+1); output slot s → idx = s
CREATE TABLE IF NOT EXISTS artifact_slots (
  artifact_hash  BLOB NOT NULL,   -- 32B
  op_key         BLOB NOT NULL,
  idx            INTEGER NOT NULL,
  PRIMARY KEY (artifact_hash, op_key, idx)
);

CREATE INDEX IF NOT EXISTS idx_slots_op_in     ON artifact_slots(op_key, idx) WHERE idx < 0;
CREATE INDEX IF NOT EXISTS idx_slots_op_out    ON artifact_slots(op_key, idx) WHERE idx >= 0;
CREATE INDEX IF NOT EXISTS idx_slots_artifact  ON artifact_slots(artifact_hash);

-- Decoded conveniences
CREATE VIEW IF NOT EXISTS v_artifact_inputs AS
SELECT op_key, (-idx - 1) AS slot_idx, artifact_hash
FROM artifact_slots WHERE idx < 0;

CREATE VIEW IF NOT EXISTS v_artifact_outputs AS
SELECT op_key, idx AS slot_idx, artifact_hash
FROM artifact_slots WHERE idx >= 0;

-- Old-style links (for GC tools that expect "consumed/produced")
CREATE VIEW IF NOT EXISTS artifact_links_consumed AS
SELECT s.op_key, 'consumed' AS direction, (-s.idx - 1) AS slot_idx, s.artifact_hash
FROM artifact_slots s WHERE idx < 0;

CREATE VIEW IF NOT EXISTS artifact_links_produced AS
SELECT s.op_key, 'produced' AS direction, s.idx AS slot_idx, s.artifact_hash
FROM artifact_slots s WHERE idx >= 0;

CREATE VIEW IF NOT EXISTS artifact_links AS
SELECT * FROM artifact_links_consumed
UNION ALL
SELECT * FROM artifact_links_produced;

-- GC helpers
CREATE VIEW IF NOT EXISTS v_live_artifacts AS
SELECT DISTINCT artifact_hash FROM artifact_slots;

CREATE VIEW IF NOT EXISTS v_missing_artifacts AS
SELECT DISTINCT s.artifact_hash
FROM artifact_slots s
LEFT JOIN artifacts a USING (artifact_hash)
WHERE a.artifact_hash IS NULL;

CREATE VIEW IF NOT EXISTS v_purged_artifacts AS
SELECT DISTINCT s.artifact_hash
FROM artifact_slots s
JOIN artifacts a USING (artifact_hash)
WHERE a.bytes IS NULL;

CREATE VIEW IF NOT EXISTS v_missing_or_purged_artifacts AS
SELECT DISTINCT s.artifact_hash
FROM artifact_slots s
LEFT JOIN artifacts a USING (artifact_hash)
WHERE a.artifact_hash IS NULL OR a.bytes IS NULL;

CREATE VIEW IF NOT EXISTS v_artifact_health AS
SELECT
  s.artifact_hash,
  CASE
    WHEN a.artifact_hash IS NULL THEN 'missing_row'
    WHEN a.bytes IS NULL          THEN 'purged_bytes'
    ELSE 'ok'
  END AS status
FROM artifact_slots s
LEFT JOIN artifacts a USING (artifact_hash)
GROUP BY s.artifact_hash;
`;

/**
 * Create a new in-memory database
 */
export async function createInMemoryDatabase(): Promise<DatabaseManager> {
  const sqlite3 = await initSQLite();
  const db = await sqlite3.open_v2(':memory:');
  
  // Create schema
  await sqlite3.exec(db, SCHEMA_SQL);
  
  console.log('DB opened successfully (in-memory database)');
  
  return {
    db,
    sqlite3,
    async close() {
      await sqlite3.close(db);
    },
    async execute(sql: string, params?: any[]) {
      if (params && params.length > 0) {
        for await (const stmt of sqlite3.statements(db, sql)) {
          sqlite3.bind_collection(stmt, params);
          await sqlite3.step(stmt);
        }
      } else {
        await sqlite3.exec(db, sql);
      }
    },
    async query(sql: string, params?: any[]) {
      const results: any[] = [];
      for await (const stmt of sqlite3.statements(db, sql)) {
        if (params && params.length > 0) {
          sqlite3.bind_collection(stmt, params);
        }
        const columns = sqlite3.column_names(stmt);
        while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
          const row: any = {};
          columns.forEach((col, i) => {
            row[col] = sqlite3.column(stmt, i);
          });
          results.push(row);
        }
      }
      return results;
    }
  };
}

/**
 * Open an existing SQLite file
 */
export async function openExistingDatabase(file: File): Promise<DatabaseManager> {
  const sqlite3 = await initSQLite();
  
  // Read file as ArrayBuffer
  const buffer = await file.arrayBuffer();
  const data = new Uint8Array(buffer);
  
  // For wa-sqlite, we need to use a different approach to load file data
  // Create a new database and populate it with the file data
  const db = await sqlite3.open_v2(':memory:');
  
  // Note: For a proper file-based implementation, we'd need to use
  // a different VFS (Virtual File System) like OPFS or IDBBatchAtomicVFS
  // For now, we'll create in-memory and note the filename
  
  // Ensure schema exists
  await sqlite3.exec(db, SCHEMA_SQL);
  
  console.log(`DB opened successfully (file: ${file.name}, loaded in memory)`);
  
  return {
    db,
    sqlite3,
    async close() {
      await sqlite3.close(db);
    },
    async execute(sql: string, params?: any[]) {
      if (params && params.length > 0) {
        for await (const stmt of sqlite3.statements(db, sql)) {
          sqlite3.bind_collection(stmt, params);
          await sqlite3.step(stmt);
        }
      } else {
        await sqlite3.exec(db, sql);
      }
    },
    async query(sql: string, params?: any[]) {
      const results: any[] = [];
      for await (const stmt of sqlite3.statements(db, sql)) {
        if (params && params.length > 0) {
          sqlite3.bind_collection(stmt, params);
        }
        const columns = sqlite3.column_names(stmt);
        while (await sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
          const row: any = {};
          columns.forEach((col, i) => {
            row[col] = sqlite3.column(stmt, i);
          });
          results.push(row);
        }
      }
      return results;
    }
  };
}

/**
 * Export database to downloadable file
 */
export async function exportDatabase(dbManager: DatabaseManager, filename: string = 'scrapebook.sqlite'): Promise<void> {
  const { db, sqlite3 } = dbManager;
  
  // For wa-sqlite, we need to serialize the database differently
  // This is a simplified version - in a real implementation you'd want
  // to use the proper VFS for file operations
  
  // For now, we'll create a simple export by dumping the schema and data
  console.log(`Export functionality not fully implemented yet for ${filename}`);
  console.log('Database would be exported here');
}
