import * as SQLite from 'wa-sqlite';
import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs';

export class OpKey {
  static readonly BYTE_LENGTH = 32;
  readonly #bytes: Uint8Array;

  private constructor(bytes: Uint8Array) {
    if (bytes.byteLength !== OpKey.BYTE_LENGTH) {
      throw new Error(`OpKey must be exactly ${OpKey.BYTE_LENGTH} bytes, got ${bytes.byteLength}`);
    }
    this.#bytes = new Uint8Array(bytes); // defensive copy
  }

  /** 32 random bytes */
  static random(): OpKey {
    const b = new Uint8Array(OpKey.BYTE_LENGTH);
    crypto.getRandomValues(b);
    return new OpKey(b);
  }

  /** SHA-256 of input (ArrayBuffer or view or string) */
  static async sha256(input: ArrayBuffer | ArrayBufferView | string): Promise<OpKey> {
    let buffer: ArrayBuffer;
    if (typeof input === "string") {
      const encoded = new TextEncoder().encode(input);
      buffer = encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength);
    } else if (input instanceof ArrayBuffer) {
      buffer = input;
    } else {
      // For ArrayBufferView, copy to a new ArrayBuffer
      const view = new Uint8Array(input.buffer, input.byteOffset, input.byteLength);
      const newBuffer = new ArrayBuffer(view.byteLength);
      new Uint8Array(newBuffer).set(view);
      buffer = newBuffer;
    }
    const digest = await crypto.subtle.digest("SHA-256", buffer);
    return new OpKey(new Uint8Array(digest));
  }

  /** Copy raw bytes out */
  toBytes(): Uint8Array {
    return new Uint8Array(this.#bytes);
  }

  /** Constant-time equality */
  equals(other: OpKey): boolean {
    const a = this.#bytes, b = other.#bytes;
    let diff = a.length ^ b.length;
    for (let i = 0; i < a.length && i < b.length; i++) diff |= a[i] ^ b[i];
    return diff === 0;
  }

  /** Total order: lexicographic by unsigned bytes */
  static compare(a: OpKey, b: OpKey): number {
    const A = a.#bytes, B = b.#bytes;
    for (let i = 0; i < OpKey.BYTE_LENGTH; i++) {
      if (A[i] !== B[i]) return A[i] - B[i];
    }
    return 0;
  }

  /**
   * Extend: SHA256( "OpKey/extend" || key || u32be(len(data)) || data )
   * Length prefix prevents concatenation ambiguities for variable-length data.
   */
  async extend(newData: ArrayBuffer | ArrayBufferView | string): Promise<OpKey> {
    const data =
      typeof newData === "string"
        ? new TextEncoder().encode(newData)
        : newData instanceof ArrayBuffer
          ? new Uint8Array(newData)
          : new Uint8Array(newData.buffer, newData.byteOffset, newData.byteLength);

    const total = this.#bytes.length + data.length;
    const buf = new Uint8Array(total);
    let o = 0;
    buf.set(this.#bytes, o); o += this.#bytes.length;
    buf.set(data, o); o += data.length;
    const digest = await crypto.subtle.digest("SHA-256", buf);
    return new OpKey(new Uint8Array(digest));
  }
}


export interface DbLifecycle {
  initialize(): Promise<void>;
  close(): Promise<void>;
}

export interface OpDb {
  expandObservationSets(observationSets: Set<string>): Promise<Set<string>>;
  unionObservationSets(observationSets: Set<string>): Promise<string | null>;
}

export interface QueryExecutor {
  execute(sql: string, params?: any[]): Promise<void>;
  query(sql: string, params?: any[]): Promise<any[]>;
}

export abstract class SqlOpDb implements OpDb, QueryExecutor {
  abstract execute(sql: string, params?: any[]): Promise<void>;
  abstract query(sql: string, params?: any[]): Promise<any[]>;

  async expandObservationSets(observationSets: Set<string>): Promise<Set<string>> {
    if (observationSets.size == 0) {
      return observationSets
    }
    throw Error("observation set expansion not yet implemented")
  }

  async unionObservationSets(observationSets: Set<string>): Promise<string | null> {
    if (observationSets.size <= 1) {
      const { value, done } = observationSets.values().next();
      return done ? null : value;
    }
    throw Error("observation set union not yet implemented")
  }
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
 * • observation_sets: content-addressed sets for multi-origin ground truths.
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
   • observation_sets: content-addressed sets for multi-origin ground truths.
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

  input_digest  BLOB NOT NULL,      -- 32B SHA-256 hash

  ground_truth  BLOB,               -- 32B; NULL only if synthetic / no provenance
  observed_at   TEXT,               -- optional wall clock; MUST be NULL unless observation

  alias         BLOB,               -- nullable: op_key this row *encapsulates* (outputs copied)

  CHECK ( (observed_at IS NULL) OR (op_key = ground_truth) )
  CHECK ( (input_digest <> ground_truth) )
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
CREATE TABLE IF NOT EXISTS observation_sets (
  set_hash      BLOB NOT NULL,    -- SHA-256 hash of the keys in this set in sorted order
  member        BLOB NOT NULL,    -- op_key which is a member of this set 
  PRIMARY KEY (set_hash, member)
);

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
`;

export class SqliteOpDb extends SqlOpDb implements DbLifecycle {
  constructor(
    public db: number,
    public sqlite3: any
  ) {
    super();
  }

  async initialize(): Promise<void> {
    await this.sqlite3.exec(this.db, SCHEMA_SQL);
  }

  async close(): Promise<void> {
    await this.sqlite3.close(this.db);
  }

  async execute(sql: string, params?: any[]): Promise<void> {
    if (params && params.length > 0) {
      for await (const stmt of this.sqlite3.statements(this.db, sql)) {
        this.sqlite3.bind_collection(stmt, params);
        await this.sqlite3.step(stmt);
      }
    } else {
      await this.sqlite3.exec(this.db, sql);
    }
  }

  async query(sql: string, params?: any[]): Promise<any[]> {
    const results: any[] = [];
    for await (const stmt of this.sqlite3.statements(this.db, sql)) {
      if (params && params.length > 0) {
        this.sqlite3.bind_collection(stmt, params);
      }
      const columns = this.sqlite3.column_names(stmt);
      while (await this.sqlite3.step(stmt) === SQLite.SQLITE_ROW) {
        const row: any = {};
        columns.forEach((col: any, i: number) => {
          row[col] = this.sqlite3.column(stmt, i);
        });
        results.push(row);
      }
    }
    return results;
  }
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
 * Create a new in-memory database
 */
export async function createInMemoryDatabase(): Promise<SqliteOpDb> {
  const sqlite3 = await initSQLite();
  const db = await sqlite3.open_v2(':memory:');

  console.log('DB opened successfully (in-memory database)');

  const dbm = new SqliteOpDb(db, sqlite3);
  await dbm.initialize();
  return dbm;
}

/**
 * Open an existing SQLite file
 */
export async function openExistingDatabase(file: File): Promise<SqliteOpDb> {
  throw new Error(`Opening existing SQLite files is not yet implemented. File: ${file.name}`);
}

/**
 * Export database to downloadable file
 */
export async function exportDatabase(dbManager: SqliteOpDb, filename: string = 'scrapebook.sqlite'): Promise<void> {
  throw new Error(`Exporting SQLite files is not yet implemented. File: ${filename}`);
}
