<script lang="ts">
  import { onMount } from 'svelte';
  import { createInMemoryDatabase, openExistingDatabase, type SqliteOpDb } from '$lib/database.js';

  let dbManager: SqliteOpDb | null = null;
  let statusText = 'Not connected';
  let status = 'DISCONNECTED';
  let fileInput: HTMLInputElement;

  async function createNewDatabase() {
    try {
      // If a database is currently open, close it
      await closeDatabase();

      statusText = 'Creating in-memory database...';
      status = 'CONNECTING';
      dbManager = await createInMemoryDatabase();
      statusText = 'Connected to in-memory database';
      status = 'CONNECTED';

      // Test the database by inserting a sample record
      await testDatabase();
    } catch (error) {
      console.error('Failed to create database:', error);
      statusText = `${error}`;
      status = 'ERROR';
    }
  }

  async function openFileDatabase() {
    // If a database is currently open, close it
    await closeDatabase();

    const file = fileInput.files?.[0];
    if (!file) {
      alert('Please select a file first');
      return;
    }

    try {
      statusText = `Opening database from ${file.name}...`;
      status = 'CONNECTING';
      dbManager = await openExistingDatabase(file);
      statusText = `Connected to database: ${file.name}`;
      status = 'CONNECTED';

      // Test the database
      await testDatabase();
    } catch (error) {
      console.error('Failed to open database:', error);
      statusText = `${error}`;
      status = 'ERROR';
    }
  }

  async function testDatabase() {
    if (!dbManager) return;

    try {      
      // Query the ops table
      const ops = await dbManager.query('SELECT COUNT(*) as count FROM ops');
      console.log('Operations in database:', ops);
      
      // Query all table names to verify schema
      const tables = await dbManager.query(`
        SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
      `);
      console.log('Tables in database:', tables);
      
    } catch (error) {
      console.error('Database test failed:', error);
    }
  }

  async function closeDatabase() {
    if (dbManager) {
      await dbManager.close();
      dbManager = null;
      statusText = 'Disconnected';
      status = 'DISCONNECTED';
    }
  }

  onMount(() => {
    // Auto-create in-memory database on load for demo
    createNewDatabase();
  });
</script>

<main>
  <h1>Scrapebook SPA</h1>
  <p>SQLite Database Test - First Milestone</p>
  
  <div class="status">
    <strong>Status ({status}):</strong> {statusText}
  </div>

  <div class="controls">
    <button on:click={createNewDatabase}>Create New In-Memory Database</button>
    
    <div class="file-input">
      <input 
        bind:this={fileInput}
        type="file" 
        accept=".sqlite,.sqlite3,.db" 
        id="file-input"
      />
      <button on:click={openFileDatabase}>Open Existing Database File</button>
    </div>
    
    {#if dbManager}
      <button on:click={testDatabase}>Test Database</button>
      <button on:click={closeDatabase}>Close Database</button>
    {/if}
  </div>

  <div class="info">
    <h2>Database Schema</h2>
    <p>The database includes the following tables:</p>
    <ul>
      <li><code>ops</code> - Operations in the pipeline graph</li>
      <li><code>op_outputs</code> - Output payloads from operations</li>
      <li><code>op_inputs</code> - Input edges between operations</li>
      <li><code>composite_ground_truth</code> - Composite ground truth data</li>
      <li><code>artifacts</code> - Artifact store for GC/deduplication</li>
    </ul>
    
    <p><strong>Check the browser console</strong> for detailed logs about database operations.</p>
  </div>
</main>

<style>
  main {
    max-width: 800px;
    margin: 0 auto;
    padding: 2rem;
    font-family: system-ui, sans-serif;
  }

  .status {
    background: #f0f0f0;
    padding: 1rem;
    border-radius: 4px;
    margin: 1rem 0;
    font-family: monospace;
  }

  .controls {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    margin: 2rem 0;
  }

  .file-input {
    display: flex;
    gap: 1rem;
    align-items: center;
  }

  button {
    padding: 0.5rem 1rem;
    background: #007acc;
    color: white;
    border: none;
    border-radius: 4px;
    cursor: pointer;
    font-size: 1rem;
  }

  button:hover {
    background: #005999;
  }

  button:disabled {
    background: #ccc;
    cursor: not-allowed;
  }

  .info {
    background: #f9f9f9;
    padding: 1.5rem;
    border-radius: 4px;
    margin-top: 2rem;
  }

  .info h2 {
    margin-top: 0;
  }

  .info ul {
    margin: 1rem 0;
  }

  .info code {
    background: #e0e0e0;
    padding: 0.2rem 0.4rem;
    border-radius: 2px;
    font-family: monospace;
  }
</style>
