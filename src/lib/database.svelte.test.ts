import { describe, it, expect } from 'vitest';
import { createInMemoryDatabase } from './database.js';

describe('Database', () => {
	it('should create and initialize database successfully', async () => {
		const dbManager = await createInMemoryDatabase();
		expect(dbManager).toBeDefined();
		expect(dbManager.db).toBeDefined();
		expect(dbManager.sqlite3).toBeDefined();
		
		// Test that we can query the schema tables
		const tables = await dbManager.query("SELECT name FROM sqlite_master WHERE type='table'");
		expect(tables).toBeDefined();
		expect(tables.length).toBeGreaterThan(0);
		
		// Verify core tables exist
		const tableNames = tables.map(row => row.name);
		expect(tableNames).toContain('ops');
		expect(tableNames).toContain('op_outputs');
		expect(tableNames).toContain('op_inputs');
		
		console.log('✅ DB opened successfully - in-memory database');
		console.log('Tables created:', tableNames);
		
		await dbManager.close();
	});
});
