import test from 'node:test';
import assert from 'node:assert';
import * as fs from 'node:fs';
import * as path from 'node:path';
import {
  FileDataSource,
  MemoryDataSource,
  DataFusionContext,
  UnifiedDataSource,
  CsvOptions
} from './index';

interface TestData {
  id: number;
  name: string;
  email: string;
  age: number;
}

interface ProductData {
  id: number;
  value: number;
}

interface CategoryData {
  id: number;
  category: string;
  value: number;
}

function createTestData(): string {
  const testDir = path.join(__dirname, 'test-data');
  if (!fs.existsSync(testDir)) {
    fs.mkdirSync(testDir);
  }

  const csvData = `id,name,email,age
1,Alice,alice@example.com,30
2,Bob,bob@example.com,25
3,Charlie,charlie@example.com,35
4,Diana,diana@example.com,28`;
  fs.writeFileSync(path.join(testDir, 'test.csv'), csvData);

  const jsonData = [
    '{"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30}',
    '{"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25}',
    '{"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35}',
    '{"id": 4, "name": "Diana", "email": "diana@example.com", "age": 28}'
  ].join('\n');
  fs.writeFileSync(path.join(testDir, 'test.json'), jsonData);

  return testDir;
}

test('FileDataSource handles CSV files with options', async (t) => {
  const testDir = createTestData();

  await t.test('loads CSV with default options', async () => {
    const csvPath = path.join(testDir, 'test.csv');
    const dataSource = await FileDataSource.fromCsv(csvPath);
    
    const schema = await dataSource.getSchema();
    assert(schema.length > 0, 'Schema should have fields');
    assert(schema.some(field => field.includes('id')), 'Should have id field');
    assert(schema.some(field => field.includes('name')), 'Should have name field');
    
    const rowCount = await dataSource.countRows();
    assert.strictEqual(rowCount, 4, 'Should have 4 rows');
  });

  await t.test('loads CSV with custom delimiter', async () => {
    const csvData = `id;name;value
1;Item1;100
2;Item2;200`;
    const csvPath = path.join(testDir, 'test-semicolon.csv');
    fs.writeFileSync(csvPath, csvData);

    const options: CsvOptions = { delimiter: ';' };
    const dataSource = await FileDataSource.fromCsv(csvPath, options);
    
    const rowCount = await dataSource.countRows();
    assert.strictEqual(rowCount, 2, 'Should have 2 rows');
  });

  await t.test('handles CSV without headers', async () => {
    const csvData = `1,Alice,30
2,Bob,25
3,Charlie,35`;
    const csvPath = path.join(testDir, 'test-no-header.csv');
    fs.writeFileSync(csvPath, csvData);

    const options: CsvOptions = { hasHeaders: false };
    const dataSource = await FileDataSource.fromCsv(csvPath, options);
    
    const rowCount = await dataSource.countRows();
    assert.strictEqual(rowCount, 3, 'Should have 3 rows');
  });
});

test('FileDataSource handles JSON files', async (t) => {
  const testDir = createTestData();

  await t.test('loads newline-delimited JSON', async () => {
    const jsonPath = path.join(testDir, 'test.json');
    const dataSource = await FileDataSource.fromJson(jsonPath);
    
    const schema = await dataSource.getSchema();
    assert(schema.length > 0, 'Schema should have fields');
    
    const rowCount = await dataSource.countRows();
    assert.strictEqual(rowCount, 4, 'Should have 4 rows');
  });
});

test('MemoryDataSource handles JavaScript arrays', async (t) => {
  await t.test('creates data source from JSON array', async () => {
    const data: TestData[] = [
      { id: 1, name: 'Alice', email: 'alice@example.com', age: 30 },
      { id: 2, name: 'Bob', email: 'bob@example.com', age: 25 },
      { id: 3, name: 'Charlie', email: 'charlie@example.com', age: 35 }
    ];
    
    const jsonString = JSON.stringify(data);
    const dataSource = await MemoryDataSource.fromJsonArray(jsonString);
    
    const schema = await dataSource.getSchema();
    assert(schema.length === 4, 'Should have 4 fields');
    assert(schema.some(field => field.includes('id')), 'Should have id field');
    assert(schema.some(field => field.includes('name')), 'Should have name field');
    assert(schema.some(field => field.includes('email')), 'Should have email field');
    assert(schema.some(field => field.includes('age')), 'Should have age field');
    
    const rowCount = await dataSource.countRows();
    assert.strictEqual(rowCount, 3, 'Should have 3 rows');
  });

  await t.test('creates data source from individual records', async () => {
    const records = [
      JSON.stringify({ id: 1, value: 100 }),
      JSON.stringify({ id: 2, value: 200 }),
      JSON.stringify({ id: 3, value: 300 })
    ];
    
    const dataSource = await MemoryDataSource.fromRecords(records);
    
    const rowCount = await dataSource.countRows();
    assert.strictEqual(rowCount, 3, 'Should have 3 rows');
  });

  await t.test('executes SQL queries on memory data', async () => {
    const data: CategoryData[] = [
      { id: 1, category: 'A', value: 100 },
      { id: 2, category: 'B', value: 200 },
      { id: 3, category: 'A', value: 150 },
      { id: 4, category: 'B', value: 250 }
    ];
    
    const jsonString = JSON.stringify(data);
    const dataSource = await MemoryDataSource.fromJsonArray(jsonString);
    
    const result = await dataSource.query(
      "SELECT category, SUM(value) as total FROM data GROUP BY category ORDER BY category"
    );
    
    const parsed = JSON.parse(result) as Array<{ category: string; total: number }>;
    assert.strictEqual(parsed.length, 2, 'Should have 2 groups');
    assert.strictEqual(parsed[0].category, 'A', 'First group should be A');
    assert.strictEqual(parsed[0].total, 250, 'Category A total should be 250');
    assert.strictEqual(parsed[1].category, 'B', 'Second group should be B');
    assert.strictEqual(parsed[1].total, 450, 'Category B total should be 450');
  });
});

test('DataFusionContext provides advanced data operations', async (t) => {
  const testDir = createTestData();

  await t.test('registers multiple tables', async () => {
    const ctx = new DataFusionContext();
    
    const csv1Path = path.join(testDir, 'test.csv');
    await ctx.registerCsv('table1', csv1Path, true);
    
    const csv2Data = `product_id,product_name,price
101,Laptop,999.99
102,Mouse,29.99
103,Keyboard,79.99`;
    const csv2Path = path.join(testDir, 'products.csv');
    fs.writeFileSync(csv2Path, csv2Data);
    
    await ctx.registerCsv('products', csv2Path, true);
    
    const tables = ctx.listTables();
    assert(tables.includes('table1'), 'Should have table1');
    assert(tables.includes('products'), 'Should have products table');
  });

  await t.test('creates and queries views', async () => {
    const ctx = new DataFusionContext();
    const csvPath = path.join(testDir, 'test.csv');
    await ctx.registerCsv('users', csvPath, true);
    
    await ctx.createView('adults', 'SELECT * FROM users WHERE age >= 30');
    
    const result = await ctx.sql('SELECT COUNT(*) as count FROM adults');
    const parsed = JSON.parse(result) as Array<{ count: number }>;
    assert(parsed.length > 0, 'Should have results');
    assert.strictEqual(parsed[0].count, 2, 'Should have 2 adults');
  });

  await t.test('performs table joins', async () => {
    const ctx = new DataFusionContext();
    
    const csv1Data = `user_id,name
1,Alice
2,Bob`;
    const csv1Path = path.join(testDir, 'users_simple.csv');
    fs.writeFileSync(csv1Path, csv1Data);
    await ctx.registerCsv('users', csv1Path, true);
    
    const csv2Data = `user_id,order_id,amount
1,101,50.00
1,102,75.00
2,103,100.00`;
    const csv2Path = path.join(testDir, 'orders.csv');
    fs.writeFileSync(csv2Path, csv2Data);
    await ctx.registerCsv('orders', csv2Path, true);
    
    await ctx.joinTables('user_orders', 'users', 'orders', 'user_id', 'user_id', 'INNER');
    
    const result = await ctx.sql('SELECT COUNT(*) as count FROM user_orders');
    const parsed = JSON.parse(result) as Array<{ count: number }>;
    assert.strictEqual(parsed[0].count, 3, 'Should have 3 joined rows');
  });
});

test('UnifiedDataSource provides a simple API for various sources', async (t) => {
  const testDir = createTestData();

  await t.test('automatically detects file format', async () => {
    const csvPath = path.join(testDir, 'test.csv');
    const dataSource = await UnifiedDataSource.fromFile(csvPath);
    
    assert.strictEqual(dataSource.getSourceType(), 'csv', 'Should detect CSV format');
    
    const rowCount = await dataSource.countRows();
    assert.strictEqual(rowCount, 4, 'Should have 4 rows');
  });

  await t.test('loads memory data', async () => {
    const data = [
      { x: 1, y: 2 },
      { x: 3, y: 4 },
      { x: 5, y: 6 }
    ];
    
    const dataSource = await UnifiedDataSource.fromMemory(JSON.stringify(data));
    
    assert.strictEqual(dataSource.getSourceType(), 'memory', 'Should be memory source');
    
    const result = await dataSource.sql('SELECT SUM(x) as sum_x, SUM(y) as sum_y FROM data');
    const parsed = JSON.parse(result) as Array<{ sum_x: number; sum_y: number }>;
    assert.strictEqual(parsed[0].sum_x, 9, 'Sum of x should be 9');
    assert.strictEqual(parsed[0].sum_y, 12, 'Sum of y should be 12');
  });
});

test.after(() => {
  const testDir = path.join(__dirname, 'test-data');
  if (fs.existsSync(testDir)) {
    fs.rmSync(testDir, { recursive: true, force: true });
  }
});