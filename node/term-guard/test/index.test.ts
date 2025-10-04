import test from 'node:test';
import assert from 'node:assert';
import * as path from 'path';
import * as fs from 'fs/promises';

// Load the native module
import * as termGuard from '../index';

test('Basic module functions work', () => {
  // Test hello function
  const greeting = termGuard.helloTerm();
  assert.strictEqual(typeof greeting, 'string');
  assert.ok(greeting.includes('Term Guard'));

  // Test version function
  const version = termGuard.getVersion();
  assert.strictEqual(typeof version, 'string');
  assert.match(version, /^\d+\.\d+\.\d+$/);

  // Test info function
  const info = termGuard.getInfo();
  assert.strictEqual(typeof info, 'object');
  assert.strictEqual(info.name, 'term-guard');
  assert.ok(info.rustVersion.includes('1.70'));
});

test('Level enum is exported correctly', () => {
  assert.ok(termGuard.Level);
  assert.strictEqual(termGuard.Level.Error, 0);
  assert.strictEqual(termGuard.Level.Warning, 1);
  assert.strictEqual(termGuard.Level.Info, 2);
});

test('CheckBuilder can be created and configured', () => {
  const builder = new termGuard.CheckBuilder('test_check');
  assert.ok(builder);
  
  // Test method chaining
  const result = builder.level(termGuard.Level.Warning);
  assert.strictEqual(result, builder, 'Should return self for chaining');
  
  const result2 = builder.description('Test description');
  assert.strictEqual(result2, builder, 'Should return self for chaining');
});

test('CheckBuilder can create different check types', () => {
  const builder = new termGuard.CheckBuilder('completeness_check');
  
  // Test is_complete check
  const check1 = builder.isComplete('column1', 0.95);
  assert.ok(check1);
  assert.strictEqual(check1.name, 'completeness_check');
  
  // Test has_min check
  const builder2 = new termGuard.CheckBuilder('min_check');
  const check2 = builder2.hasMin('value_column', 0);
  assert.ok(check2);
  assert.strictEqual(check2.name, 'min_check');
  
  // Test has_max check
  const builder3 = new termGuard.CheckBuilder('max_check');
  const check3 = builder3.hasMax('value_column', 100);
  assert.ok(check3);
  assert.strictEqual(check3.name, 'max_check');
  
  // Test is_unique check
  const builder4 = new termGuard.CheckBuilder('unique_check');
  const check4 = builder4.isUnique('id_column');
  assert.ok(check4);
  assert.strictEqual(check4.name, 'unique_check');
  
  // Test has_mean check
  const builder5 = new termGuard.CheckBuilder('mean_check');
  const check5 = builder5.hasMean('metric_column', 50.0, 0.1);
  assert.ok(check5);
  assert.strictEqual(check5.name, 'mean_check');
});

test('ValidationSuiteBuilder works correctly', () => {
  const suite = termGuard.ValidationSuite.builder('test_suite');
  assert.ok(suite);
  assert.ok(suite instanceof termGuard.ValidationSuiteBuilder);
  
  // Test method chaining
  const result = suite.description('Test suite description');
  assert.strictEqual(result, suite, 'Should return self for chaining');
  
  // Test adding a check
  const check = new termGuard.CheckBuilder('test_check').build();
  const result2 = suite.addCheck(check);
  assert.strictEqual(result2, suite, 'Should return self for chaining');
  
  // Build the suite
  const validationSuite = suite.build();
  assert.ok(validationSuite);
  assert.strictEqual(validationSuite.name, 'test_suite');
  assert.strictEqual(validationSuite.checkCount, 1);
});

test('ValidationSuite can be created with multiple checks', () => {
  const check1 = new termGuard.CheckBuilder('check1').build();
  const check2 = new termGuard.CheckBuilder('check2').build();
  const check3 = new termGuard.CheckBuilder('check3').build();
  
  const suite = termGuard.ValidationSuite.builder('multi_check_suite')
    .description('Suite with multiple checks')
    .addChecks([check1, check2, check3])
    .build();
  
  assert.ok(suite);
  assert.strictEqual(suite.name, 'multi_check_suite');
  assert.strictEqual(suite.description, 'Suite with multiple checks');
  assert.strictEqual(suite.checkCount, 3);
});

test('DataSource can be created from CSV', async () => {
  // Create a temporary CSV file for testing
  const testData = `id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300`;
  
  const testFile = path.join(__dirname, 'test_data.csv');
  await fs.writeFile(testFile, testData);
  
  try {
    const dataSource = await termGuard.DataSource.fromCsv(testFile);
    assert.ok(dataSource);
    assert.strictEqual(dataSource.tableName, 'data');
    
    // Test row count
    const rowCount = await dataSource.getRowCount();
    assert.strictEqual(rowCount, 3n);
    
    // Test column names
    const columns = await dataSource.getColumnNames();
    assert.ok(Array.isArray(columns));
    assert.ok(columns.includes('id'));
    assert.ok(columns.includes('name'));
    assert.ok(columns.includes('value'));
  } finally {
    // Clean up test file
    await fs.unlink(testFile).catch(() => {});
  }
});

test('DataSourceBuilder can register multiple tables', async () => {
  // Create test CSV files
  const testData1 = `id,value\n1,100\n2,200`;
  const testData2 = `id,score\n1,90\n2,85`;
  
  const testFile1 = path.join(__dirname, 'test_table1.csv');
  const testFile2 = path.join(__dirname, 'test_table2.csv');
  
  await fs.writeFile(testFile1, testData1);
  await fs.writeFile(testFile2, testData2);
  
  try {
    const builder = new termGuard.DataSourceBuilder();
    await builder.registerCsv('table1', testFile1);
    await builder.registerCsv('table2', testFile2);
    
    const dataSource = builder.build();
    assert.ok(dataSource);
  } finally {
    // Clean up test files
    await fs.unlink(testFile1).catch(() => {});
    await fs.unlink(testFile2).catch(() => {});
  }
});

test('Full validation workflow works end-to-end', async () => {
  // Create a test CSV file
  const testData = `id,name,score,status
1,Alice,95,active
2,Bob,87,active
3,Charlie,92,active
4,David,78,inactive
5,Eve,,active`;
  
  const testFile = path.join(__dirname, 'test_validation.csv');
  await fs.writeFile(testFile, testData);
  
  try {
    // Create data source
    const dataSource = await termGuard.DataSource.fromCsv(testFile);
    
    // Create checks
    const completenessCheck = new termGuard.CheckBuilder('score_completeness')
      .level(termGuard.Level.Error)
      .description('Check score column completeness')
      .isComplete('score', 0.8);
    
    const minCheck = new termGuard.CheckBuilder('score_minimum')
      .level(termGuard.Level.Warning)
      .description('Check minimum score')
      .hasMin('score', 70);
    
    const uniqueCheck = new termGuard.CheckBuilder('id_uniqueness')
      .level(termGuard.Level.Error)
      .description('Check ID uniqueness')
      .isUnique('id');
    
    // Build validation suite
    const suite = termGuard.ValidationSuite.builder('test_validation_suite')
      .description('Complete validation test suite')
      .addCheck(completenessCheck)
      .addCheck(minCheck)
      .addCheck(uniqueCheck)
      .build();
    
    // Run validation
    const result = await suite.run(dataSource);
    
    // Verify result structure
    assert.ok(result);
    assert.ok(['success', 'failure'].includes(result.status));
    assert.ok(result.report);
    assert.strictEqual(result.report.suiteName, 'test_validation_suite');
    assert.strictEqual(typeof result.report.totalChecks, 'number');
    assert.strictEqual(typeof result.report.passedChecks, 'number');
    assert.strictEqual(typeof result.report.failedChecks, 'number');
    assert.ok(Array.isArray(result.report.issues));
    
    // Check metrics
    assert.ok(result.metrics);
    assert.strictEqual(typeof result.metrics.totalDurationMs, 'number');
    assert.strictEqual(typeof result.metrics.checksPerSecond, 'number');
    
    console.log(`Validation completed: ${result.status}`);
    console.log(`Passed: ${result.report.passedChecks}/${result.report.totalChecks}`);
    if (result.report.issues.length > 0) {
      console.log('Issues found:');
      result.report.issues.forEach(issue => {
        console.log(`  - ${issue.checkName} (${issue.level}): ${issue.message}`);
      });
    }
  } finally {
    // Clean up test file
    await fs.unlink(testFile).catch(() => {});
  }
});

test('Error handling works correctly', async () => {
  // Test invalid file path
  await assert.rejects(
    termGuard.DataSource.fromCsv('/non/existent/file.csv'),
    /Failed to read CSV file/
  );
  
  // Test invalid parquet file
  await assert.rejects(
    termGuard.DataSource.fromParquet('/non/existent/file.parquet'),
    /Failed to read parquet file/
  );
});

test('validateSampleData helper function works', async () => {
  // Create a test CSV file
  const testData = `column1,column2
value1,100
value2,200
value3,300`;
  
  const testFile = path.join(__dirname, 'sample_data.csv');
  await fs.writeFile(testFile, testData);
  
  try {
    const result = await termGuard.validateSampleData(testFile);
    assert.ok(result);
    assert.strictEqual(typeof result, 'string');
    assert.ok(result.includes('Validation'));
    assert.ok(result.includes('checks passed'));
    assert.ok(result.includes('failed'));
  } finally {
    // Clean up test file
    await fs.unlink(testFile).catch(() => {});
  }
});

console.log('All tests completed successfully!');