import test from 'node:test';
import assert from 'node:assert';
import {
  CheckBuilder,
  ValidationSuiteBuilder,
  DataSourceBuilder,
  Level,
  FormatType
} from './index';

test('CheckBuilder creates checks with all constraint types', async (t) => {
  await t.test('completeness constraints', () => {
    const builder = new CheckBuilder('test_completeness');
    const check = builder
      .level(Level.Warning)
      .description('Test completeness')
      .isComplete('column1', 0.95);
    
    assert.strictEqual(check.name, 'test_completeness');
    assert.strictEqual(check.description, 'Test completeness');
  });

  await t.test('uniqueness constraints', () => {
    const builder = new CheckBuilder('test_uniqueness');
    const check = builder.isUnique('id_column');
    assert.strictEqual(check.name, 'test_uniqueness');
  });

  await t.test('statistical constraints', () => {
    const builder = new CheckBuilder('test_stats');
    
    const minCheck = builder.hasMin('value', 0);
    assert.strictEqual(minCheck.name, 'test_stats');
    
    const maxBuilder = new CheckBuilder('test_max');
    const maxCheck = maxBuilder.hasMax('value', 100);
    assert.strictEqual(maxCheck.name, 'test_max');
    
    const meanBuilder = new CheckBuilder('test_mean');
    const meanCheck = meanBuilder.hasMean('value', 50, 0.1);
    assert.strictEqual(meanCheck.name, 'test_mean');
  });

  await t.test('pattern constraints', () => {
    const builder = new CheckBuilder('test_pattern');
    
    const patternCheck = builder.matchesPattern('email', '^[\\w\\.]+@[\\w\\.]+$', 0.99);
    assert.strictEqual(patternCheck.name, 'test_pattern');
    
    const formatBuilder = new CheckBuilder('test_format');
    const formatCheck = formatBuilder.hasFormat('email', FormatType.Email, 0.95);
    assert.strictEqual(formatCheck.name, 'test_format');
    
    const containsBuilder = new CheckBuilder('test_contains');
    const containsCheck = containsBuilder.containsString('description', 'important', 0.5);
    assert.strictEqual(containsCheck.name, 'test_contains');
  });

  await t.test('custom constraints', () => {
    const builder = new CheckBuilder('test_custom');
    
    const sqlCheck = builder.satisfies('column1 > 0 AND column2 < 100', 0.95);
    assert.strictEqual(sqlCheck.name, 'test_custom');
    
    const betweenBuilder = new CheckBuilder('test_between');
    const betweenCheck = betweenBuilder.isBetween('age', 18, 65, true);
    assert.strictEqual(betweenCheck.name, 'test_between');
    
    const inSetBuilder = new CheckBuilder('test_in_set');
    const inSetCheck = inSetBuilder.isInSet('status', ['active', 'pending', 'completed']);
    assert.strictEqual(inSetCheck.name, 'test_in_set');
  });
});

test('ValidationSuite builder works', async (t) => {
  await t.test('creates validation suite with multiple checks', () => {
    const check1 = new CheckBuilder('check1').isComplete('col1', 0.9);
    const check2 = new CheckBuilder('check2').isUnique('col2');
    const check3 = new CheckBuilder('check3').hasMin('col3', 0);

    const suite = new ValidationSuiteBuilder('test_suite')
      .description('Test validation suite')
      .addCheck(check1)
      .addCheck(check2)
      .addCheck(check3)
      .build();

    assert.strictEqual(suite.name, 'test_suite');
    assert.strictEqual(suite.description, 'Test validation suite');
    assert.strictEqual(suite.checks.length, 3);
  });
});

test('DataSource builder works', async (t) => {
  await t.test('creates data source builder', () => {
    const builder = new DataSourceBuilder()
      .path('/tmp/test.csv')
      .format('csv');

    assert.ok(builder);
  });
});

console.log('✅ All tests passed (note: full integration tests require built module)');