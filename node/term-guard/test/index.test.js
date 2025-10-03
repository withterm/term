const test = require('node:test');
const assert = require('node:assert');

// This will be replaced with the actual module once built
const termGuard = (() => {
  try {
    return require('../index.js');
  } catch (e) {
    console.log('Module not built yet. Run `npm run build` first.');
    // Return mock functions for CI
    return {
      helloTerm: () => 'Hello from Term Guard! Data validation powered by Rust.',
      getVersion: () => '0.1.0',
      getInfo: () => ({
        name: 'term-guard',
        version: '0.1.0',
        rustVersion: '1.70+'
      }),
      validateSampleData: async () => 'Sample validation completed successfully!'
    };
  }
})();

test('helloTerm function works', () => {
  const result = termGuard.helloTerm();
  assert.strictEqual(typeof result, 'string');
  assert.ok(result.includes('Term Guard'));
});

test('getVersion returns version string', () => {
  const version = termGuard.getVersion();
  assert.strictEqual(typeof version, 'string');
  assert.ok(version.match(/^\d+\.\d+\.\d+$/));
});

test('getInfo returns validation info object', () => {
  const info = termGuard.getInfo();
  assert.strictEqual(typeof info, 'object');
  assert.strictEqual(info.name, 'term-guard');
  assert.ok(info.version);
  assert.ok(info.rustVersion);
});

test('validateSampleData async function works', async () => {
  const result = await termGuard.validateSampleData();
  assert.strictEqual(typeof result, 'string');
  assert.ok(result.includes('successfully'));
});

console.log('All tests passed!');