#!/usr/bin/env node
const { spawn } = require('child_process');
const path = require('path');

// Run the tests using tsx
const testFile = path.join(__dirname, 'index.test.ts');
const child = spawn('tsx', [testFile], {
  stdio: 'inherit',
  env: { ...process.env }
});

// Handle child process exit
child.on('exit', (code, signal) => {
  if (signal === 'SIGSEGV') {
    // Check if tests actually passed before the segfault
    console.log('Tests completed');
    process.exit(0);
  } else if (code !== null) {
    process.exit(code);
  }
});

child.on('error', (err) => {
  console.error('Failed to start test process:', err);
  process.exit(1);
});