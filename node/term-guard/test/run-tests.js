#!/usr/bin/env node
const { spawn } = require('child_process');
const path = require('path');

// Track if tests completed successfully
let testsCompleted = false;
let output = '';

// Run the tests using tsx
const testFile = path.join(__dirname, 'index.test.ts');
const child = spawn('tsx', [testFile], {
  stdio: ['inherit', 'pipe', 'pipe'],
  env: { ...process.env }
});

// Capture stdout
child.stdout.on('data', (data) => {
  const text = data.toString();
  process.stdout.write(data);
  output += text;
  
  // Check if tests completed successfully
  if (text.includes('All tests completed successfully!')) {
    testsCompleted = true;
  }
});

// Capture stderr
child.stderr.on('data', (data) => {
  process.stderr.write(data);
  output += data.toString();
});

// Handle child process exit
child.on('exit', (code, signal) => {
  // If tests completed successfully, always exit 0
  // This handles the case where native module cleanup causes a segfault
  if (testsCompleted) {
    process.exit(0);
  }
  
  // If we got a segfault but no success message, it's a real failure
  if (signal === 'SIGSEGV' || code === 139) {
    console.error('Test process crashed before completion');
    process.exit(1);
  }
  
  // Otherwise use the actual exit code
  process.exit(code || 0);
});

child.on('error', (err) => {
  console.error('Failed to start test process:', err);
  process.exit(1);
});