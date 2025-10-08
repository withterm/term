import { existsSync } from 'node:fs';
import { join } from 'node:path';

const { platform, arch } = process;

let nativeBinding: any;

const POSSIBLE_TARGETS = [
  ['darwin', 'x64', 'darwin-x64'],
  ['darwin', 'arm64', 'darwin-arm64'],
  ['linux', 'x64', 'linux-x64-gnu'],
  ['linux', 'arm64', 'linux-arm64-gnu'],
  ['win32', 'x64', 'win32-x64-msvc'],
];

const loadError = new Error(`Failed to load native binding for ${platform} ${arch}`);

for (const [targetPlatform, targetArch, targetName] of POSSIBLE_TARGETS) {
  if (platform === targetPlatform && arch === targetArch) {
    const localPath = join(__dirname, '..', `term-guard.${targetName}.node`);
    const npmPath = join(__dirname, '..', `term-guard-${targetName}`, `term-guard.${targetName}.node`);
    
    if (existsSync(localPath)) {
      try {
        nativeBinding = require(localPath);
        break;
      } catch (e) {
        console.error(`Failed to load native binding from ${localPath}:`, e);
      }
    }
    
    if (existsSync(npmPath)) {
      try {
        nativeBinding = require(npmPath);
        break;
      } catch (e) {
        console.error(`Failed to load native binding from ${npmPath}:`, e);
      }
    }
  }
}

if (!nativeBinding) {
  const buildPath = join(__dirname, '..', 'index.node');
  if (existsSync(buildPath)) {
    try {
      nativeBinding = require(buildPath);
    } catch (e) {
      console.error(`Failed to load native binding from ${buildPath}:`, e);
      throw loadError;
    }
  } else {
    throw loadError;
  }
}

export const {
  CheckBuilder,
  ValidationSuite,
  ValidationSuiteBuilder,
  ValidationSuiteRunner,
  DataSourceBuilder,
  ValidationResult,
  Level,
  FormatType,
  FileDataSource,
  MemoryDataSource,
  DataFusionContext,
  UnifiedDataSource,
  CloudDataSource
} = nativeBinding;

export interface CsvOptions {
  delimiter?: string;
  hasHeaders?: boolean;
  skipRows?: number;
  maxRows?: number;
  encoding?: string;
}

export interface JsonOptions {
  maxRows?: number;
}

export interface ParquetOptions {
  maxRows?: number;
}