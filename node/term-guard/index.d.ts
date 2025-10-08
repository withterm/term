export enum Level {
  Error = "Error",
  Warning = "Warning",
  Info = "Info"
}

export interface ValidationResult {
  success: boolean;
  score: number;
  results: ValidationReport[];
}

export interface ValidationReport {
  checkName: string;
  checkLevel: string;
  checkDescription?: string;
  constraintMessage?: string;
  status: string;
  metric?: number;
}

export class Check {
  get name(): string;
  get description(): string | undefined;
}

export class CheckBuilder {
  constructor(name: string);
  level(level: Level): this;
  description(desc: string): this;
  
  // Completeness constraint
  completeness(column: string, threshold: number): Check;
  
  // Uniqueness constraint
  uniqueness(column: string, threshold?: number): Check;
  
  // Statistical constraints
  hasMin(column: string, minValue: number): Check;
  hasMax(column: string, maxValue: number): Check;
  hasMean(column: string, expected: number, tolerance?: number): Check;
  hasSum(column: string, expected: number, tolerance?: number): Check;
  hasStandardDeviation(column: string, expected: number, tolerance?: number): Check;
  hasQuantile(column: string, quantile: number, expected: number, tolerance?: number): Check;
  
  // Format validation
  validatesEmail(column: string, threshold?: number): Check;
  validatesUuid(column: string, threshold?: number): Check;
  
  // Custom constraints
  satisfies(expression: string): Check;
  hasSize(expected: number): Check;
  
  // Build the check
  build(): Check;
}

// Legacy DataSource API (kept for compatibility)
export class DataSource {
  static fromCsv(path: string): Promise<DataSource>;
  static fromParquet(path: string): Promise<DataSource>;
  static fromJson(path: string): Promise<DataSource>;
  sql(query: string): Promise<string[]>;
}

export class DataSourceBuilder {
  constructor();
  path(path: string): this;
  format(format: string): this;
  build(): Promise<DataSource>;
}

// New enhanced data source APIs
export interface CsvOptions {
  delimiter?: string;
  hasHeaders?: boolean;
  skipRows?: number;
  quoteChar?: string;
  escapeChar?: string;
  encoding?: string;
  maxRecords?: number;
  nullValue?: string;
}

export interface JsonOptions {
  lines?: boolean;
  maxRecords?: number;
  schemaInferenceSampleSize?: number;
}

export interface ParquetOptions {
  enableStatistics?: boolean;
  enablePageIndex?: boolean;
  batchSize?: number;
  rowGroupSize?: number;
}

export class FileDataSource {
  static fromCsv(path: string, options?: CsvOptions): Promise<FileDataSource>;
  static fromJson(path: string, options?: JsonOptions): Promise<FileDataSource>;
  static fromParquet(path: string, options?: ParquetOptions): Promise<FileDataSource>;
  getSchema(): Promise<string[]>;
  countRows(): Promise<number>;
}

export class MemoryDataSource {
  static fromJsonArray(data: string): Promise<MemoryDataSource>;
  static fromRecords(records: string[]): Promise<MemoryDataSource>;
  getSchema(): Promise<string[]>;
  countRows(): Promise<number>;
  query(sql: string): Promise<string>;
}

export class DataFusionContext {
  constructor();
  registerCsv(tableName: string, path: string, hasHeader?: boolean): Promise<void>;
  registerParquet(tableName: string, path: string): Promise<void>;
  registerJson(tableName: string, path: string): Promise<void>;
  sql(query: string): Promise<string>;
  execute(query: string): Promise<string[]>;
  listTables(): string[];
  getTableSchema(tableName: string): Promise<string[]>;
  createView(viewName: string, sql: string): Promise<void>;
  dropTable(tableName: string): Promise<void>;
  unionTables(resultTable: string, table1: string, table2: string): Promise<void>;
  joinTables(
    resultTable: string, 
    leftTable: string, 
    rightTable: string, 
    leftKey: string, 
    rightKey: string, 
    joinType?: string
  ): Promise<void>;
}

export enum DataSourceType {
  Csv = "Csv",
  Json = "Json",
  Parquet = "Parquet",
  Memory = "Memory",
  S3 = "S3",
  Azure = "Azure",
  Gcs = "Gcs"
}

export class UnifiedDataSource {
  static fromFile(path: string, format?: string): Promise<UnifiedDataSource>;
  static fromMemory(jsonData: string): Promise<UnifiedDataSource>;
  sql(query: string): Promise<string>;
  getSchema(): Promise<string[]>;
  countRows(): Promise<number>;
  getSourceType(): string;
}

// Cloud storage options (only available with cloud-storage feature)
export interface S3Options {
  region: string;
  accessKeyId?: string;
  secretAccessKey?: string;
  sessionToken?: string;
  endpoint?: string;
  allowHttp?: boolean;
}

export interface AzureOptions {
  accountName: string;
  accessKey?: string;
  sasToken?: string;
  containerName: string;
}

export interface GcsOptions {
  projectId: string;
  credentialsPath?: string;
  bucketName: string;
}

export class CloudDataSource {
  static fromS3(path: string, options: S3Options): Promise<CloudDataSource>;
  static fromAzure(path: string, options: AzureOptions): Promise<CloudDataSource>;
  static fromGcs(path: string, options: GcsOptions): Promise<CloudDataSource>;
  getSchema(): Promise<string[]>;
  getSourceType(): string;
}

export class ValidationSuite {
  run(data: DataSource): Promise<ValidationResult>;
  get name(): string;
  get description(): string | undefined;
  get checks(): Check[];
}

export class ValidationSuiteBuilder {
  constructor(name: string);
  description(desc: string): this;
  addCheck(check: Check): this;
  addChecks(checks: Check[]): this;
  build(): ValidationSuite;
}