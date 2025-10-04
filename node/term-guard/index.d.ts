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